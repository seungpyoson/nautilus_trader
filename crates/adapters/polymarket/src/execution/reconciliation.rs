// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Reconciliation report generation for the Polymarket execution client.

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_core::{
    UnixNanos, collections::AtomicMap, datetime::NANOSECONDS_IN_SECOND, time::AtomicTime,
};
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus, PositionSideSpecified},
    identifiers::{AccountId, ClientId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    order_fill_tracker::OrderFillTrackerMap,
    parse::{
        build_maker_fill_report, instrument_fee_exponent, instrument_taker_fee, parse_fill_report,
        parse_order_status_report, parse_timestamp, parse_timestamp_checked,
    },
    report_validation::{condition_id, non_negative_quantity, positive_quantity, token_id},
};
use crate::{
    common::{
        consts::{DUST_POSITION_THRESHOLD, DUST_SNAP_THRESHOLD_DEC, USDC_DECIMALS},
        enums::{PolymarketLiquiditySide, PolymarketTradeStatus},
    },
    http::{
        clob::PolymarketClobHttpClient,
        data_api::PolymarketDataApiHttpClient,
        models::{DataApiPosition, PolymarketOpenOrder, PolymarketTradeReport},
        query::{GetOrdersParams, GetTradesParams},
    },
};

/// Shared context for trade-to-fill-report conversion.
pub(crate) struct FillContext<'a> {
    pub account_id: AccountId,
    pub user_address: &'a str,
    pub api_key: &'a str,
    pub pusd: Currency,
    pub clock: &'static AtomicTime,
}

/// Counts of confirmed trade evidence dropped while building fill reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FillBuildDiscards {
    /// Fill entries dropped because their instrument is not loaded.
    pub unmapped_instruments: usize,
    /// In-scope historical fills dropped because their instrument is not loaded.
    pub in_scope_historical: usize,
    /// In-scope confirmed maker trades dropped because no maker order in the
    /// match is owned by the account.
    pub unowned_maker_trades: usize,
}

impl FillBuildDiscards {
    pub(crate) fn ensure_no_in_scope_discards(self, operation: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.in_scope_historical == 0,
            "unmapped in-scope confirmed fill evidence while {operation}: {} row(s)",
            self.in_scope_historical,
        );
        anyhow::ensure!(
            self.unowned_maker_trades == 0,
            "unowned confirmed maker fill evidence while {operation}: {} trade(s)",
            self.unowned_maker_trades,
        );
        Ok(())
    }
}

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
pub(crate) fn build_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<(Vec<FillReport>, FillBuildDiscards)> {
    build_fill_reports_from_trades_scoped(
        trades,
        ctx,
        instruments,
        instrument_filter,
        None,
        ts_init,
        load_ids,
    )
}

pub(crate) fn build_fill_reports_from_trades_scoped(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    venue_order_filter: Option<VenueOrderId>,
    ts_init: UnixNanos,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<(Vec<FillReport>, FillBuildDiscards)> {
    let mut reports = Vec::new();
    let mut discards = FillBuildDiscards::default();

    for trade in trades {
        if trade.status != PolymarketTradeStatus::Confirmed {
            continue;
        }

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

        if is_maker {
            if !trade
                .maker_orders
                .iter()
                .any(|order| venue_order_in_scope(&order.order_id, venue_order_filter))
            {
                continue;
            }
            if !trade
                .maker_orders
                .iter()
                .filter(|order| venue_order_in_scope(&order.order_id, venue_order_filter))
                .any(|mo| mo.is_owned_by(ctx.user_address, ctx.api_key))
            {
                let mut in_scope = false;
                for order in &trade.maker_orders {
                    if !venue_order_in_scope(&order.order_id, venue_order_filter) {
                        continue;
                    }
                    let instrument_id = instrument_id_from_market_token(
                        trade.market.as_str(),
                        order.asset_id.as_str(),
                    )
                    .with_context(|| {
                        format!(
                            "invalid identity for confirmed maker trade {} order {}",
                            trade.id, order.order_id,
                        )
                    })?;
                    if historical_instrument_in_scope(instrument_id, instrument_filter, load_ids) {
                        in_scope = true;
                        break;
                    }
                }
                if in_scope {
                    discards.unowned_maker_trades += 1;
                    log::warn!(
                        "Confirmed in-scope maker trade {} holds no maker order owned by the \
                         account",
                        trade.id,
                    );
                } else {
                    log::debug!(
                        "Dropping out-of-scope maker trade {} with no order owned by the account",
                        trade.id,
                    );
                }
                continue;
            }

            for mo in &trade.maker_orders {
                if !venue_order_in_scope(&mo.order_id, venue_order_filter)
                    || !mo.is_owned_by(ctx.user_address, ctx.api_key)
                {
                    continue;
                }
                let token_id = mo.asset_id;
                let instrument = instruments.get_cloned(&token_id);
                let (instrument_id, price_prec, size_prec) = match instrument {
                    Some(i) => (i.id(), i.price_precision(), i.size_precision()),
                    None => {
                        classify_unmapped_historical(
                            &mut discards,
                            instrument_filter,
                            load_ids,
                            &trade.market,
                            token_id.as_str(),
                        )?;
                        continue;
                    }
                };

                if let Some(filter_id) = instrument_filter
                    && instrument_id != filter_id
                {
                    continue;
                }

                let ts_event =
                    parse_timestamp_checked(&trade.match_time, "confirmed maker trade match_time")
                        .with_context(|| {
                            format!("invalid match_time for confirmed maker trade {}", trade.id)
                        })?;
                let report = build_maker_fill_report(
                    mo,
                    &trade.id,
                    trade.trader_side,
                    trade.side,
                    trade.asset_id.as_str(),
                    ctx.account_id,
                    instrument_id,
                    price_prec,
                    size_prec,
                    ctx.pusd,
                    LiquiditySide::Maker,
                    ts_event,
                    ts_init,
                )
                .with_context(|| {
                    format!(
                        "failed to build maker fill report for trade {} and order {}",
                        trade.id, mo.order_id,
                    )
                })?;
                reports.push(report);
            }
        } else {
            if !venue_order_in_scope(&trade.taker_order_id, venue_order_filter) {
                continue;
            }
            let token_id = trade.asset_id;
            let Some(instrument) = instruments.get_cloned(&token_id) else {
                classify_unmapped_historical(
                    &mut discards,
                    instrument_filter,
                    load_ids,
                    &trade.market,
                    token_id.as_str(),
                )?;
                continue;
            };
            let instrument_id = instrument.id();

            if let Some(filter_id) = instrument_filter
                && instrument_id != filter_id
            {
                continue;
            }

            let price_prec = instrument.price_precision();
            let size_prec = instrument.size_precision();
            let taker_fee_rate = instrument_taker_fee(&instrument);
            let fee_exponent = instrument_fee_exponent(&instrument).with_context(|| {
                format!("invalid fee schedule for confirmed trade {}", trade.id)
            })?;

            let report = parse_fill_report(
                trade,
                instrument_id,
                ctx.account_id,
                None,
                price_prec,
                size_prec,
                ctx.pusd,
                taker_fee_rate,
                fee_exponent,
                ts_init,
            )
            .with_context(|| format!("failed to build taker fill report for trade {}", trade.id))?;
            reports.push(report);
        }
    }

    Ok((reports, discards))
}

/// Converts open orders into order status reports.
pub(crate) fn build_order_reports_from_orders(
    orders: &[PolymarketOpenOrder],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<(Vec<OrderStatusReport>, usize)> {
    let mut reports = Vec::new();
    let mut filtered = 0usize;

    for order in orders {
        let token_id = order.asset_id;
        let instrument = instruments.get_cloned(&token_id);
        let (instrument_id, price_prec, size_prec) = match instrument {
            Some(i) => (i.id(), i.price_precision(), i.size_precision()),
            None => {
                let instrument_id =
                    instrument_id_from_market_token(order.market.as_str(), token_id.as_str())
                        .with_context(|| {
                            format!("invalid identity for unmapped open order {}", order.id)
                        })?;

                if historical_instrument_in_scope(instrument_id, instrument_filter, load_ids) {
                    anyhow::bail!(unmapped_in_scope_message(
                        "open order",
                        instrument_id,
                        Some(&format!("token {token_id}")),
                        load_ids,
                    ));
                }
                log::debug!("Dropping out-of-scope unmapped open order instrument {instrument_id}");
                filtered += 1;
                continue;
            }
        };

        if let Some(filter_id) = instrument_filter
            && instrument_id != filter_id
        {
            continue;
        }

        let report = parse_order_status_report(
            order,
            instrument_id,
            account_id,
            None,
            price_prec,
            size_prec,
            ts_init,
        )
        .with_context(|| format!("failed to build order report for order {}", order.id))?;
        reports.push(report);
    }

    Ok((reports, filtered))
}

/// Builds position status reports from Data API positions, deliberately excluding flat and dust
/// rows while rejecting invalid in-scope authority evidence.
pub(crate) fn build_position_reports(
    positions: &[DataApiPosition],
    account_id: AccountId,
    ts: UnixNanos,
) -> anyhow::Result<Vec<PositionStatusReport>> {
    build_position_reports_scoped(positions, account_id, None, ts)
}

pub(crate) fn build_position_reports_scoped(
    positions: &[DataApiPosition],
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts: UnixNanos,
) -> anyhow::Result<Vec<PositionStatusReport>> {
    let mut reports = Vec::new();

    for position in positions {
        let condition_id = condition_id(&position.condition_id, "position condition ID")?;
        token_id(&position.asset, "position token ID")?;

        let raw_instrument_id = format!("{condition_id}-{}.POLYMARKET", position.asset);
        let instrument_id = InstrumentId::from_as_ref(&raw_instrument_id)
            .with_context(|| format!("invalid position instrument ID {raw_instrument_id:?}"))?;

        if let Some(filter_id) = instrument_filter
            && instrument_id != filter_id
        {
            continue;
        }

        if position.size >= Decimal::ZERO && position.size < DUST_POSITION_THRESHOLD {
            if position.size > Decimal::ZERO {
                log::debug!(
                    "Filtering dust position: {}-{}, size={}",
                    position.condition_id,
                    position.asset,
                    position.size
                );
            }
            continue;
        }
        let quantity = positive_quantity(position.size, USDC_DECIMALS as u8, "position quantity")?;
        let avg_price = position.avg_price.with_context(|| {
            format!("positive position {raw_instrument_id} has no average price")
        })?;
        anyhow::ensure!(
            avg_price > Decimal::ZERO && avg_price < Decimal::ONE,
            "position average price must satisfy 0 < price < 1, was {avg_price} for \
             {raw_instrument_id}"
        );

        reports.push(PositionStatusReport::new(
            account_id,
            instrument_id,
            PositionSideSpecified::Long,
            quantity,
            ts,
            ts,
            None,
            None,
            Some(avg_price),
        ));
    }

    Ok(reports)
}

pub(crate) fn retain_mapped_position_reports(
    reports: Vec<PositionStatusReport>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<Vec<PositionStatusReport>> {
    let mut kept = Vec::with_capacity(reports.len());

    for report in reports {
        if position_instrument_loaded(report.instrument_id, instruments) {
            kept.push(report);
            continue;
        }

        if instrument_in_load_ids_scope(report.instrument_id, load_ids) {
            anyhow::bail!(unmapped_in_scope_message(
                "position",
                report.instrument_id,
                None,
                load_ids,
            ));
        }
        log::debug!(
            "Dropping out-of-scope unmapped position instrument {}",
            report.instrument_id
        );
    }

    Ok(kept)
}

/// Full reconciliation mass status generation.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn generate_mass_status(
    http_client: &PolymarketClobHttpClient,
    data_api_client: &PolymarketDataApiHttpClient,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    fill_tracker: &OrderFillTrackerMap,
    ctx: &FillContext<'_>,
    client_id: ClientId,
    venue: Venue,
    lookback_mins: Option<u64>,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<Option<ExecutionMassStatus>> {
    let ts_init = ctx.clock.get_time_ns();
    let lookback_start = lookback_mins.map(|mins| {
        UnixNanos::from(
            ts_init.as_u64().saturating_sub(
                mins.saturating_mul(60)
                    .saturating_mul(NANOSECONDS_IN_SECOND),
            ),
        )
    });

    let orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;

    let (mut order_reports, orders_filtered) = build_order_reports_from_orders(
        &orders,
        instruments,
        ctx.account_id,
        None,
        ts_init,
        load_ids,
    )?;

    let mut trades = http_client
        .get_trades(trades_params_for_window(
            lookback_start,
            lookback_start.map(|_| ts_init),
        ))
        .await
        .context("failed to fetch trades for mass status")?;

    let mut untimestamped_trades = 0usize;

    if let Some(cutoff) = lookback_start {
        let mut retained_trades = Vec::with_capacity(trades.len());
        for trade in trades {
            match parse_timestamp(&trade.match_time) {
                Some(ts_event) if ts_event >= cutoff => retained_trades.push(trade),
                Some(_) => {}
                None if trade.status != PolymarketTradeStatus::Confirmed => {}
                None => {
                    let instrument_id = instrument_id_from_market_token(
                        trade.market.as_str(),
                        trade.asset_id.as_str(),
                    )
                    .with_context(|| {
                        format!(
                            "invalid identity for confirmed historical trade {}",
                            trade.id
                        )
                    })?;

                    if instrument_in_load_ids_scope(instrument_id, load_ids) {
                        untimestamped_trades += 1;
                    } else {
                        log::debug!(
                            "Dropping out-of-scope historical trade {} with unparsable match_time",
                            trade.id
                        );
                    }
                }
            }
        }
        trades = retained_trades;
    }

    let (mut fill_reports, fill_discards) =
        build_fill_reports_from_trades(&trades, ctx, instruments, None, ts_init, load_ids)?;

    if lookback_start.is_none() {
        fill_discards.ensure_no_in_scope_discards("generating unwindowed mass status")?;
    }

    if fill_discards.unowned_maker_trades > 0 {
        log::error!(
            "Mass status is missing {} confirmed maker trade(s) holding no maker order owned by \
             the account; executed quantity may be understated",
            fill_discards.unowned_maker_trades,
        );
    }

    fill_tracker.snap_fill_reports(&mut fill_reports);

    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;

    let position_reports = retain_mapped_position_reports(
        build_position_reports(&positions, ctx.account_id, ts_init)?,
        instruments,
        load_ids,
    )?;

    log::debug!(
        "Generated mass status: {} orders ({} filtered), {} fills ({} instrument-filtered, \
         {} in-scope historical misses, {} unowned maker trades), {} positions",
        order_reports.len(),
        orders_filtered,
        fill_reports.len(),
        fill_discards.unmapped_instruments,
        fill_discards.in_scope_historical,
        fill_discards.unowned_maker_trades,
        position_reports.len(),
    );

    if lookback_start.is_none() {
        cap_order_reports_to_confirmed_fills(&mut order_reports, &fill_reports)?;
    }

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);

    if let Some(lookback_start) = lookback_start {
        let reported_orders: AHashSet<VenueOrderId> = order_reports
            .iter()
            .map(|report| report.venue_order_id)
            .collect();
        let reports_complete = fill_discards.in_scope_historical == 0
            && fill_discards.unowned_maker_trades == 0
            && untimestamped_trades == 0
            && fill_reports
                .iter()
                .all(|report| reported_orders.contains(&report.venue_order_id));
        mass_status.set_report_window(Some(lookback_start), reports_complete);
    }

    mass_status.add_order_reports(order_reports);
    mass_status.add_position_reports(position_reports);
    mass_status.add_fill_reports(fill_reports);

    Ok(Some(mass_status))
}

pub(crate) fn trades_params_for_window(
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> GetTradesParams {
    GetTradesParams {
        // CLOB `after` is exclusive of the given Unix second
        after: start.map(|ts| unix_secs(ts).saturating_sub(1)),
        before: end.map(unix_secs),
        ..Default::default()
    }
}

fn unix_secs(ts: UnixNanos) -> u64 {
    ts.as_u64() / NANOSECONDS_IN_SECOND
}

fn instrument_id_from_market_token(
    market: &str,
    raw_token_id: &str,
) -> anyhow::Result<InstrumentId> {
    let market = condition_id(market, "historical condition ID")?;
    token_id(raw_token_id, "historical token ID")?;
    let raw_instrument_id = format!("{market}-{raw_token_id}.POLYMARKET");
    InstrumentId::from_as_ref(&raw_instrument_id)
        .with_context(|| format!("invalid historical instrument ID {raw_instrument_id:?}"))
}

fn instrument_in_load_ids_scope(
    instrument_id: InstrumentId,
    load_ids: Option<&[InstrumentId]>,
) -> bool {
    match load_ids {
        Some(ids) if !ids.is_empty() => ids.contains(&instrument_id),
        _ => true,
    }
}

fn historical_instrument_in_scope(
    instrument_id: InstrumentId,
    instrument_filter: Option<InstrumentId>,
    load_ids: Option<&[InstrumentId]>,
) -> bool {
    instrument_filter.map_or_else(
        || instrument_in_load_ids_scope(instrument_id, load_ids),
        |filter_id| filter_id == instrument_id,
    )
}

fn venue_order_in_scope(venue_order_id: &str, venue_order_filter: Option<VenueOrderId>) -> bool {
    venue_order_filter.is_none_or(|filter_id| venue_order_id == filter_id.as_str())
}

pub(crate) fn confirmed_trade_in_static_scope(
    trade: &PolymarketTradeReport,
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    venue_order_filter: Option<VenueOrderId>,
    load_ids: Option<&[InstrumentId]>,
) -> anyhow::Result<bool> {
    if trade.status != PolymarketTradeStatus::Confirmed {
        return Ok(false);
    }

    let instrument_in_scope = |raw_token_id: &str| -> anyhow::Result<bool> {
        if let Some(instrument) = instruments.get_cloned(&Ustr::from(raw_token_id)) {
            return Ok(instrument_filter.is_none_or(|filter_id| instrument.id() == filter_id));
        }

        let instrument_id = instrument_id_from_market_token(trade.market.as_str(), raw_token_id)
            .with_context(|| format!("invalid identity for confirmed trade {}", trade.id))?;
        Ok(historical_instrument_in_scope(
            instrument_id,
            instrument_filter,
            load_ids,
        ))
    };

    if trade.trader_side == PolymarketLiquiditySide::Maker {
        let has_owned_order = trade
            .maker_orders
            .iter()
            .filter(|order| venue_order_in_scope(&order.order_id, venue_order_filter))
            .any(|order| order.is_owned_by(ctx.user_address, ctx.api_key));
        for order in &trade.maker_orders {
            if !venue_order_in_scope(&order.order_id, venue_order_filter)
                || (has_owned_order && !order.is_owned_by(ctx.user_address, ctx.api_key))
            {
                continue;
            }
            if instrument_in_scope(order.asset_id.as_str())? {
                return Ok(true);
            }
        }
        Ok(false)
    } else if venue_order_in_scope(&trade.taker_order_id, venue_order_filter) {
        instrument_in_scope(trade.asset_id.as_str())
    } else {
        Ok(false)
    }
}

fn unmapped_in_scope_message(
    kind: &str,
    instrument_id: InstrumentId,
    detail: Option<&str>,
    load_ids: Option<&[InstrumentId]>,
) -> String {
    let hint = match load_ids {
        Some(ids) if ids.contains(&instrument_id) => {
            "this instrument is in instrument_config.load_ids but was not loaded"
        }
        _ => "set instrument_config.load_ids to the instruments this node should reconcile",
    };

    match detail {
        Some(detail) => {
            format!("unmapped in-scope {kind} instrument {instrument_id} ({detail}); {hint}")
        }
        None => format!("unmapped in-scope {kind} instrument {instrument_id}; {hint}"),
    }
}

fn position_instrument_loaded(
    instrument_id: InstrumentId,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
) -> bool {
    let symbol = instrument_id.symbol.as_str();
    symbol
        .rsplit_once('-')
        .is_some_and(|(_, token_id)| instruments.contains_key(&Ustr::from(token_id)))
}

fn classify_unmapped_historical(
    discards: &mut FillBuildDiscards,
    instrument_filter: Option<InstrumentId>,
    load_ids: Option<&[InstrumentId]>,
    market: &str,
    raw_token_id: &str,
) -> anyhow::Result<()> {
    let instrument_id = instrument_id_from_market_token(market, raw_token_id)?;
    discards.unmapped_instruments += 1;
    let in_scope = historical_instrument_in_scope(instrument_id, instrument_filter, load_ids);
    if in_scope {
        discards.in_scope_historical += 1;
        log::warn!("Unmapped in-scope historical instrument {instrument_id}");
        return Ok(());
    }

    log::debug!("Dropping out-of-scope unmapped historical instrument {instrument_id}");
    Ok(())
}

fn cap_order_reports_to_confirmed_fills(
    order_reports: &mut [OrderStatusReport],
    fill_reports: &[FillReport],
) -> anyhow::Result<()> {
    let confirmed_by_order = confirmed_filled_quantities(fill_reports)?;

    for report in order_reports {
        let local_filled = Quantity::zero(report.quantity.precision);
        cap_order_report_filled_qty(
            report,
            local_filled,
            confirmed_by_order.get(&report.venue_order_id).copied(),
        )?;
    }
    Ok(())
}

pub(crate) fn confirmed_filled_quantities(
    fill_reports: &[FillReport],
) -> anyhow::Result<AHashMap<VenueOrderId, Decimal>> {
    let mut confirmed_by_order = AHashMap::new();
    for fill in fill_reports {
        let total = confirmed_by_order
            .entry(fill.venue_order_id)
            .or_insert(Decimal::ZERO);
        *total = total
            .checked_add(fill.last_qty.as_decimal())
            .with_context(|| {
                format!(
                    "confirmed filled quantity overflow for order {}",
                    fill.venue_order_id
                )
            })?;
    }

    Ok(confirmed_by_order)
}

pub(crate) fn cap_order_report_filled_qty(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Option<Decimal>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        local_filled <= report.quantity,
        "local filled quantity {local_filled} exceeds order quantity {} for {}",
        report.quantity,
        report.venue_order_id,
    );
    let confirmed_filled = match confirmed_filled {
        Some(qty) => {
            non_negative_quantity(qty, report.quantity.precision, "confirmed filled quantity")?
        }
        None => Quantity::zero(report.quantity.precision),
    };
    anyhow::ensure!(
        confirmed_filled <= report.quantity,
        "confirmed filled quantity {confirmed_filled} exceeds order quantity {} for {}",
        report.quantity,
        report.venue_order_id,
    );
    let capped = report.filled_qty.min(local_filled.max(confirmed_filled));
    report.filled_qty = capped;
    normalize_terminal_order_report_quantity(report);
    anyhow::ensure!(
        report.filled_qty <= report.quantity,
        "filled quantity {} exceeds order quantity {} for {}",
        report.filled_qty,
        report.quantity,
        report.venue_order_id,
    );
    anyhow::ensure!(
        report.order_status != OrderStatus::Filled || report.filled_qty == report.quantity,
        "Filled order {} has filled quantity {} but order quantity {}",
        report.venue_order_id,
        report.filled_qty,
        report.quantity,
    );
    Ok(())
}

pub(crate) fn normalize_terminal_order_report_quantity(report: &mut OrderStatusReport) {
    if report.order_status != OrderStatus::Filled
        || report.filled_qty.is_zero()
        || report.filled_qty >= report.quantity
    {
        return;
    }

    let leaves = report.quantity.as_decimal() - report.filled_qty.as_decimal();
    if leaves < DUST_SNAP_THRESHOLD_DEC {
        log::debug!(
            "Normalizing terminal order report {} quantity from {} to confirmed fills {}",
            report.venue_order_id,
            report.quantity,
            report.filled_qty,
        );
        report.quantity = report.filled_qty;
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
        identifiers::TradeId,
        types::{Money, Price},
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    fn position(size: Decimal, avg_price: Option<Decimal>) -> DataApiPosition {
        DataApiPosition {
            asset: "123456789".to_string(),
            condition_id: format!("0x{}", "a".repeat(64)),
            size,
            avg_price,
        }
    }

    #[rstest]
    fn position_report_preserves_exact_average_price() {
        let reports = build_position_reports(
            &[position(dec!(1.000001), Some(dec!(0.1234567)))],
            AccountId::from("POLY-001"),
            UnixNanos::from(1),
        )
        .unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].quantity, Quantity::from("1.000001"));
        assert_eq!(reports[0].avg_px_open, Some(dec!(0.1234567)));
    }

    #[rstest]
    fn position_report_canonicalizes_equivalent_condition_id_hex_case() {
        let mut position = position(dec!(1), Some(dec!(0.5)));
        position.condition_id = format!("0X{}", "A".repeat(64));

        let reports =
            build_position_reports(&[position], AccountId::from("POLY-001"), UnixNanos::from(1))
                .expect("equivalent hexadecimal casing must be accepted");

        assert_eq!(
            reports[0].instrument_id,
            InstrumentId::from(format!("0x{}-123456789.POLYMARKET", "a".repeat(64)).as_str())
        );
    }

    #[rstest]
    fn position_reports_deliberately_exclude_flat_and_dust_rows() {
        let reports = build_position_reports(
            &[
                position(Decimal::ZERO, None),
                position(dec!(0.005), Some(dec!(0.5))),
                position(dec!(1), Some(dec!(0.5))),
            ],
            AccountId::from("POLY-001"),
            UnixNanos::from(1),
        )
        .unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].quantity, Quantity::from("1.000000"));
    }

    #[rstest]
    #[case::negative_size(dec!(-1), Some(dec!(0.5)), "position quantity")]
    #[case::inexact_size(dec!(1.0000001), Some(dec!(0.5)), "position quantity")]
    #[case::missing_average(dec!(1), None, "has no average price")]
    #[case::zero_average(dec!(1), Some(dec!(0)), "position average price")]
    #[case::unit_average(dec!(1), Some(dec!(1)), "position average price")]
    fn position_reports_reject_invalid_authority_values(
        #[case] size: Decimal,
        #[case] avg_price: Option<Decimal>,
        #[case] expected: &str,
    ) {
        let error = build_position_reports(
            &[position(size, avg_price)],
            AccountId::from("POLY-001"),
            UnixNanos::from(1),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "unexpected position error: {error}"
        );
    }

    #[rstest]
    fn position_reports_reject_invalid_instrument_identity() {
        let mut position = position(dec!(1), Some(dec!(0.5)));
        position.condition_id = "invalid-condition".to_string();

        let error =
            build_position_reports(&[position], AccountId::from("POLY-001"), UnixNanos::from(1))
                .unwrap_err();

        assert!(error.to_string().contains("position condition ID"));
    }

    #[rstest]
    fn position_reports_reject_invalid_identity_before_dust_exclusion() {
        let mut position = position(dec!(0.005), Some(dec!(0.5)));
        position.condition_id = "invalid-condition".to_string();

        let error =
            build_position_reports(&[position], AccountId::from("POLY-001"), UnixNanos::from(1))
                .expect_err("dust is excludable only after its provider identity is validated");

        assert!(error.to_string().contains("position condition ID"));
    }

    #[rstest]
    fn position_filter_excludes_unrelated_invalid_authority_values() {
        let unrelated = position(dec!(1), None);

        let reports = build_position_reports_scoped(
            &[unrelated],
            AccountId::from("POLY-001"),
            Some(InstrumentId::from(
                format!("0x{}-987654321.POLYMARKET", "b".repeat(64)).as_str(),
            )),
            UnixNanos::from(1),
        )
        .expect("unrelated position authority is outside the requested instrument scope");

        assert!(reports.is_empty());
    }

    #[rstest]
    fn caps_order_report_to_confirmed_companion_fills() {
        let account_id = AccountId::from("POLY-001");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let venue_order_id = VenueOrderId::from("V-1");
        let mut reports = vec![OrderStatusReport::new(
            account_id,
            instrument_id,
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from("10.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let fills = vec![FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::from("T-1"),
            OrderSide::Buy,
            Quantity::from("4.0000"),
            Price::from("0.5000"),
            Money::zero(Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &fills).unwrap();

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
    }

    #[rstest]
    #[case::below_threshold("99.995", "99.995")]
    fn normalizes_confirmed_dust_residual_to_order_quantity(
        #[case] confirmed: &str,
        #[case] expected_quantity: &str,
    ) {
        let account_id = AccountId::from("POLY-001");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let venue_order_id = VenueOrderId::from("V-DUST");
        let mut reports = vec![OrderStatusReport::new(
            account_id,
            instrument_id,
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("100.000"),
            Quantity::from("100.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let fills = vec![FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::from("T-DUST"),
            OrderSide::Buy,
            Quantity::from(confirmed),
            Price::from("0.5000"),
            Money::zero(Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &fills).unwrap();

        assert_eq!(reports[0].quantity, Quantity::from(expected_quantity));
        assert_eq!(reports[0].filled_qty, Quantity::from(confirmed));
    }

    #[rstest]
    fn rejects_filled_status_with_non_dust_partial_evidence() {
        let account_id = AccountId::from("POLY-001");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let venue_order_id = VenueOrderId::from("V-PARTIAL");
        let mut reports = vec![OrderStatusReport::new(
            account_id,
            instrument_id,
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("100.000"),
            Quantity::from("100.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let fills = vec![FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::from("T-PARTIAL"),
            OrderSide::Buy,
            Quantity::from("99.990"),
            Price::from("0.5000"),
            Money::zero(Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        let error = cap_order_reports_to_confirmed_fills(&mut reports, &fills)
            .expect_err("Filled must not survive non-dust partial evidence");

        assert!(error.to_string().contains("Filled order V-PARTIAL"));
    }

    #[rstest]
    fn trades_params_for_window_uses_exclusive_after_unix_seconds() {
        let start = UnixNanos::from(100 * NANOSECONDS_IN_SECOND);
        let end = UnixNanos::from(250 * NANOSECONDS_IN_SECOND);

        let params = trades_params_for_window(Some(start), Some(end));

        assert_eq!(params.after, Some(99));
        assert_eq!(params.before, Some(250));
    }

    fn unmapped_open_order() -> crate::http::models::PolymarketOpenOrder {
        crate::http::models::PolymarketOpenOrder {
            associate_trades: None,
            id: "0xid".to_string(),
            status: crate::common::enums::PolymarketOrderStatus::Live,
            market: Ustr::from(format!("0x{}", "a".repeat(64)).as_str()),
            original_size: rust_decimal_macros::dec!(10),
            outcome: crate::common::enums::PolymarketOutcome::yes(),
            maker_address: "0xmaker".to_string(),
            owner: "owner".to_string(),
            price: rust_decimal_macros::dec!(0.5),
            side: crate::common::enums::PolymarketOrderSide::Buy,
            size_matched: rust_decimal_macros::dec!(0),
            asset_id: Ustr::from("123456789"),
            expiration: None,
            order_type: crate::common::enums::PolymarketOrderType::GTC,
            created_at: 1_703_875_200,
        }
    }

    #[rstest]
    fn in_scope_unmapped_open_order_errors() {
        let error = build_order_reports_from_orders(
            &[unmapped_open_order()],
            &AtomicMap::new(),
            AccountId::from("POLY-001"),
            None,
            UnixNanos::from(1),
            None,
        )
        .expect_err("in-scope open-order miss must fail");

        let message = error.to_string();

        assert!(message.contains("unmapped in-scope open order"));
        assert!(message.contains("set instrument_config.load_ids"));
    }

    #[rstest]
    fn named_load_ids_unmapped_open_order_names_failed_load() {
        let instrument_id =
            InstrumentId::from(format!("0x{}-123456789.POLYMARKET", "a".repeat(64)).as_str());
        let error = build_order_reports_from_orders(
            &[unmapped_open_order()],
            &AtomicMap::new(),
            AccountId::from("POLY-001"),
            None,
            UnixNanos::from(1),
            Some(std::slice::from_ref(&instrument_id)),
        )
        .expect_err("named in-scope open-order miss must fail");
        let message = error.to_string();

        assert!(message.contains("unmapped in-scope open order"));
        assert!(message.contains("in instrument_config.load_ids but was not loaded"));
    }

    #[rstest]
    fn out_of_scope_unmapped_open_order_is_dropped() {
        let scoped = InstrumentId::from("OTHER.POLYMARKET");

        let (reports, filtered) = build_order_reports_from_orders(
            &[unmapped_open_order()],
            &AtomicMap::new(),
            AccountId::from("POLY-001"),
            None,
            UnixNanos::from(1),
            Some(std::slice::from_ref(&scoped)),
        )
        .expect("out-of-scope open-order miss is dropped");

        assert!(reports.is_empty());
        assert_eq!(filtered, 1);
    }

    #[rstest]
    fn instrument_filter_excludes_unrelated_unmapped_open_order() {
        let (reports, filtered) = build_order_reports_from_orders(
            &[unmapped_open_order()],
            &AtomicMap::new(),
            AccountId::from("POLY-001"),
            Some(InstrumentId::from("OTHER.POLYMARKET")),
            UnixNanos::from(1),
            None,
        )
        .expect("an explicit instrument filter excludes unrelated unmapped evidence");

        assert!(reports.is_empty());
        assert_eq!(filtered, 1);
    }

    #[rstest]
    fn in_scope_unmapped_position_errors() {
        let reports = vec![PositionStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("0xmarket-token.POLYMARKET"),
            PositionSideSpecified::Long,
            Quantity::from("10.000000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
            None,
            None,
        )];

        let error = retain_mapped_position_reports(reports, &AtomicMap::new(), None)
            .expect_err("in-scope position miss must fail");

        let message = error.to_string();

        assert!(message.contains("unmapped in-scope position"));
        assert!(message.contains("set instrument_config.load_ids"));
    }
}
