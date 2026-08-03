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

use ahash::AHashMap;
use anyhow::Context;
use nautilus_core::{UnixNanos, collections::AtomicMap, time::AtomicTime};
use nautilus_model::{
    enums::{LiquiditySide, PositionSideSpecified},
    identifiers::{AccountId, ClientId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    local_orders::{ArtifactAdmission, LocalOrderCoordinator},
    parse::{
        build_maker_fill_report, instrument_fee_exponent, instrument_taker_fee, parse_fill_report,
        parse_order_status_report, parse_timestamp,
    },
};
use crate::{
    common::{
        consts::{DUST_POSITION_THRESHOLD, USDC_DECIMALS},
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct FillReportQuery {
    pub instrument_filter: Option<InstrumentId>,
    pub venue_order_filter: Option<VenueOrderId>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
}

/// Losses encountered while converting authenticated venue trades into fills.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FillBuildFindings {
    /// Entries whose instrument was not loaded, so no report could be built.
    pub unmapped_instruments: usize,
    /// Confirmed maker trades holding none of this account's maker orders.
    pub unowned_maker_trades: usize,
    /// Relevant trades rejected by the caller's time window.
    pub outside_lookback: usize,
    /// Relevant trades kept despite an unparsable venue timestamp.
    pub unknown_age: usize,
}

impl FillBuildFindings {
    pub(crate) fn is_empty(self) -> bool {
        self.unmapped_instruments == 0
            && self.unowned_maker_trades == 0
            && self.outside_lookback == 0
            && self.unknown_age == 0
    }

    pub(crate) fn report(self, level: log::Level, context: &str) {
        if self.unmapped_instruments == 0 && self.unowned_maker_trades == 0 && self.unknown_age == 0
        {
            return;
        }
        log::log!(
            level,
            "{context}: {} entr(ies) had no loaded instrument; {} confirmed maker trade(s) held no maker order owned by this account; {} trade(s) of unknown age were kept inside a bounded query",
            self.unmapped_instruments,
            self.unowned_maker_trades,
            self.unknown_age,
        );
    }
}

fn trade_mentions_venue_order(trade: &PolymarketTradeReport, venue_order_id: VenueOrderId) -> bool {
    trade.taker_order_id == venue_order_id.as_str()
        || trade
            .maker_orders
            .iter()
            .any(|order| order.order_id == venue_order_id.as_str())
}

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
pub(crate) fn build_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    query: FillReportQuery,
) -> (Vec<FillReport>, FillBuildFindings) {
    build_fill_reports_for_status(
        trades,
        ctx,
        instruments,
        query,
        TradeStatusSelection::Confirmed,
    )
}

pub(crate) fn build_pending_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    ts_init: UnixNanos,
) -> (Vec<FillReport>, FillBuildFindings) {
    build_fill_reports_for_status(
        trades,
        ctx,
        instruments,
        FillReportQuery {
            instrument_filter: None,
            venue_order_filter: None,
            start: None,
            end: None,
            ts_init,
        },
        TradeStatusSelection::PendingSettlement,
    )
}

#[derive(Clone, Copy)]
enum TradeStatusSelection {
    Confirmed,
    PendingSettlement,
}

impl TradeStatusSelection {
    fn includes(self, status: PolymarketTradeStatus) -> bool {
        match self {
            Self::Confirmed => status == PolymarketTradeStatus::Confirmed,
            Self::PendingSettlement => status.is_pending_settlement(),
        }
    }
}

fn build_fill_reports_for_status(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    query: FillReportQuery,
    status_selection: TradeStatusSelection,
) -> (Vec<FillReport>, FillBuildFindings) {
    let FillReportQuery {
        instrument_filter,
        venue_order_filter,
        start,
        end,
        ts_init,
    } = query;
    let mut reports = Vec::new();
    let mut findings = FillBuildFindings::default();
    let requested_asset_ids = instrument_filter.map(|filter_id| {
        instruments
            .load()
            .iter()
            .filter_map(|(asset_id, instrument)| {
                (instrument.id() == filter_id).then_some(*asset_id)
            })
            .collect::<Vec<_>>()
    });

    for trade in trades {
        if !status_selection.includes(trade.status) {
            continue;
        }

        if venue_order_filter
            .is_some_and(|venue_order_id| !trade_mentions_venue_order(trade, venue_order_id))
        {
            continue;
        }

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;
        if venue_order_filter.is_none()
            && let Some(asset_ids) = &requested_asset_ids
        {
            let mentions_requested_instrument = if is_maker {
                trade
                    .maker_orders
                    .iter()
                    .any(|order| asset_ids.contains(&order.asset_id))
            } else {
                asset_ids.contains(&trade.asset_id)
            };
            if !mentions_requested_instrument {
                continue;
            }
        }

        let parsed_timestamp = parse_timestamp(&trade.match_time);
        if parsed_timestamp.is_some_and(|timestamp| {
            start.is_some_and(|cutoff| timestamp < cutoff)
                || end.is_some_and(|cutoff| timestamp > cutoff)
        }) {
            findings.outside_lookback += 1;
            continue;
        }
        let age_unknown = parsed_timestamp.is_none() && (start.is_some() || end.is_some());
        let reports_before = reports.len();

        if is_maker {
            if !trade
                .maker_orders
                .iter()
                .any(|order| order.is_owned_by(ctx.user_address, ctx.api_key))
            {
                findings.unowned_maker_trades += 1;
                continue;
            }
            for mo in &trade.maker_orders {
                if !mo.is_owned_by(ctx.user_address, ctx.api_key) {
                    continue;
                }
                if venue_order_filter
                    .is_some_and(|venue_order_id| mo.order_id != venue_order_id.as_str())
                    || (venue_order_filter.is_none()
                        && requested_asset_ids
                            .as_ref()
                            .is_some_and(|asset_ids| !asset_ids.contains(&mo.asset_id)))
                {
                    continue;
                }
                let token_id = Ustr::from(mo.asset_id.as_str());
                let instrument = instruments.get_cloned(&token_id);
                let (instrument_id, price_prec, size_prec) = match instrument {
                    Some(i) => (i.id(), i.price_precision(), i.size_precision()),
                    None => {
                        findings.unmapped_instruments += 1;
                        continue;
                    }
                };
                if instrument_filter.is_some_and(|filter_id| instrument_id != filter_id) {
                    continue;
                }

                let ts_event = parsed_timestamp.unwrap_or_else(|| ctx.clock.get_time_ns());
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
                );
                reports.push(report);
            }
        } else {
            if venue_order_filter
                .is_some_and(|venue_order_id| trade.taker_order_id != venue_order_id.as_str())
            {
                continue;
            }
            let token_id = Ustr::from(trade.asset_id.as_str());
            let instrument = instruments.get_cloned(&token_id);
            let (instrument_id, price_prec, size_prec, taker_fee_rate, fee_exponent) =
                match instrument {
                    Some(i) => (
                        i.id(),
                        i.price_precision(),
                        i.size_precision(),
                        instrument_taker_fee(&i),
                        instrument_fee_exponent(&i),
                    ),
                    None => {
                        findings.unmapped_instruments += 1;
                        continue;
                    }
                };
            if instrument_filter.is_some_and(|filter_id| instrument_id != filter_id) {
                continue;
            }

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
            );
            reports.push(report);
        }

        if age_unknown && reports.len() > reports_before {
            findings.unknown_age += 1;
        }
    }

    (reports, findings)
}

/// Converts open orders into order status reports.
pub(crate) fn build_order_reports_from_orders(
    orders: &[PolymarketOpenOrder],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> (Vec<OrderStatusReport>, usize) {
    let mut reports = Vec::new();
    let mut filtered = 0usize;

    for order in orders {
        let token_id = Ustr::from(order.asset_id.as_str());
        let instrument = instruments.get_cloned(&token_id);
        let (instrument_id, price_prec, size_prec) = match instrument {
            Some(i) => (i.id(), i.price_precision(), i.size_precision()),
            None => {
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
        );
        reports.push(report);
    }

    (reports, filtered)
}

/// Builds position status reports from Data API positions, filtering dust.
pub(crate) fn build_position_reports(
    positions: &[DataApiPosition],
    account_id: AccountId,
    ts: UnixNanos,
) -> Vec<PositionStatusReport> {
    positions
        .iter()
        .filter(|p| {
            if p.size > Decimal::ZERO && p.size < DUST_POSITION_THRESHOLD {
                log::debug!(
                    "Filtering dust position: {}-{}, size={}",
                    p.condition_id,
                    p.asset,
                    p.size
                );
            }
            p.size >= DUST_POSITION_THRESHOLD
        })
        .filter_map(|p| {
            let instrument_id =
                InstrumentId::from(format!("{}-{}.POLYMARKET", p.condition_id, p.asset).as_str());
            let quantity = match Quantity::from_decimal_dp(p.size, USDC_DECIMALS as u8) {
                Ok(quantity) => quantity,
                Err(e) => {
                    log::warn!(
                        "Skipping invalid Data API position {}-{} size {}: {e}",
                        p.condition_id,
                        p.asset,
                        p.size,
                    );
                    return None;
                }
            };
            Some(PositionStatusReport::new(
                account_id,
                instrument_id,
                PositionSideSpecified::Long,
                quantity,
                ts,
                ts,
                None,
                None,
                p.avg_price,
            ))
        })
        .collect()
}

/// Full reconciliation mass status generation.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn generate_mass_status(
    http_client: &PolymarketClobHttpClient,
    data_api_client: &PolymarketDataApiHttpClient,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    local_orders: &LocalOrderCoordinator,
    ctx: &FillContext<'_>,
    client_id: ClientId,
    venue: Venue,
    lookback_mins: Option<u64>,
) -> anyhow::Result<Option<ExecutionMassStatus>> {
    let ts_init = ctx.clock.get_time_ns();
    let lookback = lookback_mins.map(|mins| (mins, lookback_cutoff(ts_init, mins)));

    // Fetch orders
    let orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;

    let (order_reports, orders_filtered) =
        build_order_reports_from_orders(&orders, instruments, ctx.account_id, None, ts_init);
    let admitted_orders = local_orders.admit_reconciliation_order_reports(order_reports);
    let mut order_reports = admitted_orders.artifacts;

    // Fetch and parse fill reports
    let trades = http_client
        .get_trades(GetTradesParams::default())
        .await
        .context("failed to fetch trades for mass status")?;

    let (fill_reports, fill_findings) = build_fill_reports_from_trades(
        &trades,
        ctx,
        instruments,
        FillReportQuery {
            instrument_filter: None,
            venue_order_filter: None,
            start: lookback.map(|(_, cutoff)| cutoff),
            end: None,
            ts_init,
        },
    );
    fill_findings.report(
        log::Level::Warn,
        "Mass-status generation lost fill evidence",
    );
    let admitted_fills = local_orders.admit_reconciliation_fill_reports(fill_reports);
    let mut fill_reports = admitted_fills.artifacts;

    // Position reports from Data API
    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;

    let position_reports = build_position_reports(&positions, ctx.account_id, ts_init);

    // Identity may be claimed while the position request is in flight. Re-enter the same
    // point-of-use admission boundary before any venue-keyed joins or emission.
    let final_fills = local_orders.admit_reconciliation_fill_reports(fill_reports);
    fill_reports = final_fills.artifacts;
    let confirmed_filled = confirmed_filled_quantities(&fill_reports);
    let mut final_order_conflicts = 0usize;
    order_reports = order_reports
        .into_iter()
        .filter_map(|report| {
            let venue_filled = report.filled_qty;
            let report_key = (report.venue_order_id, report.instrument_id);
            match local_orders.admit_point_of_use_order_report(
                report,
                venue_filled,
                confirmed_filled.get(&report_key).copied(),
            ) {
                ArtifactAdmission::Owned { artifact, .. }
                | ArtifactAdmission::Untracked(artifact) => Some(artifact),
                ArtifactAdmission::Conflict(_) => {
                    final_order_conflicts += 1;
                    None
                }
            }
        })
        .collect();
    let identity_conflicts = admitted_orders.conflicts
        + admitted_fills.conflicts
        + final_order_conflicts
        + final_fills.conflicts;
    if identity_conflicts > 0 {
        log::warn!(
            "Rejected {identity_conflicts} reconciliation artifacts with conflicting local order identity"
        );
    }

    // Apply lookback filter
    if let Some((mins, cutoff)) = lookback {
        let orders_before = order_reports.len();
        order_reports.retain(|r| r.ts_last >= cutoff);
        let orders_removed = orders_before - order_reports.len();

        log::debug!(
            "Lookback filter ({}min): orders {}->{} (removed {}), fills {} (removed {})",
            mins,
            orders_before,
            order_reports.len(),
            orders_removed,
            fill_reports.len(),
            fill_findings.outside_lookback,
        );
    } else {
        log::debug!(
            "Generated mass status: {} orders ({} filtered), {} fills, {} positions",
            order_reports.len(),
            orders_filtered,
            fill_reports.len(),
            position_reports.len(),
        );
    }

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);

    mass_status.add_order_reports(order_reports);
    mass_status.add_position_reports(position_reports);
    mass_status.add_fill_reports(fill_reports);

    Ok(Some(mass_status))
}

fn lookback_cutoff(ts_init: UnixNanos, lookback_mins: u64) -> UnixNanos {
    let lookback_ns = lookback_mins
        .saturating_mul(60)
        .saturating_mul(1_000_000_000);
    UnixNanos::from(ts_init.as_u64().saturating_sub(lookback_ns))
}

pub(crate) fn confirmed_filled_quantities(
    fill_reports: &[FillReport],
) -> AHashMap<(VenueOrderId, InstrumentId), Decimal> {
    let mut confirmed_by_order = AHashMap::new();
    for fill in fill_reports {
        *confirmed_by_order
            .entry((fill.venue_order_id, fill.instrument_id))
            .or_default() += fill.last_qty.as_decimal();
    }

    confirmed_by_order
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;

    use super::lookback_cutoff;

    #[test]
    fn lookback_cutoff_saturates_for_unbounded_minutes() {
        assert_eq!(
            lookback_cutoff(UnixNanos::from(1_000_000_000u64), u64::MAX),
            UnixNanos::from(0u64),
        );
    }
}
