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
        build_maker_fill_report_checked, instrument_fee_exponent, instrument_taker_fee,
        parse_fill_report_checked, parse_order_status_report_checked, parse_timestamp,
    },
    report_build::{
        ReportBatch, ReportField, ReportOmission, ReportRow, ReportValueError, binary_price,
        omission_summary, positive_quantity, unix_seconds,
    },
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

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
pub(crate) fn build_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> anyhow::Result<ReportBatch<FillReport>> {
    let mut batch = ReportBatch::default();

    for trade in trades {
        if trade.status != PolymarketTradeStatus::Confirmed {
            continue;
        }

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

        if is_maker {
            if !trade
                .maker_orders
                .iter()
                .any(|mo| mo.is_owned_by(ctx.user_address, ctx.api_key))
            {
                batch.push_omission(ReportOmission::UnownedMakerTrade);
                continue;
            }

            for mo in &trade.maker_orders {
                if !mo.is_owned_by(ctx.user_address, ctx.api_key) {
                    continue;
                }
                let token_id = mo.asset_id;
                let instrument = instruments.get_cloned(&token_id);
                let (instrument_id, price_prec, size_prec) = match instrument {
                    Some(i) => (i.id(), i.price_precision(), i.size_precision()),
                    None => {
                        batch.push_omission(ReportOmission::UnmappedInstrument {
                            row: ReportRow::Fill,
                        });
                        continue;
                    }
                };

                if let Some(filter_id) = instrument_filter
                    && instrument_id != filter_id
                {
                    continue;
                }

                let Some(ts_event) = parse_timestamp(&trade.match_time) else {
                    batch.push_omission(ReportOmission::InvalidValue {
                        row: ReportRow::Fill,
                        field: ReportField::Timestamp,
                        reason: ReportValueError::InvalidTimestamp,
                    });
                    continue;
                };
                let result = build_maker_fill_report_checked(
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
                .map_err(|e| {
                    e.with_context(format!(
                        "failed to build maker fill report for trade {} and order {}",
                        trade.id, mo.order_id,
                    ))
                });
                batch.push_result(result)?;
            }
        } else {
            let token_id = trade.asset_id;
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
                        batch.push_omission(ReportOmission::UnmappedInstrument {
                            row: ReportRow::Fill,
                        });
                        continue;
                    }
                };

            if let Some(filter_id) = instrument_filter
                && instrument_id != filter_id
            {
                continue;
            }

            let result = parse_fill_report_checked(
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
            .map_err(|e| {
                e.with_context(format!(
                    "failed to build taker fill report for trade {}",
                    trade.id,
                ))
            });
            batch.push_result(result)?;
        }
    }

    Ok(batch)
}

/// Converts open orders into order status reports.
pub(crate) fn build_order_reports_from_orders(
    orders: &[PolymarketOpenOrder],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> anyhow::Result<ReportBatch<OrderStatusReport>> {
    let mut batch = ReportBatch::default();

    for order in orders {
        let token_id = order.asset_id;
        let instrument = instruments.get_cloned(&token_id);
        let (instrument_id, price_prec, size_prec) = match instrument {
            Some(i) => (i.id(), i.price_precision(), i.size_precision()),
            None => {
                batch.push_omission(ReportOmission::UnmappedInstrument {
                    row: ReportRow::Order,
                });
                continue;
            }
        };

        if let Some(filter_id) = instrument_filter
            && instrument_id != filter_id
        {
            continue;
        }

        batch.push_result(parse_order_status_report_checked(
            order,
            instrument_id,
            account_id,
            None,
            price_prec,
            size_prec,
            ts_init,
        ))?;
    }

    Ok(batch)
}

/// Applies venue_order_id and time-range filters to fill reports.
pub(crate) fn apply_fill_filters(
    mut reports: Vec<FillReport>,
    venue_order_id: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> Vec<FillReport> {
    if let Some(vid) = venue_order_id {
        reports.retain(|r| r.venue_order_id == vid);
    }

    match (start, end) {
        (Some(s), Some(e)) => reports.retain(|r| r.ts_event >= s && r.ts_event <= e),
        (Some(s), None) => reports.retain(|r| r.ts_event >= s),
        (None, Some(e)) => reports.retain(|r| r.ts_event <= e),
        (None, None) => {}
    }

    reports
}

/// Builds position status reports from Data API positions.
pub(crate) fn build_position_reports(
    positions: &[DataApiPosition],
    account_id: AccountId,
    ts: UnixNanos,
) -> ReportBatch<PositionStatusReport> {
    let mut batch = ReportBatch::default();
    for position in positions {
        batch.push_omittable(build_position_report_checked(position, account_id, ts));
    }
    batch
}

fn build_position_report_checked(
    position: &DataApiPosition,
    account_id: AccountId,
    ts: UnixNanos,
) -> Result<PositionStatusReport, ReportOmission> {
    let raw_instrument_id = format!("{}-{}.POLYMARKET", position.condition_id, position.asset);
    let instrument_id = InstrumentId::from_as_ref(&raw_instrument_id).map_err(|_| {
        ReportOmission::InvalidValue {
            row: ReportRow::Position,
            field: ReportField::InstrumentIdentity,
            reason: ReportValueError::InvalidIdentifier,
        }
    })?;
    let (position_side, quantity) = match position.size.cmp(&Decimal::ZERO) {
        std::cmp::Ordering::Less => {
            return Err(ReportOmission::InvalidValue {
                row: ReportRow::Position,
                field: ReportField::PositionQuantity,
                reason: ReportValueError::Negative,
            });
        }
        std::cmp::Ordering::Equal => (
            PositionSideSpecified::Flat,
            Quantity::zero(USDC_DECIMALS as u8),
        ),
        std::cmp::Ordering::Greater if position.size < DUST_POSITION_THRESHOLD => {
            return Err(ReportOmission::InvalidValue {
                row: ReportRow::Position,
                field: ReportField::PositionQuantity,
                reason: ReportValueError::PositionDust,
            });
        }
        std::cmp::Ordering::Greater => (
            PositionSideSpecified::Long,
            positive_quantity(position.size, USDC_DECIMALS as u8).map_err(|reason| {
                ReportOmission::InvalidValue {
                    row: ReportRow::Position,
                    field: ReportField::PositionQuantity,
                    reason,
                }
            })?,
        ),
    };
    let avg_px_open = match (position_side, position.avg_price) {
        (PositionSideSpecified::Flat, _) | (_, None) => None,
        (_, Some(value)) => Some(
            binary_price(value, USDC_DECIMALS as u8)
                .map_err(|reason| ReportOmission::InvalidValue {
                    row: ReportRow::Position,
                    field: ReportField::AveragePrice,
                    reason,
                })?
                .as_decimal(),
        ),
    };

    Ok(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side,
        quantity,
        ts,
        ts,
        None,
        None,
        avg_px_open,
    ))
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
) -> anyhow::Result<Option<ExecutionMassStatus>> {
    let ts_init = ctx.clock.get_time_ns();
    let cutoff = mass_status_cutoff(ts_init, lookback_mins);

    // Fetch orders
    let mut orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;
    let orders_removed = cutoff.map_or(0, |cutoff| filter_orders_to_lookback(&mut orders, cutoff));

    let order_batch =
        build_order_reports_from_orders(&orders, instruments, ctx.account_id, None, ts_init)?;
    let mut order_reports = order_batch.reports;
    let mut omissions = order_batch.omissions;
    omissions.extend(duplicate_identity_omissions(
        order_reports.iter().map(|report| report.venue_order_id),
        ReportRow::Order,
    ));

    // Fetch and parse fill reports
    let mut trades = http_client
        .get_trades(GetTradesParams::default())
        .await
        .context("failed to fetch trades for mass status")?;
    let trades_removed = cutoff.map_or(0, |cutoff| filter_trades_to_lookback(&mut trades, cutoff));

    let fill_batch = build_fill_reports_from_trades(&trades, ctx, instruments, None, ts_init)?;
    let mut fill_reports = fill_batch.reports;
    omissions.extend(fill_batch.omissions);
    omissions.extend(duplicate_identity_omissions(
        fill_reports
            .iter()
            .map(|report| (report.venue_order_id, report.trade_id)),
        ReportRow::Fill,
    ));

    // Snap dust drift on REST fills the same way the WS path does.
    // Commission stays as venue-reported.
    fill_tracker.snap_fill_reports(&mut fill_reports);

    // Position reports from Data API
    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;

    let position_batch = build_position_reports(&positions, ctx.account_id, ts_init);
    let position_reports = position_batch.reports;
    omissions.extend(position_batch.omissions);
    omissions.extend(duplicate_identity_omissions(
        position_reports.iter().map(|report| report.instrument_id),
        ReportRow::Position,
    ));

    anyhow::ensure!(
        omissions.is_empty(),
        "Polymarket mass status is incomplete with {} omitted row(s): {}",
        omissions.len(),
        omission_summary(&omissions),
    );
    log::debug!(
        "Generated mass status: {} orders, {} fills, {} positions; lookback removed {} order(s) \
         and {} trade(s)",
        order_reports.len(),
        fill_reports.len(),
        position_reports.len(),
        orders_removed,
        trades_removed,
    );

    cap_order_reports_to_confirmed_fills(&mut order_reports, &fill_reports);

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);

    mass_status.add_order_reports(order_reports);
    mass_status.add_position_reports(position_reports);
    mass_status.add_fill_reports(fill_reports);
    mass_status.set_report_window(cutoff, true);

    Ok(Some(mass_status))
}

fn duplicate_identity_omissions<K>(
    identities: impl IntoIterator<Item = K>,
    row: ReportRow,
) -> Vec<ReportOmission>
where
    K: Eq + std::hash::Hash,
{
    let mut seen = AHashSet::new();
    identities
        .into_iter()
        .filter_map(|identity| {
            (!seen.insert(identity)).then_some(ReportOmission::DuplicateIdentity { row })
        })
        .collect()
}

fn mass_status_cutoff(ts_init: UnixNanos, lookback_mins: Option<u64>) -> Option<UnixNanos> {
    lookback_mins.map(|minutes| {
        let duration_ns = minutes
            .saturating_mul(60)
            .saturating_mul(NANOSECONDS_IN_SECOND);
        UnixNanos::from(ts_init.as_u64().saturating_sub(duration_ns))
    })
}

fn filter_orders_to_lookback(orders: &mut Vec<PolymarketOpenOrder>, cutoff: UnixNanos) -> usize {
    let before = orders.len();
    orders.retain(|order| match unix_seconds(order.created_at) {
        Ok(timestamp) => timestamp >= cutoff,
        Err(_) => true,
    });
    before - orders.len()
}

fn filter_trades_to_lookback(trades: &mut Vec<PolymarketTradeReport>, cutoff: UnixNanos) -> usize {
    let before = trades.len();
    trades.retain(|trade| match parse_timestamp(&trade.match_time) {
        Some(timestamp) => timestamp >= cutoff,
        None => true,
    });
    before - trades.len()
}

fn cap_order_reports_to_confirmed_fills(
    order_reports: &mut [OrderStatusReport],
    fill_reports: &[FillReport],
) {
    let confirmed_by_order = confirmed_filled_quantities(fill_reports);

    for report in order_reports {
        let local_filled = Quantity::zero(report.quantity.precision);
        cap_order_report_filled_qty(
            report,
            local_filled,
            confirmed_by_order.get(&report.venue_order_id).copied(),
        );
    }
}

pub(crate) fn confirmed_filled_quantities(
    fill_reports: &[FillReport],
) -> AHashMap<VenueOrderId, Decimal> {
    let mut confirmed_by_order = AHashMap::new();
    for fill in fill_reports {
        *confirmed_by_order.entry(fill.venue_order_id).or_default() += fill.last_qty.as_decimal();
    }

    confirmed_by_order
}

pub(crate) fn cap_order_report_filled_qty(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Option<Decimal>,
) {
    let confirmed_filled = confirmed_filled
        .and_then(|qty| Quantity::from_decimal_dp(qty, report.quantity.precision).ok())
        .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
    let capped = report.filled_qty.min(local_filled.max(confirmed_filled));
    report.filled_qty = capped;
    normalize_terminal_order_report_quantity(report);
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

    use super::*;

    #[rstest]
    fn mass_status_cutoff_saturates_without_overflow() {
        assert_eq!(
            mass_status_cutoff(UnixNanos::from(1_000_000_000), Some(u64::MAX)),
            Some(UnixNanos::from(0)),
        );
        assert_eq!(mass_status_cutoff(UnixNanos::from(1), None), None);
    }

    #[rstest]
    fn lookback_excludes_known_old_rows_but_retains_unclassifiable_rows() {
        let cutoff = UnixNanos::from(2_000_000_000_u64 * NANOSECONDS_IN_SECOND);
        let old_order: PolymarketOpenOrder =
            serde_json::from_str(include_str!("../../test_data/http_open_order.json"))
                .expect("order fixture must deserialize");
        let mut invalid_order = old_order.clone();
        invalid_order.created_at = u64::MAX;
        let mut orders = vec![old_order, invalid_order];

        let old_trade: PolymarketTradeReport =
            serde_json::from_str(include_str!("../../test_data/http_trade_report.json"))
                .expect("trade fixture must deserialize");
        let mut invalid_trade = old_trade.clone();
        invalid_trade.match_time = "not-a-timestamp".to_string();
        let mut trades = vec![old_trade, invalid_trade];

        assert_eq!(filter_orders_to_lookback(&mut orders, cutoff), 1);
        assert_eq!(filter_trades_to_lookback(&mut trades, cutoff), 1);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].created_at, u64::MAX);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].match_time, "not-a-timestamp");
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

        cap_order_reports_to_confirmed_fills(&mut reports, &fills);

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
    }

    #[rstest]
    #[case::below_threshold("99.995", "99.995")]
    #[case::at_threshold("99.990", "100.000")]
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

        cap_order_reports_to_confirmed_fills(&mut reports, &fills);

        assert_eq!(reports[0].quantity, Quantity::from(expected_quantity));
        assert_eq!(reports[0].filled_qty, Quantity::from(confirmed));
    }
}
