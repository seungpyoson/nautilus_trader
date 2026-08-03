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

/// Losses encountered while converting authenticated venue trades into fills.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FillBuildFindings {
    /// Entries whose instrument was not loaded, so no report could be built.
    pub unmapped_instruments: usize,
    /// Confirmed maker trades holding none of this account's maker orders.
    pub unowned_maker_trades: usize,
}

impl FillBuildFindings {
    pub(crate) fn is_empty(self) -> bool {
        self.unmapped_instruments == 0 && self.unowned_maker_trades == 0
    }

    pub(crate) fn report(self, level: log::Level, context: &str) {
        if self.is_empty() {
            return;
        }
        log::log!(
            level,
            "{context}: {} entr(ies) had no loaded instrument; {} confirmed maker trade(s) held no maker order owned by this account",
            self.unmapped_instruments,
            self.unowned_maker_trades,
        );
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
) -> (Vec<FillReport>, FillBuildFindings) {
    build_fill_reports_for_status(
        trades,
        ctx,
        instruments,
        instrument_filter,
        ts_init,
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
        None,
        ts_init,
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
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
    status_selection: TradeStatusSelection,
) -> (Vec<FillReport>, FillBuildFindings) {
    let mut reports = Vec::new();
    let mut findings = FillBuildFindings::default();

    for trade in trades {
        if !status_selection.includes(trade.status) {
            continue;
        }

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

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
                let token_id = Ustr::from(mo.asset_id.as_str());
                let instrument = instruments.get_cloned(&token_id);
                let (instrument_id, price_prec, size_prec) = match instrument {
                    Some(i) => (i.id(), i.price_precision(), i.size_precision()),
                    None => {
                        findings.unmapped_instruments += 1;
                        continue;
                    }
                };

                if let Some(filter_id) = instrument_filter
                    && instrument_id != filter_id
                {
                    continue;
                }

                let ts_event =
                    parse_timestamp(&trade.match_time).unwrap_or(ctx.clock.get_time_ns());
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

            if let Some(filter_id) = instrument_filter
                && instrument_id != filter_id
            {
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

    let (fill_reports, fill_findings) =
        build_fill_reports_from_trades(&trades, ctx, instruments, None, ts_init);
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
    if let Some(mins) = lookback_mins {
        let now_ns = ctx.clock.get_time_ns();
        let cutoff_ns = now_ns.as_u64().saturating_sub(mins * 60 * 1_000_000_000);
        let cutoff = UnixNanos::from(cutoff_ns);

        let orders_before = order_reports.len();
        order_reports.retain(|r| r.ts_last >= cutoff);
        let orders_removed = orders_before - order_reports.len();

        let fills_before = fill_reports.len();
        fill_reports.retain(|r| r.ts_event >= cutoff);
        let fills_removed = fills_before - fill_reports.len();

        log::debug!(
            "Lookback filter ({}min): orders {}->{} (removed {}), fills {}->{} (removed {})",
            mins,
            orders_before,
            order_reports.len(),
            orders_removed,
            fills_before,
            fill_reports.len(),
            fills_removed,
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
