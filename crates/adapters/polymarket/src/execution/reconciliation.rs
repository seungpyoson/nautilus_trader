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
        build_maker_fill_report, instrument_taker_fee, parse_fill_report,
        parse_order_status_report, parse_timestamp,
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

/// What [`build_fill_reports_from_trades`] could not turn into a fill report.
///
/// The builder returns partial results with these counts so each caller can
/// choose the appropriate severity. `ExecutionMassStatus` has no field for the
/// counts, so callers currently surface them through logs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FillBuildDiscards {
    /// Entries whose instrument was not loaded, so no report could be built.
    pub unmapped_instruments: usize,
    /// Confirmed maker trades holding none of the account's own maker orders.
    pub unowned_maker_trades: usize,
    /// Trades outside the caller's reconciliation window.
    pub outside_lookback: usize,
    /// Trades kept despite an unparsable match time, so their age is unknown.
    ///
    /// Counted rather than left implicit: the fill built from one is stamped with
    /// the current time, which is what makes keeping it the safe choice, but it
    /// also means a trade of any age enters the window looking current. A window
    /// that silently admits trades of unknown age is not the window the caller
    /// asked for, so how many did is reported.
    pub unknown_age: usize,
}

impl FillBuildDiscards {
    /// Say what was lost, at the level the caller owns, or say nothing.
    ///
    /// The caller passes the level because reconciliation and polling paths have
    /// different logging requirements.
    pub(crate) fn report(&self, level: log::Level, context: &str) {
        if !self.lost_anything() {
            return;
        }
        log::log!(level, "{context}: {}", self.findings().join("; "));
    }

    /// Whether the reports built from this query are weaker than they read.
    ///
    /// Derived from the findings rather than stated again beside them, because
    /// stating it twice is how the predicate and the message came to disagree:
    /// the message named `unknown_age` on one path while the predicate did not
    /// count it on any.
    ///
    /// Deliberately not every field: `outside_lookback` produces no finding
    /// because it is the window doing its job, not a weakness.
    pub(crate) fn lost_anything(&self) -> bool {
        !self.findings().is_empty()
    }

    fn findings(&self) -> Vec<String> {
        let mut findings = Vec::new();
        if self.unowned_maker_trades > 0 {
            findings.push(format!(
                "{} confirmed maker trade(s) held no maker order owned by this account",
                self.unowned_maker_trades,
            ));
        }

        if self.unmapped_instruments > 0 {
            findings.push(format!(
                "{} entr(ies) had no loaded instrument, so their filled quantity is understated",
                self.unmapped_instruments,
            ));
        }

        if self.unknown_age > 0 {
            findings.push(format!(
                "{} trade(s) had an unparsable match time and were kept, so the window admitted \
                 trades of unknown age",
                self.unknown_age,
            ));
        }
        findings
    }
}

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
///
/// `start` and `end` apply to the venue's `match_time`. A confirmed trade with
/// an unparsable match time in either bounded query is kept and counted as
/// [`FillBuildDiscards::unknown_age`].
fn trade_mentions_venue_order(
    trade: &PolymarketTradeReport,
    venue_order_id: &VenueOrderId,
) -> bool {
    trade.taker_order_id == venue_order_id.as_str()
        || trade
            .maker_orders
            .iter()
            .any(|order| order.order_id == venue_order_id.as_str())
}

pub(crate) fn build_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    venue_order_filter: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    ts_init: UnixNanos,
) -> (Vec<FillReport>, FillBuildDiscards) {
    let mut reports = Vec::new();
    let mut discards = FillBuildDiscards::default();

    for trade in trades {
        let mut age_unknown = false;

        if trade.status != PolymarketTradeStatus::Confirmed {
            continue;
        }

        // Order-scoped queries own diagnostics only for trades that mention
        // that order. Keep the report-level filter below as well because one
        // maker trade can contain the target plus other owned maker orders.
        if venue_order_filter
            .as_ref()
            .is_some_and(|id| !trade_mentions_venue_order(trade, id))
        {
            continue;
        }

        match parse_timestamp(&trade.match_time) {
            Some(ts)
                if start.is_some_and(|cutoff| ts < cutoff)
                    || end.is_some_and(|cutoff| ts > cutoff) =>
            {
                discards.outside_lookback += 1;
                continue;
            }
            Some(_) => {}
            None if start.is_some() || end.is_some() => age_unknown = true,
            None => {}
        }

        let reports_before = reports.len();
        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

        if is_maker {
            // `GET /trades` returns the trades of the authenticated account and reports
            // `trader_side` from its perspective, so a confirmed trade the account filled
            // as maker holds at least one of its own maker orders. A trade holding none
            // cannot be interpreted, and reporting no fill for it would silently
            // understate the filled quantity of a live order.
            //
            // Ownership is judged across the whole match, before any instrument filter,
            // because the unified book matches complementary tokens across assets: the
            // account's own order can sit on the token an instrument-scoped request did
            // not ask about while a counterparty's sits on the token it did.
            if !trade
                .maker_orders
                .iter()
                .any(|mo| mo.is_owned_by(ctx.user_address, ctx.api_key))
            {
                discards.unowned_maker_trades += 1;
                log::debug!(
                    "Polymarket confirmed maker trade {} holds no maker order owned by the \
                     configured account {}, so no fill is reported for it and any quantity \
                     it filled is understated",
                    trade.id,
                    ctx.user_address
                );
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
                        discards.unmapped_instruments += 1;
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

                if venue_order_filter
                    .as_ref()
                    .is_none_or(|id| report.venue_order_id == *id)
                {
                    reports.push(report);
                }
            }
        } else {
            let token_id = Ustr::from(trade.asset_id.as_str());
            let instrument = instruments.get_cloned(&token_id);
            let (instrument_id, price_prec, size_prec, taker_fee_rate) = match instrument {
                Some(i) => (
                    i.id(),
                    i.price_precision(),
                    i.size_precision(),
                    instrument_taker_fee(&i),
                ),
                None => {
                    discards.unmapped_instruments += 1;
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
                ts_init,
            );

            if venue_order_filter
                .as_ref()
                .is_none_or(|id| report.venue_order_id == *id)
            {
                reports.push(report);
            }
        }

        if age_unknown && reports.len() > reports_before {
            discards.unknown_age += 1;
        }
    }

    (reports, discards)
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
    fill_tracker: &OrderFillTrackerMap,
    ctx: &FillContext<'_>,
    client_id: ClientId,
    venue: Venue,
    lookback_mins: Option<u64>,
) -> anyhow::Result<Option<ExecutionMassStatus>> {
    let ts_init = ctx.clock.get_time_ns();

    // One cutoff, read from the clock once, and applied to the trades rather
    // than only to the reports built from them. `get_trades` follows the cursor
    // to the end, so it returns the account's whole trade history; without this
    // all of it is interpreted on every reconciliation pass just to discard most
    // of it afterwards. Bounding the input is what makes the configured lookback
    // mean something to the work done rather than only to the output kept.
    // Saturating throughout, including the conversion: `mins * 60 * 1_000_000_000`
    // overflows a `u64` above roughly 584 years of lookback, which panics in
    // debug and wraps in release -- turning an absurd-but-valid configuration
    // into a cutoff in the future, which discards every trade. Saturating gives
    // it the meaning the value asks for, an unbounded window.
    let cutoff = reconciliation_cutoff(ts_init, lookback_mins);

    // Fetch orders
    let orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;

    let (mut order_reports, orders_filtered) =
        build_order_reports_from_orders(&orders, instruments, ctx.account_id, None, ts_init);

    // Fetch and parse fill reports
    let all_trades = http_client
        .get_trades(GetTradesParams::default())
        .await
        .context("failed to fetch trades for mass status")?;

    let trades_before = all_trades.len();

    let (mut fill_reports, fills_filtered) = build_fill_reports_from_trades(
        &all_trades,
        ctx,
        instruments,
        None,
        None,
        cutoff,
        None,
        ts_init,
    );
    let trades_after = trades_before - fills_filtered.outside_lookback;

    // Snap dust drift on REST fills the same way the WS path does.
    // Commission stays as venue-reported.
    fill_tracker.snap_fill_reports(&mut fill_reports);

    // Position reports from Data API
    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;

    let position_reports = build_position_reports(&positions, ctx.account_id, ts_init);

    // Apply lookback filter. Fills are already inside the window: their trades
    // were filtered above, by the same cutoff, which is why there is no second
    // retain here to keep in step with the first.
    if let (Some(mins), Some(cutoff)) = (lookback_mins, cutoff) {
        let orders_before = order_reports.len();
        order_reports.retain(|r| r.ts_last >= cutoff);
        let orders_removed = orders_before - order_reports.len();

        log::debug!(
            "Lookback filter ({}min): orders {}->{} (removed {}), trades {}->{} (removed {})",
            mins,
            orders_before,
            order_reports.len(),
            orders_removed,
            trades_before,
            trades_after,
            fills_filtered.outside_lookback,
        );
    } else {
        log::debug!(
            "Generated mass status: {} orders ({} filtered), {} fills ({} filtered), {} positions",
            order_reports.len(),
            orders_filtered,
            fill_reports.len(),
            fills_filtered.unmapped_instruments,
            position_reports.len(),
        );
    }

    // The severity this pass owns, stated once for the whole pass rather than
    // once per trade. This is the caller that can do the least about it -- the
    // mass status it returns has no field to carry the count -- so an operator
    // reading the log is the only channel, and it says what was lost and that
    // the report does not show it.
    fills_filtered.report(
        log::Level::Error,
        &format!(
            "Polymarket mass status for account {} is incomplete, and this report carries no \
             count of it",
            ctx.user_address
        ),
    );

    cap_order_reports_to_confirmed_fills(&mut order_reports, &fill_reports, fill_tracker);

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);

    mass_status.add_order_reports(order_reports);
    mass_status.add_position_reports(position_reports);
    mass_status.add_fill_reports(fill_reports);

    Ok(Some(mass_status))
}

/// Caps reported filled quantities, using locally tracked fills as the floor.
///
/// The floor matters because only `Confirmed` trades become fill reports. An
/// order whose fills are still settling has no confirmed quantity here, so a
/// zero floor would cap it to zero and report an order the venue calls filled
/// as having filled nothing, understating filled quantity and overstating what
/// remains. `generate_order_status_report` already floors at what this client
/// saw fill; this path must agree with it, or the same order is described
/// differently depending on which one produced the report.
fn cap_order_reports_to_confirmed_fills(
    order_reports: &mut [OrderStatusReport],
    fill_reports: &[FillReport],
    fill_tracker: &OrderFillTrackerMap,
) {
    let confirmed_by_order = confirmed_filled_quantities(fill_reports);

    for report in order_reports {
        let local_filled = fill_tracker
            .get_cumulative_filled(&report.venue_order_id)
            .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
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

fn reconciliation_cutoff(ts_init: UnixNanos, lookback_mins: Option<u64>) -> Option<UnixNanos> {
    lookback_mins.map(|mins| {
        let span = mins.saturating_mul(60).saturating_mul(1_000_000_000);
        UnixNanos::from(ts_init.as_u64().saturating_sub(span))
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::datetime::NANOSECONDS_IN_SECOND;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
        identifiers::TradeId,
        instruments::stubs::binary_option,
        types::{Money, Price},
    };
    use rstest::rstest;

    use super::*;

    /// Maker address of the configured account, as the trade fixture reports it.
    const USER_ADDRESS: &str = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8";
    const USER_API_KEY: &str = "00000000-0000-0000-0000-000000000001";
    const COUNTERPARTY_ADDRESS: &str = "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc";
    const COUNTERPARTY_API_KEY: &str = "00000000-0000-0000-0000-000000000003";
    const COMPLEMENTARY_TOKEN: &str = "COMPLEMENTARY-TOKEN";

    fn mapped_instrument() -> (AtomicMap<Ustr, InstrumentAny>, InstrumentAny) {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let instruments = AtomicMap::new();
        instruments.insert(
            Ustr::from(instrument.raw_symbol().as_str()),
            instrument.clone(),
        );
        (instruments, instrument)
    }

    /// A confirmed trade the configured account filled as a maker. The fixture's first
    /// maker order is the account's own; the second belongs to a counterparty.
    fn confirmed_maker_trade_for(instrument: &InstrumentAny) -> PolymarketTradeReport {
        let content = std::fs::read_to_string("test_data/http_trade_report.json")
            .expect("trade fixture should load");
        let mut trade: PolymarketTradeReport =
            serde_json::from_str(&content).expect("trade fixture should decode");
        trade.status = PolymarketTradeStatus::Confirmed;
        trade.trader_side = PolymarketLiquiditySide::Maker;
        trade.asset_id = Ustr::from(instrument.raw_symbol().as_str());
        for maker_order in &mut trade.maker_orders {
            maker_order.asset_id = Ustr::from(instrument.raw_symbol().as_str());
        }
        trade
    }

    fn disown_maker_orders(trade: &mut PolymarketTradeReport) {
        for maker_order in &mut trade.maker_orders {
            maker_order.maker_address = COUNTERPARTY_ADDRESS.to_string();
            maker_order.owner = COUNTERPARTY_API_KEY.to_string();
        }
    }

    fn fill_context() -> FillContext<'static> {
        FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: USER_ADDRESS,
            api_key: USER_API_KEY,
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        }
    }

    /// The account filled this trade as a maker, so the response must carry one of its
    /// maker orders. Answering with no fill would understate a live order's filled
    /// quantity rather than describe an account with no fill. The pass does not fail
    /// over it -- see [`FillBuildDiscards`] -- so what is pinned here is that the trade
    /// is *counted* as undescribed rather than passing as an ordinary empty result.
    #[rstest]
    fn counts_a_confirmed_maker_trade_with_no_owned_maker_order() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty(), "{reports:?}");
        assert_eq!(discards.unowned_maker_trades, 1);
        assert_eq!(discards.unmapped_instruments, 0);
        assert!(
            discards.lost_anything(),
            "an undescribable trade is a loss the caller must be able to report"
        );
    }

    /// A confirmed maker trade carrying no maker orders at all is undescribable for the
    /// same reason as one carrying only other accounts': the account filled it as a maker,
    /// so an order of its own is missing. The empty case reaches the rule down a different
    /// structural path than the disowned one, so it is pinned separately.
    #[rstest]
    fn counts_a_confirmed_maker_trade_with_no_maker_orders_at_all() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.maker_orders.clear();

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty(), "{reports:?}");
        assert_eq!(discards.unowned_maker_trades, 1);
    }

    #[rstest]
    fn order_filter_ignores_unowned_maker_trades_for_other_orders() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            Some(VenueOrderId::from("TARGET-NOT-IN-TRADE")),
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty());
        assert_eq!(
            discards.unowned_maker_trades, 0,
            "an unrelated trade must not become a diagnostic for an order-scoped query"
        );
    }

    #[rstest]
    fn order_filter_ignores_unmapped_instruments_for_other_orders() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.trader_side = PolymarketLiquiditySide::Taker;
        trade.asset_id = Ustr::from("UNMAPPED-TOKEN");

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            Some(VenueOrderId::from("TARGET-NOT-IN-TRADE")),
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty());
        assert_eq!(
            discards.unmapped_instruments, 0,
            "an unrelated trade must not become a diagnostic for an order-scoped query"
        );
    }

    /// The configured account, written the way a block explorer displays it. `user_address`
    /// is the operator's funder taken verbatim where one is set, and the venue's payload
    /// carries whatever case the venue chose, so the two sides of the ownership test can
    /// disagree on case while naming one account. Comparing them exactly turns that into a
    /// trade belonging to somebody else -- silently before the rule above existed, and
    /// loudly enough to stop the node after it.
    #[rstest]
    fn owns_a_maker_order_whose_address_differs_only_in_case() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        // The API key must not be able to carry this on its own, or the case of the
        // address would never be consulted and the test would pass without it.
        for maker_order in &mut trade.maker_orders {
            maker_order.owner = COUNTERPARTY_API_KEY.to_string();
        }
        let checksummed = format!("0x{}", USER_ADDRESS[2..].to_uppercase());
        assert_ne!(checksummed, USER_ADDRESS, "the fixture must vary the case");
        let context = FillContext {
            user_address: &checksummed,
            ..fill_context()
        };

        let (reports, _) = build_fill_reports_from_trades(
            &[trade],
            &context,
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(
            reports.len(),
            1,
            "the account's own maker order must still be reported"
        );
    }

    /// The window bounds the guard above, which is what keeps a single uninterpretable
    /// trade from being able to stop the node forever. Without this the whole trade
    /// history of the account is judged on every pass, so a trade from any point in the
    /// account's past fails reconciliation with nothing an operator can configure to get
    /// past it.
    /// Driven through the builder rather than a filter helper on purpose: a helper
    /// tested on its own passes whether or not anything calls it, and the defect this
    /// pins was exactly that -- a window that was computed and then not applied to what
    /// the builder read.
    #[rstest]
    fn excludes_a_trade_older_than_the_lookback_window() {
        let (instruments, instrument) = mapped_instrument();
        let cutoff = UnixNanos::from(2_000 * NANOSECONDS_IN_SECOND);
        let ts_init = UnixNanos::from(1_000_000_000u64);

        let mut old = confirmed_maker_trade_for(&instrument);
        old.match_time = "1000".to_string();
        let mut recent = confirmed_maker_trade_for(&instrument);
        recent.match_time = "3000".to_string();
        let mut undated = confirmed_maker_trade_for(&instrument);
        undated.match_time = "not a timestamp".to_string();

        let (windowed, discards) = build_fill_reports_from_trades(
            &[old, recent, undated],
            &fill_context(),
            &instruments,
            None,
            None,
            Some(cutoff),
            None,
            ts_init,
        );

        // The same two trades the window should have kept, read with no window, so the
        // comparison states what survived rather than a count copied from the fixture.
        let mut recent_again = confirmed_maker_trade_for(&instrument);
        recent_again.match_time = "3000".to_string();
        let mut undated_again = confirmed_maker_trade_for(&instrument);
        undated_again.match_time = "not a timestamp".to_string();
        let (expected, _) = build_fill_reports_from_trades(
            &[recent_again, undated_again],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            ts_init,
        );

        assert_eq!(discards.outside_lookback, 1);
        assert_eq!(
            windowed.len(),
            expected.len(),
            "the window drops the old trade and keeps the recent and undated ones"
        );
        assert_eq!(
            discards.unknown_age, 1,
            "the undated trade is kept, and the window says so rather than counting it as \
             evidence that it was recent"
        );
        // A windowed-out trade is the window working and is not reportable. An
        // undated trade weakens the window's guarantee and is reportable.
        assert!(
            discards.lost_anything(),
            "an undated trade weakens the window's guarantee, so it must be reportable"
        );
        let windowed_out_only = FillBuildDiscards {
            unknown_age: 0,
            ..discards
        };
        assert!(
            !windowed_out_only.lost_anything(),
            "a windowed-out trade on its own is the window working, not a loss"
        );
    }

    /// No lookback means no window, so unbounded reconciliation must remain unbounded.
    #[rstest]
    fn keeps_every_trade_when_no_lookback_is_configured() {
        let (instruments, instrument) = mapped_instrument();
        let mut old = confirmed_maker_trade_for(&instrument);
        old.match_time = "1".to_string();

        let (reports, discards) = build_fill_reports_from_trades(
            &[old],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(discards.outside_lookback, 0);
        assert!(
            !reports.is_empty(),
            "an unbounded window must still reconcile the oldest trade"
        );
    }

    #[rstest]
    fn maximum_lookback_saturates_to_an_unbounded_window() {
        let cutoff = reconciliation_cutoff(UnixNanos::from(u64::MAX), Some(u64::MAX));

        assert_eq!(cutoff, Some(UnixNanos::from(0)));
    }

    /// Only confirmed trades are reconciled, so one that has merely matched must not be
    /// judged for ownership at all. This pins the rule below the status filter: hoisting it
    /// above would fail reconciliation on every pending trade belonging to someone else.
    #[rstest]
    #[case::matched(PolymarketTradeStatus::Matched)]
    #[case::mined(PolymarketTradeStatus::Mined)]
    #[case::retrying(PolymarketTradeStatus::Retrying)]
    #[case::failed(PolymarketTradeStatus::Failed)]
    fn ignores_an_unconfirmed_maker_trade_with_no_owned_maker_order(
        #[case] status: PolymarketTradeStatus,
    ) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.status = status;
        disown_maker_orders(&mut trade);

        let (reports, _) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty());
    }

    /// A match carries every maker's order, so only the account's own becomes a report.
    #[rstest]
    #[case::owned_by_maker_address(USER_ADDRESS, COUNTERPARTY_API_KEY)]
    #[case::owned_by_api_key(COUNTERPARTY_ADDRESS, USER_API_KEY)]
    fn reports_only_the_owned_maker_order(#[case] maker_address: &str, #[case] api_key: &str) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = maker_address.to_string();
        trade.maker_orders[0].owner = api_key.to_string();
        let owned = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());

        let (reports, _) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned);
    }

    /// The unified book matches complementary tokens across assets, so the account's own
    /// maker order can sit on a token an instrument-scoped request did not ask about
    /// while a counterparty's sits on the token it did. That trade is interpretable and
    /// simply contributes no report, so it must not fail.
    #[rstest]
    fn accepts_a_cross_asset_match_with_no_report_for_the_requested_instrument() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        trade.maker_orders[0].asset_id = Ustr::from(COMPLEMENTARY_TOKEN);

        let (reports, _) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            Some(instrument.id()),
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(reports.is_empty());
    }

    /// A taker trade carries no maker order of the account, so the rule must not reach it.
    #[rstest]
    fn accepts_a_confirmed_taker_trade_without_owned_maker_orders() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.trader_side = PolymarketLiquiditySide::Taker;
        disown_maker_orders(&mut trade);

        let (reports, _) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(reports.len(), 1);
    }

    #[rstest]
    fn counts_unknown_age_for_a_taker_trade() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.trader_side = PolymarketLiquiditySide::Taker;
        trade.match_time = "not a timestamp".to_string();

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            Some(UnixNanos::from(1)),
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(discards.unknown_age, 1);
    }

    #[rstest]
    fn counts_unknown_age_for_an_end_only_window() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.trader_side = PolymarketLiquiditySide::Taker;
        trade.match_time = "not a timestamp".to_string();
        let ts_init = UnixNanos::from(1_000_000_000u64);

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            None,
            Some(UnixNanos::from(1_000_000_001u64)),
            ts_init,
        );

        assert_eq!(reports.len(), 1);
        assert_eq!(discards.unknown_age, 1);
    }

    #[rstest]
    fn counts_unknown_age_once_for_a_maker_trade_with_multiple_reports() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.match_time = "not a timestamp".to_string();
        for maker_order in &mut trade.maker_orders {
            maker_order.maker_address = USER_ADDRESS.to_string();
            maker_order.owner = COUNTERPARTY_API_KEY.to_string();
        }
        let expected_reports = trade.maker_orders.len();

        let (reports, discards) = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            None,
            None,
            Some(UnixNanos::from(1)),
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(reports.len(), expected_reports);
        assert_eq!(discards.unknown_age, 1);
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
            Money::new(0.0, Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &OrderFillTrackerMap::new());

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
    }

    /// Builds a partially filled report for an order the venue says filled `filled`.
    fn order_report_filled(venue_order_id: VenueOrderId, filled: &str) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from(filled),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )
    }

    #[rstest]
    fn floors_filled_quantity_at_locally_tracked_fills_without_confirmed_ones() {
        let venue_order_id = VenueOrderId::from("V-PENDING");
        let mut reports = vec![order_report_filled(venue_order_id, "6.0000")];

        // Fills that have matched but not settled produce no confirmed fill
        // report, so a zero floor would erase them.
        let tracker = OrderFillTrackerMap::new();
        tracker.restore_order(
            venue_order_id,
            Quantity::from("10.0000"),
            Quantity::from("6.0000"),
            OrderSide::Buy,
        );

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &tracker);

        assert_eq!(reports[0].filled_qty, Quantity::from("6.0000"));
    }

    #[rstest]
    fn takes_the_greater_of_tracked_and_confirmed_fills() {
        let venue_order_id = VenueOrderId::from("V-BOTH");
        let mut reports = vec![order_report_filled(venue_order_id, "9.0000")];
        let fills = vec![FillReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            venue_order_id,
            TradeId::from("T-BOTH"),
            OrderSide::Buy,
            Quantity::from("7.0000"),
            Price::from("0.5000"),
            Money::new(0.0, Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let tracker = OrderFillTrackerMap::new();
        tracker.restore_order(
            venue_order_id,
            Quantity::from("10.0000"),
            Quantity::from("3.0000"),
            OrderSide::Buy,
        );

        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &tracker);

        assert_eq!(reports[0].filled_qty, Quantity::from("7.0000"));
    }

    #[rstest]
    fn still_caps_a_venue_quantity_above_everything_known_locally() {
        let venue_order_id = VenueOrderId::from("V-OVER");
        let mut reports = vec![order_report_filled(venue_order_id, "10.0000")];
        let tracker = OrderFillTrackerMap::new();
        tracker.restore_order(
            venue_order_id,
            Quantity::from("10.0000"),
            Quantity::from("2.0000"),
            OrderSide::Buy,
        );

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &tracker);

        assert_eq!(reports[0].filled_qty, Quantity::from("2.0000"));
    }

    #[rstest]
    fn caps_an_untracked_order_to_its_confirmed_fills() {
        let venue_order_id = VenueOrderId::from("V-UNTRACKED");
        let mut reports = vec![order_report_filled(venue_order_id, "5.0000")];

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &OrderFillTrackerMap::new());

        assert_eq!(reports[0].filled_qty, Quantity::zero(4));
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

        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &OrderFillTrackerMap::new());

        assert_eq!(reports[0].quantity, Quantity::from(expected_quantity));
        assert_eq!(reports[0].filled_qty, Quantity::from(confirmed));
    }
}
