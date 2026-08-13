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

use std::{fmt::Display, hash::Hash};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use indexmap::IndexMap;
use nautilus_core::{
    UnixNanos, collections::AtomicMap, datetime::NANOSECONDS_IN_SECOND, time::AtomicTime,
};
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus, PositionSideSpecified},
    identifiers::{AccountId, ClientId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    order_fill_tracker::OrderFillTrackerMap,
    parse::{
        ReportParseError, build_maker_fill_report, instrument_fee_exponent, instrument_taker_fee,
        parse_fill_report, parse_order_status_report, parse_timestamp,
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

/// What an omission concerns, which bounds the authority the omission can destroy.
///
/// The client answers for the instruments in its execution lookup and for nothing else. A row
/// naming any other asset belongs to another user of the same funder wallet, so it is evidence
/// about something this client does not trade and cannot make this client's own evidence
/// unusable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OmissionScope {
    /// Evidence about an instrument in the execution lookup.
    Instrument(InstrumentId),
    /// Evidence naming an asset outside the execution lookup.
    Foreign,
    /// Evidence this client cannot bind to any asset.
    Account,
}

impl OmissionScope {
    /// Returns whether unusable evidence with this scope leaves nothing for the caller to state.
    ///
    /// `requested` is the instrument the caller asked about, if any.
    fn blocks_response(self, requested: Option<InstrumentId>) -> bool {
        match self {
            Self::Instrument(instrument_id) => requested == Some(instrument_id),
            Self::Foreign => false,
            Self::Account => true,
        }
    }
}

impl Display for OmissionScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Instrument(instrument_id) => write!(f, "{instrument_id}"),
            Self::Foreign => f.write_str("foreign"),
            Self::Account => f.write_str("account"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PositionOmission {
    Dust,
    Zero,
    UnmappedInstrument,
    InvalidSize,
    InvalidAveragePrice,
    Conflicting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReconciliationOmission {
    PendingTrade,
    FailedTrade,
    UnmappedOrder,
    InvalidOrder(ReportParseError),
    UnmappedFill,
    InvalidFill(ReportParseError),
    UnownedMakerTrade,
    /// A trade naming a requested order while naming another asset.
    ContradictoryFill,
    Position(PositionOmission),
    LookbackOrder,
    LookbackFill,
    /// An order row outside the instrument, status, or time range asked about.
    OutOfScopeOrder,
    /// A trade row outside the orders, instrument, or time range asked about.
    OutOfScopeFill,
    /// A position row outside the instrument asked about.
    OutOfScopePosition,
}

impl ReconciliationOmission {
    /// Returns whether the omission leaves what it concerns unstatable.
    ///
    /// A row the caller did not ask about is evidence about something else, so leaving it out is
    /// the narrowing the caller requested rather than evidence this client failed to account for.
    /// A row that was asked about and could not be used is the opposite: the answer would be
    /// missing part of what it claims to cover.
    const fn invalidates_snapshot(self) -> bool {
        match self {
            Self::PendingTrade
            | Self::UnmappedOrder
            | Self::InvalidOrder(_)
            | Self::UnmappedFill
            | Self::InvalidFill(_)
            | Self::UnownedMakerTrade
            | Self::ContradictoryFill
            | Self::Position(
                PositionOmission::UnmappedInstrument
                | PositionOmission::InvalidSize
                | PositionOmission::InvalidAveragePrice
                | PositionOmission::Conflicting,
            ) => true,
            Self::FailedTrade
            | Self::Position(PositionOmission::Dust | PositionOmission::Zero)
            | Self::LookbackOrder
            | Self::LookbackFill
            | Self::OutOfScopeOrder
            | Self::OutOfScopeFill
            | Self::OutOfScopePosition => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReconciliationOmissions {
    counts: IndexMap<(OmissionScope, ReconciliationOmission), usize>,
}

impl ReconciliationOmissions {
    pub(crate) fn record(&mut self, scope: OmissionScope, reason: ReconciliationOmission) {
        self.record_n(scope, reason, 1);
    }

    pub(crate) fn record_n(
        &mut self,
        scope: OmissionScope,
        reason: ReconciliationOmission,
        count: usize,
    ) {
        if count > 0 {
            *self.counts.entry((scope, reason)).or_default() += count;
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn count(&self, scope: OmissionScope, reason: ReconciliationOmission) -> usize {
        self.counts
            .get(&(scope, reason))
            .copied()
            .unwrap_or_default()
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    pub(crate) fn merge(&mut self, other: Self) {
        for ((scope, reason), count) in other.counts {
            self.record_n(scope, reason, count);
        }
    }

    /// Returns whether any omission leaves what it concerns unstatable.
    #[must_use]
    pub(crate) fn has_invalidating(&self) -> bool {
        self.counts
            .keys()
            .any(|(_, reason)| reason.invalidates_snapshot())
    }

    /// Returns the instruments whose evidence is unusable, so no report about them can be stated.
    #[must_use]
    pub(crate) fn non_authoritative_instruments(&self) -> AHashSet<InstrumentId> {
        self.counts
            .keys()
            .filter(|(_, reason)| reason.invalidates_snapshot())
            .filter_map(|(scope, _)| match scope {
                OmissionScope::Instrument(instrument_id) => Some(*instrument_id),
                OmissionScope::Foreign | OmissionScope::Account => None,
            })
            .collect()
    }

    /// Fails when unusable evidence leaves nothing the caller can state.
    ///
    /// Unusable evidence about one instrument says nothing about another, so it withholds that
    /// instrument's reports (see [`withhold_non_authoritative`]) rather than failing the whole
    /// response. A response still fails when the evidence cannot be bound to any instrument, or
    /// when it concerns `requested`: the single instrument the caller asked about.
    pub(crate) fn ensure_authoritative(
        &self,
        context: &str,
        requested: Option<InstrumentId>,
    ) -> anyhow::Result<()> {
        let invalidating = self
            .counts
            .iter()
            .filter(|((scope, reason), _)| {
                reason.invalidates_snapshot() && scope.blocks_response(requested)
            })
            .map(|((scope, reason), count)| format!("{reason:?}@{scope}={count}"))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            invalidating.is_empty(),
            "{context} is not authoritative: {}",
            invalidating.join(", "),
        );
        Ok(())
    }
}

impl Display for ReconciliationOmissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut separator = "";

        for ((scope, reason), count) in &self.counts {
            write!(f, "{separator}{reason:?}@{scope}={count}")?;
            separator = ", ";
        }

        if separator.is_empty() {
            f.write_str("none")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReportSet<T> {
    pub reports: Vec<T>,
    pub omissions: ReconciliationOmissions,
}

impl<T> ReportSet<T> {
    fn new() -> Self {
        Self {
            reports: Vec::new(),
            omissions: ReconciliationOmissions::default(),
        }
    }

    fn omit(&mut self, scope: OmissionScope, reason: ReconciliationOmission) {
        self.omissions.record(scope, reason);
    }
}

/// Returns what a venue row is evidence about, which bounds the authority its omission can destroy.
///
/// An asset outside the execution lookup belongs to another user of the same funder wallet, so a
/// row naming it is [`OmissionScope::Foreign`] rather than evidence this client left out.
pub(crate) fn evidence_scope(
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    asset: Ustr,
) -> OmissionScope {
    instruments
        .load()
        .get(&asset)
        .map_or(OmissionScope::Foreign, |instrument| {
            OmissionScope::Instrument(instrument.id())
        })
}

/// Fails when the client holds no execution lookup while the venue reports evidence for the wallet.
///
/// With an empty lookup every row is [`OmissionScope::Foreign`], so a response would be
/// authoritative only because it covers nothing, and a consumer reads an absent order or position
/// as "not at the venue". Instruments that failed to load would therefore present as a flat
/// account. Absence must not read as authority, so this is reported as a load failure rather than
/// answered. An empty lookup with no evidence is an account that genuinely holds nothing.
pub(crate) fn ensure_execution_lookup_loaded(
    context: &str,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    evidence_rows: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        evidence_rows == 0 || !instruments.load().is_empty(),
        "{context} cannot be stated: the execution lookup holds no instruments while the venue reports {evidence_rows} row(s) for the account",
    );
    Ok(())
}

/// Withholds every report about an instrument whose evidence is unusable, returning how many.
///
/// Authority is decided per instrument, so the reports the remaining evidence does support are
/// still stated.
pub(crate) fn withhold_non_authoritative<T>(
    reports: &mut Vec<T>,
    blocked: &AHashSet<InstrumentId>,
    instrument_of: impl Fn(&T) -> InstrumentId,
) -> usize {
    if blocked.is_empty() {
        return 0;
    }

    let before = reports.len();
    reports.retain(|report| !blocked.contains(&instrument_of(report)));
    before - reports.len()
}

#[derive(Debug)]
struct ReconciliationSnapshot {
    orders: Vec<OrderStatusReport>,
    fills: Vec<FillReport>,
    positions: Vec<PositionStatusReport>,
}

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
pub(crate) fn build_fill_reports_from_trades<'a>(
    trades: impl IntoIterator<Item = &'a PolymarketTradeReport>,
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> ReportSet<FillReport> {
    let mut output = ReportSet::new();

    for trade in trades {
        // Settlement is decided for the whole trade, but the omission it produces belongs to the
        // instrument the trade is evidence about, so it is recorded once each leg is resolved.
        let pending_settlement = match trade.status {
            PolymarketTradeStatus::Confirmed => false,
            PolymarketTradeStatus::Matched
            | PolymarketTradeStatus::Mined
            | PolymarketTradeStatus::Retrying => true,
            PolymarketTradeStatus::Failed => {
                // A failed trade is a terminal venue answer that never becomes a fill, so there
                // is no evidence about any instrument to account for.
                output.omit(OmissionScope::Account, ReconciliationOmission::FailedTrade);
                continue;
            }
        };

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

        if is_maker {
            if !trade
                .maker_orders
                .iter()
                .any(|mo| mo.is_owned_by(ctx.user_address, ctx.api_key))
            {
                // The venue states the account made this trade, yet none of the maker orders
                // identify it, so the fill cannot be bound to an instrument to withhold.
                output.omit(
                    OmissionScope::Account,
                    ReconciliationOmission::UnownedMakerTrade,
                );
                log::debug!(
                    "Maker trade {} holds no maker order owned by the account",
                    trade.id,
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
                        output.omit(OmissionScope::Foreign, ReconciliationOmission::UnmappedFill);
                        continue;
                    }
                };

                if let Some(filter_id) = instrument_filter
                    && instrument_id != filter_id
                {
                    output.omit(
                        OmissionScope::Instrument(instrument_id),
                        ReconciliationOmission::OutOfScopeFill,
                    );
                    continue;
                }

                if pending_settlement {
                    output.omit(
                        OmissionScope::Instrument(instrument_id),
                        ReconciliationOmission::PendingTrade,
                    );
                    continue;
                }

                let Some(ts_event) = parse_timestamp(&trade.match_time) else {
                    output.omit(
                        OmissionScope::Instrument(instrument_id),
                        ReconciliationOmission::InvalidFill(ReportParseError::Timestamp),
                    );
                    continue;
                };
                let report = match build_maker_fill_report(
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
                ) {
                    Ok(report) => report,
                    Err(e) => {
                        output.omit(
                            OmissionScope::Instrument(instrument_id),
                            ReconciliationOmission::InvalidFill(e),
                        );
                        continue;
                    }
                };
                output.reports.push(report);
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
                        output.omit(OmissionScope::Foreign, ReconciliationOmission::UnmappedFill);
                        continue;
                    }
                };

            if let Some(filter_id) = instrument_filter
                && instrument_id != filter_id
            {
                output.omit(
                    OmissionScope::Instrument(instrument_id),
                    ReconciliationOmission::OutOfScopeFill,
                );
                continue;
            }

            if pending_settlement {
                output.omit(
                    OmissionScope::Instrument(instrument_id),
                    ReconciliationOmission::PendingTrade,
                );
                continue;
            }

            let report = match parse_fill_report(
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
            ) {
                Ok(report) => report,
                Err(e) => {
                    output.omit(
                        OmissionScope::Instrument(instrument_id),
                        ReconciliationOmission::InvalidFill(e),
                    );
                    continue;
                }
            };
            output.reports.push(report);
        }
    }

    collapse_repeated_evidence(
        &mut output,
        fill_evidence_key,
        |fill| fill.instrument_id,
        same_fill_evidence,
        ReconciliationOmission::InvalidFill(ReportParseError::ConflictingFill),
    );

    output
}

/// Converts open orders into order status reports.
pub(crate) fn build_order_reports_from_orders<'a>(
    orders: impl IntoIterator<Item = &'a PolymarketOpenOrder>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> ReportSet<OrderStatusReport> {
    let mut output = ReportSet::new();

    for order in orders {
        let token_id = Ustr::from(order.asset_id.as_str());
        let instrument = instruments.get_cloned(&token_id);
        let (instrument_id, price_prec, size_prec) = match instrument {
            Some(i) => (i.id(), i.price_precision(), i.size_precision()),
            None => {
                output.omit(
                    OmissionScope::Foreign,
                    ReconciliationOmission::UnmappedOrder,
                );
                continue;
            }
        };

        if let Some(filter_id) = instrument_filter
            && instrument_id != filter_id
        {
            output.omit(
                OmissionScope::Instrument(instrument_id),
                ReconciliationOmission::OutOfScopeOrder,
            );
            continue;
        }

        let report = match parse_order_status_report(
            order,
            instrument_id,
            account_id,
            None,
            price_prec,
            size_prec,
            ts_init,
        ) {
            Ok(report) => report,
            Err(e) => {
                output.omit(
                    OmissionScope::Instrument(instrument_id),
                    ReconciliationOmission::InvalidOrder(e),
                );
                continue;
            }
        };
        output.reports.push(report);
    }

    // A consumer keys order reports by venue order id, so two rows for one id there overwrite
    // each other. The disagreement has to be caught before it reaches that keyed insert.
    collapse_repeated_evidence(
        &mut output,
        |report| report.venue_order_id,
        |report| report.instrument_id,
        same_order_evidence,
        ReconciliationOmission::InvalidOrder(ReportParseError::ConflictingOrder),
    );

    output
}

/// Applies venue_order_id and time-range filters to fill reports.
pub(crate) fn apply_fill_filters(
    mut reports: Vec<FillReport>,
    venue_order_id: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    omissions: &mut ReconciliationOmissions,
) -> Vec<FillReport> {
    reports.retain(|report| {
        let in_scope = venue_order_id.is_none_or(|id| report.venue_order_id == id)
            && start.is_none_or(|start| report.ts_event >= start)
            && end.is_none_or(|end| report.ts_event <= end);

        if !in_scope {
            omissions.record(
                OmissionScope::Instrument(report.instrument_id),
                ReconciliationOmission::OutOfScopeFill,
            );
        }

        in_scope
    });

    reports
}

/// Builds position status reports from Data API positions, reporting dust as flat.
///
/// A row at or below [`DUST_POSITION_THRESHOLD`] is the venue stating a flat balance, so it is
/// reported as [`PositionSideSpecified::Flat`] rather than dropped. Dropping it would leave a
/// consumer to infer flat from a report that is simply absent, which is indistinguishable from
/// having no evidence for the instrument at all.
pub(crate) fn build_position_reports<'a>(
    positions: impl IntoIterator<Item = &'a DataApiPosition>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    ts: UnixNanos,
) -> ReportSet<PositionStatusReport> {
    let mut output = ReportSet::new();
    // The venue states one balance per asset, so repeated rows for one asset are the venue
    // disagreeing with itself. A consumer keeps every position report an instrument gets and
    // applies them in order, so both rows would be applied as though both were true.
    let rows = positions.into_iter().collect::<Vec<_>>();
    let (unique, conflicting) = dedupe_evidence(
        &rows,
        |position| Ustr::from(position.asset.as_str()),
        |left, right| left == right,
    );

    for asset in &conflicting {
        output.omit(
            evidence_scope(instruments, *asset),
            ReconciliationOmission::Position(PositionOmission::Conflicting),
        );
    }

    for position in unique {
        let asset = Ustr::from(position.asset.as_str());

        if conflicting.contains(&asset) {
            continue;
        }

        let instrument = instruments.get_cloned(&asset);

        if position.size >= Decimal::ZERO && position.size < DUST_POSITION_THRESHOLD {
            // Without an instrument mapping there is no position for a report to be about, so a
            // flat row for an unmapped asset carries no evidence and stays out of the snapshot.
            let Some(instrument) = instrument else {
                let reason = if position.size > Decimal::ZERO {
                    PositionOmission::Dust
                } else {
                    PositionOmission::Zero
                };
                output.omit(
                    OmissionScope::Foreign,
                    ReconciliationOmission::Position(reason),
                );
                continue;
            };

            if position.size > Decimal::ZERO {
                log::debug!(
                    "Reporting dust position as flat: {}-{}, size={}",
                    position.condition_id,
                    position.asset,
                    position.size,
                );
            }

            output.reports.push(PositionStatusReport::new(
                account_id,
                instrument.id(),
                PositionSideSpecified::Flat,
                Quantity::zero(USDC_DECIMALS as u8),
                ts,
                ts,
                None,
                None,
                None,
            ));
            continue;
        }

        let Some(instrument) = instrument else {
            output.omit(
                OmissionScope::Foreign,
                ReconciliationOmission::Position(PositionOmission::UnmappedInstrument),
            );
            continue;
        };
        let instrument_id = instrument.id();

        let quantity = match Quantity::from_decimal_dp(position.size, USDC_DECIMALS as u8) {
            Ok(quantity) => quantity,
            Err(_) => {
                output.omit(
                    OmissionScope::Instrument(instrument_id),
                    ReconciliationOmission::Position(PositionOmission::InvalidSize),
                );
                continue;
            }
        };
        let Some(avg_price) = position
            .avg_price
            .filter(|price| *price > Decimal::ZERO && *price < Decimal::ONE)
        else {
            output.omit(
                OmissionScope::Instrument(instrument_id),
                ReconciliationOmission::Position(PositionOmission::InvalidAveragePrice),
            );
            continue;
        };
        output.reports.push(PositionStatusReport::new(
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

    output
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
    let orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;
    let trades = http_client
        .get_trades(GetTradesParams::default())
        .await
        .context("failed to fetch trades for mass status")?;
    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;
    let snapshot = build_reconciliation_snapshot(
        &orders,
        &trades,
        &positions,
        instruments,
        fill_tracker,
        ctx,
        lookback_mins,
        ts_init,
    )?;

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);
    mass_status.add_order_reports(snapshot.orders);
    mass_status.add_position_reports(snapshot.positions);
    mass_status.add_fill_reports(snapshot.fills);

    Ok(Some(mass_status))
}

#[expect(clippy::too_many_arguments)]
fn build_reconciliation_snapshot(
    orders: &[PolymarketOpenOrder],
    trades: &[PolymarketTradeReport],
    positions: &[DataApiPosition],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    fill_tracker: &OrderFillTrackerMap,
    ctx: &FillContext<'_>,
    lookback_mins: Option<u64>,
    ts_init: UnixNanos,
) -> anyhow::Result<ReconciliationSnapshot> {
    ensure_execution_lookup_loaded(
        "Mass status",
        instruments,
        orders.len() + trades.len() + positions.len(),
    )?;

    let cutoff = lookback_mins.map(|mins| {
        let lookback_ns = mins.saturating_mul(60).saturating_mul(1_000_000_000);
        UnixNanos::from(ts_init.as_u64().saturating_sub(lookback_ns))
    });
    let (scoped_orders, old_orders) = scope_by_lookback(orders, cutoff, |order| {
        // An order still resting at the venue is evidence about the present, whatever its age:
        // reconciliation exists to see it. Only an order the venue has already finished with is
        // history the window can leave out.
        if OrderStatus::from(order.status).is_open() {
            return None;
        }

        order
            .created_at
            .checked_mul(NANOSECONDS_IN_SECOND)
            .map(UnixNanos::from)
    });
    let (scoped_trades, old_trades) =
        scope_by_lookback(trades, cutoff, |trade| parse_timestamp(&trade.match_time));

    let mut order_set =
        build_order_reports_from_orders(scoped_orders, instruments, ctx.account_id, None, ts_init);
    let mut fill_set =
        build_fill_reports_from_trades(scoped_trades, ctx, instruments, None, ts_init);
    let mut position_set = build_position_reports(positions, instruments, ctx.account_id, ts_init);

    fill_tracker.snap_fill_reports(&mut fill_set.reports);
    order_set.omissions.record_n(
        OmissionScope::Account,
        ReconciliationOmission::LookbackOrder,
        old_orders,
    );
    fill_set.omissions.record_n(
        OmissionScope::Account,
        ReconciliationOmission::LookbackFill,
        old_trades,
    );

    cap_order_reports_to_confirmed_fills(
        &mut order_set.reports,
        &fill_set.reports,
        &mut order_set.omissions,
    )?;

    let mut omissions = order_set.omissions;
    omissions.merge(fill_set.omissions);
    omissions.merge(position_set.omissions);

    // An instrument without usable evidence is withheld whole: stating a position without the
    // fills behind it, or an order without the position it moved, would have a consumer close
    // the difference itself.
    let blocked = omissions.non_authoritative_instruments();
    let withheld = withhold_non_authoritative(&mut order_set.reports, &blocked, |report| {
        report.instrument_id
    }) + withhold_non_authoritative(&mut fill_set.reports, &blocked, |report| {
        report.instrument_id
    }) + withhold_non_authoritative(&mut position_set.reports, &blocked, |report| {
        report.instrument_id
    });

    log_reconciliation_summary(
        "mass status",
        order_set.reports.len(),
        fill_set.reports.len(),
        position_set.reports.len(),
        withheld,
        &omissions,
    );
    omissions.ensure_authoritative("Mass status", None)?;

    Ok(ReconciliationSnapshot {
        orders: order_set.reports,
        fills: fill_set.reports,
        positions: position_set.reports,
    })
}

/// Narrows rows to the lookback window, returning how many the window left out.
///
/// `timestamp` returns `None` for a row the window cannot exclude, either because the row is not
/// history or because it carries no time the window could be compared against.
fn scope_by_lookback<T>(
    rows: &[T],
    cutoff: Option<UnixNanos>,
    timestamp: impl Fn(&T) -> Option<UnixNanos>,
) -> (Vec<&T>, usize) {
    let Some(cutoff) = cutoff else {
        return (rows.iter().collect(), 0);
    };

    let mut selected = Vec::with_capacity(rows.len());
    let mut omitted = 0;

    for row in rows {
        match timestamp(row) {
            Some(ts) if ts < cutoff => omitted += 1,
            _ => selected.push(row),
        }
    }
    (selected, omitted)
}

pub(crate) fn log_reconciliation_summary(
    route: &str,
    order_reports: usize,
    fill_reports: usize,
    position_reports: usize,
    withheld_reports: usize,
    omissions: &ReconciliationOmissions,
) {
    let message = format!(
        "Polymarket {route}: reports(order={order_reports}, fill={fill_reports}, position={position_reports}); withheld={withheld_reports}; omissions={omissions}"
    );

    // Narrowing the evidence to what was asked about is ordinary, so the summary is only raised
    // to a warning when something the response claims to cover could not be stated.
    if withheld_reports == 0 && !omissions.has_invalidating() {
        log::debug!("{message}");
    } else {
        log::warn!("{message}");
    }
}

fn cap_order_reports_to_confirmed_fills(
    order_reports: &mut Vec<OrderStatusReport>,
    fill_reports: &[FillReport],
    omissions: &mut ReconciliationOmissions,
) -> Result<(), ReportParseError> {
    let confirmed_by_order = confirmed_filled_quantities(fill_reports)?;
    let mut invalid = Vec::new();

    order_reports.retain_mut(|report| {
        let local_filled = Quantity::zero(report.quantity.precision);

        match cap_order_report_filled_qty(
            report,
            local_filled,
            confirmed_by_order.get(&report.venue_order_id).copied(),
        ) {
            Ok(()) => true,
            Err(e) => {
                invalid.push((report.instrument_id, e));
                false
            }
        }
    });

    for (instrument_id, error) in invalid {
        omissions.record(
            OmissionScope::Instrument(instrument_id),
            ReconciliationOmission::InvalidOrder(error),
        );
    }

    Ok(())
}

pub(crate) fn confirmed_filled_quantities(
    fill_reports: &[FillReport],
) -> Result<AHashMap<VenueOrderId, Decimal>, ReportParseError> {
    let mut confirmed_by_order = AHashMap::new();
    for fill in unique_fill_evidence(fill_reports)? {
        *confirmed_by_order.entry(fill.venue_order_id).or_default() += fill.last_qty.as_decimal();
    }

    Ok(confirmed_by_order)
}

fn unique_fill_evidence(fill_reports: &[FillReport]) -> Result<Vec<&FillReport>, ReportParseError> {
    let (unique, conflicting) = dedupe_fill_evidence(fill_reports);

    if conflicting.is_empty() {
        return Ok(unique);
    }

    Err(ReportParseError::ConflictingFill)
}

fn dedupe_fill_evidence(fill_reports: &[FillReport]) -> (Vec<&FillReport>, AHashSet<FillKey>) {
    dedupe_evidence(fill_reports, fill_evidence_key, same_fill_evidence)
}

/// The venue's identity for a fill: one trade, on one instrument, for one account.
type FillKey = (AccountId, InstrumentId, TradeId);

fn fill_evidence_key(fill: &FillReport) -> FillKey {
    (fill.account_id, fill.instrument_id, fill.trade_id)
}

/// Collapses repeated venue rows in a set, recording the instruments whose repeated rows disagree.
///
/// Rows that disagree about one venue key leave no way to tell which of them the venue meant, so
/// the instrument they name is withheld rather than answered from whichever row happened to be
/// consumed last.
fn collapse_repeated_evidence<T: Clone, K: Copy + Eq + Hash>(
    output: &mut ReportSet<T>,
    key: impl Fn(&T) -> K,
    instrument_of: impl Fn(&T) -> InstrumentId,
    same: impl Fn(&T, &T) -> bool,
    conflict: ReconciliationOmission,
) {
    let (unique, conflicting) = dedupe_evidence(&output.reports, &key, same);
    let blocked = unique
        .iter()
        .filter(|report| conflicting.contains(&key(report)))
        .map(|report| instrument_of(report))
        .collect::<AHashSet<_>>();
    output.reports = unique
        .into_iter()
        .filter(|report| !blocked.contains(&instrument_of(report)))
        .cloned()
        .collect();

    for instrument_id in blocked {
        output.omit(OmissionScope::Instrument(instrument_id), conflict);
    }
}

/// Collapses repeated venue rows, naming the keys whose repeated rows disagree.
///
/// `key` must be the venue's own identity for the row, so that two answers to one question are
/// what gets compared. Rows the venue keeps apart are not made to answer for each other.
fn dedupe_evidence<T, K: Copy + Eq + Hash>(
    rows: &[T],
    key: impl Fn(&T) -> K,
    same: impl Fn(&T, &T) -> bool,
) -> (Vec<&T>, AHashSet<K>) {
    let mut unique = Vec::with_capacity(rows.len());
    let mut seen = AHashMap::<K, &T>::new();
    let mut conflicting = AHashSet::new();

    for row in rows {
        if let Some(previous) = seen.insert(key(row), row) {
            if !same(previous, row) {
                conflicting.insert(key(row));
            }
        } else {
            unique.push(row);
        }
    }

    (unique, conflicting)
}

/// Returns whether two rows for one venue key carry the same evidence.
///
/// `report_id` and `ts_init` identify the report this client built rather than what the venue
/// stated, so they are normalized away and every remaining field is compared. Comparing whole
/// reports keeps a field added later inside the check instead of silently outside it.
fn same_fill_evidence(left: &FillReport, right: &FillReport) -> bool {
    let mut right = right.clone();
    right.report_id = left.report_id;
    right.ts_init = left.ts_init;
    *left == right
}

/// Returns whether two rows for one venue order carry the same evidence.
///
/// See [`same_fill_evidence`] for why the report's own identifiers are normalized away.
fn same_order_evidence(left: &OrderStatusReport, right: &OrderStatusReport) -> bool {
    let mut right = right.clone();
    right.report_id = left.report_id;
    right.ts_init = left.ts_init;
    *left == right
}

pub(crate) fn cap_order_report_filled_qty(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Option<Decimal>,
) -> Result<(), ReportParseError> {
    let confirmed_filled = match confirmed_filled {
        Some(quantity) => Quantity::from_decimal_dp(quantity, report.quantity.precision)
            .map_err(|_| ReportParseError::FilledQuantity)?,
        None => Quantity::zero(report.quantity.precision),
    };
    let capped = report.filled_qty.min(local_filled.max(confirmed_filled));
    report.filled_qty = capped;
    normalize_terminal_order_report_quantity(report);
    if report.order_status == OrderStatus::Filled && report.filled_qty < report.quantity {
        return Err(ReportParseError::FilledQuantity);
    }

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

    use super::*;
    use crate::{
        common::enums::PolymarketTradeStatus,
        http::{
            models::GammaMarket,
            parse::{create_instrument_from_def, parse_gamma_market},
        },
    };

    fn load<T: serde::de::DeserializeOwned>(filename: &str) -> T {
        let path = format!("test_data/{filename}");
        let content = std::fs::read_to_string(path).expect("failed to read test data");
        serde_json::from_str(&content).expect("failed to parse test data")
    }

    /// The two outcome instruments of the fixture market, which are distinct instruments the
    /// client can be responsible for independently.
    fn test_instruments() -> Vec<InstrumentAny> {
        let market: GammaMarket = load("gamma_market.json");
        let defs = parse_gamma_market(&market).expect("market should parse");
        defs.iter()
            .map(|def| {
                create_instrument_from_def(def, UnixNanos::from(1_000_000_000u64))
                    .expect("instrument should parse")
            })
            .collect()
    }

    fn test_instrument() -> InstrumentAny {
        test_instruments().swap_remove(0)
    }

    const TRADED_TOKEN: &str = "traded-token";
    const OTHER_TRADED_TOKEN: &str = "other-traded-token";
    const FOREIGN_TOKEN: &str = "foreign-token";

    /// An execution lookup holding the two instruments this client trades.
    fn traded_instruments() -> (AtomicMap<Ustr, InstrumentAny>, InstrumentId, InstrumentId) {
        let mut instruments = test_instruments();
        let other = instruments.remove(1);
        let traded = instruments.remove(0);
        let traded_id = traded.id();
        let other_id = other.id();
        let lookup = AtomicMap::new();
        lookup.insert(Ustr::from(TRADED_TOKEN), traded);
        lookup.insert(Ustr::from(OTHER_TRADED_TOKEN), other);
        (lookup, traded_id, other_id)
    }

    fn order_on(token: &str, venue_order_id: &str) -> PolymarketOpenOrder {
        let mut order: PolymarketOpenOrder = load("http_open_order.json");
        order.asset_id = Ustr::from(token);
        order.id = venue_order_id.to_string();
        order
    }

    fn taker_trade_on(token: &str, venue_order_id: &str, trade_id: &str) -> PolymarketTradeReport {
        let mut trade: PolymarketTradeReport = load("http_trade_report.json");
        trade.asset_id = Ustr::from(token);
        trade.taker_order_id = venue_order_id.to_string();
        trade.id = trade_id.to_string();
        trade
    }

    fn position_on(token: &str, size: Decimal) -> DataApiPosition {
        DataApiPosition {
            asset: token.to_string(),
            condition_id: "0xabc".to_string(),
            size,
            avg_price: Some(Decimal::new(5, 1)),
        }
    }

    fn test_fill_context() -> FillContext<'static> {
        FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
            api_key: "00000000-0000-0000-0000-000000000001",
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        }
    }

    #[rstest]
    fn reconciliation_snapshot_ignores_evidence_for_instruments_not_traded() {
        let (instruments, traded_id, _) = traded_instruments();
        let traded_order = order_on(TRADED_TOKEN, "V-TRADED");
        let foreign_order = order_on(FOREIGN_TOKEN, "V-FOREIGN");
        let traded_trade = taker_trade_on(TRADED_TOKEN, "V-TRADED", "T-TRADED");
        let mut foreign_trade = taker_trade_on(FOREIGN_TOKEN, "V-FOREIGN", "T-FOREIGN");
        foreign_trade.status = PolymarketTradeStatus::Retrying;

        let snapshot = build_reconciliation_snapshot(
            &[traded_order, foreign_order],
            &[traded_trade, foreign_trade],
            &[
                position_on(TRADED_TOKEN, Decimal::TEN),
                position_on(FOREIGN_TOKEN, Decimal::TEN),
            ],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("evidence for instruments the client does not trade must not destroy authority");

        // Every foreign row belongs to another user of the same funder wallet: an open order in
        // a market this client does not trade, an unsettled trade there, and the position it
        // holds. None of them says anything about the instruments this client is responsible
        // for, so all of the in-scope evidence is still stated.
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].instrument_id, traded_id);
        assert_eq!(snapshot.fills.len(), 1);
        assert_eq!(snapshot.fills[0].instrument_id, traded_id);
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].instrument_id, traded_id);
    }

    #[rstest]
    fn reconciliation_snapshot_bounds_pending_settlement_to_its_own_instrument() {
        let (instruments, traded_id, other_id) = traded_instruments();
        let mut pending = taker_trade_on(TRADED_TOKEN, "V-PENDING", "T-PENDING");
        pending.status = PolymarketTradeStatus::Retrying;

        let snapshot = build_reconciliation_snapshot(
            &[
                order_on(TRADED_TOKEN, "V-PENDING"),
                order_on(OTHER_TRADED_TOKEN, "V-SETTLED"),
            ],
            &[pending],
            &[
                position_on(TRADED_TOKEN, Decimal::TEN),
                position_on(OTHER_TRADED_TOKEN, Decimal::TEN),
            ],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an unsettled trade on one order must not withhold every other order");

        // Polymarket settles asynchronously, so an unsettled trade is ordinary. It leaves the
        // order it belongs to unstatable and says nothing about any other order.
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].instrument_id, other_id);
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].instrument_id, other_id);
        assert!(
            !snapshot
                .orders
                .iter()
                .any(|report| report.instrument_id == traded_id),
        );
    }

    #[rstest]
    fn reconciliation_snapshot_withholds_instrument_with_pending_settlement() {
        let (instruments, _, _) = traded_instruments();
        let mut trade = taker_trade_on(TRADED_TOKEN, "V-PENDING", "T-PENDING");
        trade.status = PolymarketTradeStatus::Matched;

        let snapshot = build_reconciliation_snapshot(
            &[order_on(TRADED_TOKEN, "V-PENDING")],
            &[trade],
            &[position_on(TRADED_TOKEN, Decimal::TEN)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a bounded omission withholds its instrument rather than failing the snapshot");

        // Negative control: the instrument IS traded, so the unsettled trade still destroys
        // authority over it. Stating the position without the fill behind it would have the
        // consumer close the difference itself.
        assert!(snapshot.orders.is_empty());
        assert!(snapshot.fills.is_empty());
        assert!(snapshot.positions.is_empty());
    }

    #[rstest]
    fn reconciliation_snapshot_rejects_fill_evidence_bound_to_no_instrument() {
        let (instruments, _, _) = traded_instruments();
        let mut trade = taker_trade_on(TRADED_TOKEN, "V-MAKER", "T-MAKER");
        trade.trader_side = PolymarketLiquiditySide::Maker;

        for maker_order in &mut trade.maker_orders {
            maker_order.maker_address = "0x000000000000000000000000000000000000dead".to_string();
            maker_order.owner = "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string();
        }

        let error = build_reconciliation_snapshot(
            &[order_on(TRADED_TOKEN, "V-TRADED")],
            &[trade],
            &[position_on(TRADED_TOKEN, Decimal::TEN)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("evidence that names no instrument leaves nothing that can be stated");

        // The venue states the account made this trade but no maker order identifies it, so
        // there is no instrument to withhold and the whole snapshot must be refused.
        assert_eq!(
            error.to_string(),
            "Mass status is not authoritative: UnownedMakerTrade@account=1",
        );
    }

    #[rstest]
    fn fill_builder_collapses_repeated_rest_rows() {
        let instrument = test_instrument();
        let trade: PolymarketTradeReport = load("http_trade_report.json");
        let instruments = AtomicMap::new();
        instruments.insert(trade.asset_id, instrument);
        let trades = vec![trade.clone(), trade];

        let output = build_fill_reports_from_trades(
            &trades,
            &test_fill_context(),
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(output.reports.len(), 1);
        assert!(output.omissions.is_empty());
    }

    #[rstest]
    fn fill_builder_rejects_conflicting_repeated_rest_rows() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let trade: PolymarketTradeReport = load("http_trade_report.json");
        let mut conflicting = trade.clone();
        conflicting.size += Decimal::ONE;
        let instruments = AtomicMap::new();
        instruments.insert(trade.asset_id, instrument);

        let output = build_fill_reports_from_trades(
            &[trade, conflicting],
            &test_fill_context(),
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(output.reports.is_empty());
        assert_eq!(
            output.omissions.count(
                OmissionScope::Instrument(instrument_id),
                ReconciliationOmission::InvalidFill(ReportParseError::ConflictingFill),
            ),
            1,
        );
    }

    #[rstest]
    fn order_builder_collapses_repeated_rest_rows() {
        let (instruments, _, _) = traded_instruments();
        let order = order_on(TRADED_TOKEN, "V-REPEATED");

        let output = build_order_reports_from_orders(
            &[order.clone(), order],
            &instruments,
            AccountId::from("POLY-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(output.reports.len(), 1);
        assert!(output.omissions.is_empty());
    }

    #[rstest]
    fn order_builder_rejects_conflicting_repeated_rest_rows() {
        let (instruments, traded_id, _) = traded_instruments();
        let order = order_on(TRADED_TOKEN, "V-REPEATED");
        let mut conflicting = order.clone();
        conflicting.size_matched += Decimal::ONE;

        let output = build_order_reports_from_orders(
            &[order, conflicting],
            &instruments,
            AccountId::from("POLY-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(output.reports.is_empty());
        assert_eq!(
            output.omissions.count(
                OmissionScope::Instrument(traded_id),
                ReconciliationOmission::InvalidOrder(ReportParseError::ConflictingOrder),
            ),
            1,
        );
    }

    #[rstest]
    fn reconciliation_snapshot_withholds_conflicting_repeated_order_rows() {
        let (instruments, _, other_id) = traded_instruments();
        let order = order_on(TRADED_TOKEN, "V-REPEATED");
        let mut conflicting = order.clone();
        conflicting.size_matched += Decimal::ONE;

        let snapshot = build_reconciliation_snapshot(
            &[order, conflicting, order_on(OTHER_TRADED_TOKEN, "V-LIVE")],
            &[],
            &[],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an unusable order withholds its instrument rather than failing the snapshot");

        // A consumer keys order reports by venue order id, so these two rows would collapse into
        // whichever was consumed last. Neither can be stated, and the other order is untouched.
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].instrument_id, other_id);
    }

    #[rstest]
    fn position_builder_collapses_repeated_rows() {
        let (instruments, _, _) = traded_instruments();
        let position = position_on(TRADED_TOKEN, Decimal::TEN);

        let output = build_position_reports(
            &[position.clone(), position],
            &instruments,
            AccountId::from("POLY-001"),
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(output.reports.len(), 1);
        assert!(output.omissions.is_empty());
    }

    #[rstest]
    fn position_builder_rejects_conflicting_repeated_rows() {
        let (instruments, traded_id, _) = traded_instruments();
        let position = position_on(TRADED_TOKEN, Decimal::TEN);
        let mut conflicting = position.clone();
        conflicting.size += Decimal::ONE;

        let output = build_position_reports(
            &[position, conflicting],
            &instruments,
            AccountId::from("POLY-001"),
            UnixNanos::from(1_000_000_000u64),
        );

        // A consumer keeps every position row for an instrument and applies them in order, so
        // two rows that disagree would both be applied as though both were true.
        assert!(output.reports.is_empty());
        assert_eq!(
            output.omissions.count(
                OmissionScope::Instrument(traded_id),
                ReconciliationOmission::Position(PositionOmission::Conflicting),
            ),
            1,
        );
    }

    #[rstest]
    fn reconciliation_snapshot_keeps_open_order_older_than_the_lookback() {
        let (instruments, traded_id, _) = traded_instruments();
        let mut resting = order_on(TRADED_TOKEN, "V-OLD");
        resting.created_at = 1;

        let snapshot = build_reconciliation_snapshot(
            &[resting],
            &[],
            &[],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            Some(1),
            UnixNanos::from(121_000_000_000u64),
        )
        .expect("a resting order is evidence about the present");

        // Reconciliation exists to see orders still resting at the venue. When one was placed
        // is not evidence that it is no longer there, so the window must not hide it.
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].instrument_id, traded_id);
    }

    #[rstest]
    fn reconciliation_snapshot_scopes_terminal_order_older_than_the_lookback() {
        let (instruments, _, _) = traded_instruments();
        let mut finished = order_on(TRADED_TOKEN, "V-OLD");
        finished.created_at = 1;
        finished.status = crate::common::enums::PolymarketOrderStatus::Canceled;

        let snapshot = build_reconciliation_snapshot(
            &[finished],
            &[],
            &[],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            Some(1),
            UnixNanos::from(121_000_000_000u64),
        )
        .expect("out-of-window history must not invalidate the selected snapshot");

        // Negative control: an order the venue has finished with is history, which is what the
        // window bounds, so it is still left out.
        assert!(snapshot.orders.is_empty());
    }

    #[rstest]
    fn reconciliation_snapshot_scopes_old_pending_trade_before_authority_check() {
        // The instrument is traded, so the lookback window is the only thing keeping this
        // unsettled trade out of the authority check.
        let (instruments, _, _) = traded_instruments();
        let mut trade = taker_trade_on(TRADED_TOKEN, "V-OLD", "T-OLD");
        trade.status = PolymarketTradeStatus::Matched;
        trade.match_time = "1".to_string();

        let snapshot = build_reconciliation_snapshot(
            &[],
            &[trade],
            &[position_on(TRADED_TOKEN, Decimal::TEN)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            Some(1),
            UnixNanos::from(121_000_000_000u64),
        )
        .expect("out-of-window pending evidence must not invalidate the selected snapshot");

        assert!(snapshot.fills.is_empty());
        assert_eq!(snapshot.positions.len(), 1);
    }

    #[rstest]
    fn reconciliation_snapshot_reports_zero_position_as_flat() {
        let position = DataApiPosition {
            asset: "123".to_string(),
            condition_id: "0xabc".to_string(),
            size: Decimal::ZERO,
            avg_price: Some(Decimal::new(5, 1)),
        };
        let instruments = AtomicMap::new();
        instruments.insert(Ustr::from("123"), test_instrument());

        let snapshot = build_reconciliation_snapshot(
            &[],
            &[],
            &[position],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a stated zero balance must not make the snapshot non-authoritative");

        // The venue stated a flat balance, so the snapshot states it too. Dropping the row would
        // leave the consumer to read flat out of a report that is merely absent, which it cannot
        // tell apart from having no evidence for the instrument.
        assert_eq!(snapshot.positions.len(), 1);
        assert!(snapshot.positions[0].is_flat());
        assert!(snapshot.positions[0].quantity.is_zero());
        assert_eq!(snapshot.positions[0].avg_px_open, None);
    }

    #[rstest]
    fn reconciliation_snapshot_omits_unmapped_zero_position() {
        let (instruments, _, _) = traded_instruments();
        let snapshot = build_reconciliation_snapshot(
            &[],
            &[],
            &[position_on(FOREIGN_TOKEN, Decimal::ZERO)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a flat row for an unmapped asset must not make the snapshot non-authoritative");

        assert!(snapshot.positions.is_empty());
    }

    #[rstest]
    fn reconciliation_snapshot_keeps_failed_trade_out_of_filled_quantity() {
        let instrument = test_instrument();
        let order: PolymarketOpenOrder = load("http_open_order.json");
        let mut trade: PolymarketTradeReport = load("http_trade_report.json");
        trade.status = PolymarketTradeStatus::Failed;

        let instruments = AtomicMap::new();
        instruments.insert(order.asset_id, instrument);
        let snapshot = build_reconciliation_snapshot(
            &[order],
            &[trade],
            &[],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a failed trade is a terminal venue answer, not missing evidence");

        // A failed trade never becomes a fill, and the order report it belongs to reports the
        // confirmed quantity only. That lower filled quantity is what lets a consumer void a
        // provisional fill applied before the trade failed, so the snapshot must still be
        // produced rather than withheld.
        assert!(snapshot.fills.is_empty());
        assert_eq!(snapshot.orders.len(), 1);
        assert!(snapshot.orders[0].filled_qty.is_zero());
    }

    #[rstest]
    fn reconciliation_snapshot_withholds_instrument_with_invalid_open_position() {
        let (instruments, _, other_id) = traded_instruments();
        let mut invalid = position_on(TRADED_TOKEN, Decimal::TEN);
        invalid.avg_price = None;

        let snapshot = build_reconciliation_snapshot(
            &[],
            &[],
            &[invalid, position_on(OTHER_TRADED_TOKEN, Decimal::TEN)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an unusable row withholds its instrument rather than failing the snapshot");

        // Negative control: the row names an instrument this client trades and cannot be
        // parsed, so that instrument stays out of the snapshot.
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].instrument_id, other_id);
    }

    #[rstest]
    fn reconciliation_snapshot_ignores_open_position_for_instrument_not_traded() {
        let (instruments, _, _) = traded_instruments();

        let snapshot = build_reconciliation_snapshot(
            &[],
            &[],
            &[position_on(FOREIGN_TOKEN, Decimal::TEN)],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a position in a market this client does not trade is not its evidence");

        assert!(snapshot.positions.is_empty());
    }

    #[rstest]
    fn reconciliation_snapshot_rejects_account_evidence_without_an_execution_lookup() {
        let error = build_reconciliation_snapshot(
            &[order_on(TRADED_TOKEN, "V-TRADED")],
            &[],
            &[position_on(TRADED_TOKEN, Decimal::TEN)],
            &AtomicMap::new(),
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("an empty execution lookup cannot make the venue's evidence foreign");

        // Instruments that failed to load leave every row foreign, which would state an empty
        // and therefore vacuously authoritative snapshot: a consumer reads that as a flat
        // account. It is a load failure, not evidence that the venue holds nothing.
        assert_eq!(
            error.to_string(),
            "Mass status cannot be stated: the execution lookup holds no instruments while the venue reports 2 row(s) for the account",
        );
    }

    #[rstest]
    fn reconciliation_snapshot_allows_an_empty_execution_lookup_without_evidence() {
        let snapshot = build_reconciliation_snapshot(
            &[],
            &[],
            &[],
            &AtomicMap::new(),
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an account the venue reports nothing for is genuinely flat");

        assert!(snapshot.orders.is_empty());
        assert!(snapshot.fills.is_empty());
        assert!(snapshot.positions.is_empty());
    }

    #[rstest]
    fn reconciliation_snapshot_withholds_filled_order_without_confirmed_fill() {
        let (instruments, _, other_id) = traded_instruments();
        let mut order = order_on(TRADED_TOKEN, "V-MATCHED");
        order.status = crate::common::enums::PolymarketOrderStatus::Matched;
        order.size_matched = order.original_size;

        let snapshot = build_reconciliation_snapshot(
            &[order, order_on(OTHER_TRADED_TOKEN, "V-LIVE")],
            &[],
            &[],
            &instruments,
            &OrderFillTrackerMap::new(),
            &test_fill_context(),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an unusable order withholds its instrument rather than failing the snapshot");

        // Negative control: a filled order with no confirmed fill behind it is malformed state
        // for an instrument this client trades, so it is never emitted.
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].instrument_id, other_id);
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

        cap_order_reports_to_confirmed_fills(
            &mut reports,
            &fills,
            &mut ReconciliationOmissions::default(),
        )
        .unwrap();

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
    }

    #[rstest]
    fn confirmed_fill_overflow_rejects_order_report() {
        let mut report = OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            VenueOrderId::from("V-OVERFLOW"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from("5.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        );

        let result =
            cap_order_report_filled_qty(&mut report, Quantity::zero(4), Some(Decimal::MAX));

        assert_eq!(result, Err(ReportParseError::FilledQuantity));
    }

    #[rstest]
    fn incomplete_confirmed_fill_rejects_filled_order_report() {
        let mut report = OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            VenueOrderId::from("V-INCOMPLETE"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("10.0000"),
            Quantity::from("10.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        );

        let result =
            cap_order_report_filled_qty(&mut report, Quantity::zero(4), Some(Decimal::from(5)));

        assert_eq!(result, Err(ReportParseError::FilledQuantity));
    }

    #[rstest]
    #[case::below_threshold("99.995", Some("99.995"))]
    #[case::at_threshold("99.990", None)]
    fn normalizes_confirmed_dust_residual_to_order_quantity(
        #[case] confirmed: &str,
        #[case] expected_quantity: Option<&str>,
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

        let mut omissions = ReconciliationOmissions::default();
        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &mut omissions).unwrap();
        let invalid = omissions.count(
            OmissionScope::Instrument(instrument_id),
            ReconciliationOmission::InvalidOrder(ReportParseError::FilledQuantity),
        );

        if let Some(expected_quantity) = expected_quantity {
            assert_eq!(invalid, 0);
            assert_eq!(reports[0].quantity, Quantity::from(expected_quantity));
            assert_eq!(reports[0].filled_qty, Quantity::from(confirmed));
        } else {
            assert_eq!(invalid, 1);
            assert!(reports.is_empty());
        }
    }
}
