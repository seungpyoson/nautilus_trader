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

//! Per-order fill tracking with terminal quantity normalization for the Polymarket adapter.

use std::sync::Mutex;

use ahash::{AHashMap, AHashSet};
use nautilus_common::cache::fifo::{FifoCache, FifoCacheMap};
use nautilus_core::MUTEX_POISONED;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide},
    events::OrderFilled,
    identifiers::{AccountId, InstrumentId, TradeId, VenueOrderId},
    reports::FillReport,
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{common::consts::DUST_SNAP_THRESHOLD_DEC, execution::identity::OrderIdentity};

/// Cumulative fill state for a single order.
#[derive(Debug, Clone)]
struct OrderFillState {
    submitted_qty: Quantity,
    cumulative_filled: Quantity,
    order_side: OrderSide,
    trade_keys: AHashSet<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FillCorrectionMetadata {
    pub correction_key: String,
    pub venue_trade_id: String,
    pub is_confirmed: bool,
}

pub(crate) enum PreparedTradeFill {
    Tracked {
        report: FillReport,
        identity: OrderIdentity,
        event: Box<OrderFilled>,
    },
    Anonymous(FillReport),
}

impl PreparedTradeFill {
    fn report(&self) -> &FillReport {
        match self {
            Self::Tracked { report, .. } | Self::Anonymous(report) => report,
        }
    }
}

pub(crate) enum ReadyTradeFill {
    Tracked {
        identity: OrderIdentity,
        event: Box<OrderFilled>,
        quantity_update: Option<Quantity>,
    },
    Anonymous(FillReport),
}

pub(crate) enum TradeFillApplication {
    AlreadyProcessed,
    Deferred,
    Confirmed(Vec<OrderFilled>),
    Ready(Vec<ReadyTradeFill>),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeSettlement {
    Confirmed,
    Voided,
}

#[derive(Debug)]
struct SettledTradeCorrection {
    #[cfg(test)]
    settlement: TradeSettlement,
    evidence: Option<Vec<TradeFillEvidence>>,
}

impl SettledTradeCorrection {
    fn confirmed(evidence: Vec<TradeFillEvidence>) -> Self {
        Self {
            #[cfg(test)]
            settlement: TradeSettlement::Confirmed,
            evidence: Some(evidence),
        }
    }

    fn voided(evidence: Option<Vec<TradeFillEvidence>>) -> Self {
        Self {
            #[cfg(test)]
            settlement: TradeSettlement::Voided,
            evidence,
        }
    }

    fn ensure_matches(&self, evidence: &[TradeFillEvidence]) -> anyhow::Result<()> {
        if let Some(expected) = &self.evidence {
            anyhow::ensure!(
                expected == evidence,
                "Conflicting evidence for a settled Polymarket trade correction"
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
struct TradeCorrectionState {
    fills: Vec<OrderFilled>,
    evidence: Vec<TradeFillEvidence>,
}

/// Canonical venue-authored execution fields used across pending, confirmed,
/// and event-store-restored correction state.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TradeFillEvidence {
    account_id: AccountId,
    instrument_id: InstrumentId,
    venue_order_id: VenueOrderId,
    trade_id: TradeId,
    order_side: OrderSide,
    last_qty: Quantity,
    last_px: Price,
    commission: Option<Money>,
    liquidity_side: LiquiditySide,
}

impl TradeFillEvidence {
    fn from_report(report: &FillReport) -> Self {
        Self {
            account_id: report.account_id,
            instrument_id: report.instrument_id,
            venue_order_id: report.venue_order_id,
            trade_id: report.trade_id,
            order_side: report.order_side,
            last_qty: report.last_qty,
            last_px: report.last_px,
            commission: Some(report.commission),
            liquidity_side: report.liquidity_side,
        }
    }

    fn from_event(event: &OrderFilled) -> Self {
        Self {
            account_id: event.account_id,
            instrument_id: event.instrument_id,
            venue_order_id: event.venue_order_id,
            trade_id: event.trade_id,
            order_side: event.order_side,
            last_qty: event.last_qty,
            last_px: event.last_px,
            commission: event.commission,
            liquidity_side: event.liquidity_side,
        }
    }
}

impl TradeCorrectionState {
    fn from_live(fills: Vec<OrderFilled>, evidence: Vec<TradeFillEvidence>) -> Self {
        Self { fills, evidence }
    }

    fn from_restored(fills: Vec<OrderFilled>) -> Self {
        let evidence = canonical_trade_evidence(fills.iter().map(TradeFillEvidence::from_event));
        Self { fills, evidence }
    }

    fn ensure_matches(&self, evidence: &[TradeFillEvidence]) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.evidence == evidence,
            "Conflicting evidence for an existing Polymarket trade correction"
        );
        Ok(())
    }
}

/// Active order state and bounded settlement history under one mutex.
#[derive(Debug, Default)]
struct TrackerInner {
    orders: AHashMap<VenueOrderId, OrderFillState>,
    retired_orders: FifoCacheMap<VenueOrderId, AHashSet<String>, 10_000>,
    active_corrections: AHashMap<String, TradeCorrectionState>,
    settled_corrections: FifoCacheMap<String, SettledTradeCorrection, 10_000>,
    confirmed_venue_trades: FifoCache<String, 10_000>,
    voiding_orders: AHashSet<VenueOrderId>,
}

/// Tracks per-order fill accumulation and correction settlement.
///
/// Thread-safe: a single internal `Mutex<TrackerInner>` is safe to share via `Arc` across the WS
/// task and spawned order submission tasks.
#[derive(Debug)]
pub(crate) struct OrderFillTrackerMap {
    inner: Mutex<TrackerInner>,
}

impl OrderFillTrackerMap {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(TrackerInner::default()),
        }
    }

    pub(crate) fn restore_order(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        filled_qty: Quantity,
        order_side: OrderSide,
    ) {
        let mut state = new_order_state(submitted_qty, order_side);
        state.cumulative_filled = filled_qty;
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        guard.retired_orders.remove(&venue_order_id);
        guard.orders.insert(venue_order_id, state);
    }

    pub(crate) fn register_reconciled_order(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        filled_qty: Quantity,
        order_side: OrderSide,
    ) {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let mut state = new_order_state(submitted_qty, order_side);
        state.cumulative_filled = filled_qty;
        guard.retired_orders.remove(&venue_order_id);
        guard.orders.insert(venue_order_id, state);
    }

    pub(crate) fn retire_order(&self, venue_order_id: VenueOrderId) {
        retire_order_in(
            &mut self.inner.lock().expect(MUTEX_POISONED),
            venue_order_id,
        );
    }

    /// Returns true if the order has been registered (accepted).
    pub(crate) fn contains(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .is_some()
    }

    /// Returns true if the order has received any fills or was explicitly retired.
    pub(crate) fn has_fills_or_settled(&self, venue_order_id: &VenueOrderId) -> bool {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard.voiding_orders.contains(venue_order_id) {
            return true;
        }
        guard
            .orders
            .get(venue_order_id)
            .is_some_and(|state| !state.cumulative_filled.is_zero())
            || guard.retired_orders.contains_key(venue_order_id)
    }

    /// Returns the cumulative filled quantity for an order, if tracked.
    pub(crate) fn get_cumulative_filled(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|s| s.cumulative_filled)
    }

    /// Returns `true` if cumulative fills have reached the submitted quantity.
    pub(crate) fn is_fully_filled(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .is_some_and(|s| s.cumulative_filled >= s.submitted_qty)
    }

    /// Applies every fill leg from one venue trade under a single tracker lock.
    ///
    /// A settlement-pending fill without a captured local identity cannot be
    /// reversed if emitted as a raw report. If any such leg already belongs to
    /// a registered order, the entire trade is deferred before cumulative
    /// quantity or buffers are changed. Confirmed trades may emit anonymous
    /// reports because they are no longer correction-eligible.
    pub(crate) fn apply_trade_fills_atomically(
        &self,
        fills: Vec<PreparedTradeFill>,
        correction: FillCorrectionMetadata,
    ) -> anyhow::Result<TradeFillApplication> {
        let fills = normalize_prepared_trade_fills(fills)?;
        let evidence = canonical_trade_evidence(
            fills
                .iter()
                .map(|fill| TradeFillEvidence::from_report(fill.report())),
        );
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let correction_key = &correction.correction_key;
        if let Some(settled) = guard.settled_corrections.get(correction_key) {
            settled.ensure_matches(&evidence)?;
            return Ok(TradeFillApplication::AlreadyProcessed);
        }
        if let Some(active) = guard.active_corrections.get(correction_key) {
            active.ensure_matches(&evidence)?;
            if !correction.is_confirmed {
                return Ok(TradeFillApplication::AlreadyProcessed);
            }
            let active = guard
                .active_corrections
                .remove(correction_key)
                .ok_or_else(|| anyhow::anyhow!("Active trade correction disappeared under lock"))?;
            let fills = active.fills;
            guard.settled_corrections.insert(
                correction.correction_key,
                SettledTradeCorrection::confirmed(active.evidence),
            );
            guard.confirmed_venue_trades.add(correction.venue_trade_id);
            return Ok(TradeFillApplication::Confirmed(fills));
        }
        let already_processed = fills.iter().any(|fill| {
            let venue_order_id = &fill.report().venue_order_id;
            guard
                .orders
                .get(venue_order_id)
                .is_some_and(|order| order.trade_keys.contains(correction_key))
                || guard
                    .retired_orders
                    .get(venue_order_id)
                    .is_some_and(|trade_keys| trade_keys.contains(correction_key))
        });
        if already_processed {
            return Ok(TradeFillApplication::AlreadyProcessed);
        }

        let has_irreversible_leg = !correction.is_confirmed
            && fills
                .iter()
                .any(|fill| matches!(fill, PreparedTradeFill::Anonymous(_)));
        if has_irreversible_leg {
            return Ok(TradeFillApplication::Deferred);
        }

        let mut projected_by_order = AHashMap::<VenueOrderId, Quantity>::new();
        let mut cumulative_after_fill = Vec::with_capacity(fills.len());
        for fill in &fills {
            let venue_order_id = fill.report().venue_order_id;
            let initial = projected_by_order
                .get(&venue_order_id)
                .copied()
                .or_else(|| {
                    guard
                        .orders
                        .get(&venue_order_id)
                        .map(|state| state.cumulative_filled)
                })
                .or_else(|| match fill {
                    PreparedTradeFill::Tracked { identity, .. } => {
                        Some(Quantity::zero(identity.quantity.precision))
                    }
                    PreparedTradeFill::Anonymous(_) => None,
                });
            let projected = initial
                .map(|quantity| {
                    quantity.checked_add(fill.report().last_qty).ok_or_else(|| {
                        anyhow::anyhow!("Fill quantity overflows for venue order {venue_order_id}")
                    })
                })
                .transpose()?;
            if let Some(projected) = projected {
                projected_by_order.insert(venue_order_id, projected);
            }
            cumulative_after_fill.push(projected);
        }

        let mut ready = Vec::with_capacity(fills.len());
        let mut applied = Vec::new();
        for (fill, cumulative_filled) in fills.into_iter().zip(cumulative_after_fill) {
            let venue_order_id = fill.report().venue_order_id;
            match fill {
                PreparedTradeFill::Tracked {
                    report: _,
                    identity,
                    event,
                } => {
                    guard
                        .orders
                        .entry(venue_order_id)
                        .or_insert_with(|| new_order_state(identity.quantity, identity.order_side));
                    let order = guard
                        .orders
                        .get_mut(&venue_order_id)
                        .expect("tracked fill registered its order");
                    order.cumulative_filled =
                        cumulative_filled.expect("tracked fill cumulative quantity was projected");
                    order.trade_keys.insert(correction.correction_key.clone());
                    let quantity_update = buy_overfill_bump_in(&mut guard.orders, &venue_order_id);
                    applied.push((*event).clone());
                    ready.push(ReadyTradeFill::Tracked {
                        identity,
                        event,
                        quantity_update,
                    });
                }
                PreparedTradeFill::Anonymous(report) => {
                    if let Some(order) = guard.orders.get_mut(&venue_order_id) {
                        order.cumulative_filled = cumulative_filled
                            .expect("tracked anonymous fill quantity was projected");
                        order.trade_keys.insert(correction.correction_key.clone());
                    }
                    ready.push(ReadyTradeFill::Anonymous(report));
                }
            }
        }
        if correction.is_confirmed {
            guard.settled_corrections.insert(
                correction.correction_key,
                SettledTradeCorrection::confirmed(evidence),
            );
            guard.confirmed_venue_trades.add(correction.venue_trade_id);
        } else {
            guard.active_corrections.insert(
                correction.correction_key,
                TradeCorrectionState::from_live(applied, evidence),
            );
        }
        Ok(TradeFillApplication::Ready(ready))
    }

    /// Reverses every emitted leg of a failed trade as one correction transaction.
    ///
    /// Orders remain marked as correction-in-progress until every void event has been emitted, so
    /// concurrent terminal-status checks cannot observe zero tracker quantity before the engine
    /// has received the corresponding reversals.
    pub(crate) fn void_trade_atomically<F>(
        &self,
        correction_key: &str,
        venue_trade_id: &str,
        trade_venue_order_ids: &[VenueOrderId],
        emit: F,
    ) -> bool
    where
        F: FnOnce(Vec<OrderFilled>),
    {
        let key = correction_key.to_string();
        let (fills, venue_order_ids) = {
            let mut guard = self.inner.lock().expect(MUTEX_POISONED);
            if guard.settled_corrections.contains_key(&key) {
                return false;
            }
            for venue_order_id in trade_venue_order_ids {
                if let Some(order) = guard.orders.get_mut(venue_order_id) {
                    order.trade_keys.insert(key.clone());
                }
            }
            let (fills, settled) = match guard.active_corrections.remove(&key) {
                Some(trade) => (
                    trade.fills,
                    SettledTradeCorrection::voided(Some(trade.evidence)),
                ),
                None => (Vec::new(), SettledTradeCorrection::voided(None)),
            };
            guard.settled_corrections.insert(key, settled);
            guard
                .confirmed_venue_trades
                .remove(&venue_trade_id.to_string());
            let mut venue_order_ids = Vec::with_capacity(fills.len());
            for fill in &fills {
                reverse_fill_in(&mut guard.orders, &fill.venue_order_id, fill.last_qty);
                if !venue_order_ids.contains(&fill.venue_order_id) {
                    venue_order_ids.push(fill.venue_order_id);
                    guard.voiding_orders.insert(fill.venue_order_id);
                }
            }
            (fills, venue_order_ids)
        };

        emit(fills);

        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        for venue_order_id in venue_order_ids {
            guard.voiding_orders.remove(&venue_order_id);
        }
        true
    }

    pub(crate) fn restore_matched_trade(&self, key: String, fills: Vec<OrderFilled>) {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        for fill in &fills {
            if let Some(order) = guard.orders.get_mut(&fill.venue_order_id) {
                order.trade_keys.insert(key.clone());
            }
        }
        guard
            .active_corrections
            .insert(key, TradeCorrectionState::from_restored(fills));
    }

    pub(crate) fn restore_voided_trade(&self, key: String, fills: Vec<OrderFilled>) {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let evidence = canonical_trade_evidence(fills.iter().map(TradeFillEvidence::from_event));
        for fill in fills {
            if let Some(order) = guard.orders.get_mut(&fill.venue_order_id) {
                order.trade_keys.insert(key.clone());
            }
        }
        guard
            .settled_corrections
            .insert(key, SettledTradeCorrection::voided(Some(evidence)));
    }

    pub(crate) fn restore_confirmed_trade(
        &self,
        key: String,
        venue_trade_id: String,
        fills: Vec<OrderFilled>,
    ) {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let evidence = canonical_trade_evidence(fills.iter().map(TradeFillEvidence::from_event));
        for fill in fills {
            if let Some(order) = guard.orders.get_mut(&fill.venue_order_id) {
                order.trade_keys.insert(key.clone());
            }
        }
        guard
            .settled_corrections
            .insert(key, SettledTradeCorrection::confirmed(evidence));
        guard.confirmed_venue_trades.add(venue_trade_id);
    }

    pub(crate) fn reset(&self) {
        *self.inner.lock().expect(MUTEX_POISONED) = TrackerInner::default();
    }

    #[cfg(test)]
    fn active_correction_count(&self) -> usize {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .active_corrections
            .len()
    }

    #[cfg(test)]
    fn tracked_order_count(&self) -> usize {
        self.inner.lock().expect(MUTEX_POISONED).orders.len()
    }

    #[cfg(test)]
    pub(crate) fn is_voided_trade(&self, key: &str) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .settled_corrections
            .get(&key.to_string())
            .is_some_and(|settlement| settlement.settlement == TradeSettlement::Voided)
    }

    #[cfg(test)]
    pub(crate) fn is_trade_processed(&self, correction_key: &str) -> bool {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let key = correction_key.to_string();
        guard.active_corrections.contains_key(&key) || guard.settled_corrections.contains_key(&key)
    }

    pub(crate) fn are_venue_trades_confirmed(&self, venue_trade_ids: &[String]) -> bool {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        venue_trade_ids
            .iter()
            .all(|trade_id| guard.confirmed_venue_trades.contains(trade_id))
    }

    /// Snap each report's `last_qty` against the registered submitted quantity
    /// for its `venue_order_id`. Reports for orders the tracker does not know
    /// about (e.g. orders from another session) pass through unchanged.
    ///
    /// Commission is intentionally not recomputed: it tracks the venue charge
    /// from the on-chain fill, which is independent of our local snap.
    pub(crate) fn snap_fill_reports(&self, reports: &mut [FillReport]) {
        let guard = self.inner.lock().expect(MUTEX_POISONED);

        for report in reports {
            report.last_qty =
                snap_fill_qty_in(&guard.orders, &report.venue_order_id, report.last_qty);
        }
    }

    /// Snap a single fill qty DOWN to `submitted_qty` when the venue reports
    /// dust overfill (within `DUST_SNAP_THRESHOLD_DEC`).
    ///
    /// Overfill snapping is required because the engine rejects fills past
    /// `submitted_qty`. Underfill is intentionally left alone here: a single
    /// partial fill that happens to land near submitted_qty might still be
    /// followed by additional matches, or the order might end up canceled
    /// with the dust remaining as legitimate leaves. Terminal quantity
    /// normalization handles the CLOB cent-tick truncation
    /// case after all associated trades confirm.
    ///
    /// See `docs/integrations/polymarket.md` (Fill quantity normalization).
    pub(crate) fn snap_fill_qty(
        &self,
        venue_order_id: &VenueOrderId,
        fill_qty: Quantity,
    ) -> Quantity {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        snap_fill_qty_in(&guard.orders, venue_order_id, fill_qty)
    }

    /// Raise the registered quantity to the cumulative BUY fills when they exceed it, returning
    /// the new quantity to emit via `OrderUpdated` (or `None` when no raise is needed).
    ///
    /// A Polymarket BUY is bounded by the USDC it spends (`makerAmount`), so a marketable fill
    /// below the limit price returns more shares than the nominal quantity. The engine rejects a
    /// fill past the order quantity, so the quantity is raised to the actual fill before the
    /// `OrderFilled` applies. SELL orders are share-denominated and never overfill, so they always
    /// return `None`. Dust overfills are handled earlier by `snap_fill_qty`, so only a gross
    /// overfill reaches here.
    ///
    /// Raising `submitted_qty` to exactly the cumulative fill makes the following `OrderFilled`
    /// reach `Filled`. That is correct because an overfill only ever occurs on a marketable taker
    /// BUY, whose fill the venue reports as a single aggregated trade event (one `FillReport` per
    /// taker order): the bumping fill is therefore terminal, with no later fill to strand. Passive
    /// maker BUYs can fill across several events but execute at their own price, so they never
    /// overfill and never reach this raise. A venue that split one marketable BUY across multiple
    /// trade events would close the order on the first crossing fill; this is not Polymarket's
    /// observed behaviour and would need a final-fill signal to handle.
    /// Returns the venue-filled quantity when a terminal order has sub-cent-share leaves.
    ///
    /// The returned quantity is used for an order-only reconciliation update. It is not a fill and
    /// must not change positions, balances, or commissions. The entry is removed on normalization
    /// so repeated terminal messages are idempotent.
    pub(crate) fn check_terminal_quantity_normalization(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let state = guard.orders.get(venue_order_id)?.clone();
        if state.cumulative_filled >= state.submitted_qty {
            retire_order_in(&mut guard, *venue_order_id);
            return None;
        }
        let leaves = state.submitted_qty.as_decimal() - state.cumulative_filled.as_decimal();

        if leaves > Decimal::ZERO && leaves < DUST_SNAP_THRESHOLD_DEC {
            let filled_qty = state.cumulative_filled;

            log::debug!(
                "Normalizing terminal order {venue_order_id} quantity from {} to {filled_qty} \
                 (non-economic leaves={leaves})",
                state.submitted_qty,
            );
            retire_order_in(&mut guard, *venue_order_id);
            Some(filled_qty)
        } else {
            if leaves >= DUST_SNAP_THRESHOLD_DEC {
                log::debug!(
                    "Order {venue_order_id} MATCHED with significant residual \
                     {leaves} (filled {}/{})",
                    state.cumulative_filled,
                    state.submitted_qty,
                );
            }
            None
        }
    }

    /// Returns the real unfilled remainder of a terminal IOC order.
    ///
    /// The entry is removed so duplicate `CONFIRMED` trade messages cannot emit repeated
    /// cancellations. The caller must use this only after a taker trade confirms: that proves the
    /// FAK order has finished matching and the venue has killed the returned remainder.
    pub(crate) fn take_terminal_ioc_remainder(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let state = guard.orders.get(venue_order_id)?.clone();
        if state.cumulative_filled.is_zero() {
            return None;
        }
        if state.cumulative_filled >= state.submitted_qty {
            retire_order_in(&mut guard, *venue_order_id);
            return None;
        }

        let remainder = state.submitted_qty - state.cumulative_filled;
        retire_order_in(&mut guard, *venue_order_id);
        Some(remainder)
    }
}

fn normalize_prepared_trade_fills(
    fills: Vec<PreparedTradeFill>,
) -> anyhow::Result<Vec<PreparedTradeFill>> {
    let mut evidence_by_key = AHashMap::new();
    let mut unique = Vec::with_capacity(fills.len());

    for fill in fills {
        let evidence = TradeFillEvidence::from_report(fill.report());
        let key = (evidence.venue_order_id, evidence.trade_id);
        if let Some(previous) = evidence_by_key.get(&key) {
            anyhow::ensure!(
                previous == &evidence,
                "Conflicting duplicate execution {} for venue order {}",
                evidence.trade_id,
                evidence.venue_order_id,
            );
            continue;
        }
        evidence_by_key.insert(key, evidence);
        unique.push(fill);
    }

    Ok(unique)
}

fn canonical_trade_evidence(
    evidence: impl IntoIterator<Item = TradeFillEvidence>,
) -> Vec<TradeFillEvidence> {
    let mut evidence = evidence.into_iter().collect::<Vec<_>>();
    evidence.sort_by(|left, right| {
        left.venue_order_id
            .as_str()
            .cmp(right.venue_order_id.as_str())
            .then_with(|| left.trade_id.as_str().cmp(right.trade_id.as_str()))
    });
    evidence
}

fn retire_order_in(inner: &mut TrackerInner, venue_order_id: VenueOrderId) {
    let mut trade_keys = inner
        .retired_orders
        .remove(&venue_order_id)
        .unwrap_or_default();
    if let Some(order) = inner.orders.remove(&venue_order_id) {
        trade_keys.extend(order.trade_keys);
    }
    inner.retired_orders.insert(venue_order_id, trade_keys);
}

fn new_order_state(submitted_qty: Quantity, order_side: OrderSide) -> OrderFillState {
    OrderFillState {
        submitted_qty,
        cumulative_filled: Quantity::zero(submitted_qty.precision),
        order_side,
        trade_keys: AHashSet::new(),
    }
}

fn buy_overfill_bump_in(
    orders: &mut AHashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
) -> Option<Quantity> {
    let state = orders.get_mut(venue_order_id)?;
    if state.order_side != OrderSide::Buy {
        return None;
    }

    if state.cumulative_filled > state.submitted_qty {
        state.submitted_qty = state.cumulative_filled;
        Some(state.cumulative_filled)
    } else {
        None
    }
}

#[cfg(test)]
impl OrderFillTrackerMap {
    /// Registers an order directly, for tests that set up an already-accepted order.
    pub(crate) fn register(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        order_side: OrderSide,
        _instrument_id: InstrumentId,
        _size_precision: u8,
        _price_precision: u8,
    ) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .insert(venue_order_id, new_order_state(submitted_qty, order_side));
    }

    /// Returns the registered submitted quantity for an order, if tracked.
    pub(crate) fn submitted_qty(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|s| s.submitted_qty)
    }

    /// Records a fill against a registered order, for tests that drive fill accumulation directly.
    #[cfg(test)]
    pub(crate) fn record_fill(&self, venue_order_id: &VenueOrderId, qty: Quantity) {
        record_fill_in(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            qty,
        );
    }

    pub(crate) fn buy_overfill_bump(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        buy_overfill_bump_in(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
        )
    }

    #[cfg(test)]
    pub(crate) fn is_trade_confirmed(&self, correction_key: &str) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .settled_corrections
            .get(&correction_key.to_string())
            .is_some_and(|settlement| settlement.settlement == TradeSettlement::Confirmed)
    }
}

#[cfg(test)]
fn record_fill_in(
    orders: &mut AHashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
    qty: Quantity,
) {
    if let Some(s) = orders.get_mut(venue_order_id) {
        s.cumulative_filled = s
            .cumulative_filled
            .checked_add(qty)
            .expect("test fill quantity must be representable");
    }
}

fn reverse_fill_in(
    orders: &mut AHashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
    qty: Quantity,
) {
    if let Some(state) = orders.get_mut(venue_order_id) {
        state.cumulative_filled = if qty >= state.cumulative_filled {
            Quantity::zero(state.cumulative_filled.precision)
        } else {
            state.cumulative_filled - qty
        };
    }
}

fn snap_fill_qty_in(
    orders: &AHashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
    fill_qty: Quantity,
) -> Quantity {
    match orders.get(venue_order_id) {
        Some(s) => {
            let diff = s.submitted_qty.as_decimal() - fill_qty.as_decimal();
            if diff < Decimal::ZERO && diff.abs() < DUST_SNAP_THRESHOLD_DEC {
                log::debug!(
                    "Snapping overfill {fill_qty} -> {} (dust={diff})",
                    s.submitted_qty,
                );
                s.submitted_qty
            } else {
                fill_qty
            }
        }
        None => fill_qty,
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{LiquiditySide, OrderType, TimeInForce},
        identifiers::{AccountId, ClientOrderId, StrategyId, TradeId, TraderId},
        types::{Currency, Money, Price, quantity::QUANTITY_RAW_MAX},
    };
    use rstest::rstest;

    use super::*;

    fn pusd() -> Currency {
        Currency::pUSD()
    }

    fn tracked_fill_evidence(
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        trade_id: &str,
        quantity: Quantity,
    ) -> (FillReport, OrderIdentity, Box<OrderFilled>) {
        let report = FillReport {
            account_id: AccountId::from("POLY-001"),
            instrument_id,
            venue_order_id,
            trade_id: TradeId::from(trade_id),
            order_side: OrderSide::Buy,
            last_qty: quantity,
            last_px: Price::new(0.55, 2),
            commission: Money::zero(pusd()),
            liquidity_side: LiquiditySide::Taker,
            avg_px: None,
            report_id: UUID4::new(),
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
            client_order_id: None,
            venue_position_id: None,
        };
        let identity = OrderIdentity {
            client_order_id: ClientOrderId::from("O-TRACKED"),
            strategy_id: StrategyId::from("S-001"),
            instrument_id,
            order_side: OrderSide::Buy,
            quantity,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Fok,
        };
        let event = OrderFilled::new(
            TraderId::from("TESTER-001"),
            identity.strategy_id,
            instrument_id,
            identity.client_order_id,
            venue_order_id,
            report.account_id,
            report.trade_id,
            report.order_side,
            identity.order_type,
            report.last_qty,
            report.last_px,
            pusd(),
            report.liquidity_side,
            UUID4::new(),
            report.ts_event,
            report.ts_init,
            false,
            None,
            Some(report.commission),
            None,
        );
        (report, identity, Box::new(event))
    }

    #[rstest]
    fn test_register_and_contains() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        assert!(!tracker.contains(&vid));
        assert!(!tracker.has_fills_or_settled(&vid));

        tracker.register(
            vid,
            Quantity::from("100"),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );
        assert!(tracker.contains(&vid));
    }

    #[rstest]
    fn test_active_orders_are_not_evicted_by_registration_volume() {
        let tracker = OrderFillTrackerMap::new();
        let durable_id = VenueOrderId::from("order-durable-active");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            durable_id,
            Quantity::from("10.000000"),
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );

        for index in 0..10_001 {
            tracker.register(
                VenueOrderId::from(format!("order-churn-{index}")),
                Quantity::from("1.000000"),
                OrderSide::Buy,
                instrument_id,
                6,
                2,
            );
        }
        tracker.record_fill(&durable_id, Quantity::from("4.000000"));

        assert!(tracker.contains(&durable_id));
        assert_eq!(
            tracker.get_cumulative_filled(&durable_id),
            Some(Quantity::from("4.000000"))
        );
    }

    #[rstest]
    fn test_terminal_order_churn_does_not_retain_active_state() {
        let tracker = OrderFillTrackerMap::new();
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");

        for index in 0..10_001 {
            let venue_order_id = VenueOrderId::from(format!("terminal-{index}").as_str());
            tracker.register(
                venue_order_id,
                Quantity::from("1.000000"),
                OrderSide::Buy,
                instrument_id,
                6,
                2,
            );
            tracker.retire_order(venue_order_id);
        }

        assert_eq!(tracker.tracked_order_count(), 0);
    }

    #[rstest]
    fn test_reopened_order_replaces_retired_tombstone() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("reopened");
        tracker.restore_order(
            venue_order_id,
            Quantity::from("10.000000"),
            Quantity::from("4.000000"),
            OrderSide::Buy,
        );
        tracker.retire_order(venue_order_id);
        tracker.restore_order(
            venue_order_id,
            Quantity::from("10.000000"),
            Quantity::from("3.000000"),
            OrderSide::Buy,
        );

        assert!(tracker.contains(&venue_order_id));
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::from("3.000000"))
        );
    }

    #[rstest]
    fn test_retired_order_rejects_known_trade_after_global_correction_eviction() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("retired-known-trade");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let quantity = Quantity::from("1.000000");
        let correction_key = "retired-known-trade-key";
        tracker.register(
            venue_order_id,
            quantity,
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let (report, identity, event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "retired-known-trade",
            quantity,
        );
        let applied = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report: report.clone(),
                    identity: identity.clone(),
                    event: event.clone(),
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "retired-known-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();
        assert!(matches!(applied, TradeFillApplication::Ready(_)));
        tracker.retire_order(venue_order_id);

        for index in 0..10_000 {
            let churn_venue_order_id =
                VenueOrderId::from(format!("correction-churn-{index}").as_str());
            let churn_trade_id = format!("correction-churn-trade-{index}");
            let (churn_report, _, _) = tracked_fill_evidence(
                churn_venue_order_id,
                instrument_id,
                &churn_trade_id,
                quantity,
            );
            tracker
                .apply_trade_fills_atomically(
                    vec![PreparedTradeFill::Anonymous(churn_report)],
                    FillCorrectionMetadata {
                        correction_key: format!("{churn_trade_id}-{churn_venue_order_id}"),
                        venue_trade_id: churn_trade_id,
                        is_confirmed: true,
                    },
                )
                .unwrap();
        }

        let replay = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "retired-known-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();

        assert!(matches!(replay, TradeFillApplication::AlreadyProcessed));
        assert!(!tracker.contains(&venue_order_id));
        assert_eq!(tracker.active_correction_count(), 0);
    }

    #[rstest]
    fn test_pending_anonymous_fill_is_deferred_without_retained_state() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("anonymous-pending");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let (report, _, _) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "anonymous-pending-trade",
            Quantity::from("1.000000"),
        );
        let correction_key = "anonymous-pending-trade-anonymous-pending";

        let result = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Anonymous(report)],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "anonymous-pending-trade".to_string(),
                    is_confirmed: false,
                },
            )
            .unwrap();

        assert!(matches!(result, TradeFillApplication::Deferred));
        assert!(!tracker.is_trade_processed(correction_key));
        assert_eq!(tracker.active_correction_count(), 0);
        assert!(!tracker.contains(&venue_order_id));
    }

    #[rstest]
    fn test_confirmed_anonymous_fill_is_ready_without_active_correction() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("anonymous-confirmed");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let (report, _, _) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "anonymous-confirmed-trade",
            Quantity::from("1.000000"),
        );
        let correction_key = "anonymous-confirmed-trade-anonymous-confirmed";

        let result = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Anonymous(report)],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "anonymous-confirmed-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();

        assert!(matches!(
            result,
            TradeFillApplication::Ready(ready)
                if matches!(ready.as_slice(), [ReadyTradeFill::Anonymous(_)])
        ));
        assert!(tracker.is_trade_processed(correction_key));
        assert_eq!(tracker.active_correction_count(), 0);
        assert!(!tracker.contains(&venue_order_id));
    }

    #[rstest]
    fn test_tracked_fill_registers_order_from_identity() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("tracked-self-register");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let fill_qty = Quantity::from("4.000000");
        let (report, mut identity, event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-self-register-trade",
            fill_qty,
        );
        identity.quantity = Quantity::from("10.000000");

        let result = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: "tracked-self-register-trade-tracked-self-register".to_string(),
                    venue_trade_id: "tracked-self-register-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();

        assert!(matches!(result, TradeFillApplication::Ready(ready) if ready.len() == 1));
        assert!(tracker.contains(&venue_order_id));
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(fill_qty)
        );
        assert_eq!(tracker.active_correction_count(), 0);
    }

    #[rstest]
    fn test_trade_fill_projection_rejects_quantity_overflow_without_mutation() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("tracked-overflow");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let submitted_qty = Quantity::from_raw(QUANTITY_RAW_MAX, 0);
        let half_plus_one = Quantity::from_raw((QUANTITY_RAW_MAX / 2) + 1, 0);
        tracker.register(
            venue_order_id,
            submitted_qty,
            OrderSide::Buy,
            instrument_id,
            0,
            2,
        );
        let (first_report, first_identity, first_event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-overflow-1",
            half_plus_one,
        );
        let (second_report, second_identity, second_event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-overflow-2",
            half_plus_one,
        );
        let correction_key = "tracked-overflow-correction";

        let result = tracker.apply_trade_fills_atomically(
            vec![
                PreparedTradeFill::Tracked {
                    report: first_report,
                    identity: first_identity,
                    event: first_event,
                },
                PreparedTradeFill::Tracked {
                    report: second_report,
                    identity: second_identity,
                    event: second_event,
                },
            ],
            FillCorrectionMetadata {
                correction_key: correction_key.to_string(),
                venue_trade_id: "tracked-overflow".to_string(),
                is_confirmed: true,
            },
        );

        assert!(result.is_err());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(0)),
        );
        assert!(!tracker.is_trade_processed(correction_key));
    }

    #[rstest]
    fn test_trade_correction_rejects_changed_confirmation_evidence_without_mutation() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("tracked-conflict");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::from("10.000000"),
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let correction_key = "tracked-conflict-correction";
        let (report, identity, event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-conflict-trade",
            Quantity::from("5.000000"),
        );
        let pending = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "tracked-conflict-trade".to_string(),
                    is_confirmed: false,
                },
            )
            .unwrap();
        assert!(matches!(pending, TradeFillApplication::Ready(ready) if ready.len() == 1));

        let (report, identity, event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-conflict-trade",
            Quantity::from("6.000000"),
        );
        let conflict = tracker.apply_trade_fills_atomically(
            vec![PreparedTradeFill::Tracked {
                report,
                identity,
                event,
            }],
            FillCorrectionMetadata {
                correction_key: correction_key.to_string(),
                venue_trade_id: "tracked-conflict-trade".to_string(),
                is_confirmed: true,
            },
        );

        assert!(conflict.is_err());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::from("5.000000")),
        );
        assert_eq!(tracker.active_correction_count(), 1);

        let (mut report, identity, mut event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-conflict-trade",
            Quantity::from("5.000000"),
        );
        report.ts_event = UnixNanos::from(1_000_000);
        event.ts_event = report.ts_event;
        let confirmed = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "tracked-conflict-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();
        assert!(matches!(confirmed, TradeFillApplication::Confirmed(_)));

        let (report, identity, event) = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-conflict-trade",
            Quantity::from("6.000000"),
        );
        let settled_conflict = tracker.apply_trade_fills_atomically(
            vec![PreparedTradeFill::Tracked {
                report,
                identity,
                event,
            }],
            FillCorrectionMetadata {
                correction_key: correction_key.to_string(),
                venue_trade_id: "tracked-conflict-trade".to_string(),
                is_confirmed: true,
            },
        );
        assert!(settled_conflict.is_err());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::from("5.000000")),
        );
    }

    #[rstest]
    fn test_equivalent_duplicate_fill_in_one_trade_is_applied_once() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("tracked-duplicate");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::from("10.000000"),
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let first = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-duplicate-trade",
            Quantity::from("5.000000"),
        );
        let second = tracked_fill_evidence(
            venue_order_id,
            instrument_id,
            "tracked-duplicate-trade",
            Quantity::from("5.000000"),
        );

        let result = tracker
            .apply_trade_fills_atomically(
                vec![
                    PreparedTradeFill::Tracked {
                        report: first.0,
                        identity: first.1,
                        event: first.2,
                    },
                    PreparedTradeFill::Tracked {
                        report: second.0,
                        identity: second.1,
                        event: second.2,
                    },
                ],
                FillCorrectionMetadata {
                    correction_key: "tracked-duplicate-correction".to_string(),
                    venue_trade_id: "tracked-duplicate-trade".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();

        assert!(matches!(result, TradeFillApplication::Ready(ready) if ready.len() == 1));
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::from("5.000000")),
        );
    }

    #[rstest]
    fn test_confirmed_anonymous_fill_churn_never_creates_active_corrections() {
        let tracker = OrderFillTrackerMap::new();
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");

        for index in 0..1_001 {
            let venue_order_id =
                VenueOrderId::from(format!("anonymous-confirmed-{index}").as_str());
            let trade_id = format!("anonymous-confirmed-trade-{index}");
            let (report, _, _) = tracked_fill_evidence(
                venue_order_id,
                instrument_id,
                &trade_id,
                Quantity::from("1.000000"),
            );
            let result = tracker
                .apply_trade_fills_atomically(
                    vec![PreparedTradeFill::Anonymous(report)],
                    FillCorrectionMetadata {
                        correction_key: format!("{trade_id}-{venue_order_id}"),
                        venue_trade_id: trade_id,
                        is_confirmed: true,
                    },
                )
                .unwrap();
            assert!(matches!(result, TradeFillApplication::Ready(ready) if ready.len() == 1));
        }

        assert_eq!(tracker.active_correction_count(), 0);
    }

    #[rstest]
    fn test_failed_trade_hides_zero_quantity_until_void_event_is_emitted() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-correcting");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let quantity = Quantity::new(5.0, 6);
        tracker.register(
            venue_order_id,
            quantity,
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let (report, identity, event) =
            tracked_fill_evidence(venue_order_id, instrument_id, "trade-correcting", quantity);
        let correction_key = "trade-correcting-order-correcting";

        let applied = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "trade-correcting".to_string(),
                    is_confirmed: false,
                },
            )
            .unwrap();
        assert!(matches!(applied, TradeFillApplication::Ready(ready) if ready.len() == 1));
        assert!(tracker.has_fills_or_settled(&venue_order_id));

        let mut emitted = false;
        assert!(tracker.void_trade_atomically(
            correction_key,
            "trade-correcting",
            &[venue_order_id],
            |fills| {
                assert_eq!(fills.len(), 1);
                assert!(tracker.has_fills_or_settled(&venue_order_id));
                emitted = true;
            }
        ));

        assert!(emitted);
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(6))
        );
        assert!(!tracker.has_fills_or_settled(&venue_order_id));
    }

    #[rstest]
    fn test_registered_order_dedup_survives_global_trade_history_eviction() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-durable-dedup");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let submitted_qty = Quantity::new(10.0, 6);
        let fill_qty = Quantity::new(5.0, 6);
        tracker.register(
            venue_order_id,
            submitted_qty,
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let correction_key = "trade-durable-order-durable-dedup";
        let (report, identity, event) =
            tracked_fill_evidence(venue_order_id, instrument_id, "trade-durable", fill_qty);
        let first = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "trade-durable".to_string(),
                    is_confirmed: false,
                },
            )
            .unwrap();
        assert!(matches!(first, TradeFillApplication::Ready(ready) if ready.len() == 1));
        let (report, identity, event) =
            tracked_fill_evidence(venue_order_id, instrument_id, "trade-durable", fill_qty);
        let confirmed = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "trade-durable".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();
        assert!(matches!(
            confirmed,
            TradeFillApplication::Confirmed(ref fills) if fills.len() == 1
        ));

        for index in 0..10_001 {
            tracker.restore_voided_trade(format!("unrelated-{index}"), Vec::new());
        }
        assert!(!tracker.is_trade_processed(correction_key));

        let (report, identity, event) =
            tracked_fill_evidence(venue_order_id, instrument_id, "trade-durable", fill_qty);
        let replay = tracker
            .apply_trade_fills_atomically(
                vec![PreparedTradeFill::Tracked {
                    report,
                    identity,
                    event,
                }],
                FillCorrectionMetadata {
                    correction_key: correction_key.to_string(),
                    venue_trade_id: "trade-durable".to_string(),
                    is_confirmed: true,
                },
            )
            .unwrap();

        assert!(matches!(replay, TradeFillApplication::AlreadyProcessed));
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(fill_qty)
        );
    }

    // snap_fill_qty is overfill-only. Underfill is preserved so partial fills
    // followed by cancel keep their venue-reported size; terminal quantity
    // normalization handles CLOB cent-tick truncation without a synthetic fill.
    #[rstest]
    // Underfill within the dust band: NOT snapped. The fill is recorded
    // as-is; terminal normalization later lowers the order quantity.
    #[case::underfill_dust_preserved(23.696681, 23.690000, 23.690000)]
    #[case::underfill_near_band_preserved(100.000000, 99.990100, 99.990100)]
    // Underfill at exactly the band: NOT snapped.
    #[case::underfill_at_band(100.000000, 99.990000, 99.990000)]
    // Underfill above the band: NOT snapped (real partial leaves).
    #[case::underfill_above_band(100.000000, 99.980000, 99.980000)]
    // Underfill far past band: NOT snapped.
    #[case::large_underfill(100.000000, 50.000000, 50.000000)]
    // Overfill within the band: V2 market BUY where the SDK truncates the
    // registered base qty to USDC scale but the on-chain fill comes back at
    // full precision. Observed production drift is 4-66 ulps. Snap DOWN so
    // the engine does not reject as overfill.
    #[case::overfill_dust(714.285710, 714.285714, 714.285710)]
    // Overfill near the band (0.0099 < 0.01): still snaps.
    #[case::overfill_near_band(100.000000, 100.009900, 100.000000)]
    // Overfill at exactly the band must NOT snap (exclusive boundary).
    #[case::overfill_at_band(100.000000, 100.010000, 100.010000)]
    // Overfill above the band: leave fill alone, surfaces as engine-side
    // error since this is no longer dust.
    #[case::overfill_above_band(100.000000, 100.020000, 100.020000)]
    // Overfill far past band: leave fill alone.
    #[case::large_overfill(100.000000, 150.000000, 150.000000)]
    // Exact match: no-op (returns the fill qty, which equals submitted).
    #[case::exact(100.000000, 100.000000, 100.000000)]
    fn test_snap_fill_qty(#[case] submitted: f64, #[case] fill: f64, #[case] expected: f64) {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-1");
        tracker.register(
            venue_order_id,
            Quantity::new(submitted, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        let snapped = tracker.snap_fill_qty(&venue_order_id, Quantity::new(fill, 6));
        assert_eq!(snapped, Quantity::new(expected, 6));
    }

    // The band is in absolute share units; it does not scale with
    // size_precision. CLOB cent-tick truncation and V2 USDC-scale truncation
    // are both fixed in absolute share terms, so the threshold is too.
    // snap_fill_qty is overfill-only, so underfill cases pass through.
    #[rstest]
    #[case::underfill_within_band_preserved(100.000, 99.995, 99.995)]
    #[case::underfill_above_band(100.000, 95.000, 95.000)]
    #[case::overfill_within_band(100.000, 100.005, 100.000)]
    #[case::overfill_above_band(100.000, 100.050, 100.050)]
    fn test_snap_fill_qty_at_lower_precision(
        #[case] submitted: f64,
        #[case] fill: f64,
        #[case] expected: f64,
    ) {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-1");
        tracker.register(
            venue_order_id,
            Quantity::new(submitted, 3),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            3,
            2,
        );

        let snapped = tracker.snap_fill_qty(&venue_order_id, Quantity::new(fill, 3));
        assert_eq!(snapped, Quantity::new(expected, 3));
    }

    #[rstest]
    fn test_snap_fill_qty_unregistered_order() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("unknown");
        let fill_qty = Quantity::new(50.0, 6);
        let result = tracker.snap_fill_qty(&venue_order_id, fill_qty);
        assert_eq!(result, fill_qty);
    }

    // Verifies the batch helper used by REST callers (`generate_fill_reports`,
    // `generate_mass_status`) snaps each report's `last_qty` and leaves
    // unregistered reports alone. Commission is intentionally untouched.
    #[rstest]
    fn test_snap_fill_reports_snaps_each_in_place() {
        use nautilus_model::{
            enums::LiquiditySide, identifiers::TradeId, reports::FillReport, types::Money,
        };

        let tracker = OrderFillTrackerMap::new();
        let known_id = VenueOrderId::from("known");
        let unknown_id = VenueOrderId::from("unknown");
        tracker.register(
            known_id,
            Quantity::new(714.285710, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        let make_report =
            |venue_order_id: VenueOrderId, last_qty: f64, commission: f64| FillReport {
                account_id: AccountId::from("POLY-001"),
                instrument_id: InstrumentId::from("TEST.POLYMARKET"),
                venue_order_id,
                trade_id: TradeId::from("trade"),
                order_side: OrderSide::Buy,
                last_qty: Quantity::new(last_qty, 6),
                last_px: Price::new(0.55, 2),
                commission: Money::new(commission, pusd()),
                liquidity_side: LiquiditySide::Taker,
                avg_px: None,
                report_id: UUID4::new(),
                ts_event: UnixNanos::default(),
                ts_init: UnixNanos::default(),
                client_order_id: None,
                venue_position_id: None,
            };

        // Known order: 4-ulp overfill, within band, last_qty must snap down.
        // Unknown order: tracker has no entry, reports pass through unchanged.
        let mut reports = vec![
            make_report(known_id, 714.285714, 1.234),
            make_report(unknown_id, 999.0, 5.678),
        ];

        tracker.snap_fill_reports(&mut reports);

        assert_eq!(reports[0].last_qty, Quantity::new(714.285710, 6));
        // Commission untouched even though qty was snapped: it tracks venue truth.
        assert_eq!(reports[0].commission, Money::new(1.234, pusd()));
        assert_eq!(reports[1].last_qty, Quantity::new(999.0, 6));
        assert_eq!(reports[1].commission, Money::new(5.678, pusd()));
    }

    #[rstest]
    fn test_record_fill_accumulates() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        tracker.record_fill(&vid, Quantity::new(50.0, 6));
        tracker.record_fill(&vid, Quantity::new(49.997714, 6));

        let normalized = tracker.check_terminal_quantity_normalization(&vid);

        assert_eq!(normalized, Some(Quantity::new(99.997714, 6)));
    }

    #[rstest]
    fn test_check_dust_no_residual() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        // Exact fill
        tracker.record_fill(&vid, Quantity::new(100.0, 6));

        assert!(
            tracker
                .check_terminal_quantity_normalization(&vid)
                .is_none()
        );
    }

    #[rstest]
    fn test_check_dust_significant_residual() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        // Only half filled, residual = 50 >> 0.01
        tracker.record_fill(&vid, Quantity::new(50.0, 6));

        assert!(
            tracker
                .check_terminal_quantity_normalization(&vid)
                .is_none()
        );
    }

    #[rstest]
    fn test_take_terminal_ioc_remainder_is_exact_and_idempotent() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-partial-ioc");
        tracker.register(
            vid,
            Quantity::from("30.000000"),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            3,
        );
        tracker.record_fill(&vid, Quantity::from("20.000000"));

        let remainder = tracker.take_terminal_ioc_remainder(&vid);

        assert_eq!(remainder, Some(Quantity::from("10.000000")));
        assert!(!tracker.contains(&vid));
        assert!(tracker.has_fills_or_settled(&vid));
        assert!(tracker.take_terminal_ioc_remainder(&vid).is_none());
    }

    #[rstest]
    fn test_terminal_full_ioc_is_retired_without_a_remainder() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-full-ioc");
        tracker.register(
            venue_order_id,
            Quantity::from("20.000000"),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            3,
        );
        tracker.record_fill(&venue_order_id, Quantity::from("20.000000"));

        assert!(
            tracker
                .take_terminal_ioc_remainder(&venue_order_id)
                .is_none()
        );
        assert!(!tracker.contains(&venue_order_id));
        assert!(tracker.has_fills_or_settled(&venue_order_id));
    }

    #[rstest]
    fn test_take_terminal_ioc_remainder_requires_a_partial_fill() {
        let tracker = OrderFillTrackerMap::new();
        let unfilled = VenueOrderId::from("order-unfilled-ioc");
        let filled = VenueOrderId::from("order-filled-ioc");
        for venue_order_id in [unfilled, filled] {
            tracker.register(
                venue_order_id,
                Quantity::from("20.000000"),
                OrderSide::Buy,
                InstrumentId::from("TEST.POLYMARKET"),
                6,
                3,
            );
        }
        tracker.record_fill(&filled, Quantity::from("20.000000"));

        assert!(tracker.take_terminal_ioc_remainder(&unfilled).is_none());
        assert!(tracker.take_terminal_ioc_remainder(&filled).is_none());
        assert!(tracker.contains(&unfilled));
        assert!(!tracker.contains(&filled));
        assert!(tracker.has_fills_or_settled(&filled));
    }

    #[rstest]
    fn test_check_dust_unregistered() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("unknown");

        assert!(
            tracker
                .check_terminal_quantity_normalization(&vid)
                .is_none()
        );
    }

    #[rstest]
    fn test_dust_settlement_removes_entry() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        tracker.record_fill(&vid, Quantity::new(99.995, 6));

        let normalized = tracker.check_terminal_quantity_normalization(&vid);
        assert_eq!(normalized, Some(Quantity::new(99.995, 6)));

        // Entry should be removed, second check returns None (no duplicate).
        assert!(!tracker.contains(&vid));
        assert!(
            tracker
                .check_terminal_quantity_normalization(&vid)
                .is_none()
        );
    }

    #[rstest]
    fn test_get_cumulative_filled_no_fills() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        let filled = tracker.get_cumulative_filled(&vid);
        assert_eq!(filled, Some(Quantity::zero(6)));
    }

    #[rstest]
    fn test_get_cumulative_filled_with_fills() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        tracker.record_fill(&vid, Quantity::new(30.0, 6));
        tracker.record_fill(&vid, Quantity::new(20.0, 6));

        let filled = tracker.get_cumulative_filled(&vid);
        assert_eq!(filled, Some(Quantity::new(50.0, 6)));
    }

    #[rstest]
    fn test_get_cumulative_filled_unregistered() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("unknown");
        assert!(tracker.get_cumulative_filled(&vid).is_none());
    }

    #[rstest]
    fn test_is_fully_filled_unregistered() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("unknown");
        assert!(!tracker.is_fully_filled(&vid));
    }

    #[rstest]
    fn test_is_fully_filled_partial() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        tracker.record_fill(&vid, Quantity::new(50.0, 6));
        assert!(!tracker.is_fully_filled(&vid));
    }

    #[rstest]
    fn test_is_fully_filled_complete() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(100.0, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        tracker.record_fill(&vid, Quantity::new(60.0, 6));
        tracker.record_fill(&vid, Quantity::new(40.0, 6));
        assert!(tracker.is_fully_filled(&vid));
    }

    fn register_buy(tracker: &OrderFillTrackerMap, vid: VenueOrderId, submitted: f64) {
        tracker.register(
            vid,
            Quantity::new(submitted, 6),
            OrderSide::Buy,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );
    }

    #[rstest]
    fn test_buy_overfill_bump_unregistered_is_none() {
        let tracker = OrderFillTrackerMap::new();
        assert!(
            tracker
                .buy_overfill_bump(&VenueOrderId::from("unknown"))
                .is_none()
        );
    }

    #[rstest]
    fn test_buy_overfill_bump_within_qty_is_none() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        register_buy(&tracker, vid, 10.0);

        // Exact fill: cumulative equals submitted, no raise.
        tracker.record_fill(&vid, Quantity::new(10.0, 6));
        assert!(tracker.buy_overfill_bump(&vid).is_none());
    }

    #[rstest]
    fn test_buy_overfill_bump_sell_is_none() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        tracker.register(
            vid,
            Quantity::new(10.0, 6),
            OrderSide::Sell,
            InstrumentId::from("TEST.POLYMARKET"),
            6,
            2,
        );

        // A SELL is share-denominated; even an over-report does not raise the quantity.
        tracker.record_fill(&vid, Quantity::new(12.0, 6));
        assert!(tracker.buy_overfill_bump(&vid).is_none());
    }

    #[rstest]
    fn test_buy_overfill_bump_raises_to_cumulative_and_is_idempotent() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        register_buy(&tracker, vid, 10.0);

        // Marketable BUY fills below its limit: 12 shares against a nominal 10.
        tracker.record_fill(&vid, Quantity::new(12.0, 6));

        let bumped = tracker.buy_overfill_bump(&vid).expect("expected a raise");
        assert_eq!(bumped, Quantity::new(12.0, 6));
        // Submitted is raised, so a second emit for the same fill does not re-raise.
        assert!(tracker.buy_overfill_bump(&vid).is_none());
        // Leaves are non-negative after the raise, so no spurious dust residual.
        assert!(tracker.is_fully_filled(&vid));
    }

    #[rstest]
    fn test_buy_overfill_bump_tracks_each_crossing_fill() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        register_buy(&tracker, vid, 10.0);

        // First partial stays within the nominal qty: no raise.
        tracker.record_fill(&vid, Quantity::new(6.0, 6));
        assert!(tracker.buy_overfill_bump(&vid).is_none());

        // Second partial crosses the nominal qty: raise to cumulative 14.
        tracker.record_fill(&vid, Quantity::new(8.0, 6));
        assert_eq!(
            tracker.buy_overfill_bump(&vid),
            Some(Quantity::new(14.0, 6))
        );

        // Third partial crosses again: raise to cumulative 20.
        tracker.record_fill(&vid, Quantity::new(6.0, 6));
        assert_eq!(
            tracker.buy_overfill_bump(&vid),
            Some(Quantity::new(20.0, 6))
        );
    }

    // A dust overfill is snapped DOWN by snap_fill_qty before recording, so it never reaches
    // buy_overfill_bump as a raise: the two mechanisms do not double-handle the same fill.
    #[rstest]
    fn test_buy_overfill_bump_ignores_dust_snapped_fill() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        register_buy(&tracker, vid, 100.0);

        let raw = Quantity::new(100.005, 6);
        let snapped = tracker.snap_fill_qty(&vid, raw);
        assert_eq!(snapped, Quantity::new(100.0, 6));

        tracker.record_fill(&vid, snapped);
        assert!(tracker.buy_overfill_bump(&vid).is_none());
    }
}
