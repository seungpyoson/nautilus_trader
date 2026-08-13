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
//!
//! The trade evidence ledger lives here, under the same lock as the per-order quantities, because
//! the two always change together: a leg the venue failed has to give its quantity back in the same
//! critical section that decides the leg is void, and a fill queued for an order that is not yet
//! registered has to be admitted or refused against that same decision. Two locks, or two places
//! holding trade state, is what let a fill be applied and voided out of order.

use std::sync::Mutex;

use indexmap::IndexMap;
use nautilus_common::cache::fifo::FifoCacheMap;
use nautilus_core::MUTEX_POISONED;
#[cfg(test)]
use nautilus_model::identifiers::InstrumentId;
use nautilus_model::{
    enums::OrderSide,
    events::{OrderFillVoided, OrderFilled},
    identifiers::{ClientOrderId, TradeId, VenueOrderId},
    reports::{FillReport, OrderStatusReport},
    types::Quantity,
};
use rust_decimal::Decimal;
use ustr::Ustr;

#[cfg(test)]
use crate::execution::trade_evidence::EvidenceState;
use crate::{
    common::consts::DUST_SNAP_THRESHOLD_DEC,
    execution::{
        evidence_ledger::{EvidenceOutcome, TradeEvidenceLedger, VoidedLeg},
        parse::make_composite_trade_id,
        trade_evidence::{FillDelivery, Settlement, TradeEvidence},
    },
};

/// Cumulative fill state for a single order.
#[derive(Debug, Clone, Copy)]
struct OrderFillState {
    submitted_qty: Quantity,
    cumulative_filled: Quantity,
    order_side: OrderSide,
}

/// What the caller must emit for one venue statement about a trade leg.
#[derive(Debug)]
pub(crate) enum TradeAdmission {
    /// The leg was applied against a registered order: emit this fill.
    Emit(Box<FillReport>),
    /// The leg was applied, and its fill is queued until the order it fills is registered.
    Queued,
    /// The statement changes nothing that leaves this adapter.
    Ignored,
    /// The leg was applied and the venue has now failed it: reverse it.
    Voided(Box<VoidedLeg>),
}

/// A fill accepted from the venue while the order it fills was not yet registered.
///
/// The evidence behind it is already in the ledger, so a failure that lands before this reaches
/// the engine still refuses it, and a re-delivery of the same trade never queues it twice.
#[derive(Clone, Debug)]
pub(crate) struct BufferedFill {
    pub report: FillReport,
    /// The venue trade fields carried into `OrderFilled.info`, for a fill that came from a trade
    /// payload rather than the order path.
    pub info: Option<IndexMap<Ustr, Ustr>>,
}

/// Registration map plus the fill and order-report buffers, all under one mutex.
///
/// Co-locating the buffers with the registration map is what closes the buffer-after-drain race:
/// the WS dispatch's accepted-check and buffer, and the submit path's register and drain, are all
/// single critical sections on this one lock, so a buffer can never slip between a register and the
/// drain that follows it.
#[derive(Debug, Default)]
struct TrackerInner {
    orders: FifoCacheMap<VenueOrderId, OrderFillState, 10_000>,
    pending_fills: FifoCacheMap<VenueOrderId, Vec<BufferedFill>, 1_000>,
    pending_reports: FifoCacheMap<VenueOrderId, Vec<OrderStatusReport>, 1_000>,
    evidence: TradeEvidenceLedger,
}

/// Tracks per-order fill accumulation, detects dust residuals, and buffers WS messages that arrive
/// before the order is registered.
///
/// Thread-safe: a single internal `Mutex<TrackerInner>` -- safe to share via `Arc` across the WS
/// task and spawned order submission tasks. Because registration and buffering share that lock, the
/// accepted-or-buffer decision and the register-and-drain are mutually atomic.
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
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .insert(venue_order_id, state);
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

    /// Returns true if the order has received any fills or been removed (settled).
    pub(crate) fn has_fills_or_settled(&self, venue_order_id: &VenueOrderId) -> bool {
        match self
            .inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
        {
            Some(s) => !s.cumulative_filled.is_zero(),
            None => true, // Removed = already settled
        }
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

    /// Records what the venue has stated about one trade leg and applies it to the fill state.
    ///
    /// The ledger decision, the dust snap, the quantity the order accumulates, and the queueing of
    /// a fill for an order that is not yet registered all happen under one lock, so a re-delivered
    /// trade cannot be applied twice and a failure cannot cross a fill that is mid-flight.
    ///
    /// `candidate` is the evidence the statement supports, and is `None` only when the payload
    /// could not produce complete evidence, which only a failure acts on.
    pub(crate) fn observe_trade_leg(
        &self,
        trade_id: TradeId,
        candidate: Option<TradeEvidence>,
        settlement: Settlement,
        delivery: FillDelivery,
        info: Option<IndexMap<Ustr, Ustr>>,
    ) -> TradeAdmission {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let inner = &mut *guard;
        let candidate = candidate.map(|mut evidence| {
            evidence.last_qty =
                snap_fill_qty_in(&inner.orders, &evidence.venue_order_id, evidence.last_qty);
            evidence
        });

        match inner.evidence.observe(trade_id, candidate, settlement) {
            EvidenceOutcome::Ignore => TradeAdmission::Ignored,
            EvidenceOutcome::Void(leg) => {
                // A fill still queued never reached the order's quantity, so the leg is simply
                // dropped from the queue. Anything else was already counted, either by an emitted
                // event or by the drain that is carrying it, and has to be given back here.
                let queued =
                    remove_buffered_fill(&mut inner.pending_fills, &leg.venue_order_id, &trade_id);

                if !queued {
                    reverse_fill_in(&mut inner.orders, &leg.venue_order_id, leg.last_qty);
                }

                TradeAdmission::Voided(leg)
            }
            EvidenceOutcome::Apply => {
                let Some(evidence) = inner.evidence.evidence(&trade_id) else {
                    return TradeAdmission::Ignored;
                };
                let report = evidence.to_fill_report(delivery);
                let venue_order_id = report.venue_order_id;

                if inner.orders.get(&venue_order_id).is_some() {
                    record_fill_in(&mut inner.orders, &venue_order_id, report.last_qty);
                    TradeAdmission::Emit(Box::new(report))
                } else {
                    push_buffered(
                        &mut inner.pending_fills,
                        venue_order_id,
                        BufferedFill { report, info },
                    );
                    TradeAdmission::Queued
                }
            }
        }
    }

    /// Returns a tracked order report to emit, or buffers it until the order is registered.
    ///
    /// The accepted-check and the buffer insert run under one lock, so the submit path's register
    /// (sequenced before its report drain) cannot leave the report buffered with no later drain.
    /// Returns the report to emit when the order is registered, or `None` when it was buffered.
    pub(crate) fn accept_or_buffer_report(
        &self,
        venue_order_id: VenueOrderId,
        report: OrderStatusReport,
    ) -> Option<OrderStatusReport> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard.orders.get(&venue_order_id).is_some() {
            Some(report)
        } else {
            push_buffered(&mut guard.pending_reports, venue_order_id, report);
            None
        }
    }

    /// Registers the order, then drains and prepares its buffered fills under one lock.
    ///
    /// Registration and the drain are a single critical section, so a concurrent
    /// [`Self::observe_trade_leg`] cannot read the order as unregistered and queue a fill into the
    /// window after this drain.
    pub(crate) fn register_and_take_pending_fills(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        submitted_qty: Quantity,
        order_side: OrderSide,
    ) -> Vec<BufferedFill> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        guard
            .orders
            .insert(venue_order_id, new_order_state(submitted_qty, order_side));
        take_and_prepare_fills(&mut guard, venue_order_id, client_order_id)
    }

    /// Registers the order and drains its buffered fills only when a fill is already buffered.
    ///
    /// Used by the unknown-submit path, where acceptance is deferred until a buffered fill proves
    /// the venue took the order. Returns `None` (registering nothing) when no fill is buffered.
    pub(crate) fn register_and_take_pending_fills_if_buffered(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
        submitted_qty: Quantity,
        order_side: OrderSide,
    ) -> Option<Vec<BufferedFill>> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if !guard.pending_fills.contains_key(&venue_order_id) {
            return None;
        }
        guard
            .orders
            .insert(venue_order_id, new_order_state(submitted_qty, order_side));
        Some(take_and_prepare_fills(
            &mut guard,
            venue_order_id,
            client_order_id,
        ))
    }

    /// Drains and prepares buffered fills for an already-registered order.
    pub(crate) fn take_pending_fills(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: Option<ClientOrderId>,
    ) -> Vec<BufferedFill> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        take_and_prepare_fills(&mut guard, venue_order_id, client_order_id)
    }

    /// Drains buffered order reports for a registered order (raw, for conversion by the caller).
    pub(crate) fn take_pending_reports(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Vec<OrderStatusReport> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .pending_reports
            .remove(venue_order_id)
            .unwrap_or_default()
    }

    /// Emits a fill drained from the queue and records the event against its evidence, atomically.
    ///
    /// A leg the venue failed after the drain took its fill is refused here, so the failure can
    /// win that race without ever letting the fill through. The quantity is not given back on that
    /// refusal: the failure gave it back when it decided the leg was void. Otherwise the event is
    /// sent before it becomes visible to the failure path, preserving `OrderFilled` before
    /// `OrderFillVoided` on the execution channel.
    pub(crate) fn emit_buffered_fill<F>(&self, fill: OrderFilled, emit: F) -> bool
    where
        F: FnOnce(OrderFilled, Option<Quantity>),
    {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let inner = &mut *guard;

        if inner.evidence.is_voided(&fill.trade_id) {
            log::warn!("Refusing a drained fill for failed trade {}", fill.trade_id);
            return false;
        }

        let new_qty = buy_overfill_bump_in(&mut inner.orders, &fill.venue_order_id);
        emit(fill.clone(), new_qty);
        inner.evidence.record_fill_event(fill);

        true
    }

    /// Emits a fill drained from the queue that has no local order to attach it to, as a report.
    ///
    /// Reports leave no event the engine can reverse, so nothing is recorded against the evidence,
    /// but a leg the venue has failed is refused on the same terms as one that emits an event.
    pub(crate) fn emit_buffered_report<F>(&self, fill: &FillReport, emit: F) -> bool
    where
        F: FnOnce(),
    {
        let guard = self.inner.lock().expect(MUTEX_POISONED);

        if guard.evidence.is_voided(&fill.trade_id) {
            log::warn!(
                "Refusing a drained fill report for failed trade {}",
                fill.trade_id
            );
            return false;
        }

        drop(guard);
        emit();

        true
    }

    /// Records the event an applied leg produced, so a later failure reverses what was applied.
    pub(crate) fn record_fill_event(&self, filled: OrderFilled) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .record_fill_event(filled);
    }

    /// Returns whether the venue has confirmed the leg this fill came from.
    #[must_use]
    pub(crate) fn is_trade_confirmed(&self, trade_id: &TradeId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .is_confirmed(trade_id)
    }

    /// Returns whether the venue has confirmed the leg of `venue_trade_id` that filled this order.
    ///
    /// The order channel names its trades by the venue's own trade ID, while a maker leg is
    /// recorded under the composite ID the engine indexes that fill by. Both forms are asked for,
    /// so the answer comes from the evidence rather than from a second set kept alongside it.
    #[must_use]
    pub(crate) fn is_venue_trade_confirmed(
        &self,
        venue_trade_id: &str,
        venue_order_id: &VenueOrderId,
    ) -> bool {
        let composite = make_composite_trade_id(venue_trade_id, venue_order_id.as_str());
        let guard = self.inner.lock().expect(MUTEX_POISONED);

        guard.evidence.is_confirmed(&composite)
            || guard.evidence.is_confirmed(&TradeId::from(venue_trade_id))
    }

    /// Restores a leg applied in an earlier session, so the venue can still fail it.
    pub(crate) fn restore_applied_evidence(&self, filled: &OrderFilled) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .restore_applied(filled);
    }

    /// Restores a leg voided in an earlier session, so it can never be applied again.
    pub(crate) fn restore_voided_evidence(&self, voided: &OrderFillVoided) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .restore_voided(voided);
    }

    /// Drops every trade leg this adapter has accepted.
    pub(crate) fn clear_trade_evidence(&self) {
        self.inner.lock().expect(MUTEX_POISONED).evidence.clear();
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
    pub(crate) fn buy_overfill_bump(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        buy_overfill_bump_in(&mut guard.orders, venue_order_id)
    }

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
        let s = guard.orders.get(venue_order_id)?;
        if s.cumulative_filled >= s.submitted_qty {
            return None;
        }
        let leaves = s.submitted_qty.as_decimal() - s.cumulative_filled.as_decimal();

        if leaves > Decimal::ZERO && leaves < DUST_SNAP_THRESHOLD_DEC {
            let filled_qty = s.cumulative_filled;

            log::debug!(
                "Normalizing terminal order {venue_order_id} quantity from {} to {filled_qty} \
                 (non-economic leaves={leaves})",
                s.submitted_qty,
            );
            guard.orders.remove(venue_order_id);
            Some(filled_qty)
        } else {
            if leaves >= DUST_SNAP_THRESHOLD_DEC {
                log::debug!(
                    "Order {venue_order_id} MATCHED with significant residual \
                     {leaves} (filled {}/{})",
                    s.cumulative_filled,
                    s.submitted_qty,
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
        let state = guard.orders.get(venue_order_id)?;
        if state.cumulative_filled.is_zero() || state.cumulative_filled >= state.submitted_qty {
            return None;
        }

        let remainder = state.submitted_qty - state.cumulative_filled;
        guard.orders.remove(venue_order_id);
        Some(remainder)
    }
}

fn new_order_state(submitted_qty: Quantity, order_side: OrderSide) -> OrderFillState {
    OrderFillState {
        submitted_qty,
        cumulative_filled: Quantity::zero(submitted_qty.precision),
        order_side,
    }
}

fn buy_overfill_bump_in(
    orders: &mut FifoCacheMap<VenueOrderId, OrderFillState, 10_000>,
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

/// Drains the buffered fills for `venue_order_id`, stamping the client order ID and snapping and
/// recording each one. The caller must hold the lock and have registered the order first.
fn take_and_prepare_fills(
    inner: &mut TrackerInner,
    venue_order_id: VenueOrderId,
    client_order_id: Option<ClientOrderId>,
) -> Vec<BufferedFill> {
    let Some(buffered) = inner.pending_fills.remove(&venue_order_id) else {
        return Vec::new();
    };
    buffered
        .into_iter()
        .map(|mut buffered| {
            buffered.report.client_order_id = client_order_id;
            buffered.report.last_qty =
                snap_fill_qty_in(&inner.orders, &venue_order_id, buffered.report.last_qty);
            record_fill_in(&mut inner.orders, &venue_order_id, buffered.report.last_qty);
            inner
                .evidence
                .record_applied_quantity(&buffered.report.trade_id, buffered.report.last_qty);
            buffered
        })
        .collect()
}

/// Drops a queued fill for one trade leg, returning whether it was still queued.
///
/// A queued fill has not been counted against the order, so dropping it is the whole reversal.
fn remove_buffered_fill(
    buffer: &mut FifoCacheMap<VenueOrderId, Vec<BufferedFill>, 1_000>,
    venue_order_id: &VenueOrderId,
    trade_id: &TradeId,
) -> bool {
    let Some(fills) = buffer.get_mut(venue_order_id) else {
        return false;
    };
    let before = fills.len();
    fills.retain(|fill| fill.report.trade_id != *trade_id);
    let removed = fills.len() != before;

    if fills.is_empty() {
        buffer.remove(venue_order_id);
    }

    removed
}

fn push_buffered<V>(
    buffer: &mut FifoCacheMap<VenueOrderId, Vec<V>, 1_000>,
    venue_order_id: VenueOrderId,
    value: V,
) {
    if let Some(values) = buffer.get_mut(&venue_order_id) {
        values.push(value);
    } else {
        buffer.insert(venue_order_id, vec![value]);
    }
}

#[cfg(test)]
impl OrderFillTrackerMap {
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
    pub(crate) fn record_fill(&self, venue_order_id: &VenueOrderId, qty: Quantity) {
        record_fill_in(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            qty,
        );
    }

    /// Buffers a fill as if it arrived on the WS channel before the order was registered.
    pub(crate) fn buffer_fill_for_test(&self, venue_order_id: VenueOrderId, report: FillReport) {
        push_buffered(
            &mut self.inner.lock().expect(MUTEX_POISONED).pending_fills,
            venue_order_id,
            BufferedFill { report, info: None },
        );
    }

    /// Marks a trade leg settled, as a `CONFIRMED` statement from the venue would.
    pub(crate) fn confirm_evidence_for_test(&self, filled: &OrderFilled) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .restore_confirmed(filled);
    }

    /// Returns the state the ledger holds for a trade leg.
    pub(crate) fn evidence_state_for_test(&self, trade_id: &TradeId) -> Option<EvidenceState> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .evidence
            .evidence(trade_id)
            .map(|evidence| evidence.state)
    }

    /// Buffers an order report as if it arrived on the WS channel before the order was registered.
    pub(crate) fn buffer_report_for_test(
        &self,
        venue_order_id: VenueOrderId,
        report: OrderStatusReport,
    ) {
        push_buffered(
            &mut self.inner.lock().expect(MUTEX_POISONED).pending_reports,
            venue_order_id,
            report,
        );
    }

    /// Returns true if a fill is currently buffered for the order.
    pub(crate) fn has_pending_fill(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .pending_fills
            .contains_key(venue_order_id)
    }

    /// Returns the fills currently buffered for the order.
    pub(crate) fn pending_fills_for(&self, venue_order_id: &VenueOrderId) -> Vec<FillReport> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .pending_fills
            .get(venue_order_id)
            .map(|fills| fills.iter().map(|fill| fill.report.clone()).collect())
            .unwrap_or_default()
    }

    /// Returns true if an order report is currently buffered for the order.
    pub(crate) fn has_pending_report(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .pending_reports
            .contains_key(venue_order_id)
    }
}

fn record_fill_in(
    orders: &mut FifoCacheMap<VenueOrderId, OrderFillState, 10_000>,
    venue_order_id: &VenueOrderId,
    qty: Quantity,
) {
    if let Some(s) = orders.get_mut(venue_order_id) {
        s.cumulative_filled = s.cumulative_filled + qty;
    }
}

fn reverse_fill_in(
    orders: &mut FifoCacheMap<VenueOrderId, OrderFillState, 10_000>,
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
    orders: &FifoCacheMap<VenueOrderId, OrderFillState, 10_000>,
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
        enums::LiquiditySide,
        identifiers::{AccountId, TradeId},
        types::{Currency, Money, Price},
    };
    use rstest::rstest;

    use super::*;
    use crate::execution::trade_evidence::{LegEconomics, LegIdentity};

    fn pusd() -> Currency {
        Currency::pUSD()
    }

    #[rstest]
    fn test_register_and_contains() {
        let tracker = OrderFillTrackerMap::new();
        let vid = VenueOrderId::from("order-1");
        assert!(!tracker.contains(&vid));

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

    /// Builds the evidence a report states, so tests can drive the ledger the way dispatch does.
    fn evidence_for(report: &FillReport) -> TradeEvidence {
        TradeEvidence::build(
            LegIdentity {
                trade_id: report.trade_id,
                venue_order_id: report.venue_order_id,
                instrument_id: report.instrument_id,
                order_side: report.order_side,
                liquidity_side: report.liquidity_side,
            },
            LegEconomics {
                size: report.last_qty.as_decimal(),
                price: report.last_px.as_decimal(),
                size_precision: report.last_qty.precision,
                price_precision: report.last_px.precision,
                fee_rate: Decimal::ZERO,
                fee_exponent: 1.0,
                currency: pusd(),
            },
            Some(report.ts_event),
        )
        .expect("valid evidence")
    }

    fn delivery_for(report: &FillReport) -> FillDelivery {
        FillDelivery {
            account_id: report.account_id,
            client_order_id: report.client_order_id,
            ts_init: report.ts_init,
        }
    }

    fn queued_fill_report(venue_order_id: VenueOrderId, trade_id: &str) -> FillReport {
        FillReport {
            account_id: AccountId::from("POLY-001"),
            instrument_id: InstrumentId::from("TEST.POLYMARKET"),
            venue_order_id,
            trade_id: TradeId::from(trade_id),
            order_side: OrderSide::Buy,
            last_qty: Quantity::new(5.0, 6),
            last_px: Price::new(0.55, 2),
            commission: Money::zero(pusd()),
            liquidity_side: LiquiditySide::Taker,
            avg_px: None,
            report_id: UUID4::new(),
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
            client_order_id: None,
            venue_position_id: None,
        }
    }

    fn filled_from(report: &FillReport) -> OrderFilled {
        use nautilus_model::{
            enums::OrderType,
            identifiers::{StrategyId, TraderId},
        };

        OrderFilled::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            report.instrument_id,
            ClientOrderId::from("O-FAILED-BEFORE-DRAIN"),
            report.venue_order_id,
            report.account_id,
            report.trade_id,
            report.order_side,
            OrderType::Limit,
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
        )
    }

    #[rstest]
    fn test_failed_trade_drops_a_still_queued_fill() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-failed-before-drain");
        let report = queued_fill_report(venue_order_id, "trade-failed-before-drain");
        let evidence = evidence_for(&report);

        let queued = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
            delivery_for(&report),
            None,
        );
        let failed = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence),
            Settlement::Failed,
            delivery_for(&report),
            None,
        );
        let drained = tracker.register_and_take_pending_fills(
            venue_order_id,
            Some(ClientOrderId::from("O-FAILED-BEFORE-DRAIN")),
            Quantity::new(10.0, 6),
            OrderSide::Buy,
        );

        assert!(matches!(queued, TradeAdmission::Queued));
        match failed {
            TradeAdmission::Voided(leg) => assert!(leg.filled.is_none()),
            other => panic!("expected a void, was {other:?}"),
        }
        assert!(drained.is_empty());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(6))
        );
    }

    #[rstest]
    fn test_failed_trade_refuses_a_fill_already_drained() {
        use std::cell::Cell;

        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-failed-mid-flight");
        let report = queued_fill_report(venue_order_id, "trade-failed-mid-flight");
        let evidence = evidence_for(&report);

        let queued = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
            delivery_for(&report),
            None,
        );
        let drained = tracker.register_and_take_pending_fills(
            venue_order_id,
            Some(ClientOrderId::from("O-FAILED-BEFORE-DRAIN")),
            Quantity::new(10.0, 6),
            OrderSide::Buy,
        );
        let failed = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence),
            Settlement::Failed,
            delivery_for(&report),
            None,
        );
        let was_emitted = Cell::new(false);
        let emitted = tracker.emit_buffered_fill(filled_from(&report), |_, _| {
            was_emitted.set(true);
        });

        assert!(matches!(queued, TradeAdmission::Queued));
        assert_eq!(drained.len(), 1);

        match failed {
            TradeAdmission::Voided(leg) => assert!(leg.filled.is_none()),
            other => panic!("expected a void, was {other:?}"),
        }

        assert!(!emitted);
        assert!(!was_emitted.get());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(6))
        );
    }

    /// A fill for an order this client does not own locally leaves the adapter as a report rather
    /// than an event, and a failed trade has to refuse it on the same terms: reconciliation would
    /// otherwise read the report as a fill the venue never settled.
    #[rstest]
    fn test_failed_trade_refuses_a_drained_fill_report() {
        use std::cell::Cell;

        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-report-after-failure");
        let report = queued_fill_report(venue_order_id, "trade-report-after-failure");
        let evidence = evidence_for(&report);

        tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
            delivery_for(&report),
            None,
        );
        let drained = tracker.register_and_take_pending_fills(
            venue_order_id,
            None,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
        );
        tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence),
            Settlement::Failed,
            delivery_for(&report),
            None,
        );
        let was_emitted = Cell::new(false);
        let emitted = tracker.emit_buffered_report(&drained[0].report, || {
            was_emitted.set(true);
        });

        assert!(!emitted);
        assert!(!was_emitted.get());
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(6))
        );
    }

    /// A leg is queued before the order's registered size is known, so the drain's dust snap is
    /// the first moment its applied quantity is settled. The `CONFIRMED` statement that follows
    /// restates the venue's own numbers, and must settle the leg rather than read as a conflict
    /// against the snapped quantity.
    #[rstest]
    fn test_confirmation_after_a_snapped_drain_settles_the_leg() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-snapped-drain");
        let mut report = queued_fill_report(venue_order_id, "trade-snapped-drain");
        report.last_qty = Quantity::new(5.202914, 6);
        let submitted_qty = Quantity::new(5.202910, 6);
        let evidence = evidence_for(&report);

        let queued = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
            delivery_for(&report),
            None,
        );
        let drained = tracker.register_and_take_pending_fills(
            venue_order_id,
            Some(ClientOrderId::from("O-SNAPPED-DRAIN")),
            submitted_qty,
            OrderSide::Buy,
        );
        let confirmed = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence),
            Settlement::Confirmed,
            delivery_for(&report),
            None,
        );

        assert!(matches!(queued, TradeAdmission::Queued));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].report.last_qty, submitted_qty);
        assert!(matches!(confirmed, TradeAdmission::Ignored));
        assert!(tracker.is_trade_confirmed(&report.trade_id));
    }

    #[rstest]
    fn test_applied_fill_is_voided_once_with_its_event() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-applied-then-failed");
        let report = queued_fill_report(venue_order_id, "trade-applied-then-failed");
        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            report.instrument_id,
            6,
            2,
        );
        let evidence = evidence_for(&report);

        let applied = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
            delivery_for(&report),
            None,
        );
        tracker.record_fill_event(filled_from(&report));
        let failed = tracker.observe_trade_leg(
            report.trade_id,
            Some(evidence),
            Settlement::Failed,
            delivery_for(&report),
            None,
        );

        assert!(matches!(applied, TradeAdmission::Emit(_)));
        match failed {
            TradeAdmission::Voided(leg) => {
                assert_eq!(
                    leg.filled.map(|filled| filled.trade_id),
                    Some(report.trade_id)
                );
            }
            other => panic!("expected a void, was {other:?}"),
        }
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(6))
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
        assert!(tracker.take_terminal_ioc_remainder(&vid).is_none());
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
        assert!(tracker.contains(&filled));
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
