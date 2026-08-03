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

//! Single owner for Polymarket local-order identity, lifecycle, and fill normalization.

use std::{collections::HashMap, sync::Mutex};

use nautilus_core::MUTEX_POISONED;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::common::consts::DUST_SNAP_THRESHOLD_DEC;

/// Stable local identity captured before an order is submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderIdentity {
    pub client_order_id: ClientOrderId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

impl OrderIdentity {
    pub(crate) fn from_order(order: &OrderAny) -> Self {
        Self {
            client_order_id: order.client_order_id(),
            strategy_id: order.strategy_id(),
            instrument_id: order.instrument_id(),
            order_side: order.order_side(),
            order_type: order.order_type(),
            time_in_force: order.time_in_force(),
        }
    }

    pub(crate) fn requires_terminal_quantity_normalization(self) -> bool {
        self.time_in_force == TimeInForce::Fok
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocalOrderConflict;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocalOrderSnapshot {
    pub identity: OrderIdentity,
    pub quantity: Quantity,
    pub filled_qty: Quantity,
    pub price: Option<Price>,
}

#[derive(Clone, Debug)]
pub(crate) enum ArtifactAdmission<T> {
    Owned {
        artifact: T,
        identity: OrderIdentity,
    },
    Untracked(T),
    Conflict(LocalOrderConflict),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelAdmission {
    Ready(VenueOrderId),
    Deferred,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubmissionAcceptance {
    pub newly_accepted: bool,
    pub cancel_requested: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AdmissionBatch<T> {
    pub artifacts: Vec<T>,
    pub conflicts: usize,
}

impl<T> Default for AdmissionBatch<T> {
    fn default() -> Self {
        Self {
            artifacts: Vec::new(),
            conflicts: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FillApplication {
    Observe,
    Reconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderApplication {
    Observe,
    Reconcile,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubmitRejection {
    Removed,
    Rejected,
    AlreadyAccepted {
        venue_order_id: VenueOrderId,
        cancel_requested: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubmissionState {
    Pending,
    OutcomeUnknown,
    Accepted,
    Rejected,
}

/// All mutable state for one local order, owned by one coordinator lock.
#[derive(Debug, Clone, Copy)]
struct LocalOrderState {
    identity: OrderIdentity,
    submitted_qty: Quantity,
    cumulative_filled: Quantity,
    price: Option<Price>,
    submission: SubmissionState,
    cancel_requested: bool,
    terminal_quantity_normalized: bool,
    terminal_ioc_remainder_taken: bool,
}

#[derive(Debug, Clone, Copy)]
struct PreparingOrderState {
    identity: OrderIdentity,
    submission_started: bool,
    cancel_requested: bool,
}

#[derive(Debug, Default)]
struct TrackerInner {
    preparing: HashMap<ClientOrderId, PreparingOrderState>,
    client_to_venue: HashMap<ClientOrderId, VenueOrderId>,
    // Intentionally non-evicting: late REST/WS artifacts and venue-ID reuse must still be checked
    // against the immutable identity originally claimed for this process lifetime.
    orders: HashMap<VenueOrderId, LocalOrderState>,
}

/// Owns local-order identity, lifecycle, deferred cancellation, and fill normalization.
#[derive(Debug)]
pub(crate) struct LocalOrderCoordinator {
    inner: Mutex<TrackerInner>,
}

impl Default for LocalOrderCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalOrderCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(TrackerInner::default()),
        }
    }

    pub(crate) fn begin_submission(
        &self,
        identity: OrderIdentity,
    ) -> Result<(), LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard
            .client_to_venue
            .contains_key(&identity.client_order_id)
        {
            return Err(LocalOrderConflict);
        }
        if let Some(preparing) = guard.preparing.get_mut(&identity.client_order_id) {
            if preparing.identity != identity || preparing.submission_started {
                return Err(LocalOrderConflict);
            }
            preparing.submission_started = true;
            return Ok(());
        }
        guard.preparing.insert(
            identity.client_order_id,
            PreparingOrderState {
                identity,
                submission_started: true,
                cancel_requested: false,
            },
        );
        Ok(())
    }

    pub(crate) fn claim_submission(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
        submitted_qty: Quantity,
        price: Option<Price>,
    ) -> Result<(), LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(preparing) = guard.preparing.get(&identity.client_order_id).copied() else {
            return Err(LocalOrderConflict);
        };
        if guard.orders.contains_key(&venue_order_id)
            || guard
                .client_to_venue
                .contains_key(&identity.client_order_id)
            || preparing.identity != identity
            || !preparing.submission_started
        {
            return Err(LocalOrderConflict);
        }
        guard.preparing.remove(&identity.client_order_id);
        let mut state = new_order_state(identity, submitted_qty, price, SubmissionState::Pending);
        state.cancel_requested = preparing.cancel_requested;
        guard.orders.insert(venue_order_id, state);
        guard
            .client_to_venue
            .insert(identity.client_order_id, venue_order_id);
        Ok(())
    }

    pub(crate) fn identity(&self, venue_order_id: &VenueOrderId) -> Option<OrderIdentity> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|state| state.identity)
    }

    pub(crate) fn snapshot(&self, venue_order_id: &VenueOrderId) -> Option<LocalOrderSnapshot> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|state| LocalOrderSnapshot {
                identity: state.identity,
                quantity: state.submitted_qty,
                filled_qty: state.cumulative_filled,
                price: state.price,
            })
    }

    pub(crate) fn admit_fill_report(&self, report: FillReport) -> ArtifactAdmission<FillReport> {
        self.resolve_fill_report(report, FillApplication::Observe)
    }

    pub(crate) fn admit_reconciliation_fill_reports(
        &self,
        reports: Vec<FillReport>,
    ) -> AdmissionBatch<FillReport> {
        let mut batch = AdmissionBatch::default();
        for report in reports {
            match self.resolve_fill_report(report, FillApplication::Reconcile) {
                ArtifactAdmission::Owned { artifact, .. }
                | ArtifactAdmission::Untracked(artifact) => batch.artifacts.push(artifact),
                ArtifactAdmission::Conflict(_) => batch.conflicts += 1,
            }
        }
        batch
    }

    fn resolve_fill_report(
        &self,
        mut report: FillReport,
        application: FillApplication,
    ) -> ArtifactAdmission<FillReport> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id).copied() else {
            return ArtifactAdmission::Untracked(report);
        };
        if state.identity.instrument_id != report.instrument_id
            || state.identity.order_side != report.order_side
            || report
                .client_order_id
                .is_some_and(|client| client != state.identity.client_order_id)
        {
            return ArtifactAdmission::Conflict(LocalOrderConflict);
        }
        report.client_order_id = Some(state.identity.client_order_id);
        report.last_qty = snap_fill_qty_in(&guard.orders, &report.venue_order_id, report.last_qty);
        if application == FillApplication::Observe {
            record_fill_in(&mut guard.orders, &report.venue_order_id, report.last_qty);
        }
        ArtifactAdmission::Owned {
            artifact: report,
            identity: state.identity,
        }
    }

    pub(crate) fn admit_reconciliation_order_reports(
        &self,
        reports: Vec<OrderStatusReport>,
    ) -> AdmissionBatch<OrderStatusReport> {
        let mut batch = AdmissionBatch::default();
        for report in reports {
            match self.resolve_order_report(report, OrderApplication::Reconcile) {
                ArtifactAdmission::Owned { artifact, .. }
                | ArtifactAdmission::Untracked(artifact) => batch.artifacts.push(artifact),
                ArtifactAdmission::Conflict(_) => batch.conflicts += 1,
            }
        }
        batch
    }

    pub(crate) fn admit_order_report(
        &self,
        report: OrderStatusReport,
    ) -> ArtifactAdmission<OrderStatusReport> {
        self.resolve_order_report(report, OrderApplication::Observe)
    }

    fn resolve_order_report(
        &self,
        mut report: OrderStatusReport,
        application: OrderApplication,
    ) -> ArtifactAdmission<OrderStatusReport> {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id).copied() else {
            return ArtifactAdmission::Untracked(report);
        };
        if state.identity.instrument_id != report.instrument_id
            || state.identity.order_side != report.order_side
            || state.identity.time_in_force != report.time_in_force
            || report
                .client_order_id
                .is_some_and(|client| client != state.identity.client_order_id)
        {
            return ArtifactAdmission::Conflict(LocalOrderConflict);
        }
        report.client_order_id = Some(state.identity.client_order_id);
        // Polymarket status payloads carry the venue instruction (GTC/GTD/FAK/FOK),
        // but not Nautilus's originating Limit/Market order type. The immutable
        // local identity is the authority for that missing field.
        report.order_type = state.identity.order_type;
        if application == OrderApplication::Observe && report.filled_qty > state.cumulative_filled {
            log::debug!(
                "Capping filled_qty for {} from {} to {} while awaiting admitted fills",
                report.venue_order_id,
                report.filled_qty,
                state.cumulative_filled,
            );
            report.filled_qty = state.cumulative_filled;
        }
        ArtifactAdmission::Owned {
            artifact: report,
            identity: state.identity,
        }
    }

    pub(crate) fn request_cancel(
        &self,
        identity: OrderIdentity,
        requested_venue_order_id: Option<VenueOrderId>,
    ) -> CancelAdmission {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if let Some(preparing) = guard.preparing.get_mut(&identity.client_order_id) {
            return if preparing.identity == identity && requested_venue_order_id.is_none() {
                preparing.cancel_requested = true;
                CancelAdmission::Deferred
            } else {
                CancelAdmission::Conflict
            };
        }
        let Some(venue_order_id) = guard
            .client_to_venue
            .get(&identity.client_order_id)
            .copied()
        else {
            if requested_venue_order_id.is_some() {
                return CancelAdmission::Conflict;
            }
            guard.preparing.insert(
                identity.client_order_id,
                PreparingOrderState {
                    identity,
                    submission_started: false,
                    cancel_requested: true,
                },
            );
            return CancelAdmission::Deferred;
        };
        let Some(state) = guard.orders.get(&venue_order_id) else {
            return CancelAdmission::Conflict;
        };
        if state.identity != identity
            || requested_venue_order_id.is_some_and(|requested| requested != venue_order_id)
        {
            return CancelAdmission::Conflict;
        }
        match state.submission {
            SubmissionState::Pending => {
                guard
                    .orders
                    .get_mut(&venue_order_id)
                    .expect("order exists under coordinator lock")
                    .cancel_requested = true;
                CancelAdmission::Deferred
            }
            SubmissionState::OutcomeUnknown | SubmissionState::Accepted => {
                CancelAdmission::Ready(venue_order_id)
            }
            SubmissionState::Rejected => CancelAdmission::Conflict,
        }
    }

    pub(crate) fn accept_submission(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
    ) -> Result<SubmissionAcceptance, LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get_mut(&venue_order_id) else {
            return Err(LocalOrderConflict);
        };
        if state.identity != identity {
            return Err(LocalOrderConflict);
        }
        match state.submission {
            SubmissionState::Pending | SubmissionState::OutcomeUnknown => {
                state.submission = SubmissionState::Accepted;
                let cancel_requested = std::mem::take(&mut state.cancel_requested);
                Ok(SubmissionAcceptance {
                    newly_accepted: true,
                    cancel_requested,
                })
            }
            SubmissionState::Accepted => Ok(SubmissionAcceptance {
                newly_accepted: false,
                cancel_requested: std::mem::take(&mut state.cancel_requested),
            }),
            SubmissionState::Rejected => Err(LocalOrderConflict),
        }
    }

    pub(crate) fn observe_acceptance(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
    ) -> Result<bool, LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get_mut(&venue_order_id) else {
            return Err(LocalOrderConflict);
        };
        if state.identity != identity || state.submission == SubmissionState::Rejected {
            return Err(LocalOrderConflict);
        }
        let newly_accepted = state.submission != SubmissionState::Accepted;
        state.submission = SubmissionState::Accepted;
        Ok(newly_accepted)
    }

    pub(crate) fn mark_outcome_unknown(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
    ) -> Result<bool, LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get_mut(&venue_order_id) else {
            return Err(LocalOrderConflict);
        };
        if state.identity != identity {
            return Err(LocalOrderConflict);
        }
        if state.submission == SubmissionState::Pending {
            state.submission = SubmissionState::OutcomeUnknown;
        }
        Ok(std::mem::take(&mut state.cancel_requested))
    }

    pub(crate) fn reject_submission(
        &self,
        identity: OrderIdentity,
    ) -> Result<SubmitRejection, LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if let Some(preparing) = guard.preparing.get(&identity.client_order_id) {
            if preparing.identity != identity {
                return Err(LocalOrderConflict);
            }
            guard.preparing.remove(&identity.client_order_id);
            return Ok(SubmitRejection::Removed);
        }
        let Some(venue_order_id) = guard
            .client_to_venue
            .get(&identity.client_order_id)
            .copied()
        else {
            return Err(LocalOrderConflict);
        };
        let Some(state) = guard.orders.get(&venue_order_id).copied() else {
            return Err(LocalOrderConflict);
        };
        if state.identity != identity {
            return Err(LocalOrderConflict);
        }
        if state.submission == SubmissionState::Accepted {
            let state = guard
                .orders
                .get_mut(&venue_order_id)
                .expect("order exists under coordinator lock");
            return Ok(SubmitRejection::AlreadyAccepted {
                venue_order_id,
                cancel_requested: std::mem::take(&mut state.cancel_requested),
            });
        }
        let state = guard
            .orders
            .get_mut(&venue_order_id)
            .expect("order exists under coordinator lock");
        state.submission = SubmissionState::Rejected;
        state.cancel_requested = false;
        Ok(SubmitRejection::Rejected)
    }

    pub(crate) fn restore_order(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
        submitted_qty: Quantity,
        filled_qty: Quantity,
        price: Option<Price>,
    ) -> Result<(), LocalOrderConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard
            .client_to_venue
            .get(&identity.client_order_id)
            .is_some_and(|known| *known != venue_order_id)
        {
            return Err(LocalOrderConflict);
        }
        if let Some(state) = guard.orders.get_mut(&venue_order_id) {
            if state.identity != identity {
                return Err(LocalOrderConflict);
            }
            state.submitted_qty = submitted_qty;
            state.cumulative_filled = filled_qty;
            state.price = price;
            state.submission = SubmissionState::Accepted;
            return Ok(());
        }
        let mut state = new_order_state(identity, submitted_qty, price, SubmissionState::Accepted);
        state.cumulative_filled = filled_qty;
        guard.orders.insert(venue_order_id, state);
        guard
            .client_to_venue
            .insert(identity.client_order_id, venue_order_id);
        Ok(())
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

    #[cfg(test)]
    pub(crate) fn cumulative_filled_for_test(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
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

    pub(crate) fn reverse_fill(&self, venue_order_id: &VenueOrderId, quantity: Quantity) {
        reverse_fill_in(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            quantity,
        );
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
    /// must not change positions, balances, or commissions. The transition is recorded in-place so
    /// repeated terminal messages are idempotent without forgetting local identity.
    pub(crate) fn check_terminal_quantity_normalization(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let state = guard.orders.get_mut(venue_order_id)?;
        if state.terminal_quantity_normalized || state.cumulative_filled >= state.submitted_qty {
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
            state.terminal_quantity_normalized = true;
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
    /// The transition is recorded in-place so duplicate `CONFIRMED` trade messages cannot emit
    /// repeated cancellations. The caller must use this only after a taker trade confirms: that
    /// proves the FAK order has finished matching and the venue has killed the returned remainder.
    pub(crate) fn take_terminal_ioc_remainder(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let state = guard.orders.get_mut(venue_order_id)?;
        if state.terminal_ioc_remainder_taken
            || state.cumulative_filled.is_zero()
            || state.cumulative_filled >= state.submitted_qty
        {
            return None;
        }

        let remainder = state.submitted_qty - state.cumulative_filled;
        state.terminal_ioc_remainder_taken = true;
        Some(remainder)
    }
}

fn new_order_state(
    identity: OrderIdentity,
    submitted_qty: Quantity,
    price: Option<Price>,
    submission: SubmissionState,
) -> LocalOrderState {
    LocalOrderState {
        identity,
        submitted_qty,
        cumulative_filled: Quantity::zero(submitted_qty.precision),
        price,
        submission,
        cancel_requested: false,
        terminal_quantity_normalized: false,
        terminal_ioc_remainder_taken: false,
    }
}

fn buy_overfill_bump_in(
    orders: &mut HashMap<VenueOrderId, LocalOrderState>,
    venue_order_id: &VenueOrderId,
) -> Option<Quantity> {
    let state = orders.get_mut(venue_order_id)?;
    if state.identity.order_side != OrderSide::Buy {
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
impl LocalOrderCoordinator {
    /// Registers an order directly, for tests that set up an already-accepted order.
    pub(crate) fn register(
        &self,
        venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
        submitted_qty: Quantity,
        order_side: OrderSide,
        instrument_id: InstrumentId,
    ) {
        let identity = OrderIdentity {
            client_order_id,
            strategy_id: StrategyId::from("S-TEST"),
            instrument_id,
            order_side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        };
        self.begin_submission(identity)
            .expect("test order preparation should succeed");
        self.claim_submission(venue_order_id, identity, submitted_qty, None)
            .expect("test order claim should succeed");
        self.accept_submission(venue_order_id, identity)
            .expect("test order acceptance should succeed");
    }

    /// Records a fill against a registered order, for tests that drive fill accumulation directly.
    pub(crate) fn record_fill(&self, venue_order_id: &VenueOrderId, qty: Quantity) {
        record_fill_in(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            qty,
        );
    }
}

fn record_fill_in(
    orders: &mut HashMap<VenueOrderId, LocalOrderState>,
    venue_order_id: &VenueOrderId,
    qty: Quantity,
) {
    if let Some(s) = orders.get_mut(venue_order_id) {
        s.cumulative_filled = s.cumulative_filled + qty;
    }
}

fn reverse_fill_in(
    orders: &mut HashMap<VenueOrderId, LocalOrderState>,
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
    orders: &HashMap<VenueOrderId, LocalOrderState>,
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
        enums::{LiquiditySide, OrderStatus},
        identifiers::{AccountId, TradeId},
        types::{Currency, Money, Price},
    };
    use rstest::rstest;

    use super::*;

    fn identity(client: &str, instrument: &str, side: OrderSide) -> OrderIdentity {
        OrderIdentity {
            client_order_id: ClientOrderId::from(client),
            strategy_id: StrategyId::from("S-TEST"),
            instrument_id: InstrumentId::from(instrument),
            order_side: side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn claim(
        coordinator: &LocalOrderCoordinator,
        venue: VenueOrderId,
        identity: OrderIdentity,
        quantity: Quantity,
    ) {
        coordinator.begin_submission(identity).unwrap();
        coordinator
            .claim_submission(venue, identity, quantity, None)
            .unwrap();
    }

    fn fill(venue: &str, instrument: &str, side: OrderSide, qty: &str) -> FillReport {
        FillReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from(instrument),
            VenueOrderId::from(venue),
            TradeId::from("T-1"),
            side,
            Quantity::from(qty),
            Price::from("0.5000"),
            Money::zero(Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            Some(UUID4::new()),
        )
    }

    fn order_report(
        venue: &str,
        instrument: &str,
        side: OrderSide,
        filled: &str,
    ) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from(instrument),
            None,
            VenueOrderId::from(venue),
            side,
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
    fn claim_is_immutable_in_both_identity_directions() {
        let coordinator = LocalOrderCoordinator::new();
        let v1 = VenueOrderId::from("V-1");
        let v2 = VenueOrderId::from("V-2");
        let first = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, v1, first, Quantity::from("10.0000"));

        assert!(
            coordinator
                .claim_submission(
                    v1,
                    identity("C-2", "I-2.POLYMARKET", OrderSide::Sell),
                    Quantity::from("10.0000"),
                    None,
                )
                .is_err()
        );
        assert!(
            coordinator
                .claim_submission(v2, first, Quantity::from("10.0000"), None)
                .is_err()
        );
        assert_eq!(coordinator.identity(&v1), Some(first));
        assert_eq!(
            coordinator.request_cancel(first, None),
            CancelAdmission::Deferred
        );
    }

    #[rstest]
    fn identities_are_not_capacity_evicted() {
        let coordinator = LocalOrderCoordinator::new();
        let original_venue = VenueOrderId::from("V-ORIGINAL");
        let original = identity("C-ORIGINAL", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, original_venue, original, Quantity::from("1"));

        for index in 0..10_001 {
            let venue = VenueOrderId::from(format!("V-{index}").as_str());
            let client = format!("C-{index}");
            claim(
                &coordinator,
                venue,
                identity(&client, "I-1.POLYMARKET", OrderSide::Buy),
                Quantity::from("1"),
            );
        }

        assert_eq!(coordinator.identity(&original_venue), Some(original));
    }

    #[rstest]
    fn fill_admission_is_typed_and_atomic() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));

        assert!(matches!(
            coordinator.admit_fill_report(fill("V-1", "I-2.POLYMARKET", OrderSide::Buy, "2.0000")),
            ArtifactAdmission::Conflict(_)
        ));
        assert_eq!(
            coordinator.cumulative_filled_for_test(&venue),
            Some(Quantity::from("0.0000"))
        );

        let ArtifactAdmission::Owned { artifact, identity } =
            coordinator.admit_fill_report(fill("V-1", "I-1.POLYMARKET", OrderSide::Buy, "2.0000"))
        else {
            panic!("matching fill should be owned");
        };
        assert_eq!(identity, expected);
        assert_eq!(artifact.client_order_id, Some(expected.client_order_id));
        assert_eq!(
            coordinator.cumulative_filled_for_test(&venue),
            Some(Quantity::from("2.0000"))
        );
    }

    #[rstest]
    fn every_fill_identity_component_is_admitted_together() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));

        let mut wrong_instrument = fill("V-1", "I-2.POLYMARKET", OrderSide::Buy, "2.0000");
        wrong_instrument.client_order_id = Some(expected.client_order_id);
        let mut wrong_side = fill("V-1", "I-1.POLYMARKET", OrderSide::Sell, "2.0000");
        wrong_side.client_order_id = Some(expected.client_order_id);
        let mut wrong_client = fill("V-1", "I-1.POLYMARKET", OrderSide::Buy, "2.0000");
        wrong_client.client_order_id = Some(ClientOrderId::from("C-2"));

        for report in [wrong_instrument, wrong_side, wrong_client] {
            assert!(matches!(
                coordinator.admit_fill_report(report),
                ArtifactAdmission::Conflict(_)
            ));
        }
        assert_eq!(
            coordinator.cumulative_filled_for_test(&venue),
            Some(Quantity::from("0.0000"))
        );
    }

    #[rstest]
    fn order_report_admission_caps_only_after_identity_matches() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));
        let _ =
            coordinator.admit_fill_report(fill("V-1", "I-1.POLYMARKET", OrderSide::Buy, "3.0000"));

        assert!(matches!(
            coordinator.admit_order_report(order_report(
                "V-1",
                "I-1.POLYMARKET",
                OrderSide::Sell,
                "8.0000"
            )),
            ArtifactAdmission::Conflict(_)
        ));
        let ArtifactAdmission::Owned { artifact, .. } = coordinator.admit_order_report(
            order_report("V-1", "I-1.POLYMARKET", OrderSide::Buy, "8.0000"),
        ) else {
            panic!("matching order report should be owned");
        };
        assert_eq!(artifact.filled_qty, Quantity::from("3.0000"));
        assert_eq!(artifact.client_order_id, Some(expected.client_order_id));
    }

    #[rstest]
    fn every_venue_order_identity_component_is_admitted_together() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));

        let mut wrong_instrument = order_report("V-1", "I-2.POLYMARKET", OrderSide::Buy, "0.0000");
        wrong_instrument.client_order_id = Some(expected.client_order_id);
        let mut wrong_side = order_report("V-1", "I-1.POLYMARKET", OrderSide::Sell, "0.0000");
        wrong_side.client_order_id = Some(expected.client_order_id);
        let mut wrong_client = order_report("V-1", "I-1.POLYMARKET", OrderSide::Buy, "0.0000");
        wrong_client.client_order_id = Some(ClientOrderId::from("C-2"));
        let mut wrong_time_in_force =
            order_report("V-1", "I-1.POLYMARKET", OrderSide::Buy, "0.0000");
        wrong_time_in_force.client_order_id = Some(expected.client_order_id);
        wrong_time_in_force.time_in_force = TimeInForce::Fok;

        for report in [
            wrong_instrument,
            wrong_side,
            wrong_client,
            wrong_time_in_force,
        ] {
            assert!(matches!(
                coordinator.admit_order_report(report),
                ArtifactAdmission::Conflict(_)
            ));
        }
    }

    #[rstest]
    fn reconciliation_preserves_confirmed_quantity_and_restores_missing_order_type() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-FOK");
        let mut expected = identity("C-FOK", "I-1.POLYMARKET", OrderSide::Buy);
        expected.order_type = OrderType::Market;
        expected.time_in_force = TimeInForce::Fok;
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));

        let mut report = order_report("V-FOK", "I-1.POLYMARKET", OrderSide::Buy, "8.0000");
        report.time_in_force = TimeInForce::Fok;
        let admitted = coordinator.admit_reconciliation_order_reports(vec![report]);

        assert_eq!(admitted.conflicts, 0);
        assert_eq!(admitted.artifacts[0].filled_qty, Quantity::from("8.0000"));
        assert_eq!(admitted.artifacts[0].order_type, OrderType::Market);
        assert_eq!(
            admitted.artifacts[0].client_order_id,
            Some(expected.client_order_id)
        );
    }

    #[rstest]
    fn cancel_request_is_atomic_with_submission_acceptance() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        coordinator.begin_submission(expected).unwrap();
        assert_eq!(
            coordinator.request_cancel(expected, None),
            CancelAdmission::Deferred
        );
        coordinator
            .claim_submission(venue, expected, Quantity::from("10.0000"), None)
            .unwrap();

        let acceptance = coordinator.accept_submission(venue, expected).unwrap();

        assert!(acceptance.newly_accepted);
        assert!(acceptance.cancel_requested);
    }

    #[rstest]
    fn cancel_admission_distinguishes_pending_ready_and_conflict() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);

        assert_eq!(
            coordinator.request_cancel(expected, None),
            CancelAdmission::Deferred
        );
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));
        assert_eq!(
            coordinator.request_cancel(expected, None),
            CancelAdmission::Deferred
        );
        let acceptance = coordinator.accept_submission(venue, expected).unwrap();
        assert!(acceptance.cancel_requested);
        assert_eq!(
            coordinator.request_cancel(expected, None),
            CancelAdmission::Ready(venue)
        );
        assert_eq!(
            coordinator.request_cancel(identity("C-1", "I-2.POLYMARKET", OrderSide::Buy), None,),
            CancelAdmission::Conflict
        );
    }

    #[rstest]
    fn rejected_claim_retains_immutable_identity() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        coordinator.begin_submission(expected).unwrap();
        coordinator
            .claim_submission(venue, expected, Quantity::from("10.0000"), None)
            .unwrap();
        assert_eq!(
            coordinator.reject_submission(expected),
            Ok(SubmitRejection::Rejected)
        );

        assert_eq!(coordinator.identity(&venue), Some(expected));
        assert_eq!(
            coordinator.request_cancel(expected, Some(venue)),
            CancelAdmission::Conflict
        );
        assert!(
            coordinator
                .begin_submission(identity("C-1", "I-2.POLYMARKET", OrderSide::Buy))
                .is_err()
        );
    }

    #[rstest]
    fn restore_refreshes_mutable_state_without_replacing_identity() {
        let coordinator = LocalOrderCoordinator::new();
        let venue = VenueOrderId::from("V-1");
        let expected = identity("C-1", "I-1.POLYMARKET", OrderSide::Buy);
        claim(&coordinator, venue, expected, Quantity::from("10.0000"));

        coordinator
            .restore_order(
                venue,
                expected,
                Quantity::from("12.0000"),
                Quantity::from("5.0000"),
                Some(Price::from("0.5000")),
            )
            .unwrap();

        let snapshot = coordinator.snapshot(&venue).unwrap();
        assert_eq!(snapshot.identity, expected);
        assert_eq!(snapshot.quantity, Quantity::from("12.0000"));
        assert_eq!(snapshot.filled_qty, Quantity::from("5.0000"));
        assert_eq!(snapshot.price, Some(Price::from("0.5000")));
    }
}
