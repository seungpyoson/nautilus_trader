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

use std::{collections::HashMap, sync::Mutex};

use nautilus_core::MUTEX_POISONED;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    events::OrderFilled,
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    reports::{FillReport, OrderStatusReport},
    types::Quantity,
};
use rust_decimal::Decimal;

use super::identity::OrderReportIdentity;
use crate::common::consts::DUST_SNAP_THRESHOLD_DEC;

/// Cumulative fill state for a single order.
#[derive(Debug, Clone, Copy)]
struct OrderFillState {
    submitted_qty: Quantity,
    cumulative_filled: Quantity,
    order_side: OrderSide,
    order_type: OrderType,
    time_in_force: TimeInForce,
    instrument_id: InstrumentId,
    client_order_id: Option<ClientOrderId>,
    terminal_adjustment_applied: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TrackerIdentityConflict;

/// Process-lifetime identity and quantity state for locally submitted orders.
#[derive(Debug, Default)]
struct TrackerInner {
    orders: HashMap<VenueOrderId, OrderFillState>,
}

/// Tracks per-order fill accumulation and terminal quantity normalization.
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

    pub(super) fn restore_registered_order(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderReportIdentity,
        submitted_qty: Quantity,
        filled_qty: Quantity,
    ) -> Result<(), TrackerIdentityConflict> {
        let mut state = new_order_state(submitted_qty, identity);
        state.cumulative_filled = filled_qty;
        register_order_state(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            state,
            true,
        )
    }

    /// Installs local fill state before the network submit begins.
    ///
    /// The identity registry holds its registration gate while calling this method, so a local
    /// WebSocket artifact can never observe a registered identity without its tracker state.
    pub(super) fn register_pending_order(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderReportIdentity,
        submitted_qty: Quantity,
    ) -> Result<(), TrackerIdentityConflict> {
        register_order_state(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            new_order_state(submitted_qty, identity),
            false,
        )
    }

    /// Returns true if the order has been registered (accepted).
    pub(crate) fn contains(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .contains_key(venue_order_id)
    }

    /// Returns whether a fill agrees with tracker-owned identity, or `None`
    /// when the venue order is genuinely untracked.
    pub(crate) fn fill_identity_matches(
        &self,
        venue_order_id: &VenueOrderId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        order_side: OrderSide,
    ) -> Option<bool> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|state| artifact_matches_state(state, instrument_id, client_order_id, order_side))
    }

    pub(crate) fn order_identity_matches(
        &self,
        venue_order_id: &VenueOrderId,
        identity: OrderReportIdentity,
    ) -> Option<bool> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|state| order_identity_matches_state(state, identity))
    }

    /// Validates tracker-owned identity and applies dust snapping atomically.
    /// Untracked venue orders pass through as external reconciliation reports.
    pub(crate) fn admit_and_snap_fill_report(&self, report: &mut FillReport) -> bool {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id) else {
            return true;
        };
        if !fill_report_matches_state(state, report) {
            return false;
        }
        report.client_order_id = state.client_order_id;
        report.last_qty = snap_fill_qty_in(&guard.orders, &report.venue_order_id, report.last_qty);
        true
    }

    /// Returns fill evidence only when the complete retained identity matches the local order.
    /// Missing state is not settlement evidence; callers must query the venue.
    pub(crate) fn has_recorded_fills_for_identity(
        &self,
        venue_order_id: &VenueOrderId,
        identity: OrderReportIdentity,
    ) -> Result<bool, TrackerIdentityConflict> {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(venue_order_id) else {
            return Ok(false);
        };
        if !order_identity_matches_state(state, identity) {
            return Err(TrackerIdentityConflict);
        }
        Ok(!state.cumulative_filled.is_zero())
    }

    /// Returns the cumulative filled quantity for an order, if tracked.
    #[cfg(test)]
    pub(crate) fn get_cumulative_filled(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .map(|s| s.cumulative_filled)
    }

    /// Returns tracker quantity only when the complete order-report identity agrees.
    pub(crate) fn cumulative_filled_for_report(
        &self,
        report: &OrderStatusReport,
    ) -> Result<Option<Quantity>, TrackerIdentityConflict> {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id) else {
            return Ok(None);
        };
        if !order_report_matches_state(state, report) {
            return Err(TrackerIdentityConflict);
        }
        Ok(Some(state.cumulative_filled))
    }

    /// Returns whether cumulative fills have reached the submitted quantity, after validating the
    /// complete report identity against tracker-owned state.
    pub(crate) fn is_fully_filled_for_report(
        &self,
        report: &OrderStatusReport,
    ) -> Result<bool, TrackerIdentityConflict> {
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id) else {
            return Ok(false);
        };
        if !order_report_matches_state(state, report) {
            return Err(TrackerIdentityConflict);
        }
        Ok(state.cumulative_filled >= state.submitted_qty)
    }

    /// Returns `true` if cumulative fills have reached the submitted quantity.
    #[cfg(test)]
    pub(crate) fn is_fully_filled(&self, venue_order_id: &VenueOrderId) -> bool {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .get(venue_order_id)
            .is_some_and(|s| s.cumulative_filled >= s.submitted_qty)
    }

    /// Records a fill only when its complete local tracker identity agrees.
    pub(crate) fn admit_fill_report(
        &self,
        venue_order_id: VenueOrderId,
        mut report: FillReport,
    ) -> Option<FillReport> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if let Some(state) = guard.orders.get(&venue_order_id).copied() {
            if !fill_report_matches_state(&state, &report) {
                log::error!(
                    "Fill for venue order {venue_order_id} contradicts tracked order identity; dropping it"
                );
                return None;
            }
            report.client_order_id = state.client_order_id;
            report.last_qty = snap_fill_qty_in(&guard.orders, &venue_order_id, report.last_qty);
            record_fill_in(&mut guard.orders, &venue_order_id, report.last_qty);
            Some(report)
        } else if report.client_order_id.is_some() {
            log::error!(
                "Local fill for venue order {venue_order_id} has no tracker state; dropping it"
            );
            None
        } else {
            Some(report)
        }
    }

    pub(crate) fn reverse_fill_report(&self, report: &FillReport) {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(&report.venue_order_id) else {
            return;
        };
        if fill_report_matches_state(state, report) {
            reverse_fill_in(&mut guard.orders, &report.venue_order_id, report.last_qty);
        }
    }

    pub(crate) fn reverse_order_fill(&self, fill: &OrderFilled) {
        reverse_order_fill_in(&mut self.inner.lock().expect(MUTEX_POISONED).orders, fill);
    }

    /// Snap each report's `last_qty` against the registered submitted quantity
    /// for its `venue_order_id`. Reports for orders the tracker does not know
    /// about (e.g. orders from another session) pass through unchanged.
    ///
    /// Commission is intentionally not recomputed: it tracks the venue charge
    /// from the on-chain fill, which is independent of our local snap.
    #[cfg(test)]
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
    #[cfg(test)]
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
    pub(crate) fn buy_overfill_bump(&self, venue_order_id: &VenueOrderId) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        buy_overfill_bump_in(&mut guard.orders, venue_order_id)
    }

    /// Returns the venue-filled quantity when a terminal order has sub-cent-share leaves.
    ///
    /// The returned quantity is used for an order-only reconciliation update. It is not a fill and
    /// must not change positions, balances, or commissions. The adjustment is marked in the
    /// process-lifetime entry so repeated terminal messages are idempotent without discarding
    /// identity.
    #[cfg(test)]
    pub(crate) fn check_terminal_quantity_normalization(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        check_terminal_quantity_normalization_in(&mut guard.orders, venue_order_id)
    }

    pub(crate) fn check_terminal_quantity_normalization_for_identity(
        &self,
        venue_order_id: &VenueOrderId,
        identity: OrderReportIdentity,
    ) -> Result<Option<Quantity>, TrackerIdentityConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(venue_order_id) else {
            return Ok(None);
        };
        if !order_identity_matches_state(state, identity) {
            return Err(TrackerIdentityConflict);
        }
        Ok(check_terminal_quantity_normalization_in(
            &mut guard.orders,
            venue_order_id,
        ))
    }

    /// Returns the real unfilled remainder of a terminal IOC order.
    ///
    /// The adjustment is marked so duplicate `CONFIRMED` trade messages cannot emit repeated
    /// cancellations. The caller must use this only after a taker trade confirms: that proves the
    /// FAK order has finished matching and the venue has killed the returned remainder.
    #[cfg(test)]
    pub(crate) fn take_terminal_ioc_remainder(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<Quantity> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        take_terminal_ioc_remainder_in(&mut guard.orders, venue_order_id)
    }

    pub(crate) fn take_terminal_ioc_remainder_for_identity(
        &self,
        venue_order_id: &VenueOrderId,
        identity: OrderReportIdentity,
    ) -> Result<Option<Quantity>, TrackerIdentityConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        let Some(state) = guard.orders.get(venue_order_id) else {
            return Ok(None);
        };
        if !order_identity_matches_state(state, identity) {
            return Err(TrackerIdentityConflict);
        }
        Ok(take_terminal_ioc_remainder_in(
            &mut guard.orders,
            venue_order_id,
        ))
    }
}

fn check_terminal_quantity_normalization_in(
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
) -> Option<Quantity> {
    let state = orders.get_mut(venue_order_id)?;
    if state.terminal_adjustment_applied {
        return None;
    }
    if state.cumulative_filled >= state.submitted_qty {
        return None;
    }
    let leaves = state.submitted_qty.as_decimal() - state.cumulative_filled.as_decimal();
    if leaves > Decimal::ZERO && leaves < DUST_SNAP_THRESHOLD_DEC {
        let filled_qty = state.cumulative_filled;
        let submitted_qty = state.submitted_qty;
        log::debug!(
            "Normalizing terminal order {venue_order_id} quantity from {submitted_qty} to \
             {filled_qty} (non-economic leaves={leaves})"
        );
        state.terminal_adjustment_applied = true;
        Some(filled_qty)
    } else {
        if leaves >= DUST_SNAP_THRESHOLD_DEC {
            log::debug!(
                "Order {venue_order_id} MATCHED with significant residual {leaves} \
                 (filled {}/{})",
                state.cumulative_filled,
                state.submitted_qty,
            );
        }
        None
    }
}

fn take_terminal_ioc_remainder_in(
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
) -> Option<Quantity> {
    let state = orders.get_mut(venue_order_id)?;
    if state.terminal_adjustment_applied
        || state.cumulative_filled.is_zero()
        || state.cumulative_filled >= state.submitted_qty
    {
        return None;
    }
    let remainder = state.submitted_qty - state.cumulative_filled;
    state.terminal_adjustment_applied = true;
    Some(remainder)
}

fn new_order_state(submitted_qty: Quantity, identity: OrderReportIdentity) -> OrderFillState {
    OrderFillState {
        submitted_qty,
        cumulative_filled: Quantity::zero(submitted_qty.precision),
        order_side: identity.order_side,
        order_type: identity.order_type,
        time_in_force: identity.time_in_force,
        instrument_id: identity.instrument_id,
        client_order_id: identity.client_order_id,
        terminal_adjustment_applied: false,
    }
}

fn buy_overfill_bump_in(
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
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

fn artifact_matches_state(
    state: &OrderFillState,
    instrument_id: InstrumentId,
    client_order_id: Option<ClientOrderId>,
    order_side: OrderSide,
) -> bool {
    artifact_identities_agree(
        state.instrument_id,
        state.client_order_id,
        state.order_side,
        instrument_id,
        client_order_id,
        order_side,
    )
}

fn artifact_identities_agree(
    left_instrument_id: InstrumentId,
    left_client_order_id: Option<ClientOrderId>,
    left_order_side: OrderSide,
    right_instrument_id: InstrumentId,
    right_client_order_id: Option<ClientOrderId>,
    right_order_side: OrderSide,
) -> bool {
    left_instrument_id == right_instrument_id
        && left_order_side == right_order_side
        && (left_client_order_id.is_none()
            || right_client_order_id.is_none()
            || left_client_order_id == right_client_order_id)
}

fn fill_report_matches_state(state: &OrderFillState, report: &FillReport) -> bool {
    artifact_matches_state(
        state,
        report.instrument_id,
        report.client_order_id,
        report.order_side,
    )
}

fn order_report_matches_state(state: &OrderFillState, report: &OrderStatusReport) -> bool {
    artifact_matches_state(
        state,
        report.instrument_id,
        report.client_order_id,
        report.order_side,
    ) && state.time_in_force == report.time_in_force
}

fn order_identity_matches_state(state: &OrderFillState, identity: OrderReportIdentity) -> bool {
    artifact_matches_state(
        state,
        identity.instrument_id,
        identity.client_order_id,
        identity.order_side,
    ) && state.order_type == identity.order_type
        && state.time_in_force == identity.time_in_force
}

fn register_order_state(
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
    venue_order_id: VenueOrderId,
    mut incoming: OrderFillState,
    replace_filled_qty: bool,
) -> Result<(), TrackerIdentityConflict> {
    if let Some(current) = orders.get_mut(&venue_order_id) {
        if !artifact_matches_state(
            current,
            incoming.instrument_id,
            incoming.client_order_id,
            incoming.order_side,
        ) || current.order_type != incoming.order_type
            || current.time_in_force != incoming.time_in_force
        {
            return Err(TrackerIdentityConflict);
        }
        if current.client_order_id.is_none() {
            current.client_order_id = incoming.client_order_id;
        }
        current.submitted_qty = incoming.submitted_qty;
        if replace_filled_qty {
            current.cumulative_filled = incoming.cumulative_filled;
        }
        return Ok(());
    }

    if !replace_filled_qty {
        incoming.cumulative_filled = Quantity::zero(incoming.submitted_qty.precision);
    }
    orders.insert(venue_order_id, incoming);
    Ok(())
}

#[cfg(test)]
impl OrderFillTrackerMap {
    pub(crate) fn remove_order_for_test(&self, venue_order_id: &VenueOrderId) {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .orders
            .remove(venue_order_id);
    }

    pub(crate) fn restore_order_for_test(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        filled_qty: Quantity,
        order_side: OrderSide,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> Result<(), TrackerIdentityConflict> {
        self.restore_registered_order(
            venue_order_id,
            OrderReportIdentity {
                client_order_id: Some(client_order_id),
                instrument_id,
                order_side,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
            submitted_qty,
            filled_qty,
        )
    }

    /// Registers an order directly, for tests that set up an already-accepted order.
    pub(crate) fn register(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        order_side: OrderSide,
        instrument_id: InstrumentId,
        _size_precision: u8,
        _price_precision: u8,
    ) {
        self.register_identity_for_test(
            venue_order_id,
            submitted_qty,
            OrderReportIdentity {
                client_order_id: None,
                instrument_id,
                order_side,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        );
    }

    pub(crate) fn register_identity_for_test(
        &self,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
        identity: OrderReportIdentity,
    ) {
        register_order_state(
            &mut self.inner.lock().expect(MUTEX_POISONED).orders,
            venue_order_id,
            new_order_state(submitted_qty, identity),
            false,
        )
        .expect("test registration must not contradict tracked identity");
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
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
    venue_order_id: &VenueOrderId,
    qty: Quantity,
) {
    if let Some(s) = orders.get_mut(venue_order_id) {
        s.cumulative_filled = s.cumulative_filled + qty;
    }
}

fn reverse_fill_in(
    orders: &mut HashMap<VenueOrderId, OrderFillState>,
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

fn reverse_order_fill_in(orders: &mut HashMap<VenueOrderId, OrderFillState>, fill: &OrderFilled) {
    let Some(state) = orders.get(&fill.venue_order_id) else {
        return;
    };
    if artifact_matches_state(
        state,
        fill.instrument_id,
        Some(fill.client_order_id),
        fill.order_side,
    ) {
        reverse_fill_in(orders, &fill.venue_order_id, fill.last_qty);
    }
}

fn snap_fill_qty_in(
    orders: &HashMap<VenueOrderId, OrderFillState>,
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

    fn pusd() -> Currency {
        Currency::pUSD()
    }

    fn test_fill_report(
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        order_side: OrderSide,
    ) -> FillReport {
        FillReport {
            account_id: AccountId::from("POLY-001"),
            instrument_id,
            venue_order_id,
            trade_id: TradeId::from("trade-identity"),
            order_side,
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

    fn test_order_report(
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        order_side: OrderSide,
    ) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("POLY-001"),
            instrument_id,
            None,
            venue_order_id,
            order_side,
            nautilus_model::enums::OrderType::Limit,
            nautilus_model::enums::TimeInForce::Gtc,
            nautilus_model::enums::OrderStatus::Canceled,
            Quantity::new(10.0, 6),
            Quantity::zero(6),
            UnixNanos::default(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
    }

    #[rstest]
    fn test_fill_admission_rejects_tracked_side_conflict() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-side-conflict");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );

        let mut report = test_fill_report(venue_order_id, instrument_id, OrderSide::Sell);
        assert!(!tracker.admit_and_snap_fill_report(&mut report));
    }

    #[rstest]
    fn test_order_report_rejects_tracked_time_in_force_conflict() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-tif-conflict");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            instrument_id,
            6,
            2,
        );
        let mut report = test_order_report(venue_order_id, instrument_id, OrderSide::Buy);
        report.time_in_force = TimeInForce::Fok;

        assert_eq!(
            tracker.cumulative_filled_for_report(&report),
            Err(TrackerIdentityConflict)
        );
    }

    #[rstest]
    fn test_missing_tracker_state_is_not_fill_or_settlement_evidence() {
        let tracker = OrderFillTrackerMap::new();
        assert_eq!(
            tracker.has_recorded_fills_for_identity(
                &VenueOrderId::from("V-EVICTED"),
                OrderReportIdentity {
                    client_order_id: Some(ClientOrderId::from("O-EVICTED")),
                    instrument_id: InstrumentId::from("EVICTED.POLYMARKET"),
                    order_side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    time_in_force: TimeInForce::Fok,
                },
            ),
            Ok(false),
        );
    }

    #[rstest]
    fn test_late_rollback_cannot_cross_reused_order_identity() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("V-REUSED");
        let original_instrument = InstrumentId::from("ORIGINAL.POLYMARKET");
        let original_fill = test_fill_report(venue_order_id, original_instrument, OrderSide::Buy);
        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            original_instrument,
            6,
            2,
        );
        tracker.remove_order_for_test(&venue_order_id);

        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            InstrumentId::from("REPLACEMENT.POLYMARKET"),
            6,
            2,
        );
        tracker.record_fill(&venue_order_id, Quantity::new(5.0, 6));
        tracker.reverse_fill_report(&original_fill);

        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::new(5.0, 6))
        );
    }

    #[rstest]
    fn test_conflicting_registration_does_not_replace_tracker_identity() {
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("order-registration-conflict");
        let original_instrument = InstrumentId::from("ORIGINAL.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::new(10.0, 6),
            OrderSide::Buy,
            original_instrument,
            6,
            2,
        );
        tracker.record_fill(&venue_order_id, Quantity::new(5.0, 6));

        let replacement_identity = OrderReportIdentity {
            client_order_id: Some(ClientOrderId::from("O-CONFLICT")),
            instrument_id: InstrumentId::from("REPLACEMENT.POLYMARKET"),
            order_side: OrderSide::Sell,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        };
        assert!(matches!(
            tracker.register_pending_order(
                venue_order_id,
                replacement_identity,
                Quantity::new(20.0, 6),
            ),
            Err(TrackerIdentityConflict)
        ));

        let mut original = test_fill_report(venue_order_id, original_instrument, OrderSide::Buy);
        assert!(tracker.admit_and_snap_fill_report(&mut original));
        assert_eq!(
            tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::new(5.0, 6))
        );
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
    fn test_take_terminal_ioc_remainder_is_exact_idempotent_and_retains_identity() {
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
        assert!(tracker.contains(&vid));
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
    fn test_dust_settlement_is_idempotent_and_retains_identity() {
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

        assert!(tracker.contains(&vid));
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
