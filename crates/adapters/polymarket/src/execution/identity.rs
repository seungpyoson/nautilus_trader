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

//! Tracked own-order identity registry for the Polymarket execution client.
//!
//! The user WebSocket dispatch runs on a spawned task without cache access, so it cannot
//! resolve an [`OrderAny`](nautilus_model::orders::OrderAny) to build order events. The submit
//! path captures the identity fields needed to construct `OrderAccepted` / `OrderFilled` /
//! `OrderCanceled` / `OrderRejected` / `OrderExpired` directly, keyed by venue order ID, and the
//! dispatch consults this registry to emit events for tracked orders (reserving reports for
//! externally-managed orders and reconciliation).

use std::sync::{Mutex, MutexGuard};

use nautilus_common::cache::fifo::{FifoCache, FifoCacheMap};
use nautilus_core::MUTEX_POISONED;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
};

use super::order_fill_tracker::OrderFillTrackerMap;

/// Identity fields captured at submit so the cache-free WS dispatch can build order events.
///
/// `trader_id` and `account_id` are client-wide constants threaded from the dispatch context,
/// so they are not stored here. Fill-specific values (`last_qty`, `last_px`, `trade_id`,
/// `commission`) come from the venue trade payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderIdentity {
    pub client_order_id: ClientOrderId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

/// Complete identity carried by order-status artifacts and tracker state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderReportIdentity {
    pub client_order_id: Option<ClientOrderId>,
    pub instrument_id: InstrumentId,
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

impl OrderReportIdentity {
    pub(crate) fn from_order(order: &OrderAny) -> Self {
        Self {
            client_order_id: Some(order.client_order_id()),
            instrument_id: order.instrument_id(),
            order_side: order.order_side(),
            order_type: order.order_type(),
            time_in_force: order.time_in_force(),
        }
    }

    pub(crate) fn from_report(report: &OrderStatusReport) -> Self {
        Self {
            client_order_id: report.client_order_id,
            instrument_id: report.instrument_id,
            order_side: report.order_side,
            order_type: report.order_type,
            time_in_force: report.time_in_force,
        }
    }

    fn agrees_with_registered(self, registered: OrderIdentity) -> bool {
        self.instrument_id == registered.instrument_id
            && self.order_side == registered.order_side
            && self.order_type == registered.order_type
            && self.time_in_force == registered.time_in_force
            && self
                .client_order_id
                .is_none_or(|client| client == registered.client_order_id)
    }
}

/// Opaque proof that a tracker identity was checked while holding the one
/// registry/tracker registration gate.
pub(crate) struct TrackerRegistrationPermit<'a> {
    _guard: MutexGuard<'a, ()>,
    pub(crate) venue_order_id: VenueOrderId,
    pub(crate) identity: OrderReportIdentity,
}

impl OrderIdentity {
    /// Captures the identity from an order held by the submit path.
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

    fn report_identity(self) -> OrderReportIdentity {
        OrderReportIdentity {
            client_order_id: Some(self.client_order_id),
            instrument_id: self.instrument_id,
            order_side: self.order_side,
            order_type: self.order_type,
            time_in_force: self.time_in_force,
        }
    }

    /// Returns true when any taker fill implies full completion.
    ///
    /// FOK is atomic, so a sub-cent difference between its registered and filled quantities is
    /// normalization. IOC maps to venue FAK: every positive remainder is canceled.
    pub(crate) fn requires_terminal_quantity_normalization(&self) -> bool {
        self.time_in_force == TimeInForce::Fok
    }
}

/// Shared registry of tracked own-order identities, keyed by venue order ID.
///
/// Populated by the submit path (which holds the `OrderAny`) and consulted by the WS dispatch
/// and buffer-drain paths. The `accepted` set deduplicates `OrderAccepted` so acceptance is
/// emitted exactly once across the submit confirmation and the WS stream, including when a fill
/// or cancel races ahead of the acceptance message.
#[derive(Debug, Default)]
pub(crate) struct OrderIdentityRegistry {
    registration_gate: Mutex<()>,
    inner: Mutex<RegistryInner>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderIdentityConflict;

#[derive(Debug, Default)]
struct RegistryInner {
    identities: FifoCacheMap<VenueOrderId, OrderIdentity, 10_000>,
    client_to_venue: FifoCacheMap<ClientOrderId, VenueOrderId, 10_000>,
    accepted: FifoCache<VenueOrderId, 10_000>,
}

impl OrderIdentityRegistry {
    /// Records the identity for a tracked order under its venue order ID.
    pub(crate) fn register_order_identity(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
        fill_tracker: &OrderFillTrackerMap,
    ) -> Result<(), OrderIdentityConflict> {
        let _registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        if fill_tracker
            .order_identity_matches(&venue_order_id, identity.report_identity())
            .is_some_and(|matches| !matches)
        {
            return Err(OrderIdentityConflict);
        }
        self.register_order_identity_inner(venue_order_id, identity)
    }

    fn register_order_identity_inner(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
    ) -> Result<(), OrderIdentityConflict> {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard
            .identities
            .get(&venue_order_id)
            .is_some_and(|registered| *registered != identity)
            || guard
                .client_to_venue
                .get(&identity.client_order_id)
                .is_some_and(|registered| *registered != venue_order_id)
        {
            return Err(OrderIdentityConflict);
        }
        guard.identities.insert(venue_order_id, identity);
        guard
            .client_to_venue
            .insert(identity.client_order_id, venue_order_id);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn register_order_identity_for_test(
        &self,
        venue_order_id: VenueOrderId,
        identity: OrderIdentity,
    ) -> Result<(), OrderIdentityConflict> {
        self.register_order_identity_inner(venue_order_id, identity)
    }

    /// Serializes every tracker identity claim with registry registration.
    pub(crate) fn admit_tracker_registration(
        &self,
        venue_order_id: VenueOrderId,
        mut identity: OrderReportIdentity,
    ) -> Result<TrackerRegistrationPermit<'_>, OrderIdentityConflict> {
        let registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        let client_order_id = resolve_order_request_in(
            &guard,
            venue_order_id,
            identity.instrument_id,
            identity.client_order_id,
            None,
        )?;
        if guard
            .identities
            .get(&venue_order_id)
            .copied()
            .is_some_and(|registered| !identity.agrees_with_registered(registered))
        {
            return Err(OrderIdentityConflict);
        }
        drop(guard);
        identity.client_order_id = client_order_id;
        Ok(TrackerRegistrationPermit {
            _guard: registration,
            venue_order_id,
            identity,
        })
    }

    /// Returns the identity for a tracked order, if known.
    pub(crate) fn get(&self, venue_order_id: &VenueOrderId) -> Option<OrderIdentity> {
        let _registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .identities
            .get(venue_order_id)
            .copied()
    }

    /// Resolves the client identity only when every registered identity agrees.
    ///
    /// Returns an error on any client, instrument, or venue contradiction.
    /// The complete check uses one registry lock so the two indexes cannot drift
    /// between independent reads.
    pub(crate) fn resolve_order_request(
        &self,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        requested_client_order_id: Option<ClientOrderId>,
        indexed_client_order_id: Option<ClientOrderId>,
    ) -> Result<Option<ClientOrderId>, OrderIdentityConflict> {
        let _registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        let guard = self.inner.lock().expect(MUTEX_POISONED);
        resolve_order_request_in(
            &guard,
            venue_order_id,
            instrument_id,
            requested_client_order_id,
            indexed_client_order_id,
        )
    }

    /// Admits fill economics only when registry and tracker identity agree.
    ///
    /// A venue order absent from both stores is genuinely external. If the
    /// registry has aged out first, tracker-owned instrument/client identity
    /// must still agree before the fill can use tracker quantity.
    pub(crate) fn resolve_fill_report(
        &self,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        order_side: OrderSide,
        fill_tracker: &OrderFillTrackerMap,
    ) -> Result<Option<OrderIdentity>, OrderIdentityConflict> {
        let _registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        let identity = {
            let guard = self.inner.lock().expect(MUTEX_POISONED);
            resolve_order_request_in(&guard, venue_order_id, instrument_id, client_order_id, None)?;
            let identity = guard.identities.get(&venue_order_id).copied();
            if identity.is_some_and(|identity| identity.order_side != order_side) {
                return Err(OrderIdentityConflict);
            }
            identity
        };
        if fill_tracker
            .fill_identity_matches(&venue_order_id, instrument_id, client_order_id, order_side)
            .is_some_and(|matches| !matches)
        {
            return Err(OrderIdentityConflict);
        }
        Ok(identity)
    }

    /// Admits an order report only when registry and tracker identity agree.
    pub(crate) fn resolve_order_status_report(
        &self,
        report: &OrderStatusReport,
        fill_tracker: &OrderFillTrackerMap,
    ) -> Result<Option<OrderIdentity>, OrderIdentityConflict> {
        let _registration = self.registration_gate.lock().expect(MUTEX_POISONED);
        let report_identity = OrderReportIdentity::from_report(report);
        let identity = {
            let guard = self.inner.lock().expect(MUTEX_POISONED);
            resolve_order_request_in(
                &guard,
                report.venue_order_id,
                report.instrument_id,
                report.client_order_id,
                None,
            )?;
            let identity = guard.identities.get(&report.venue_order_id).copied();
            if identity.is_some_and(|identity| !report_identity.agrees_with_registered(identity)) {
                return Err(OrderIdentityConflict);
            }
            identity
        };
        fill_tracker
            .cumulative_filled_for_report(report)
            .map_err(|_| OrderIdentityConflict)?;
        Ok(identity)
    }

    /// Returns the latest venue order ID captured for a tracked client order.
    pub(crate) fn venue_order_id(&self, client_order_id: &ClientOrderId) -> Option<VenueOrderId> {
        self.inner
            .lock()
            .expect(MUTEX_POISONED)
            .client_to_venue
            .get(client_order_id)
            .copied()
    }

    /// Marks acceptance as emitted, returning `true` only when this call newly marks it.
    ///
    /// Callers emit `OrderAccepted` only on a `true` result, so acceptance is emitted once
    /// across the submit confirmation and the WS stream.
    pub(crate) fn mark_accepted(&self, venue_order_id: VenueOrderId) -> bool {
        let mut guard = self.inner.lock().expect(MUTEX_POISONED);
        if guard.accepted.contains(&venue_order_id) {
            false
        } else {
            guard.accepted.add(venue_order_id);
            true
        }
    }
}

fn resolve_order_request_in(
    guard: &RegistryInner,
    venue_order_id: VenueOrderId,
    instrument_id: InstrumentId,
    requested_client_order_id: Option<ClientOrderId>,
    indexed_client_order_id: Option<ClientOrderId>,
) -> Result<Option<ClientOrderId>, OrderIdentityConflict> {
    let registered_identity = guard.identities.get(&venue_order_id).copied();
    if registered_identity.is_some_and(|identity| identity.instrument_id != instrument_id) {
        return Err(OrderIdentityConflict);
    }

    let registered_client_order_id = registered_identity.map(|identity| identity.client_order_id);
    let known_client_order_ids = [
        requested_client_order_id,
        indexed_client_order_id,
        registered_client_order_id,
    ];
    let resolved_client_order_id = known_client_order_ids.iter().flatten().next().copied();
    if resolved_client_order_id.is_some_and(|expected| {
        known_client_order_ids
            .iter()
            .flatten()
            .any(|known| *known != expected)
    }) {
        return Err(OrderIdentityConflict);
    }

    if resolved_client_order_id.is_some_and(|client_order_id| {
        guard
            .client_to_venue
            .get(&client_order_id)
            .is_some_and(|known| *known != venue_order_id)
    }) {
        return Err(OrderIdentityConflict);
    }

    Ok(resolved_client_order_id)
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        enums::OrderStatus, identifiers::AccountId, reports::OrderStatusReport, types::Quantity,
    };
    use rstest::rstest;

    use super::*;
    use crate::execution::order_fill_tracker::OrderFillTrackerMap;

    fn test_identity() -> OrderIdentity {
        OrderIdentity {
            client_order_id: ClientOrderId::from("O-1"),
            strategy_id: StrategyId::from("S-1"),
            instrument_id: InstrumentId::from("TEST.POLYMARKET"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn test_report(time_in_force: TimeInForce) -> OrderStatusReport {
        OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            Some(ClientOrderId::from("O-1")),
            VenueOrderId::from("V-1"),
            OrderSide::Buy,
            OrderType::Limit,
            time_in_force,
            OrderStatus::Accepted,
            Quantity::from("10"),
            Quantity::zero(0),
            UnixNanos::default(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
    }

    #[rstest]
    fn test_register_and_get() {
        let registry = OrderIdentityRegistry::default();
        let vid = VenueOrderId::from("V-1");
        assert!(registry.get(&vid).is_none());

        registry
            .register_order_identity_for_test(vid, test_identity())
            .expect("first identity must register");
        let identity = registry.get(&vid).expect("identity registered");
        assert_eq!(identity.client_order_id, ClientOrderId::from("O-1"));
        assert_eq!(identity.order_side, OrderSide::Buy);
        assert_eq!(
            registry.venue_order_id(&ClientOrderId::from("O-1")),
            Some(vid)
        );
    }

    #[rstest]
    fn test_registered_identity_is_immutable() {
        let registry = OrderIdentityRegistry::default();
        let venue_order_id = VenueOrderId::from("V-1");
        let identity = test_identity();
        registry
            .register_order_identity_for_test(venue_order_id, identity)
            .expect("first identity must register");

        let conflicting = OrderIdentity {
            instrument_id: InstrumentId::from("OTHER.POLYMARKET"),
            ..identity
        };
        assert_eq!(
            registry.register_order_identity_for_test(venue_order_id, conflicting),
            Err(OrderIdentityConflict)
        );
        assert_eq!(registry.get(&venue_order_id), Some(identity));
    }

    #[rstest]
    fn test_fill_identity_uses_tracker_identity_after_registry_eviction() {
        let registry = OrderIdentityRegistry::default();
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("V-1");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        tracker.register(
            venue_order_id,
            Quantity::from("10"),
            OrderSide::Buy,
            instrument_id,
            4,
            4,
        );

        assert_eq!(
            registry.resolve_fill_report(
                venue_order_id,
                instrument_id,
                None,
                OrderSide::Buy,
                &tracker,
            ),
            Ok(None)
        );
        assert_eq!(
            registry.resolve_fill_report(
                venue_order_id,
                InstrumentId::from("OTHER.POLYMARKET"),
                None,
                OrderSide::Buy,
                &tracker,
            ),
            Err(OrderIdentityConflict)
        );
    }

    #[rstest]
    fn test_registration_cannot_replace_tracker_identity_after_registry_eviction() {
        let registry = OrderIdentityRegistry::default();
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("V-EVICTED");
        tracker.register(
            venue_order_id,
            Quantity::from("10"),
            OrderSide::Buy,
            InstrumentId::from("ORIGINAL.POLYMARKET"),
            4,
            4,
        );
        let conflicting = OrderIdentity {
            instrument_id: InstrumentId::from("REPLACEMENT.POLYMARKET"),
            ..test_identity()
        };

        assert_eq!(
            registry.register_order_identity(venue_order_id, conflicting, &tracker),
            Err(OrderIdentityConflict)
        );
        assert!(registry.get(&venue_order_id).is_none());
    }

    #[rstest]
    fn test_order_report_admission_uses_type_and_time_in_force() {
        let registry = OrderIdentityRegistry::default();
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("V-1");
        registry
            .register_order_identity_for_test(venue_order_id, test_identity())
            .expect("identity must register");

        assert_eq!(
            registry.resolve_order_status_report(&test_report(TimeInForce::Fok), &tracker),
            Err(OrderIdentityConflict)
        );
    }

    #[rstest]
    fn test_tracker_claim_and_registry_registration_are_serialized() {
        let registry = OrderIdentityRegistry::default();
        let tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("V-RACE");
        let permit = registry
            .admit_tracker_registration(
                venue_order_id,
                OrderReportIdentity {
                    client_order_id: Some(ClientOrderId::from("O-B")),
                    instrument_id: InstrumentId::from("B.POLYMARKET"),
                    order_side: OrderSide::Sell,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::Ioc,
                },
            )
            .expect("first tracker claimant must acquire the gate");

        std::thread::scope(|scope| {
            let registration = scope.spawn(|| {
                registry.register_order_identity(venue_order_id, test_identity(), &tracker)
            });
            tracker
                .register_and_take_pending_fills(&permit, Quantity::from("10"))
                .expect("permitted tracker identity must register");
            drop(permit);
            assert_eq!(registration.join().unwrap(), Err(OrderIdentityConflict));
        });
        assert!(registry.get(&venue_order_id).is_none());
    }

    #[rstest]
    fn test_mark_accepted_is_idempotent() {
        let registry = OrderIdentityRegistry::default();
        let vid = VenueOrderId::from("V-1");

        assert!(registry.mark_accepted(vid), "first mark is new");
        assert!(!registry.mark_accepted(vid), "second mark is a no-op");
    }
}
