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

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_common::live::get_runtime;
use nautilus_core::{MUTEX_POISONED, UUID4, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderSide, OrderStatus},
    events::{OrderEventAny, OrderUpdated},
    identifiers::{AccountId, VenueOrderId},
    orders::{Order, OrderAny},
    types::Quantity,
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use super::{
    cancellations::execute_deferred_cancel,
    identity::{OrderIdentity, OrderIdentityRegistry, OrderReportIdentity, SubmitRejection},
    order_fill_tracker::OrderFillTrackerMap,
    parse::parse_order_status_report,
    pending::PendingCancelTracker,
    reconciliation::cap_order_report_filled_qty,
    submitter::OrderSubmitter,
    types::BatchLimitOrderContext,
};
use crate::http::query::OrderResponse;

#[expect(clippy::too_many_arguments)]
pub(super) async fn handle_batch_order_responses(
    responses: Vec<OrderResponse>,
    batch_orders: Vec<BatchLimitOrderContext>,
    expected_venue_order_ids: Vec<VenueOrderId>,
    submitter: &OrderSubmitter,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    order_identities: &OrderIdentityRegistry,
    pending_cancels: &PendingCancelTracker,
    pending_tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let response_len = responses.len();
    let order_len = batch_orders.len();

    if response_len != order_len {
        log::warn!(
            "Batch submit response length ({response_len}) does not match order count ({order_len})"
        );
    }

    let mut deferred = Vec::new();

    for ((batch_order, expected_venue_order_id), response) in batch_orders
        .iter()
        .zip(&expected_venue_order_ids)
        .zip(responses)
    {
        if let Some((order_id_str, venue_order_id)) = handle_order_response(
            Ok(response),
            &batch_order.order,
            *expected_venue_order_id,
            emitter,
            clock,
            order_identities,
            pending_cancels,
        ) {
            deferred.push((batch_order.order.clone(), order_id_str, venue_order_id));
        }
    }

    if order_len > response_len {
        for (batch_order, expected_venue_order_id) in batch_orders
            .iter()
            .zip(expected_venue_order_ids)
            .skip(response_len)
        {
            if let Some((order_id_str, venue_order_id)) = handle_unknown_submit_result(
                &batch_order.order,
                expected_venue_order_id,
                "batch response omitted order",
                order_identities,
                pending_cancels,
            ) {
                deferred.push((batch_order.order.clone(), order_id_str, venue_order_id));
            }
        }
    }

    if !deferred.is_empty() {
        let mut tasks = pending_tasks.lock().expect(MUTEX_POISONED);

        tasks.retain(|handle| !handle.is_finished());

        for (order, order_id_str, venue_order_id) in deferred {
            let submitter = submitter.clone();
            let emitter = emitter.clone();
            let pending_cancels = pending_cancels.clone();

            let handle = get_runtime().spawn(async move {
                execute_deferred_cancel(
                    &submitter,
                    &order,
                    &order_id_str,
                    venue_order_id,
                    &emitter,
                    &pending_cancels,
                    clock,
                )
                .await;
            });
            tasks.push(handle);
        }
    }
}

pub(super) fn reject_submit_order(
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    pending_cancels: &PendingCancelTracker,
) {
    let ts_now = clock.get_time_ns();
    emitter.emit_order_rejected(order, reason, ts_now, is_post_only_crossing(reason));
    pending_cancels.remove(&order.client_order_id());
}

pub(super) fn reject_registered_submit(
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    reason: &str,
    order_identities: &OrderIdentityRegistry,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    pending_cancels: &PendingCancelTracker,
) -> bool {
    match order_identities
        .reject_submit_response(expected_venue_order_id, OrderIdentity::from_order(order))
    {
        Ok(SubmitRejection::Rejected) => {
            reject_submit_order(order, reason, emitter, clock, pending_cancels);
            false
        }
        Ok(SubmitRejection::AlreadyRejected) => false,
        Ok(SubmitRejection::AlreadyAccepted) => {
            log::warn!(
                "Submit failure for {expected_venue_order_id} arrived after WebSocket acceptance; preserving acceptance"
            );
            pending_cancels.contains(&order.client_order_id())
        }
        Err(_) => {
            log::error!(
                "Submit failure for {expected_venue_order_id} contradicts a different identity; refusing a false rejection"
            );
            false
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) fn emit_market_order_submitted(
    order: &mut OrderAny,
    is_quote_qty: bool,
    side: OrderSide,
    amount: Quantity,
    expected_base_qty: Decimal,
    update_quantity: bool,
    size_precision: u8,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) {
    emitter.emit_order_submitted(order);

    if !update_quantity || expected_base_qty.is_zero() {
        return;
    }

    let Ok(base_qty) = Quantity::from_decimal_dp(expected_base_qty, size_precision) else {
        return;
    };

    if base_qty == order.quantity() && !order.is_quote_quantity() {
        return;
    }

    log::debug!(
        "Normalized {} {side:?} {} quantity {amount} to signed base quantity {base_qty}",
        order.instrument_id(),
        if is_quote_qty { "quote" } else { "base" },
    );

    let ts_now = clock.get_time_ns();
    let updated = OrderUpdated::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        base_qty,
        UUID4::new(),
        ts_now,
        ts_now,
        false,
        order.venue_order_id(),
        order.account_id(),
        order.price(),
        None,
        None,
        false,
    );

    let event = OrderEventAny::Updated(updated);
    emitter.send_order_event(event.clone());

    if let Err(e) = order.apply(event) {
        log::error!("Failed to apply signed base-quantity OrderUpdated: {e}");
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn handle_single_order_response(
    result: crate::http::error::Result<OrderResponse>,
    batch_order: BatchLimitOrderContext,
    expected_venue_order_id: VenueOrderId,
    submitter: &OrderSubmitter,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    order_identities: &OrderIdentityRegistry,
    pending_cancels: &PendingCancelTracker,
) {
    match result {
        Ok(response) => {
            if let Some((order_id_str, venue_order_id)) = handle_order_response(
                Ok(response),
                &batch_order.order,
                expected_venue_order_id,
                emitter,
                clock,
                order_identities,
                pending_cancels,
            ) {
                execute_deferred_cancel(
                    submitter,
                    &batch_order.order,
                    &order_id_str,
                    venue_order_id,
                    emitter,
                    pending_cancels,
                    clock,
                )
                .await;
            }
        }
        Err(e) if e.is_submit_outcome_unknown() => {
            if let Some((order_id_str, venue_order_id)) = handle_unknown_submit_result(
                &batch_order.order,
                expected_venue_order_id,
                &e.to_string(),
                order_identities,
                pending_cancels,
            ) {
                execute_deferred_cancel(
                    submitter,
                    &batch_order.order,
                    &order_id_str,
                    venue_order_id,
                    emitter,
                    pending_cancels,
                    clock,
                )
                .await;
            }
        }
        Err(e) => {
            if reject_registered_submit(
                &batch_order.order,
                expected_venue_order_id,
                &format!("{e}"),
                order_identities,
                emitter,
                clock,
                pending_cancels,
            ) {
                execute_deferred_cancel(
                    submitter,
                    &batch_order.order,
                    expected_venue_order_id.as_str(),
                    expected_venue_order_id,
                    emitter,
                    pending_cancels,
                    clock,
                )
                .await;
            }
        }
    }
}

pub(super) fn handle_unknown_submit_result(
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    reason: &str,
    order_identities: &OrderIdentityRegistry,
    pending_cancels: &PendingCancelTracker,
) -> Option<(String, VenueOrderId)> {
    if order_identities
        .mark_outcome_unknown(expected_venue_order_id)
        .is_err()
    {
        log::error!(
            "Unknown submit outcome for {expected_venue_order_id} contradicts submission state"
        );
        return None;
    }
    log::warn!(
        "Submit outcome unknown for {}: {reason}. Tracking expected venue order ID {}",
        order.client_order_id(),
        expected_venue_order_id
    );

    if pending_cancels.contains(&order.client_order_id()) {
        let order_id_str = expected_venue_order_id.to_string();
        return Some((order_id_str, expected_venue_order_id));
    }

    None
}

pub(super) fn handle_order_response(
    result: crate::http::error::Result<OrderResponse>,
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    order_identities: &OrderIdentityRegistry,
    pending_cancels: &PendingCancelTracker,
) -> Option<(String, VenueOrderId)> {
    match result {
        Ok(response) => {
            if response.success {
                // VenueOrderId panics on an empty string
                if let Some(order_id) = response.order_id.filter(|s| !s.is_empty()) {
                    let venue_order_id = VenueOrderId::from(order_id.as_str());
                    if venue_order_id != expected_venue_order_id {
                        if reject_registered_submit(
                            order,
                            expected_venue_order_id,
                            &format!(
                                "Venue returned order ID {venue_order_id}, expected {expected_venue_order_id}"
                            ),
                            order_identities,
                            emitter,
                            clock,
                            pending_cancels,
                        ) {
                            return Some((
                                expected_venue_order_id.to_string(),
                                expected_venue_order_id,
                            ));
                        }
                        return None;
                    }
                    let ts_now = clock.get_time_ns();
                    match order_identities.mark_accepted(venue_order_id) {
                        Ok(true) => emitter.emit_order_accepted(order, venue_order_id, ts_now),
                        Ok(false) => {}
                        Err(_) => {
                            log::error!(
                                "Accepted venue order {venue_order_id} was already rejected; refusing submit response"
                            );
                            return None;
                        }
                    }

                    if pending_cancels.contains(&order.client_order_id()) {
                        log::debug!(
                            "Order {} has pending cancel, issuing deferred cancel for {}",
                            order.client_order_id(),
                            venue_order_id
                        );
                        return Some((order_id, venue_order_id));
                    }
                } else if let Some(reason) = response.error_msg.filter(|s| !s.is_empty()) {
                    // Batch endpoint reports a rejected leg as success=true with an empty orderID; reason in error_msg
                    if reject_registered_submit(
                        order,
                        expected_venue_order_id,
                        &reason,
                        order_identities,
                        emitter,
                        clock,
                        pending_cancels,
                    ) {
                        return Some((
                            expected_venue_order_id.to_string(),
                            expected_venue_order_id,
                        ));
                    }
                } else {
                    return handle_unknown_submit_result(
                        order,
                        expected_venue_order_id,
                        "successful response omitted order ID and rejection reason",
                        order_identities,
                        pending_cancels,
                    );
                }
            } else {
                let reason = response
                    .error_msg
                    .unwrap_or_else(|| "unknown error".to_string());
                if reject_registered_submit(
                    order,
                    expected_venue_order_id,
                    &reason,
                    order_identities,
                    emitter,
                    clock,
                    pending_cancels,
                ) {
                    return Some((expected_venue_order_id.to_string(), expected_venue_order_id));
                }
            }
        }
        Err(e) => {
            if reject_registered_submit(
                order,
                expected_venue_order_id,
                &format!("HTTP request failed: {e}"),
                order_identities,
                emitter,
                clock,
                pending_cancels,
            ) {
                return Some((expected_venue_order_id.to_string(), expected_venue_order_id));
            }
        }
    }
    None
}

// Require both terms so only a post-only crossing matches, not any post-only reason
fn is_post_only_crossing(reason: &str) -> bool {
    reason.contains("post-only") && reason.contains("cross")
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn check_fok_status(
    submitter: &OrderSubmitter,
    order_id: &str,
    expected_asset_id: &str,
    order: &OrderAny,
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &OrderIdentityRegistry,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    size_precision: u8,
    price_precision: u8,
    clock: &'static AtomicTime,
) {
    const FOK_CHECK_DELAY: Duration = Duration::from_secs(5);

    tokio::time::sleep(FOK_CHECK_DELAY).await;

    let venue_order_id = VenueOrderId::from(order_id);
    match fill_tracker
        .has_recorded_fills_for_identity(&venue_order_id, OrderReportIdentity::from_order(order))
    {
        Ok(true) => return,
        Ok(false) => {}
        Err(_) => {
            log::error!(
                "Tracker identity contradicts FOK order {venue_order_id}; deferring reconciliation"
            );
            return;
        }
    }

    log::warn!("FOK order {order_id} unresolved after 5s, checking REST status");

    let venue_order = match submitter.get_order(order_id).await {
        Ok(Some(o)) => o,
        Ok(None) => {
            log::debug!("FOK order {order_id} not found (empty response), WS will reconcile");
            return;
        }
        Err(e) => {
            log::warn!("FOK status check failed for {order_id}: {e}");
            return;
        }
    };

    if venue_order.id != order_id || venue_order.asset_id.as_str() != expected_asset_id {
        log::error!(
            "FOK status answer for {order_id} returned order {} and asset {}; expected order {order_id} and asset {expected_asset_id}; deferring reconciliation",
            venue_order.id,
            venue_order.asset_id,
        );
        return;
    }

    let ts_now = clock.get_time_ns();
    let mut report = parse_order_status_report(
        &venue_order,
        order.instrument_id(),
        account_id,
        Some(order.client_order_id()),
        price_precision,
        size_precision,
        ts_now,
    );
    if order_identities
        .resolve_order_status_report(&report, fill_tracker)
        .is_err()
    {
        log::error!(
            "FOK status answer for {venue_order_id} contradicts the complete local order identity; deferring reconciliation"
        );
        return;
    }
    let order_status = report.order_status;

    match order_status {
        OrderStatus::Rejected => {
            log::debug!("FOK order {order_id} resolved via REST as Rejected");
            emitter.emit_order_rejected(order, "FOK order unfilled", ts_now, false);
        }
        OrderStatus::Canceled => {
            log::debug!("FOK order {order_id} resolved via REST as Canceled");
            emitter.emit_order_canceled(order, Some(venue_order_id), ts_now);
        }
        OrderStatus::Expired => {
            log::debug!("FOK order {order_id} resolved via REST as Expired");
            emitter.emit_order_expired(order, Some(venue_order_id), ts_now);
        }
        OrderStatus::Filled => {
            let confirmed_filled = match fill_tracker.cumulative_filled_for_report(&report) {
                Ok(filled) => filled.unwrap_or_else(|| Quantity::zero(size_precision)),
                Err(_) => {
                    log::error!(
                        "Tracker identity contradicts FOK order {venue_order_id}; deferring reconciliation"
                    );
                    return;
                }
            };
            cap_order_report_filled_qty(&mut report, confirmed_filled, None);

            log::debug!(
                "FOK order {order_id} resolved via REST as Filled; deferring fill quantity until confirmation"
            );
            emitter.send_order_status_report(report);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::ExecutionEvent;
    use nautilus_core::{UnixNanos, collections::AtomicMap};
    use nautilus_model::{
        enums::{AccountType, TimeInForce},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId},
        instruments::{Instrument, InstrumentAny},
        orders::{LimitOrder, MarketOrder, Order, stubs::TestOrderEventStubs},
        types::{Currency, Price},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        common::enums::{
            PolymarketEventType, PolymarketLiquiditySide, PolymarketOrderSide, PolymarketOutcome,
            PolymarketTradeStatus,
        },
        http::{
            models::GammaMarket,
            parse::{create_instrument_from_def, parse_gamma_market},
        },
        websocket::{
            dispatch::{WsDispatchContext, WsDispatchState, dispatch_user_message},
            messages::{PolymarketUserOrder, PolymarketUserTrade, UserWsMessage},
        },
    };

    fn load<T: serde::de::DeserializeOwned>(filename: &str) -> T {
        let path = format!("test_data/{filename}");
        let content = std::fs::read_to_string(path).expect("failed to read test data");
        serde_json::from_str(&content).expect("failed to parse test data")
    }

    fn test_instrument() -> InstrumentAny {
        let market: GammaMarket = load("gamma_market.json");
        let defs = parse_gamma_market(&market).unwrap();
        create_instrument_from_def(&defs[0], UnixNanos::from(1_000_000_000u64)).unwrap()
    }

    fn test_emitter() -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let mut emitter = ExecutionEventEmitter::new(
            nautilus_core::time::get_atomic_clock_realtime(),
            TraderId::from("TESTER-001"),
            AccountId::from("POLY-001"),
            AccountType::Cash,
            Some(Currency::pUSD()),
        );
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        (emitter, receiver)
    }

    fn test_limit_order(client_order_id: &str, instrument_id: InstrumentId) -> OrderAny {
        OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            ClientOrderId::from(client_order_id),
            OrderSide::Buy,
            Quantity::new(10.0, 0),
            Price::new(0.50, 4),
            TimeInForce::Gtc,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        ))
    }

    #[rstest]
    fn test_market_sell_submission_uses_signed_wire_quantity() {
        let instrument_id = InstrumentId::from("TEST-TOKEN.POLYMARKET");
        let mut order = OrderAny::Market(MarketOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            ClientOrderId::from("O-MARKET-SELL"),
            OrderSide::Sell,
            Quantity::from("5.208000"),
            TimeInForce::Fok,
            UUID4::new(),
            UnixNanos::default(),
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let (emitter, mut receiver) = test_emitter();
        let venue_order_id = VenueOrderId::from("0xmarket-sell");
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let order_identities = OrderIdentityRegistry::default();
        let pending_cancels = PendingCancelTracker::default();
        emit_market_order_submitted(
            &mut order,
            false,
            OrderSide::Sell,
            Quantity::from("5.208000"),
            Decimal::new(5_200_000, 6),
            true,
            6,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
        );
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");
        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        let deferred_cancel = handle_order_response(
            Ok(response),
            &order,
            venue_order_id,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &order_identities,
            &pending_cancels,
        );

        let submitted = receiver.try_recv().expect("expected submitted event");
        let updated = receiver.try_recv().expect("expected quantity update");
        let accepted = receiver.try_recv().expect("expected accepted event");
        fill_tracker.record_fill(&venue_order_id, Quantity::from("5.200000"));

        assert!(matches!(
            submitted,
            ExecutionEvent::Order(OrderEventAny::Submitted(_))
        ));
        assert!(matches!(
            updated,
            ExecutionEvent::Order(OrderEventAny::Updated(_))
        ));
        assert!(matches!(
            accepted,
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));
        assert!(deferred_cancel.is_none());
        assert_eq!(order.quantity(), Quantity::from("5.200000"));
        assert_eq!(order.filled_qty(), Quantity::zero(6));
        assert!(fill_tracker.is_fully_filled(&venue_order_id));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    #[case(PolymarketTradeStatus::Matched)]
    #[case(PolymarketTradeStatus::Mined)]
    #[case(PolymarketTradeStatus::Retrying)]
    #[case(PolymarketTradeStatus::Failed)]
    fn test_non_confirmed_rest_trade_does_not_generate_fill_report(
        #[case] status: PolymarketTradeStatus,
    ) {
        let instrument = test_instrument();
        let mut trade: crate::http::models::PolymarketTradeReport = load("http_trade_report.json");
        trade.status = status;

        let instruments = AtomicMap::new();
        instruments.insert(trade.asset_id, instrument);
        let ctx = crate::execution::reconciliation::FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
            api_key: "00000000-0000-0000-0000-000000000001",
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        };

        let (reports, _) = crate::execution::reconciliation::build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            crate::execution::reconciliation::FillReportQuery {
                instrument_filter: None,
                venue_order_filter: None,
                start: None,
                end: None,
                ts_init: UnixNanos::from(1_000_000_000u64),
            },
        );

        assert!(reports.is_empty());
    }

    #[rstest]
    fn test_unknown_submit_tracks_expected_id_for_ws_order_recovery() {
        let ws_order: PolymarketUserOrder = load("ws_user_order_placement.json");
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let order = test_limit_order("O-UNKNOWN-WS", instrument_id);
        let expected_venue_order_id = VenueOrderId::from(ws_order.id.as_str());
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                expected_venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        assert!(
            handle_unknown_submit_result(
                &order,
                expected_venue_order_id,
                "transport timeout",
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        assert_eq!(
            order_identities
                .get(&expected_venue_order_id)
                .map(|identity| identity.client_order_id),
            Some(order.client_order_id()),
        );

        let token_instruments = AtomicMap::new();
        token_instruments.insert(ws_order.asset_id, instrument);
        let mut state = WsDispatchState::default();
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };

        dispatch_user_message(&UserWsMessage::Order(ws_order), &ctx, &mut state);

        // The tracked own order emits an OrderAccepted event, not a report.
        let event = receiver.try_recv().expect("expected accepted event");
        match event {
            ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) => {
                assert_eq!(accepted.client_order_id, order.client_order_id());
            }
            other => panic!("expected accepted event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_deferred_cancel_survives_websocket_acceptance_before_unknown_http_result() {
        let instrument = test_instrument();
        let order = test_limit_order("O-WS-ACCEPT-CANCEL", instrument.id());
        let venue_order_id = VenueOrderId::from("V-WS-ACCEPT-CANCEL");
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");
        assert_eq!(order_identities.mark_accepted(venue_order_id), Ok(true));
        pending_cancels.insert(order.client_order_id());

        assert_eq!(
            handle_unknown_submit_result(
                &order,
                venue_order_id,
                "HTTP result lost after WebSocket acceptance",
                &order_identities,
                &pending_cancels,
            ),
            Some((venue_order_id.to_string(), venue_order_id))
        );
    }

    #[rstest]
    fn test_deferred_cancel_survives_websocket_acceptance_before_negative_http_result() {
        let instrument = test_instrument();
        let order = test_limit_order("O-WS-ACCEPT-ERROR", instrument.id());
        let venue_order_id = VenueOrderId::from("V-WS-ACCEPT-ERROR");
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");
        assert_eq!(order_identities.mark_accepted(venue_order_id), Ok(true));
        pending_cancels.insert(order.client_order_id());

        assert!(
            reject_registered_submit(
                &order,
                venue_order_id,
                "definitive submit failure",
                &order_identities,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &pending_cancels,
            ),
            "the caller must execute the already-accepted order's pending cancel"
        );
        assert!(
            receiver.try_recv().is_err(),
            "acceptance must not become rejection"
        );
    }

    fn test_taker_trade(
        asset_id: Ustr,
        venue_order_id: VenueOrderId,
        size: &str,
        price: &str,
    ) -> PolymarketUserTrade {
        PolymarketUserTrade {
            asset_id,
            bucket_index: 0,
            fee_rate_bps: "0".to_string(),
            id: "trade-race".to_string(),
            last_update: "1700000001".to_string(),
            maker_address: Ustr::from("0xmaker"),
            maker_orders: vec![],
            market: Ustr::from("0xmarket"),
            match_time: "1700000000".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            price: price.to_string(),
            side: PolymarketOrderSide::Buy,
            size: size.to_string(),
            status: PolymarketTradeStatus::Confirmed,
            taker_order_id: venue_order_id.as_str().to_string(),
            timestamp: "1700000000000".to_string(),
            trade_owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            trader_side: PolymarketLiquiditySide::Taker,
            event_type: PolymarketEventType::Trade,
        }
    }

    // A fast-filling marketable limit order whose WS taker trade arrives before the HTTP submit
    // response: deterministic pre-registration emits it immediately. A later FAILED update must
    // still find and void that fill.
    #[rstest]
    fn test_ws_taker_fill_before_submit_response_emits_immediately_and_can_be_voided() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let asset_id = instrument_id.symbol.inner();
        let size_precision = instrument.size_precision();
        let price_precision = instrument.price_precision();
        let account_id = AccountId::from("POLY-001");
        let venue_order_id = VenueOrderId::from("0xrace-taker-fill");

        let mut order = OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            ClientOrderId::from("O-RACE-FILL"),
            OrderSide::Buy,
            Quantity::new(5.192100, size_precision),
            Price::new(0.963, price_precision),
            TimeInForce::Fok,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        ));
        order
            .apply(TestOrderEventStubs::submitted(&order, account_id))
            .unwrap();

        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        // Step 1: the WS taker trade arrives BEFORE the submit response. The deterministic local
        // identity and tracker state already exist, so it emits without a one-shot drain race.
        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id,
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();
        let mut trade = test_taker_trade(asset_id, venue_order_id, "5.192081", "0.963");
        trade.status = PolymarketTradeStatus::Matched;
        dispatch_user_message(&UserWsMessage::Trade(trade.clone()), &ctx, &mut state);

        // Step 2: the submit response confirms the identity without duplicating either event.
        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        assert!(
            handle_order_response(
                Ok(response),
                &order,
                venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        let accepted = match receiver.try_recv().expect("expected accepted event") {
            ExecutionEvent::Order(event @ OrderEventAny::Accepted(_)) => event,
            other => panic!("expected accepted event, was {other:?}"),
        };
        let filled = match receiver.try_recv().expect("expected filled event") {
            ExecutionEvent::Order(event @ OrderEventAny::Filled(_)) => {
                if let OrderEventAny::Filled(ref fill) = event {
                    assert_eq!(fill.venue_order_id, venue_order_id);
                    assert_eq!(fill.last_qty, Quantity::new(5.192081, size_precision));
                    let info = fill.info.as_ref().expect("expected trade metadata");
                    assert_eq!(info.get(&Ustr::from("id")), Some(&Ustr::from("trade-race")));
                }
                event
            }
            other => panic!("expected filled event, was {other:?}"),
        };
        let filled_event_id = match &filled {
            OrderEventAny::Filled(fill) => fill.event_id,
            _ => unreachable!(),
        };
        order.apply(accepted).unwrap();
        order.apply(filled).unwrap();
        assert_eq!(order.status(), OrderStatus::PartiallyFilled);
        assert!(receiver.try_recv().is_err());

        trade.status = PolymarketTradeStatus::Failed;
        assert!(dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state).is_some());
        let voided = match receiver.try_recv().expect("expected fill correction") {
            ExecutionEvent::Order(OrderEventAny::FillVoided(event)) => event,
            other => panic!("expected fill-void event, was {other:?}"),
        };

        assert_eq!(voided.trade_id, TradeId::from("trade-race"));
        assert_eq!(voided.voided_qty, Quantity::new(5.192081, size_precision));
        assert_eq!(voided.causation_id, Some(filled_event_id));
        assert!(receiver.try_recv().is_err());
    }

    // A WS terminal report can arrive before the HTTP response because deterministic identity and
    // tracker state are installed before submission. It closes the exact local order immediately;
    // the later HTTP response is idempotent.
    #[rstest]
    fn test_ws_order_report_before_submit_response_reaches_canceled() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let account_id = AccountId::from("POLY-001");
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        let mut order = test_limit_order("O-RACE-CANCEL", instrument_id);
        order
            .apply(TestOrderEventStubs::submitted(&order, account_id))
            .unwrap();

        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        // Step 1: the submit's expected identity is already registered as pending, so a WS cancel
        // arriving before the HTTP response can claim and close that exact order immediately.
        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id,
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();
        dispatch_user_message(&UserWsMessage::Order(cancel_order), &ctx, &mut state);

        // Step 2: the later submit response is idempotent and emits no duplicate lifecycle event.
        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        assert!(
            handle_order_response(
                Ok(response),
                &order,
                venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        let accepted = match receiver.try_recv().expect("expected accepted event") {
            ExecutionEvent::Order(event @ OrderEventAny::Accepted(_)) => event,
            other => panic!("expected accepted event, was {other:?}"),
        };
        let canceled = match receiver.try_recv().expect("expected canceled event") {
            ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) => event,
            other => panic!("expected canceled event, was {other:?}"),
        };

        // Applying the WS events carries the order to Canceled before the HTTP response.
        order.apply(accepted).unwrap();
        order.apply(canceled).unwrap();
        assert_eq!(order.status(), OrderStatus::Canceled);
    }

    // Polymarket fills a marketable BUY by spending a USDC amount, so the share fill can exceed the
    // nominal order qty (here 12 vs 10) when it executes below the limit. The adapter must raise the
    // order qty to the actual fill (OrderUpdated) before OrderFilled, otherwise the engine drops the
    // fill as an overfill and the order orphans.
    #[rstest]
    fn test_ws_taker_overfill_bumps_order_qty_then_fills() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let asset_id = instrument_id.symbol.inner();
        let size_precision = instrument.size_precision();
        let price_precision = instrument.price_precision();
        let account_id = AccountId::from("POLY-001");
        let venue_order_id = VenueOrderId::from("0xoverfill-buy");

        let mut order = OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            ClientOrderId::from("O-OVERFILL"),
            OrderSide::Buy,
            Quantity::new(10.0, size_precision),
            Price::new(0.50, price_precision),
            TimeInForce::Fok,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        ));
        order
            .apply(TestOrderEventStubs::submitted(&order, account_id))
            .unwrap();

        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        // WS taker fill of 12 shares (the marketable BUY filled below its limit) before the response.
        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id,
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();
        dispatch_user_message(
            &UserWsMessage::Trade(test_taker_trade(asset_id, venue_order_id, "12", "0.50")),
            &ctx,
            &mut state,
        );

        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        handle_order_response(
            Ok(response),
            &order,
            venue_order_id,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &order_identities,
            &pending_cancels,
        );

        let accepted = match receiver.try_recv().expect("expected accepted event") {
            ExecutionEvent::Order(event @ OrderEventAny::Accepted(_)) => event,
            other => panic!("expected accepted event, was {other:?}"),
        };
        // The overfill must raise the order qty to 12 before the fill is applied.
        let updated = match receiver.try_recv().expect("expected updated event") {
            ExecutionEvent::Order(event @ OrderEventAny::Updated(_)) => {
                if let OrderEventAny::Updated(ref u) = event {
                    assert_eq!(u.quantity, Quantity::new(12.0, size_precision));
                }
                event
            }
            other => panic!("expected updated event raising qty to the fill, was {other:?}"),
        };
        let filled = match receiver.try_recv().expect("expected filled event") {
            ExecutionEvent::Order(event @ OrderEventAny::Filled(_)) => {
                if let OrderEventAny::Filled(ref fill) = event {
                    assert_eq!(fill.last_qty, Quantity::new(12.0, size_precision));
                }
                event
            }
            other => panic!("expected filled event, was {other:?}"),
        };

        order.apply(accepted).unwrap();
        order.apply(updated).unwrap();
        order.apply(filled).unwrap();
        assert_eq!(order.quantity(), Quantity::new(12.0, size_precision));
        assert_eq!(order.status(), OrderStatus::Filled);
    }

    // An empty orderID with no reason is ambiguous: retain the deterministic identity as an
    // outcome-unknown order so cancellation and reconciliation can still address it.
    #[rstest]
    fn test_batch_leg_empty_order_id_no_reason_does_not_panic() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let order = test_limit_order("O-BATCH-EMPTY", instrument_id);
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        let expected_venue_order_id = VenueOrderId::from("V-BATCH-EMPTY");
        order_identities
            .register_pending_order_identity(
                expected_venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");
        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: None,
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                expected_venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        assert_eq!(
            order_identities.venue_order_id(&order.client_order_id()),
            Some(expected_venue_order_id)
        );
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_submit_response_refuses_a_different_venue_order_id() {
        let instrument = test_instrument();
        let order = test_limit_order("O-ID-MISMATCH", instrument.id());
        let expected_venue_order_id = VenueOrderId::from("V-EXPECTED");
        let returned_venue_order_id = VenueOrderId::from("V-RETURNED");
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities
            .register_pending_order_identity(
                expected_venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        let response = OrderResponse {
            success: true,
            order_id: Some(returned_venue_order_id.to_string()),
            error_msg: None,
        };
        assert!(
            handle_order_response(
                Ok(response),
                &order,
                expected_venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        assert!(matches!(
            receiver.try_recv(),
            Ok(ExecutionEvent::Order(OrderEventAny::Rejected(_)))
        ));
        assert!(receiver.try_recv().is_err());
        assert!(order_identities.get(&expected_venue_order_id).is_some());
        assert_eq!(
            order_identities.mark_accepted(expected_venue_order_id),
            Err(crate::execution::identity::OrderIdentityConflict)
        );
        assert!(order_identities.get(&returned_venue_order_id).is_none());
    }

    // The batch endpoint reports a rejected leg as success=true with an empty orderID and the reason
    // in error_msg (live: a naked SELL rejected for no balance). Surface it as OrderRejected fast,
    // carrying due_post_only when the reason is a post-only crossing.
    #[rstest]
    #[case("not enough balance / allowance: the balance is not enough", false)]
    #[case("invalid post-only order: order crosses book", true)]
    fn test_batch_leg_empty_order_id_with_reason_rejects(
        #[case] reason: &str,
        #[case] expected_post_only: bool,
    ) {
        let instrument = test_instrument();
        let order = test_limit_order("O-BATCH-REJECT", instrument.id());
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        let venue_order_id = VenueOrderId::from("V-BATCH-REJECT");
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        match receiver.try_recv().expect("expected rejected event") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.reason.as_str(), reason);
                assert_eq!(event.due_post_only, expected_post_only);
            }
            other => panic!("expected rejected event, was {other:?}"),
        }
    }

    // A post-only limit rejected for crossing the book must surface due_post_only=true so strategies
    // can distinguish it from other venue rejections; any other reason stays false.
    #[rstest]
    #[case("invalid post-only order: order crosses book", true)]
    #[case("not enough balance / allowance", false)]
    fn test_submit_reject_flags_post_only_crossing(
        #[case] reason: &str,
        #[case] expected_post_only: bool,
    ) {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let order = test_limit_order("O-REJECT", instrument_id);
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        let venue_order_id = VenueOrderId::from("V-REJECT");
        order_identities
            .register_pending_order_identity(
                venue_order_id,
                OrderIdentity::from_order(&order),
                order.quantity(),
                &fill_tracker,
            )
            .expect("pending identity must register");

        let response = OrderResponse {
            success: false,
            order_id: None,
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                venue_order_id,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &order_identities,
                &pending_cancels,
            )
            .is_none()
        );

        match receiver.try_recv().expect("expected rejected event") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.reason.as_str(), reason);
                assert_eq!(event.due_post_only, expected_post_only);
            }
            other => panic!("expected rejected event, was {other:?}"),
        }
    }

    // Live path: a single-order post-only crossing rejection arrives as an HTTP 400 error and is
    // emitted via reject_submit_order, not the success=false branch, so the flag must be set here
    // too. The reason carries the venue message that the HTTP path wraps.
    #[rstest]
    #[case("invalid post-only order: order crosses book", true)]
    #[case("invalid post-only order: unsupported tick size", false)]
    #[case("not enough balance / allowance", false)]
    fn test_reject_submit_order_flags_post_only_crossing(
        #[case] reason: &str,
        #[case] expected_post_only: bool,
    ) {
        let instrument = test_instrument();
        let order = test_limit_order("O-REJECT-SUBMIT", instrument.id());
        let (emitter, mut receiver) = test_emitter();
        let pending_cancels = PendingCancelTracker::default();
        pending_cancels.insert(order.client_order_id());

        reject_submit_order(
            &order,
            reason,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &pending_cancels,
        );

        match receiver.try_recv().expect("expected rejected event") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.reason.as_str(), reason);
                assert_eq!(event.due_post_only, expected_post_only);
            }
            other => panic!("expected rejected event, was {other:?}"),
        }

        // The reject funnel clears any tracked pending cancel for the order
        assert!(!pending_cancels.contains(&order.client_order_id()));
    }
}
