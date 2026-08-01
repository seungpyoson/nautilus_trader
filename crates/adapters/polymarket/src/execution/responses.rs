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

use std::{sync::Arc, time::Duration};

use nautilus_common::live::{get_runtime, task::TaskHandles};
use nautilus_core::{UUID4, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderSide, OrderStatus},
    events::{OrderEventAny, OrderUpdated},
    identifiers::{AccountId, VenueOrderId},
    orders::{Order, OrderAny},
    types::Quantity,
};
use rust_decimal::Decimal;

use super::{
    cancellations::execute_deferred_cancel,
    local_orders::{ArtifactAdmission, LocalOrderCoordinator, OrderIdentity, SubmitRejection},
    parse::parse_order_status_report,
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
    local_orders: &Arc<LocalOrderCoordinator>,
    pending_tasks: &Arc<TaskHandles>,
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
        .zip(expected_venue_order_ids.iter().copied())
        .zip(responses)
    {
        if let Some((order_id_str, venue_order_id)) = handle_order_response(
            Ok(response),
            expected_venue_order_id,
            &batch_order.order,
            emitter,
            clock,
            local_orders,
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
                local_orders,
            ) {
                deferred.push((batch_order.order.clone(), order_id_str, venue_order_id));
            }
        }
    }

    if !deferred.is_empty() {
        for (order, order_id_str, venue_order_id) in deferred {
            let submitter = submitter.clone();
            let emitter = emitter.clone();

            let handle = get_runtime().spawn(async move {
                execute_deferred_cancel(
                    &submitter,
                    &order,
                    &order_id_str,
                    venue_order_id,
                    &emitter,
                    clock,
                )
                .await;
            });
            pending_tasks.push(handle);
        }
    }
}

pub(super) fn reject_submit_order(
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &LocalOrderCoordinator,
) {
    let identity = OrderIdentity::from_order(order);
    if local_orders.reject_submission(identity).is_err() {
        log::error!(
            "Cannot remove rejected submission {}: local order identity conflict",
            order.client_order_id(),
        );
    }
    emit_submit_rejected(order, reason, emitter, clock, local_orders);
}

fn emit_submit_rejected(
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &LocalOrderCoordinator,
) {
    let ts_now = clock.get_time_ns();
    emitter.emit_order_rejected(order, reason, ts_now, is_post_only_crossing(reason));
    local_orders.take_deferred_cancel(OrderIdentity::from_order(order));
}

pub(super) fn deny_preparing_order(
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    local_orders: &LocalOrderCoordinator,
) {
    if local_orders
        .reject_submission(OrderIdentity::from_order(order))
        .is_err()
    {
        log::error!(
            "Cannot remove denied preparation {}: local order identity conflict",
            order.client_order_id(),
        );
    }
    emitter.emit_order_denied(order, reason);
    local_orders.take_deferred_cancel(OrderIdentity::from_order(order));
}

pub(super) fn reject_claimed_submit_order(
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &LocalOrderCoordinator,
) -> Option<(String, VenueOrderId)> {
    match local_orders.reject_submission(OrderIdentity::from_order(order)) {
        Ok(SubmitRejection::Removed) => {
            emit_submit_rejected(order, reason, emitter, clock, local_orders);
            None
        }
        Ok(SubmitRejection::AlreadyAccepted(venue_order_id)) => {
            log::debug!(
                "Ignoring negative submit response for already accepted order {}",
                order.client_order_id(),
            );
            local_orders
                .take_deferred_cancel(OrderIdentity::from_order(order))
                .then(|| (venue_order_id.to_string(), venue_order_id))
        }
        Err(_) => {
            log::error!(
                "Cannot reject {}: local order identity conflict",
                order.client_order_id(),
            );
            None
        }
    }
}

pub(super) async fn reject_claimed_submit_order_and_cancel(
    submitter: &OrderSubmitter,
    order: &OrderAny,
    reason: &str,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &Arc<LocalOrderCoordinator>,
) {
    if let Some((order_id, venue_order_id)) =
        reject_claimed_submit_order(order, reason, emitter, clock, local_orders)
    {
        execute_deferred_cancel(submitter, order, &order_id, venue_order_id, emitter, clock).await;
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

pub(super) async fn handle_single_order_response(
    result: crate::http::error::Result<OrderResponse>,
    batch_order: BatchLimitOrderContext,
    expected_venue_order_id: VenueOrderId,
    submitter: &OrderSubmitter,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &Arc<LocalOrderCoordinator>,
) {
    match result {
        Ok(response) => {
            if let Some((order_id_str, venue_order_id)) = handle_order_response(
                Ok(response),
                expected_venue_order_id,
                &batch_order.order,
                emitter,
                clock,
                local_orders,
            ) {
                execute_deferred_cancel(
                    submitter,
                    &batch_order.order,
                    &order_id_str,
                    venue_order_id,
                    emitter,
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
                local_orders,
            ) {
                execute_deferred_cancel(
                    submitter,
                    &batch_order.order,
                    &order_id_str,
                    venue_order_id,
                    emitter,
                    clock,
                )
                .await;
            }
        }
        Err(e) => {
            reject_submit_order(
                &batch_order.order,
                &format!("{e}"),
                emitter,
                clock,
                local_orders,
            );
        }
    }
}

pub(super) fn handle_unknown_submit_result(
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    reason: &str,
    local_orders: &Arc<LocalOrderCoordinator>,
) -> Option<(String, VenueOrderId)> {
    log::warn!(
        "Submit outcome unknown for {}: {reason}. Tracking expected venue order ID {}",
        order.client_order_id(),
        expected_venue_order_id
    );

    if local_orders
        .mark_outcome_unknown(expected_venue_order_id, OrderIdentity::from_order(order))
        .is_err()
    {
        log::error!(
            "Cannot retain unknown submit outcome for {}: local order identity conflict",
            order.client_order_id(),
        );
        return None;
    }
    if local_orders.take_deferred_cancel(OrderIdentity::from_order(order)) {
        let order_id_str = expected_venue_order_id.to_string();
        return Some((order_id_str, expected_venue_order_id));
    }

    None
}

pub(super) fn handle_order_response(
    result: crate::http::error::Result<OrderResponse>,
    expected_venue_order_id: VenueOrderId,
    order: &OrderAny,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    local_orders: &Arc<LocalOrderCoordinator>,
) -> Option<(String, VenueOrderId)> {
    match result {
        Ok(response) => {
            if response.success {
                // VenueOrderId panics on an empty string
                if let Some(order_id) = response.order_id.filter(|s| !s.is_empty()) {
                    let venue_order_id = VenueOrderId::from(order_id.as_str());
                    if venue_order_id != expected_venue_order_id {
                        return reject_claimed_submit_order(
                            order,
                            &format!(
                                "Venue returned order ID {venue_order_id}, expected signed order ID {expected_venue_order_id}"
                            ),
                            emitter,
                            clock,
                            local_orders,
                        );
                    }
                    let ts_now = clock.get_time_ns();
                    match local_orders
                        .accept_submission(venue_order_id, OrderIdentity::from_order(order))
                    {
                        Ok(true) => emitter.emit_order_accepted(order, venue_order_id, ts_now),
                        Ok(false) => {}
                        Err(_) => {
                            log::error!(
                                "Rejecting submit response for {venue_order_id}: local order identity conflict"
                            );
                            return None;
                        }
                    }

                    if local_orders.take_deferred_cancel(OrderIdentity::from_order(order)) {
                        log::debug!(
                            "Order {} has pending cancel, issuing deferred cancel for {}",
                            order.client_order_id(),
                            venue_order_id
                        );
                        return Some((order_id, venue_order_id));
                    }
                } else if let Some(reason) = response.error_msg.filter(|s| !s.is_empty()) {
                    // Batch endpoint reports a rejected leg as success=true with an empty orderID; reason in error_msg
                    if let Some(cancel) =
                        reject_claimed_submit_order(order, &reason, emitter, clock, local_orders)
                    {
                        return Some(cancel);
                    }
                } else {
                    log::warn!(
                        "Order accepted but no order_id returned for {}",
                        order.client_order_id()
                    );
                }
            } else {
                let reason = response
                    .error_msg
                    .unwrap_or_else(|| "unknown error".to_string());
                if let Some(cancel) =
                    reject_claimed_submit_order(order, &reason, emitter, clock, local_orders)
                {
                    return Some(cancel);
                }
            }
        }
        Err(e) => {
            if let Some(cancel) = reject_claimed_submit_order(
                order,
                &format!("HTTP request failed: {e}"),
                emitter,
                clock,
                local_orders,
            ) {
                return Some(cancel);
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
    local_orders: &Arc<LocalOrderCoordinator>,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    size_precision: u8,
    price_precision: u8,
    clock: &'static AtomicTime,
) {
    const FOK_CHECK_DELAY: Duration = Duration::from_secs(5);

    tokio::time::sleep(FOK_CHECK_DELAY).await;

    let venue_order_id = VenueOrderId::from(order_id);
    if local_orders.has_fills_or_settled(&venue_order_id) {
        return;
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
            "Rejecting FOK status response for {order_id}: returned identity does not match the submitted order"
        );
        return;
    }
    let ts_now = clock.get_time_ns();
    let report = parse_order_status_report(
        &venue_order,
        order.instrument_id(),
        account_id,
        Some(order.client_order_id()),
        price_precision,
        size_precision,
        ts_now,
    );
    let ArtifactAdmission::Owned {
        artifact: report, ..
    } = local_orders.admit_order_report(report)
    else {
        log::error!("Rejecting FOK status response for {order_id}: local order identity conflict");
        return;
    };

    match report.order_status {
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
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        instruments::{Instrument, InstrumentAny},
        orders::{LimitOrder, MarketOrder, Order},
        types::{Currency, Price},
    };
    use rstest::rstest;

    use super::*;
    use crate::{
        common::enums::PolymarketTradeStatus,
        http::{
            models::GammaMarket,
            parse::{create_instrument_from_def, parse_gamma_market},
        },
        websocket::{
            dispatch::{WsDispatchContext, WsDispatchState, dispatch_user_message},
            messages::{PolymarketUserOrder, UserWsMessage},
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

    fn claim_order(
        local_orders: &LocalOrderCoordinator,
        order: &OrderAny,
        venue_order_id: VenueOrderId,
    ) {
        local_orders
            .begin_submission(OrderIdentity::from_order(order))
            .expect("fixture order preparation should succeed");
        local_orders
            .claim_submission(
                venue_order_id,
                OrderIdentity::from_order(order),
                order.quantity(),
                order.price(),
            )
            .expect("fixture order claim should succeed");
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
        let local_orders = Arc::new(LocalOrderCoordinator::new());
        local_orders
            .begin_submission(OrderIdentity::from_order(&order))
            .unwrap();
        local_orders
            .claim_submission(
                venue_order_id,
                OrderIdentity::from_order(&order),
                Quantity::from("5.200000"),
                order.price(),
            )
            .unwrap();

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
        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        let deferred_cancel = handle_order_response(
            Ok(response),
            venue_order_id,
            &order,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &local_orders,
        );

        let submitted = receiver.try_recv().expect("expected submitted event");
        let updated = receiver.try_recv().expect("expected quantity update");
        let accepted = receiver.try_recv().expect("expected accepted event");
        local_orders.record_fill(&venue_order_id, Quantity::from("5.200000"));

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
        assert!(local_orders.is_fully_filled(&venue_order_id));
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
            None,
            UnixNanos::from(1_000_000_000u64),
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
        let local_orders = Arc::new(LocalOrderCoordinator::new());
        claim_order(&local_orders, &order, expected_venue_order_id);

        assert!(
            handle_unknown_submit_result(
                &order,
                expected_venue_order_id,
                "transport timeout",
                &local_orders,
            )
            .is_none()
        );

        assert_eq!(
            local_orders.admit_cancel(OrderIdentity::from_order(&order)),
            crate::execution::local_orders::CancelAdmission::Ready(expected_venue_order_id)
        );

        let token_instruments = AtomicMap::new();
        token_instruments.insert(ws_order.asset_id, instrument);
        let mut state = WsDispatchState::default();
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
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

        assert!(local_orders.identity(&expected_venue_order_id).is_some());
    }

    #[rstest]
    fn negative_http_result_preserves_ws_acceptance_and_releases_deferred_cancel() {
        let instrument = test_instrument();
        let order = test_limit_order("O-ACCEPTED-RACE", instrument.id());
        let venue_order_id = VenueOrderId::from("V-ACCEPTED-RACE");
        let (emitter, _receiver) = test_emitter();
        let local_orders = LocalOrderCoordinator::new();
        claim_order(&local_orders, &order, venue_order_id);
        local_orders
            .accept_submission(venue_order_id, OrderIdentity::from_order(&order))
            .unwrap();
        local_orders
            .defer_cancel(OrderIdentity::from_order(&order))
            .unwrap();

        assert_eq!(
            reject_claimed_submit_order(
                &order,
                "late negative response",
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &local_orders,
            ),
            Some((venue_order_id.to_string(), venue_order_id))
        );
    }

    #[rstest]
    fn test_batch_leg_empty_order_id_no_reason_does_not_panic() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let order = test_limit_order("O-BATCH-EMPTY", instrument_id);
        let (emitter, mut receiver) = test_emitter();
        let local_orders = Arc::new(LocalOrderCoordinator::new());
        let expected_venue_order_id = VenueOrderId::from("expected-batch-empty");
        claim_order(&local_orders, &order, expected_venue_order_id);

        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: None,
        };

        assert!(
            handle_order_response(
                Ok(response),
                expected_venue_order_id,
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &local_orders,
            )
            .is_none()
        );

        // The empty id routes to the warn branch: no order events emitted
        assert!(receiver.try_recv().is_err());
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
        let local_orders = Arc::new(LocalOrderCoordinator::new());
        let expected_venue_order_id = VenueOrderId::from("expected-batch-reject");
        claim_order(&local_orders, &order, expected_venue_order_id);

        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                expected_venue_order_id,
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &local_orders,
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
        let local_orders = Arc::new(LocalOrderCoordinator::new());
        let expected_venue_order_id = VenueOrderId::from("expected-submit-reject");
        claim_order(&local_orders, &order, expected_venue_order_id);

        let response = OrderResponse {
            success: false,
            order_id: None,
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                expected_venue_order_id,
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &local_orders,
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
        let local_orders = LocalOrderCoordinator::new();
        local_orders
            .begin_submission(OrderIdentity::from_order(&order))
            .unwrap();
        local_orders
            .defer_cancel(OrderIdentity::from_order(&order))
            .unwrap();

        reject_submit_order(
            &order,
            reason,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &local_orders,
        );

        match receiver.try_recv().expect("expected rejected event") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.reason.as_str(), reason);
                assert_eq!(event.due_post_only, expected_post_only);
            }
            other => panic!("expected rejected event, was {other:?}"),
        }

        // The reject funnel clears any tracked pending cancel for the order
        assert!(!local_orders.take_deferred_cancel(OrderIdentity::from_order(&order)));
    }
}
