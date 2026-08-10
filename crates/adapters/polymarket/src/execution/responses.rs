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
#[cfg(test)]
use nautilus_model::enums::TimeInForce;
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
    identity::{OrderIdentity, OrderIdentityRegistry},
    order_fill_tracker::OrderFillTrackerMap,
    parse::parse_order_status_report,
    pending::{PendingCancelTracker, PendingSubmitTracker},
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
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &OrderIdentityRegistry,
    pending_submits: &PendingSubmitTracker,
    pending_cancels: &PendingCancelTracker,
    pending_tasks: &Arc<TaskHandles>,
    account_id: AccountId,
) {
    let response_len = responses.len();
    let order_len = batch_orders.len();

    if response_len != order_len {
        log::warn!(
            "Batch submit response length ({response_len}) does not match order count ({order_len})"
        );
    }

    let mut deferred = Vec::new();

    for (batch_order, response) in batch_orders.iter().zip(responses) {
        if let Some((order_id_str, venue_order_id)) = handle_order_response(
            Ok(response),
            &batch_order.order,
            emitter,
            clock,
            fill_tracker,
            order_identities,
            pending_cancels,
            account_id,
            batch_order.size_precision,
            batch_order.price_precision,
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
                None,
                emitter,
                clock,
                fill_tracker,
                order_identities,
                pending_submits,
                pending_cancels,
                account_id,
                batch_order.size_precision,
                batch_order.price_precision,
            ) {
                deferred.push((batch_order.order.clone(), order_id_str, venue_order_id));
            }
        }
    }

    if !deferred.is_empty() {
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
            pending_tasks.push(handle);
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
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &OrderIdentityRegistry,
    pending_submits: &PendingSubmitTracker,
    pending_cancels: &PendingCancelTracker,
    account_id: AccountId,
) {
    match result {
        Ok(response) => {
            if let Some((order_id_str, venue_order_id)) = handle_order_response(
                Ok(response),
                &batch_order.order,
                emitter,
                clock,
                fill_tracker,
                order_identities,
                pending_cancels,
                account_id,
                batch_order.size_precision,
                batch_order.price_precision,
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
                None,
                emitter,
                clock,
                fill_tracker,
                order_identities,
                pending_submits,
                pending_cancels,
                account_id,
                batch_order.size_precision,
                batch_order.price_precision,
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
            reject_submit_order(
                &batch_order.order,
                &format!("{e}"),
                emitter,
                clock,
                pending_cancels,
            );
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) fn handle_unknown_submit_result(
    order: &OrderAny,
    expected_venue_order_id: VenueOrderId,
    reason: &str,
    fill_tracker_quantity: Option<Quantity>,
    _emitter: &ExecutionEventEmitter,
    _clock: &'static AtomicTime,
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &OrderIdentityRegistry,
    pending_submits: &PendingSubmitTracker,
    pending_cancels: &PendingCancelTracker,
    _account_id: AccountId,
    _size_precision: u8,
    _price_precision: u8,
) -> Option<(String, VenueOrderId)> {
    log::warn!(
        "Submit outcome unknown for {}: {reason}. Tracking expected venue order ID {}",
        order.client_order_id(),
        expected_venue_order_id
    );

    order_identities
        .register_order_identity(expected_venue_order_id, OrderIdentity::from_order(order));
    pending_submits.insert(expected_venue_order_id, order.client_order_id());

    fill_tracker.register_reconciled_order(
        expected_venue_order_id,
        fill_tracker_quantity.unwrap_or_else(|| order.quantity()),
        order.filled_qty(),
        order.order_side(),
    );

    if pending_cancels.contains(&order.client_order_id()) {
        let order_id_str = expected_venue_order_id.to_string();
        return Some((order_id_str, expected_venue_order_id));
    }

    None
}

#[expect(clippy::too_many_arguments)]
pub(super) fn handle_order_response(
    result: crate::http::error::Result<OrderResponse>,
    order: &OrderAny,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    fill_tracker: &Arc<OrderFillTrackerMap>,
    order_identities: &OrderIdentityRegistry,
    pending_cancels: &PendingCancelTracker,
    _account_id: AccountId,
    _size_precision: u8,
    _price_precision: u8,
) -> Option<(String, VenueOrderId)> {
    match result {
        Ok(response) => {
            if response.success {
                // VenueOrderId panics on an empty string
                if let Some(order_id) = response.order_id.filter(|s| !s.is_empty()) {
                    let venue_order_id = VenueOrderId::from(order_id.as_str());
                    let ts_now = clock.get_time_ns();
                    order_identities
                        .register_order_identity(venue_order_id, OrderIdentity::from_order(order));
                    if order_identities.mark_accepted(venue_order_id) {
                        emitter.emit_order_accepted(order, venue_order_id, ts_now);
                    }

                    fill_tracker.register_reconciled_order(
                        venue_order_id,
                        order.quantity(),
                        order.filled_qty(),
                        order.order_side(),
                    );

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
                    reject_submit_order(order, &reason, emitter, clock, pending_cancels);
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
                reject_submit_order(order, &reason, emitter, clock, pending_cancels);
            }
        }
        Err(e) => {
            reject_submit_order(
                order,
                &format!("HTTP request failed: {e}"),
                emitter,
                clock,
                pending_cancels,
            );
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
    order: &OrderAny,
    fill_tracker: &Arc<OrderFillTrackerMap>,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    size_precision: u8,
    price_precision: u8,
    clock: &'static AtomicTime,
) {
    const FOK_CHECK_DELAY: Duration = Duration::from_secs(5);

    tokio::time::sleep(FOK_CHECK_DELAY).await;

    let venue_order_id = VenueOrderId::from(order_id);
    if fill_tracker.has_fills_or_settled(&venue_order_id) {
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

    let order_status = OrderStatus::from(venue_order.status);
    let ts_now = clock.get_time_ns();

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
            let confirmed_filled = fill_tracker
                .get_cumulative_filled(&venue_order_id)
                .unwrap_or_else(|| Quantity::zero(size_precision));
            let mut report = match parse_order_status_report(
                &venue_order,
                order.instrument_id(),
                account_id,
                Some(order.client_order_id()),
                price_precision,
                size_precision,
                ts_now,
            ) {
                Ok(report) => report,
                Err(e) => {
                    log::warn!("Skipping invalid FOK order report {order_id}: {e}");
                    return;
                }
            };

            if let Err(e) = cap_order_report_filled_qty(&mut report, confirmed_filled, None) {
                log::warn!("Skipping invalid FOK order report {order_id}: {e}");
                return;
            }

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
    use super::*;
    use crate::{
        common::enums::{PolymarketLiquiditySide, PolymarketTradeStatus},
        http::{
            models::GammaMarket,
            parse::{create_instrument_from_def, parse_gamma_market},
        },
        websocket::{
            dispatch::{WsDispatchContext, WsDispatchState, dispatch_user_message},
            messages::{PolymarketUserOrder, UserWsMessage},
        },
    };
    use nautilus_common::messages::ExecutionEvent;
    use nautilus_core::{UnixNanos, collections::AtomicMap};
    use nautilus_model::{
        enums::AccountType,
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        instruments::{Instrument, InstrumentAny},
        orders::{LimitOrder, MarketOrder, Order},
        types::{Currency, Price},
    };
    use rstest::rstest;

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
            nautilus_model::identifiers::ClientId::from("POLYMARKET"),
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
        let response = OrderResponse {
            success: true,
            order_id: Some(venue_order_id.to_string()),
            error_msg: None,
        };
        let deferred_cancel = handle_order_response(
            Ok(response),
            &order,
            &emitter,
            nautilus_core::time::get_atomic_clock_realtime(),
            &fill_tracker,
            &order_identities,
            &pending_cancels,
            AccountId::from("POLY-001"),
            6,
            4,
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

        let output = crate::execution::reconciliation::build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(output.reports.is_empty());
        let expected = if status.is_pending_settlement() {
            crate::execution::reconciliation::ReconciliationOmission::PendingTrade
        } else {
            crate::execution::reconciliation::ReconciliationOmission::FailedTrade
        };
        assert_eq!(output.omissions.count(expected), 1);
    }

    #[rstest]
    fn test_confirmed_maker_trade_owned_by_case_variant_address_generates_fill_report() {
        let instrument = test_instrument();
        let mut trade: crate::http::models::PolymarketTradeReport = load("http_trade_report.json");
        trade.trader_side = PolymarketLiquiditySide::Maker;

        // Recorded venue payloads carry EIP-55 checksummed (mixed-case) maker
        // addresses while configured funder addresses are commonly lowercase.
        // Mirror that direction: give the payload side a case variant (all-
        // uppercase hex stands in for the checksummed form), keep the
        // configured side lowercase. Any case variant of the same address
        // must still establish ownership.
        let configured_address = trade.maker_orders[0].maker_address.clone();
        let uppercase_variant_address = configured_address
            .to_ascii_uppercase()
            .replacen("0X", "0x", 1);
        assert_ne!(uppercase_variant_address, configured_address);
        trade.maker_orders[0].maker_address = uppercase_variant_address;
        let foreign_api_key = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        assert_ne!(trade.maker_orders[0].owner, foreign_api_key);
        let expected_venue_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());

        let instruments = AtomicMap::new();
        instruments.insert(trade.asset_id, instrument);
        let ctx = crate::execution::reconciliation::FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: &configured_address,
            api_key: foreign_api_key,
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        };

        let output = crate::execution::reconciliation::build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(
            output.reports.len(),
            1,
            "the account's own confirmed maker fill must be reported",
        );
        assert_eq!(output.reports[0].venue_order_id, expected_venue_order_id);
        assert_eq!(
            output
                .omissions
                .count(crate::execution::reconciliation::ReconciliationOmission::UnownedMakerTrade),
            0,
            "entry-level skips of foreign entries in an owned trade are not trade drops",
        );
        assert_eq!(
            output
                .omissions
                .count(crate::execution::reconciliation::ReconciliationOmission::UnmappedFill),
            0,
        );
    }

    #[rstest]
    #[case(PolymarketLiquiditySide::Maker)]
    #[case(PolymarketLiquiditySide::Taker)]
    fn test_confirmed_trade_without_instrument_counts_unmapped_discard(
        #[case] trader_side: PolymarketLiquiditySide,
    ) {
        let mut trade: crate::http::models::PolymarketTradeReport = load("http_trade_report.json");
        trade.trader_side = trader_side;
        let instruments = AtomicMap::new();
        let ctx = crate::execution::reconciliation::FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
            api_key: "ffffffff-ffff-ffff-ffff-ffffffffffff",
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        };

        let output = crate::execution::reconciliation::build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert_eq!(output.reports.len(), 0,);
        assert_eq!(
            output
                .omissions
                .count(crate::execution::reconciliation::ReconciliationOmission::UnmappedFill),
            1,
        );
    }

    #[rstest]
    fn test_confirmed_maker_trade_without_owned_order_is_counted() {
        let instrument = test_instrument();
        let mut trade: crate::http::models::PolymarketTradeReport = load("http_trade_report.json");
        trade.trader_side = PolymarketLiquiditySide::Maker;

        let instruments = AtomicMap::new();
        instruments.insert(trade.asset_id, instrument);
        // Neither the address nor the API key matches any maker order, so the
        // whole confirmed trade is dropped; the drop must be observable.
        let ctx = crate::execution::reconciliation::FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: "0x000000000000000000000000000000000000dead",
            api_key: "ffffffff-ffff-ffff-ffff-ffffffffffff",
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        };

        let output = crate::execution::reconciliation::build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            None,
            UnixNanos::from(1_000_000_000u64),
        );

        assert!(output.reports.is_empty());
        assert_eq!(
            output
                .omissions
                .count(crate::execution::reconciliation::ReconciliationOmission::UnownedMakerTrade),
            1,
            "a confirmed maker trade dropped whole must be counted, not silent",
        );
        assert_eq!(
            output
                .omissions
                .count(crate::execution::reconciliation::ReconciliationOmission::UnmappedFill),
            0,
        );
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
        let pending_submits = PendingSubmitTracker::default();
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();

        assert!(
            handle_unknown_submit_result(
                &order,
                expected_venue_order_id,
                "transport timeout",
                None,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &fill_tracker,
                &order_identities,
                &pending_submits,
                &pending_cancels,
                AccountId::from("POLY-001"),
                instrument.size_precision(),
                instrument.price_precision(),
            )
            .is_none()
        );

        assert_eq!(
            pending_submits.client_order_id(&expected_venue_order_id),
            Some(order.client_order_id())
        );

        let token_instruments = AtomicMap::new();
        token_instruments.insert(ws_order.asset_id, instrument);
        let mut state = WsDispatchState::default();
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
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

    // An empty orderID with no reason is ambiguous: it must not panic constructing a VenueOrderId,
    // and with nothing to report it stays on the warn branch (no event) for reconciliation.
    #[rstest]
    fn test_batch_leg_empty_order_id_no_reason_does_not_panic() {
        let instrument = test_instrument();
        let instrument_id = instrument.id();
        let order = test_limit_order("O-BATCH-EMPTY", instrument_id);
        let (emitter, mut receiver) = test_emitter();
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();

        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: None,
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &fill_tracker,
                &order_identities,
                &pending_cancels,
                AccountId::from("POLY-001"),
                instrument.size_precision(),
                instrument.price_precision(),
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
        let fill_tracker = Arc::new(OrderFillTrackerMap::new());
        let pending_cancels = PendingCancelTracker::default();
        let order_identities = OrderIdentityRegistry::default();

        let response = OrderResponse {
            success: true,
            order_id: Some(String::new()),
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &fill_tracker,
                &order_identities,
                &pending_cancels,
                AccountId::from("POLY-001"),
                instrument.size_precision(),
                instrument.price_precision(),
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

        let response = OrderResponse {
            success: false,
            order_id: None,
            error_msg: Some(reason.to_string()),
        };

        assert!(
            handle_order_response(
                Ok(response),
                &order,
                &emitter,
                nautilus_core::time::get_atomic_clock_realtime(),
                &fill_tracker,
                &order_identities,
                &pending_cancels,
                AccountId::from("POLY-001"),
                instrument.size_precision(),
                instrument.price_precision(),
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
