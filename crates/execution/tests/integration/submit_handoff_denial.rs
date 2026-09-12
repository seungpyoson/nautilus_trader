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

//! Minimal harness for the handed-off-order denial reported in issue 4944.
//!
//! Order A is submitted through the engine's queued command endpoint and reaches the
//! execution client once. The stub client returns success without emitting
//! `OrderSubmitted`, which models the interval before an asynchronous adapter task
//! emits it. A later `SubmitOrderList` that includes A and a fresh order B, carrying a
//! position ID that is invalid for NETTING, then reaches the engine through the same
//! queued endpoint.

use std::{cell::RefCell, rc::Rc, sync::Arc};

use nautilus_common::{
    cache::Cache,
    clock::TestClock,
    messages::execution::{SubmitOrder, SubmitOrderList, TradingCommand},
    msgbus::{self, MessageBus, MessagingSwitchboard, TypedHandler, switchboard},
    runner::{
        SyncTradingCommandSender, drain_trading_cmd_queue, replace_exec_cmd_sender,
        trading_cmd_queue_is_empty,
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::{ExecutionEngine, stubs::StubExecutionClient};
use nautilus_model::{
    enums::{OmsType, OrderSide, OrderStatus, OrderType},
    events::{OrderEventAny, OrderSubmitted},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, PositionId, StrategyId,
        TraderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
    orders::{Order, OrderAny, OrderList, builder::OrderTestBuilder},
    stubs::TestDefault,
    types::Quantity,
};
use rstest::rstest;

const CLIENT: &str = "STUB";
const ACCOUNT: &str = "TEST-ACCOUNT";
const INVALID_NETTING_POSITION_ID: &str = "invalid-netting-position";

struct Harness {
    /// Kept alive for the harness lifetime: the registered msgbus handlers hold a `Weak`.
    _engine: Rc<RefCell<ExecutionEngine>>,
    cache: Rc<RefCell<Cache>>,
    instrument: InstrumentAny,
    submitted: Rc<RefCell<Vec<ClientOrderId>>>,
    published: Rc<RefCell<Vec<OrderEventAny>>>,
}

impl Harness {
    /// Engine with one registered NETTING stub client and the queued command endpoint
    /// wired exactly as the live runner wires it.
    fn new() -> Self {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));
        assert!(trading_cmd_queue_is_empty());

        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let engine = Rc::new(RefCell::new(ExecutionEngine::new(
            clock,
            cache.clone(),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&engine);

        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let client = StubExecutionClient::new(
            ClientId::from(CLIENT),
            AccountId::from(ACCOUNT),
            instrument.id().venue,
            OmsType::Netting,
            None,
        );
        let submitted = client.submitted_order_ids();
        engine
            .borrow_mut()
            .register_client(Box::new(client))
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();

        let published = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let published = published.clone();
            move |event: &OrderEventAny| published.borrow_mut().push(event.clone())
        });
        msgbus::subscribe_order_events(
            switchboard::get_event_order_topic(StrategyId::test_default()).into(),
            handler,
            None,
        );

        Self {
            _engine: engine,
            cache,
            instrument,
            submitted,
            published,
        }
    }

    fn instrument_id(&self) -> InstrumentId {
        self.instrument.id()
    }

    fn order(&self, client_order_id: &str) -> OrderAny {
        OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::test_default())
            .strategy_id(StrategyId::test_default())
            .instrument_id(self.instrument_id())
            .client_order_id(ClientOrderId::from(client_order_id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100_000))
            .build()
    }

    /// What `Strategy::submit_order` does before it sends the command: cache the order.
    fn cache_order(&self, order: &OrderAny) {
        self.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
    }

    fn submit_order_command(&self, order: &OrderAny) -> SubmitOrder {
        SubmitOrder {
            trader_id: order.trader_id(),
            client_id: Some(ClientId::from(CLIENT)),
            strategy_id: order.strategy_id(),
            instrument_id: order.instrument_id(),
            client_order_id: order.client_order_id(),
            order_init: order.init_event().clone(),
            exec_algorithm_id: None,
            position_id: None,
            params: None,
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            causation_id: None,
        }
    }

    fn submit_order_list_command(
        &self,
        orders: &[&OrderAny],
        position_id: Option<PositionId>,
    ) -> SubmitOrderList {
        let first = orders[0];
        let order_list = OrderList::new(
            OrderListId::from("L-1"),
            first.instrument_id(),
            first.strategy_id(),
            orders.iter().map(|order| order.client_order_id()).collect(),
            UnixNanos::default(),
        );
        SubmitOrderList {
            trader_id: first.trader_id(),
            client_id: Some(ClientId::from(CLIENT)),
            strategy_id: first.strategy_id(),
            instrument_id: first.instrument_id(),
            order_list,
            order_inits: orders
                .iter()
                .map(|order| order.init_event().clone())
                .collect(),
            exec_algorithm_id: None,
            position_id,
            params: None,
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            causation_id: None,
        }
    }

    /// The command enqueueing path: the queued execution endpoint, then the runner drain.
    fn enqueue_and_drain(&self, command: TradingCommand) {
        msgbus::send_trading_command(MessagingSwitchboard::exec_engine_queue_execute(), command);
        drain_trading_cmd_queue();
    }

    /// The client's first submission event for `order`, delivered the way the live runner
    /// delivers execution events to the engine.
    fn deliver_submitted(&self, order: &OrderAny) {
        let event = OrderSubmitted::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            AccountId::from(ACCOUNT),
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
        );
        msgbus::send_order_event(
            MessagingSwitchboard::exec_engine_process(),
            OrderEventAny::Submitted(event),
        );
    }

    fn cached(&self, order: &OrderAny) -> OrderAny {
        self.cache
            .borrow()
            .order_owned(&order.client_order_id())
            .expect("order should be cached")
    }

    fn submitted(&self) -> Vec<ClientOrderId> {
        self.submitted.borrow().clone()
    }

    fn denied_reason(&self, order: &OrderAny) -> String {
        match self.cached(order).last_event() {
            OrderEventAny::Denied(denied) => denied.reason.to_string(),
            other => panic!("expected OrderDenied, got {other:?}"),
        }
    }

    fn describe(&self, label: &str, orders: &[&OrderAny]) {
        for order in orders {
            let cached = self.cached(order);
            eprintln!(
                "{label}: {} status={:?} last_event={}",
                order.client_order_id(),
                cached.status(),
                match cached.last_event() {
                    OrderEventAny::Denied(denied) => format!("Denied({})", denied.reason),
                    other => format!("{other:?}")
                        .split('(')
                        .next()
                        .unwrap_or("")
                        .to_string(),
                }
            );
        }
        eprintln!(
            "{label}: client submissions={:?} published_order_events={}",
            self.submitted(),
            self.published.borrow().len()
        );
    }
}

/// Submits A once, then enqueues the invalid list [A, B] before A's first submission event.
fn hand_off_a_then_enqueue_invalid_list(h: &Harness) -> (OrderAny, OrderAny) {
    let a = h.order("O-A");
    h.cache_order(&a);
    h.enqueue_and_drain(TradingCommand::SubmitOrder(h.submit_order_command(&a)));
    assert_eq!(h.submitted(), vec![a.client_order_id()]);
    assert_eq!(h.cached(&a).status(), OrderStatus::Initialized);
    h.describe("after A handed off", &[&a]);

    let b = h.order("O-B");
    let list = h.submit_order_list_command(
        &[&a, &b],
        Some(PositionId::from(INVALID_NETTING_POSITION_ID)),
    );
    h.enqueue_and_drain(TradingCommand::SubmitOrderList(list));
    h.describe("after invalid list [A, B]", &[&a, &b]);

    // The invalid list never reached the client and B received the NETTING refusal.
    assert_eq!(h.submitted(), vec![a.client_order_id()]);
    assert_eq!(h.cached(&b).status(), OrderStatus::Denied);
    assert!(
        h.denied_reason(&b).contains("not valid for NETTING OMS"),
        "B reason: {}",
        h.denied_reason(&b)
    );
    (a, b)
}

#[rstest]
fn later_invalid_list_before_first_submission_event_preserves_handed_off_order() {
    let h = Harness::new();
    let (a, _b) = hand_off_a_then_enqueue_invalid_list(&h);

    assert_eq!(
        h.cached(&a).status(),
        OrderStatus::Initialized,
        "a later invalid command must not close the order already handed to the client",
    );
}

#[rstest]
fn later_invalid_list_before_first_submission_event_then_original_submitted_event_applies() {
    let h = Harness::new();
    let (a, _b) = hand_off_a_then_enqueue_invalid_list(&h);

    h.deliver_submitted(&a);
    h.describe("after A's OrderSubmitted", &[&a]);

    assert_eq!(
        h.cached(&a).status(),
        OrderStatus::Submitted,
        "the client's first submission event for A must still apply",
    );
}

#[rstest]
fn later_invalid_list_after_first_submission_event_preserves_submitted_order() {
    let h = Harness::new();
    let a = h.order("O-A");
    h.cache_order(&a);
    h.enqueue_and_drain(TradingCommand::SubmitOrder(h.submit_order_command(&a)));
    h.deliver_submitted(&a);
    assert_eq!(h.cached(&a).status(), OrderStatus::Submitted);

    let b = h.order("O-B");
    let list = h.submit_order_list_command(
        &[&a, &b],
        Some(PositionId::from(INVALID_NETTING_POSITION_ID)),
    );
    h.enqueue_and_drain(TradingCommand::SubmitOrderList(list));
    h.describe("after invalid list [A, B] (A already Submitted)", &[&a, &b]);

    assert_eq!(h.submitted(), vec![a.client_order_id()]);
    assert_eq!(h.cached(&a).status(), OrderStatus::Submitted);
    assert_eq!(h.cached(&b).status(), OrderStatus::Denied);
}
