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

use std::{cell::RefCell, rc::Rc};

use nautilus_common::messages::execution::{
    BatchModifyOrders, CancelAllOrders, ModifyOrder, SubmitOrder, SubmitOrderList,
};
use nautilus_core::UnixNanos;
use nautilus_execution::engine::stubs::StubExecutionClient;
use nautilus_model::{
    accounts::{Account, CashAccount},
    data::Data,
    enums::{
        AccountType, InstrumentCloseType, LiquiditySide, OmsType, OrderSide, OrderType,
        PositionSide,
    },
    events::{AccountState, OrderDenied, OrderFilled},
    identifiers::{AccountId, OrderListId, PositionId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny, stubs::binary_option},
    orders::{OrderAny, OrderList, OrderTestBuilder, stubs::TestOrderEventStubs},
    reports::ExecutionMassStatus,
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use rstest::rstest;
use rust_decimal_macros::dec;

use super::*;
use crate::execution::manager::ReportClientCoverage;

struct Fixture {
    node: LiveNode,
    instrument: InstrumentAny,
    account_id: AccountId,
    client_id: ClientId,
    submitted: Rc<RefCell<Vec<ClientOrderId>>>,
    modified: Rc<RefCell<Vec<ClientOrderId>>>,
    cancels: Rc<RefCell<Vec<CancelAllOrders>>>,
    positions: Rc<RefCell<Vec<PositionEvent>>>,
    position_handler: TypedHandler<PositionEvent>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_base_currency(Currency::USDC())
    }

    fn with_base_currency(base_currency: Currency) -> Self {
        let config = LiveNodeConfig {
            portfolio: Some(nautilus_portfolio::config::PortfolioConfig {
                use_mark_xrates: true,
                ..Default::default()
            }),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                position_check_threshold_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let node = LiveNode::build("ContractSettlement".to_string(), Some(config)).unwrap();
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let account_id = AccountId::from("POLYMARKET-001");
        let client_id = ClientId::from("SETTLEMENT");
        let state = AccountState::new(
            account_id,
            AccountType::Cash,
            vec![AccountBalance::new(
                Money::new(100.0, base_currency),
                Money::zero(base_currency),
                Money::new(100.0, base_currency),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(base_currency),
        );
        node.kernel
            .cache
            .borrow_mut()
            .add_account(CashAccount::new(state, false, false).into())
            .unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let client = StubExecutionClient::new(
            client_id,
            account_id,
            instrument.id().venue,
            OmsType::Netting,
            None,
        );
        let submitted = client.submitted_order_ids();
        let modified = client.modified_order_ids();
        let cancels = client.cancel_all_commands();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(client))
            .unwrap();
        let positions = Rc::new(RefCell::new(Vec::new()));
        let captured = positions.clone();
        let position_handler = TypedHandler::from(move |event: &PositionEvent| {
            captured.borrow_mut().push(event.clone());
        });
        msgbus::subscribe_position_events(
            "events.position.*".into(),
            position_handler.clone(),
            None,
        );
        Self {
            node,
            instrument,
            account_id,
            client_id,
            submitted,
            modified,
            cancels,
            positions,
            position_handler,
        }
    }

    fn position_id(&self, strategy: &str) -> PositionId {
        PositionId::new(format!("{}-{strategy}", self.instrument.id()))
    }

    fn fill(
        &mut self,
        strategy: &str,
        tag: &str,
        side: OrderSide,
        qty: &str,
        px: &str,
    ) -> OrderFilled {
        let order = self.accept_order(strategy, tag, side, qty, px);
        self.fill_order(&order, tag, qty, px)
    }

    fn accept_order(
        &self,
        strategy: &str,
        tag: &str,
        side: OrderSide,
        qty: &str,
        px: &str,
    ) -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(self.node.trader_id())
            .strategy_id(StrategyId::from(strategy))
            .instrument_id(self.instrument.id())
            .client_order_id(ClientOrderId::new(tag))
            .side(side)
            .reduce_only(side == OrderSide::Sell)
            .quantity(Quantity::from(qty))
            .price(Price::from(px))
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, self.account_id);
        self.node
            .kernel
            .cache
            .borrow_mut()
            .add_order(order, None, Some(self.client_id), false)
            .unwrap();
        let order = self
            .node
            .kernel
            .cache
            .borrow_mut()
            .update_order(&submitted)
            .unwrap();
        let accepted =
            TestOrderEventStubs::accepted(&order, self.account_id, VenueOrderId::new(tag));
        self.node
            .kernel
            .cache
            .borrow_mut()
            .update_order(&accepted)
            .unwrap()
    }

    fn fill_order(&mut self, order: &OrderAny, tag: &str, qty: &str, px: &str) -> OrderFilled {
        // These are real fills executed before expiry but may be delivered after settlement.
        let ts_event = UnixNanos::from(self.instrument.expiration_ns().unwrap().as_u64() - 1);
        let event = TestOrderEventStubs::filled(
            order,
            &self.instrument,
            Some(TradeId::new(tag)),
            None,
            Some(Price::from(px)),
            Some(Quantity::from(qty)),
            Some(LiquiditySide::Taker),
            Some(Money::from("0.10 USDC")),
            Some(ts_event),
            Some(self.account_id),
        );
        let OrderEventAny::Filled(fill) = event else {
            unreachable!()
        };
        self.node
            .process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(fill.clone())));
        fill
    }

    fn close(&mut self, price: &str, close_type: InstrumentCloseType) -> InstrumentClose {
        let close = InstrumentClose::new(
            self.instrument.id(),
            Price::from(price),
            close_type,
            self.instrument.expiration_ns().unwrap(),
            self.instrument.expiration_ns().unwrap(),
        );
        self.node
            .process_runner_event(PendingRunnerEvent::DataEvent(DataEvent::Data(
                Data::InstrumentClose(close),
            )));
        close
    }

    fn assert_settled(&self, expected_pnl: &str, closes: usize) {
        assert!(!self.node.handle.should_stop());
        assert_eq!(
            self.node.kernel.cache.borrow().positions_open_count(
                None,
                Some(&self.instrument.id()),
                None,
                None,
                None,
            ),
            0
        );
        assert_eq!(
            self.node
                .kernel
                .portfolio
                .borrow_mut()
                .realized_pnl(&self.instrument.id()),
            Some(Money::from(expected_pnl)),
        );
        assert_eq!(
            self.positions
                .borrow()
                .iter()
                .filter(|event| matches!(event, PositionEvent::PositionClosed(_)),)
                .count(),
            closes
        );
        assert_eq!(
            self.node
                .kernel
                .cache
                .borrow()
                .account(&self.account_id)
                .unwrap()
                .balance_total(Some(Currency::USDC())),
            Some(Money::from("100 USDC"))
        );
        assert!(
            self.submitted.borrow().is_empty(),
            "settlement must not submit to the venue"
        );
    }

    fn reconcile(&mut self, venue_qty: Option<Quantity>) -> Vec<OrderEventAny> {
        let mut check = self
            .node
            .exec_manager
            .prepare_position_report_check(UUID4::new(), &[]);
        check.client_coverage.insert(
            (self.instrument.id(), self.account_id),
            ReportClientCoverage::Resolved(IndexSet::from([self.client_id])),
        );
        let reports = venue_qty
            .into_iter()
            .map(|quantity| {
                PositionStatusReport::new(
                    self.account_id,
                    self.instrument.id(),
                    PositionSide::Long,
                    quantity,
                    UnixNanos::from(1_000),
                    UnixNanos::from(1_000),
                    None,
                    None,
                    Some(dec!(0.4)),
                )
            })
            .collect();
        self.node.exec_manager.reconcile_position_reports(
            &check,
            reports,
            &IndexSet::from([self.client_id]),
            &IndexSet::new(),
        )
    }

    fn position_report_result(&self) -> PositionReportResult {
        let mut check = self
            .node
            .exec_manager
            .prepare_position_report_check(UUID4::new(), &[]);
        check.client_coverage.insert(
            (self.instrument.id(), self.account_id),
            ReportClientCoverage::Resolved(IndexSet::from([self.client_id])),
        );
        PositionReportResult {
            check,
            reports: vec![PositionStatusReport::new(
                self.account_id,
                self.instrument.id(),
                PositionSide::Long,
                Quantity::from("20.00"),
                UnixNanos::from(1_000),
                UnixNanos::from(1_000),
                None,
                None,
                Some(dec!(0.4)),
            )],
            queried_clients: IndexSet::from([self.client_id]),
            failed_clients: IndexSet::new(),
        }
    }

    fn trading_commands(&self) -> Vec<TradingCommandMessage> {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(self.node.trader_id())
            .strategy_id(StrategyId::from("OWNER-001"))
            .instrument_id(self.instrument.id())
            .client_order_id(ClientOrderId::from("QUEUED-SUBMIT"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.00"))
            .price(Price::from("0.400"))
            .build();
        self.node
            .kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(self.client_id), false)
            .unwrap();
        let submit = SubmitOrder::new(
            order.trader_id(),
            Some(self.client_id),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let list_order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(ClientOrderId::from("QUEUED-LIST"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.00"))
            .price(Price::from("0.400"))
            .build();
        self.node
            .kernel
            .cache
            .borrow_mut()
            .add_order(list_order.clone(), None, Some(self.client_id), false)
            .unwrap();
        let submit_list = SubmitOrderList::new(
            order.trader_id(),
            Some(self.client_id),
            order.strategy_id(),
            OrderList::new(
                OrderListId::from("QUEUED-LIST"),
                order.instrument_id(),
                order.strategy_id(),
                vec![list_order.client_order_id()],
                UnixNanos::default(),
            ),
            vec![list_order.init_event().clone()],
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let working = self.accept_order("OWNER-001", "WORKING", OrderSide::Buy, "1.00", "0.400");
        let modify = ModifyOrder::new(
            working.trader_id(),
            Some(self.client_id),
            working.strategy_id(),
            working.instrument_id(),
            working.client_order_id(),
            working.venue_order_id(),
            None,
            Some(Price::from("0.500")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        let cancel = CancelAllOrders::new(
            working.trader_id(),
            Some(self.client_id),
            working.strategy_id(),
            working.instrument_id(),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        let batch_modify = BatchModifyOrders::new(
            order.trader_id(),
            Some(self.client_id),
            order.strategy_id(),
            order.instrument_id(),
            vec![modify.clone()],
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        [
            TradingCommand::SubmitOrder(submit),
            TradingCommand::SubmitOrderList(submit_list),
            TradingCommand::ModifyOrder(modify),
            TradingCommand::ModifyOrders(batch_modify),
            TradingCommand::CancelAllOrders(cancel),
        ]
        .into_iter()
        .map(|command| {
            TradingCommandMessage::new(MessagingSwitchboard::exec_engine_execute(), command)
        })
        .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        msgbus::unsubscribe_position_events("events.position.*".into(), &self.position_handler);
    }
}

#[rstest]
#[case("1.000", "5.90 USDC")]
#[case("0.000", "-4.10 USDC")]
fn contract_settlement_payout_and_duplicate_close(#[case] price: &str, #[case] pnl: &str) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    let close = f.close(price, InstrumentCloseType::ContractExpired);
    f.assert_settled(pnl, 1);
    f.node
        .process_runner_event(PendingRunnerEvent::DataEvent(DataEvent::Data(
            Data::InstrumentClose(close),
        )));
    f.assert_settled(pnl, 1);
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        2
    );
    let cache = f.node.kernel.cache.borrow();
    let position = cache.position(&f.position_id("OWNER-001")).unwrap();
    assert_eq!(position.strategy_id, StrategyId::from("OWNER-001"));
    assert_eq!(position.account_id, f.account_id);
    let settlement = position.last_event().unwrap();
    assert_eq!(settlement.last_px, Price::from(price));
    assert_eq!(settlement.commission, Some(Money::zero(Currency::USDC())));
}

#[rstest]
#[case(OrderSide::Buy, "1.000", "7.30 USDC")]
#[case(OrderSide::Sell, "1.000", "4.30 USDC")]
#[case(OrderSide::Buy, "0.000", "-5.70 USDC")]
#[case(OrderSide::Sell, "0.000", "-2.70 USDC")]
fn contract_settlement_late_fill_and_duplicate(
    #[case] side: OrderSide,
    #[case] price: &str,
    #[case] pnl: &str,
) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close(price, InstrumentCloseType::ContractExpired);
    let late = f.fill("OWNER-001", "LATE", side, "3.00", "0.500");
    f.assert_settled(pnl, 2);
    let recorded = f.node.kernel.portfolio.borrow().recorded_realized_pnls();
    assert_eq!(recorded[&Currency::USDC()].len(), 2);
    f.node
        .process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(late)));
    f.assert_settled(pnl, 2);
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        4
    );
}

#[rstest]
fn contract_settlement_freezes_authoritative_fill_report_planner() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    let result = f.position_report_result();
    assert!(
        f.node.handle_position_report_result(result).is_some(),
        "the unresolved discrepancy must query fills"
    );
    f.close("1.000", InstrumentCloseType::ContractExpired);
    for _ in 0..3 {
        let result = f.position_report_result();
        assert!(
            f.node.handle_position_report_result(result).is_none(),
            "held tokens after settlement must not launch a fill-report task"
        );
        let mut result = f.position_report_result();
        let plan = f.node.exec_manager.prepare_position_fill_report_plan(
            &mut result.check,
            &result.reports,
            &result.queried_clients,
            &result.failed_clients,
            &[],
        );
        assert!(plan.queries.is_empty());
        assert!(plan.discrepancy_keys.is_empty());
    }
}

#[rstest]
fn contract_settlement_records_same_timestamp_partial_fill_cycles() {
    let mut f = Fixture::with_base_currency(Currency::EUR());
    f.node
        .kernel
        .cache
        .borrow_mut()
        .set_mark_xrate(Currency::USDC(), Currency::EUR(), 0.5);
    let opening = f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    let order = f.accept_order("OWNER-001", "LATE", OrderSide::Buy, "6.00", "0.500");
    for trade in ["LATE-1", "LATE-2"] {
        let order = f
            .node
            .kernel
            .cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        let fill = f.fill_order(&order, trade, "3.00", "0.500");
        assert_eq!(fill.ts_event, opening.ts_event);
        f.node
            .process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(fill)));
    }
    assert!(!f.node.handle.should_stop());
    let recorded = f.node.kernel.portfolio.borrow().recorded_realized_pnls();
    assert_eq!(
        recorded[&Currency::USDC()]
            .iter()
            .map(|entry| entry.2)
            .collect::<Vec<_>>(),
        vec![5.9, 1.4, 1.4]
    );
    assert_eq!(
        recorded[&Currency::EUR()]
            .iter()
            .map(|entry| entry.2)
            .collect::<Vec<_>>(),
        vec![2.95, 0.7, 0.7]
    );
    assert_eq!(
        f.node
            .kernel
            .portfolio
            .borrow_mut()
            .realized_pnl_for_account(
                &f.instrument.id(),
                Some(&f.account_id),
                Some(Currency::USDC())
            ),
        Some(Money::from("8.70 USDC"))
    );
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn contract_settlement_failure_blocks_queued_trading_and_returns_error(
    #[case] application_failure: bool,
) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    let commands = f.trading_commands();
    // Positive control: these exact commands reach the client before the failure.
    for command in &commands {
        f.node.process_exec_command(TradingCommandMessage::new(
            command.endpoint(),
            command.command().clone(),
        ));
    }
    assert_eq!(f.submitted.borrow().len(), 2);
    assert_eq!(f.modified.borrow().len(), 2);
    assert_eq!(f.cancels.borrow().len(), 1);
    f.submitted.borrow_mut().clear();
    f.modified.borrow_mut().clear();
    f.cancels.borrow_mut().clear();
    f.node.runner.as_ref().unwrap().bind_senders();
    let sender = nautilus_common::runner::try_get_trading_cmd_sender().unwrap();
    for command in commands {
        sender.execute(command);
    }
    // Change the just-initialized settlement order through the real order-event path.
    // This fails after add_order/publish, rather than at settlement preparation.
    let cache = f.node.kernel.cache.clone();
    let injected = Rc::new(RefCell::new(0));
    let captured = injected.clone();
    let handler = TypedHandler::from(move |event: &OrderEventAny| {
        if application_failure
            && let OrderEventAny::Initialized(init) = event
            && init.client_order_id.as_str().starts_with("EXPIRATION-")
        {
            let denied = OrderEventAny::Denied(OrderDenied::new(
                init.trader_id,
                init.strategy_id,
                init.instrument_id,
                init.client_order_id,
                "Application-phase test rejection".into(),
                UUID4::new(),
                init.ts_init,
                init.ts_init,
            ));
            cache.borrow_mut().update_order(&denied).unwrap();
            *captured.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_order_events("events.order.*".into(), handler.clone(), None);
    f.close("1.000", InstrumentCloseType::ContractExpired);
    if !application_failure {
        f.close("0.000", InstrumentCloseType::ContractExpired);
    }
    assert!(f.node.handle.should_stop());
    assert_eq!(*injected.borrow(), usize::from(application_failure));
    f.node.drain_runner_pending();
    assert!(f.submitted.borrow().is_empty());
    assert!(f.modified.borrow().is_empty());
    assert_eq!(
        f.cancels.borrow().len(),
        1,
        "cancels remain available during shutdown"
    );
    msgbus::unsubscribe_order_events("events.order.*".into(), &handler);
    let error = f.node.finalize_stop().await.unwrap_err().to_string();
    let expected = if application_failure {
        "Settlement did not close position"
    } else {
        "Conflicting contract close"
    };
    assert!(error.contains(expected), "fatal reason was lost: {error}");
}

#[rstest]
#[case::healthy("1.000", None)]
#[case::conflicting("0.000", Some("Conflicting contract close"))]
#[tokio::test]
async fn contract_settlement_final_drain_gates_queued_trading(
    #[case] price: &str,
    #[case] expected_error: Option<&str>,
    #[values(false, true)] already_pending: bool,
) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    f.node.runner.as_ref().unwrap().bind_senders();
    let event = DataEvent::Data(Data::InstrumentClose(InstrumentClose::new(
        f.instrument.id(),
        Price::from(price),
        InstrumentCloseType::ContractExpired,
        f.instrument.expiration_ns().unwrap(),
        f.instrument.expiration_ns().unwrap(),
    )));

    if already_pending {
        AsyncRunner::handle_data_event(event);
    } else {
        nautilus_common::live::runner::get_data_event_sender()
            .send(event)
            .unwrap();
    }
    let sender = nautilus_common::runner::try_get_trading_cmd_sender().unwrap();
    for command in f.trading_commands() {
        sender.execute(command);
    }
    assert!(f.node.check_execution_health().is_ok());

    // Finalization stops clients before draining; the shared halt must still gate dispatch
    f.node.kernel.exec_engine.borrow_mut().stop();
    let AsyncRunnerChannels {
        mut time_evt_rx,
        mut system_evt_rx,
        mut system_cmd_rx,
        mut exec_evt_rx,
        mut exec_cmd_rx,
        mut data_evt_rx,
        mut data_cmd_rx,
    } = f.node.runner.take().unwrap().take_channels();
    f.node.drain_channels(&mut RunnerReceivers {
        time_evt: &mut time_evt_rx,
        system_evt: &mut system_evt_rx,
        system_cmd: &mut system_cmd_rx,
        exec_evt: &mut exec_evt_rx,
        exec_cmd: &mut exec_cmd_rx,
        data_evt: &mut data_evt_rx,
        data_cmd: &mut data_cmd_rx,
    });
    let expected_dispatches = if expected_error.is_some() { 0 } else { 2 };
    assert_eq!(f.submitted.borrow().len(), expected_dispatches);
    assert_eq!(f.modified.borrow().len(), expected_dispatches);
    assert_eq!(f.cancels.borrow().len(), 1);
    let result = f.node.finalize_stop().await;

    match expected_error {
        Some(expected) => {
            let error = result.unwrap_err();
            assert!(format!("{error:#}").contains(expected));
        }
        None => result.unwrap(),
    }
}

#[rstest]
fn contract_settlement_partial_exit_and_multiple_owners() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN-A", OrderSide::Buy, "10.00", "0.400");
    f.fill("OWNER-001", "EXIT-A", OrderSide::Sell, "3.00", "0.600");
    f.fill("OWNER-002", "OPEN-B", OrderSide::Buy, "5.00", "0.200");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    f.assert_settled("8.50 USDC", 2);
    let cache = f.node.kernel.cache.borrow();
    assert_eq!(
        cache
            .position(&f.position_id("OWNER-001"))
            .unwrap()
            .realized_pnl,
        Some(Money::from("4.60 USDC"))
    );
    assert_eq!(
        cache
            .position(&f.position_id("OWNER-002"))
            .unwrap()
            .realized_pnl,
        Some(Money::from("3.90 USDC"))
    );
}

#[rstest]
fn contract_settlement_freezes_inventory_before_and_after_redemption() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    // Unredeemed tokens still exist at the venue. Redemption subsequently removes them.
    assert!(f.reconcile(Some(Quantity::from("10.00"))).is_empty());
    assert!(f.reconcile(None).is_empty());
    f.assert_settled("5.90 USDC", 1);
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        2
    );
}

#[rstest]
fn contract_settlement_session_close_keeps_normal_reconciliation() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::EndOfSession);
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .is_position_open(&f.position_id("OWNER-001"))
    );
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .instrument_close(&f.instrument.id())
            .is_none()
    );
    assert!(
        !f.reconcile(None).is_empty(),
        "quantity reconciliation must remain active"
    );
}

#[rstest]
fn contract_settlement_conflicting_price_stops_without_repricing() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    f.close("0.000", InstrumentCloseType::ContractExpired);
    assert!(f.node.handle.should_stop());
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .instrument_close(&f.instrument.id())
            .unwrap()
            .close_price,
        Price::from("1.000")
    );
    assert_eq!(
        f.node
            .kernel
            .portfolio
            .borrow_mut()
            .realized_pnl(&f.instrument.id()),
        Some(Money::from("5.90 USDC"))
    );
}

#[rstest]
fn contract_settlement_unknown_instrument_stops() {
    let mut f = Fixture::new();
    f.node
        .process_settlement(SettlementInput::Close(InstrumentClose::new(
            InstrumentId::from("UNKNOWN.POLYMARKET"),
            Price::from("1.000"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(1),
            UnixNanos::from(1),
        )));
    assert!(f.node.handle.should_stop());
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        0
    );
}

#[rstest]
fn contract_settlement_queue_unsubscribe_releases_both_handlers() {
    let mut f = Fixture::new();
    f.node.settlement.unsubscribe();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    assert!(f.node.settlement.receiver.try_recv().is_err());
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .is_position_open(&f.position_id("OWNER-001"))
    );
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .instrument_close(&f.instrument.id())
            .is_none()
    );
}

#[rstest]
fn contract_settlement_preparation_error_does_not_partially_apply() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN-A", OrderSide::Buy, "10.00", "0.400");
    f.fill("OWNER-002", "OPEN-B", OrderSide::Buy, "5.00", "0.200");
    let close = InstrumentClose::new(
        f.instrument.id(),
        Price::from("1.000"),
        InstrumentCloseType::ContractExpired,
        f.instrument.expiration_ns().unwrap(),
        f.instrument.expiration_ns().unwrap(),
    );
    let mut prepared = f.node.exec_manager.process_instrument_close(close).unwrap();
    assert_eq!(prepared.len(), 2);
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        2
    );
    // An existing but incomplete accounting order is an error, never a retry path.
    let collision = prepared.pop().unwrap();
    f.node
        .kernel
        .cache
        .borrow_mut()
        .add_order(collision.order, Some(collision.position_id), None, false)
        .unwrap();
    f.node.process_settlement(SettlementInput::Close(close));
    assert!(f.node.handle.should_stop());
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .positions_open_count(None, None, None, None, None),
        2
    );
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        3
    );
    assert!(
        f.positions
            .borrow()
            .iter()
            .all(|event| !matches!(event, PositionEvent::PositionClosed(_)))
    );
}

#[rstest]
fn contract_settlement_does_not_relax_unexpired_reduce_only() {
    let mut f = Fixture::new();
    f.fill(
        "OWNER-001",
        "UNEXPECTED-SELL",
        OrderSide::Sell,
        "3.00",
        "0.500",
    );
    assert_eq!(
        f.node
            .kernel
            .cache
            .borrow()
            .positions_open_count(None, None, None, None, None),
        0
    );
    assert!(f.positions.borrow().is_empty());
}

#[rstest]
#[tokio::test]
async fn contract_settlement_startup_drain_precedes_reconciliation() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    let close = InstrumentClose::new(
        f.instrument.id(),
        Price::from("1.000"),
        InstrumentCloseType::ContractExpired,
        f.instrument.expiration_ns().unwrap(),
        f.instrument.expiration_ns().unwrap(),
    );
    // Startup data is flushed before trader activation. Its bus callback only queues work.
    AsyncRunner::handle_data_event(DataEvent::Data(Data::InstrumentClose(close)));
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .is_position_open(&f.position_id("OWNER-001"))
    );
    f.node.perform_startup_reconciliation().await.unwrap();
    f.assert_settled("5.90 USDC", 1);
}

#[rstest]
#[case(None)]
#[case(Some(NodeRunMode::Owned))]
#[case(Some(NodeRunMode::Hosted))]
#[tokio::test]
async fn contract_settlement_startup_abort_reports_final_drain_fault(
    #[case] mode: Option<NodeRunMode>,
    #[values(false, true)] conflicting: bool,
) {
    let mut f = Fixture::new();
    // No venue operation is involved; the order stub has no asynchronous disconnect lifecycle.
    f.node
        .kernel
        .exec_engine
        .borrow_mut()
        .deregister_client(f.client_id)
        .unwrap();
    f.node.runner.as_ref().unwrap().bind_senders();
    let sender = nautilus_common::live::runner::get_data_event_sender();
    for price in ["1.000", if conflicting { "0.000" } else { "1.000" }] {
        sender
            .send(DataEvent::Data(Data::InstrumentClose(
                InstrumentClose::new(
                    f.instrument.id(),
                    Price::from(price),
                    InstrumentCloseType::ContractExpired,
                    f.instrument.expiration_ns().unwrap(),
                    f.instrument.expiration_ns().unwrap(),
                ),
            )))
            .unwrap();
    }
    // A separate stop request aborts connection before reconciliation handles the closes.
    // The fault is discovered only when the abort drains buffered channel traffic.
    f.node.handle().stop();
    assert!(f.node.check_execution_health().is_ok());

    let result = match mode {
        Some(mode) => f.node.run_with_mode(mode).await,
        None => f.node.start().await,
    };

    assert_eq!(f.node.state(), NodeState::Stopped);
    assert!(f.submitted.borrow().is_empty());

    if conflicting {
        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("Conflicting contract close"));
        assert!(f.node.check_execution_health().is_err());
    } else {
        result.unwrap();
        f.node.check_execution_health().unwrap();
    }
}

#[rstest]
fn contract_settlement_final_health_preserves_previous_failure() {
    let mut f = Fixture::new();
    f.close("1.000", InstrumentCloseType::ContractExpired);
    f.close("0.000", InstrumentCloseType::ContractExpired);

    let error = f
        .node
        .with_execution_health(Err(anyhow::anyhow!("connection failure")))
        .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("connection failure"));
    assert!(message.contains("Conflicting contract close"));
}

#[rstest]
fn contract_settlement_preserves_accounts_and_unrelated_instrument() {
    let mut f = Fixture::new();
    let first_account = f.account_id;
    f.fill("OWNER-001", "OPEN-A", OrderSide::Buy, "10.00", "0.400");
    let mut state = f
        .node
        .kernel
        .cache
        .borrow()
        .account(&first_account)
        .unwrap()
        .last_event()
        .unwrap();
    f.account_id = AccountId::from("POLYMARKET-002");
    state.account_id = f.account_id;
    f.node
        .kernel
        .cache
        .borrow_mut()
        .add_account(CashAccount::new(state, false, false).into())
        .unwrap();
    f.fill("OWNER-002", "OPEN-B", OrderSide::Buy, "5.00", "0.400");

    let contract = f.instrument.clone();
    let mut unrelated = binary_option();
    unrelated.id = InstrumentId::from("OTHER.POLYMARKET");
    f.instrument = InstrumentAny::BinaryOption(unrelated);
    f.node
        .kernel
        .cache
        .borrow_mut()
        .add_instrument(f.instrument.clone())
        .unwrap();
    f.fill("OWNER-003", "OPEN-OTHER", OrderSide::Buy, "4.00", "0.400");
    let unrelated = f.instrument.clone();
    f.instrument = contract;

    f.close("1.000", InstrumentCloseType::ContractExpired);
    f.assert_settled("8.80 USDC", 2);
    {
        let cache = f.node.kernel.cache.borrow();
        assert_eq!(
            cache
                .position(&f.position_id("OWNER-001"))
                .unwrap()
                .account_id,
            first_account
        );
        assert_eq!(
            cache
                .position(&f.position_id("OWNER-002"))
                .unwrap()
                .account_id,
            f.account_id
        );
        assert_eq!(
            cache
                .account(&first_account)
                .unwrap()
                .balance_total(Some(Currency::USDC())),
            Some(Money::from("100 USDC"))
        );
    }
    f.instrument = unrelated;
    assert!(
        f.node
            .kernel
            .cache
            .borrow()
            .is_position_open(&f.position_id("OWNER-003"))
    );
    assert!(
        !f.reconcile(None).is_empty(),
        "other instruments retain quantity reconciliation"
    );
}

#[rstest]
#[tokio::test]
async fn contract_settlement_queue_wakes_idle_runner() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    AsyncRunner::handle_data_event(DataEvent::Data(Data::InstrumentClose(
        InstrumentClose::new(
            f.instrument.id(),
            Price::from("1.000"),
            InstrumentCloseType::ContractExpired,
            f.instrument.expiration_ns().unwrap(),
            f.instrument.expiration_ns().unwrap(),
        ),
    )));
    f.node.process_runner_for(Duration::from_millis(1)).await;
    f.assert_settled("5.90 USDC", 1);
}

#[rstest]
#[tokio::test]
async fn contract_settlement_mass_status_keeps_resolved_inventory_flat(
    #[values(false, true)] late_cycles: bool,
) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    if late_cycles {
        f.fill("OWNER-001", "LATE-1", OrderSide::Buy, "2.50", "0.400");
        f.fill("OWNER-001", "LATE-2", OrderSide::Buy, "2.50", "0.400");
    }
    let held = if late_cycles { "15.00" } else { "10.00" };
    let pnl = if late_cycles {
        "8.70 USDC"
    } else {
        "5.90 USDC"
    };
    let closes = if late_cycles { 3 } else { 1 };

    for (venue_qty, expected) in [
        (Some(Quantity::from(held)), true),
        (None, true),
        (Some(Quantity::from("0.00")), true),
        (Some(Quantity::from("16.00")), false),
    ] {
        let mut mass = ExecutionMassStatus::new(
            f.client_id,
            f.account_id,
            f.instrument.id().venue,
            UnixNanos::from(1_000),
            None,
        );
        mass.add_position_reports(
            venue_qty
                .into_iter()
                .map(|quantity| {
                    PositionStatusReport::new(
                        f.account_id,
                        f.instrument.id(),
                        if quantity.is_zero() {
                            PositionSide::Flat
                        } else {
                            PositionSide::Long
                        },
                        quantity,
                        UnixNanos::from(1_000),
                        UnixNanos::from(1_000),
                        None,
                        None,
                        Some(dec!(0.4)),
                    )
                })
                .collect(),
        );
        let result = f
            .node
            .exec_manager
            .reconcile_execution_mass_status_ref(&mass, &f.node.kernel.exec_engine);
        assert!(result.events.is_empty());
        assert_eq!(result.summary.all_received_reports_reconciled(), expected);
        f.node.process_pending_settlements();
        let inventory = f
            .node
            .exec_manager
            .check_mass_status_inventory(&mass, &f.node.kernel.exec_engine.borrow());
        assert_eq!(inventory.all_inventory_reconciled(), expected);
        f.assert_settled(pnl, closes);
    }
}

#[rstest]
#[tokio::test]
async fn contract_settlement_inventory_requires_matching_account_and_instrument(
    #[values(false, true)] valid_account: bool,
    #[values(false, true)] valid_instrument: bool,
) {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    let mut mass = ExecutionMassStatus::new(
        f.client_id,
        f.account_id,
        f.instrument.id().venue,
        UnixNanos::from(1_000),
        None,
    );
    mass.add_position_reports(vec![PositionStatusReport::new(
        if valid_account {
            f.account_id
        } else {
            AccountId::from("OTHER-001")
        },
        if valid_instrument {
            f.instrument.id()
        } else {
            InstrumentId::from("UNKNOWN.POLYMARKET")
        },
        PositionSide::Long,
        Quantity::from("10.00"),
        UnixNanos::from(1_000),
        UnixNanos::from(1_000),
        None,
        None,
        Some(dec!(0.4)),
    )]);
    let result = f
        .node
        .exec_manager
        .reconcile_execution_mass_status_ref(&mass, &f.node.kernel.exec_engine);
    let inventory = f
        .node
        .exec_manager
        .check_mass_status_inventory(&mass, &f.node.kernel.exec_engine.borrow());
    assert_eq!(
        result.summary.all_received_reports_reconciled(),
        valid_account && valid_instrument
    );
    assert_eq!(
        inventory.all_inventory_reconciled(),
        valid_account && valid_instrument
    );
    f.assert_settled("5.90 USDC", 1);
}

#[rstest]
#[case::matching_history(true)]
#[case::conflicting_history(false)]
fn contract_settlement_mass_status_replays_prior_cycles_once(#[case] matching: bool) {
    let mut f = Fixture::new();
    let opening = f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    let late_1 = f.fill("OWNER-001", "LATE-1", OrderSide::Buy, "2.50", "0.400");
    let late_2 = f.fill("OWNER-001", "LATE-2", OrderSide::Buy, "2.50", "0.400");
    assert_eq!(opening.ts_event, late_1.ts_event);
    assert_eq!(opening.ts_event, late_2.ts_event);
    f.assert_settled("8.70 USDC", 3);

    let mut mass = ExecutionMassStatus::new(
        f.client_id,
        f.account_id,
        f.instrument.id().venue,
        opening.ts_event,
        None,
    );
    mass.set_report_window(None, true);
    mass.add_fill_reports(
        [opening, late_1, late_2]
            .into_iter()
            .map(|fill| {
                FillReport::new(
                    fill.account_id,
                    fill.instrument_id,
                    fill.venue_order_id,
                    fill.trade_id,
                    fill.order_side,
                    fill.last_qty,
                    fill.last_px,
                    if matching {
                        fill.commission.unwrap()
                    } else {
                        Money::from("0.20 USDC")
                    },
                    fill.liquidity_side,
                    Some(fill.client_order_id),
                    None,
                    fill.ts_event,
                    fill.ts_init,
                    None,
                )
            })
            .collect(),
    );
    for _ in 0..2 {
        let result = f
            .node
            .exec_manager
            .reconcile_execution_mass_status_ref(&mass, &f.node.kernel.exec_engine);
        f.node.process_pending_settlements();
        assert_eq!(result.summary.all_received_reports_reconciled(), matching);
        f.assert_settled("8.70 USDC", 3);
    }
}
