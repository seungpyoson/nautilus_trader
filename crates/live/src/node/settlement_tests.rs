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

use nautilus_core::UnixNanos;
use nautilus_execution::engine::stubs::StubExecutionClient;
use nautilus_model::{
    accounts::{Account, CashAccount},
    data::Data,
    enums::{
        AccountType, InstrumentCloseType, LiquiditySide, OmsType, OrderSide, OrderType,
        PositionSide,
    },
    events::{AccountState, OrderFilled},
    identifiers::{AccountId, PositionId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny, stubs::binary_option},
    orders::{OrderTestBuilder, stubs::TestOrderEventStubs},
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
    positions: Rc<RefCell<Vec<PositionEvent>>>,
    position_handler: TypedHandler<PositionEvent>,
}

impl Fixture {
    fn new() -> Self {
        let config = LiveNodeConfig {
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
                Money::from("100 USDC"),
                Money::from("0 USDC"),
                Money::from("100 USDC"),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(Currency::USDC()),
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
        let order = self
            .node
            .kernel
            .cache
            .borrow_mut()
            .update_order(&accepted)
            .unwrap();
        // These are real fills executed before expiry but may be delivered after settlement.
        let ts_event = UnixNanos::from(self.instrument.expiration_ns().unwrap().as_u64() - 1);
        let event = TestOrderEventStubs::filled(
            &order,
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

    fn assert_settled(&mut self, expected_pnl: &str, closes: usize) {
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
async fn contract_settlement_mass_status_keeps_resolved_inventory_flat() {
    let mut f = Fixture::new();
    f.fill("OWNER-001", "OPEN", OrderSide::Buy, "10.00", "0.400");
    f.close("1.000", InstrumentCloseType::ContractExpired);
    for venue_qty in [Some(Quantity::from("10.00")), None] {
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
                        PositionSide::Long,
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
            .reconcile_execution_mass_status(mass, f.node.kernel.exec_engine.clone())
            .await;
        assert!(result.events.is_empty());
        f.node.process_pending_settlements();
        f.assert_settled("5.90 USDC", 1);
    }
}
