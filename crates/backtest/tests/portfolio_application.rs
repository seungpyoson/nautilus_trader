// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautilustrader.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Execution acknowledgement of real Portfolio account calculations over a shared native cache.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clock::{Clock, TestClock},
    messages::execution::EventApplicationOutcome,
    msgbus::{self, MessageBus, MessagingSwitchboard},
};
use nautilus_core::UUID4;
use nautilus_execution::engine::ExecutionEngine;
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::QuoteTick,
    enums::{AccountType, OrderSide, OrderStatus, OrderType},
    events::{AccountState, OrderEventAny, order::spec::OrderFilledSpec},
    identifiers::{AccountId, PositionId, Symbol, VenueOrderId},
    instruments::{
        Instrument, InstrumentAny,
        stubs::{audusd_sim, default_fx_ccy},
    },
    orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
    stubs::TestDefault,
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use nautilus_portfolio::Portfolio;
use rstest::rstest;

fn funded_execution(
    account_type: AccountType,
) -> (Portfolio, Rc<RefCell<ExecutionEngine>>, OrderAny) {
    *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut portfolio = Portfolio::new(Rc::clone(&clock), Rc::clone(&cache), None);
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock,
        Rc::clone(&cache),
        None,
    )));
    ExecutionEngine::register_msgbus_handlers(&engine);
    let account_id = AccountId::test_default();
    let total = Money::from("1000.00 USD");
    portfolio.update_account(&AccountState::new(
        account_id,
        account_type,
        vec![AccountBalance::new(
            total,
            Money::zero(Currency::USD()),
            total,
        )],
        vec![],
        true,
        UUID4::new(),
        0.into(),
        0.into(),
        Some(Currency::USD()),
    ));
    cache
        .borrow_mut()
        .account_mut(&account_id)
        .unwrap()
        .set_calculate_account_state(true);
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            instrument.id(),
            Price::from("1.00000"),
            Price::from("1.00000"),
            Quantity::from("10"),
            Quantity::from("10"),
            0.into(),
            0.into(),
        ))
        .unwrap();
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10"))
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let endpoint = MessagingSwitchboard::exec_engine_process();
    for event in [
        TestOrderEventStubs::submitted(&order, account_id),
        TestOrderEventStubs::accepted(&order, account_id, VenueOrderId::from("V-APPLICATION")),
    ] {
        assert_eq!(
            msgbus::send_order_event_with_outcome(endpoint, event),
            Some(EventApplicationOutcome::Applied)
        );
    }
    let order = cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .clone();
    (portfolio, engine, order)
}

#[rstest]
#[case(AccountType::Cash, "986.00 USD")]
#[case(AccountType::Margin, "996.00 USD")]
fn execution_requires_portfolio_fee_conversion_but_preserves_executed_position(
    #[case] account_type: AccountType,
    #[case] healthy_balance: &str,
    #[values(false, true)] conversion_available: bool,
) {
    let (_portfolio, engine, order) = funded_execution(account_type);
    let cache = Rc::clone(engine.borrow().cache());
    let instrument_id = order.instrument_id();
    let account_id = order.account_id().unwrap();
    let endpoint = MessagingSwitchboard::exec_engine_process();
    if conversion_available {
        let conversion = InstrumentAny::CurrencyPair(default_fx_ccy(
            Symbol::from("GBP/USD"),
            Some(instrument_id.venue),
        ));
        let quote = QuoteTick::new(
            conversion.id(),
            Price::from("2.00000"),
            Price::from("2.00000"),
            Quantity::from("10"),
            Quantity::from("10"),
            0.into(),
            0.into(),
        );
        cache.borrow_mut().add_instrument(conversion).unwrap();
        cache.borrow_mut().add_quote(quote).unwrap();
    }
    let fill = OrderFilledSpec::builder()
        .instrument_id(instrument_id)
        .client_order_id(order.client_order_id())
        .account_id(account_id)
        .venue_order_id(VenueOrderId::from("V-APPLICATION"))
        .order_side(OrderSide::Buy)
        .last_qty(Quantity::from("10"))
        .last_px(Price::from("1.00000"))
        .position_id(PositionId::from("P-FEE"))
        .commission(Money::from("2.00 GBP"))
        .build();

    assert_eq!(
        msgbus::send_order_event_with_outcome(endpoint, OrderEventAny::Filled(fill)),
        Some(if conversion_available {
            EventApplicationOutcome::Applied
        } else {
            EventApplicationOutcome::Incomplete
        }),
    );
    let cache = cache.borrow();
    let account = cache.account(&account_id).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USD())),
        Some(Money::from(if conversion_available {
            healthy_balance
        } else {
            "1000.00 USD"
        })),
    );
    let commission = match &*account {
        AccountAny::Cash(account) => account.commission(&Currency::USD()),
        AccountAny::Margin(account) => account.commission(&Currency::USD()),
        _ => unreachable!(),
    };
    assert_eq!(
        commission,
        conversion_available.then_some(Money::from("4.00 USD"))
    );
    assert_eq!(
        cache.order(&order.client_order_id()).unwrap().status(),
        OrderStatus::Filled
    );
    let positions = cache.positions_open(None, Some(&instrument_id), None, Some(&account_id), None);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, Quantity::from("10"));
    assert_eq!(
        positions[0].commissions.get(&Currency::GBP()),
        Some(&Money::from("2.00 GBP"))
    );
}

#[rstest]
fn cancellation_invalidates_cached_pnl_when_assigned_account_is_missing(
    #[values(false, true)] account_available: bool,
) {
    let (mut portfolio, engine, order) = funded_execution(AccountType::Cash);
    let cache = Rc::clone(engine.borrow().cache());
    let instrument_id = order.instrument_id();
    let account_id = order.account_id().unwrap();
    let zero = Money::zero(Currency::USD());
    assert_eq!(portfolio.unrealized_pnl(&instrument_id), Some(zero));
    if !account_available {
        assert!(cache.borrow_mut().take_account(&account_id).is_some());
        assert!(cache.borrow().account(&account_id).is_none());
        // The cached value is still warm until the native Portfolio owner invalidates it.
        assert_eq!(portfolio.unrealized_pnl(&instrument_id), Some(zero));
    }

    assert_eq!(
        msgbus::send_order_event_with_outcome(
            MessagingSwitchboard::exec_engine_process(),
            TestOrderEventStubs::canceled(&order, account_id, order.venue_order_id()),
        ),
        Some(if account_available {
            EventApplicationOutcome::Applied
        } else {
            EventApplicationOutcome::Incomplete
        }),
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled,
    );
    assert_eq!(
        portfolio.unrealized_pnl(&instrument_id),
        account_available.then_some(zero)
    );
    assert_eq!(
        cache
            .borrow()
            .positions_total_count(None, None, None, None, None),
        0
    );
}

#[rstest]
fn margin_fill_requires_instrument_before_account_or_position_economics(
    #[values(false, true)] instrument_available: bool,
) {
    let (mut portfolio, engine, order) = funded_execution(AccountType::Margin);
    let cache = Rc::clone(engine.borrow().cache());
    let instrument_id = order.instrument_id();
    let account_id = order.account_id().unwrap();
    let (balances_before, events_before) = {
        let cache = cache.borrow();
        let account = cache.account(&account_id).unwrap();
        (account.balances(), account.event_count())
    };
    let zero = Money::zero(Currency::USD());
    assert_eq!(portfolio.unrealized_pnl(&instrument_id), Some(zero));
    if !instrument_available {
        cache
            .borrow_mut()
            .purge_instrument_skip_order_guard(instrument_id);
        assert!(cache.borrow().instrument(&instrument_id).is_none());
        assert_eq!(portfolio.unrealized_pnl(&instrument_id), Some(zero));
    }
    let fill = OrderFilledSpec::builder()
        .instrument_id(instrument_id)
        .client_order_id(order.client_order_id())
        .account_id(account_id)
        .venue_order_id(order.venue_order_id().unwrap())
        .order_side(OrderSide::Buy)
        .last_qty(Quantity::from("10"))
        .last_px(Price::from("1.00000"))
        .position_id(PositionId::from("P-INSTRUMENT"))
        .commission(Money::from("2.00 USD"))
        .build();

    assert_eq!(
        msgbus::send_order_event_with_outcome(
            MessagingSwitchboard::exec_engine_process(),
            OrderEventAny::Filled(fill),
        ),
        Some(if instrument_available {
            EventApplicationOutcome::Applied
        } else {
            EventApplicationOutcome::Incomplete
        }),
    );
    assert_eq!(
        portfolio.unrealized_pnl(&instrument_id),
        instrument_available.then_some(zero)
    );
    let cache = cache.borrow();
    assert_eq!(
        cache.order(&order.client_order_id()).unwrap().status(),
        OrderStatus::Filled
    );
    assert_eq!(
        cache.positions_total_count(None, None, None, None, None),
        usize::from(instrument_available),
    );
    let account = cache.account(&account_id).unwrap();
    let AccountAny::Margin(margin) = &*account else {
        panic!("Expected margin account")
    };
    assert_eq!(
        margin.commission(&Currency::USD()),
        instrument_available.then_some(Money::from("2.00 USD"))
    );
    if instrument_available {
        assert_eq!(
            account.balance_total(Some(Currency::USD())),
            Some(Money::from("998.00 USD"))
        );
        assert!(account.event_count() > events_before);
        let position_id = cache.position_id(&order.client_order_id()).unwrap();
        assert_eq!(
            *position_id,
            PositionId::new(format!("{instrument_id}-{}", order.strategy_id())),
        );
        let position = cache.position(position_id).unwrap();
        assert_eq!(position.quantity, Quantity::from("10"));
    } else {
        assert_eq!(account.balances(), balances_before);
        assert_eq!(account.event_count(), events_before);
    }
}
