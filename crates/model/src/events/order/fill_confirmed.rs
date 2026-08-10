// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::fmt::{Debug, Display};

use nautilus_core::{UUID4, UnixNanos};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::{
    enums::{
        ContingencyType, LiquiditySide, OrderSide, OrderType, TimeInForce, TrailingOffsetType,
        TriggerType,
    },
    events::{OrderEvent, OrderFilled},
    identifiers::{
        AccountId, ClientOrderId, ExecAlgorithmId, InstrumentId, OrderListId, PositionId,
        StrategyId, TradeId, TraderId, VenueOrderId,
    },
    types::{Currency, Money, Price, Quantity},
};

/// Records that a provisional fill reached venue finality.
///
/// The referenced [`OrderFilled`] remains the sole economic record. This event stores immutable
/// identity and settlement evidence only, so engine-derived fill fields are not duplicated.
#[repr(C)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct OrderFillConfirmed {
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub account_id: AccountId,
    pub confirmation_id: Ustr,
    pub settlement_id: Ustr,
    pub trade_id: TradeId,
    pub fill_event_id: UUID4,
    pub event_id: UUID4,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub reconciliation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<UUID4>,
}

impl OrderFillConfirmed {
    #[must_use]
    pub fn new(
        fill: &OrderFilled,
        confirmation_id: Ustr,
        settlement_id: Ustr,
        event_id: UUID4,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            trader_id: fill.trader_id,
            strategy_id: fill.strategy_id,
            instrument_id: fill.instrument_id,
            client_order_id: fill.client_order_id,
            venue_order_id: fill.venue_order_id,
            account_id: fill.account_id,
            confirmation_id,
            settlement_id,
            trade_id: fill.trade_id,
            fill_event_id: fill.event_id,
            event_id,
            ts_event,
            ts_init,
            reconciliation: fill.reconciliation,
            causation_id: Some(fill.event_id),
        }
    }
}

impl Debug for OrderFillConfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(client_order_id={}, venue_order_id={}, confirmation_id={}, settlement_id={}, trade_id={}, fill_event_id={}, event_id={}, ts_event={}, ts_init={})",
            stringify!(OrderFillConfirmed),
            self.client_order_id,
            self.venue_order_id,
            self.confirmation_id,
            self.settlement_id,
            self.trade_id,
            self.fill_event_id,
            self.event_id,
            self.ts_event,
            self.ts_init,
        )
    }
}

impl Display for OrderFillConfirmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(instrument_id={}, client_order_id={}, venue_order_id={}, confirmation_id={}, settlement_id={}, trade_id={}, ts_event={})",
            stringify!(OrderFillConfirmed),
            self.instrument_id,
            self.client_order_id,
            self.venue_order_id,
            self.confirmation_id,
            self.settlement_id,
            self.trade_id,
            self.ts_event,
        )
    }
}

impl OrderEvent for OrderFillConfirmed {
    fn id(&self) -> UUID4 {
        self.event_id
    }

    fn type_name(&self) -> &'static str {
        stringify!(OrderFillConfirmed)
    }

    fn order_type(&self) -> Option<OrderType> {
        None
    }

    fn order_side(&self) -> Option<OrderSide> {
        None
    }

    fn trader_id(&self) -> TraderId {
        self.trader_id
    }

    fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    fn trade_id(&self) -> Option<TradeId> {
        Some(self.trade_id)
    }

    fn currency(&self) -> Option<Currency> {
        None
    }

    fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }

    fn reason(&self) -> Option<Ustr> {
        None
    }

    fn quantity(&self) -> Option<Quantity> {
        None
    }

    fn time_in_force(&self) -> Option<TimeInForce> {
        None
    }

    fn liquidity_side(&self) -> Option<LiquiditySide> {
        None
    }

    fn post_only(&self) -> Option<bool> {
        None
    }

    fn reduce_only(&self) -> Option<bool> {
        None
    }

    fn quote_quantity(&self) -> Option<bool> {
        None
    }

    fn reconciliation(&self) -> bool {
        self.reconciliation
    }

    fn price(&self) -> Option<Price> {
        None
    }

    fn last_px(&self) -> Option<Price> {
        None
    }

    fn last_qty(&self) -> Option<Quantity> {
        None
    }

    fn activation_price(&self) -> Option<Price> {
        None
    }

    fn trigger_price(&self) -> Option<Price> {
        None
    }

    fn trigger_type(&self) -> Option<TriggerType> {
        None
    }

    fn limit_offset(&self) -> Option<Decimal> {
        None
    }

    fn trailing_offset(&self) -> Option<Decimal> {
        None
    }

    fn trailing_offset_type(&self) -> Option<TrailingOffsetType> {
        None
    }

    fn expire_time(&self) -> Option<UnixNanos> {
        None
    }

    fn display_qty(&self) -> Option<Quantity> {
        None
    }

    fn emulation_trigger(&self) -> Option<TriggerType> {
        None
    }

    fn trigger_instrument_id(&self) -> Option<InstrumentId> {
        None
    }

    fn contingency_type(&self) -> Option<ContingencyType> {
        None
    }

    fn order_list_id(&self) -> Option<OrderListId> {
        None
    }

    fn linked_order_ids(&self) -> Option<Vec<ClientOrderId>> {
        None
    }

    fn parent_order_id(&self) -> Option<ClientOrderId> {
        None
    }

    fn exec_algorithm_id(&self) -> Option<ExecAlgorithmId> {
        None
    }

    fn exec_spawn_id(&self) -> Option<ClientOrderId> {
        None
    }

    fn venue_order_id(&self) -> Option<VenueOrderId> {
        Some(self.venue_order_id)
    }

    fn account_id(&self) -> Option<AccountId> {
        Some(self.account_id)
    }

    fn position_id(&self) -> Option<PositionId> {
        None
    }

    fn commission(&self) -> Option<Money> {
        None
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}
