// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use indexmap::IndexMap;
use nautilus_core::{UUID4, UnixNanos};
use ustr::Ustr;

use crate::{
    events::OrderFillConfirmed,
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, VenueOrderId,
    },
    stubs::{TestDefault, test_uuid},
};

/// Test-only fluent spec for [`OrderFillConfirmed`].
#[derive(Debug, Clone, bon::Builder)]
#[builder(finish_fn = into_spec)]
pub struct OrderFillConfirmedSpec {
    #[builder(default = TraderId::test_default())]
    pub trader_id: TraderId,
    #[builder(default = StrategyId::test_default())]
    pub strategy_id: StrategyId,
    #[builder(default = InstrumentId::test_default())]
    pub instrument_id: InstrumentId,
    #[builder(default = ClientOrderId::test_default())]
    pub client_order_id: ClientOrderId,
    #[builder(default = VenueOrderId::test_default())]
    pub venue_order_id: VenueOrderId,
    #[builder(default = AccountId::test_default())]
    pub account_id: AccountId,
    #[builder(default = TradeId::test_default())]
    pub trade_id: TradeId,
    pub info: Option<IndexMap<Ustr, Ustr>>,
    #[builder(default = test_uuid())]
    pub event_id: UUID4,
    #[builder(default = UnixNanos::default())]
    pub ts_event: UnixNanos,
    #[builder(default = UnixNanos::default())]
    pub ts_init: UnixNanos,
    #[builder(default = false)]
    pub reconciliation: bool,
}

impl<S: order_fill_confirmed_spec_builder::IsComplete> OrderFillConfirmedSpecBuilder<S> {
    /// Builds the spec through the production constructor.
    #[must_use]
    pub fn build(self) -> OrderFillConfirmed {
        let spec = self.into_spec();
        OrderFillConfirmed::new(
            spec.trader_id,
            spec.strategy_id,
            spec.instrument_id,
            spec.client_order_id,
            spec.venue_order_id,
            spec.account_id,
            spec.trade_id,
            spec.info,
            spec.event_id,
            spec.ts_event,
            spec.ts_init,
            spec.reconciliation,
        )
    }
}
