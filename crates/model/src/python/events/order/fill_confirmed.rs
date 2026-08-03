// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use indexmap::IndexMap;
use nautilus_core::{
    UUID4,
    python::{
        IntoPyObjectNautilusExt,
        serialization::{from_dict_pyo3, to_dict_pyo3},
    },
};
use pyo3::{basic::CompareOp, prelude::*, types::PyDict};

use crate::{
    events::OrderFillConfirmed,
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, VenueOrderId,
    },
    orders::str_indexmap_to_ustr,
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OrderFillConfirmed {
    #[expect(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (trader_id, strategy_id, instrument_id, client_order_id, venue_order_id, account_id, trade_id, event_id, ts_event, ts_init, reconciliation, info=None))]
    fn py_new(
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        account_id: AccountId,
        trade_id: TradeId,
        event_id: UUID4,
        ts_event: u64,
        ts_init: u64,
        reconciliation: bool,
        info: Option<IndexMap<String, String>>,
    ) -> Self {
        Self::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            account_id,
            trade_id,
            info.map(str_indexmap_to_ustr),
            event_id,
            ts_event.into(),
            ts_init.into(),
            reconciliation,
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp, py: Python<'_>) -> Py<PyAny> {
        match op {
            CompareOp::Eq => self.eq(other).into_py_any_unwrap(py),
            CompareOp::Ne => self.ne(other).into_py_any_unwrap(py),
            _ => py.NotImplemented(),
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    #[getter]
    fn trader_id(&self) -> TraderId {
        self.trader_id
    }

    #[getter]
    fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    #[getter]
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter]
    fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }

    #[getter]
    fn venue_order_id(&self) -> VenueOrderId {
        self.venue_order_id
    }

    #[getter]
    fn account_id(&self) -> AccountId {
        self.account_id
    }

    #[getter]
    fn trade_id(&self) -> TradeId {
        self.trade_id
    }

    #[getter]
    fn info(&self) -> Option<IndexMap<&str, &str>> {
        self.info.as_ref().map(|info| {
            info.iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect()
        })
    }

    #[getter]
    fn event_id(&self) -> UUID4 {
        self.event_id
    }

    #[getter]
    fn ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter]
    fn ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }

    #[getter]
    fn reconciliation(&self) -> bool {
        self.reconciliation
    }

    #[getter]
    fn causation_id(&self) -> Option<UUID4> {
        self.causation_id
    }

    #[staticmethod]
    #[pyo3(name = "from_dict")]
    fn py_from_dict(py: Python<'_>, values: Py<PyDict>) -> PyResult<Self> {
        from_dict_pyo3(py, values)
    }

    #[pyo3(name = "to_dict")]
    fn py_to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        to_dict_pyo3(py, self)
    }
}
