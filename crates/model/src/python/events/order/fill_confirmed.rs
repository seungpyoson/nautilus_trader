// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use nautilus_core::{
    UUID4,
    python::{
        IntoPyObjectNautilusExt,
        serialization::{from_dict_pyo3, to_dict_pyo3},
    },
};
use pyo3::{basic::CompareOp, prelude::*, types::PyDict};
use ustr::Ustr;

use crate::{
    events::{OrderFillConfirmed, OrderFilled},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, VenueOrderId,
    },
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl OrderFillConfirmed {
    /// Records that a provisional fill reached venue finality.
    #[new]
    fn py_new(
        fill: OrderFilled,
        confirmation_id: &str,
        settlement_id: &str,
        event_id: UUID4,
        ts_event: u64,
        ts_init: u64,
    ) -> Self {
        Self::new(
            &fill,
            Ustr::from(confirmation_id),
            Ustr::from(settlement_id),
            event_id,
            ts_event.into(),
            ts_init.into(),
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
    fn confirmation_id(&self) -> &str {
        self.confirmation_id.as_str()
    }

    #[getter]
    fn settlement_id(&self) -> &str {
        self.settlement_id.as_str()
    }

    #[getter]
    fn trade_id(&self) -> TradeId {
        self.trade_id
    }

    #[getter]
    fn fill_event_id(&self) -> UUID4 {
        self.fill_event_id
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
