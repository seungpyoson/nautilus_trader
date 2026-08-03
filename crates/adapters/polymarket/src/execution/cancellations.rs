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

use anyhow::Context;
use nautilus_common::messages::execution::{BatchCancelOrders, CancelAllOrders, CancelOrder};
use nautilus_core::time::AtomicTime;
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    identifiers::VenueOrderId,
    orders::{Order, OrderAny},
};

use super::{
    PolymarketExecutionClient,
    local_orders::{CancelAdmission, LocalOrderSnapshot},
};
use crate::{execution::types::CancelOutcome, http::query::CancelResponse};

impl PolymarketExecutionClient {
    pub(super) fn cancel_order_command(&self, cmd: &CancelOrder) {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone());
        let order_ref = match &order {
            Some(o) => o,
            None => {
                log::warn!(
                    "Order not found in cache for cancel: {}",
                    cmd.client_order_id
                );
                return;
            }
        };

        if order_ref.is_closed() {
            log::warn!("Cannot cancel closed order: {}", cmd.client_order_id);
            return;
        }

        if cmd.strategy_id != order_ref.strategy_id()
            || cmd.instrument_id != order_ref.instrument_id()
        {
            log::error!(
                "Cancel for {} rejected: command identity conflicts with the cached order",
                cmd.client_order_id
            );
            return;
        }

        let venue_order_id = match self.local_orders.request_cancel(
            LocalOrderSnapshot::from_order(order_ref),
            cmd.venue_order_id,
        ) {
            CancelAdmission::Ready(id) => id,
            CancelAdmission::Deferred => {
                log::debug!(
                    "Cancel for {} deferred until its local identity is accepted",
                    cmd.client_order_id
                );
                return;
            }
            CancelAdmission::Conflict => {
                log::error!(
                    "Cancel for {} rejected: local order identity is missing or conflicting",
                    cmd.client_order_id
                );
                return;
            }
        };

        let clock = self.clock;
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let order_id_str = venue_order_id.to_string();
        let order_clone = order.unwrap();

        self.spawn_task("cancel_order", async move {
            match submitter.cancel_order(&order_id_str).await {
                Ok(response) => {
                    process_cancel_result(
                        &response,
                        &order_id_str,
                        &order_clone,
                        venue_order_id,
                        &emitter,
                        clock,
                    );
                }
                Err(e) => {
                    log::warn!(
                        "Cancel outcome unknown for {} ({}), awaiting reconciliation: {e}",
                        order_clone.client_order_id(),
                        venue_order_id,
                    );
                    return Err(anyhow::Error::new(e).context("cancel order failed"));
                }
            }
            Ok(())
        });
    }

    pub(super) fn cancel_all_orders_command(&self, cmd: &CancelAllOrders) {
        let cache = self.core.cache();
        let open_orders = cache.orders_open(
            Some(&self.core.venue),
            Some(&cmd.instrument_id),
            Some(&cmd.strategy_id),
            None,
            Some(cmd.order_side),
        );

        if open_orders.is_empty() {
            log::debug!("No open orders to cancel for {}", cmd.instrument_id);
            return;
        }

        let mut venue_order_ids = Vec::new();
        let mut orders = Vec::new();

        for order in open_orders {
            match self
                .local_orders
                .request_cancel(LocalOrderSnapshot::from_order(&order), None)
            {
                CancelAdmission::Ready(venue_order_id) => {
                    venue_order_ids.push(venue_order_id.to_string());
                    orders.push((venue_order_id, order.clone()));
                }
                CancelAdmission::Deferred => {
                    log::debug!(
                        "Cancel all for {} deferred until its local identity is accepted",
                        order.client_order_id()
                    );
                }
                CancelAdmission::Conflict => log::error!(
                    "Cancel all skipped {}: local order identity is missing or conflicting",
                    order.client_order_id()
                ),
            }
        }

        if venue_order_ids.is_empty() {
            log::debug!("No matching cancel has an admitted local order identity");
            return;
        }

        let clock = self.clock;
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();

        self.spawn_task("cancel_all_orders", async move {
            let order_id_refs: Vec<&str> = venue_order_ids.iter().map(String::as_str).collect();
            let response = submitter
                .cancel_orders(&order_id_refs)
                .await
                .context("failed to cancel all orders")?;

            for (venue_order_id, order) in &orders {
                let venue_order_id_str = venue_order_id.to_string();
                process_cancel_result(
                    &response,
                    &venue_order_id_str,
                    order,
                    *venue_order_id,
                    &emitter,
                    clock,
                );
            }

            log::debug!("Canceled {} orders", response.canceled.len());
            Ok(())
        });
    }

    pub(super) fn batch_cancel_orders_command(&self, cmd: &BatchCancelOrders) {
        if cmd.cancels.is_empty() {
            return;
        }

        let mut venue_to_order: Vec<(String, OrderAny)> = Vec::new();

        for c in &cmd.cancels {
            if let Some(order) = self.core.cache().order(&c.client_order_id) {
                match self
                    .local_orders
                    .request_cancel(LocalOrderSnapshot::from_order(&order), c.venue_order_id)
                {
                    CancelAdmission::Ready(venue_order_id) => {
                        venue_to_order.push((venue_order_id.to_string(), order.clone()));
                    }
                    CancelAdmission::Deferred => {
                        log::debug!(
                            "Batch cancel for {} deferred until its local identity is accepted",
                            c.client_order_id
                        );
                    }
                    CancelAdmission::Conflict => log::error!(
                        "Batch cancel skipped {}: local order identity is missing or conflicting",
                        c.client_order_id
                    ),
                }
            }
        }

        if venue_to_order.is_empty() {
            log::debug!("No batch cancel has an admitted local order identity");
            return;
        }

        let clock = self.clock;
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let order_ids: Vec<String> = venue_to_order.iter().map(|(id, _)| id.clone()).collect();

        self.spawn_task("batch_cancel_orders", async move {
            let order_id_refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
            let response = submitter
                .cancel_orders(&order_id_refs)
                .await
                .context("failed to batch cancel orders")?;

            for (venue_id_str, order) in &venue_to_order {
                let vid = VenueOrderId::from(venue_id_str.as_str());
                process_cancel_result(&response, venue_id_str, order, vid, &emitter, clock);
            }

            log::debug!("Batch canceled {} orders", response.canceled.len());
            Ok(())
        });
    }
}

pub(super) fn process_cancel_result(
    response: &CancelResponse,
    venue_order_id_str: &str,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) -> CancelResponseStatus {
    if let Some(reason_opt) = response.not_canceled.get(venue_order_id_str) {
        let reason = reason_opt.as_deref().unwrap_or("unknown reason");
        match CancelOutcome::classify(reason) {
            CancelOutcome::AlreadyDone => {
                log::debug!(
                    "Cancel rejected for {}: {reason} - awaiting WS for terminal state",
                    order.client_order_id()
                );
            }
            CancelOutcome::Rejected(msg) => {
                let ts_now = clock.get_time_ns();
                emitter.emit_order_cancel_rejected(order, Some(venue_order_id), &msg, ts_now);
            }
        }
        return CancelResponseStatus::PerOrderResult;
    }

    if response
        .canceled
        .iter()
        .any(|order_id| order_id == venue_order_id_str)
    {
        return CancelResponseStatus::PerOrderResult;
    }

    log::warn!(
        "Cancel response for {} did not include per-order result for {}",
        order.client_order_id(),
        venue_order_id
    );
    CancelResponseStatus::MissingPerOrderResult
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CancelResponseStatus {
    PerOrderResult,
    MissingPerOrderResult,
}

pub(super) async fn execute_deferred_cancel(
    submitter: &super::submitter::OrderSubmitter,
    order: &OrderAny,
    order_id_str: &str,
    venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) {
    match submitter.cancel_order(order_id_str).await {
        Ok(response) => {
            process_cancel_result(
                &response,
                order_id_str,
                order,
                venue_order_id,
                emitter,
                clock,
            );
        }
        Err(e) => {
            log::warn!(
                "Deferred cancel outcome unknown for {} ({}), awaiting reconciliation: {e}",
                order.client_order_id(),
                venue_order_id,
            );
        }
    }
}
