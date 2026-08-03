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
use nautilus_common::messages::execution::{
    GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
    GeneratePositionStatusReports, QueryAccount, QueryOrder,
};
use nautilus_core::{UnixNanos, collections::AtomicMap, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderStatus, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    PolymarketExecutionClient,
    local_orders::{ArtifactAdmission, LocalOrderCoordinator},
    parse::{
        parse_balance_allowance, parse_order_status_report, sum_filled_quantity,
        weighted_average_price,
    },
    reconciliation::{
        FillContext, apply_fill_filters, build_fill_reports_from_trades,
        build_pending_fill_reports_from_trades, build_position_reports,
        confirmed_filled_quantities,
    },
};
use crate::{
    common::{consts::DUST_SNAP_THRESHOLD_DEC, enums::SignatureType},
    http::{
        clob::PolymarketClobHttpClient,
        models::{PolymarketOpenOrder, PolymarketTradeReport},
        query::{GetBalanceAllowanceParams, GetTradesParams},
    },
};

#[derive(Clone, Copy)]
enum RecoveryTradeStatus {
    Pending,
    Confirmed,
}

fn trade_references_order(trade: &PolymarketTradeReport, venue_order_id: VenueOrderId) -> bool {
    trade.taker_order_id == venue_order_id.as_str()
        || trade
            .maker_orders
            .iter()
            .any(|order| order.order_id == venue_order_id.as_str())
}

fn build_recovery_fill_reports(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    venue_order_id: VenueOrderId,
    ts_init: UnixNanos,
    status: RecoveryTradeStatus,
) -> (Vec<FillReport>, bool) {
    let mut reports = Vec::new();
    let mut unresolved_identity = false;

    for trade in trades.iter().filter(|trade| {
        trade_references_order(trade, venue_order_id)
            && match status {
                RecoveryTradeStatus::Pending => trade.status.is_pending_settlement(),
                RecoveryTradeStatus::Confirmed => {
                    trade.status == crate::common::enums::PolymarketTradeStatus::Confirmed
                }
            }
    }) {
        let trade = std::slice::from_ref(trade);
        let (built, _) = match status {
            RecoveryTradeStatus::Pending => {
                build_pending_fill_reports_from_trades(trade, ctx, instruments, ts_init)
            }
            RecoveryTradeStatus::Confirmed => {
                build_fill_reports_from_trades(trade, ctx, instruments, None, ts_init)
            }
        };
        let matching: Vec<_> = built
            .into_iter()
            .filter(|fill| fill.venue_order_id == venue_order_id)
            .collect();
        unresolved_identity |= matching.is_empty();
        reports.extend(matching);
    }

    (reports, unresolved_identity)
}

fn open_order_matches_request(
    order: &PolymarketOpenOrder,
    venue_order_id: VenueOrderId,
    expected_asset_id: &str,
) -> bool {
    order.id == venue_order_id.as_str() && order.asset_id.as_str() == expected_asset_id
}

impl PolymarketExecutionClient {
    pub(super) fn fill_context(&self) -> FillContext<'_> {
        let user_address = self
            .secrets
            .funder
            .as_deref()
            .unwrap_or(&self.secrets.address);
        FillContext {
            account_id: self.core.account_id,
            user_address,
            api_key: self.secrets.credential.api_key().as_str(),
            pusd: get_pusd_currency(),
            clock: self.clock,
        }
    }

    pub(super) async fn recover_terminal_status_from_trades(
        &self,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        size_prec: u8,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let ts_init = self.clock.get_time_ns();
        let ctx = self.fill_context();

        let trades = self
            .http_client
            .get_trades(GetTradesParams::default())
            .await
            .context("failed to fetch trades for order recovery")?;

        let Some(local) = self.local_orders.snapshot(&venue_order_id) else {
            log::debug!(
                "Order {venue_order_id} is not active at the venue and has no local identity; deferring recovery"
            );
            return Ok(None);
        };
        if local.identity.instrument_id != instrument_id
            || client_order_id.is_some_and(|client| client != local.identity.client_order_id)
        {
            log::error!(
                "Deferring terminal recovery for {venue_order_id}: requested identity conflicts with the local order"
            );
            return Ok(None);
        }
        let resolved_client_order_id = Some(local.identity.client_order_id);

        let (pending_fills, pending_unresolved) = build_recovery_fill_reports(
            &trades,
            &ctx,
            &self.shared_token_instruments,
            venue_order_id,
            ts_init,
            RecoveryTradeStatus::Pending,
        );
        let pending = self
            .local_orders
            .admit_reconciliation_fill_reports(pending_fills);
        if pending_unresolved || pending.conflicts > 0 {
            log::warn!(
                "Deferring terminal recovery for {venue_order_id}: pending trade identity is incomplete or conflicting"
            );
            return Ok(None);
        }

        if !pending.artifacts.is_empty() {
            let order_status = if local.filled_qty.is_zero() {
                OrderStatus::Accepted
            } else {
                OrderStatus::PartiallyFilled
            };
            let mut report = OrderStatusReport::new(
                self.core.account_id,
                instrument_id,
                resolved_client_order_id,
                venue_order_id,
                local.identity.order_side,
                local.identity.order_type,
                local.identity.time_in_force,
                order_status,
                local.quantity,
                local.filled_qty,
                ts_init,
                ts_init,
                ts_init,
                None,
            );
            report.price = local.price;

            let report = owned_order_report(
                self.local_orders
                    .admit_pending_recovery_order_report(report),
                "pending terminal recovery",
            );
            if let Some(report) = &report {
                log::debug!(
                    "Order {venue_order_id} has unsettled trades; reporting non-terminal {}",
                    report.order_status
                );
            }
            return Ok(report);
        }

        let (order_fills, confirmed_unresolved) = build_recovery_fill_reports(
            &trades,
            &ctx,
            &self.shared_token_instruments,
            venue_order_id,
            ts_init,
            RecoveryTradeStatus::Confirmed,
        );
        let admitted = self
            .local_orders
            .admit_reconciliation_fill_reports(order_fills);
        if confirmed_unresolved || admitted.conflicts > 0 {
            log::warn!(
                "Deferring terminal recovery for {venue_order_id}: confirmed trade identity conflicts with the local order"
            );
            return Ok(None);
        }
        let order_fills = admitted.artifacts;

        if order_fills.is_empty() {
            log::debug!(
                "Order {venue_order_id} not active at venue and no trades found; recovering as Canceled"
            );
            let mut report = OrderStatusReport::new(
                self.core.account_id,
                instrument_id,
                resolved_client_order_id,
                venue_order_id,
                local.identity.order_side,
                local.identity.order_type,
                local.identity.time_in_force,
                OrderStatus::Canceled,
                local.quantity,
                local.filled_qty,
                ts_init,
                ts_init,
                ts_init,
                None,
            );
            report.price = local.price;
            report.cancel_reason = Some("ORDER_NOT_FOUND_AT_VENUE".to_string());
            return Ok(owned_order_report(
                self.local_orders
                    .admit_unfilled_terminal_order_report(report),
                "unfilled terminal recovery",
            ));
        }

        let quantity = local.quantity;

        let total_filled_dec = sum_filled_quantity(&order_fills);
        let avg_px = weighted_average_price(&order_fills, total_filled_dec);
        let raw_filled_qty = Quantity::from_decimal_dp(total_filled_dec, size_prec)
            .unwrap_or_else(|_| Quantity::zero(size_prec));
        let order_side = local.identity.order_side;
        let ts_event = order_fills
            .iter()
            .map(|f| f.ts_event)
            .max()
            .unwrap_or(ts_init);

        let order_status =
            recovered_terminal_order_status(local.identity.time_in_force, quantity, raw_filled_qty);
        let filled_qty = raw_filled_qty;

        log::debug!(
            "Recovered {} status for {venue_order_id} from {} trade(s) (filled_qty={filled_qty}, quantity={quantity})",
            if order_status == OrderStatus::Filled {
                "Filled"
            } else {
                "Canceled (partially filled)"
            },
            order_fills.len(),
        );

        let mut report = OrderStatusReport::new(
            self.core.account_id,
            instrument_id,
            resolved_client_order_id,
            venue_order_id,
            order_side,
            local.identity.order_type,
            local.identity.time_in_force,
            order_status,
            quantity,
            filled_qty,
            ts_event,
            ts_event,
            ts_init,
            None,
        );
        report.price = local.price;
        report.avg_px = avg_px;
        Ok(admit_point_of_use_order_report(
            &self.local_orders,
            report,
            raw_filled_qty,
            Some(total_filled_dec),
            "confirmed terminal recovery",
        ))
    }

    pub(super) fn query_account_command(&self, _cmd: QueryAccount) {
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let signature_type = self.config.signature_type;

        self.spawn_task("query_account", async move {
            fetch_and_emit_account_state(&http_client, &emitter, clock, signature_type).await
        });
    }

    pub(super) fn query_order_command(&self, cmd: &QueryOrder) {
        log::debug!("Querying order: client_order_id={}", cmd.client_order_id);

        let venue_order_id = match &cmd.venue_order_id {
            Some(id) => id.to_string(),
            None => {
                log::warn!("query_order requires venue_order_id for Polymarket");
                return;
            }
        };

        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let account_id = self.core.account_id;
        let cache = self.core.cache();

        let (price_prec, size_prec, expected_asset_id) = match cache.instrument(&instrument_id) {
            Some(i) => (
                i.price_precision(),
                i.size_precision(),
                i.raw_symbol().to_string(),
            ),
            None => {
                log::warn!(
                    "Cannot query {venue_order_id}: instrument {instrument_id} is unavailable"
                );
                return;
            }
        };

        let http_client = self.http_client.clone();
        let local_orders = self.local_orders.clone();
        let token_instruments = self.shared_token_instruments.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let user_address = self
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| self.secrets.address.clone());
        let api_key = self.secrets.credential.api_key().to_string();
        self.spawn_task("query_order", async move {
            match http_client.get_order_optional(&venue_order_id).await {
                Ok(Some(order)) => {
                    let requested_venue_order_id = VenueOrderId::from(venue_order_id.as_str());
                    if !open_order_matches_request(
                        &order,
                        requested_venue_order_id,
                        &expected_asset_id,
                    ) {
                        log::error!(
                            "Rejecting query response for {venue_order_id}: returned order or asset identity does not match the request",
                        );
                        return Ok(());
                    }
                    let mut report = parse_order_status_report(
                        &order,
                        instrument_id,
                        account_id,
                        Some(client_order_id),
                        price_prec,
                        size_prec,
                        clock.get_time_ns(),
                    );
                    let venue_filled = report.filled_qty;
                    let Some(prepared) = prepare_order_report(
                        &local_orders,
                        report,
                        "query order response",
                    ) else {
                        return Ok(());
                    };
                    report = prepared.report;
                    let venue_order_id = requested_venue_order_id;
                    let local_filled = prepared.local_filled;
                    let confirmed_filled = if venue_filled > local_filled {
                        let ctx = FillContext {
                            account_id,
                            user_address: &user_address,
                            api_key: &api_key,
                            pusd: get_pusd_currency(),
                            clock,
                        };

                        match fetch_confirmed_fill_reports(
                            &http_client,
                            &ctx,
                            &token_instruments,
                            &local_orders,
                            GetTradesParams::default(),
                            Some(instrument_id),
                            clock.get_time_ns(),
                        )
                        .await
                        {
                            Ok(fills) => confirmed_filled_quantities(&fills)
                                .get(&(venue_order_id, instrument_id))
                                .copied(),
                            Err(e) => {
                                log::warn!(
                                    "Failed to fetch confirmed fills for order {venue_order_id}: {e}"
                                );
                                None
                            }
                        }
                    } else {
                        None
                    };
                    if let Some(report) = admit_point_of_use_order_report(
                        &local_orders,
                        report,
                        venue_filled,
                        confirmed_filled,
                        "final query order report",
                    )
                    {
                        emitter.send_order_status_report(report);
                    }
                }
                Ok(None) => {
                    log::warn!("Order {venue_order_id} not found (empty response)");
                }
                Err(e) => {
                    log::warn!("Failed to query order {venue_order_id}: {e}");
                }
            }
            Ok(())
        });
    }

    pub(super) async fn generate_order_status_report_impl(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let venue_order_id = match cmd.venue_order_id {
            Some(id) => id,
            None => {
                log::warn!("generate_order_status_report requires venue_order_id");
                return Ok(None);
            }
        };

        let instrument_id = match cmd.instrument_id {
            Some(id) => id,
            None => {
                log::warn!("generate_order_status_report requires instrument_id");
                return Ok(None);
            }
        };

        let instrument = self.core.cache().instrument(&instrument_id).cloned();
        let (price_prec, size_prec, expected_asset_id) = match &instrument {
            Some(i) => (
                i.price_precision(),
                i.size_precision(),
                i.raw_symbol().to_string(),
            ),
            None => {
                log::warn!(
                    "Cannot generate report for {venue_order_id}: instrument {instrument_id} is unavailable"
                );
                return Ok(None);
            }
        };

        let order = self
            .http_client
            .get_order_optional(venue_order_id.as_str())
            .await
            .context("failed to fetch order")?;

        if let Some(order) = order {
            if !open_order_matches_request(&order, venue_order_id, &expected_asset_id) {
                log::error!(
                    "Rejecting order response for {venue_order_id}: returned order or asset identity does not match the request",
                );
                return Ok(None);
            }
            let mut report = parse_order_status_report(
                &order,
                instrument_id,
                self.core.account_id,
                cmd.client_order_id,
                price_prec,
                size_prec,
                self.clock.get_time_ns(),
            );
            let venue_filled = report.filled_qty;
            let Some(prepared) =
                prepare_order_report(&self.local_orders, report, "single order response")
            else {
                return Ok(None);
            };
            report = prepared.report;
            let local_filled = prepared.local_filled;
            let confirmed_filled = if venue_filled > local_filled {
                match fetch_confirmed_fill_reports(
                    &self.http_client,
                    &self.fill_context(),
                    &self.shared_token_instruments,
                    &self.local_orders,
                    GetTradesParams::default(),
                    Some(instrument_id),
                    self.clock.get_time_ns(),
                )
                .await
                {
                    Ok(fills) => confirmed_filled_quantities(&fills)
                        .get(&(venue_order_id, instrument_id))
                        .copied(),
                    Err(e) => {
                        log::warn!(
                            "Failed to fetch confirmed fills for order {venue_order_id}: {e}"
                        );
                        None
                    }
                }
            } else {
                None
            };
            return Ok(admit_point_of_use_order_report(
                &self.local_orders,
                report,
                venue_filled,
                confirmed_filled,
                "final single order report",
            ));
        }

        self.recover_terminal_status_from_trades(
            venue_order_id,
            instrument_id,
            cmd.client_order_id,
            size_prec,
        )
        .await
    }

    pub(super) async fn generate_order_status_reports_impl(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let params = crate::http::query::GetOrdersParams::default();
        let orders = self
            .http_client
            .get_orders(params)
            .await
            .context("failed to fetch orders")?;

        let (reports, _) = super::reconciliation::build_order_reports_from_orders(
            &orders,
            &self.shared_token_instruments,
            self.core.account_id,
            cmd.instrument_id,
            self.clock.get_time_ns(),
        );

        let mut reports_with_venue_filled = Vec::with_capacity(reports.len());
        for report in reports {
            let venue_filled = report.filled_qty;
            if let Some(prepared) =
                prepare_order_report(&self.local_orders, report, "bulk order response")
            {
                reports_with_venue_filled.push((prepared, venue_filled));
            }
        }

        let needs_confirmed_fills = reports_with_venue_filled
            .iter()
            .any(|(prepared, venue_filled)| *venue_filled > prepared.local_filled);
        let confirmed_fills = if needs_confirmed_fills {
            match fetch_confirmed_fill_reports(
                &self.http_client,
                &self.fill_context(),
                &self.shared_token_instruments,
                &self.local_orders,
                GetTradesParams::default(),
                cmd.instrument_id,
                self.clock.get_time_ns(),
            )
            .await
            {
                Ok(fills) => confirmed_filled_quantities(&fills),
                Err(e) => {
                    log::warn!("Failed to fetch confirmed fills for open-order check: {e}");
                    Default::default()
                }
            }
        } else {
            Default::default()
        };

        let mut reports = Vec::with_capacity(reports_with_venue_filled.len());
        for (prepared, venue_filled) in reports_with_venue_filled {
            let report = prepared.report;
            let report_key = (report.venue_order_id, report.instrument_id);
            if let Some(report) = admit_point_of_use_order_report(
                &self.local_orders,
                report,
                venue_filled,
                confirmed_fills.get(&report_key).copied(),
                "final bulk order report",
            ) {
                reports.push(report);
            }
        }

        let reports = if cmd.open_only {
            reports
                .into_iter()
                .filter(|r| r.order_status.is_open())
                .collect()
        } else {
            reports
        };

        log::debug!("Generated {} order status reports", reports.len());
        Ok(reports)
    }

    pub(super) async fn generate_fill_reports_impl(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let trades = self
            .http_client
            .get_trades(GetTradesParams::default())
            .await
            .context("failed to fetch trades")?;

        let ctx = self.fill_context();
        let (reports, _) = build_fill_reports_from_trades(
            &trades,
            &ctx,
            &self.shared_token_instruments,
            cmd.instrument_id,
            self.clock.get_time_ns(),
        );

        let admitted = self.local_orders.admit_reconciliation_fill_reports(reports);
        if admitted.conflicts > 0 {
            log::warn!(
                "Rejected {} fill reports with conflicting local order identity",
                admitted.conflicts
            );
        }

        let reports =
            apply_fill_filters(admitted.artifacts, cmd.venue_order_id, cmd.start, cmd.end);

        log::debug!("Generated {} fill reports", reports.len());
        Ok(reports)
    }

    pub(super) async fn generate_position_status_reports_impl(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let ctx = self.fill_context();
        let positions = self
            .data_api_client
            .get_positions(ctx.user_address)
            .await
            .context("failed to fetch positions from Data API")?;

        let ts_now = self.clock.get_time_ns();
        let mut reports = build_position_reports(&positions, self.core.account_id, ts_now);

        if let Some(ref filter_id) = cmd.instrument_id {
            reports.retain(|r| &r.instrument_id == filter_id);
        }

        log::debug!("Generated {} position status reports", reports.len());
        Ok(reports)
    }

    pub(super) async fn generate_mass_status_impl(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ctx = self.fill_context();
        super::reconciliation::generate_mass_status(
            &self.http_client,
            &self.data_api_client,
            &self.shared_token_instruments,
            &self.local_orders,
            &ctx,
            self.core.client_id,
            self.core.venue,
            lookback_mins,
        )
        .await
    }
}

fn recovered_terminal_order_status(
    time_in_force: TimeInForce,
    quantity: Quantity,
    filled_qty: Quantity,
) -> OrderStatus {
    if time_in_force == TimeInForce::Ioc && filled_qty < quantity {
        return OrderStatus::Canceled;
    }

    let dust_diff = (quantity.as_decimal() - filled_qty.as_decimal()).abs();
    if filled_qty >= quantity || dust_diff < DUST_SNAP_THRESHOLD_DEC {
        OrderStatus::Filled
    } else {
        OrderStatus::Canceled
    }
}

async fn fetch_confirmed_fill_reports(
    http_client: &PolymarketClobHttpClient,
    ctx: &FillContext<'_>,
    token_instruments: &AtomicMap<Ustr, InstrumentAny>,
    local_orders: &LocalOrderCoordinator,
    params: GetTradesParams,
    instrument_id: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<FillReport>> {
    let trades = http_client
        .get_trades(params)
        .await
        .context("failed to fetch confirmed trades")?;
    let (reports, _) =
        build_fill_reports_from_trades(&trades, ctx, token_instruments, instrument_id, ts_init);
    let admitted = local_orders.admit_reconciliation_fill_reports(reports);
    if admitted.conflicts > 0 {
        log::warn!(
            "Rejected {} confirmed fills with conflicting local order identity",
            admitted.conflicts
        );
    }
    Ok(admitted.artifacts)
}

fn admit_point_of_use_order_report(
    local_orders: &LocalOrderCoordinator,
    report: OrderStatusReport,
    venue_filled: Quantity,
    confirmed_filled: Option<Decimal>,
    context: &str,
) -> Option<OrderStatusReport> {
    match local_orders.admit_point_of_use_order_report(report, venue_filled, confirmed_filled) {
        ArtifactAdmission::Owned { artifact, .. } | ArtifactAdmission::Untracked(artifact) => {
            Some(artifact)
        }
        ArtifactAdmission::Conflict(_) => {
            log::warn!("Rejecting {context}: local order identity conflict");
            None
        }
    }
}

fn owned_order_report(
    admission: ArtifactAdmission<OrderStatusReport>,
    context: &str,
) -> Option<OrderStatusReport> {
    match admission {
        ArtifactAdmission::Owned { artifact, .. } => Some(artifact),
        ArtifactAdmission::Untracked(_) | ArtifactAdmission::Conflict(_) => {
            log::warn!("Rejecting {context}: local order identity or lifecycle changed");
            None
        }
    }
}

struct PreparedOrderReport {
    report: OrderStatusReport,
    local_filled: Quantity,
}

fn prepare_order_report(
    local_orders: &LocalOrderCoordinator,
    report: OrderStatusReport,
    context: &str,
) -> Option<PreparedOrderReport> {
    match local_orders.admit_order_report(report) {
        ArtifactAdmission::Owned { artifact, .. } => Some(PreparedOrderReport {
            local_filled: artifact.filled_qty,
            report: artifact,
        }),
        ArtifactAdmission::Untracked(mut artifact) => {
            let local_filled = Quantity::zero(artifact.filled_qty.precision);
            artifact.filled_qty = local_filled;
            Some(PreparedOrderReport {
                report: artifact,
                local_filled,
            })
        }
        ArtifactAdmission::Conflict(_) => {
            log::warn!("Rejected {context} with conflicting local order identity");
            None
        }
    }
}

pub(crate) fn get_pusd_currency() -> Currency {
    Currency::pUSD()
}

pub(super) async fn fetch_and_emit_account_state(
    http_client: &PolymarketClobHttpClient,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    signature_type: SignatureType,
) -> anyhow::Result<()> {
    let params = GetBalanceAllowanceParams {
        asset_type: Some(crate::http::query::AssetType::Collateral),
        signature_type: Some(signature_type),
        ..Default::default()
    };

    let balance_allowance = http_client
        .get_balance_allowance(params)
        .await
        .context("failed to fetch balance allowance")?;

    let pusd = get_pusd_currency();
    let account_balance = parse_balance_allowance(balance_allowance.balance, pusd)
        .context("failed to parse balance allowance")?;

    let ts_event = clock.get_time_ns();
    log::debug!(
        "Account state updated: balance={} pUSD",
        account_balance.total
    );
    emitter.emit_account_state(vec![account_balance], vec![], true, ts_event);
    Ok(())
}

pub(super) async fn fetch_collateral_balance_pusd(
    http_client: &PolymarketClobHttpClient,
    signature_type: SignatureType,
) -> anyhow::Result<Decimal> {
    let params = GetBalanceAllowanceParams {
        asset_type: Some(crate::http::query::AssetType::Collateral),
        signature_type: Some(signature_type),
        ..Default::default()
    };

    let balance_allowance = http_client
        .get_balance_allowance(params)
        .await
        .context("failed to fetch balance allowance")?;

    let usdc_scale = Decimal::from(1_000_000u32);
    Ok(balance_allowance.balance / usdc_scale)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::ioc_dust(TimeInForce::Ioc, "5.202910", "5.202897", OrderStatus::Canceled)]
    #[case::ioc_partial(TimeInForce::Ioc, "30", "20", OrderStatus::Canceled)]
    #[case::fok_dust(TimeInForce::Fok, "5.202910", "5.202897", OrderStatus::Filled)]
    #[case::gtc_dust(TimeInForce::Gtc, "5.202910", "5.202897", OrderStatus::Filled)]
    fn test_recovered_terminal_order_status(
        #[case] time_in_force: TimeInForce,
        #[case] quantity: &str,
        #[case] filled_qty: &str,
        #[case] expected: OrderStatus,
    ) {
        assert_eq!(
            recovered_terminal_order_status(
                time_in_force,
                Quantity::from(quantity),
                Quantity::from(filled_qty),
            ),
            expected
        );
    }
}
