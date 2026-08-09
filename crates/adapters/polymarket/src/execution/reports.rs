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
    enums::{OrderStatus, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    PolymarketExecutionClient,
    parse::{
        ReportParseError, parse_balance_allowance, parse_order_status_report, parse_timestamp,
        sum_filled_quantity, weighted_average_price,
    },
    reconciliation::{
        FillContext, ReconciliationOmission, apply_fill_filters, build_fill_reports_from_trades,
        build_position_reports, cap_order_report_filled_qty, confirmed_filled_quantities,
        log_reconciliation_summary, normalize_terminal_order_report_quantity,
    },
};
use crate::{
    common::{consts::DUST_SNAP_THRESHOLD_DEC, enums::SignatureType},
    http::{
        clob::PolymarketClobHttpClient,
        models::{DataApiPosition, PolymarketTradeReport},
        query::{GetBalanceAllowanceParams, GetTradesParams},
    },
};

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
        let order_trades = scope_fill_trades(
            &trades,
            &self.shared_token_instruments,
            Some(instrument_id),
            std::slice::from_ref(&venue_order_id),
            None,
            None,
        )?;

        let resolved_client_order_id =
            client_order_id.or_else(|| self.core.cache().client_order_id(&venue_order_id).copied());
        let cached = resolved_client_order_id.and_then(|cid| self.core.cache().order_owned(&cid));
        let cached_quantity = cached.as_ref().map(Order::quantity);
        let cached_order_type = cached.as_ref().map_or(OrderType::Limit, Order::order_type);
        let cached_tif = cached
            .as_ref()
            .map_or(TimeInForce::Gtc, Order::time_in_force);
        let cached_price = cached.as_ref().and_then(Order::price);
        let cached_side = cached.as_ref().map(Order::order_side);

        let has_pending_trade = order_trades
            .iter()
            .any(|trade| trade.status.is_pending_settlement());

        if has_pending_trade {
            let Some(cached) = cached.as_ref() else {
                log::debug!(
                    "Order {venue_order_id} has unsettled trades but no cached order; deferring recovery"
                );
                return Ok(None);
            };
            let order_status = if cached.filled_qty().is_zero() {
                OrderStatus::Accepted
            } else {
                OrderStatus::PartiallyFilled
            };
            let mut report = OrderStatusReport::new(
                self.core.account_id,
                instrument_id,
                resolved_client_order_id,
                venue_order_id,
                cached.order_side(),
                cached.order_type(),
                cached.time_in_force(),
                order_status,
                cached.quantity(),
                cached.filled_qty(),
                ts_init,
                ts_init,
                ts_init,
                None,
            );
            report.price = cached_price;

            log::debug!(
                "Order {venue_order_id} has unsettled trades; reporting non-terminal {order_status}"
            );
            return Ok(Some(report));
        }

        let output = build_fill_reports_from_trades(
            &order_trades,
            &ctx,
            &self.shared_token_instruments,
            Some(instrument_id),
            ts_init,
        );
        output
            .omissions
            .ensure_authoritative(&format!("Order recovery for {venue_order_id}"))?;
        let mut order_fills = output.reports;
        self.fill_tracker.snap_fill_reports(&mut order_fills);

        if order_fills.is_empty() {
            let Some(cached) = cached.as_ref() else {
                log::debug!(
                    "Order {venue_order_id} not active at venue, no trades found, and no cached order; nothing to recover"
                );
                return Ok(None);
            };
            log::debug!(
                "Order {venue_order_id} not active at venue and no trades found; recovering as Canceled"
            );
            let mut report = OrderStatusReport::new(
                self.core.account_id,
                instrument_id,
                resolved_client_order_id,
                venue_order_id,
                cached.order_side(),
                cached.order_type(),
                cached.time_in_force(),
                OrderStatus::Canceled,
                cached.quantity(),
                cached.filled_qty(),
                ts_init,
                ts_init,
                ts_init,
                None,
            );
            report.price = cached_price;
            report.cancel_reason = Some("ORDER_NOT_FOUND_AT_VENUE".to_string());
            return Ok(Some(report));
        }

        let Some(quantity) = cached_quantity else {
            log::debug!(
                "Order {venue_order_id} has trades but no cached order; deferring to engine"
            );
            return Ok(None);
        };

        let total_filled_dec = sum_filled_quantity(&order_fills);
        let avg_px = weighted_average_price(&order_fills, total_filled_dec);
        let raw_filled_qty = Quantity::from_decimal_dp(total_filled_dec, size_prec)
            .map_err(|_| ReportParseError::FilledQuantity)?;
        let order_side = cached_side.unwrap_or(order_fills[0].order_side);
        let ts_event = order_fills
            .iter()
            .map(|f| f.ts_event)
            .max()
            .unwrap_or(ts_init);

        let order_status = recovered_terminal_order_status(cached_tif, quantity, raw_filled_qty);
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
            cached_order_type,
            cached_tif,
            order_status,
            quantity,
            filled_qty,
            ts_event,
            ts_event,
            ts_init,
            None,
        );
        report.price = cached_price;
        report.avg_px = avg_px;
        normalize_terminal_order_report_quantity(&mut report);

        Ok(Some(report))
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

        let (price_prec, size_prec) = match cache.instrument(&instrument_id) {
            Some(i) => (i.price_precision(), i.size_precision()),
            None => (4, 6),
        };

        let http_client = self.http_client.clone();
        let fill_tracker = self.fill_tracker.clone();
        let token_instruments = self.shared_token_instruments.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let user_address = self
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| self.secrets.address.clone());
        let api_key = self.secrets.credential.api_key().to_string();
        let cached_filled = cache
            .order(&client_order_id)
            .map_or_else(|| Quantity::zero(size_prec), |order| order.filled_qty());

        self.spawn_task("query_order", async move {
            match http_client.get_order_optional(&venue_order_id).await {
                Ok(Some(order)) => {
                    let mut report = match parse_order_status_report(
                        &order,
                        instrument_id,
                        account_id,
                        Some(client_order_id),
                        price_prec,
                        size_prec,
                        clock.get_time_ns(),
                    ) {
                        Ok(report) => report,
                        Err(e) => {
                            log::warn!("Skipping invalid order report {venue_order_id}: {e}");
                            return Ok(());
                        }
                    };
                    let venue_order_id = VenueOrderId::from(venue_order_id.as_str());
                    let tracked_filled = fill_tracker
                        .get_cumulative_filled(&venue_order_id)
                        .unwrap_or_else(|| Quantity::zero(size_prec));
                    let local_filled = cached_filled.max(tracked_filled);
                    let confirmed_filled = if report.filled_qty > local_filled {
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
                            std::slice::from_ref(&venue_order_id),
                            Some(instrument_id),
                            clock.get_time_ns(),
                            &format!("Order query for {venue_order_id}"),
                        )
                        .await
                        {
                            Ok(fills) => match confirmed_filled_quantities(&fills) {
                                Ok(quantities) => quantities.get(&venue_order_id).copied(),
                                Err(e) => {
                                    log::warn!(
                                        "Conflicting confirmed fills for order {venue_order_id}: {e}"
                                    );
                                    return Ok(());
                                }
                            },
                            Err(e) => {
                                log::warn!(
                                    "Failed to fetch confirmed fills for order {venue_order_id}: {e}"
                                );
                                return Ok(());
                            }
                        }
                    } else {
                        None
                    };

                    if let Err(e) = cap_order_report_filled_qty(
                        &mut report,
                        local_filled,
                        confirmed_filled,
                    ) {
                        log::warn!("Skipping invalid order report {venue_order_id}: {e}");
                        return Ok(());
                    }
                    emitter.send_order_status_report(report);
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

        let order = self
            .http_client
            .get_order_optional(venue_order_id.as_str())
            .await
            .context("failed to fetch order")?;

        let instrument = self.core.cache().instrument(&instrument_id).cloned();
        let (price_prec, size_prec) = match &instrument {
            Some(i) => (i.price_precision(), i.size_precision()),
            None => (4, 6),
        };

        if let Some(order) = order {
            let mut report = match parse_order_status_report(
                &order,
                instrument_id,
                self.core.account_id,
                cmd.client_order_id,
                price_prec,
                size_prec,
                self.clock.get_time_ns(),
            ) {
                Ok(report) => report,
                Err(e) => {
                    anyhow::bail!("Invalid order report {venue_order_id}: {e}");
                }
            };
            let cached_filled = cmd
                .client_order_id
                .and_then(|id| self.core.cache().order(&id).map(|order| order.filled_qty()))
                .or_else(|| {
                    self.core
                        .cache()
                        .client_order_id(&venue_order_id)
                        .and_then(|id| self.core.cache().order(id).map(|order| order.filled_qty()))
                })
                .unwrap_or_else(|| Quantity::zero(size_prec));
            let tracked_filled = self
                .fill_tracker
                .get_cumulative_filled(&venue_order_id)
                .unwrap_or_else(|| Quantity::zero(size_prec));
            let local_filled = cached_filled.max(tracked_filled);
            let confirmed_filled = if report.filled_qty > local_filled {
                match fetch_confirmed_fill_reports(
                    &self.http_client,
                    &self.fill_context(),
                    &self.shared_token_instruments,
                    std::slice::from_ref(&venue_order_id),
                    Some(instrument_id),
                    self.clock.get_time_ns(),
                    &format!("Order status for {venue_order_id}"),
                )
                .await
                {
                    Ok(fills) => confirmed_filled_quantities(&fills)?
                        .get(&venue_order_id)
                        .copied(),
                    Err(e) => {
                        return Err(e).context(format!(
                            "failed to fetch confirmed fills for order {venue_order_id}"
                        ));
                    }
                }
            } else {
                None
            };

            if let Err(e) = cap_order_report_filled_qty(&mut report, local_filled, confirmed_filled)
            {
                anyhow::bail!("Invalid order report {venue_order_id}: {e}");
            }
            return Ok(Some(report));
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

        let scoped_orders = scope_order_rows(
            &orders,
            &self.shared_token_instruments,
            cmd.instrument_id,
            cmd.open_only,
            cmd.start,
            cmd.end,
        )?;
        let mut output = super::reconciliation::build_order_reports_from_orders(
            scoped_orders,
            &self.shared_token_instruments,
            self.core.account_id,
            cmd.instrument_id,
            self.clock.get_time_ns(),
        );

        let needs_confirmed_fills = output.reports.iter().any(|report| {
            let cached_filled = report
                .client_order_id
                .and_then(|id| self.core.cache().order(&id).map(|order| order.filled_qty()))
                .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
            report.filled_qty > cached_filled
        });
        let confirmed_fills = if needs_confirmed_fills {
            let venue_order_ids = output
                .reports
                .iter()
                .map(|report| report.venue_order_id)
                .collect::<Vec<_>>();
            match fetch_confirmed_fill_reports(
                &self.http_client,
                &self.fill_context(),
                &self.shared_token_instruments,
                &venue_order_ids,
                cmd.instrument_id,
                self.clock.get_time_ns(),
                "Order reports",
            )
            .await
            {
                Ok(fills) => confirmed_filled_quantities(&fills)?,
                Err(e) => {
                    return Err(e).context("failed to fetch confirmed fills for open-order check");
                }
            }
        } else {
            Default::default()
        };

        let before_normalization = output.reports.len();
        output.reports.retain_mut(|report| {
            let cached_filled = report
                .client_order_id
                .and_then(|id| self.core.cache().order(&id).map(|order| order.filled_qty()))
                .or_else(|| {
                    self.core
                        .cache()
                        .client_order_id(&report.venue_order_id)
                        .and_then(|id| self.core.cache().order(id).map(|order| order.filled_qty()))
                })
                .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
            let tracked_filled = self
                .fill_tracker
                .get_cumulative_filled(&report.venue_order_id)
                .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
            cap_order_report_filled_qty(
                report,
                cached_filled.max(tracked_filled),
                confirmed_fills.get(&report.venue_order_id).copied(),
            )
            .is_ok()
        });
        output.omissions.record_n(
            ReconciliationOmission::InvalidOrder(super::parse::ReportParseError::FilledQuantity),
            before_normalization - output.reports.len(),
        );

        let reports = if cmd.open_only {
            output
                .reports
                .into_iter()
                .filter(|r| r.order_status.is_open())
                .collect()
        } else {
            output.reports
        };

        log_reconciliation_summary("order reports", reports.len(), 0, 0, &output.omissions);
        output.omissions.ensure_authoritative("Order reports")?;
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

        let scoped_trades = scope_fill_trades(
            &trades,
            &self.shared_token_instruments,
            cmd.instrument_id,
            cmd.venue_order_id.as_slice(),
            cmd.start,
            cmd.end,
        )?;
        let ctx = self.fill_context();
        let mut output = build_fill_reports_from_trades(
            &scoped_trades,
            &ctx,
            &self.shared_token_instruments,
            cmd.instrument_id,
            self.clock.get_time_ns(),
        );

        self.fill_tracker.snap_fill_reports(&mut output.reports);

        let reports = apply_fill_filters(output.reports, cmd.venue_order_id, cmd.start, cmd.end);

        log_reconciliation_summary("fill reports", 0, reports.len(), 0, &output.omissions);
        output.omissions.ensure_authoritative("Fill reports")?;
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
        let scoped_positions = scope_position_rows(
            &positions,
            &self.shared_token_instruments,
            cmd.instrument_id,
        )?;
        let output = build_position_reports(
            scoped_positions,
            &self.shared_token_instruments,
            self.core.account_id,
            ts_now,
        );

        log_reconciliation_summary(
            "position reports",
            0,
            0,
            output.reports.len(),
            &output.omissions,
        );
        output.omissions.ensure_authoritative("Position reports")?;
        Ok(output.reports)
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
            &self.fill_tracker,
            &ctx,
            self.core.client_id,
            self.core.venue,
            lookback_mins,
        )
        .await
    }
}

fn scope_fill_trades(
    trades: &[PolymarketTradeReport],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    venue_order_ids: &[VenueOrderId],
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<PolymarketTradeReport>> {
    let asset_filter = resolve_asset_filter(instruments, instrument_filter, "fill")?;

    Ok(trades
        .iter()
        .filter_map(|trade| {
            let in_time_range = parse_timestamp(&trade.match_time).is_none_or(|timestamp| {
                start.is_none_or(|start| timestamp >= start)
                    && end.is_none_or(|end| timestamp <= end)
            });
            if !in_time_range {
                return None;
            }

            let mut scoped = trade.clone();
            if trade.trader_side == crate::common::enums::PolymarketLiquiditySide::Maker {
                scoped.maker_orders.retain(|order| {
                    (venue_order_ids.is_empty()
                        || venue_order_ids
                            .iter()
                            .any(|venue_order_id| order.order_id == venue_order_id.as_str()))
                        && asset_filter.is_none_or(|asset| order.asset_id == asset.as_str())
                });
                (!scoped.maker_orders.is_empty()).then_some(scoped)
            } else {
                let order_matches = venue_order_ids.is_empty()
                    || venue_order_ids
                        .iter()
                        .any(|venue_order_id| trade.taker_order_id == venue_order_id.as_str());
                let asset_matches =
                    asset_filter.is_none_or(|asset| trade.asset_id == asset.as_str());
                (order_matches && asset_matches).then_some(scoped)
            }
        })
        .collect())
}

fn scope_position_rows<'a>(
    positions: &'a [DataApiPosition],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
) -> anyhow::Result<Vec<&'a DataApiPosition>> {
    let asset = resolve_asset_filter(instruments, instrument_filter, "position")?;
    Ok(positions
        .iter()
        .filter(|position| asset.is_none_or(|asset| position.asset == asset.as_str()))
        .collect())
}

fn resolve_asset_filter(
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    report_kind: &str,
) -> anyhow::Result<Option<Ustr>> {
    let Some(instrument_id) = instrument_filter else {
        return Ok(None);
    };
    instruments
        .load()
        .iter()
        .find_map(|(asset, instrument)| (instrument.id() == instrument_id).then_some(*asset))
        .map(Some)
        .with_context(|| {
            format!("No Polymarket token mapping for {report_kind} instrument {instrument_id}")
        })
}

fn scope_order_rows<'a>(
    orders: &'a [crate::http::models::PolymarketOpenOrder],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_filter: Option<InstrumentId>,
    open_only: bool,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<&'a crate::http::models::PolymarketOpenOrder>> {
    let asset = resolve_asset_filter(instruments, instrument_filter, "order")?;
    Ok(orders
        .iter()
        .filter(|order| asset.is_none_or(|asset| order.asset_id == asset))
        .filter(|order| !open_only || OrderStatus::from(order.status).is_open())
        .filter(|order| {
            order
                .created_at
                .checked_mul(1_000_000_000)
                .map(UnixNanos::from)
                .is_none_or(|timestamp| {
                    start.is_none_or(|start| timestamp >= start)
                        && end.is_none_or(|end| timestamp <= end)
                })
        })
        .collect())
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
    venue_order_ids: &[VenueOrderId],
    instrument_id: Option<InstrumentId>,
    ts_init: UnixNanos,
    authority_context: &str,
) -> anyhow::Result<Vec<FillReport>> {
    let trades = http_client
        .get_trades(GetTradesParams::default())
        .await
        .context("failed to fetch confirmed trades")?;
    let relevant_trades = scope_fill_trades(
        &trades,
        token_instruments,
        instrument_id,
        venue_order_ids,
        None,
        None,
    )?;
    let output = build_fill_reports_from_trades(
        &relevant_trades,
        ctx,
        token_instruments,
        instrument_id,
        ts_init,
    );
    output.omissions.ensure_authoritative(authority_context)?;
    Ok(output.reports)
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
    use nautilus_model::instruments::stubs::binary_option;
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;

    fn test_open_order() -> crate::http::models::PolymarketOpenOrder {
        let content = std::fs::read_to_string("test_data/http_open_order.json")
            .expect("failed to read open-order fixture");
        serde_json::from_str(&content).expect("failed to parse open-order fixture")
    }

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

    #[rstest]
    fn test_scope_position_rows_isolates_requested_instrument_authority() {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let instrument_id = instrument.id();
        let instruments = AtomicMap::new();
        instruments.insert(Ustr::from("TARGET"), instrument);
        let positions = vec![
            DataApiPosition {
                asset: "TARGET".to_string(),
                condition_id: "0xtarget".to_string(),
                size: Decimal::ONE,
                avg_price: Some(Decimal::new(5, 1)),
            },
            DataApiPosition {
                asset: "UNMAPPED".to_string(),
                condition_id: "0xunrelated".to_string(),
                size: Decimal::ONE,
                avg_price: None,
            },
        ];

        let scoped = scope_position_rows(&positions, &instruments, Some(instrument_id)).unwrap();

        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].asset, "TARGET");
    }

    #[rstest]
    fn test_scope_position_rows_rejects_unmapped_requested_instrument() {
        let positions = Vec::new();
        let instruments = AtomicMap::new();

        let result = scope_position_rows(
            &positions,
            &instruments,
            Some(InstrumentId::from("UNKNOWN.POLYMARKET")),
        );

        assert!(result.is_err());
    }

    #[rstest]
    fn test_scope_order_rows_isolates_requested_instrument_before_validation() {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let instrument_id = instrument.id();
        let instruments = AtomicMap::new();
        let mut target = test_open_order();
        target.asset_id = Ustr::from("TARGET");
        let mut unrelated = target.clone();
        unrelated.asset_id = Ustr::from("UNMAPPED");
        unrelated.price = Decimal::ZERO;
        instruments.insert(target.asset_id, instrument);
        let orders = vec![target, unrelated];

        let scoped = scope_order_rows(
            &orders,
            &instruments,
            Some(instrument_id),
            false,
            None,
            None,
        )
        .unwrap();

        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].asset_id, Ustr::from("TARGET"));
    }

    #[rstest]
    fn test_scope_order_rows_applies_time_and_open_contract_before_validation() {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let instrument_id = instrument.id();
        let instruments = AtomicMap::new();
        let mut old = test_open_order();
        old.asset_id = Ustr::from("TARGET");
        old.created_at = 1;
        old.price = Decimal::ZERO;
        let mut terminal = old.clone();
        terminal.created_at = 3;
        terminal.status = crate::common::enums::PolymarketOrderStatus::Matched;
        instruments.insert(old.asset_id, instrument);
        let orders = vec![old, terminal];

        let scoped = scope_order_rows(
            &orders,
            &instruments,
            Some(instrument_id),
            true,
            Some(UnixNanos::from(2_000_000_000u64)),
            None,
        )
        .unwrap();

        assert!(scoped.is_empty());
    }
}
