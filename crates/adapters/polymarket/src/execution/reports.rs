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
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    PolymarketExecutionClient,
    identity::{OrderIdentityConflict, OrderIdentityRegistry, OrderReportIdentity},
    order_fill_tracker::OrderFillTrackerMap,
    parse::{
        parse_balance_allowance, parse_order_status_report, sum_filled_quantity,
        weighted_average_price,
    },
    reconciliation::{
        FillContext, FillReportQuery, build_identity_admitted_fill_reports_from_trades,
        build_position_reports, cap_order_report_filled_qty, confirmed_filled_quantities,
        normalize_terminal_order_report_quantity,
    },
};
use crate::{
    common::{consts::DUST_SNAP_THRESHOLD_DEC, enums::SignatureType},
    http::{
        clob::PolymarketClobHttpClient,
        query::{GetBalanceAllowanceParams, GetTradesParams},
    },
};

/// The precisions to build a venue order's report with.
///
/// Returns `None` when the answer belongs to a different asset or neither the
/// requested nor answered asset can be resolved.
fn order_report_precisions(
    instrument_id: InstrumentId,
    asset_id: &str,
    requested: Option<&InstrumentAny>,
    answered: Option<&InstrumentAny>,
) -> Option<(u8, u8)> {
    match (requested, answered) {
        (Some(requested), _) if requested.raw_symbol().as_str() == asset_id => {
            Some((requested.price_precision(), requested.size_precision()))
        }
        (None, Some(answered)) if answered.id() == instrument_id => {
            Some((answered.price_precision(), answered.size_precision()))
        }
        _ => None,
    }
}

fn client_order_ids_match_request(
    requested_client_order_id: Option<ClientOrderId>,
    cached_client_order_id: Option<ClientOrderId>,
    indexed_client_order_id: Option<ClientOrderId>,
) -> bool {
    let known = [
        requested_client_order_id,
        cached_client_order_id,
        indexed_client_order_id,
    ];
    known
        .iter()
        .flatten()
        .next()
        .is_none_or(|expected| known.iter().flatten().all(|known| known == expected))
}

fn venue_answer_matches_request(answered_order_id: &str, venue_order_id: VenueOrderId) -> bool {
    VenueOrderId::from(answered_order_id) == venue_order_id
}

/// Whether every available local identity agrees with an order-scoped request.
///
/// Registry identity is checked once by `resolve_local_order_request`;
/// this pure check covers the cached object and NT indexes.
fn cached_order_matches_request(
    cached: &OrderAny,
    cache_venue_order_id: Option<VenueOrderId>,
    requested_client_order_id: Option<ClientOrderId>,
    indexed_client_order_id: Option<ClientOrderId>,
    instrument_id: InstrumentId,
    venue_order_id: VenueOrderId,
) -> bool {
    if cached.instrument_id() != instrument_id
        || !client_order_ids_match_request(
            requested_client_order_id,
            Some(cached.client_order_id()),
            indexed_client_order_id,
        )
    {
        return false;
    }

    let known_venue_order_ids = [cached.venue_order_id(), cache_venue_order_id];

    known_venue_order_ids
        .iter()
        .flatten()
        .all(|known| *known == venue_order_id)
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

    /// Resolves the client identity only when every local index agrees.
    fn resolve_local_order_request(
        &self,
        requested_client_order_id: Option<ClientOrderId>,
        indexed_client_order_id: Option<ClientOrderId>,
        instrument_id: InstrumentId,
        venue_order_id: VenueOrderId,
    ) -> Result<Option<ClientOrderId>, OrderIdentityConflict> {
        let client_order_id = self.order_identities.resolve_order_request(
            venue_order_id,
            instrument_id,
            requested_client_order_id,
            indexed_client_order_id,
        )?;

        if client_order_id.is_some_and(|client_order_id| {
            self.core
                .cache()
                .venue_order_id(&client_order_id)
                .is_some_and(|known| *known != venue_order_id)
        }) {
            return Err(OrderIdentityConflict);
        }

        Ok(client_order_id)
    }

    /// Returns cached fill evidence for an admitted report.
    ///
    /// An absent cache entry is valid for an external order. A registered identity
    /// contradiction is not absence: callers must drop the report before joining any
    /// venue-order-keyed fill evidence.
    fn cached_filled_for_report(
        &self,
        report: &OrderStatusReport,
    ) -> Result<Option<Quantity>, OrderIdentityConflict> {
        self.order_identities
            .resolve_order_status_report(report, &self.fill_tracker)?;
        let indexed_client_order_id = self
            .core
            .cache()
            .client_order_id(&report.venue_order_id)
            .copied();
        let client_order_id = self.resolve_local_order_request(
            report.client_order_id,
            indexed_client_order_id,
            report.instrument_id,
            report.venue_order_id,
        )?;
        let Some(client_order_id) = client_order_id else {
            return Ok(None);
        };
        let cache = self.core.cache();
        let Some(cached) = cache.order(&client_order_id) else {
            return Ok(None);
        };
        let cache_venue_order_id = cache.venue_order_id(&client_order_id).copied();

        if !cached_order_matches_request(
            &cached,
            cache_venue_order_id,
            report.client_order_id,
            indexed_client_order_id,
            report.instrument_id,
            report.venue_order_id,
        ) || cached.order_side() != report.order_side
            || cached.order_type() != report.order_type
            || cached.time_in_force() != report.time_in_force
        {
            log::error!(
                "Cached order {client_order_id} does not match venue report order {} and \
                 instrument {}; rejecting the report",
                report.venue_order_id,
                report.instrument_id,
            );
            return Err(OrderIdentityConflict);
        }

        Ok(Some(cached.filled_qty()))
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

        let indexed_client_order_id = self.core.cache().client_order_id(&venue_order_id).copied();

        if self
            .resolve_local_order_request(
                client_order_id,
                indexed_client_order_id,
                instrument_id,
                venue_order_id,
            )
            .is_err()
        {
            log::error!(
                "Requested order identity does not match local indexes for venue order \
                 {venue_order_id}; reporting nothing"
            );
            return Ok(None);
        }
        let resolved_client_order_id = client_order_id.or(indexed_client_order_id);
        let cached = resolved_client_order_id.and_then(|cid| self.core.cache().order_owned(&cid));
        if let Some(cached) = &cached {
            let cached_client_order_id = cached.client_order_id();
            let cache_venue_order_id = self
                .core
                .cache()
                .venue_order_id(&cached_client_order_id)
                .copied();

            if !cached_order_matches_request(
                cached,
                cache_venue_order_id,
                client_order_id,
                indexed_client_order_id,
                instrument_id,
                venue_order_id,
            ) {
                log::error!(
                    "Cached order {cached_client_order_id} does not match requested venue order \
                     {venue_order_id} and instrument {instrument_id}; reporting nothing"
                );
                return Ok(None);
            }
        }
        let cached_quantity = cached.as_ref().map(Order::quantity);
        let cached_order_type = cached.as_ref().map_or(OrderType::Limit, Order::order_type);
        let cached_tif = cached
            .as_ref()
            .map_or(TimeInForce::Gtc, Order::time_in_force);
        let cached_price = cached.as_ref().and_then(Order::price);
        let cached_side = cached.as_ref().map(Order::order_side);

        let pending_trades = trades.iter().filter(|trade| {
            trade.status.is_pending_settlement()
                && (trade.taker_order_id == venue_order_id.as_str()
                    || trade
                        .maker_orders
                        .iter()
                        .any(|order| order.order_id == venue_order_id.as_str()))
        });
        let mut has_pending_trade = false;
        for trade in pending_trades {
            has_pending_trade = true;
            let asset_id = if trade.taker_order_id == venue_order_id.as_str() {
                trade.asset_id
            } else {
                Ustr::from(
                    trade
                        .maker_orders
                        .iter()
                        .find(|order| order.order_id == venue_order_id.as_str())
                        .expect("matching maker order was established above")
                        .asset_id
                        .as_str(),
                )
            };
            if self
                .shared_token_instruments
                .get_cloned(&asset_id)
                .is_none_or(|instrument| instrument.id() != instrument_id)
            {
                log::error!(
                    "Pending trade for venue order {venue_order_id} contradicts requested instrument {instrument_id}; deferring terminal recovery"
                );
                return Ok(None);
            }
        }

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
            if self
                .order_identities
                .resolve_order_status_report(&report, &self.fill_tracker)
                .is_err()
            {
                log::error!(
                    "Recovered order {venue_order_id} contradicts current identity; reporting nothing"
                );
                return Ok(None);
            }
            return Ok(Some(report));
        }

        let (order_fills, discards) = build_identity_admitted_fill_reports_from_trades(
            &trades,
            &ctx,
            &self.shared_token_instruments,
            &self.order_identities,
            &self.fill_tracker,
            FillReportQuery {
                instrument_filter: Some(instrument_id),
                venue_order_filter: Some(venue_order_id),
                start: None,
                end: None,
                ts_init,
            },
        );

        discards.report(log::Level::Debug, "Polymarket order status report");

        if discards.identity_conflicts > 0 {
            log::error!(
                "Trade identity for venue order {venue_order_id} is contradictory; deferring terminal recovery"
            );
            return Ok(None);
        }

        if let Some(cached) = cached.as_ref()
            && order_fills.iter().any(|fill| {
                fill.instrument_id != cached.instrument_id()
                    || fill.order_side != cached.order_side()
                    || fill
                        .client_order_id
                        .is_some_and(|client| client != cached.client_order_id())
            })
        {
            log::error!(
                "Confirmed fills for venue order {venue_order_id} contradict the complete cached order identity; deferring terminal recovery"
            );
            return Ok(None);
        }

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
            if self
                .order_identities
                .resolve_order_status_report(&report, &self.fill_tracker)
                .is_err()
            {
                log::error!(
                    "Recovered order {venue_order_id} contradicts current identity; reporting nothing"
                );
                return Ok(None);
            }
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
            .unwrap_or_else(|_| Quantity::zero(size_prec));
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

        if self
            .order_identities
            .resolve_order_status_report(&report, &self.fill_tracker)
            .is_err()
        {
            log::error!(
                "Recovered terminal order {venue_order_id} contradicts current identity; reporting nothing"
            );
            return Ok(None);
        }

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

        // Resolved after the venue answers, not here: the answer names the asset,
        // and the precisions depend on which side resolved. Only the size
        // precision is needed before then, for the cached filled quantity.
        let requested_instrument = cache.instrument(&instrument_id).cloned();
        let size_prec = requested_instrument
            .as_ref()
            .map_or(6, |instrument| instrument.size_precision());

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
        let requested_venue_order_id = VenueOrderId::from(venue_order_id.as_str());
        let indexed_client_order_id = cache.client_order_id(&requested_venue_order_id).copied();

        if self
            .resolve_local_order_request(
                Some(client_order_id),
                indexed_client_order_id,
                instrument_id,
                requested_venue_order_id,
            )
            .is_err()
        {
            log::error!(
                "Requested order {client_order_id} does not match local indexes for venue order \
                 {requested_venue_order_id}; reporting nothing"
            );
            return;
        }
        let (cached_filled, cached_identity) = match cache.order(&client_order_id) {
            Some(order) => {
                let cache_venue_order_id = cache.venue_order_id(&client_order_id).copied();

                if !cached_order_matches_request(
                    &order,
                    cache_venue_order_id,
                    Some(client_order_id),
                    indexed_client_order_id,
                    instrument_id,
                    requested_venue_order_id,
                ) {
                    log::error!(
                        "Cached order {client_order_id} does not match requested venue order \
                         {requested_venue_order_id} and instrument {instrument_id}; reporting \
                         nothing"
                    );
                    return;
                }
                (
                    order.filled_qty(),
                    Some(OrderReportIdentity::from_order(&order)),
                )
            }
            None => (Quantity::zero(size_prec), None),
        };
        let order_identities = self.order_identities.clone();

        self.spawn_task("query_order", async move {
            match http_client.get_order_optional(&venue_order_id).await {
                Ok(Some(order)) => {
                    if !venue_answer_matches_request(
                        order.id.as_str(),
                        requested_venue_order_id,
                    ) {
                        log::error!(
                            "Polymarket answered query for {requested_venue_order_id} with order {}; \
                             reporting nothing",
                            order.id,
                        );
                        return Ok(());
                    }
                    let answered = match &requested_instrument {
                        Some(_) => None,
                        None => token_instruments.get_cloned(&Ustr::from(order.asset_id.as_str())),
                    };
                    let Some((price_prec, size_prec)) = order_report_precisions(
                        instrument_id,
                        order.asset_id.as_str(),
                        requested_instrument.as_ref(),
                        answered.as_ref(),
                    ) else {
                        log::error!(
                            "Cannot validate Polymarket order {venue_order_id} asset {} against \
                             requested instrument {instrument_id}; reporting nothing",
                            order.asset_id,
                        );
                        return Ok(());
                    };
                    let mut report = parse_order_status_report(
                        &order,
                        instrument_id,
                        account_id,
                        Some(client_order_id),
                        price_prec,
                        size_prec,
                        clock.get_time_ns(),
                    );
                    if cached_identity
                        .is_some_and(|identity| identity != OrderReportIdentity::from_report(&report))
                    {
                        log::error!(
                            "Cached order {client_order_id} contradicts the complete venue report identity for {requested_venue_order_id}; reporting nothing"
                        );
                        return Ok(());
                    }
                    let tracked_filled = match fill_tracker.cumulative_filled_for_report(&report) {
                        Ok(filled) => filled.unwrap_or_else(|| Quantity::zero(size_prec)),
                        Err(_) => {
                            log::error!(
                                "Tracker identity changed while querying venue order {requested_venue_order_id}; reporting nothing"
                            );
                            return Ok(());
                        }
                    };
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
                            &order_identities,
                            &fill_tracker,
                            ConfirmedFillQuery {
                                params: GetTradesParams::default(),
                                instrument_id: Some(instrument_id),
                                ts_init: clock.get_time_ns(),
                            },
                        )
                        .await
                        {
                            Ok(fills) => confirmed_filled_quantities(&fills)
                                .get(&(
                                    requested_venue_order_id,
                                    instrument_id,
                                    report.order_side,
                                ))
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
                    cap_order_report_filled_qty(
                        &mut report,
                        local_filled,
                        confirmed_filled,
                    );

                    if order_identities
                        .resolve_order_status_report(&report, &fill_tracker)
                        .is_err()
                    {
                        log::error!(
                            "Requested order {client_order_id} changed local identity while \
                             querying venue order {requested_venue_order_id}; reporting nothing"
                        );
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
        let indexed_client_order_id = self.core.cache().client_order_id(&venue_order_id).copied();

        if self
            .resolve_local_order_request(
                cmd.client_order_id,
                indexed_client_order_id,
                instrument_id,
                venue_order_id,
            )
            .is_err()
        {
            log::error!(
                "Requested order identity does not match local indexes for venue order \
                 {venue_order_id}; reporting nothing"
            );
            return Ok(None);
        }

        let order = self
            .http_client
            .get_order_optional(venue_order_id.as_str())
            .await
            .context("failed to fetch order")?;

        let instrument = self.core.cache().instrument(&instrument_id).cloned();
        // One definition of "the precisions to build a report with", including
        // what they are when nothing is known, so the two paths below cannot
        // drift on the defaults.
        let precisions = |known: Option<&InstrumentAny>| match known {
            Some(instrument) => (instrument.price_precision(), instrument.size_precision()),
            None => (4, 6),
        };

        if let Some(order) = order {
            if !venue_answer_matches_request(order.id.as_str(), venue_order_id) {
                log::error!(
                    "Polymarket answered query for {venue_order_id} with order {}; reporting nothing",
                    order.id,
                );
                return Ok(None);
            }
            let answered = match &instrument {
                Some(_) => None,
                None => self
                    .shared_token_instruments
                    .get_cloned(&Ustr::from(order.asset_id.as_str())),
            };

            let Some((price_prec, size_prec)) = order_report_precisions(
                instrument_id,
                order.asset_id.as_str(),
                instrument.as_ref(),
                answered.as_ref(),
            ) else {
                log::error!(
                    "Cannot validate Polymarket order {venue_order_id} asset {} against requested \
                     instrument {instrument_id}; reporting nothing",
                    order.asset_id,
                );
                return Ok(None);
            };

            let mut report = parse_order_status_report(
                &order,
                instrument_id,
                self.core.account_id,
                cmd.client_order_id,
                price_prec,
                size_prec,
                self.clock.get_time_ns(),
            );
            let cached_filled = {
                let cache = self.core.cache();
                let cached = cmd
                    .client_order_id
                    .and_then(|id| cache.order(&id))
                    .or_else(|| {
                        cache
                            .client_order_id(&venue_order_id)
                            .and_then(|id| cache.order(id))
                    });

                if let Some(ref cached) = cached {
                    let client_order_id = cached.client_order_id();
                    let cache_venue_order_id = cache.venue_order_id(&client_order_id).copied();

                    if !cached_order_matches_request(
                        cached,
                        cache_venue_order_id,
                        cmd.client_order_id,
                        indexed_client_order_id,
                        instrument_id,
                        venue_order_id,
                    ) {
                        log::error!(
                            "Cached order {client_order_id} does not match requested venue order \
                             {venue_order_id} and instrument {instrument_id}; reporting nothing"
                        );
                        return Ok(None);
                    }
                    if OrderReportIdentity::from_order(cached)
                        != OrderReportIdentity::from_report(&report)
                    {
                        log::error!(
                            "Cached order {client_order_id} contradicts the complete venue report identity for {venue_order_id}; reporting nothing"
                        );
                        return Ok(None);
                    }
                }
                cached.map_or_else(|| Quantity::zero(size_prec), |order| order.filled_qty())
            };
            let tracked_filled = match self.fill_tracker.cumulative_filled_for_report(&report) {
                Ok(filled) => filled.unwrap_or_else(|| Quantity::zero(size_prec)),
                Err(_) => {
                    log::error!(
                        "Tracker identity contradicts venue order {venue_order_id}; reporting nothing"
                    );
                    return Ok(None);
                }
            };
            let local_filled = cached_filled.max(tracked_filled);
            let confirmed_filled = if report.filled_qty > local_filled {
                match fetch_confirmed_fill_reports(
                    &self.http_client,
                    &self.fill_context(),
                    &self.shared_token_instruments,
                    &self.order_identities,
                    &self.fill_tracker,
                    ConfirmedFillQuery {
                        params: GetTradesParams::default(),
                        instrument_id: Some(instrument_id),
                        ts_init: self.clock.get_time_ns(),
                    },
                )
                .await
                {
                    Ok(fills) => confirmed_filled_quantities(&fills)
                        .get(&(venue_order_id, instrument_id, report.order_side))
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
            cap_order_report_filled_qty(&mut report, local_filled, confirmed_filled);
            if self
                .order_identities
                .resolve_order_status_report(&report, &self.fill_tracker)
                .is_err()
            {
                log::error!(
                    "Requested order identity changed while querying venue order \
                     {venue_order_id}; reporting nothing"
                );
                return Ok(None);
            }
            return Ok(Some(report));
        }

        self.recover_terminal_status_from_trades(
            venue_order_id,
            instrument_id,
            cmd.client_order_id,
            precisions(instrument.as_ref()).1,
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

        let (mut reports, _) = super::reconciliation::build_order_reports_from_orders(
            &orders,
            &self.shared_token_instruments,
            self.core.account_id,
            cmd.instrument_id,
            self.clock.get_time_ns(),
        );

        let mut needs_confirmed_fills = false;
        reports.retain(|report| match self.cached_filled_for_report(report) {
            Ok(cached_filled) => {
                let cached_filled =
                    cached_filled.unwrap_or_else(|| Quantity::zero(report.quantity.precision));
                needs_confirmed_fills |= report.filled_qty > cached_filled;
                true
            }
            Err(_) => {
                log::error!(
                    "Registered identity contradicts venue report order {} and instrument {}; \
                         dropping the report",
                    report.venue_order_id,
                    report.instrument_id,
                );
                false
            }
        });
        let confirmed_fills = if needs_confirmed_fills {
            match fetch_confirmed_fill_reports(
                &self.http_client,
                &self.fill_context(),
                &self.shared_token_instruments,
                &self.order_identities,
                &self.fill_tracker,
                ConfirmedFillQuery {
                    params: GetTradesParams::default(),
                    instrument_id: cmd.instrument_id,
                    ts_init: self.clock.get_time_ns(),
                },
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

        reports.retain_mut(|report| {
            let cached_filled = match self.cached_filled_for_report(report) {
                Ok(cached_filled) => {
                    cached_filled.unwrap_or_else(|| Quantity::zero(report.quantity.precision))
                }
                Err(_) => {
                    log::error!(
                        "Registered identity changed before venue report order {} and instrument \
                         {} were reconciled; dropping the report",
                        report.venue_order_id,
                        report.instrument_id,
                    );
                    return false;
                }
            };
            let tracked_filled = match self.fill_tracker.cumulative_filled_for_report(report) {
                Ok(filled) => {
                    filled.unwrap_or_else(|| Quantity::zero(report.quantity.precision))
                }
                Err(_) => {
                    log::error!(
                        "Tracker identity contradicts venue report order {} and instrument {}; dropping the report",
                        report.venue_order_id,
                        report.instrument_id,
                    );
                    return false;
                }
            };
            cap_order_report_filled_qty(
                report,
                cached_filled.max(tracked_filled),
                confirmed_fills
                    .get(&(
                        report.venue_order_id,
                        report.instrument_id,
                        report.order_side,
                    ))
                    .copied(),
            );
            true
        });

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
        let (reports, discards) = build_identity_admitted_fill_reports_from_trades(
            &trades,
            &ctx,
            &self.shared_token_instruments,
            &self.order_identities,
            &self.fill_tracker,
            FillReportQuery {
                instrument_filter: cmd.instrument_id,
                venue_order_filter: cmd.venue_order_id,
                start: cmd.start,
                end: cmd.end,
                ts_init: self.clock.get_time_ns(),
            },
        );

        // A bounded query that answered with trades it could not place in time
        // has weakened the caller's window, which is worth more than a line the
        // operator only sees with debug logging enabled.
        discards.report(log::Level::Warn, "Polymarket fill reports");

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
            &self.fill_tracker,
            &self.order_identities,
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
    order_identities: &OrderIdentityRegistry,
    fill_tracker: &OrderFillTrackerMap,
    query: ConfirmedFillQuery,
) -> anyhow::Result<Vec<FillReport>> {
    let ConfirmedFillQuery {
        params,
        instrument_id,
        ts_init,
    } = query;
    let trades = http_client
        .get_trades(params)
        .await
        .context("failed to fetch confirmed trades")?;
    let (reports, discards) = build_identity_admitted_fill_reports_from_trades(
        &trades,
        ctx,
        token_instruments,
        order_identities,
        fill_tracker,
        FillReportQuery {
            instrument_filter: instrument_id,
            venue_order_filter: None,
            start: None,
            end: None,
            ts_init,
        },
    );
    // Debug, not error. This runs on the open-order poll, so a permanent
    // condition -- one historical trade the account cannot interpret -- would
    // otherwise reprint at error level every few seconds forever. The pass that
    // owns the operator-visible severity is `generate_mass_status`, which sees
    // the same trade once.
    discards.report(log::Level::Debug, "Polymarket fill fetch");
    Ok(reports)
}

struct ConfirmedFillQuery {
    params: GetTradesParams,
    instrument_id: Option<InstrumentId>,
    ts_init: UnixNanos,
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
