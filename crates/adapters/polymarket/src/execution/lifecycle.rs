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

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use indexmap::IndexMap;
use nautilus_common::{
    live::{runner::get_exec_event_sender, runtime::get_runtime},
    msgbus::{self, TypedHandler},
};
use nautilus_core::{MUTEX_POISONED, collections::AtomicMap, time::AtomicTime};
use nautilus_model::{
    enums::{OrderStatus, TimeInForce},
    events::{OrderEventAny, OrderFillVoided, OrderFilled, PositionEvent},
    identifiers::{InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use super::PolymarketExecutionClient;
use crate::{
    common::enums::{PolymarketLiquiditySide, PolymarketTradeStatus},
    execution::{
        identity::OrderIdentityRegistry,
        order_fill_tracker::{
            FillFingerprint, OrderFillTrackerMap, RestoredOrder, TradeCorrectionIdentity,
        },
        reconciliation::build_fill_reports_from_trades,
        reports::{fetch_and_emit_account_state, pending_trade_matches_known_order},
    },
    http::{clob::HeartbeatResponse, error::Error as HttpError, models::PolymarketTradeReport},
    websocket::{
        dispatch::{WsDispatchContext, dispatch_user_message, finalize_cached_confirmed_fill},
        messages::PolymarketWsMessage,
    },
};

const SUPPORTED_CLOB_VERSION: u8 = 2;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const HEARTBEAT_SAFETY_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_HEALTH_MARGIN: Duration = Duration::from_secs(1);
const HEARTBEAT_REQUEST_FAILURE_LIMIT: u32 = 2;

impl PolymarketExecutionClient {
    fn start_heartbeat_task(&mut self) {
        if !self.config.heartbeat_enabled {
            return;
        }

        if self
            .heartbeat_task
            .as_ref()
            .is_some_and(|task| !task.handle.is_finished())
        {
            return;
        }

        if let Some(completed) = self.heartbeat_task.take() {
            completed.handle.abort();
        }

        self.heartbeat_healthy.store(false, Ordering::Release);
        let cancellation = CancellationToken::new();

        let handle = get_runtime().spawn(run_heartbeats(
            self.http_client.clone(),
            cancellation.clone(),
            Arc::clone(&self.heartbeat_healthy),
        ));
        self.heartbeat_task = Some(super::HeartbeatTask {
            cancellation,
            handle,
        });
    }

    fn abort_heartbeat_task(&mut self) {
        if let Some(task) = self.heartbeat_task.take() {
            task.cancellation.cancel();
            task.handle.abort();
        }
    }

    async fn stop_heartbeat_task(&mut self) {
        if let Some(task) = self.heartbeat_task.take() {
            task.cancellation.cancel();
            if let Err(e) = task.handle.await
                && !e.is_cancelled()
            {
                log::warn!("Heartbeat task failed to join during disconnect: {e}");
            }
        }
    }

    fn ensure_order_event_subscription(&mut self) {
        if self.order_event_handler.is_some() {
            return;
        }

        let core = self.core.clone();
        let clock = self.clock;
        let shared_token_instruments = self.shared_token_instruments.clone();
        let neg_risk_index = self.neg_risk_index.clone();
        let fill_tracker = self.fill_tracker.clone();
        let order_identities = self.order_identities.clone();
        let pending_submits = self.pending_submits.clone();
        let pending_cancels = self.pending_cancels.clone();
        let ws_dispatch_state = self.ws_dispatch_state.clone();
        let emitter = self.emitter.clone();
        let user_address = self
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| self.secrets.address.clone());
        let user_api_key = self.secrets.credential.api_key().to_string();
        let handler = TypedHandler::from(move |event: &OrderEventAny| {
            if !is_terminal_order_event(event) || event.instrument_id().venue != core.venue {
                return;
            }

            if let OrderEventAny::Filled(fill) = event {
                let correction_key = TradeCorrectionIdentity::from_info(fill.info.as_ref());
                let mut state = ws_dispatch_state.lock().expect(MUTEX_POISONED);
                let confirmed = state.record_cached_fill(fill);

                match confirmed {
                    Ok(true) => {
                        if let Some(correction_key) = correction_key {
                            fill_tracker.compact_confirmed_correction(&correction_key);
                        }
                        let ctx = WsDispatchContext {
                            token_instruments: &shared_token_instruments,
                            fill_tracker: &fill_tracker,
                            pending_submits: &pending_submits,
                            order_identities: &order_identities,
                            emitter: &emitter,
                            account_id: core.account_id,
                            clock,
                            user_address: &user_address,
                            user_api_key: &user_api_key,
                        };
                        finalize_cached_confirmed_fill(fill, &ctx, &mut state);
                    }
                    Ok(false) => {}
                    Err(e) => log::error!("Cannot retain cached Polymarket fill correction: {e}"),
                }
                drop(state);
            }

            sync_execution_lookup_for_instrument(
                &core,
                clock,
                &shared_token_instruments,
                &neg_risk_index,
                &fill_tracker,
                &order_identities,
                event.instrument_id(),
            );

            let client_order_id = event.client_order_id();
            let order_state = {
                let cache = core.cache();
                cache.order(&client_order_id).and_then(|order| {
                    order
                        .venue_order_id()
                        .map(|venue_order_id| (venue_order_id, order.is_closed()))
                })
            };
            let Some((venue_order_id, is_closed)) = order_state else {
                return;
            };
            let mut state = ws_dispatch_state.lock().expect(MUTEX_POISONED);
            if !is_closed {
                state.cancel_deferred_order_cleanup(&venue_order_id);
                return;
            }

            let has_provisional = state.has_provisional_for_order(venue_order_id);
            let has_confirmed = state.has_confirmed_for_order(venue_order_id);
            let immediate_order = order_identities
                .get(&venue_order_id)
                .is_some_and(|identity| {
                    matches!(identity.time_in_force, TimeInForce::Fok | TimeInForce::Ioc)
                });

            if has_provisional {
                debug_assert!(fill_tracker.has_operational_order(&venue_order_id));
                state.defer_order_cleanup(venue_order_id);
                return;
            }
            let can_cleanup = match event {
                OrderEventAny::Rejected(_) => true,
                OrderEventAny::Filled(fill) => has_confirmed || fill.info.is_none(),
                OrderEventAny::Updated(_) => has_confirmed,
                OrderEventAny::Canceled(_) => immediate_order && has_confirmed,
                OrderEventAny::FillVoided(_) => true,
                _ => false,
            };

            if !can_cleanup {
                return;
            }

            if !fill_tracker.try_remove_order(&venue_order_id) {
                debug_assert!(fill_tracker.has_operational_order(&venue_order_id));
                state.defer_order_cleanup(venue_order_id);
                return;
            }
            pending_cancels.remove(&client_order_id);
            order_identities.remove(&venue_order_id);
            pending_submits.remove(&venue_order_id);
            state.cleanup_order_buffers(&venue_order_id);
            drop(state);
            sync_execution_lookup_for_instrument(
                &core,
                clock,
                &shared_token_instruments,
                &neg_risk_index,
                &fill_tracker,
                &order_identities,
                event.instrument_id(),
            );
        });

        msgbus::subscribe_order_events("events.order.*".into(), handler.clone(), Some(10));
        self.order_event_handler = Some(handler);
    }

    fn clear_order_event_subscription(&mut self) {
        if let Some(handler) = self.order_event_handler.take() {
            msgbus::unsubscribe_order_events("events.order.*".into(), &handler);
        }
    }

    fn ensure_position_event_subscription(&mut self) {
        if self.position_event_handler.is_some() {
            return;
        }

        let core = self.core.clone();
        let clock = self.clock;
        let shared_token_instruments = self.shared_token_instruments.clone();
        let neg_risk_index = self.neg_risk_index.clone();
        let fill_tracker = self.fill_tracker.clone();
        let order_identities = self.order_identities.clone();
        let handler = TypedHandler::from(move |event: &PositionEvent| {
            if !matches!(event, PositionEvent::PositionClosed(_)) {
                return;
            }

            if event.instrument_id().venue != core.venue {
                return;
            }

            sync_execution_lookup_for_instrument(
                &core,
                clock,
                &shared_token_instruments,
                &neg_risk_index,
                &fill_tracker,
                &order_identities,
                event.instrument_id(),
            );
        });

        msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), Some(10));
        self.position_event_handler = Some(handler);
    }

    fn clear_position_event_subscription(&mut self) {
        if let Some(handler) = self.position_event_handler.take() {
            msgbus::unsubscribe_position_events("events.position.*".into(), &handler);
        }
    }

    pub(super) fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = get_runtime();
        let handle = runtime.spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        });

        self.pending_tasks.push(handle);
    }

    pub(super) fn abort_pending_tasks(&self) {
        self.pending_tasks.abort_all();
    }

    pub(super) async fn await_pending_tasks(&self) {
        loop {
            let tasks = self.pending_tasks.take_all();

            if tasks.is_empty() {
                break;
            }

            for handle in tasks {
                if let Err(e) = handle.await {
                    log::warn!("Pending execution task failed to join during disconnect: {e}");
                }
            }
        }
    }

    pub(super) async fn refresh_account_state(&self) -> anyhow::Result<()> {
        fetch_and_emit_account_state(
            &self.http_client,
            &self.emitter,
            self.clock,
            self.config.signature_type,
        )
        .await
    }

    pub(super) async fn await_account_registered(&self, timeout_secs: f64) -> anyhow::Result<()> {
        let account_id = self.core.account_id;

        if self.core.cache().account(&account_id).is_some() {
            log::info!("Account {account_id} registered");
            return Ok(());
        }

        let start = Instant::now();
        let timeout = Duration::from_secs_f64(timeout_secs);
        let interval = Duration::from_millis(10);

        loop {
            tokio::time::sleep(interval).await;

            if self.core.cache().account(&account_id).is_some() {
                log::info!("Account {account_id} registered");
                return Ok(());
            }

            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "Timeout waiting for account {account_id} to be registered after {timeout_secs}s"
                );
            }
        }
    }

    pub(super) async fn start_ws_stream(&mut self) -> anyhow::Result<()> {
        self.ws_client
            .connect()
            .await
            .context("failed to connect user WebSocket")?;

        self.ws_client
            .subscribe_user()
            .await
            .context("failed to subscribe to user channel")?;

        let mut rx = self
            .ws_client
            .take_message_receiver()
            .ok_or_else(|| anyhow::anyhow!("WebSocket message receiver not available"))?;

        let emitter = self.emitter.clone();
        let token_instruments = self.shared_token_instruments.clone();
        let account_id = self.core.account_id;
        let http_client = self.http_client.clone();
        let clock = self.clock;
        let signature_type = self.config.signature_type;
        let stopping = self.stopping.clone();
        let user_address = self
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| self.secrets.address.clone());
        let user_api_key = self.secrets.credential.api_key().to_string();

        let fill_tracker = self.fill_tracker.clone();
        let pending_submits = self.pending_submits.clone();
        let order_identities = self.order_identities.clone();
        let ws_dispatch_state = self.ws_dispatch_state.clone();

        let handle = get_runtime().spawn(async move {
            let ctx = WsDispatchContext {
                token_instruments: &token_instruments,
                fill_tracker: &fill_tracker,
                pending_submits: &pending_submits,
                order_identities: &order_identities,
                emitter: &emitter,
                account_id,
                clock,
                user_address: &user_address,
                user_api_key: &user_api_key,
            };

            loop {
                match rx.recv().await {
                    Some(PolymarketWsMessage::User(user_msg)) => {
                        let refresh = {
                            let mut state = ws_dispatch_state.lock().expect(MUTEX_POISONED);
                            dispatch_user_message(&user_msg, &ctx, &mut state)
                        };

                        if refresh.is_some() {
                            let http = http_client.clone();
                            let emit = emitter.clone();

                            get_runtime().spawn(async move {
                                match fetch_and_emit_account_state(
                                    &http, &emit, clock, signature_type,
                                )
                                .await
                                {
                                    Ok(()) => log::debug!(
                                        "Account state refreshed after finalized trade for {account_id}"
                                    ),
                                    Err(e) => log::warn!(
                                        "Failed to refresh account after finalized trade: {e}"
                                    ),
                                }
                            });
                        }
                    }
                    Some(PolymarketWsMessage::Market(_)) => {}
                    Some(PolymarketWsMessage::Reconnected) => {
                        log::info!("User WebSocket reconnected");
                        if stopping.load(Ordering::Acquire) {
                            log::debug!("Skipping account refresh because execution client is stopping");
                            continue;
                        }

                        let http = http_client.clone();
                        let emit = emitter.clone();
                        get_runtime().spawn(async move {
                            match fetch_and_emit_account_state(&http, &emit, clock, signature_type)
                                .await
                            {
                                Ok(()) => {
                                    log::info!("Account state refreshed after WebSocket reconnect");
                                }
                                Err(e) => {
                                    log::warn!("Failed to refresh account after reconnect: {e}");
                                }
                            }
                        });
                    }
                    None => {
                        log::debug!("User WebSocket stream ended");
                        break;
                    }
                }
            }

            log::debug!("User WebSocket handler task completed");
        });

        self.ws_stream_handle = Some(handle);
        Ok(())
    }

    pub(super) fn get_neg_risk(&self, instrument_id: &InstrumentId) -> bool {
        self.neg_risk_index
            .get_cloned(instrument_id)
            .unwrap_or(false)
    }

    pub(super) fn get_neg_risk_from_snapshot(
        neg_risk_index: &AHashMap<InstrumentId, bool>,
        instrument_id: &InstrumentId,
    ) -> bool {
        neg_risk_index.get(instrument_id).copied().unwrap_or(false)
    }

    fn upsert_execution_lookup(&self, instrument: &InstrumentAny) {
        upsert_execution_lookup(
            &self.shared_token_instruments,
            &self.neg_risk_index,
            instrument,
        );
    }

    pub(super) fn load_instruments_from_cache(&self) {
        let cache = self.core.cache();
        let instruments: Vec<InstrumentAny> = cache
            .instruments(&self.core.venue, None)
            .into_iter()
            .cloned()
            .collect();

        for inst in &instruments {
            self.upsert_execution_lookup(inst);
        }

        log::debug!("Loaded {} instruments from cache", instruments.len());
    }

    pub(super) fn load_orders_from_cache(&self) -> anyhow::Result<()> {
        // A normal reconnect must retain in-process evidence which NT cache events cannot encode:
        // ambiguous-submit expected IDs and signed quote budgets, pre-registration WS activity,
        // and order/trade associations awaiting correction finality. Explicit client reset owns
        // destructive clearing; this path merges durable cache evidence into retained state.
        let cache = self.core.cache();
        let orders: Vec<_> = cache
            .orders(
                Some(&self.core.venue),
                None,
                None,
                Some(&self.core.account_id),
                None,
            )
            .into_iter()
            .map(|order| order.cloned())
            .collect();
        drop(cache);

        let mut matched_fills: AHashMap<TradeCorrectionIdentity, Vec<OrderFilled>> =
            AHashMap::new();
        let mut voided_trades: AHashMap<TradeCorrectionIdentity, Vec<OrderFillVoided>> =
            AHashMap::new();
        let mut restored_orders = Vec::new();
        let mut restored_identities = Vec::new();

        for order in &orders {
            let Some(venue_order_id) = order.venue_order_id() else {
                continue;
            };

            for event in order.events() {
                match event {
                    OrderEventAny::Filled(fill) => {
                        if let Some(key) = polymarket_trade_key(fill.info.as_ref()) {
                            matched_fills.entry(key).or_default().push(fill.clone());
                        }
                    }
                    OrderEventAny::FillVoided(voided) => {
                        if let Some(key) = polymarket_trade_key(voided.info.as_ref()) {
                            voided_trades.entry(key).or_default().push(voided.clone());
                        }
                    }
                    _ => {}
                }
            }

            if !order.is_open() {
                continue;
            }

            let active_trade_ids = order
                .trade_ids()
                .into_iter()
                .copied()
                .collect::<AHashSet<_>>();
            let active_fills = order
                .events()
                .into_iter()
                .filter_map(|event| match event {
                    OrderEventAny::Filled(fill) if active_trade_ids.contains(&fill.trade_id) => {
                        Some(fill.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let restored_trade_ids = active_fills
                .iter()
                .map(|fill| fill.trade_id)
                .collect::<AHashSet<_>>();
            anyhow::ensure!(
                restored_trade_ids == active_trade_ids,
                "cannot restore open order {venue_order_id}: active trade IDs and cached fill events differ"
            );

            let identity = self
                .order_identities
                .resolve_cached_order_identity(venue_order_id, order)?;
            let authority = self
                .fill_tracker
                .resolve_order_authority(&venue_order_id, Some(order))?;
            restored_orders.push(RestoredOrder {
                venue_order_id,
                original_submitted_qty: authority.restored_original_quantity(order),
                submitted_qty: order.quantity(),
                filled_qty: order.filled_qty(),
                applied_fills: active_fills,
                authority,
            });
            restored_identities.push((venue_order_id, identity));
        }

        let mut state_guard = self.ws_dispatch_state.lock().expect(MUTEX_POISONED);
        let mut restored_state = state_guard.clone();

        for (key, fills) in matched_fills {
            if !voided_trades.contains_key(&key) {
                restored_state
                    .restore_matched_trade(key.clone(), fills)
                    .with_context(|| format!("cannot restore correction {key}"))?;
            }
        }

        for (key, fills) in voided_trades {
            restored_state
                .restore_voided_trade(key.clone(), &fills)
                .with_context(|| format!("cannot restore voided correction {key}"))?;
        }

        self.fill_tracker
            .restore_orders(restored_orders)
            .context("cannot restore open orders")?;
        for (venue_order_id, identity) in restored_identities {
            self.order_identities
                .register_order_identity(venue_order_id, identity);
            self.order_identities.mark_accepted(venue_order_id);
        }
        *state_guard = restored_state;

        log::debug!("Loaded {} order lifecycles from cache", orders.len());
        Ok(())
    }

    async fn hydrate_pending_terminal_orders(&self) -> anyhow::Result<()> {
        let trades = self
            .http_client
            .get_trades(Default::default())
            .await
            .context("failed to fetch pending trades for terminal-order hydration")?;
        let hydrated = self.hydrate_pending_terminal_orders_from(trades)?;
        log::debug!("Hydrated {hydrated} terminal order(s) with pending venue trades");
        Ok(())
    }

    fn hydrate_pending_terminal_orders_from(
        &self,
        trades: Vec<PolymarketTradeReport>,
    ) -> anyhow::Result<usize> {
        let ctx = self.fill_context();
        let pending_trades = trades
            .into_iter()
            .filter(|trade| trade.status.is_pending_settlement())
            .collect::<Vec<_>>();
        let mut pending_resolutions = Vec::with_capacity(pending_trades.len());

        for trade in &pending_trades {
            let mut canonical = trade.clone();
            canonical.status = PolymarketTradeStatus::Confirmed;
            let (reports, discards) = build_fill_reports_from_trades(
                &[canonical],
                &ctx,
                &self.shared_token_instruments,
                None,
                self.clock.get_time_ns(),
                self.config.reconciliation_load_ids(),
            )?;
            discards.ensure_complete("pending terminal-order hydration")?;
            let fingerprints = reports
                .iter()
                .map(FillFingerprint::from_report)
                .collect::<Vec<_>>();
            let key = TradeCorrectionIdentity::new(&trade.id, &trade.taker_order_id);
            pending_resolutions.push((key, fingerprints));
        }

        let mut pending_by_order = AHashMap::<VenueOrderId, Vec<_>>::new();

        for trade in pending_trades {
            if trade.trader_side == PolymarketLiquiditySide::Maker {
                for maker_order in &trade.maker_orders {
                    if maker_order.is_owned_by(ctx.user_address, ctx.api_key) {
                        pending_by_order
                            .entry(VenueOrderId::from(maker_order.order_id.as_str()))
                            .or_default()
                            .push(trade.clone());
                    }
                }
            } else {
                pending_by_order
                    .entry(VenueOrderId::from(trade.taker_order_id.as_str()))
                    .or_default()
                    .push(trade);
            }
        }

        let mut hydrated = 0usize;
        let mut pending_terminal_orders = AHashSet::new();
        let mut restored_orders = Vec::new();
        let mut restored_identities = Vec::new();
        let mut terminal_cancels = Vec::new();

        for (venue_order_id, pending_trades) in pending_by_order {
            let Some((order, instrument)) = ({
                let cache = self.core.cache();
                self.order_identities
                    .get(&venue_order_id)
                    .map(|identity| identity.client_order_id)
                    .or_else(|| cache.client_order_id(&venue_order_id).copied())
                    .and_then(|client_order_id| cache.order(&client_order_id))
                    .filter(|order| {
                        matches!(
                            order.status(),
                            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Filled
                        )
                    })
                    .and_then(|order| {
                        cache
                            .instrument(&order.instrument_id())
                            .cloned()
                            .map(|instrument| (order.cloned(), instrument))
                    })
            }) else {
                continue;
            };

            for trade in &pending_trades {
                anyhow::ensure!(
                    pending_trade_matches_known_order(
                        trade,
                        venue_order_id,
                        &instrument,
                        Some(order.order_side()),
                    )?,
                    "pending trade {} does not bind to terminal order {venue_order_id}",
                    trade.id,
                );
            }
            pending_terminal_orders.insert(venue_order_id);

            if self.fill_tracker.contains(&venue_order_id) {
                continue;
            }

            let active_trade_ids = order
                .trade_ids()
                .into_iter()
                .copied()
                .collect::<AHashSet<_>>();
            let active_fills = order
                .events()
                .into_iter()
                .filter_map(|event| match event {
                    OrderEventAny::Filled(fill) if active_trade_ids.contains(&fill.trade_id) => {
                        Some(fill.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            anyhow::ensure!(
                active_fills
                    .iter()
                    .map(|fill| fill.trade_id)
                    .collect::<AHashSet<_>>()
                    == active_trade_ids,
                "cannot hydrate terminal order {venue_order_id}: active trade IDs and cached fill events differ",
            );

            let authority = self
                .fill_tracker
                .resolve_order_authority(&venue_order_id, Some(&order))?;
            restored_orders.push(RestoredOrder {
                venue_order_id,
                original_submitted_qty: authority.restored_original_quantity(&order),
                submitted_qty: order.quantity(),
                filled_qty: order.filled_qty(),
                applied_fills: active_fills,
                authority,
            });
            restored_identities.push((
                venue_order_id,
                self.order_identities
                    .resolve_cached_order_identity(venue_order_id, &order)?,
            ));

            if order.status() == OrderStatus::Canceled {
                terminal_cancels.push((venue_order_id, order.ts_last()));
            }
            hydrated += 1;
        }

        let mut state_guard = self.ws_dispatch_state.lock().expect(MUTEX_POISONED);
        let mut staged_state = state_guard.clone();
        for (key, fingerprints) in pending_resolutions {
            staged_state.resolve_restored_pending(&key, &fingerprints)?;
        }
        for (venue_order_id, ts_event) in terminal_cancels {
            staged_state.restore_terminal_cancel(venue_order_id, ts_event);
        }
        self.fill_tracker.restore_orders(restored_orders)?;
        for (venue_order_id, identity) in restored_identities {
            self.order_identities
                .register_order_identity(venue_order_id, identity);
            self.order_identities.mark_accepted(venue_order_id);
            debug_assert!(self.fill_tracker.has_operational_order(&venue_order_id));
        }
        *state_guard = staged_state;
        drop(state_guard);

        for venue_order_id in self.fill_tracker.operational_order_ids() {
            if pending_terminal_orders.contains(&venue_order_id) {
                continue;
            }
            let Some((client_order_id, instrument_id)) = ({
                let cache = self.core.cache();
                self.order_identities
                    .get(&venue_order_id)
                    .map(|identity| identity.client_order_id)
                    .or_else(|| cache.client_order_id(&venue_order_id).copied())
                    .and_then(|client_order_id| {
                        cache.order(&client_order_id).and_then(|order| {
                            matches!(order.status(), OrderStatus::Canceled | OrderStatus::Expired)
                                .then_some((client_order_id, order.instrument_id()))
                        })
                    })
            }) else {
                continue;
            };

            let mut state = self.ws_dispatch_state.lock().expect(MUTEX_POISONED);
            if state.has_provisional_for_order(venue_order_id)
                || !self.fill_tracker.try_remove_order(&venue_order_id)
            {
                state.defer_order_cleanup(venue_order_id);
                continue;
            }
            self.pending_cancels.remove(&client_order_id);
            self.order_identities.remove(&venue_order_id);
            self.pending_submits.remove(&venue_order_id);
            state.cleanup_order_buffers(&venue_order_id);
            drop(state);
            sync_execution_lookup_for_instrument(
                &self.core,
                self.clock,
                &self.shared_token_instruments,
                &self.neg_risk_index,
                &self.fill_tracker,
                &self.order_identities,
                instrument_id,
            );
        }

        Ok(hydrated)
    }

    pub(super) fn start_client(&mut self) {
        if self.core.is_started() {
            return;
        }

        self.stopping.store(false, Ordering::Release);
        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();

        log::info!(
            "Started: client_id={}, account_id={}",
            self.core.client_id,
            self.core.account_id,
        );
    }

    pub(super) fn stop_client(&mut self) {
        if self.core.is_stopped() {
            return;
        }

        log::info!("Stopping Polymarket execution client");

        self.stopping.store(true, Ordering::Release);
        self.clear_order_event_subscription();
        self.clear_position_event_subscription();

        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }

        self.abort_heartbeat_task();
        self.ws_client.abort();

        self.core.set_disconnected();
        self.core.set_stopped();

        log::info!("Polymarket execution client stopped");
    }

    pub(super) fn reset_client(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting Polymarket execution client");

        anyhow::ensure!(
            self.pending_tasks.all_finished(),
            "cannot reset Polymarket execution state while submit tasks are still running"
        );

        self.clear_order_event_subscription();
        self.clear_position_event_subscription();
        self.shared_token_instruments.store(AHashMap::new());
        self.neg_risk_index.store(AHashMap::new());
        self.fill_tracker.clear();
        self.order_identities.clear();
        self.pending_submits.clear();
        self.pending_cancels.clear();
        self.ws_dispatch_state.lock().expect(MUTEX_POISONED).clear();
        Ok(())
    }

    pub(super) async fn connect_client(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        log::info!("Connecting Polymarket execution client");

        self.stopping.store(false, Ordering::Release);

        let version = self
            .http_client
            .get_version()
            .await
            .context("failed to query Polymarket CLOB protocol version")?
            .version;

        if version != SUPPORTED_CLOB_VERSION {
            anyhow::bail!(
                "Polymarket CLOB protocol version {version} is unsupported; adapter supports V2 only"
            );
        }

        self.load_instruments_from_cache();
        self.load_orders_from_cache()?;
        self.hydrate_pending_terminal_orders().await?;
        self.core.set_instruments_initialized();

        self.start_ws_stream().await?;
        self.ensure_order_event_subscription();
        self.ensure_position_event_subscription();

        let post_ws = async {
            self.refresh_account_state().await?;
            self.await_account_registered(30.0).await?;
            Ok::<(), anyhow::Error>(())
        };

        if let Err(e) = post_ws.await {
            log::warn!("Connect failed after WS started, tearing down: {e}");
            self.stopping.store(true, Ordering::Release);
            self.clear_order_event_subscription();
            self.clear_position_event_subscription();
            let _ = self.ws_client.disconnect().await;
            self.abort_pending_tasks();
            return Err(e);
        }

        self.core.set_connected();
        self.start_heartbeat_task();

        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    pub(super) async fn disconnect_client(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        log::info!("Disconnecting Polymarket execution client");

        self.stopping.store(true, Ordering::Release);
        self.await_pending_tasks().await;
        self.stop_heartbeat_task().await;
        self.clear_order_event_subscription();
        self.clear_position_event_subscription();

        self.ws_client.disconnect().await?;

        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }

        self.core.set_disconnected();

        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    pub(super) fn on_instrument_update(&self, instrument: &InstrumentAny) {
        self.upsert_execution_lookup(instrument);
    }
}

async fn run_heartbeats(
    http_client: crate::http::clob::PolymarketClobHttpClient,
    cancellation: CancellationToken,
    healthy: Arc<AtomicBool>,
) {
    let heartbeat_health_timeout = HEARTBEAT_SAFETY_TIMEOUT
        .checked_sub(HEARTBEAT_HEALTH_MARGIN)
        .expect("heartbeat health margin should be shorter than the safety timeout");
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat_id = String::new();
    let mut request_failures = 0;
    let mut last_acknowledged = None;

    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            _ = interval.tick() => {}
        }

        let mut resynchronized = false;

        loop {
            let now = tokio::time::Instant::now();
            let request_timeout = now
                .checked_add(HEARTBEAT_REQUEST_TIMEOUT)
                .expect("heartbeat request timeout should fit in an instant");
            let health_deadline = last_acknowledged.map(|acknowledged: tokio::time::Instant| {
                acknowledged
                    .checked_add(heartbeat_health_timeout)
                    .expect("heartbeat health timeout should fit in an instant")
            });

            if health_deadline.is_some_and(|deadline| deadline <= now) {
                log::error!("Polymarket heartbeat health deadline elapsed");
                healthy.store(false, Ordering::Release);
                return;
            }
            let request_deadline =
                health_deadline.map_or(request_timeout, |deadline| deadline.min(request_timeout));
            let response = tokio::select! {
                () = cancellation.cancelled() => return,
                response = tokio::time::timeout_at(
                    request_deadline,
                    http_client.post_heartbeat(&heartbeat_id),
                ) => response.unwrap_or(Err(HttpError::Timeout)),
            };

            match response {
                Ok(HeartbeatResponse::Acknowledged(next_id)) => {
                    let acknowledged = tokio::time::Instant::now();
                    if health_deadline.is_some_and(|deadline| acknowledged >= deadline) {
                        log::error!(
                            "Polymarket heartbeat was acknowledged after the health deadline"
                        );
                        healthy.store(false, Ordering::Release);
                        return;
                    }

                    heartbeat_id = next_id;
                    request_failures = 0;
                    last_acknowledged = Some(acknowledged);
                    healthy.store(true, Ordering::Release);
                    interval.reset_after(HEARTBEAT_INTERVAL);
                    break;
                }
                Ok(HeartbeatResponse::Resynchronize(next_id)) if !resynchronized => {
                    heartbeat_id = next_id;
                    resynchronized = true;
                }
                Ok(HeartbeatResponse::Resynchronize(_)) => {
                    log::error!("Polymarket heartbeat rejected after ID resynchronization");
                    healthy.store(false, Ordering::Release);
                    return;
                }
                Err(e) if e.is_retryable() => {
                    request_failures += 1;
                    if request_failures >= HEARTBEAT_REQUEST_FAILURE_LIMIT {
                        log::error!(
                            "Polymarket heartbeat failed after {request_failures} consecutive request attempts"
                        );
                        healthy.store(false, Ordering::Release);
                        return;
                    }

                    let Some(retry_after) = e.retry_after() else {
                        log::warn!(
                            "Polymarket heartbeat request attempt {request_failures} failed"
                        );
                        continue;
                    };
                    let now = tokio::time::Instant::now();
                    let Some(retry_at) = now.checked_add(retry_after) else {
                        log::error!(
                            "Polymarket heartbeat retry delay exceeded the safety deadline"
                        );
                        healthy.store(false, Ordering::Release);
                        return;
                    };

                    if health_deadline.is_some_and(|deadline| retry_at >= deadline) {
                        log::error!(
                            "Polymarket heartbeat retry delay exceeded the health deadline"
                        );
                        healthy.store(false, Ordering::Release);
                        return;
                    }

                    log::warn!(
                        "Polymarket heartbeat request attempt {request_failures} was rate limited; retrying after {retry_after:?}"
                    );
                    tokio::select! {
                        () = cancellation.cancelled() => return,
                        () = tokio::time::sleep_until(retry_at) => {}
                    }

                    if cancellation.is_cancelled() {
                        return;
                    }
                }
                Err(e) if e.is_auth_error() => {
                    log::error!("Polymarket heartbeat authentication failed");
                    healthy.store(false, Ordering::Release);
                    return;
                }
                Err(_) => {
                    log::error!("Polymarket heartbeat was rejected by the venue");
                    healthy.store(false, Ordering::Release);
                    return;
                }
            }
        }
    }
}

fn polymarket_trade_key(info: Option<&IndexMap<Ustr, Ustr>>) -> Option<TradeCorrectionIdentity> {
    TradeCorrectionIdentity::from_info(info)
}

fn upsert_execution_lookup(
    shared_token_instruments: &AtomicMap<Ustr, InstrumentAny>,
    neg_risk_index: &AtomicMap<InstrumentId, bool>,
    instrument: &InstrumentAny,
) {
    let token_id = Ustr::from(instrument.raw_symbol().as_str());
    shared_token_instruments.insert(token_id, instrument.clone());

    if let InstrumentAny::BinaryOption(bo) = instrument {
        let neg_risk = bo
            .info
            .as_ref()
            .and_then(|i| i.get_bool("neg_risk"))
            .unwrap_or(false);
        neg_risk_index.insert(bo.id, neg_risk);
    }
}

fn remove_execution_lookup(
    shared_token_instruments: &AtomicMap<Ustr, InstrumentAny>,
    neg_risk_index: &AtomicMap<InstrumentId, bool>,
    instrument: &InstrumentAny,
) {
    shared_token_instruments.remove(&Ustr::from(instrument.raw_symbol().as_str()));
    neg_risk_index.remove(&instrument.id());
}

fn sync_execution_lookup_for_instrument(
    core: &nautilus_live::ExecutionClientCore,
    clock: &'static AtomicTime,
    shared_token_instruments: &AtomicMap<Ustr, InstrumentAny>,
    neg_risk_index: &AtomicMap<InstrumentId, bool>,
    fill_tracker: &OrderFillTrackerMap,
    order_identities: &OrderIdentityRegistry,
    instrument_id: InstrumentId,
) {
    let now_ns = clock.get_time_ns();
    let account_id = core.account_id;
    let cache = core.cache();

    let instrument = cache.instrument(&instrument_id).cloned();
    let retain_for_operational_order = order_identities
        .venue_order_ids_for_instrument(&instrument_id)
        .into_iter()
        .any(|venue_order_id| fill_tracker.has_operational_order(&venue_order_id));
    let retain = instrument.as_ref().is_some_and(|instrument| {
        if !crate::filters::is_expired(instrument, now_ns) {
            return true;
        }

        cache.has_orders_open(
            Some(&core.venue),
            Some(&instrument_id),
            None,
            Some(&account_id),
            None,
        ) || cache.has_positions_open(
            Some(&core.venue),
            Some(&instrument_id),
            None,
            Some(&account_id),
            None,
        )
    }) || retain_for_operational_order;

    drop(cache);

    match instrument {
        Some(instrument) if retain => {
            upsert_execution_lookup(shared_token_instruments, neg_risk_index, &instrument);
        }
        Some(instrument) => {
            remove_execution_lookup(shared_token_instruments, neg_risk_index, &instrument);
        }
        // Instrument not in cache: token key cannot be derived, so drop only the neg-risk entry
        None => neg_risk_index.remove(&instrument_id),
    }
}

fn is_terminal_order_event(event: &OrderEventAny) -> bool {
    matches!(
        event,
        OrderEventAny::Canceled(_)
            | OrderEventAny::Expired(_)
            | OrderEventAny::Rejected(_)
            | OrderEventAny::Updated(_)
            | OrderEventAny::Filled(_)
            | OrderEventAny::FillVoided(_)
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use nautilus_common::{
        cache::Cache,
        live::runner::set_exec_event_sender,
        messages::ExecutionEvent,
        msgbus::{publish_order_event, publish_position_event},
    };
    use nautilus_core::{UUID4, UnixNanos, nanos::DurationNanos};
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        enums::{
            AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
            TimeInForce,
        },
        events::{
            OrderEventAny, OrderExpired, OrderUpdated, PositionClosed, PositionEvent,
            order::spec::{OrderFillVoidedSpec, OrderUpdatedSpec},
        },
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Symbol, TradeId,
            TraderId, VenueOrderId,
        },
        instruments::stubs::binary_option,
        orders::{LimitOrder, MarketOrder, Order, OrderAny, stubs::TestOrderEventStubs},
        position::Position,
        types::{Currency, Money, Price, Price as ModelPrice, Quantity, Quantity as ModelQuantity},
    };
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::{
        execution::{identity::OrderIdentity, order_authority::OrderAuthority},
        factories::spawn_rejecting_proxy,
        websocket::messages::{PolymarketUserOrder, PolymarketUserTrade, UserWsMessage},
    };

    const TEST_PRIVATE_KEY: &str =
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    const TEST_API_SECRET_B64: &str = "dGVzdF9zZWNyZXRfa2V5XzMyYnl0ZXNfcGFkMTIzNDU=";
    const TEST_VENUE_ORDER_ID: &str =
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    const SECOND_TEST_VENUE_ORDER_ID: &str =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn test_client() -> (PolymarketExecutionClient, Rc<RefCell<Cache>>) {
        test_client_with_proxy(None)
    }

    fn test_client_with_proxy(
        proxy_url: Option<String>,
    ) -> (PolymarketExecutionClient, Rc<RefCell<Cache>>) {
        test_client_with_proxy_and_http_urls(
            proxy_url,
            "http://127.0.0.1:3000",
            "http://127.0.0.1:3000",
        )
    }

    fn test_client_with_proxy_and_http_urls(
        proxy_url: Option<String>,
        base_url_http: &str,
        base_url_data_api: &str,
    ) -> (PolymarketExecutionClient, Rc<RefCell<Cache>>) {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("POLYMARKET"),
            *crate::common::consts::POLYMARKET_VENUE,
            OmsType::Netting,
            AccountId::from("POLYMARKET-001"),
            AccountType::Cash,
            None,
            cache.clone(),
        );
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        set_exec_event_sender(tx);
        let client = PolymarketExecutionClient::new(
            core,
            crate::config::PolymarketExecClientConfig {
                private_key: Some(TEST_PRIVATE_KEY.to_string()),
                api_key: Some("test_api_key".to_string()),
                api_secret: Some(TEST_API_SECRET_B64.to_string()),
                passphrase: Some("test_pass".to_string()),
                funder: None,
                base_url_http: Some(base_url_http.to_string()),
                base_url_ws: Some("ws://127.0.0.1:3000/ws".to_string()),
                base_url_data_api: Some(base_url_data_api.to_string()),
                proxy_url,
                ..crate::config::PolymarketExecClientConfig::default()
            },
        )
        .expect("test client should construct");

        (client, cache)
    }

    #[rstest]
    #[tokio::test]
    async fn execution_client_propagates_proxy_without_debug_exposure() {
        const USERNAME: &str = "exec-user";
        const SECRET: &str = "exec-client-proxy-secret";
        let (proxy_addr, requests) = spawn_rejecting_proxy(2).await;
        let proxy_url = format!("http://{USERNAME}:{SECRET}@{proxy_addr}");
        let (client, _cache) = test_client_with_proxy_and_http_urls(
            Some(proxy_url.clone()),
            "https://clob-auth.fixture",
            "https://data-auth.fixture",
        );
        let debug = format!("{client:?}");
        let errors = [
            client
                .http_client
                .get_book("auth-token")
                .await
                .unwrap_err()
                .to_string(),
            client
                .data_api_client
                .get_positions("0x0000000000000000000000000000000000000002")
                .await
                .unwrap_err()
                .to_string(),
        ];
        let requests = requests.lock().await;
        let request_lines = requests
            .iter()
            .map(|request| request.lines().next().unwrap().to_string())
            .collect::<Vec<_>>();
        let expected_auth = format!("Basic {}", BASE64.encode(format!("{USERNAME}:{SECRET}")));

        assert_eq!(client.config.proxy_url.as_deref(), Some(proxy_url.as_str()));
        assert_eq!(client.ws_client.proxy_url().unwrap().expose(), proxy_url);
        assert_eq!(
            request_lines,
            [
                "CONNECT clob-auth.fixture:443 HTTP/1.1",
                "CONNECT data-auth.fixture:443 HTTP/1.1",
            ]
        );

        for request in requests.iter() {
            let auth = request
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("proxy-authorization")
                        .then_some(value.trim())
                })
                .expect("Proxy-Authorization header");
            assert_eq!(auth, expected_auth);
        }

        for error in errors {
            assert!(!error.contains(SECRET));
            assert!(!error.contains(&expected_auth));
        }
        assert!(!debug.contains(SECRET));
    }

    fn test_binary_option(raw_symbol: &str, expired: bool, neg_risk: bool) -> InstrumentAny {
        let clock = nautilus_core::time::get_atomic_clock_realtime();
        let mut binary = binary_option();
        binary.id = InstrumentId::from(format!("{raw_symbol}.POLYMARKET").as_str());
        binary.raw_symbol = Symbol::new(raw_symbol);
        binary.currency = Currency::pUSD();
        binary.expiration_ns = if expired {
            UnixNanos::from(clock.get_time_ns().as_u64().saturating_sub(1_000_000_000))
        } else {
            UnixNanos::from(
                clock
                    .get_time_ns()
                    .as_u64()
                    .saturating_add(86_400_000_000_000),
            )
        };

        let mut info = nautilus_core::Params::new();
        info.insert("neg_risk".to_string(), Value::Bool(neg_risk));
        binary.info = Some(info);

        InstrumentAny::BinaryOption(binary)
    }

    fn open_limit_order(instrument_id: InstrumentId) -> OrderAny {
        open_limit_order_with_tif(instrument_id, TimeInForce::Gtc)
    }

    fn open_limit_order_with_tif(
        instrument_id: InstrumentId,
        time_in_force: TimeInForce,
    ) -> OrderAny {
        open_limit_order_with_identity(
            instrument_id,
            time_in_force,
            ClientOrderId::from("O-RETAIN"),
        )
    }

    fn open_limit_order_with_identity(
        instrument_id: InstrumentId,
        time_in_force: TimeInForce,
        client_order_id: ClientOrderId,
    ) -> OrderAny {
        OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            ModelQuantity::new(10.0, 0),
            ModelPrice::from("0.5000"),
            time_in_force,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            nautilus_core::UUID4::new(),
            UnixNanos::default(),
        ))
    }

    fn open_market_order_with_identity(
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
    ) -> OrderAny {
        OrderAny::Market(MarketOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            order_side,
            ModelQuantity::from("10.00"),
            TimeInForce::Ioc,
            UUID4::new(),
            UnixNanos::default(),
            false,
            order_side == OrderSide::Buy,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    fn cache_accepted_open_order(cache: &mut Cache, instrument_id: InstrumentId) -> OrderAny {
        cache_accepted_order(cache, open_limit_order(instrument_id))
    }

    fn cache_accepted_order(cache: &mut Cache, order: OrderAny) -> OrderAny {
        cache_accepted_order_with_venue(cache, order, VenueOrderId::from(TEST_VENUE_ORDER_ID))
    }

    fn cache_accepted_order_with_venue(
        cache: &mut Cache,
        mut order: OrderAny,
        venue_order_id: VenueOrderId,
    ) -> OrderAny {
        cache.add_order(order.clone(), None, None, false).unwrap();

        let submitted = TestOrderEventStubs::submitted(&order, AccountId::from("POLYMARKET-001"));
        order = cache.update_order(&submitted).unwrap();

        let accepted = TestOrderEventStubs::accepted(
            &order,
            AccountId::from("POLYMARKET-001"),
            venue_order_id,
        );
        cache.update_order(&accepted).unwrap()
    }

    #[rstest]
    #[case::open(false)]
    #[case::pending_terminal(true)]
    fn retained_signed_market_authority_survives_cache_merge(#[case] pending_terminal: bool) {
        let (client, cache) = test_client();
        let venue_order_id = VenueOrderId::from(TEST_VENUE_ORDER_ID);
        let mut trade_json: Value =
            serde_json::from_str(include_str!("../../test_data/http_trade_report.json")).unwrap();
        trade_json["status"] = Value::String("MATCHED".to_string());
        trade_json["owner"] = Value::String("test_api_key".to_string());
        trade_json["taker_order_id"] = Value::String(TEST_VENUE_ORDER_ID.to_string());
        trade_json["size"] = Value::String("4.00".to_string());
        let trade: PolymarketTradeReport = serde_json::from_value(trade_json).unwrap();
        let instrument = {
            let mut binary = binary_option();
            binary.id = InstrumentId::from(
                format!("{}-{}.POLYMARKET", trade.market, trade.asset_id).as_str(),
            );
            binary.raw_symbol = Symbol::new(trade.asset_id.as_str());
            binary.currency = Currency::pUSD();
            binary.outcome = Some(Ustr::from("Yes"));
            let mut info = nautilus_core::Params::new();
            info.insert(
                "condition_id".to_string(),
                Value::String(trade.market.to_string()),
            );
            info.insert("fees_enabled".to_string(), Value::Bool(false));
            binary.info = Some(info);
            InstrumentAny::BinaryOption(binary)
        };
        let order = open_market_order_with_identity(
            instrument.id(),
            ClientOrderId::from("O-RETAIN-SIGNED-MARKET"),
            OrderSide::Buy,
        );
        let authority = OrderAuthority::for_test(
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Ioc,
            ModelQuantity::from("10.00"),
            ModelPrice::from("0.5000"),
            None,
        );

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument).unwrap();
            let order = cache_accepted_order_with_venue(&mut cache, order, venue_order_id);
            if pending_terminal {
                let canceled = TestOrderEventStubs::canceled(
                    &order,
                    AccountId::from("POLYMARKET-001"),
                    Some(venue_order_id),
                );
                cache.update_order(&canceled).unwrap();
            }
        }
        client
            .fill_tracker
            .reserve_orders_with_authority(&[(venue_order_id, authority)])
            .unwrap();

        client.load_instruments_from_cache();
        client.load_orders_from_cache().unwrap();
        if pending_terminal {
            assert_eq!(
                client
                    .hydrate_pending_terminal_orders_from(vec![trade])
                    .unwrap(),
                1,
            );
        }

        assert_eq!(
            client.fill_tracker.order_authority(&venue_order_id),
            Some(authority),
        );
        assert!(client.fill_tracker.contains(&venue_order_id));
    }

    #[rstest]
    fn retained_signed_authority_rejects_contradictory_cached_order() {
        let (client, cache) = test_client();
        let venue_order_id = VenueOrderId::from(TEST_VENUE_ORDER_ID);
        let instrument = test_binary_option("0xRETAINED-AUTHORITY", false, false);
        let cached_order = open_market_order_with_identity(
            instrument.id(),
            ClientOrderId::from("O-RETAINED-AUTHORITY"),
            OrderSide::Sell,
        );
        let authority = OrderAuthority::for_test(
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Ioc,
            ModelQuantity::from("10.00"),
            ModelPrice::from("0.5000"),
            None,
        );
        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument).unwrap();
            cache_accepted_order_with_venue(&mut cache, cached_order, venue_order_id);
        }
        client
            .fill_tracker
            .reserve_orders_with_authority(&[(venue_order_id, authority)])
            .unwrap();

        let error = client.load_orders_from_cache().unwrap_err();
        assert!(format!("{error:#}").contains("signed side"), "{error:#}");
    }

    #[rstest]
    fn retained_identity_rejects_contradictory_cached_order() {
        let (client, cache) = test_client();
        let venue_order_id = VenueOrderId::from(TEST_VENUE_ORDER_ID);
        let expected_instrument = test_binary_option("0xEXPECTED-IDENTITY", false, false);
        let cached_instrument = test_binary_option("0xCACHED-IDENTITY", false, false);
        let expected_order = open_market_order_with_identity(
            expected_instrument.id(),
            ClientOrderId::from("O-RETAINED-IDENTITY"),
            OrderSide::Buy,
        );
        let cached_order = open_market_order_with_identity(
            cached_instrument.id(),
            expected_order.client_order_id(),
            OrderSide::Buy,
        );
        let authority = OrderAuthority::for_test(
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Ioc,
            ModelQuantity::from("10.00"),
            ModelPrice::from("0.5000"),
            None,
        );
        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expected_instrument).unwrap();
            cache.add_instrument(cached_instrument).unwrap();
            cache_accepted_order_with_venue(&mut cache, cached_order, venue_order_id);
        }
        client
            .fill_tracker
            .reserve_orders_with_authority(&[(venue_order_id, authority)])
            .unwrap();
        client
            .order_identities
            .reserve_order_identity(venue_order_id, OrderIdentity::from_order(&expected_order));

        let error = client.load_orders_from_cache().unwrap_err();
        assert!(
            format!("{error:#}").contains("tracked instrument"),
            "{error:#}"
        );
    }

    #[rstest]
    fn load_orders_from_cache_is_atomic_on_capacity_failure() {
        let (mut client, cache) = test_client();
        client.fill_tracker = std::sync::Arc::new(OrderFillTrackerMap::with_capacity(1));
        let instrument = test_binary_option("0xRESTORE-ATOMIC", false, false);
        let venue_order_ids = [
            VenueOrderId::from("V-ATOMIC-1"),
            VenueOrderId::from("V-ATOMIC-2"),
        ];

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            for (index, venue_order_id) in venue_order_ids.iter().copied().enumerate() {
                let order = open_limit_order_with_identity(
                    instrument.id(),
                    TimeInForce::Gtc,
                    ClientOrderId::from(format!("O-ATOMIC-{index}")),
                );
                cache_accepted_order_with_venue(&mut cache, order, venue_order_id);
            }
        }

        let error = client.load_orders_from_cache().unwrap_err();
        assert!(
            format!("{error:#}").contains("operational order capacity 1 exhausted"),
            "{error:#}"
        );
        for venue_order_id in venue_order_ids {
            assert!(!client.fill_tracker.contains(&venue_order_id));
            assert!(client.order_identities.get(&venue_order_id).is_none());
        }
    }

    #[rstest]
    fn pending_terminal_hydration_is_atomic_on_capacity_failure() {
        let (mut client, cache) = test_client();
        client.fill_tracker = std::sync::Arc::new(OrderFillTrackerMap::with_capacity(1));
        let venue_order_ids = [
            VenueOrderId::from(TEST_VENUE_ORDER_ID),
            VenueOrderId::from(SECOND_TEST_VENUE_ORDER_ID),
        ];
        let mut trade_json: Value =
            serde_json::from_str(include_str!("../../test_data/http_trade_report.json")).unwrap();
        trade_json["status"] = Value::String("MATCHED".to_string());
        trade_json["owner"] = Value::String("test_api_key".to_string());
        let template: PolymarketTradeReport = serde_json::from_value(trade_json).unwrap();
        let instrument = {
            let mut binary = binary_option();
            binary.id = InstrumentId::from(
                format!("{}-{}.POLYMARKET", template.market, template.asset_id).as_str(),
            );
            binary.raw_symbol = Symbol::new(template.asset_id.as_str());
            binary.currency = Currency::pUSD();
            binary.outcome = Some(Ustr::from("Yes"));
            let mut info = nautilus_core::Params::new();
            info.insert(
                "condition_id".to_string(),
                Value::String(template.market.to_string()),
            );
            info.insert("fees_enabled".to_string(), Value::Bool(false));
            binary.info = Some(info);
            InstrumentAny::BinaryOption(binary)
        };
        let mut trades = Vec::new();

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            for (index, venue_order_id) in venue_order_ids.iter().copied().enumerate() {
                let order = open_limit_order_with_identity(
                    instrument.id(),
                    TimeInForce::Gtc,
                    ClientOrderId::from(format!("O-HYDRATE-ATOMIC-{index}")),
                );
                let order = cache_accepted_order_with_venue(&mut cache, order, venue_order_id);
                let canceled = TestOrderEventStubs::canceled(
                    &order,
                    AccountId::from("POLYMARKET-001"),
                    Some(venue_order_id),
                );
                cache.update_order(&canceled).unwrap();

                let mut trade = template.clone();
                trade.id = format!("trade-hydrate-atomic-{index}");
                trade.taker_order_id = venue_order_id.to_string();
                trades.push(trade);
            }
        }

        client.load_instruments_from_cache();
        client.load_orders_from_cache().unwrap();
        let error = client
            .hydrate_pending_terminal_orders_from(trades)
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("operational order capacity 1 exhausted"),
            "{error:#}"
        );
        for venue_order_id in venue_order_ids {
            assert!(!client.fill_tracker.contains(&venue_order_id));
            assert!(client.order_identities.get(&venue_order_id).is_none());
        }
    }

    fn open_position(instrument: &InstrumentAny) -> Position {
        let order = open_limit_order(instrument.id());
        let filled = match TestOrderEventStubs::filled(
            &order,
            instrument,
            None,
            None,
            Some(ModelPrice::from("0.5000")),
            None,
            None,
            None,
            None,
            Some(AccountId::from("POLYMARKET-001")),
        ) {
            OrderEventAny::Filled(filled) => filled,
            other => panic!("expected filled event, was {other:?}"),
        };

        Position::new(instrument, filled)
    }

    fn closed_position(position: &Position) -> Position {
        let mut closed = position.clone();
        closed.side = PositionSide::Flat;
        closed.signed_qty = 0.0;
        closed.quantity = Quantity::zero(position.size_precision);
        closed.ts_closed = Some(position.ts_last);
        closed.duration_ns = 1;
        closed
    }

    fn position_closed_event(position: &Position) -> PositionEvent {
        PositionEvent::PositionClosed(PositionClosed {
            trader_id: position.trader_id,
            strategy_id: position.strategy_id,
            instrument_id: position.instrument_id,
            position_id: position.id,
            account_id: position.account_id,
            opening_order_id: position.opening_order_id,
            closing_order_id: position.closing_order_id,
            entry: position.entry,
            side: PositionSide::Flat,
            signed_qty: 0.0,
            quantity: Quantity::zero(position.size_precision),
            peak_quantity: position.peak_qty,
            last_qty: Quantity::zero(position.size_precision),
            last_px: Price::zero(position.price_precision),
            currency: position.quote_currency,
            avg_px_open: position.avg_px_open,
            avg_px_close: position.avg_px_close,
            realized_return: position.realized_return,
            realized_pnl: position.realized_pnl,
            unrealized_pnl: Money::zero(position.quote_currency),
            duration: DurationNanos::from(1_u64),
            event_id: UUID4::new(),
            ts_opened: position.ts_opened,
            ts_closed: position.ts_closed.or(Some(position.ts_last)),
            ts_event: position.ts_last,
            ts_init: position.ts_last,
        })
    }

    #[rstest]
    fn load_instruments_from_cache_preloads_expired_execution_lookup_state() {
        let (client, cache) = test_client();
        let active = test_binary_option("0xACTIVE", false, true);
        let expired = test_binary_option("0xEXPIRED", true, true);

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(active.clone()).unwrap();
            cache.add_instrument(expired.clone()).unwrap();
        }

        client.load_instruments_from_cache();

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(active.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&active.id()));
        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn load_orders_from_cache_restores_failed_trade_correction_state() {
        let (client, cache) = test_client();
        let instrument = test_binary_option("0xRESTART", false, false);
        let venue_order_id = VenueOrderId::from(TEST_VENUE_ORDER_ID);

        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            let order = cache_accepted_open_order(&mut cache, instrument.id());
            let mut filled = TestOrderEventStubs::filled(
                &order,
                &instrument,
                None,
                None,
                Some(ModelPrice::from("0.5000")),
                None,
                None,
                None,
                None,
                Some(AccountId::from("POLYMARKET-001")),
            );

            if let OrderEventAny::Filled(ref mut fill) = filled {
                fill.trade_id = TradeId::from("trade-restart");
                fill.info = Some(IndexMap::from([
                    (Ustr::from("id"), Ustr::from("trade-restart")),
                    (
                        Ustr::from("taker_order_id"),
                        Ustr::from(TEST_VENUE_ORDER_ID),
                    ),
                ]));
            }
            let filled = match filled {
                OrderEventAny::Filled(filled) => filled,
                other => panic!("expected filled event, was {other:?}"),
            };
            cache
                .update_order(&OrderEventAny::Filled(filled.clone()))
                .unwrap();
            let voided = OrderFillVoidedSpec::builder()
                .trader_id(filled.trader_id)
                .strategy_id(filled.strategy_id)
                .instrument_id(filled.instrument_id)
                .client_order_id(filled.client_order_id)
                .venue_order_id(filled.venue_order_id)
                .account_id(filled.account_id)
                .trade_id(filled.trade_id)
                .voided_qty(filled.last_qty)
                .maybe_commission_voided(filled.commission)
                .order_side(filled.order_side)
                .order_type(filled.order_type)
                .last_px(filled.last_px)
                .currency(filled.currency)
                .liquidity_side(filled.liquidity_side)
                .maybe_position_id(filled.position_id)
                .maybe_info(filled.info)
                .build();
            cache
                .update_order(&OrderEventAny::FillVoided(voided))
                .unwrap()
        };

        client.load_orders_from_cache().unwrap();

        let key = format!("trade-restart-{TEST_VENUE_ORDER_ID}");
        let state = client.ws_dispatch_state.lock().expect(MUTEX_POISONED);

        assert_eq!(order.status(), OrderStatus::Voided);
        assert!(client.order_identities.get(&venue_order_id).is_none());
        assert!(!client.fill_tracker.contains(&venue_order_id));
        assert_eq!(state.matched_fill_count(&key), 0);
        assert!(state.is_voided_trade(&key));
    }

    #[rstest]
    #[case::canceled(false, false)]
    #[case::expired(true, false)]
    #[case::filled(false, true)]
    fn pending_rest_hydration_resolves_cached_matched_fill_to_provisional(
        #[case] expired: bool,
        #[case] terminal_filled: bool,
    ) {
        let (client, cache) = test_client();
        let fill_size = if terminal_filled { "10.00" } else { "5.00" };
        let mut trade_json: Value =
            serde_json::from_str(include_str!("../../test_data/http_trade_report.json")).unwrap();
        trade_json["id"] = Value::String("trade-hydrate".to_string());
        trade_json["taker_order_id"] = Value::String(TEST_VENUE_ORDER_ID.to_string());
        trade_json["size"] = Value::String(fill_size.to_string());
        trade_json["status"] = Value::String("MATCHED".to_string());
        trade_json["owner"] = Value::String("test_api_key".to_string());
        let trade: PolymarketTradeReport = serde_json::from_value(trade_json).unwrap();

        let instrument = {
            let mut binary = binary_option();
            binary.id = InstrumentId::from(
                format!("{}-{}.POLYMARKET", trade.market, trade.asset_id).as_str(),
            );
            binary.raw_symbol = Symbol::new(trade.asset_id.as_str());
            binary.currency = Currency::pUSD();
            binary.outcome = Some(Ustr::from("Yes"));
            let mut info = nautilus_core::Params::new();
            info.insert(
                "condition_id".to_string(),
                Value::String(trade.market.to_string()),
            );
            info.insert("fees_enabled".to_string(), Value::Bool(false));
            binary.info = Some(info);
            InstrumentAny::BinaryOption(binary)
        };

        let mut order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache_accepted_open_order(&mut cache, instrument.id())
        };
        let mut filled = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("trade-hydrate")),
            None,
            Some(ModelPrice::from("0.5000")),
            Some(ModelQuantity::from(fill_size)),
            Some(LiquiditySide::Taker),
            Some(Money::zero(Currency::pUSD())),
            Some(UnixNanos::from(1_704_067_200_000_000_000_u64)),
            Some(AccountId::from("POLYMARKET-001")),
        );

        if let OrderEventAny::Filled(fill) = &mut filled {
            fill.info = Some(IndexMap::from([
                (Ustr::from("id"), Ustr::from("trade-hydrate")),
                (
                    Ustr::from("taker_order_id"),
                    Ustr::from(TEST_VENUE_ORDER_ID),
                ),
                (Ustr::from("size"), Ustr::from(fill_size)),
                (Ustr::from("status"), Ustr::from("MATCHED")),
            ]));
        }
        order = cache.borrow_mut().update_order(&filled).unwrap();
        if !terminal_filled {
            let terminal = if expired {
                OrderEventAny::Expired(OrderExpired::new(
                    order.trader_id(),
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    UUID4::new(),
                    order.ts_last(),
                    order.ts_last(),
                    false,
                    order.venue_order_id(),
                    Some(AccountId::from("POLYMARKET-001")),
                ))
            } else {
                TestOrderEventStubs::canceled(
                    &order,
                    AccountId::from("POLYMARKET-001"),
                    order.venue_order_id(),
                )
            };
            cache.borrow_mut().update_order(&terminal).unwrap();
        }

        client.load_instruments_from_cache();
        client.load_orders_from_cache().unwrap();
        let key = TradeCorrectionIdentity::new("trade-hydrate", TEST_VENUE_ORDER_ID);
        assert_eq!(
            client
                .hydrate_pending_terminal_orders_from(vec![trade])
                .unwrap(),
            1
        );
        assert!(
            client
                .fill_tracker
                .contains(&VenueOrderId::from(TEST_VENUE_ORDER_ID))
        );
        assert!(
            client
                .ws_dispatch_state
                .lock()
                .unwrap()
                .is_correction_provisional(&key)
        );

        let mut failed_json: Value =
            serde_json::from_str(include_str!("../../test_data/ws_user_trade.json")).unwrap();
        failed_json["id"] = Value::String("trade-hydrate".to_string());
        failed_json["taker_order_id"] = Value::String(TEST_VENUE_ORDER_ID.to_string());
        failed_json["size"] = Value::String(fill_size.to_string());
        failed_json["status"] = Value::String("FAILED".to_string());
        let user_address = client
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| client.secrets.address.clone());
        let user_api_key = client.secrets.credential.api_key().to_string();
        failed_json["owner"] = Value::String(user_api_key.clone());
        failed_json["trade_owner"] = Value::String(user_api_key.clone());
        failed_json["maker_address"] = Value::String(user_address.clone());
        let failed = UserWsMessage::Trade(serde_json::from_value(failed_json).unwrap());
        let ctx = WsDispatchContext {
            token_instruments: &client.shared_token_instruments,
            fill_tracker: &client.fill_tracker,
            pending_submits: &client.pending_submits,
            order_identities: &client.order_identities,
            emitter: &client.emitter,
            account_id: client.core.account_id,
            clock: client.clock,
            user_address: &user_address,
            user_api_key: &user_api_key,
        };
        let _ = dispatch_user_message(&failed, &ctx, &mut client.ws_dispatch_state.lock().unwrap());

        assert_eq!(
            client
                .fill_tracker
                .get_cumulative_filled(&VenueOrderId::from(TEST_VENUE_ORDER_ID)),
            Some(ModelQuantity::zero(2)),
        );
    }

    #[rstest]
    fn terminal_quote_growth_hydration_restores_signed_base_for_failed_reversal() {
        let (mut client, cache) = test_client();
        let mut trade_json: Value =
            serde_json::from_str(include_str!("../../test_data/http_trade_report.json")).unwrap();
        trade_json["id"] = Value::String("trade-quote-restart".to_string());
        trade_json["taker_order_id"] = Value::String(TEST_VENUE_ORDER_ID.to_string());
        trade_json["size"] = Value::String("11.00".to_string());
        trade_json["status"] = Value::String("MATCHED".to_string());
        trade_json["owner"] = Value::String("test_api_key".to_string());
        let trade: PolymarketTradeReport = serde_json::from_value(trade_json).unwrap();
        let venue_order_id = VenueOrderId::from(TEST_VENUE_ORDER_ID);

        let instrument = {
            let mut binary = binary_option();
            binary.id = InstrumentId::from(
                format!("{}-{}.POLYMARKET", trade.market, trade.asset_id).as_str(),
            );
            binary.raw_symbol = Symbol::new(trade.asset_id.as_str());
            binary.currency = Currency::pUSD();
            binary.outcome = Some(Ustr::from("Yes"));
            let mut info = nautilus_core::Params::new();
            info.insert(
                "condition_id".to_string(),
                Value::String(trade.market.to_string()),
            );
            info.insert("fees_enabled".to_string(), Value::Bool(false));
            binary.info = Some(info);
            InstrumentAny::BinaryOption(binary)
        };

        let mut order = OrderAny::Market(MarketOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument.id(),
            ClientOrderId::from("O-QUOTE-RESTART"),
            OrderSide::Buy,
            ModelQuantity::from("10.00"),
            TimeInForce::Ioc,
            UUID4::new(),
            UnixNanos::default(),
            false,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache.add_order(order.clone(), None, None, false).unwrap();

            order = cache
                .update_order(&TestOrderEventStubs::submitted(
                    &order,
                    AccountId::from("POLYMARKET-001"),
                ))
                .unwrap();
            order = cache
                .update_order(&OrderEventAny::Updated(OrderUpdated::new(
                    order.trader_id(),
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    ModelQuantity::from("10.00"),
                    UUID4::new(),
                    UnixNanos::from(1_u64),
                    UnixNanos::from(1_u64),
                    false,
                    None,
                    Some(AccountId::from("POLYMARKET-001")),
                    None,
                    None,
                    None,
                    false,
                )))
                .unwrap();
            order = cache
                .update_order(&TestOrderEventStubs::accepted(
                    &order,
                    AccountId::from("POLYMARKET-001"),
                    venue_order_id,
                ))
                .unwrap();
            order = cache
                .update_order(&OrderEventAny::Updated(OrderUpdated::new(
                    order.trader_id(),
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    ModelQuantity::from("11.00"),
                    UUID4::new(),
                    UnixNanos::from(2_u64),
                    UnixNanos::from(2_u64),
                    false,
                    Some(venue_order_id),
                    Some(AccountId::from("POLYMARKET-001")),
                    None,
                    None,
                    None,
                    false,
                )))
                .unwrap();

            let mut filled = TestOrderEventStubs::filled(
                &order,
                &instrument,
                Some(TradeId::from("trade-quote-restart")),
                None,
                Some(ModelPrice::from("0.5000")),
                Some(ModelQuantity::from("11.00")),
                Some(LiquiditySide::Taker),
                Some(Money::zero(Currency::pUSD())),
                Some(UnixNanos::from(1_704_067_200_000_000_000_u64)),
                Some(AccountId::from("POLYMARKET-001")),
            );
            if let OrderEventAny::Filled(fill) = &mut filled {
                fill.info = Some(IndexMap::from([
                    (Ustr::from("id"), Ustr::from("trade-quote-restart")),
                    (
                        Ustr::from("taker_order_id"),
                        Ustr::from(TEST_VENUE_ORDER_ID),
                    ),
                    (Ustr::from("size"), Ustr::from("11.00")),
                    (Ustr::from("status"), Ustr::from("MATCHED")),
                ]));
            }
            order = cache.update_order(&filled).unwrap();
            assert_eq!(order.status(), OrderStatus::Filled);
        }

        client.load_instruments_from_cache();
        client.load_orders_from_cache().unwrap();
        assert_eq!(
            client
                .hydrate_pending_terminal_orders_from(vec![trade])
                .unwrap(),
            1
        );
        assert_eq!(
            client.fill_tracker.submitted_qty(&venue_order_id),
            Some(ModelQuantity::from("11.00"))
        );

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        let mut failed_json: Value =
            serde_json::from_str(include_str!("../../test_data/ws_user_trade.json")).unwrap();
        failed_json["id"] = Value::String("trade-quote-restart".to_string());
        failed_json["taker_order_id"] = Value::String(TEST_VENUE_ORDER_ID.to_string());
        failed_json["size"] = Value::String("11.00".to_string());
        failed_json["status"] = Value::String("FAILED".to_string());
        let user_address = client
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| client.secrets.address.clone());
        let user_api_key = client.secrets.credential.api_key().to_string();
        failed_json["owner"] = Value::String(user_api_key.clone());
        failed_json["trade_owner"] = Value::String(user_api_key.clone());
        failed_json["maker_address"] = Value::String(user_address.clone());
        let failed = UserWsMessage::Trade(serde_json::from_value(failed_json).unwrap());
        let ctx = WsDispatchContext {
            token_instruments: &client.shared_token_instruments,
            fill_tracker: &client.fill_tracker,
            pending_submits: &client.pending_submits,
            order_identities: &client.order_identities,
            emitter: &client.emitter,
            account_id: client.core.account_id,
            clock: client.clock,
            user_address: &user_address,
            user_api_key: &user_api_key,
        };

        let _ = dispatch_user_message(&failed, &ctx, &mut client.ws_dispatch_state.lock().unwrap());

        assert!(matches!(
            receiver.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::FillVoided(_))
        ));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Updated(ref updated))
                if updated.quantity == ModelQuantity::from("10.00") && updated.reconciliation
        ));
        assert_eq!(
            client.fill_tracker.submitted_qty(&venue_order_id),
            Some(ModelQuantity::from("10.00"))
        );
        assert_eq!(
            client.fill_tracker.get_cumulative_filled(&venue_order_id),
            Some(ModelQuantity::zero(2))
        );
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn on_instrument_update_upserts_expired_execution_lookup_state() {
        let (client, _cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_ONLY", true, true);

        client.on_instrument_update(&expired);

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn sync_execution_lookup_keeps_expired_lookup_state_with_open_position() {
        let (client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_POSITION", true, true);
        let position = open_position(&expired);

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            cache.add_position(&position, OmsType::Netting).unwrap();
        }

        sync_execution_lookup_for_instrument(
            &client.core,
            client.clock,
            &client.shared_token_instruments,
            &client.neg_risk_index,
            &client.fill_tracker,
            &client.order_identities,
            expired.id(),
        );

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn sync_execution_lookup_keeps_expired_lookup_state_with_open_order() {
        let (client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_ORDER", true, true);

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            let _order = cache_accepted_open_order(&mut cache, expired.id());
        }

        sync_execution_lookup_for_instrument(
            &client.core,
            client.clock,
            &client.shared_token_instruments,
            &client.neg_risk_index,
            &client.fill_tracker,
            &client.order_identities,
            expired.id(),
        );

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn position_event_subscription_prunes_expired_lookup_after_position_closes() {
        let (client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_CLOSED", true, true);
        let position = open_position(&expired);
        let closed = closed_position(&position);

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            cache.add_position(&position, OmsType::Netting).unwrap();
        }

        sync_execution_lookup_for_instrument(
            &client.core,
            client.clock,
            &client.shared_token_instruments,
            &client.neg_risk_index,
            &client.fill_tracker,
            &client.order_identities,
            expired.id(),
        );
        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));

        {
            let mut cache = cache.borrow_mut();
            cache.update_position(&closed).unwrap();
        }

        let mut client = client;
        client.ensure_position_event_subscription();
        let event = position_closed_event(&closed);
        assert!(matches!(event, PositionEvent::PositionClosed(_)));
        publish_position_event("events.position.TEST".into(), &event);

        assert!(
            !client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(!client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn position_close_keeps_expired_lookup_for_retained_closed_order() {
        let (mut client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_RETAINED_ORDER", true, false);
        let position = open_position(&expired);
        let closed = closed_position(&position);
        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            cache.add_position(&position, OmsType::Netting).unwrap();
            cache_accepted_open_order(&mut cache, expired.id())
        };
        let venue_order_id = order.venue_order_id().unwrap();
        client.fill_tracker.register(
            venue_order_id,
            order.quantity(),
            order.order_side(),
            order.instrument_id(),
            expired.size_precision(),
            expired.price_precision(),
        );
        client
            .order_identities
            .register_order_identity(venue_order_id, OrderIdentity::from_order(&order));
        let canceled = TestOrderEventStubs::canceled(
            &order,
            AccountId::from("POLYMARKET-001"),
            Some(venue_order_id),
        );
        {
            let mut cache = cache.borrow_mut();
            cache.update_order(&canceled).unwrap();
            cache.update_position(&closed).unwrap();
        }
        client.upsert_execution_lookup(&expired);
        client.ensure_position_event_subscription();

        let event = position_closed_event(&closed);
        publish_position_event("events.position.TEST".into(), &event);

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.fill_tracker.has_operational_order(&venue_order_id));
    }

    #[rstest]
    fn order_event_subscription_prunes_expired_lookup_after_terminal_order() {
        let (client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_ORDER_CLOSED", true, true);
        let mut order;

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            order = cache_accepted_open_order(&mut cache, expired.id());
        }

        sync_execution_lookup_for_instrument(
            &client.core,
            client.clock,
            &client.shared_token_instruments,
            &client.neg_risk_index,
            &client.fill_tracker,
            &client.order_identities,
            expired.id(),
        );

        let canceled = TestOrderEventStubs::canceled(
            &order,
            AccountId::from("POLYMARKET-001"),
            order.venue_order_id(),
        );
        order.apply(canceled.clone()).unwrap();

        {
            let mut cache = cache.borrow_mut();
            cache.update_order(&canceled).unwrap();
        }

        let mut client = client;
        client.ensure_order_event_subscription();
        publish_order_event("events.order.TEST".into(), &canceled);

        assert!(
            !client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(!client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn canceled_order_retains_operational_state_for_a_late_fill() {
        let (mut client, cache) = test_client();
        let instrument = test_binary_option("0xCANCEL_THEN_FILL", true, false);
        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache_accepted_open_order(&mut cache, instrument.id())
        };
        let venue_order_id = order.venue_order_id().unwrap();
        client.fill_tracker.register(
            venue_order_id,
            order.quantity(),
            order.order_side(),
            order.instrument_id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        client
            .order_identities
            .register_order_identity(venue_order_id, OrderIdentity::from_order(&order));
        client.upsert_execution_lookup(&instrument);
        client.ensure_order_event_subscription();

        let canceled = TestOrderEventStubs::canceled(
            &order,
            AccountId::from("POLYMARKET-001"),
            Some(venue_order_id),
        );
        cache.borrow_mut().update_order(&canceled).unwrap();
        publish_order_event("events.order.TEST".into(), &canceled);

        assert!(client.fill_tracker.contains(&venue_order_id));
        assert!(client.order_identities.get(&venue_order_id).is_some());
        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(instrument.raw_symbol().as_str()))
        );

        assert_eq!(
            client
                .hydrate_pending_terminal_orders_from(Vec::new())
                .unwrap(),
            0
        );
        assert!(!client.fill_tracker.contains(&venue_order_id));
        assert!(client.order_identities.get(&venue_order_id).is_none());
        assert!(
            !client
                .shared_token_instruments
                .contains_key(&Ustr::from(instrument.raw_symbol().as_str()))
        );
    }

    #[rstest]
    fn cached_confirmed_taker_fill_closes_ioc_after_response_drain() {
        let (mut client, cache) = test_client();
        let instrument = test_binary_option("0xCONFIRMED_RESPONSE_DRAIN", false, false);
        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache_accepted_order(
                &mut cache,
                open_limit_order_with_tif(instrument.id(), TimeInForce::Ioc),
            )
        };
        let venue_order_id = order.venue_order_id().unwrap();
        client.fill_tracker.register(
            venue_order_id,
            order.quantity(),
            order.order_side(),
            order.instrument_id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        client
            .fill_tracker
            .record_fill(&venue_order_id, ModelQuantity::from("5"));
        client
            .order_identities
            .register_order_identity(venue_order_id, OrderIdentity::from_order(&order));
        client.order_identities.mark_accepted(venue_order_id);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        client.ensure_order_event_subscription();

        let mut filled = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("trade-confirmed-response-drain")),
            None,
            Some(ModelPrice::from("0.5000")),
            Some(ModelQuantity::from("5")),
            Some(LiquiditySide::Taker),
            Some(Money::zero(Currency::pUSD())),
            Some(UnixNanos::from(1_704_067_200_000_000_000_u64)),
            Some(AccountId::from("POLYMARKET-001")),
        );
        if let OrderEventAny::Filled(fill) = &mut filled {
            fill.info = Some(IndexMap::from([
                (
                    Ustr::from("id"),
                    Ustr::from("trade-confirmed-response-drain"),
                ),
                (
                    Ustr::from("taker_order_id"),
                    Ustr::from(TEST_VENUE_ORDER_ID),
                ),
                (Ustr::from("size"), Ustr::from("5")),
                (Ustr::from("status"), Ustr::from("CONFIRMED")),
                (Ustr::from("timestamp"), Ustr::from("1704067200999")),
            ]));
        }
        cache.borrow_mut().update_order(&filled).unwrap();
        publish_order_event("events.order.TEST".into(), &filled);

        assert!(matches!(
            receiver.try_recv().expect("expected IOC cancellation"),
            ExecutionEvent::Order(OrderEventAny::Canceled(_))
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn terminal_reconciliation_update_cleans_confirmed_fok_state() {
        let (mut client, cache) = test_client();
        let instrument = test_binary_option("0xFOK_TERMINAL_UPDATE", false, false);
        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache_accepted_order(
                &mut cache,
                open_limit_order_with_tif(instrument.id(), TimeInForce::Fok),
            )
        };
        let venue_order_id = order.venue_order_id().unwrap();
        client.fill_tracker.register(
            venue_order_id,
            order.quantity(),
            order.order_side(),
            order.instrument_id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        client
            .order_identities
            .register_order_identity(venue_order_id, OrderIdentity::from_order(&order));
        client.ensure_order_event_subscription();

        let mut filled = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("trade-fok-terminal-update")),
            None,
            Some(ModelPrice::from("0.5000")),
            Some(ModelQuantity::from(9)),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(AccountId::from("POLYMARKET-001")),
        );

        if let OrderEventAny::Filled(ref mut fill) = filled {
            fill.info = Some(IndexMap::from([
                (Ustr::from("id"), Ustr::from("trade-fok-terminal-update")),
                (
                    Ustr::from("taker_order_id"),
                    Ustr::from(TEST_VENUE_ORDER_ID),
                ),
                (Ustr::from("status"), Ustr::from("CONFIRMED")),
                (Ustr::from("size"), Ustr::from("9")),
            ]));
        }
        cache.borrow_mut().update_order(&filled).unwrap();
        publish_order_event("events.order.TEST".into(), &filled);
        assert!(client.fill_tracker.contains(&venue_order_id));
        assert!(client.order_identities.get(&venue_order_id).is_some());

        let updated = OrderEventAny::Updated(
            OrderUpdatedSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(order.client_order_id())
                .quantity(ModelQuantity::from(9))
                .maybe_venue_order_id(Some(venue_order_id))
                .maybe_account_id(Some(AccountId::from("POLYMARKET-001")))
                .reconciliation(true)
                .build(),
        );
        let updated_order = cache.borrow_mut().update_order(&updated).unwrap();
        assert!(updated_order.is_closed());
        publish_order_event("events.order.TEST".into(), &updated);

        assert!(!client.fill_tracker.contains(&venue_order_id));
        assert!(client.order_identities.get(&venue_order_id).is_none());
    }

    #[rstest]
    fn report_derived_confirmed_fill_cleans_terminal_order_state() {
        let (mut client, cache) = test_client();
        let instrument = test_binary_option("0xREST_CONFIRMED_FILL", false, false);
        let order = {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();
            cache_accepted_open_order(&mut cache, instrument.id())
        };
        let venue_order_id = order.venue_order_id().unwrap();
        client.fill_tracker.register(
            venue_order_id,
            order.quantity(),
            order.order_side(),
            order.instrument_id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        client
            .order_identities
            .register_order_identity(venue_order_id, OrderIdentity::from_order(&order));
        client.ensure_order_event_subscription();

        let filled = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("trade-rest-confirmed-fill")),
            None,
            Some(ModelPrice::from("0.5000")),
            Some(order.quantity()),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(AccountId::from("POLYMARKET-001")),
        );
        assert!(matches!(&filled, OrderEventAny::Filled(fill) if fill.info.is_none()));
        let filled_order = cache.borrow_mut().update_order(&filled).unwrap();
        assert!(filled_order.is_closed());
        publish_order_event("events.order.TEST".into(), &filled);

        assert!(!client.fill_tracker.contains(&venue_order_id));
        assert!(client.order_identities.get(&venue_order_id).is_none());
    }

    #[rstest]
    fn order_event_subscription_keeps_expired_lookup_after_filled_when_position_remains_open() {
        let (client, cache) = test_client();
        let expired = test_binary_option("0xEXPIRED_FILLED_OPEN", true, true);
        let order;
        let position;

        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(expired.clone()).unwrap();
            order = cache_accepted_open_order(&mut cache, expired.id());
        }

        sync_execution_lookup_for_instrument(
            &client.core,
            client.clock,
            &client.shared_token_instruments,
            &client.neg_risk_index,
            &client.fill_tracker,
            &client.order_identities,
            expired.id(),
        );

        let filled = TestOrderEventStubs::filled(
            &order,
            &expired,
            None,
            None,
            Some(ModelPrice::from("0.5000")),
            None,
            None,
            None,
            None,
            Some(AccountId::from("POLYMARKET-001")),
        );

        position = match filled.clone() {
            OrderEventAny::Filled(filled) => Position::new(&expired, filled),
            other => panic!("expected filled event, was {other:?}"),
        };

        {
            let mut cache = cache.borrow_mut();
            cache.update_order(&filled).unwrap();
            cache.add_position(&position, OmsType::Netting).unwrap();
        }

        let mut client = client;
        client.ensure_order_event_subscription();
        publish_order_event("events.order.TEST".into(), &filled);

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn position_event_subscription_ignores_other_venue_events() {
        let (mut client, _cache) = test_client();
        let expired = test_binary_option("0xOTHER_VENUE", true, true);
        client.upsert_execution_lookup(&expired);
        client.ensure_position_event_subscription();

        let mut event = position_closed_event(&closed_position(&open_position(&expired)));
        if let PositionEvent::PositionClosed(ref mut closed) = event {
            closed.instrument_id = InstrumentId::from("0xOTHER.OTHER");
        }

        publish_position_event("events.position.TEST".into(), &event);

        assert!(
            client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(client.neg_risk_index.contains_key(&expired.id()));
    }

    #[rstest]
    fn event_subscriptions_can_be_reinstalled_after_disconnect_cleanup() {
        let (mut client, _cache) = test_client();

        client.start_client();
        assert!(client.order_event_handler.is_none());
        assert!(client.position_event_handler.is_none());

        client.ensure_order_event_subscription();
        client.ensure_position_event_subscription();
        assert!(client.order_event_handler.is_some());
        assert!(client.position_event_handler.is_some());

        client.clear_order_event_subscription();
        client.clear_position_event_subscription();
        assert!(client.order_event_handler.is_none());
        assert!(client.position_event_handler.is_none());

        client.ensure_order_event_subscription();
        client.ensure_position_event_subscription();
        assert!(client.order_event_handler.is_some());
        assert!(client.position_event_handler.is_some());
    }

    #[rstest]
    fn reset_clears_subscriptions_and_lookup_state() {
        let (mut client, _cache) = test_client();
        let expired = test_binary_option("0xRESET", true, true);
        client.upsert_execution_lookup(&expired);
        client.ensure_order_event_subscription();
        client.ensure_position_event_subscription();
        client
            .ws_dispatch_state
            .lock()
            .expect(MUTEX_POISONED)
            .restore_voided_trade(TradeCorrectionIdentity::from("trade-1"), &[])
            .unwrap();

        client.reset_client().unwrap();

        assert!(client.order_event_handler.is_none());
        assert!(client.position_event_handler.is_none());
        assert!(
            !client
                .shared_token_instruments
                .contains_key(&Ustr::from(expired.raw_symbol().as_str()))
        );
        assert!(!client.neg_risk_index.contains_key(&expired.id()));
        assert!(
            !client
                .ws_dispatch_state
                .lock()
                .expect(MUTEX_POISONED)
                .is_voided_trade("trade-1")
        );
    }

    #[rstest]
    fn stop_preserves_websocket_dedup_state_for_reconnect() {
        let (mut client, _cache) = test_client();
        let dedup_key = "trade-reconnect".to_string();
        client.start_client();
        client
            .ws_dispatch_state
            .lock()
            .expect(MUTEX_POISONED)
            .restore_voided_trade(TradeCorrectionIdentity::from(dedup_key.clone()), &[])
            .unwrap();

        client.stop_client();

        assert!(
            client
                .ws_dispatch_state
                .lock()
                .expect(MUTEX_POISONED)
                .is_voided_trade(&dedup_key)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn reset_refuses_to_clear_an_in_flight_submit_reservation() {
        let (mut client, _cache) = test_client();
        let venue_order_id = VenueOrderId::from("V-RESET-IN-FLIGHT");
        client
            .fill_tracker
            .reserve_orders(&[venue_order_id])
            .unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        client.spawn_task("held submit", async move {
            let _ = release_rx.await;
            Ok(())
        });
        client.start_client();
        client.stop_client();

        let error = client.reset_client().unwrap_err();
        assert_eq!(
            error.to_string(),
            "cannot reset Polymarket execution state while submit tasks are still running"
        );
        assert!(client.fill_tracker.has_operational_order(&venue_order_id));

        release_tx.send(()).unwrap();
        client.await_pending_tasks().await;
        client.reset_client().unwrap();
        assert!(!client.fill_tracker.has_operational_order(&venue_order_id));
    }

    #[rstest]
    fn cache_reload_preserves_pending_terminal_association_until_confirmation() {
        let (mut client, cache) = test_client();
        let mut trade: PolymarketUserTrade =
            serde_json::from_str(include_str!("../../test_data/ws_user_trade.json")).unwrap();
        trade.status = PolymarketTradeStatus::Matched;
        trade.taker_order_id = TEST_VENUE_ORDER_ID.to_string();
        trade.size = "9.995000".to_string();

        let mut binary = binary_option();
        binary.id =
            InstrumentId::from(format!("{}-{}.POLYMARKET", trade.market, trade.asset_id).as_str());
        binary.raw_symbol = Symbol::new(trade.asset_id.as_str());
        binary.currency = Currency::pUSD();
        binary.outcome = Some(Ustr::from("Yes"));
        binary.size_precision = 6;
        binary.size_increment = ModelQuantity::from("0.000001");
        let mut info = nautilus_core::Params::new();
        info.insert(
            "condition_id".to_string(),
            Value::String(trade.market.to_string()),
        );
        info.insert("fees_enabled".to_string(), Value::Bool(false));
        binary.info = Some(info);
        let instrument = InstrumentAny::BinaryOption(binary);

        let order = OrderAny::Limit(LimitOrder::new(
            TraderId::from("TESTER-001"),
            StrategyId::from("S-001"),
            instrument.id(),
            ClientOrderId::from("O-RECONNECT-NORMALIZATION"),
            OrderSide::Buy,
            ModelQuantity::from("10.000000"),
            ModelPrice::from(trade.price.as_str()),
            TimeInForce::Gtc,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        ));
        {
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument).unwrap();
            cache_accepted_order(&mut cache, order);
        }
        client.load_instruments_from_cache();
        client.load_orders_from_cache().unwrap();

        let mut order: PolymarketUserOrder =
            serde_json::from_str(include_str!("../../test_data/ws_user_order_matched.json"))
                .unwrap();
        order.id = TEST_VENUE_ORDER_ID.to_string();
        order.asset_id = trade.asset_id;
        order.market = trade.market;
        order.outcome = Some(crate::common::enums::PolymarketOutcome::yes());
        order.original_size = "10.000000".to_string();
        order.size_matched = "9.995000".to_string();
        order.price.clone_from(&trade.price);
        order.associate_trades = Some(vec![trade.id.clone()]);

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.emitter.set_sender(sender);
        let user_address = client
            .secrets
            .funder
            .clone()
            .unwrap_or_else(|| client.secrets.address.clone());
        let user_api_key = client.secrets.credential.api_key().to_string();
        trade.owner = Ustr::from(user_api_key.as_str());
        trade.trade_owner = Ustr::from(user_api_key.as_str());
        trade.maker_address = Ustr::from(user_address.as_str());
        order.owner = Ustr::from(user_api_key.as_str());
        order.order_owner = Some(Ustr::from(user_api_key.as_str()));
        order.maker_address = Some(Ustr::from(user_address.as_str()));
        let ctx = WsDispatchContext {
            token_instruments: &client.shared_token_instruments,
            fill_tracker: &client.fill_tracker,
            pending_submits: &client.pending_submits,
            order_identities: &client.order_identities,
            emitter: &client.emitter,
            account_id: client.core.account_id,
            clock: client.clock,
            user_address: &user_address,
            user_api_key: &user_api_key,
        };

        dispatch_user_message(
            &UserWsMessage::Order(order),
            &ctx,
            &mut client.ws_dispatch_state.lock().unwrap(),
        );
        assert!(receiver.try_recv().is_err());
        dispatch_user_message(
            &UserWsMessage::Trade(trade.clone()),
            &ctx,
            &mut client.ws_dispatch_state.lock().unwrap(),
        );
        let ExecutionEvent::Order(OrderEventAny::Filled(fill)) = receiver.try_recv().unwrap()
        else {
            panic!("expected provisional fill");
        };
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Filled(fill))
            .unwrap();

        client.load_orders_from_cache().unwrap();

        trade.status = PolymarketTradeStatus::Confirmed;
        dispatch_user_message(
            &UserWsMessage::Trade(trade),
            &ctx,
            &mut client.ws_dispatch_state.lock().unwrap(),
        );
        let ExecutionEvent::Order(OrderEventAny::Updated(updated)) = receiver.try_recv().unwrap()
        else {
            panic!("expected post-reconnect terminal quantity normalization");
        };
        assert_eq!(updated.quantity, ModelQuantity::from("9.995000"));
        assert!(updated.reconciliation);
        assert!(receiver.try_recv().is_err());
    }
}
