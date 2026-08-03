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

//! WebSocket message dispatch for the Polymarket execution client.
//!
//! Routes user-channel WS messages (order updates and trades) for orders submitted through this
//! client into Nautilus order events (`OrderAccepted` / `OrderFilled` / `OrderFillVoided` /
//! `OrderCanceled` / `OrderRejected` / `OrderExpired`), building them from the identity captured at
//! submit (`LocalOrderCoordinator`). Order-channel messages drive lifecycle events; trade-channel
//! messages drive fills, and acceptance is synthesized before a fill or cancel that races ahead.
//! The coordinator admits local artifacts against the identity claimed before submission;
//! untracked provisional fills are retained until the trade confirms. Reports are reserved for the
//! `generate_*` query and reconciliation methods. Trade fills are emitted at `MATCHED`, retained
//! until terminal settlement, and reversed with `OrderFillVoided` if the trade reaches `FAILED`.

use std::str::FromStr;

use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use nautilus_core::{UUID4, UnixNanos, collections::AtomicMap, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{
        OrderAccepted, OrderCanceled, OrderEventAny, OrderExpired, OrderFillConfirmed,
        OrderFillVoided, OrderFilled, OrderRejected, OrderUpdated,
    },
    identifiers::{AccountId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    messages::{PolymarketUserOrder, PolymarketUserTrade, UserWsMessage},
    parse::parse_timestamp_ms,
};
use crate::{
    common::{
        enums::{PolymarketLiquiditySide, PolymarketOrderStatus, PolymarketTradeStatus},
        models::PolymarketMakerOrder,
    },
    execution::{
        get_pusd_currency,
        local_orders::{ArtifactAdmission, LocalOrderCoordinator, OrderIdentity},
        parse::{
            build_maker_fill_report, compute_commission, determine_order_side,
            instrument_fee_exponent, instrument_taker_fee, parse_liquidity_side, serialize_info,
        },
    },
};

/// Signal returned when a finalized trade requires an async account refresh.
#[derive(Debug)]
pub(crate) struct AccountRefreshRequest;

/// Mutable state retained across user WebSocket stream generations.
#[derive(Debug, Default)]
pub(crate) struct WsDispatchState {
    // Settlement truth is process-lifetime state. Capacity eviction could replay a duplicate fill
    // or discard the only provenance capable of voiding a failed provisional fill.
    pub processed_fills: AHashSet<String>,
    matched_fills: AHashMap<String, Vec<OrderFilled>>,
    voided_trades: AHashSet<String>,
    confirmed_trades: AHashSet<String>,
    pending_untracked_fills: AHashMap<String, Vec<FillReport>>,
    pending_terminal_orders: AHashMap<VenueOrderId, PendingTerminalOrder>,
    /// Cancel reports saved for orders known to be terminal at the venue.
    /// Re-emitted after a fill to restore terminal state when fills race
    /// ahead of (or arrive after) cancel messages.
    terminal_cancel_reports: AHashMap<VenueOrderId, OrderStatusReport>,
}

pub(crate) enum RestoredTradeSettlementAction {
    Pending,
    Confirmed(Vec<OrderFilled>),
    Failed(Vec<OrderFilled>),
}

impl WsDispatchState {
    pub(crate) fn restore_matched_trade(&mut self, key: String, fills: Vec<OrderFilled>) {
        self.processed_fills.insert(key.clone());
        self.matched_fills.insert(key, fills);
    }

    pub(crate) fn restore_voided_trade(&mut self, key: String) {
        self.processed_fills.insert(key.clone());
        self.matched_fills.remove(&key);
        self.voided_trades.insert(key);
    }

    pub(crate) fn restore_trade_settlement(
        &mut self,
        trade_id: &str,
        key: String,
        fills: Vec<OrderFilled>,
        status: PolymarketTradeStatus,
    ) -> RestoredTradeSettlementAction {
        if status == PolymarketTradeStatus::Failed {
            self.restore_voided_trade(key);
            return RestoredTradeSettlementAction::Failed(fills);
        }

        if status.is_finalized() {
            self.processed_fills.insert(key);
            for fill in &fills {
                self.confirmed_trades
                    .insert(confirmed_trade_key(trade_id, fill.venue_order_id));
            }
            return RestoredTradeSettlementAction::Confirmed(fills);
        }

        self.restore_matched_trade(key, fills);
        RestoredTradeSettlementAction::Pending
    }
}

#[cfg(test)]
impl WsDispatchState {
    pub(crate) fn matched_fill_count(&self, key: &str) -> usize {
        self.matched_fills.get(&key.to_string()).map_or(0, Vec::len)
    }

    pub(crate) fn is_voided_trade(&self, key: &str) -> bool {
        self.voided_trades.contains(key)
    }

    pub(crate) fn is_confirmed_trade(&self, trade_id: &str, venue_order_id: VenueOrderId) -> bool {
        self.confirmed_trades
            .contains(&confirmed_trade_key(trade_id, venue_order_id))
    }
}

#[derive(Clone, Debug)]
struct PendingTerminalOrder {
    trade_ids: Vec<String>,
    ts_event: UnixNanos,
}

/// Immutable context borrowed from the async block's owned values.
#[derive(Debug)]
pub(crate) struct WsDispatchContext<'a> {
    pub token_instruments: &'a AtomicMap<Ustr, InstrumentAny>,
    pub local_orders: &'a LocalOrderCoordinator,
    pub emitter: &'a ExecutionEventEmitter,
    pub account_id: AccountId,
    pub clock: &'static AtomicTime,
    pub user_address: &'a str,
    pub user_api_key: &'a str,
}

/// Top-level router: synchronous, returns signal for async account refresh.
pub(crate) fn dispatch_user_message(
    message: &UserWsMessage,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) -> Option<AccountRefreshRequest> {
    match message {
        UserWsMessage::Order(order) => {
            dispatch_order_update(order, ctx, state);
            None
        }
        UserWsMessage::Trade(trade) => dispatch_trade_update(trade, ctx, state),
    }
}

fn dispatch_order_update(
    order: &PolymarketUserOrder,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    let instruments = ctx.token_instruments.load();
    let instrument = match instruments.get(&order.asset_id) {
        Some(i) => i,
        None => {
            log::warn!("Unknown asset_id in order update: {}", order.asset_id);
            return;
        }
    };

    let ts_event = parse_timestamp_ms(&order.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let venue_order_id = VenueOrderId::from(order.id.as_str());

    let ts_init = ctx.clock.get_time_ns();
    let report = build_ws_order_status_report(order, instrument, ctx.account_id, ts_event, ts_init);
    let is_local_order = match ctx.local_orders.admit_order_report(report) {
        ArtifactAdmission::Owned { artifact, identity } => {
            if artifact.order_status == OrderStatus::Canceled {
                state
                    .terminal_cancel_reports
                    .insert(venue_order_id, artifact.clone());
            }
            emit_tracked_order_status(&artifact, &identity, ts_event, ctx);
            true
        }
        ArtifactAdmission::Untracked(report) => {
            ctx.emitter.send_order_status_report(report);
            false
        }
        ArtifactAdmission::Conflict(_) => {
            log::error!("Rejecting WebSocket order update for {venue_order_id}: identity conflict");
            false
        }
    };

    if is_local_order
        && order.status == PolymarketOrderStatus::Matched
        && let Some(trade_ids) = order.associate_trades.clone().filter(|ids| !ids.is_empty())
    {
        state.pending_terminal_orders.insert(
            venue_order_id,
            PendingTerminalOrder {
                trade_ids,
                ts_event,
            },
        );
        emit_quantity_normalization_if_ready(venue_order_id, ctx, state);
    }
}

fn emit_quantity_normalization_if_ready(
    venue_order_id: VenueOrderId,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    let is_ready = state
        .pending_terminal_orders
        .get(&venue_order_id)
        .is_some_and(|pending| {
            pending.trade_ids.iter().all(|trade_id| {
                state
                    .confirmed_trades
                    .contains(&confirmed_trade_key(trade_id, venue_order_id))
            })
        });

    if !is_ready {
        return;
    }

    let Some(pending) = state.pending_terminal_orders.remove(&venue_order_id) else {
        return;
    };

    let Some(identity) = ctx.local_orders.identity(&venue_order_id) else {
        log::warn!("Cannot normalize terminal order {venue_order_id} without a local identity");
        return;
    };

    if let Some(quantity) = ctx
        .local_orders
        .check_terminal_quantity_normalization(&venue_order_id)
    {
        emit_terminal_quantity_update(&identity, venue_order_id, quantity, pending.ts_event, ctx);
    }
}

/// Emits the terminal order event for a taker order once its trade confirms.
///
/// Taker fills receive no order-channel `MATCHED` update. FOK is atomic, so a sub-cent quantity
/// difference can be normalized. IOC maps to FAK, so every positive remainder was killed by the
/// venue and must close as `Canceled` without changing the venue-reported fill quantity.
fn emit_taker_terminal_status(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
    ts_event: UnixNanos,
) {
    let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());

    let Some(identity) = ctx.local_orders.identity(&venue_order_id) else {
        return;
    };

    if identity.requires_terminal_quantity_normalization() {
        if let Some(quantity) = ctx
            .local_orders
            .check_terminal_quantity_normalization(&venue_order_id)
        {
            emit_terminal_quantity_update(&identity, venue_order_id, quantity, ts_event, ctx);
        }
        return;
    }

    if identity.time_in_force == TimeInForce::Ioc
        && let Some(remainder) = ctx
            .local_orders
            .take_terminal_ioc_remainder(&venue_order_id)
    {
        log::debug!(
            "Closing terminal IOC order {venue_order_id} as Canceled (unfilled remainder={remainder})"
        );
        emit_order_canceled(&identity, venue_order_id, ts_event, ctx);
    }
}

fn dispatch_trade_update(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) -> Option<AccountRefreshRequest> {
    let dedup_key = format!("{}-{}", trade.id, trade.taker_order_id);
    if trade.status == PolymarketTradeStatus::Failed {
        void_failed_trade(trade, dedup_key, ctx, state);
        return Some(AccountRefreshRequest);
    }

    if matches!(
        trade.status,
        PolymarketTradeStatus::Mined | PolymarketTradeStatus::Retrying
    ) {
        log::debug!("Waiting for terminal trade status: {}", trade.id);
        return None;
    }

    if has_unknown_trade_instrument(trade, ctx) {
        log::warn!(
            "Deferring trade {} until its instrument is available",
            trade.id
        );
        return None;
    }

    let is_confirmed = trade.status == PolymarketTradeStatus::Confirmed;
    dispatch_trade_fills(trade, &dedup_key, is_confirmed, ctx, state);

    if !is_confirmed {
        return None;
    }

    confirm_trade(trade, &dedup_key, ctx, state);
    Some(AccountRefreshRequest)
}

fn void_failed_trade(
    trade: &PolymarketUserTrade,
    dedup_key: String,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    if state.voided_trades.contains(&dedup_key) {
        return;
    }

    let direct_fills = state.matched_fills.remove(&dedup_key).unwrap_or_default();
    for fill in &direct_fills {
        ctx.local_orders
            .reverse_fill(&fill.venue_order_id, fill.last_qty);
        state
            .confirmed_trades
            .remove(&confirmed_trade_key(&trade.id, fill.venue_order_id));
    }

    state.pending_untracked_fills.remove(&dedup_key);
    for fill in direct_fills {
        emit_order_fill_voided(&fill, trade, Some(fill.event_id), ctx);
    }

    state.processed_fills.insert(dedup_key.clone());
    state.voided_trades.insert(dedup_key);
}

fn has_unknown_trade_instrument(trade: &PolymarketUserTrade, ctx: &WsDispatchContext<'_>) -> bool {
    let instruments = ctx.token_instruments.load();

    if trade.trader_side == PolymarketLiquiditySide::Maker {
        trade
            .maker_orders
            .iter()
            .filter(|order| is_user_maker_order(order, ctx))
            .any(|order| !instruments.contains_key(&order.asset_id))
    } else {
        !instruments.contains_key(&trade.asset_id)
    }
}

fn dispatch_trade_fills(
    trade: &PolymarketUserTrade,
    dedup_key: &String,
    is_confirmed: bool,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    if state.processed_fills.contains(dedup_key) {
        log::debug!("Duplicate fill skipped: {dedup_key}");
        return;
    }

    state.processed_fills.insert(dedup_key.clone());
    let fills = if trade.trader_side == PolymarketLiquiditySide::Maker {
        dispatch_maker_fills(trade, dedup_key, is_confirmed, ctx, state)
    } else {
        dispatch_taker_fill(trade, dedup_key, is_confirmed, ctx, state)
    };

    if !fills.is_empty() {
        state.matched_fills.insert(dedup_key.clone(), fills);
    }
}

fn confirm_trade(
    trade: &PolymarketUserTrade,
    dedup_key: &str,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let mut newly_owned = Vec::new();
    if let Some(reports) = state.pending_untracked_fills.remove(&dedup_key.to_string()) {
        for report in reports {
            let venue_order_id = report.venue_order_id;
            match ctx.local_orders.admit_fill_report(report) {
                ArtifactAdmission::Owned { artifact, identity } => {
                    newly_owned.push(emit_order_filled(&identity, &artifact, None, ctx));
                }
                ArtifactAdmission::Untracked(report) => ctx.emitter.send_fill_report(report),
                ArtifactAdmission::Conflict(_) => log::error!(
                    "Rejecting retained fill for {venue_order_id}: local order identity conflict"
                ),
            }
        }
    }

    if !newly_owned.is_empty() {
        let key = dedup_key.to_string();
        let mut fills = state.matched_fills.remove(&key).unwrap_or_default();
        fills.extend(newly_owned);
        state.matched_fills.insert(key, fills);
    }

    let mut confirmed_local_venues = Vec::new();
    if let Some(fills) = state.matched_fills.get(&dedup_key.to_string()) {
        for fill in fills {
            if !confirmed_local_venues.contains(&fill.venue_order_id) {
                confirmed_local_venues.push(fill.venue_order_id);
            }
        }
    }

    let mut newly_confirmed_fills = Vec::new();
    if let Some(fills) = state.matched_fills.get(&dedup_key.to_string()) {
        for fill in fills.clone() {
            let confirmed_key = confirmed_trade_key(&trade.id, fill.venue_order_id);
            if !state.confirmed_trades.contains(&confirmed_key) {
                state.confirmed_trades.insert(confirmed_key);
                newly_confirmed_fills.push(fill);
            }
        }
    }

    for venue_order_id in &confirmed_local_venues {
        emit_quantity_normalization_if_ready(*venue_order_id, ctx, state);
    }

    let taker_venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
    if trade.trader_side != PolymarketLiquiditySide::Maker
        && confirmed_local_venues.contains(&taker_venue_order_id)
    {
        emit_taker_terminal_status(trade, ctx, ts_event);
    }

    for fill in newly_confirmed_fills {
        emit_order_fill_confirmed(&fill, trade, ts_event, ctx);
    }
    state.matched_fills.remove(&dedup_key.to_string());
}

fn confirmed_trade_key(trade_id: &str, venue_order_id: VenueOrderId) -> String {
    format!("{trade_id}-{venue_order_id}")
}

fn dispatch_maker_fills(
    trade: &PolymarketUserTrade,
    correction_key: &str,
    is_confirmed: bool,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) -> Vec<OrderFilled> {
    let user_orders: Vec<_> = trade
        .maker_orders
        .iter()
        .filter(|order| is_user_maker_order(order, ctx))
        .collect();

    if user_orders.is_empty() {
        log::warn!("No matching maker orders for user in trade: {}", trade.id);
        return Vec::new();
    }

    let instruments = ctx.token_instruments.load();
    let fill_info = trade_fill_info(trade);
    let liquidity_side = parse_liquidity_side(trade.trader_side);
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let ts_init = ctx.clock.get_time_ns();
    let mut fills = Vec::new();

    for mo in user_orders {
        let asset_id = Ustr::from(mo.asset_id.as_str());
        let instrument = match instruments.get(&asset_id) {
            Some(i) => i,
            None => {
                log::warn!("Unknown asset_id in maker order: {asset_id}");
                continue;
            }
        };
        let report = build_maker_fill_report(
            mo,
            &trade.id,
            trade.trader_side,
            trade.side,
            trade.asset_id.as_str(),
            ctx.account_id,
            instrument.id(),
            instrument.price_precision(),
            instrument.size_precision(),
            crate::execution::get_pusd_currency(),
            liquidity_side,
            ts_event,
            ts_init,
        );
        let maker_venue_order_id = report.venue_order_id;
        match ctx.local_orders.admit_fill_report(report) {
            ArtifactAdmission::Owned { artifact, identity } => {
                fills.push(emit_order_filled(
                    &identity,
                    &artifact,
                    fill_info.clone(),
                    ctx,
                ));
                reemit_terminal_cancel(maker_venue_order_id, state, ctx);
            }
            ArtifactAdmission::Untracked(report) => {
                emit_or_retain_untracked_fill(report, correction_key, is_confirmed, ctx, state);
            }
            ArtifactAdmission::Conflict(_) => log::error!(
                "Rejecting maker fill for {maker_venue_order_id}: local order identity conflict"
            ),
        }
    }
    fills
}

fn is_user_maker_order(order: &PolymarketMakerOrder, ctx: &WsDispatchContext<'_>) -> bool {
    order.is_owned_by(ctx.user_address, ctx.user_api_key)
}

fn dispatch_taker_fill(
    trade: &PolymarketUserTrade,
    correction_key: &str,
    is_confirmed: bool,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) -> Vec<OrderFilled> {
    let instruments = ctx.token_instruments.load();
    let instrument = match instruments.get(&trade.asset_id) {
        Some(i) => i,
        None => {
            log::warn!("Unknown asset_id in trade: {}", trade.asset_id);
            return Vec::new();
        }
    };

    let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
    let liquidity_side = parse_liquidity_side(trade.trader_side);
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let ts_init = ctx.clock.get_time_ns();

    let report = build_ws_taker_fill_report(
        trade,
        instrument,
        ctx.account_id,
        liquidity_side,
        ts_event,
        ts_init,
    );
    match ctx.local_orders.admit_fill_report(report) {
        ArtifactAdmission::Owned { artifact, identity } => {
            let fill = emit_order_filled(&identity, &artifact, trade_fill_info(trade), ctx);
            reemit_terminal_cancel(venue_order_id, state, ctx);
            vec![fill]
        }
        ArtifactAdmission::Untracked(report) => {
            emit_or_retain_untracked_fill(report, correction_key, is_confirmed, ctx, state);
            Vec::new()
        }
        ArtifactAdmission::Conflict(_) => {
            log::error!("Rejecting taker fill for {venue_order_id}: local order identity conflict");
            Vec::new()
        }
    }
}

fn emit_or_retain_untracked_fill(
    report: FillReport,
    correction_key: &str,
    is_confirmed: bool,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    if is_confirmed {
        ctx.emitter.send_fill_report(report);
        return;
    }
    let key = correction_key.to_string();
    let mut reports = state
        .pending_untracked_fills
        .remove(&key)
        .unwrap_or_default();
    reports.push(report);
    state.pending_untracked_fills.insert(key, reports);
}

/// Re-emits a saved cancel report after a fill to restore terminal state.
///
/// When fills race ahead of (or arrive after) cancel messages, the order can
/// get stuck in `PartiallyFilled`. This re-emission ensures the execution
/// engine transitions the order back to `Canceled`.
///
/// Skips re-emission when the coordinator shows the order is fully filled,
/// because `Filled` is already terminal and a spurious cancel would fail
/// the `Filled -> Canceled` state transition.
fn reemit_terminal_cancel(
    venue_order_id: VenueOrderId,
    state: &WsDispatchState,
    ctx: &WsDispatchContext<'_>,
) {
    if ctx.local_orders.is_fully_filled(&venue_order_id) {
        return;
    }

    if let Some(cancel_report) = state.terminal_cancel_reports.get(&venue_order_id) {
        log::debug!("Re-emitting cancel for {venue_order_id} after fill to restore terminal state");
        match ctx.local_orders.admit_order_report(cancel_report.clone()) {
            ArtifactAdmission::Owned { artifact, identity } => {
                emit_order_canceled(&identity, venue_order_id, artifact.ts_last, ctx);
            }
            ArtifactAdmission::Untracked(report) => ctx.emitter.send_order_status_report(report),
            ArtifactAdmission::Conflict(_) => log::error!(
                "Rejecting retained cancel for {venue_order_id}: local order identity conflict"
            ),
        }
    }
}

fn build_ws_order_status_report(
    order: &PolymarketUserOrder,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderStatusReport {
    let venue_order_id = VenueOrderId::from(order.id.as_str());
    let order_status =
        crate::execution::parse::resolve_order_status(order.status, order.event_type);
    let order_side = OrderSide::from(order.side);
    let time_in_force = TimeInForce::from(order.order_type);
    let size_precision = instrument.size_precision();
    let price_precision = instrument.price_precision();
    let quantity = Decimal::from_str(&order.original_size)
        .ok()
        .and_then(|d| Quantity::from_decimal_dp(d, size_precision).ok())
        .unwrap_or_else(|| Quantity::zero(size_precision));
    let filled_qty = Decimal::from_str(&order.size_matched)
        .ok()
        .and_then(|d| Quantity::from_decimal_dp(d, size_precision).ok())
        .unwrap_or_else(|| Quantity::zero(size_precision));
    let price = Decimal::from_str(&order.price)
        .ok()
        .and_then(|d| Price::from_decimal_dp(d, price_precision).ok())
        .unwrap_or_else(|| Price::zero(price_precision));

    let mut report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        None,
        venue_order_id,
        order_side,
        OrderType::Limit,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_event,
        ts_event,
        ts_init,
        None,
    );
    report.price = Some(price);
    report
}

fn build_ws_taker_fill_report(
    trade: &PolymarketUserTrade,
    instrument: &InstrumentAny,
    account_id: AccountId,
    liquidity_side: LiquiditySide,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> FillReport {
    let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
    let trade_id = TradeId::from(trade.id.as_str());
    let order_side = determine_order_side(
        trade.trader_side,
        trade.side,
        trade.asset_id.as_str(),
        trade.asset_id.as_str(),
    );

    let size_precision = instrument.size_precision();
    let price_precision = instrument.price_precision();
    let size_dec = Decimal::from_str(&trade.size).unwrap_or_default();
    let price_dec = Decimal::from_str(&trade.price).unwrap_or_default();
    let last_qty = Quantity::from_decimal_dp(size_dec, size_precision)
        .unwrap_or_else(|_| Quantity::zero(size_precision));
    let last_px = Price::from_decimal_dp(price_dec, price_precision)
        .unwrap_or_else(|_| Price::zero(price_precision));

    let fee_rate = instrument_taker_fee(instrument);
    let commission_value = compute_commission(
        fee_rate,
        instrument_fee_exponent(instrument),
        size_dec,
        price_dec,
        liquidity_side,
    );
    let pusd = crate::execution::get_pusd_currency();

    FillReport {
        account_id,
        instrument_id: instrument.id(),
        venue_order_id,
        trade_id,
        order_side,
        last_qty,
        last_px,
        commission: Money::new(commission_value, pusd),
        liquidity_side,
        avg_px: None,
        report_id: UUID4::new(),
        ts_event,
        ts_init,
        client_order_id: None,
        venue_position_id: None,
    }
}

/// Emits order events for a tracked own-order status update.
///
/// Order-channel messages drive lifecycle events only; fills arrive separately on the trade
/// channel as `OrderFilled`. `PartiallyFilled` / `Filled` statuses therefore emit no fill here,
/// they only ensure acceptance has been emitted so the order lifecycle stays well-formed.
fn emit_tracked_order_status(
    report: &OrderStatusReport,
    identity: &OrderIdentity,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let venue_order_id = report.venue_order_id;
    match report.order_status {
        OrderStatus::Accepted => ensure_accepted(identity, venue_order_id, ts_event, ctx),
        OrderStatus::PartiallyFilled | OrderStatus::Filled => {
            ensure_accepted(identity, venue_order_id, ts_event, ctx);
        }
        OrderStatus::Canceled => {
            ensure_accepted(identity, venue_order_id, ts_event, ctx);
            emit_order_canceled(identity, venue_order_id, ts_event, ctx);
        }
        OrderStatus::Expired => {
            ensure_accepted(identity, venue_order_id, ts_event, ctx);
            emit_order_expired(identity, venue_order_id, ts_event, ctx);
        }
        OrderStatus::Rejected => {
            let reason = report
                .cancel_reason
                .clone()
                .unwrap_or_else(|| "REJECTED".to_string());
            emit_order_rejected(identity, &reason, ts_event, ctx);
        }
        other => log::debug!("No order event for status {other:?} on {venue_order_id}"),
    }
}

/// Emits `OrderAccepted` for a tracked order if acceptance has not yet been emitted.
///
/// Acceptance is also emitted on the submit happy path; the coordinator's submission state ensures it
/// fires exactly once across the submit confirmation and the WS stream, including when a fill or
/// cancel races ahead of the acceptance message.
fn ensure_accepted(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let Ok(is_new) = ctx
        .local_orders
        .observe_acceptance(venue_order_id, *identity)
    else {
        log::error!("Cannot accept {venue_order_id}: local order identity conflict");
        return;
    };
    if !is_new {
        return;
    }
    let accepted = OrderAccepted::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        venue_order_id,
        ctx.account_id,
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Accepted(accepted));
}

/// Builds and emits an `OrderFilled` event for a tracked order, synthesizing acceptance first.
///
/// `info` carries the venue fill metadata (the raw trade fields) for trade-sourced fills, and is
/// `None` for order-path fills that have no originating trade payload.
pub(crate) fn emit_order_filled(
    identity: &OrderIdentity,
    fill: &FillReport,
    info: Option<IndexMap<Ustr, Ustr>>,
    ctx: &WsDispatchContext<'_>,
) -> OrderFilled {
    ensure_accepted(identity, fill.venue_order_id, fill.ts_event, ctx);

    if let Some(new_qty) = ctx.local_orders.buy_overfill_bump(&fill.venue_order_id) {
        emit_buy_overfill_update(identity, fill.venue_order_id, new_qty, fill.ts_event, ctx);
    }

    let filled = build_order_filled(identity, fill, info, ctx);
    ctx.emitter
        .send_order_event(OrderEventAny::Filled(filled.clone()));
    filled
}

fn build_order_filled(
    identity: &OrderIdentity,
    fill: &FillReport,
    info: Option<IndexMap<Ustr, Ustr>>,
    ctx: &WsDispatchContext<'_>,
) -> OrderFilled {
    OrderFilled::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        fill.venue_order_id,
        ctx.account_id,
        fill.trade_id,
        identity.order_side,
        identity.order_type,
        fill.last_qty,
        fill.last_px,
        get_pusd_currency(),
        fill.liquidity_side,
        UUID4::new(),
        fill.ts_event,
        fill.ts_init,
        false,
        fill.venue_position_id,
        Some(fill.commission),
        info,
    )
}

fn emit_order_fill_voided(
    fill: &OrderFilled,
    trade: &PolymarketUserTrade,
    causation_id: Option<UUID4>,
    ctx: &WsDispatchContext<'_>,
) {
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let mut voided = build_order_fill_voided(
        fill,
        &trade.id,
        ts_event,
        ctx.clock.get_time_ns(),
        trade_fill_info(trade),
    );
    voided.causation_id = causation_id;
    ctx.emitter
        .send_order_event(OrderEventAny::FillVoided(voided));
}

fn emit_order_fill_confirmed(
    fill: &OrderFilled,
    trade: &PolymarketUserTrade,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let mut confirmed = build_order_fill_confirmed(
        fill,
        ts_event,
        ctx.clock.get_time_ns(),
        trade_fill_info(trade),
    );
    confirmed.causation_id = Some(fill.event_id);
    ctx.emitter
        .send_order_event(OrderEventAny::FillConfirmed(confirmed));
}

pub(crate) fn build_order_fill_confirmed(
    fill: &OrderFilled,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    info: Option<IndexMap<Ustr, Ustr>>,
) -> OrderFillConfirmed {
    OrderFillConfirmed::new(
        fill.trader_id,
        fill.strategy_id,
        fill.instrument_id,
        fill.client_order_id,
        fill.venue_order_id,
        fill.account_id,
        fill.trade_id,
        info,
        UUID4::new(),
        ts_event,
        ts_init,
        false,
    )
}

pub(crate) fn build_order_fill_voided(
    fill: &OrderFilled,
    venue_trade_id: &str,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    info: Option<IndexMap<Ustr, Ustr>>,
) -> OrderFillVoided {
    OrderFillVoided::new(
        fill.trader_id,
        fill.strategy_id,
        fill.instrument_id,
        fill.client_order_id,
        fill.venue_order_id,
        fill.account_id,
        Ustr::from(&format!("{venue_trade_id}-FAILED-{}", fill.client_order_id)),
        fill.trade_id,
        fill.last_qty,
        fill.commission,
        fill.order_side,
        fill.order_type,
        fill.last_px,
        fill.currency,
        fill.liquidity_side,
        fill.position_id,
        Some(Ustr::from("FAILED")),
        info,
        UUID4::new(),
        ts_event,
        ts_init,
        false,
        false,
    )
}

/// Flattens a user trade into a string map of venue fill metadata for `OrderFilled.info`.
///
/// Mirrors the v1 adapter, which attaches the full raw trade to each fill it generates. Scalar
/// fields map to their string form; nested fields (such as `maker_orders`) become their JSON text.
fn trade_fill_info(trade: &PolymarketUserTrade) -> Option<IndexMap<Ustr, Ustr>> {
    serialize_info(trade)
}

/// Emits an `OrderUpdated` raising the order quantity to the actual BUY fill, before the fill.
///
/// A Polymarket BUY is bounded by the USDC it spends, so a marketable fill below the limit price
/// returns more shares than the nominal quantity. The engine rejects a fill past the order
/// quantity, so the quantity is raised first. The price is left unchanged (`None`).
fn emit_buy_overfill_update(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    new_qty: Quantity,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let updated = OrderUpdated::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        new_qty,
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
        Some(venue_order_id),
        Some(ctx.account_id),
        None,
        None,
        None,
        false,
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Updated(updated));
}

/// Emits an order-only reconciliation update which cannot change strategy position.
fn emit_terminal_quantity_update(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    quantity: Quantity,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let updated = OrderUpdated::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        quantity,
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        true,
        Some(venue_order_id),
        Some(ctx.account_id),
        None,
        None,
        None,
        false,
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Updated(updated));
}

fn emit_order_canceled(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let canceled = OrderCanceled::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
        Some(venue_order_id),
        Some(ctx.account_id),
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Canceled(canceled));
}

fn emit_order_expired(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let expired = OrderExpired::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
        Some(venue_order_id),
        Some(ctx.account_id),
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Expired(expired));
}

fn emit_order_rejected(
    identity: &OrderIdentity,
    reason: &str,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    let rejected = OrderRejected::new(
        ctx.emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        identity.client_order_id,
        ctx.account_id,
        Ustr::from(reason),
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
        false,
    );
    ctx.emitter
        .send_order_event(OrderEventAny::Rejected(rejected));
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::ExecutionEvent;
    use nautilus_core::time::AtomicTime;
    use nautilus_model::{
        enums::{AccountType, OrderStatus},
        events::OrderEventAny,
        identifiers::{ClientOrderId, StrategyId, TraderId},
        types::Currency,
    };
    use rstest::rstest;

    use super::*;
    use crate::http::{
        models::GammaMarket,
        parse::{create_instrument_from_def, parse_gamma_market},
    };

    fn load<T: serde::de::DeserializeOwned>(filename: &str) -> T {
        let path = format!("test_data/{filename}");
        let content = std::fs::read_to_string(path).expect("Failed to read test data");
        serde_json::from_str(&content).expect("Failed to parse test data")
    }

    fn test_instrument() -> InstrumentAny {
        let market: GammaMarket = load("gamma_market.json");
        let defs = parse_gamma_market(&market).unwrap();
        create_instrument_from_def(&defs[0], UnixNanos::from(1_000_000_000u64)).unwrap()
    }

    fn test_emitter() -> ExecutionEventEmitter {
        ExecutionEventEmitter::new(
            nautilus_core::time::get_atomic_clock_realtime(),
            TraderId::from("TESTER-001"),
            AccountId::from("POLY-001"),
            AccountType::Cash,
            Some(Currency::pUSD()),
        )
    }

    fn assert_fill_confirmed(event: &ExecutionEvent) {
        let ExecutionEvent::Order(OrderEventAny::FillConfirmed(_)) = event else {
            panic!("expected fill-confirmed event, was {event:?}");
        };
    }

    #[rstest]
    fn test_build_ws_order_status_report() {
        let order: PolymarketUserOrder = load("ws_user_order_placement.json");
        let instrument = test_instrument();
        let ts_event = UnixNanos::from(1_000_000_000u64);
        let ts_init = UnixNanos::from(2_000_000_000u64);

        let report = build_ws_order_status_report(
            &order,
            &instrument,
            AccountId::from("POLY-001"),
            ts_event,
            ts_init,
        );

        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.order_type, OrderType::Limit);
        assert!(report.price.is_some());
        assert_eq!(report.ts_accepted, ts_event);
        assert_eq!(report.ts_init, ts_init);
    }

    #[rstest]
    fn test_build_ws_order_status_report_venue_cancel_maps_to_canceled() {
        let order: PolymarketUserOrder = load("ws_user_order_venue_cancel.json");
        let instrument = test_instrument();
        let ts_event = UnixNanos::from(1_000_000_000u64);
        let ts_init = UnixNanos::from(2_000_000_000u64);

        let report = build_ws_order_status_report(
            &order,
            &instrument,
            AccountId::from("POLY-001"),
            ts_event,
            ts_init,
        );

        assert_eq!(report.order_status, OrderStatus::Canceled);
    }

    #[rstest]
    fn test_build_ws_taker_fill_report() {
        let trade: PolymarketUserTrade = load("ws_user_trade.json");
        let instrument = test_instrument();
        let ts_event = UnixNanos::from(1_000_000_000u64);
        let ts_init = UnixNanos::from(2_000_000_000u64);

        let report = build_ws_taker_fill_report(
            &trade,
            &instrument,
            AccountId::from("POLY-001"),
            LiquiditySide::Taker,
            ts_event,
            ts_init,
        );

        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
        assert_eq!(report.trade_id.as_str(), trade.id);
        assert_eq!(report.ts_event, ts_event);
        assert_eq!(report.ts_init, ts_init);
    }

    #[rstest]
    fn test_trade_fill_info_flattens_raw_trade() {
        let trade: PolymarketUserTrade = load("ws_user_trade.json");

        let info = trade_fill_info(&trade).expect("info should be present");

        // Every raw trade field is captured (mirrors v1 info=msg.to_dict()).
        assert_eq!(info.len(), 21);
        assert_eq!(info[&Ustr::from("id")], Ustr::from("trade-0xabcdef1234"));
        assert_eq!(info[&Ustr::from("fee_rate_bps")], Ustr::from("0"));
        assert_eq!(
            info[&Ustr::from("transaction_hash")],
            Ustr::from("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")
        );
        // Numeric fields flatten to their string form.
        assert_eq!(info[&Ustr::from("bucket_index")], Ustr::from("1"));
        assert_eq!(info[&Ustr::from("size")], Ustr::from("25.0"));
        assert_eq!(
            info[&Ustr::from("taker_order_id")],
            Ustr::from("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12")
        );
        // The `type` serde-rename key is preserved.
        assert_eq!(info[&Ustr::from("type")], Ustr::from("TRADE"));
        // Nested fields become their JSON text.
        let maker_orders = info[&Ustr::from("maker_orders")].as_str();
        assert!(maker_orders.starts_with('['));
        assert!(maker_orders.contains("order_id"));

        let empty_hash_trade: PolymarketUserTrade = load("ws_user_trade_msg.json");
        let empty_hash_info =
            trade_fill_info(&empty_hash_trade).expect("empty hash info should be present");
        assert!(!empty_hash_info.contains_key(&Ustr::from("transaction_hash")));
    }

    #[rstest]
    fn test_dispatch_matched_trade_emits_fill_and_failed_trade_voids_it() {
        let mut trade: PolymarketUserTrade = load("ws_user_trade.json");
        trade.status = crate::common::enums::PolymarketTradeStatus::Matched;
        let instrument = test_instrument();
        let token_instruments = AtomicMap::new();
        token_instruments.insert(trade.asset_id, instrument.clone());
        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-MATCHED-FAILED"),
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        let matched = dispatch_user_message(&UserWsMessage::Trade(trade.clone()), &ctx, &mut state);
        let filled = match receiver.try_recv().unwrap() {
            ExecutionEvent::Order(OrderEventAny::Filled(event)) => event,
            other => panic!("expected matched fill, was {other:?}"),
        };
        for index in 0..10_001 {
            let unrelated = format!("unrelated-trade-{index}");
            state.processed_fills.insert(unrelated.clone());
            state.matched_fills.insert(unrelated, Vec::new());
        }
        trade.status = crate::common::enums::PolymarketTradeStatus::Failed;
        let failed = dispatch_user_message(&UserWsMessage::Trade(trade.clone()), &ctx, &mut state);
        let voided = match receiver.try_recv().unwrap() {
            ExecutionEvent::Order(OrderEventAny::FillVoided(event)) => event,
            other => panic!("expected failed fill correction, was {other:?}"),
        };

        let mut failed_first_state = WsDispatchState::default();
        let failed_first = dispatch_user_message(
            &UserWsMessage::Trade(trade.clone()),
            &ctx,
            &mut failed_first_state,
        );
        let dedup_key = format!("{}-{}", trade.id, trade.taker_order_id);
        trade.status = crate::common::enums::PolymarketTradeStatus::Matched;
        let matched_after_failure =
            dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut failed_first_state);

        assert!(matched.is_none());
        assert!(failed.is_some());
        assert!(failed_first.is_some());
        assert!(matched_after_failure.is_none());
        assert_eq!(voided.trade_id, filled.trade_id);
        assert_eq!(voided.voided_qty, filled.last_qty);
        assert_eq!(voided.commission_voided, filled.commission);
        assert_eq!(voided.last_px, filled.last_px);
        assert!(!voided.is_reopened);
        assert_eq!(voided.causation_id, Some(filled.event_id));
        assert_eq!(
            local_orders.cumulative_filled_for_test(&venue_order_id),
            Some(Quantity::zero(instrument.size_precision()))
        );
        assert!(failed_first_state.processed_fills.contains(&dedup_key));
        assert!(failed_first_state.is_voided_trade(&dedup_key));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_order_matched_normalizes_quantity_without_fill() {
        let order: PolymarketUserOrder = load("ws_user_order_matched.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-MATCHED"),
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
        );
        local_orders.record_fill(&venue_order_id, Quantity::new(99.995, 6));
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let clock = Box::leak(Box::new(AtomicTime::new(
            false,
            UnixNanos::from(2_000_000_000u64),
        )));

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock,
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();
        state.confirmed_trades.insert(confirmed_trade_key(
            "trade-0xfill1",
            VenueOrderId::from(order.id.as_str()),
        ));

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected quantity update");
        match event {
            ExecutionEvent::Order(OrderEventAny::Updated(updated)) => {
                assert_eq!(
                    updated.ts_event,
                    UnixNanos::from(1_703_875_201_000_000_000u64)
                );
                assert_eq!(updated.ts_init, UnixNanos::from(2_000_000_000u64));
                assert_eq!(updated.quantity, Quantity::new(99.995, 6));
                assert!(updated.reconciliation);
            }
            other => panic!("expected updated event, was {other:?}"),
        }
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_confirmed_trade_normalizes_pending_matched_quantity() {
        let mut order: PolymarketUserOrder = load("ws_user_order_matched.json");
        let mut trade: PolymarketUserTrade = load("ws_user_trade.json");
        let instrument = test_instrument();
        order.associate_trades = Some(vec![trade.id.clone()]);
        trade.size = "99.995".to_string();
        trade.price = order.price.clone();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());
        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-CONFIRMED-DUST"),
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);
        assert!(receiver.try_recv().is_err());

        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);

        let real_fill = receiver.try_recv().expect("expected confirmed venue fill");
        let normalized = receiver
            .try_recv()
            .expect("expected quantity normalization");

        match (real_fill, normalized) {
            (
                ExecutionEvent::Order(OrderEventAny::Filled(real)),
                ExecutionEvent::Order(OrderEventAny::Updated(updated)),
            ) => {
                assert_eq!(real.last_qty, Quantity::from("99.995"));
                assert_eq!(updated.quantity, Quantity::from("99.995"));
                assert!(updated.reconciliation);
            }
            other => panic!("expected fill then quantity update, was {other:?}"),
        }
        assert_fill_confirmed(
            &receiver
                .try_recv()
                .expect("expected durable fill confirmation"),
        );
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_cancel_reemitted_after_fill_for_canceled_order() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let trade: PolymarketUserTrade = load("ws_user_trade.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        // Register order as accepted with original qty=100
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-CANCEL"),
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        // Step 1: Dispatch cancel (simulates message A from the bug)
        dispatch_user_message(&UserWsMessage::Order(cancel_order), &ctx, &mut state);
        let cancel_event = receiver.try_recv().expect("Expected canceled event");
        match &cancel_event {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("Expected canceled event, was {other:?}"),
        }

        // Step 2: Dispatch trade fill (simulates trade arriving after cancel)
        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);

        // Should get: filled event, then re-emitted canceled event
        let fill_event = receiver.try_recv().expect("Expected filled event");
        match &fill_event {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.venue_order_id, venue_order_id);
            }
            other => panic!("Expected filled event, was {other:?}"),
        }

        let reemitted_cancel = receiver
            .try_recv()
            .expect("Expected re-emitted canceled event");

        match &reemitted_cancel {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("Expected canceled event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_cancel_not_reemitted_when_fill_completes_order() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let trade: PolymarketUserTrade = load("ws_user_trade.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        // Register with qty=25 matching the trade size so the fill completes the order
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-CANCEL-FULL"),
            Quantity::from("25"),
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        // Cancel then fill that completes the order
        dispatch_user_message(&UserWsMessage::Order(cancel_order), &ctx, &mut state);
        let _cancel = receiver.try_recv().expect("Expected canceled event");

        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);
        let _fill = receiver.try_recv().expect("Expected filled event");
        assert_fill_confirmed(
            &receiver
                .try_recv()
                .expect("expected durable fill confirmation"),
        );

        // Channel should be empty: no re-emitted cancel for a fully-filled order
        assert!(
            receiver.try_recv().is_err(),
            "Should not re-emit cancel when fill completes the order"
        );
    }

    #[rstest]
    fn test_cancel_saved_before_acceptance() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument.clone());

        // The local identity is prepared synchronously, then claimed from the signed
        // venue order id before the HTTP request, but is not yet accepted.
        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());
        let identity = OrderIdentity {
            client_order_id: ClientOrderId::from("O-CANCEL-IN-FLIGHT"),
            strategy_id: StrategyId::from("S-001"),
            instrument_id: instrument.id(),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        };
        local_orders.begin_submission(identity).unwrap();
        local_orders
            .claim_submission(venue_order_id, identity, Quantity::from("100"), None)
            .unwrap();
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        // Dispatch cancel while the HTTP response is still pending.
        dispatch_user_message(&UserWsMessage::Order(cancel_order), &ctx, &mut state);

        assert!(matches!(
            receiver.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Accepted(_))
        ));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            ExecutionEvent::Order(OrderEventAny::Canceled(_))
        ));
        assert!(state.terminal_cancel_reports.get(&venue_order_id).is_some());
    }

    /// Replays the exact 5-message WS sequence from issue #3797.
    ///
    /// Messages in arrival order:
    ///   (A) Order Canceled, size_matched=0
    ///   (B) Trade fill 1.219511 (maker side)
    ///   (C) Order Canceled, size_matched=1.219511
    ///   (D) Order Canceled, size_matched=2.560972 (capped to tracked)
    ///   (E) Trade fill 1.341461 (maker side)
    ///
    /// Without the fix, the order ends in PartiallyFilled after (E).
    /// With the fix, a re-emitted cancel after (E) restores Canceled.
    #[rstest]
    fn test_issue_3797_interleaved_cancel_fill_sequence() {
        use crate::common::{
            enums::{
                PolymarketEventType, PolymarketLiquiditySide, PolymarketOrderSide,
                PolymarketOrderStatus, PolymarketOrderType, PolymarketOutcome,
                PolymarketTradeStatus,
            },
            models::PolymarketMakerOrder,
        };

        let instrument = test_instrument();
        let asset_id = instrument.id().symbol.inner();

        let order_id =
            "0xe743f6c823ecdfa9ddaaf08673b2441d15a38d89e14dcb25b3b70c284be4f6ad".to_string();
        let venue_order_id = VenueOrderId::from(order_id.as_str());

        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-3797"),
            Quantity::from("20"),
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xabc",
            user_api_key: "xxx",
        };
        let mut state = WsDispatchState::default();

        // Helper to build order updates
        let make_order =
            |size_matched: &str, ts: &str, event_type: PolymarketEventType| PolymarketUserOrder {
                asset_id,
                associate_trades: None,
                created_at: "1775074735".to_string(),
                expiration: Some("0".to_string()),
                id: order_id.clone(),
                maker_address: Ustr::from("0xabc"),
                market: Ustr::from("0x4134"),
                order_owner: Ustr::from("xxx"),
                order_type: PolymarketOrderType::GTC,
                original_size: "20".to_string(),
                outcome: PolymarketOutcome::yes(),
                owner: Ustr::from("xxx"),
                price: "0.18".to_string(),
                side: PolymarketOrderSide::Buy,
                size_matched: size_matched.to_string(),
                status: PolymarketOrderStatus::Canceled,
                timestamp: ts.to_string(),
                event_type,
            };

        // Helper to build maker trades
        let make_trade = |trade_id: &str, matched_amount: f64, ts: &str| PolymarketUserTrade {
            asset_id,
            bucket_index: 0,
            fee_rate_bps: "1000".to_string(),
            id: trade_id.to_string(),
            last_update: "1775074738".to_string(),
            maker_address: Ustr::from("0xother"),
            maker_orders: vec![PolymarketMakerOrder {
                asset_id,
                maker_address: "0xabc".to_string(),
                matched_amount: Decimal::from_f64_retain(matched_amount).unwrap_or(Decimal::ZERO),
                order_id: order_id.clone(),
                outcome: PolymarketOutcome::yes(),
                owner: "xxx".to_string(),
                price: Decimal::from_f64_retain(0.18).unwrap_or(Decimal::ZERO),
                side: None,
            }],
            market: Ustr::from("0x4134"),
            match_time: "1775074735".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("other-owner"),
            price: "0.82".to_string(),
            side: PolymarketOrderSide::Sell,
            size: "1.219511".to_string(),
            status: PolymarketTradeStatus::Confirmed,
            taker_order_id: "0xtaker01".to_string(),
            timestamp: ts.to_string(),
            trade_owner: Ustr::from("other-owner"),
            transaction_hash: None,
            trader_side: PolymarketLiquiditySide::Maker,
            event_type: PolymarketEventType::Trade,
        };

        // (A) Cancel with size_matched=0
        let msg_a = make_order("0", "1775074738031", PolymarketEventType::Cancellation);
        dispatch_user_message(&UserWsMessage::Order(msg_a), &ctx, &mut state);

        let evt = receiver.try_recv().expect("(A) canceled event");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(A) expected canceled event, was {other:?}"),
        }

        // (B) Trade fill 1.219511
        let msg_b = make_trade("trade-b", 1.219511, "1775074738032");
        dispatch_user_message(&UserWsMessage::Trade(msg_b), &ctx, &mut state);

        let evt = receiver.try_recv().expect("(B) filled event");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.venue_order_id, venue_order_id);
            }
            other => panic!("(B) expected filled event, was {other:?}"),
        }
        // Re-emitted cancel after fill (B)
        let evt = receiver.try_recv().expect("(B) re-emitted cancel");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(B) expected re-emitted cancel, was {other:?}"),
        }
        assert_fill_confirmed(
            &receiver
                .try_recv()
                .expect("(B) expected durable fill confirmation"),
        );

        // (C) Cancel with size_matched=1.219511
        let msg_c = make_order("1.219511", "1775074738034", PolymarketEventType::Update);
        dispatch_user_message(&UserWsMessage::Order(msg_c), &ctx, &mut state);

        let evt = receiver.try_recv().expect("(C) canceled event");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(C) expected canceled event, was {other:?}"),
        }

        // (D) Cancel with size_matched=2.560972 (capped to tracked 1.219511)
        let msg_d = make_order("2.560972", "1775074738038", PolymarketEventType::Update);
        dispatch_user_message(&UserWsMessage::Order(msg_d), &ctx, &mut state);

        let evt = receiver.try_recv().expect("(D) canceled event");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(D) expected canceled event, was {other:?}"),
        }

        // (E) Trade fill 1.341461
        let msg_e = make_trade("trade-e", 1.341461, "1775074738036");
        dispatch_user_message(&UserWsMessage::Trade(msg_e), &ctx, &mut state);

        let evt = receiver.try_recv().expect("(E) filled event");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.venue_order_id, venue_order_id);
            }
            other => panic!("(E) expected filled event, was {other:?}"),
        }

        // The fix: re-emitted cancel after (E) restores terminal state
        let evt = receiver.try_recv().expect("(E) re-emitted cancel");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(E) expected re-emitted cancel, was {other:?}"),
        }
        assert_fill_confirmed(
            &receiver
                .try_recv()
                .expect("(E) expected durable fill confirmation"),
        );

        // No more events
        assert!(
            receiver.try_recv().is_err(),
            "No further events expected after the sequence"
        );
    }

    #[rstest]
    fn test_dispatch_taker_fill_snaps_overfill_to_submitted_qty() {
        // Reproduces the V2 market-BUY scenario that motivated the dust-snap
        // fix: SDK truncates the registered qty to USDC scale, but the
        // on-chain fill comes back at full precision and exceeds submitted
        // by microshares. Without the snap the engine rejects as overfill.
        use crate::common::enums::{
            PolymarketEventType, PolymarketOrderSide, PolymarketOutcome, PolymarketTradeStatus,
        };

        let instrument = test_instrument();
        let asset_id = instrument.id().symbol.inner();
        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from("0xtaker-overfill");
        // Submitted qty truncated to USDC scale.
        let submitted = Quantity::new(714.285710, instrument.size_precision());
        local_orders.register(
            venue_order_id,
            ClientOrderId::from("O-OVERFILL"),
            submitted,
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        let trade = PolymarketUserTrade {
            asset_id,
            bucket_index: 0,
            fee_rate_bps: "0".to_string(),
            id: "trade-overfill".to_string(),
            last_update: "1700000001".to_string(),
            maker_address: Ustr::from("0xmaker"),
            maker_orders: vec![],
            market: Ustr::from("0xmarket"),
            match_time: "1700000000".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            price: "0.014".to_string(),
            side: PolymarketOrderSide::Buy,
            // Fill exceeds submitted_qty by 4 ulps at size_precision=6,
            // matching the production drift observed during smoke tests.
            size: "714.285714".to_string(),
            status: PolymarketTradeStatus::Confirmed,
            taker_order_id: venue_order_id.as_str().to_string(),
            timestamp: "1700000000000".to_string(),
            trade_owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            transaction_hash: None,
            trader_side: PolymarketLiquiditySide::Taker,
            event_type: PolymarketEventType::Trade,
        };

        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);

        // The dispatcher must record the snapped quantity in the coordinator so
        // any subsequent ORDER MATCHED with size_matched > submitted_qty is
        // capped to it. record_fill happens before the FillReport is sent.
        let cumulative = local_orders
            .cumulative_filled_for_test(&venue_order_id)
            .expect("order must be registered");
        assert_eq!(cumulative, submitted);

        // The emitted OrderFilled must carry the snapped qty so the engine
        // does not reject it as an overfill.
        let event = receiver.try_recv().expect("expected a filled event");
        match event {
            ExecutionEvent::Order(OrderEventAny::Filled(filled)) => {
                assert_eq!(
                    filled.last_qty, submitted,
                    "filled qty must be snapped to submitted",
                );
                assert_eq!(filled.venue_order_id, venue_order_id);
            }
            other => panic!("expected filled event, was {other:?}"),
        }
    }

    #[rstest]
    #[case(
        TimeInForce::Ioc,
        OrderType::Market,
        OrderSide::Buy,
        "5.202910",
        "5.202897",
        false,
        true
    )]
    #[case(
        TimeInForce::Fok,
        OrderType::Limit,
        OrderSide::Buy,
        "5.202910",
        "5.202897",
        true,
        false
    )]
    #[case(
        TimeInForce::Ioc,
        OrderType::Limit,
        OrderSide::Buy,
        "30",
        "20",
        false,
        true
    )]
    #[case(
        TimeInForce::Ioc,
        OrderType::Market,
        OrderSide::Sell,
        "5.202910",
        "5.202897",
        false,
        true
    )]
    #[case(
        TimeInForce::Gtc,
        OrderType::Limit,
        OrderSide::Buy,
        "5.202910",
        "5.202897",
        false,
        false
    )]
    fn test_taker_terminal_status_on_trade_confirm(
        #[case] time_in_force: TimeInForce,
        #[case] order_type: OrderType,
        #[case] order_side: OrderSide,
        #[case] submitted_qty: &str,
        #[case] fill_qty: &str,
        #[case] expect_normalization: bool,
        #[case] expect_cancel: bool,
    ) {
        // Takers receive no MATCHED order update. FOK is atomic, so a dust
        // difference normalizes the registered quantity. IOC maps to FAK, so
        // a positive remainder closes as Canceled without changing the fill.
        use crate::common::enums::{
            PolymarketEventType, PolymarketOrderSide, PolymarketOutcome, PolymarketTradeStatus,
        };

        let instrument = test_instrument();
        let asset_id = instrument.id().symbol.inner();
        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from("0xtaker-one-shot-dust");
        let submitted = Quantity::from_decimal_dp(
            Decimal::from_str_exact(submitted_qty).unwrap(),
            instrument.size_precision(),
        )
        .unwrap();
        let identity = OrderIdentity {
            client_order_id: ClientOrderId::from("O-ONE-SHOT"),
            strategy_id: StrategyId::from("S-001"),
            instrument_id: instrument.id(),
            order_side,
            order_type,
            time_in_force,
        };
        local_orders
            .restore_order(
                venue_order_id,
                identity,
                submitted,
                Quantity::zero(instrument.size_precision()),
                None,
            )
            .expect("test order restoration should succeed");
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        let trade = PolymarketUserTrade {
            asset_id,
            bucket_index: 0,
            fee_rate_bps: "0".to_string(),
            id: "trade-one-shot-dust".to_string(),
            last_update: "1700000001".to_string(),
            maker_address: Ustr::from("0xmaker"),
            maker_orders: vec![],
            market: Ustr::from("0xmarket"),
            match_time: "1700000000".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            price: "0.963".to_string(),
            side: if order_side == OrderSide::Buy {
                PolymarketOrderSide::Buy
            } else {
                PolymarketOrderSide::Sell
            },
            size: fill_qty.to_string(),
            status: PolymarketTradeStatus::Confirmed,
            taker_order_id: venue_order_id.as_str().to_string(),
            timestamp: "1700000000000".to_string(),
            trade_owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            transaction_hash: None,
            trader_side: PolymarketLiquiditySide::Taker,
            event_type: PolymarketEventType::Trade,
        };

        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected the venue fill event");
        match event {
            ExecutionEvent::Order(OrderEventAny::Filled(filled)) => {
                assert_eq!(
                    filled.last_qty,
                    Quantity::from_decimal_dp(
                        Decimal::from_str_exact(fill_qty).unwrap(),
                        instrument.size_precision(),
                    )
                    .unwrap(),
                );
            }
            other => panic!("expected filled event, was {other:?}"),
        }

        if expect_normalization {
            let event = receiver.try_recv().expect("expected quantity update");
            match event {
                ExecutionEvent::Order(OrderEventAny::Updated(updated)) => {
                    assert_eq!(
                        updated.quantity,
                        Quantity::new(5.202897, instrument.size_precision()),
                    );
                    assert_eq!(updated.venue_order_id, Some(venue_order_id));
                    assert!(updated.reconciliation);
                }
                other => panic!("expected updated event, was {other:?}"),
            }
            assert_eq!(local_orders.identity(&venue_order_id), Some(identity));
        } else if expect_cancel {
            let event = receiver.try_recv().expect("expected IOC cancellation");
            match event {
                ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) => {
                    assert_eq!(canceled.venue_order_id, Some(venue_order_id));
                }
                other => panic!("expected canceled event, was {other:?}"),
            }
            assert_eq!(local_orders.identity(&venue_order_id), Some(identity));
        } else {
            assert!(
                local_orders
                    .cumulative_filled_for_test(&venue_order_id)
                    .is_some(),
                "ineligible order must stay tracked with open leaves",
            );
        }
        assert_fill_confirmed(
            &receiver
                .try_recv()
                .expect("expected durable fill confirmation"),
        );
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_taker_fill_gross_overfill_raises_qty_then_fills() {
        // A marketable BUY filled below its limit returns more shares than the nominal qty (a
        // gross overfill, beyond the dust band). The dispatcher must raise the order qty via
        // OrderUpdated before the OrderFilled, or the engine drops the fill as an overfill.
        use crate::common::enums::{
            PolymarketEventType, PolymarketOrderSide, PolymarketOutcome, PolymarketTradeStatus,
        };

        let instrument = test_instrument();
        let asset_id = instrument.id().symbol.inner();
        let size_precision = instrument.size_precision();
        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let venue_order_id = VenueOrderId::from("0xtaker-gross-overfill");
        let submitted = Quantity::new(30.0, size_precision);
        local_orders.register(
            venue_order_id,
            ClientOrderId::from(venue_order_id.as_str()),
            submitted,
            OrderSide::Buy,
            instrument.id(),
        );
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        // 33.846152 shares against a nominal 30: a marketable fill below the limit price.
        let trade = PolymarketUserTrade {
            asset_id,
            bucket_index: 0,
            fee_rate_bps: "0".to_string(),
            id: "trade-gross-overfill".to_string(),
            last_update: "1700000001".to_string(),
            maker_address: Ustr::from("0xmaker"),
            maker_orders: vec![],
            market: Ustr::from("0xmarket"),
            match_time: "1700000000".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            price: "0.014".to_string(),
            side: PolymarketOrderSide::Buy,
            size: "33.846152".to_string(),
            status: PolymarketTradeStatus::Confirmed,
            taker_order_id: venue_order_id.as_str().to_string(),
            timestamp: "1700000000000".to_string(),
            trade_owner: Ustr::from("00000000-0000-0000-0000-000000000001"),
            transaction_hash: None,
            trader_side: PolymarketLiquiditySide::Taker,
            event_type: PolymarketEventType::Trade,
        };

        dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut state);

        let expected_qty = Quantity::new(33.846152, size_precision);

        // The raise must precede the fill so the engine accepts the larger quantity.
        match receiver.try_recv().expect("expected an updated event") {
            ExecutionEvent::Order(OrderEventAny::Updated(updated)) => {
                assert_eq!(updated.quantity, expected_qty);
                assert_eq!(updated.venue_order_id, Some(venue_order_id));
            }
            other => panic!("expected updated event raising qty to the fill, was {other:?}"),
        }

        match receiver.try_recv().expect("expected a filled event") {
            ExecutionEvent::Order(OrderEventAny::Filled(filled)) => {
                assert_eq!(filled.last_qty, expected_qty);
                assert_eq!(filled.venue_order_id, venue_order_id);
            }
            other => panic!("expected filled event, was {other:?}"),
        }
    }

    // Unmatched -> Rejected (placement never became live); CanceledMarketResolved -> Expired
    // (market settled). Both are tracked own-order terminal states emitted as order events.
    #[rstest]
    #[case(crate::common::enums::PolymarketOrderStatus::Unmatched, "Rejected")]
    #[case(
        crate::common::enums::PolymarketOrderStatus::CanceledMarketResolved,
        "Expired"
    )]
    fn test_dispatch_order_terminal_status_emits_event(
        #[case] status: crate::common::enums::PolymarketOrderStatus,
        #[case] expected: &str,
    ) {
        use crate::common::enums::{
            PolymarketEventType, PolymarketOrderSide, PolymarketOrderType, PolymarketOutcome,
        };

        let instrument = test_instrument();
        let asset_id = instrument.id().symbol.inner();
        let order_id = "0xterminal-order".to_string();
        let venue_order_id = VenueOrderId::from(order_id.as_str());

        let token_instruments = AtomicMap::new();
        token_instruments.insert(asset_id, instrument.clone());

        let local_orders = LocalOrderCoordinator::new();
        let identity = OrderIdentity {
            client_order_id: ClientOrderId::from("O-TERMINAL"),
            strategy_id: StrategyId::from("S-TEST"),
            instrument_id: instrument.id(),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Fok,
        };
        local_orders
            .restore_order(
                venue_order_id,
                identity,
                Quantity::from("10"),
                Quantity::zero(instrument.size_precision()),
                None,
            )
            .expect("test order restoration should succeed");
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            local_orders: &local_orders,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xabc",
            user_api_key: "xxx",
        };
        let mut state = WsDispatchState::default();

        let order = PolymarketUserOrder {
            asset_id,
            associate_trades: None,
            created_at: "1775074735".to_string(),
            expiration: Some("0".to_string()),
            id: order_id,
            maker_address: Ustr::from("0xabc"),
            market: Ustr::from("0x4134"),
            order_owner: Ustr::from("xxx"),
            order_type: PolymarketOrderType::FOK,
            original_size: "10".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: Ustr::from("xxx"),
            price: "0.50".to_string(),
            side: PolymarketOrderSide::Buy,
            size_matched: "0".to_string(),
            status,
            timestamp: "1775074738031".to_string(),
            event_type: PolymarketEventType::Placement,
        };

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected terminal order event");
        match event {
            ExecutionEvent::Order(order_event) => {
                assert!(
                    format!("{order_event:?}").starts_with(expected),
                    "expected {expected}, was {order_event:?}"
                );
                assert_eq!(
                    order_event.client_order_id(),
                    ClientOrderId::from("O-TERMINAL")
                );
            }
            other => panic!("expected order event, was {other:?}"),
        }
    }
}
