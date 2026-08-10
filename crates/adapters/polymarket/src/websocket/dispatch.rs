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
//! submit (`OrderIdentityRegistry`). Order-channel messages drive lifecycle events; trade-channel
//! messages drive fills, and acceptance is synthesized before a fill or cancel that races ahead.
//! Messages are emitted once the order is known (accepted, or with a submit in flight), otherwise
//! buffered until acceptance. Reports are reserved for the `generate_*` query and reconciliation
//! methods. Trade fills are emitted at `MATCHED`, finalized with `OrderFillConfirmed` at
//! `CONFIRMED`, and reversed with `OrderFillVoided` if the trade reaches `FAILED`.

use std::str::FromStr;

use indexmap::IndexMap;
use nautilus_common::cache::fifo::FifoCacheMap;
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
        enums::{
            PolymarketLiquiditySide, PolymarketOrderSide, PolymarketOrderStatus,
            PolymarketOrderType, PolymarketTradeStatus,
        },
        models::PolymarketMakerOrder,
    },
    execution::{
        get_pusd_currency,
        identity::{OrderIdentity, OrderIdentityRegistry},
        order_fill_tracker::{
            FillCorrectionMetadata, OrderFillTrackerMap, PreparedTradeFill, ReadyTradeFill,
            TradeFillApplication,
        },
        parse::{
            ReportParseError, build_maker_fill_report, compute_commission, determine_order_side,
            instrument_fee_exponent, instrument_taker_fee, parse_fill_values, parse_liquidity_side,
        },
        pending::PendingSubmitTracker,
    },
};

/// Signal returned when a finalized trade requires an async account refresh.
#[derive(Debug)]
pub(crate) struct AccountRefreshRequest;

/// Mutable state retained across user WebSocket stream generations.
#[derive(Debug, Default)]
pub(crate) struct WsDispatchState {
    pending_terminal_orders: FifoCacheMap<VenueOrderId, PendingTerminalOrder, 10_000>,
    /// Cancel reports saved for orders known to be terminal at the venue.
    /// Re-emitted after a fill to restore terminal state when fills race
    /// ahead of (or arrive after) cancel messages.
    terminal_cancel_reports: FifoCacheMap<VenueOrderId, OrderStatusReport, 10_000>,
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
    pub fill_tracker: &'a OrderFillTrackerMap,
    pub pending_submits: &'a PendingSubmitTracker,
    pub order_identities: &'a OrderIdentityRegistry,
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
    let mut report =
        build_ws_order_status_report(order, instrument, ctx.account_id, ts_event, ts_init);
    let local_client_order_id = ctx.pending_submits.client_order_id(&venue_order_id);
    let mut is_accepted = ctx.fill_tracker.contains(&venue_order_id);
    report.client_order_id = local_client_order_id;

    // A known own order (submit in flight) self-registers on its first WS update
    if local_client_order_id.is_some()
        && !is_accepted
        && report.order_status != OrderStatus::Rejected
    {
        is_accepted = true;
        ctx.fill_tracker.register_reconciled_order(
            venue_order_id,
            report.quantity,
            Quantity::zero(report.quantity.precision),
            report.order_side,
        );
    }

    // Order updates can race ahead of trade messages, so cap filled_qty
    // to what the fill tracker has recorded to prevent duplicate inferred fills
    if let Some(tracked_filled) = ctx.fill_tracker.get_cumulative_filled(&venue_order_id)
        && report.filled_qty > tracked_filled
    {
        log::debug!(
            "Capping filled_qty for {venue_order_id} from {} to {} (awaiting trade messages)",
            report.filled_qty,
            tracked_filled,
        );
        report.filled_qty = tracked_filled;
    }

    // Track cancel reports so we can re-emit them after late-arriving fills.
    // Saved regardless of acceptance state so that cancels arriving during
    // the HTTP round-trip are available once the order is later accepted.
    if report.order_status == OrderStatus::Canceled {
        state
            .terminal_cancel_reports
            .insert(venue_order_id, report.clone());
    }

    // Tracked own orders route through order events; externally-managed orders
    // (no captured identity) buffer until accepted or fall back to reports.
    let identity = ctx.order_identities.get(&venue_order_id);

    if is_accepted || local_client_order_id.is_some() {
        match identity {
            Some(identity) => emit_tracked_order_status(&report, &identity, ts_event, ctx),
            None => ctx.emitter.send_order_status_report(report),
        }
    } else {
        match ctx.order_identities.get(&venue_order_id) {
            Some(identity) => emit_tracked_order_status(&report, &identity, ts_event, ctx),
            None => ctx.emitter.send_order_status_report(report),
        }
    }

    if order.status == PolymarketOrderStatus::Matched
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
            ctx.fill_tracker
                .are_venue_trades_confirmed(&pending.trade_ids)
        });

    if !is_ready {
        return;
    }

    let Some(pending) = state.pending_terminal_orders.remove(&venue_order_id) else {
        return;
    };

    let Some(identity) = ctx.order_identities.get(&venue_order_id) else {
        log::warn!("Cannot normalize terminal order {venue_order_id} without a local identity");
        return;
    };

    if let Some(quantity) = ctx
        .fill_tracker
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

    let Some(identity) = ctx.order_identities.get(&venue_order_id) else {
        return;
    };

    if identity.requires_terminal_quantity_normalization() {
        if let Some(quantity) = ctx
            .fill_tracker
            .check_terminal_quantity_normalization(&venue_order_id)
        {
            emit_terminal_quantity_update(&identity, venue_order_id, quantity, ts_event, ctx);
        }
        return;
    }

    if identity.time_in_force == TimeInForce::Ioc
        && let Some(remainder) = ctx
            .fill_tracker
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
        void_failed_trade(trade, &dedup_key, ctx);
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
    let confirmed_transition = dispatch_trade_fills(trade, &dedup_key, is_confirmed, ctx, state);
    if !confirmed_transition {
        return None;
    }
    emit_confirmed_trade_effects(trade, ctx, state);
    Some(AccountRefreshRequest)
}

fn void_failed_trade(trade: &PolymarketUserTrade, dedup_key: &str, ctx: &WsDispatchContext<'_>) {
    let venue_order_ids = owned_trade_venue_order_ids(trade, ctx);
    ctx.fill_tracker
        .void_trade_atomically(dedup_key, &trade.id, &venue_order_ids, |fills| {
            for fill in fills {
                emit_order_fill_voided(&fill, trade, Some(fill.event_id), ctx);
            }
        });
}

fn owned_trade_venue_order_ids(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
) -> Vec<VenueOrderId> {
    if trade.trader_side == PolymarketLiquiditySide::Maker {
        trade
            .maker_orders
            .iter()
            .filter(|order| is_user_maker_order(order, ctx))
            .map(|order| VenueOrderId::from(order.order_id.as_str()))
            .collect()
    } else {
        vec![VenueOrderId::from(trade.taker_order_id.as_str())]
    }
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
    dedup_key: &str,
    is_confirmed: bool,
    ctx: &WsDispatchContext<'_>,
    state: &WsDispatchState,
) -> bool {
    let reports = if trade.trader_side == PolymarketLiquiditySide::Maker {
        prepare_maker_fill_reports(trade, ctx)
    } else {
        prepare_taker_fill_report(trade, ctx).map(|report| vec![report])
    };
    let Some(reports) = reports else {
        return false;
    };

    let fill_info = trade_fill_info(trade);
    let prepared = reports
        .into_iter()
        .map(
            |report| match ctx.order_identities.get(&report.venue_order_id) {
                Some(identity) => PreparedTradeFill::Tracked {
                    event: Box::new(build_order_filled(
                        &identity,
                        &report,
                        fill_info.clone(),
                        ctx,
                    )),
                    report,
                    identity,
                },
                None => PreparedTradeFill::Anonymous(report),
            },
        )
        .collect();
    let correction = FillCorrectionMetadata {
        correction_key: dedup_key.to_string(),
        venue_trade_id: trade.id.clone(),
        is_confirmed,
    };
    let (ready, confirmed_fills, confirmed_transition) = match ctx
        .fill_tracker
        .apply_trade_fills_atomically(prepared, correction)
    {
        Ok(TradeFillApplication::AlreadyProcessed) => {
            log::debug!("Duplicate fill skipped: {dedup_key}");
            return false;
        }
        Ok(TradeFillApplication::Deferred) => {
            log::warn!(
                "Deferring settlement-pending trade {} until every registered fill leg has reversible local identity",
                trade.id
            );
            return false;
        }
        Ok(TradeFillApplication::Confirmed(fills)) => (Vec::new(), fills, true),
        Ok(TradeFillApplication::Ready(ready)) => (ready, Vec::new(), is_confirmed),
        Err(error) => {
            log::error!("Rejecting invalid fill transaction {}: {error}", trade.id);
            return false;
        }
    };

    let mut newly_confirmed_fills = Vec::new();
    let mut ready_venue_order_ids = Vec::new();
    for fill in ready {
        let venue_order_id = match fill {
            ReadyTradeFill::Tracked {
                identity,
                event,
                quantity_update,
            } => {
                ensure_accepted(&identity, event.venue_order_id, event.ts_event, ctx);
                if let Some(new_qty) = quantity_update {
                    emit_buy_overfill_update(
                        &identity,
                        event.venue_order_id,
                        new_qty,
                        event.ts_event,
                        ctx,
                    );
                }
                if is_confirmed {
                    newly_confirmed_fills.push((*event).clone());
                }
                let venue_order_id = event.venue_order_id;
                ctx.emitter.send_order_event(OrderEventAny::Filled(*event));
                venue_order_id
            }
            ReadyTradeFill::Anonymous(report) => {
                let venue_order_id = report.venue_order_id;
                ctx.emitter.send_fill_report(report);
                venue_order_id
            }
        };
        ready_venue_order_ids.push(venue_order_id);
    }

    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    for fill in confirmed_fills.into_iter().chain(newly_confirmed_fills) {
        let confirmation = OrderFillConfirmed::new(
            &fill,
            Ustr::from(dedup_key),
            Ustr::from(trade.id.as_str()),
            UUID4::new(),
            ts_event,
            ctx.clock.get_time_ns(),
        );
        ctx.emitter
            .send_order_event(OrderEventAny::FillConfirmed(confirmation));
    }

    for venue_order_id in ready_venue_order_ids {
        reemit_terminal_cancel(venue_order_id, state, ctx);
    }

    confirmed_transition
}

fn emit_confirmed_trade_effects(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
    state: &mut WsDispatchState,
) {
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    if trade.trader_side == PolymarketLiquiditySide::Maker {
        for order in trade
            .maker_orders
            .iter()
            .filter(|order| is_user_maker_order(order, ctx))
        {
            emit_quantity_normalization_if_ready(
                VenueOrderId::from(order.order_id.as_str()),
                ctx,
                state,
            );
        }
    } else {
        emit_quantity_normalization_if_ready(
            VenueOrderId::from(trade.taker_order_id.as_str()),
            ctx,
            state,
        );
        emit_taker_terminal_status(trade, ctx, ts_event);
    }
}

fn prepare_maker_fill_reports(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
) -> Option<Vec<FillReport>> {
    let user_orders: Vec<_> = trade
        .maker_orders
        .iter()
        .filter(|order| is_user_maker_order(order, ctx))
        .collect();

    if user_orders.is_empty() {
        log::warn!("No matching maker orders for user in trade: {}", trade.id);
        return None;
    }

    let instruments = ctx.token_instruments.load();
    let liquidity_side = parse_liquidity_side(trade.trader_side);
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let ts_init = ctx.clock.get_time_ns();
    let mut reports = Vec::with_capacity(user_orders.len());

    for mo in user_orders {
        let asset_id = Ustr::from(mo.asset_id.as_str());
        let instrument = match instruments.get(&asset_id) {
            Some(i) => i,
            None => {
                log::warn!("Unknown asset_id in maker order: {asset_id}");
                return None;
            }
        };
        let report = match build_maker_fill_report(
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
        ) {
            Ok(report) => report,
            Err(e) => {
                log::warn!(
                    "Skipping invalid live maker fill for trade {}: {e}",
                    trade.id
                );
                return None;
            }
        };
        reports.push(report);
    }

    for report in &mut reports {
        let maker_venue_order_id = report.venue_order_id;
        report.client_order_id = ctx.pending_submits.client_order_id(&maker_venue_order_id);
        report.last_qty = ctx
            .fill_tracker
            .snap_fill_qty(&maker_venue_order_id, report.last_qty);
    }
    Some(reports)
}

fn is_user_maker_order(order: &PolymarketMakerOrder, ctx: &WsDispatchContext<'_>) -> bool {
    order.is_owned_by(ctx.user_address, ctx.user_api_key)
}

fn prepare_taker_fill_report(
    trade: &PolymarketUserTrade,
    ctx: &WsDispatchContext<'_>,
) -> Option<FillReport> {
    let instruments = ctx.token_instruments.load();
    let instrument = match instruments.get(&trade.asset_id) {
        Some(i) => i,
        None => {
            log::warn!("Unknown asset_id in trade: {}", trade.asset_id);
            return None;
        }
    };

    let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
    let liquidity_side = parse_liquidity_side(trade.trader_side);
    let ts_event = parse_timestamp_ms(&trade.timestamp).unwrap_or_else(|_| ctx.clock.get_time_ns());
    let ts_init = ctx.clock.get_time_ns();

    let mut report = match build_ws_taker_fill_report(
        trade,
        instrument,
        ctx.account_id,
        liquidity_side,
        ts_event,
        ts_init,
    ) {
        Ok(report) => report,
        Err(e) => {
            log::warn!(
                "Skipping invalid live taker fill for trade {}: {e}",
                trade.id
            );
            return None;
        }
    };
    report.client_order_id = ctx.pending_submits.client_order_id(&venue_order_id);
    report.last_qty = ctx
        .fill_tracker
        .snap_fill_qty(&venue_order_id, report.last_qty);
    Some(report)
}

/// Re-emits a saved cancel report after a fill to restore terminal state.
///
/// When fills race ahead of (or arrive after) cancel messages, the order can
/// get stuck in `PartiallyFilled`. This re-emission ensures the execution
/// engine transitions the order back to `Canceled`.
///
/// Skips re-emission when the fill tracker shows the order is fully filled,
/// because `Filled` is already terminal and a spurious cancel would fail
/// the `Filled -> Canceled` state transition.
fn reemit_terminal_cancel(
    venue_order_id: VenueOrderId,
    state: &WsDispatchState,
    ctx: &WsDispatchContext<'_>,
) {
    if ctx.fill_tracker.is_fully_filled(&venue_order_id) {
        return;
    }

    if let Some(cancel_report) = state.terminal_cancel_reports.get(&venue_order_id) {
        log::debug!("Re-emitting cancel for {venue_order_id} after fill to restore terminal state");
        match ctx.order_identities.get(&venue_order_id) {
            Some(identity) => {
                emit_order_canceled(&identity, venue_order_id, cancel_report.ts_last, ctx);
            }
            None => ctx.emitter.send_order_status_report(cancel_report.clone()),
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
    let price_dec = Decimal::from_str(&order.price).unwrap_or_default();
    let quantity = Decimal::from_str(&order.original_size)
        .ok()
        .map(|size| original_size_to_shares(size, price_dec, order.side, order.order_type))
        .and_then(|d| Quantity::from_decimal_dp(d, size_precision).ok())
        .unwrap_or_else(|| Quantity::zero(size_precision));
    let filled_qty = Decimal::from_str(&order.size_matched)
        .ok()
        .and_then(|d| Quantity::from_decimal_dp(d, size_precision).ok())
        .unwrap_or_else(|| Quantity::zero(size_precision));
    let price = Price::from_decimal_dp(price_dec, price_precision)
        .unwrap_or_else(|_| Price::zero(price_precision));

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

/// Converts a venue-reported `original_size` on a user-channel order message into shares.
///
/// The venue echoes the signed `makerAmount`, which for a BUY is the pUSD budget rather than a
/// share count (see `compute_maker_taker_amounts`). Dividing by the order price recovers the
/// signed `takerAmount`, which is the share quantity the client submitted.
///
/// This is confirmed for the market order types (`FAK` and `FOK`), where a BUY at 0.01 for 100
/// shares reports `1`. A SELL signs shares as its maker amount and needs no conversion. Resting
/// types pass through unchanged: their denomination is unconfirmed, and converting a
/// share-denominated size would misreport every externally-managed resting order.
fn original_size_to_shares(
    original_size: Decimal,
    price: Decimal,
    side: PolymarketOrderSide,
    order_type: PolymarketOrderType,
) -> Decimal {
    if side != PolymarketOrderSide::Buy
        || !matches!(
            order_type,
            PolymarketOrderType::FAK | PolymarketOrderType::FOK
        )
    {
        return original_size;
    }

    if price <= Decimal::ZERO {
        log::warn!(
            "Cannot convert {order_type} BUY size {original_size} pUSD to shares \
             without a positive price, reporting the venue amount"
        );
        return original_size;
    }

    original_size / price
}

fn build_ws_taker_fill_report(
    trade: &PolymarketUserTrade,
    instrument: &InstrumentAny,
    account_id: AccountId,
    liquidity_side: LiquiditySide,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Result<FillReport, ReportParseError> {
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
    let size_dec = Decimal::from_str(&trade.size).map_err(|_| ReportParseError::Quantity)?;
    let price_dec = Decimal::from_str(&trade.price).map_err(|_| ReportParseError::Price)?;
    let (last_qty, last_px) =
        parse_fill_values(size_dec, price_dec, size_precision, price_precision)?;

    let fee_rate = instrument_taker_fee(instrument);
    let commission_value = compute_commission(
        fee_rate,
        instrument_fee_exponent(instrument),
        size_dec,
        price_dec,
        liquidity_side,
    );
    let pusd = crate::execution::get_pusd_currency();

    Ok(FillReport {
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
    })
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
/// Acceptance is also emitted on the submit happy path; the registry's dedup set ensures it
/// fires exactly once across the submit confirmation and the WS stream, including when a fill or
/// cancel races ahead of the acceptance message.
fn ensure_accepted(
    identity: &OrderIdentity,
    venue_order_id: VenueOrderId,
    ts_event: UnixNanos,
    ctx: &WsDispatchContext<'_>,
) {
    if !ctx.order_identities.mark_accepted(venue_order_id) {
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
    let mut voided = OrderFillVoided::new(
        fill.trader_id,
        fill.strategy_id,
        fill.instrument_id,
        fill.client_order_id,
        fill.venue_order_id,
        fill.account_id,
        Ustr::from(&format!("{}-FAILED-{}", trade.id, fill.client_order_id)),
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
        trade_fill_info(trade),
        UUID4::new(),
        ts_event,
        ctx.clock.get_time_ns(),
        false,
        false,
    );
    voided.causation_id = causation_id;
    ctx.emitter
        .send_order_event(OrderEventAny::FillVoided(voided));
}

/// Flattens a user trade into a string map of venue fill metadata for `OrderFilled.info`.
///
/// Mirrors the v1 adapter, which attaches the full raw trade to each fill it generates. Scalar
/// fields map to their string form; nested fields (such as `maker_orders`) become their JSON text.
fn trade_fill_info(trade: &PolymarketUserTrade) -> Option<IndexMap<Ustr, Ustr>> {
    let value = serde_json::to_value(trade).ok()?;
    let object = value.as_object()?;
    let mut info = IndexMap::with_capacity(object.len());
    for (key, val) in object {
        let val_str = match val {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        info.insert(Ustr::from(key.as_str()), Ustr::from(val_str.as_str()));
    }
    Some(info)
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
    use nautilus_common::messages::{ExecutionEvent, ExecutionReport};
    use nautilus_core::time::AtomicTime;
    use nautilus_model::{
        enums::{AccountType, OrderStatus},
        events::OrderEventAny,
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
        types::Currency,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::http::{
        models::GammaMarket,
        parse::{create_instrument_from_def, parse_gamma_market},
    };

    /// Registers a tracked-order identity so the dispatch routes the order through events.
    fn register_identity(
        order_identities: &OrderIdentityRegistry,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        client_order_id: &str,
    ) {
        order_identities.register_order_identity(
            venue_order_id,
            OrderIdentity {
                client_order_id: ClientOrderId::from(client_order_id),
                strategy_id: StrategyId::from("S-001"),
                instrument_id,
                order_side: OrderSide::Buy,
                quantity: Quantity::from(10),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        );
    }

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
            nautilus_model::identifiers::ClientId::from("POLYMARKET"),
            AccountId::from("POLY-001"),
            AccountType::Cash,
            Some(Currency::pUSD()),
        )
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
        // A resting BUY already reports shares, so its size passes through unconverted
        assert_eq!(report.quantity.as_decimal(), dec!(100));
        assert_eq!(
            report.price.map(|price| price.as_decimal()),
            Some(dec!(0.5))
        );
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

    // A market-order-type BUY reports the signed pUSD maker amount, so shares come from
    // dividing by the price. A SELL and the resting types already report shares.
    #[rstest]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::FOK,
        dec!(1.01),
        dec!(0.01),
        dec!(101)
    )]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::FOK,
        dec!(12),
        dec!(0.6),
        dec!(20)
    )]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::FAK,
        dec!(1),
        dec!(0.01),
        dec!(100)
    )]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::GTC,
        dec!(20),
        dec!(0.18),
        dec!(20)
    )]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::GTD,
        dec!(20),
        dec!(0.18),
        dec!(20)
    )]
    #[case(
        PolymarketOrderSide::Sell,
        PolymarketOrderType::FOK,
        dec!(20),
        dec!(0.6),
        dec!(20)
    )]
    #[case(
        PolymarketOrderSide::Buy,
        PolymarketOrderType::FOK,
        dec!(1.01),
        dec!(0),
        dec!(1.01)
    )]
    fn test_original_size_to_shares(
        #[case] side: PolymarketOrderSide,
        #[case] order_type: PolymarketOrderType,
        #[case] original_size: Decimal,
        #[case] price: Decimal,
        #[case] expected: Decimal,
    ) {
        let shares = original_size_to_shares(original_size, price, side, order_type);

        assert_eq!(shares, expected);
    }

    // A non-terminating division must still round to the instrument's size precision, and a
    // price the venue omits must leave the size unconverted rather than drop the report to zero.
    #[rstest]
    #[case("1", "0.03", "33.333333", "0.03")]
    #[case("1.01", "", "1.01", "0")]
    fn test_build_ws_order_status_report_fok_buy_quantity(
        #[case] original_size: &str,
        #[case] price: &str,
        #[case] expected_quantity: &str,
        #[case] expected_price: &str,
    ) {
        let mut order: PolymarketUserOrder = load("ws_user_order_fok_buy_pusd_size.json");
        order.original_size = original_size.to_string();
        order.price = price.to_string();
        let instrument = test_instrument();

        let report = build_ws_order_status_report(
            &order,
            &instrument,
            AccountId::from("POLY-001"),
            UnixNanos::from(1_000_000_000u64),
            UnixNanos::from(2_000_000_000u64),
        );

        assert_eq!(
            report.quantity.as_decimal(),
            Decimal::from_str_exact(expected_quantity).unwrap()
        );
        assert_eq!(
            report.price.map(|price| price.as_decimal()),
            Some(Decimal::from_str_exact(expected_price).unwrap())
        );
    }

    #[rstest]
    fn test_dispatch_fok_buy_registers_share_quantity_for_in_flight_submit() {
        let order: PolymarketUserOrder = load("ws_user_order_fok_buy_pusd_size.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        // No registration: the submit response has not landed, so the order update registers it
        let fill_tracker = OrderFillTrackerMap::new();
        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        let emitter = test_emitter();

        let venue_order_id = VenueOrderId::from(order.id.as_str());
        let client_order_id = ClientOrderId::from("O-FOK-IN-FLIGHT");
        pending_submits.insert(venue_order_id, client_order_id);
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            client_order_id.as_str(),
        );

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        // The venue reported 1.01 pUSD for the 101 shares submitted at 0.01
        assert_eq!(
            fill_tracker
                .submitted_qty(&venue_order_id)
                .map(|qty| qty.as_decimal()),
            Some(dec!(101)),
        );
    }

    #[rstest]
    fn test_dispatch_fok_buy_report_quantity_is_shares_without_identity() {
        let order: PolymarketUserOrder = load("ws_user_order_fok_buy_pusd_size.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());
        fill_tracker.register(
            venue_order_id,
            Quantity::from("101"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        // No identity registered, so the order surfaces as a report for reconciliation
        let order_identities = OrderIdentityRegistry::default();
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected order report");
        let ExecutionEvent::Report(authenticated) = event else {
            panic!("expected an order report, was {event:?}");
        };
        let ExecutionReport::Order(report) = authenticated.report else {
            panic!("expected an order report");
        };

        assert_eq!(report.venue_order_id, venue_order_id);
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.time_in_force, TimeInForce::Fok);
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(report.quantity.as_decimal(), dec!(101));
        assert_eq!(report.filled_qty.as_decimal(), dec!(0));
        assert_eq!(
            report.price.map(|price| price.as_decimal()),
            Some(dec!(0.01))
        );
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
        )
        .unwrap();

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
        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-MATCHED-FAILED",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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
        let matched_after_failure = dispatch_user_message(
            &UserWsMessage::Trade(trade.clone()),
            &ctx,
            &mut failed_first_state,
        );
        trade.status = crate::common::enums::PolymarketTradeStatus::Confirmed;
        let confirmed_after_failure =
            dispatch_user_message(&UserWsMessage::Trade(trade), &ctx, &mut failed_first_state);

        assert!(matched.is_none());
        assert!(failed.is_some());
        assert!(failed_first.is_some());
        assert!(matched_after_failure.is_none());
        assert!(confirmed_after_failure.is_none());
        assert_eq!(voided.trade_id, filled.trade_id);
        assert_eq!(voided.voided_qty, filled.last_qty);
        assert_eq!(voided.commission_voided, filled.commission);
        assert_eq!(voided.last_px, filled.last_px);
        assert!(!voided.is_reopened);
        assert_eq!(voided.causation_id, Some(filled.event_id));
        assert_eq!(
            fill_tracker.get_cumulative_filled(&venue_order_id),
            Some(Quantity::zero(instrument.size_precision()))
        );
        assert!(fill_tracker.is_trade_processed(&dedup_key));
        assert!(!fill_tracker.is_trade_confirmed(&dedup_key));
        assert!(fill_tracker.is_voided_trade(&dedup_key));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_late_fill_falls_back_to_report_after_identity_eviction() {
        let trade: PolymarketUserTrade = load("ws_user_trade.json");
        let market: GammaMarket = load("gamma_market_sports_market_money_line.json");
        let defs = parse_gamma_market(&market).unwrap();
        let instrument =
            create_instrument_from_def(&defs[0], UnixNanos::from(1_000_000_000u64)).unwrap();
        let venue_order_id = VenueOrderId::from(trade.taker_order_id.as_str());

        let token_instruments = AtomicMap::new();
        token_instruments.insert(trade.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-LATE-FILL",
        );
        assert!(order_identities.get(&venue_order_id).is_some());

        for index in 0..10_000 {
            let eviction_venue_order_id = VenueOrderId::from(format!("V-EVICT-{index}").as_str());
            let eviction_client_order_id = format!("O-EVICT-{index}");
            register_identity(
                &order_identities,
                eviction_venue_order_id,
                instrument.id(),
                &eviction_client_order_id,
            );
        }
        assert!(order_identities.get(&venue_order_id).is_none());

        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Trade(trade.clone()), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected late fill report");
        let ExecutionEvent::Report(authenticated) = event else {
            panic!("expected fill report for evicted identity, was {event:?}");
        };
        let ExecutionReport::Fill(report) = authenticated.report else {
            panic!("expected fill report for evicted identity");
        };

        assert_eq!(report.venue_order_id, venue_order_id);
        assert_eq!(report.trade_id, TradeId::from(trade.id.as_str()));
        assert_eq!(report.instrument_id, instrument.id());
        assert_eq!(
            report.last_qty.as_decimal(),
            Decimal::from_str_exact(&trade.size).unwrap()
        );
        assert_eq!(
            report.last_px.as_decimal(),
            Decimal::from_str_exact(&trade.price).unwrap()
        );
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
        assert_eq!(
            report.commission.as_decimal(),
            Decimal::from_str_exact("0.1875").unwrap()
        );
        assert_eq!(report.commission.currency, Currency::pUSD());
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_order_matched_caps_filled_qty_when_no_trades_tracked() {
        let order: PolymarketUserOrder = load("ws_user_order_matched.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());

        // Register order so it is "accepted" but with no fills tracked
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        // No identity registered, so the order surfaces as a report (the external/reconciliation
        // fallback), where filled_qty is capped to tracked fills.
        let order_identities = OrderIdentityRegistry::default();
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        let event = receiver.try_recv().expect("Expected report");
        match event {
            ExecutionEvent::Report(authenticated) => match authenticated.report {
                ExecutionReport::Order(order_report) => {
                    assert_eq!(order_report.filled_qty, Quantity::from("0"));
                }
                other => panic!("Expected order report, was {other:?}"),
            },
            other => panic!("Expected report event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_dispatch_order_matched_uses_tracked_fills_for_filled_qty() {
        let order: PolymarketUserOrder = load("ws_user_order_matched.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());

        // Register and record a partial fill (50 of 100)
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        fill_tracker.record_fill(&venue_order_id, Quantity::new(50.0, 6));

        let pending_submits = PendingSubmitTracker::default();
        // No identity registered, so the order surfaces as a report (the external/reconciliation
        // fallback), where filled_qty is capped to tracked fills.
        let order_identities = OrderIdentityRegistry::default();
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        dispatch_user_message(&UserWsMessage::Order(order), &ctx, &mut state);

        let event = receiver.try_recv().expect("Expected report");
        match event {
            ExecutionEvent::Report(authenticated) => match authenticated.report {
                ExecutionReport::Order(order_report) => {
                    assert_eq!(order_report.filled_qty, Quantity::from("50"));
                }
                other => panic!("Expected order report, was {other:?}"),
            },
            other => panic!("Expected report event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_dispatch_order_matched_normalizes_quantity_without_fill() {
        let order: PolymarketUserOrder = load("ws_user_order_matched.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(order.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        fill_tracker.record_fill(&venue_order_id, Quantity::new(99.995, 6));

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-MATCHED",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let clock = Box::leak(Box::new(AtomicTime::new(
            false,
            UnixNanos::from(2_000_000_000u64),
        )));

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock,
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();
        fill_tracker.restore_matched_trade("trade-0xfill1-correction".to_string(), Vec::new());
        assert!(matches!(
            fill_tracker.apply_trade_fills_atomically(
                Vec::new(),
                FillCorrectionMetadata {
                    correction_key: "trade-0xfill1-correction".to_string(),
                    venue_trade_id: "trade-0xfill1".to_string(),
                    is_confirmed: true,
                },
            ),
            Ok(TradeFillApplication::Confirmed(_))
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
        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(order.id.as_str());
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );
        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-CONFIRMED-DUST",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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
        let confirmation = receiver
            .try_recv()
            .expect("expected persisted fill confirmation");
        let normalized = receiver
            .try_recv()
            .expect("expected quantity normalization");

        match (real_fill, confirmation, normalized) {
            (
                ExecutionEvent::Order(OrderEventAny::Filled(real)),
                ExecutionEvent::Order(OrderEventAny::FillConfirmed(confirmed)),
                ExecutionEvent::Order(OrderEventAny::Updated(updated)),
            ) => {
                assert_eq!(real.last_qty, Quantity::from("99.995"));
                assert_eq!(confirmed.fill_event_id, real.event_id);
                assert_eq!(confirmed.trade_id, real.trade_id);
                assert_eq!(updated.quantity, Quantity::from("99.995"));
                assert!(updated.reconciliation);
            }
            other => panic!("expected fill, confirmation, then quantity update, was {other:?}"),
        }
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn test_cancel_reemitted_after_fill_for_canceled_order() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let trade: PolymarketUserTrade = load("ws_user_trade.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument.clone());

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        // Register order as accepted with original qty=100
        fill_tracker.register(
            venue_order_id,
            Quantity::from("100"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-CANCEL",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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

        // Should get: fill, durable finality, then the re-emitted terminal status.
        let fill_event = receiver.try_recv().expect("Expected filled event");
        match &fill_event {
            ExecutionEvent::Order(OrderEventAny::Filled(f)) => {
                assert_eq!(f.venue_order_id, venue_order_id);
            }
            other => panic!("Expected filled event, was {other:?}"),
        }

        let confirmation = receiver
            .try_recv()
            .expect("Expected persisted fill confirmation");
        assert!(matches!(
            confirmation,
            ExecutionEvent::Order(OrderEventAny::FillConfirmed(_))
        ));

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

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        // Register with qty=25 matching the trade size so the fill completes the order
        fill_tracker.register(
            venue_order_id,
            Quantity::from("25"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-CANCEL-FULL",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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
        let confirmation = receiver
            .try_recv()
            .expect("Expected persisted fill confirmation");
        assert!(matches!(
            confirmation,
            ExecutionEvent::Order(OrderEventAny::FillConfirmed(_))
        ));

        // Channel should be empty after finality: no re-emitted cancel for a fully-filled order.
        assert!(
            receiver.try_recv().is_err(),
            "Should not re-emit cancel when fill completes the order"
        );
    }

    #[rstest]
    fn test_untracked_cancel_emits_authoritative_report() {
        let cancel_order: PolymarketUserOrder = load("ws_user_order_cancellation.json");
        let instrument = test_instrument();

        let token_instruments = AtomicMap::new();
        token_instruments.insert(cancel_order.asset_id, instrument);

        // Fill tracker has NO registration (simulates HTTP still in-flight)
        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from(cancel_order.id.as_str());

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
            emitter: &emitter,
            account_id: AccountId::from("POLY-001"),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            user_address: "0xtest",
            user_api_key: "test-key",
        };
        let mut state = WsDispatchState::default();

        // Dispatch cancel while order is not yet accepted
        dispatch_user_message(&UserWsMessage::Order(cancel_order), &ctx, &mut state);

        let event = receiver.try_recv().expect("expected cancel report");
        assert!(matches!(
            event,
            ExecutionEvent::Report(ref authenticated)
                if matches!(
                    authenticated.report,
                    ExecutionReport::Order(ref report)
                        if report.venue_order_id == venue_order_id
                            && report.order_status == OrderStatus::Canceled
                )
        ));
        assert!(state.terminal_cancel_reports.get(&venue_order_id).is_some());
    }
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

        let fill_tracker = OrderFillTrackerMap::new();
        fill_tracker.register(
            venue_order_id,
            Quantity::from("20"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(&order_identities, venue_order_id, instrument.id(), "O-3797");
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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
            side: PolymarketOrderSide::Buy,
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
        let evt = receiver.try_recv().expect("(B) fill confirmation");
        assert!(matches!(
            evt,
            ExecutionEvent::Order(OrderEventAny::FillConfirmed(_))
        ));
        // Re-emitted cancel after fill (B)
        let evt = receiver.try_recv().expect("(B) re-emitted cancel");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(B) expected re-emitted cancel, was {other:?}"),
        }

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
        let evt = receiver.try_recv().expect("(E) fill confirmation");
        assert!(matches!(
            evt,
            ExecutionEvent::Order(OrderEventAny::FillConfirmed(_))
        ));

        // The fix: re-emitted cancel after (E) restores terminal state
        let evt = receiver.try_recv().expect("(E) re-emitted cancel");
        match &evt {
            ExecutionEvent::Order(OrderEventAny::Canceled(c)) => {
                assert_eq!(c.venue_order_id, Some(venue_order_id));
            }
            other => panic!("(E) expected re-emitted cancel, was {other:?}"),
        }

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

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("0xtaker-overfill");
        // Submitted qty truncated to USDC scale.
        let submitted = Quantity::new(714.285710, instrument.size_precision());
        fill_tracker.register(
            venue_order_id,
            submitted,
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-OVERFILL",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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

        // The dispatcher must record the snapped quantity in the tracker so
        // any subsequent ORDER MATCHED with size_matched > submitted_qty is
        // capped to it. record_fill happens before the FillReport is sent.
        let cumulative = fill_tracker
            .get_cumulative_filled(&venue_order_id)
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

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("0xtaker-one-shot-dust");
        let submitted = Quantity::from_decimal_dp(
            Decimal::from_str_exact(submitted_qty).unwrap(),
            instrument.size_precision(),
        )
        .unwrap();
        fill_tracker.register(
            venue_order_id,
            submitted,
            order_side,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        order_identities.register_order_identity(
            venue_order_id,
            OrderIdentity {
                client_order_id: ClientOrderId::from("O-ONE-SHOT"),
                strategy_id: StrategyId::from("S-001"),
                instrument_id: instrument.id(),
                order_side,
                quantity: submitted,
                order_type,
                time_in_force,
            },
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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

        let event = receiver
            .try_recv()
            .expect("expected persisted fill confirmation");
        assert!(matches!(
            event,
            ExecutionEvent::Order(OrderEventAny::FillConfirmed(_))
        ));

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
            assert!(
                fill_tracker
                    .get_cumulative_filled(&venue_order_id)
                    .is_none(),
                "order must be settled and removed from the tracker",
            );
        } else if expect_cancel {
            let event = receiver.try_recv().expect("expected IOC cancellation");
            match event {
                ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) => {
                    assert_eq!(canceled.venue_order_id, Some(venue_order_id));
                }
                other => panic!("expected canceled event, was {other:?}"),
            }
            assert!(
                fill_tracker
                    .get_cumulative_filled(&venue_order_id)
                    .is_none(),
                "canceled IOC must be settled and removed from the tracker",
            );
        } else {
            assert!(
                receiver.try_recv().is_err(),
                "resting order must not receive a terminal event",
            );
            assert!(
                fill_tracker
                    .get_cumulative_filled(&venue_order_id)
                    .is_some(),
                "ineligible order must stay tracked with open leaves",
            );
        }
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

        let fill_tracker = OrderFillTrackerMap::new();
        let venue_order_id = VenueOrderId::from("0xtaker-gross-overfill");
        let submitted = Quantity::new(30.0, size_precision);
        fill_tracker.register(
            venue_order_id,
            submitted,
            OrderSide::Buy,
            instrument.id(),
            size_precision,
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-GROSS-OVERFILL",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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

        let fill_tracker = OrderFillTrackerMap::new();
        fill_tracker.register(
            venue_order_id,
            Quantity::from("10"),
            OrderSide::Buy,
            instrument.id(),
            instrument.size_precision(),
            instrument.price_precision(),
        );

        let pending_submits = PendingSubmitTracker::default();
        let order_identities = OrderIdentityRegistry::default();
        register_identity(
            &order_identities,
            venue_order_id,
            instrument.id(),
            "O-TERMINAL",
        );
        order_identities.mark_accepted(venue_order_id);
        let mut emitter = test_emitter();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);

        let ctx = WsDispatchContext {
            token_instruments: &token_instruments,
            fill_tracker: &fill_tracker,
            pending_submits: &pending_submits,
            order_identities: &order_identities,
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
