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

//! Reconciliation report generation for the Polymarket execution client.

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_core::{
    UnixNanos,
    collections::AtomicMap,
    datetime::{NANOSECONDS_IN_MINUTE, NANOSECONDS_IN_SECOND},
    time::AtomicTime,
};
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus, PositionSideSpecified},
    identifiers::{AccountId, ClientId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    order_fill_tracker::OrderFillTrackerMap,
    parse::{
        instrument_taker_fee, try_build_maker_fill_report, try_parse_fill_report,
        try_parse_order_status_report, try_parse_timestamp,
    },
};
use crate::{
    common::{
        consts::{DUST_POSITION_THRESHOLD, DUST_SNAP_THRESHOLD_DEC},
        enums::{PolymarketLiquiditySide, PolymarketTradeStatus},
        models::PolymarketMakerOrder,
    },
    http::{
        clob::PolymarketClobHttpClient,
        data_api::PolymarketDataApiHttpClient,
        models::{DataApiPosition, PolymarketOpenOrder, PolymarketTradeReport},
        query::{GetOrdersParams, GetTradesParams},
    },
};

/// Shared context for trade-to-fill-report conversion.
pub(crate) struct FillContext<'a> {
    pub account_id: AccountId,
    pub user_address: &'a str,
    pub api_key: &'a str,
    pub pusd: Currency,
    pub clock: &'static AtomicTime,
}

#[derive(Clone, Debug)]
pub(crate) enum FillReconciliationScope {
    All,
    Instrument(InstrumentId),
    VenueOrder(VenueOrderId),
    Order {
        instrument_id: InstrumentId,
        venue_order_id: VenueOrderId,
    },
    Orders {
        instrument_id: Option<InstrumentId>,
        venue_order_ids: AHashSet<VenueOrderId>,
    },
}

impl FillReconciliationScope {
    /// Resolves this request into the venue-native filters trades are matched against.
    fn resolve(
        &self,
        instruments: &AtomicMap<Ustr, InstrumentAny>,
    ) -> anyhow::Result<ResolvedFillScope> {
        let instrument_filter = match self {
            Self::All | Self::VenueOrder(_) => None,
            Self::Instrument(instrument_id) | Self::Order { instrument_id, .. } => {
                Some(*instrument_id)
            }
            Self::Orders { instrument_id, .. } => *instrument_id,
        };
        let filter_token = instrument_filter
            .map(|instrument_id| resolve_requested_instrument(instruments, instrument_id))
            .transpose()?
            .map(|(token_id, _)| token_id);
        let target_order_ids = match self {
            Self::All | Self::Instrument(_) => None,
            Self::VenueOrder(venue_order_id) | Self::Order { venue_order_id, .. } => {
                Some(AHashSet::from_iter([*venue_order_id]))
            }
            Self::Orders {
                venue_order_ids, ..
            } => Some(venue_order_ids.clone()),
        };

        Ok(ResolvedFillScope {
            filter_token,
            target_order_ids,
        })
    }
}

/// A [`FillReconciliationScope`] resolved into the filters every trade is matched against.
///
/// `filter_token` is the Polymarket token of the requested NT instrument and
/// `target_order_ids` are the requested venue order IDs. Only
/// [`FillReconciliationScope::All`] resolves to neither filter, which is what puts
/// every confirmed trade of the configured account in scope.
#[derive(Debug)]
struct ResolvedFillScope {
    filter_token: Option<Ustr>,
    target_order_ids: Option<AHashSet<VenueOrderId>>,
}

impl ResolvedFillScope {
    /// Returns whether no filter narrows the request.
    fn covers_every_trade(&self) -> bool {
        self.filter_token.is_none() && self.target_order_ids.is_none()
    }

    /// Returns whether the request names specific venue orders.
    fn has_order_targets(&self) -> bool {
        self.target_order_ids.is_some()
    }

    /// Returns whether `venue_order_id` is requested, or no venue order is named.
    fn covers_order(&self, venue_order_id: VenueOrderId) -> bool {
        self.target_order_ids
            .as_ref()
            .is_none_or(|targets| targets.contains(&venue_order_id))
    }

    /// Returns whether `asset_id` is the requested token, or no instrument is named.
    fn covers_asset(&self, asset_id: Ustr) -> bool {
        self.filter_token
            .is_none_or(|token_id| asset_id == token_id)
    }
}

/// Where one maker order of a confirmed maker trade stands relative to a request.
///
/// Scope and ownership are independent facts. Polymarket runs a unified book in
/// which complementary tokens match across assets (see `determine_order_side`), so
/// the account's own maker order can sit on the token *outside* an
/// instrument-scoped request while a counterparty's order sits inside it. Ownership
/// therefore answers whether the venue response is interpretable at all, and scope
/// answers only which orders the request asked to be reported.
#[derive(Clone, Copy, Debug)]
struct MakerOrderStanding {
    /// Whether the request covers this maker order.
    in_scope: bool,
    /// Whether the configured account owns it, independent of scope.
    owned: bool,
}

/// Converts a venue order ID reported inside a trade.
///
/// `VenueOrderId::new` panics on an empty or non-ASCII value, and a match carries
/// counterparties' identifiers as well as the account's own, so a malformed one must
/// fail reconciliation rather than abort the process.
fn try_venue_order_id(order_id: &str, trade_id: &str) -> anyhow::Result<VenueOrderId> {
    VenueOrderId::new_checked(order_id).map_err(|e| {
        anyhow::anyhow!(
            "Polymarket reconciliation trade {trade_id} has an invalid venue order ID: {e}"
        )
    })
}

/// Classifies one maker order of a confirmed maker trade against the request.
///
/// A scope contradiction (a maker order the request names by ID whose asset is not
/// the requested instrument's token) is an error rather than a further outcome: the
/// caller can neither repair nor safely ignore it. It is raised only for an order
/// the request covers, because an order the request never asked about contributes
/// nothing and failing on it would let unrelated venue history break an unrelated
/// query.
fn classify_maker_order(
    mo: &PolymarketMakerOrder,
    trade_id: &str,
    scope: &ResolvedFillScope,
    ctx: &FillContext<'_>,
) -> anyhow::Result<MakerOrderStanding> {
    let owned = mo.is_owned_by(ctx.user_address, ctx.api_key);

    if !scope.covers_order(try_venue_order_id(&mo.order_id, trade_id)?) {
        return Ok(MakerOrderStanding {
            in_scope: false,
            owned,
        });
    }

    if !scope.covers_asset(mo.asset_id) {
        if scope.has_order_targets() {
            anyhow::bail!(
                "Polymarket reconciliation target maker fill {} asset {} \
                 does not match requested NT instrument",
                trade_id,
                mo.asset_id
            );
        }

        return Ok(MakerOrderStanding {
            in_scope: false,
            owned,
        });
    }

    Ok(MakerOrderStanding {
        in_scope: true,
        owned,
    })
}

/// Selects the maker orders of `trade` that reconciliation must report.
///
/// A match carries the maker orders of every maker that filled it, so only the
/// account's own orders become reports.
///
/// The CLOB trades endpoint returns the trades of the authenticated account and
/// reports `trader_side` from that account's perspective, so a confirmed trade whose
/// `trader_side` is `MAKER` must hold at least one maker order the account placed.
/// A trade the request reaches that holds none under either reported identity
/// (see [`PolymarketMakerOrder::is_owned_by`]) means one of two things, and neither
/// permits reconciliation to continue:
///
/// - the venue response is incomplete, and answering with no fill would silently
///   understate the filled quantity of a live order; or
/// - the configured credentials do not correspond to the account whose history the
///   endpoint returned, so fills and the positions they reconcile against would
///   describe different accounts.
///
/// The failure names the configured address and the trade's maker addresses, and
/// never the API keys, so the two cases are distinguishable from the message alone.
///
/// Ownership is checked across the whole match rather than within the request:
/// a cross-asset match can put the account's maker order on the complementary token
/// while the requested token carries only a counterparty's, and that trade is
/// interpretable even though it contributes no report.
fn owned_maker_orders_in_scope<'a>(
    trade: &'a PolymarketTradeReport,
    scope: &ResolvedFillScope,
    ctx: &FillContext<'_>,
) -> anyhow::Result<Vec<&'a PolymarketMakerOrder>> {
    let standings = trade
        .maker_orders
        .iter()
        .map(|mo| classify_maker_order(mo, &trade.id, scope, ctx).map(|standing| (mo, standing)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let owned: Vec<&PolymarketMakerOrder> = standings
        .iter()
        .filter(|(_, standing)| standing.in_scope && standing.owned)
        .map(|(mo, _)| *mo)
        .collect();
    let trade_in_scope =
        scope.covers_every_trade() || standings.iter().any(|(_, standing)| standing.in_scope);
    let owned_anywhere = standings.iter().any(|(_, standing)| standing.owned);

    if trade_in_scope && !owned_anywhere {
        anyhow::bail!(
            "Polymarket reconciliation confirmed maker trade {} has no owned maker order \
             for configured maker address {}; trade maker addresses: [{}]",
            trade.id,
            ctx.user_address,
            trade
                .maker_orders
                .iter()
                .map(|mo| mo.maker_address.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(owned)
}

pub(crate) fn resolve_requested_instrument(
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    instrument_id: InstrumentId,
) -> anyhow::Result<(Ustr, InstrumentAny)> {
    instruments
        .load()
        .iter()
        .find(|(_, instrument)| instrument.id() == instrument_id)
        .map(|(token_id, instrument)| (*token_id, instrument.clone()))
        .with_context(|| {
            format!(
                "Polymarket reconciliation cannot resolve requested NT instrument {instrument_id}"
            )
        })
}

/// Converts trade reports into fill reports: single implementation of maker/taker
/// parsing used by both `generate_fill_reports()` and `generate_mass_status()`.
pub(crate) fn build_fill_reports_from_trades(
    trades: &[PolymarketTradeReport],
    ctx: &FillContext<'_>,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    scope: &FillReconciliationScope,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<FillReport>> {
    let mut reports = Vec::new();
    let scope = scope.resolve(instruments)?;

    for trade in trades {
        if trade.status != PolymarketTradeStatus::Confirmed {
            continue;
        }

        let is_maker = trade.trader_side == PolymarketLiquiditySide::Maker;

        if is_maker {
            for mo in owned_maker_orders_in_scope(trade, &scope, ctx)? {
                let token_id = Ustr::from(mo.asset_id.as_str());
                let instrument = instruments.get_cloned(&token_id);
                let (instrument_id, price_prec, size_prec) = match instrument {
                    Some(i) => (i.id(), i.price_precision(), i.size_precision()),
                    None => {
                        anyhow::bail!(
                            "Polymarket reconciliation cannot map confirmed maker fill {} \
                             asset {} to an NT instrument",
                            trade.id,
                            mo.asset_id
                        );
                    }
                };

                let ts_event = try_parse_timestamp(&trade.match_time).map_err(|e| {
                    anyhow::anyhow!(
                        "confirmed maker fill {} has invalid match_time: {e}",
                        trade.id
                    )
                })?;
                let report = try_build_maker_fill_report(
                    mo,
                    &trade.id,
                    trade.trader_side,
                    trade.side,
                    trade.asset_id.as_str(),
                    ctx.account_id,
                    instrument_id,
                    price_prec,
                    size_prec,
                    ctx.pusd,
                    LiquiditySide::Maker,
                    ts_event,
                    ts_init,
                )?;
                reports.push(report);
            }
        } else {
            let venue_order_id = try_venue_order_id(&trade.taker_order_id, &trade.id)?;

            if !scope.covers_order(venue_order_id) {
                continue;
            }

            if !scope.covers_asset(trade.asset_id) {
                if scope.has_order_targets() {
                    anyhow::bail!(
                        "Polymarket reconciliation target taker fill {} asset {} \
                         does not match requested NT instrument",
                        trade.id,
                        trade.asset_id
                    );
                }

                continue;
            }

            let token_id = Ustr::from(trade.asset_id.as_str());
            let instrument = instruments.get_cloned(&token_id);
            let (instrument_id, price_prec, size_prec, taker_fee_rate) = match instrument {
                Some(i) => (
                    i.id(),
                    i.price_precision(),
                    i.size_precision(),
                    instrument_taker_fee(&i),
                ),
                None => {
                    anyhow::bail!(
                        "Polymarket reconciliation cannot map confirmed taker fill {} \
                         asset {} to an NT instrument",
                        trade.id,
                        trade.asset_id
                    );
                }
            };

            let report = try_parse_fill_report(
                trade,
                instrument_id,
                ctx.account_id,
                None,
                price_prec,
                size_prec,
                ctx.pusd,
                taker_fee_rate,
                ts_init,
            )?;
            reports.push(report);
        }
    }

    Ok(reports)
}

/// Converts open orders into order status reports.
pub(crate) fn build_order_reports_from_orders(
    orders: &[PolymarketOpenOrder],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<OrderStatusReport>> {
    let mut reports = Vec::new();
    let filter_token = instrument_filter
        .map(|instrument_id| resolve_requested_instrument(instruments, instrument_id))
        .transpose()?
        .map(|(token_id, _)| token_id);

    for order in orders {
        if filter_token.is_some_and(|token_id| order.asset_id != token_id) {
            continue;
        }
        let token_id = Ustr::from(order.asset_id.as_str());
        let instrument = instruments.get_cloned(&token_id);
        let (instrument_id, price_prec, size_prec) = match instrument {
            Some(i) => (i.id(), i.price_precision(), i.size_precision()),
            None => {
                anyhow::bail!(
                    "Polymarket reconciliation cannot map venue open order asset {} \
                     to an NT instrument",
                    order.asset_id
                );
            }
        };

        let report = try_parse_order_status_report(
            order,
            instrument_id,
            account_id,
            None,
            price_prec,
            size_prec,
            ts_init,
        )?;
        reports.push(report);
    }

    Ok(reports)
}

/// Applies venue_order_id and time-range filters to fill reports.
pub(crate) fn apply_fill_filters(
    mut reports: Vec<FillReport>,
    venue_order_id: Option<VenueOrderId>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> Vec<FillReport> {
    if let Some(vid) = venue_order_id {
        reports.retain(|r| r.venue_order_id == vid);
    }

    match (start, end) {
        (Some(s), Some(e)) => reports.retain(|r| r.ts_event >= s && r.ts_event <= e),
        (Some(s), None) => reports.retain(|r| r.ts_event >= s),
        (None, Some(e)) => reports.retain(|r| r.ts_event <= e),
        (None, None) => {}
    }

    reports
}

/// Builds position status reports from Data API positions, filtering dust.
pub(crate) fn build_position_reports(
    positions: &[DataApiPosition],
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    account_id: AccountId,
    instrument_filter: Option<InstrumentId>,
    ts: UnixNanos,
) -> anyhow::Result<Vec<PositionStatusReport>> {
    let mut reports = Vec::new();
    let filter_token = instrument_filter
        .map(|instrument_id| resolve_requested_instrument(instruments, instrument_id))
        .transpose()?
        .map(|(token_id, _)| token_id);

    for position in positions {
        if filter_token.is_some_and(|token_id| position.asset != token_id.as_str()) {
            continue;
        }

        if position.size.is_sign_negative() {
            anyhow::bail!(
                "Polymarket reconciliation received negative Data API position \
                 {}-{} size {}",
                position.condition_id,
                position.asset,
                position.size
            );
        }

        if position.size > Decimal::ZERO && position.size < DUST_POSITION_THRESHOLD {
            log::debug!(
                "Filtering dust position: {}-{}, size={}",
                position.condition_id,
                position.asset,
                position.size
            );
        }

        if position.size < DUST_POSITION_THRESHOLD {
            continue;
        }

        let instrument = instruments
            .get_cloned(&Ustr::from(position.asset.as_str()))
            .with_context(|| {
                format!(
                    "Polymarket reconciliation cannot map Data API position \
                     {}-{} to an NT instrument",
                    position.condition_id, position.asset
                )
            })?;
        let quantity = position_quantity(
            position.size,
            instrument.size_precision(),
            position.condition_id.as_str(),
            position.asset.as_str(),
        )?;
        reports.push(PositionStatusReport::new(
            account_id,
            instrument.id(),
            PositionSideSpecified::Long,
            quantity,
            ts,
            ts,
            None,
            None,
            position.avg_price,
        ));
    }
    Ok(reports)
}

fn position_quantity(
    size: Decimal,
    precision: u8,
    condition_id: &str,
    asset: &str,
) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(size, precision).with_context(|| {
        format!(
            "Polymarket reconciliation cannot represent Data API position \
             {condition_id}-{asset} size {size}"
        )
    })
}

/// Full reconciliation mass status generation.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn generate_mass_status(
    http_client: &PolymarketClobHttpClient,
    data_api_client: &PolymarketDataApiHttpClient,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
    fill_tracker: &OrderFillTrackerMap,
    ctx: &FillContext<'_>,
    client_id: ClientId,
    venue: Venue,
    lookback_mins: Option<u64>,
) -> anyhow::Result<Option<ExecutionMassStatus>> {
    let ts_init = ctx.clock.get_time_ns();
    // Resolve the lookback window before fetching so the trade request is bounded by
    // it. Confirmed trades outside the window are discarded from the answer, so
    // fetching them would only expose reconciliation to venue records that cannot
    // contribute a report.
    let cutoff = lookback_cutoff(ts_init, lookback_mins)?;

    // Fetch orders
    let orders = http_client
        .get_orders(GetOrdersParams::default())
        .await
        .context("failed to fetch orders for mass status")?;

    let mut order_reports =
        build_order_reports_from_orders(&orders, instruments, ctx.account_id, None, ts_init)?;

    // Fetch and parse fill reports
    let trades = http_client
        .get_trades(GetTradesParams {
            after: cutoff.map(|cutoff| cutoff.as_u64() / NANOSECONDS_IN_SECOND),
            ..GetTradesParams::default()
        })
        .await
        .context("failed to fetch trades for mass status")?;

    let trades: Vec<PolymarketTradeReport> = trades
        .into_iter()
        .filter(|trade| is_within_lookback(trade, cutoff))
        .collect();

    let mut fill_reports = build_fill_reports_from_trades(
        &trades,
        ctx,
        instruments,
        &FillReconciliationScope::All,
        ts_init,
    )?;

    // Snap dust drift on REST fills the same way the WS path does.
    // Commission stays as venue-reported.
    fill_tracker.snap_fill_reports(&mut fill_reports);

    // Position reports from Data API
    let positions = data_api_client
        .get_positions(ctx.user_address)
        .await
        .context("failed to fetch positions for mass status")?;

    let position_reports =
        build_position_reports(&positions, instruments, ctx.account_id, None, ts_init)?;

    // Apply lookback filter
    if let Some(cutoff) = cutoff {
        let orders_before = order_reports.len();
        order_reports.retain(|r| r.ts_last >= cutoff);
        let orders_removed = orders_before - order_reports.len();

        let fills_before = fill_reports.len();
        fill_reports.retain(|r| r.ts_event >= cutoff);
        let fills_removed = fills_before - fill_reports.len();

        log::debug!(
            "Lookback filter (cutoff {}): orders {}->{} (removed {}), fills {}->{} (removed {})",
            cutoff,
            orders_before,
            order_reports.len(),
            orders_removed,
            fills_before,
            fill_reports.len(),
            fills_removed,
        );
    } else {
        log::debug!(
            "Generated mass status: {} orders, {} fills, {} positions",
            order_reports.len(),
            fill_reports.len(),
            position_reports.len(),
        );
    }

    cap_order_reports_to_confirmed_fills(
        &mut order_reports,
        &fill_reports,
        &orders_with_unsettled_trades(&trades),
    )?;

    let mut mass_status = ExecutionMassStatus::new(client_id, ctx.account_id, venue, ts_init, None);

    mass_status.add_order_reports(order_reports);
    mass_status.add_position_reports(position_reports);
    mass_status.add_fill_reports(fill_reports);

    Ok(Some(mass_status))
}

/// Returns whether a confirmed trade can still contribute to a bounded answer.
///
/// The venue's `after` parameter bounds the request, but its documentation does not
/// state which timestamp it filters on, so the window is enforced here as well
/// rather than trusted. A trade whose `match_time` cannot be parsed is kept: only
/// the report path decides what an unreadable timestamp means, and dropping it here
/// would silently discard a fill this reconciliation is responsible for.
fn is_within_lookback(trade: &PolymarketTradeReport, cutoff: Option<UnixNanos>) -> bool {
    let Some(cutoff) = cutoff else {
        return true;
    };

    match try_parse_timestamp(&trade.match_time) {
        Ok(ts_event) => ts_event >= cutoff,
        Err(_) => true,
    }
}

/// Resolves the reconciliation lookback into the earliest timestamp still in the
/// answer, or `None` when the request is unbounded.
fn lookback_cutoff(
    now: UnixNanos,
    lookback_mins: Option<u64>,
) -> anyhow::Result<Option<UnixNanos>> {
    let Some(mins) = lookback_mins else {
        return Ok(None);
    };

    let lookback_ns = mins
        .checked_mul(NANOSECONDS_IN_MINUTE)
        .with_context(|| format!("Polymarket reconciliation lookback {mins}min overflows"))?;

    Ok(Some(UnixNanos::from(
        now.as_u64().saturating_sub(lookback_ns),
    )))
}

/// Venue order IDs the response shows with a trade that has not settled yet.
///
/// The venue counts a matched trade toward an order's filled size before that trade
/// confirms, and the adapter defers fill quantity until confirmation, so those orders
/// are held to their confirmed fills rather than to the venue's matched size.
fn orders_with_unsettled_trades(trades: &[PolymarketTradeReport]) -> AHashSet<VenueOrderId> {
    trades
        .iter()
        .filter(|trade| trade.status.is_pending_settlement())
        .flat_map(|trade| {
            std::iter::once(trade.taker_order_id.as_str())
                .chain(trade.maker_orders.iter().map(|mo| mo.order_id.as_str()))
        })
        .filter_map(|order_id| VenueOrderId::new_checked(order_id).ok())
        .collect()
}

fn cap_order_reports_to_confirmed_fills(
    order_reports: &mut [OrderStatusReport],
    fill_reports: &[FillReport],
    unsettled_orders: &AHashSet<VenueOrderId>,
) -> anyhow::Result<()> {
    let confirmed_by_order = confirmed_filled_quantities(fill_reports)?;

    for report in order_reports {
        let confirmed = confirmed_by_order.get(&report.venue_order_id).copied();

        // An order with no confirmed fill and nothing settling is uncorroborated,
        // not known to be unfilled: its fills may predate the lookback window, and
        // zeroing it would understate a filled quantity the venue itself reports. An
        // order with a trade still settling is the opposite case, because the venue
        // already counts that volume as matched while the adapter defers it until
        // confirmation. Either way the venue's quantity relationship is normalized.
        if confirmed.is_none() && !unsettled_orders.contains(&report.venue_order_id) {
            normalize_terminal_order_report_quantity(report);
            continue;
        }

        try_cap_order_report_filled_qty(
            report,
            Quantity::zero(report.quantity.precision),
            confirmed,
        )?;
    }
    Ok(())
}

pub(crate) fn confirmed_filled_quantities(
    fill_reports: &[FillReport],
) -> anyhow::Result<AHashMap<VenueOrderId, Decimal>> {
    let mut confirmed_by_order = AHashMap::new();
    for fill in fill_reports {
        let total = confirmed_by_order
            .entry(fill.venue_order_id)
            .or_insert(Decimal::ZERO);
        *total = total
            .checked_add(fill.last_qty.as_decimal())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "confirmed fill quantity overflow for venue order {}",
                    fill.venue_order_id
                )
            })?;
    }

    Ok(confirmed_by_order)
}

pub(crate) fn cap_order_report_filled_qty(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Option<Decimal>,
) {
    let confirmed_filled = confirmed_filled
        .and_then(|qty| Quantity::from_decimal_dp(qty, report.quantity.precision).ok())
        .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
    cap_order_report_with_quantities(report, local_filled, confirmed_filled);
}

pub(crate) fn try_cap_order_report_filled_qty(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Option<Decimal>,
) -> anyhow::Result<()> {
    let confirmed_filled = confirmed_filled
        .map(|qty| {
            Quantity::from_decimal_dp(qty, report.quantity.precision).map_err(|e| {
                anyhow::anyhow!(
                    "cannot represent confirmed fill quantity for venue order {}: {e}",
                    report.venue_order_id
                )
            })
        })
        .transpose()?
        .unwrap_or_else(|| Quantity::zero(report.quantity.precision));
    cap_order_report_with_quantities(report, local_filled, confirmed_filled);
    Ok(())
}

fn cap_order_report_with_quantities(
    report: &mut OrderStatusReport,
    local_filled: Quantity,
    confirmed_filled: Quantity,
) {
    let capped = report.filled_qty.min(local_filled.max(confirmed_filled));
    report.filled_qty = capped;
    normalize_terminal_order_report_quantity(report);
}

pub(crate) fn normalize_terminal_order_report_quantity(report: &mut OrderStatusReport) {
    if report.order_status != OrderStatus::Filled
        || report.filled_qty.is_zero()
        || report.filled_qty >= report.quantity
    {
        return;
    }

    let leaves = report.quantity.as_decimal() - report.filled_qty.as_decimal();
    if leaves < DUST_SNAP_THRESHOLD_DEC {
        log::debug!(
            "Normalizing terminal order report {} quantity from {} to confirmed fills {}",
            report.venue_order_id,
            report.quantity,
            report.filled_qty,
        );
        report.quantity = report.filled_qty;
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
        identifiers::{Symbol, TradeId},
        instruments::{Instrument, InstrumentAny, stubs::binary_option},
        types::{Money, Price},
    };
    use rstest::rstest;

    use super::*;

    /// Maker address of the configured account, as the trade fixture reports it.
    const USER_ADDRESS: &str = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8";
    /// The same account in mixed case, as wallets and explorers present an address.
    const USER_ADDRESS_MIXED_CASE: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
    /// CLOB API key of the configured account.
    const USER_API_KEY: &str = "00000000-0000-0000-0000-000000000001";
    const COUNTERPARTY_ADDRESS: &str = "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc";
    const COUNTERPARTY_API_KEY: &str = "00000000-0000-0000-0000-000000000003";
    const UNMAPPED_TOKEN: &str = "UNRELATED-UNMAPPED-TOKEN";
    /// The complementary token of a binary market, which the unified book can match
    /// against the requested one inside a single trade.
    const COMPLEMENTARY_TOKEN: &str = "COMPLEMENTARY-TOKEN";

    fn mapped_instrument() -> (AtomicMap<Ustr, InstrumentAny>, InstrumentAny) {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let instruments = AtomicMap::new();
        instruments.insert(
            Ustr::from(instrument.raw_symbol().as_str()),
            instrument.clone(),
        );
        (instruments, instrument)
    }

    fn open_order_for(instrument: &InstrumentAny) -> PolymarketOpenOrder {
        let content = std::fs::read_to_string("test_data/http_open_order.json")
            .expect("open-order fixture should load");
        let mut order: PolymarketOpenOrder =
            serde_json::from_str(&content).expect("open-order fixture should decode");
        order.asset_id = Ustr::from(instrument.raw_symbol().as_str());
        order
    }

    fn confirmed_trade_for(instrument: &InstrumentAny) -> PolymarketTradeReport {
        let content = std::fs::read_to_string("test_data/http_trade_report.json")
            .expect("trade fixture should load");
        let mut trade: PolymarketTradeReport =
            serde_json::from_str(&content).expect("trade fixture should decode");
        trade.status = PolymarketTradeStatus::Confirmed;
        trade.asset_id = Ustr::from(instrument.raw_symbol().as_str());
        for maker_order in &mut trade.maker_orders {
            maker_order.asset_id = Ustr::from(instrument.raw_symbol().as_str());
        }
        trade
    }

    /// A confirmed trade the configured account filled as a maker.
    ///
    /// The fixture carries two maker orders: the first is the account's own (the
    /// fixture reports it under [`USER_ADDRESS`]) and the second belongs to a
    /// counterparty of the same match.
    fn confirmed_maker_trade_for(instrument: &InstrumentAny) -> PolymarketTradeReport {
        let mut trade = confirmed_trade_for(instrument);
        trade.trader_side = PolymarketLiquiditySide::Maker;
        trade
    }

    /// The other token of the same binary market, which the unified book can match
    /// against the requested one inside a single trade.
    fn complementary_instrument(instrument: &InstrumentAny) -> InstrumentAny {
        let InstrumentAny::BinaryOption(mut option) = instrument.clone() else {
            panic!("the Polymarket stub instrument is a binary option");
        };
        option.id = InstrumentId::from("COMPLEMENTARY-TOKEN.POLYMARKET");
        option.raw_symbol = Symbol::new(COMPLEMENTARY_TOKEN);

        InstrumentAny::BinaryOption(option)
    }

    /// Reassigns every maker order of `trade` to counterparties of the match.
    fn disown_maker_orders(trade: &mut PolymarketTradeReport) {
        for maker_order in &mut trade.maker_orders {
            maker_order.maker_address = COUNTERPARTY_ADDRESS.to_string();
            maker_order.owner = COUNTERPARTY_API_KEY.to_string();
        }
    }

    fn fill_context() -> FillContext<'static> {
        FillContext {
            account_id: AccountId::from("POLY-001"),
            user_address: USER_ADDRESS,
            api_key: USER_API_KEY,
            pusd: Currency::pUSD(),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
        }
    }

    #[rstest]
    fn rejects_unmapped_open_order() {
        let content = std::fs::read_to_string("test_data/http_open_order.json")
            .expect("open-order fixture should load");
        let order: PolymarketOpenOrder =
            serde_json::from_str(&content).expect("open-order fixture should decode");
        let instruments = AtomicMap::new();

        let error = build_order_reports_from_orders(
            &[order],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("an unmapped venue open order must fail reconciliation");

        assert!(error.to_string().contains("venue open order asset"));
    }

    #[rstest]
    fn rejects_unmapped_confirmed_fill() {
        let content = std::fs::read_to_string("test_data/http_trade_report.json")
            .expect("trade fixture should load");
        let mut trade: PolymarketTradeReport =
            serde_json::from_str(&content).expect("trade fixture should decode");
        trade.status = PolymarketTradeStatus::Confirmed;
        let instruments = AtomicMap::new();

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("an unmapped confirmed fill must fail reconciliation");

        assert!(error.to_string().contains("confirmed taker fill"));
    }

    #[rstest]
    #[case::quantity(|order: &mut PolymarketOpenOrder| order.original_size = Decimal::MAX)]
    #[case::filled(|order: &mut PolymarketOpenOrder| order.size_matched = Decimal::MAX)]
    #[case::price(|order: &mut PolymarketOpenOrder| order.price = Decimal::MAX)]
    fn rejects_unrepresentable_open_order_numeric(#[case] mutate: fn(&mut PolymarketOpenOrder)) {
        let (instruments, instrument) = mapped_instrument();
        let mut order = open_order_for(&instrument);
        mutate(&mut order);

        let error = build_order_reports_from_orders(
            &[order],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("unrepresentable open-order numerics must fail reconciliation");

        assert!(
            error
                .to_string()
                .contains("cannot represent venue open order")
        );
    }

    #[rstest]
    fn rejects_overflowed_open_order_timestamp() {
        let (instruments, instrument) = mapped_instrument();
        let mut order = open_order_for(&instrument);
        order.created_at = u64::MAX;

        let error = build_order_reports_from_orders(
            &[order],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("overflowed open-order timestamps must fail reconciliation");

        assert!(error.to_string().contains("created_at"));
    }

    #[rstest]
    fn rejects_malformed_open_order_expiration() {
        let (instruments, instrument) = mapped_instrument();
        let mut order = open_order_for(&instrument);
        order.expiration = Some("not-a-timestamp".to_string());

        let error = build_order_reports_from_orders(
            &[order],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("malformed open-order expiration must fail reconciliation");

        assert!(error.to_string().contains("expiration"));
    }

    #[rstest]
    #[case::quantity(|trade: &mut PolymarketTradeReport| trade.size = Decimal::MAX)]
    #[case::price(|trade: &mut PolymarketTradeReport| trade.price = Decimal::MAX)]
    fn rejects_unrepresentable_confirmed_taker_fill_numeric(
        #[case] mutate: fn(&mut PolymarketTradeReport),
    ) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_trade_for(&instrument);
        mutate(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("unrepresentable confirmed-fill numerics must fail reconciliation");

        assert!(
            error
                .to_string()
                .contains("cannot represent confirmed taker fill")
        );
    }

    #[rstest]
    fn rejects_unrepresentable_confirmed_maker_fill_numeric() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.maker_orders[0].matched_amount = Decimal::MAX;

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("unrepresentable maker-fill numerics must fail reconciliation");

        assert!(
            error
                .to_string()
                .contains("cannot represent confirmed maker fill")
        );
    }

    #[rstest]
    fn rejects_confirmed_maker_fill_without_owned_maker_order() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a confirmed maker trade must contain an owned maker order");

        assert!(error.to_string().contains("no owned maker order"));
    }

    #[rstest]
    fn targeted_maker_fill_rejects_target_order_ownership_mismatch() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        let target_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());
        disown_maker_orders(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::VenueOrder(target_order_id),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a targeted maker order with mismatched ownership must fail");

        assert!(error.to_string().contains("no owned maker order"));
    }

    /// Fail-closed completeness must hold for every scope that reaches a maker
    /// order, not only the two variants the defect was first reported under.
    #[rstest]
    #[case::instrument(|instrument_id: InstrumentId, _: VenueOrderId| {
        FillReconciliationScope::Instrument(instrument_id)
    })]
    #[case::order(|instrument_id: InstrumentId, venue_order_id: VenueOrderId| {
        FillReconciliationScope::Order {
            instrument_id,
            venue_order_id,
        }
    })]
    #[case::orders(|instrument_id: InstrumentId, venue_order_id: VenueOrderId| {
        FillReconciliationScope::Orders {
            instrument_id: Some(instrument_id),
            venue_order_ids: AHashSet::from_iter([venue_order_id]),
        }
    })]
    fn scoped_maker_reconciliation_fails_closed_without_owned_order(
        #[case] build_scope: fn(InstrumentId, VenueOrderId) -> FillReconciliationScope,
    ) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        let target_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());
        disown_maker_orders(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &build_scope(instrument.id(), target_order_id),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a scope-relevant maker trade must contain an owned maker order");

        assert!(error.to_string().contains("no owned maker order"));
    }

    #[rstest]
    fn targeted_maker_fill_ignores_unrelated_trade_without_owned_order() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::VenueOrder(VenueOrderId::from("unrelated-target")),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a targeted query must ignore a trade that does not contain its order");

        assert!(reports.is_empty());
    }

    /// Completeness must not fire for a trade the request never asked about, even
    /// when that trade is on the requested instrument and holds our own order.
    #[rstest]
    #[case::order(|instrument_id: InstrumentId, venue_order_id: VenueOrderId| {
        FillReconciliationScope::Order {
            instrument_id,
            venue_order_id,
        }
    })]
    #[case::orders_with_instrument(|instrument_id: InstrumentId, venue_order_id: VenueOrderId| {
        FillReconciliationScope::Orders {
            instrument_id: Some(instrument_id),
            venue_order_ids: AHashSet::from_iter([venue_order_id]),
        }
    })]
    #[case::orders_without_instrument(|_: InstrumentId, venue_order_id: VenueOrderId| {
        FillReconciliationScope::Orders {
            instrument_id: None,
            venue_order_ids: AHashSet::from_iter([venue_order_id]),
        }
    })]
    fn targeted_maker_reconciliation_ignores_untargeted_trade(
        #[case] build_scope: fn(InstrumentId, VenueOrderId) -> FillReconciliationScope,
    ) {
        let (instruments, instrument) = mapped_instrument();
        let trade = confirmed_maker_trade_for(&instrument);

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &build_scope(instrument.id(), VenueOrderId::from("unrelated-target")),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a targeted query must ignore trades outside its targets");

        assert!(reports.is_empty());
    }

    /// A match holds every maker's order, so only the account's own becomes a report
    /// even when a counterparty filled the same match.
    #[rstest]
    fn mixed_maker_trade_reports_only_the_owned_order() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        let owned_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a mixed maker trade must report the owned maker order");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned_order_id);
        assert_eq!(reports[0].last_qty.as_decimal(), Decimal::from(25));
    }

    /// One taker can fill several of the account's resting orders in a single match,
    /// and every one of them is a distinct fill.
    #[rstest]
    fn maker_trade_reports_every_owned_maker_order() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        for maker_order in &mut trade.maker_orders {
            maker_order.maker_address = USER_ADDRESS.to_string();
        }
        let order_ids: Vec<VenueOrderId> = trade
            .maker_orders
            .iter()
            .map(|maker_order| VenueOrderId::from(maker_order.order_id.as_str()))
            .collect();

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("every owned maker order of a match must reconcile");

        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].venue_order_id, order_ids[0]);
        assert_eq!(reports[1].venue_order_id, order_ids[1]);
        assert_ne!(reports[0].trade_id, reports[1].trade_id);
        assert_eq!(reports[0].last_qty.as_decimal(), Decimal::from(25));
        assert_eq!(reports[1].last_qty.as_decimal(), Decimal::from(5));
    }

    /// The CLOB matches complementary tokens across assets, so the account's maker
    /// order can sit on the token an instrument-scoped request did not ask about
    /// while a counterparty's order sits on the token it did. That trade is
    /// interpretable and simply contributes no report.
    #[rstest]
    fn instrument_scoped_cross_asset_match_reports_nothing_without_failing() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        trade.maker_orders[0].asset_id = Ustr::from(COMPLEMENTARY_TOKEN);

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::Instrument(instrument.id()),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a cross-asset match must not fail an instrument-scoped request");

        assert!(reports.is_empty());
    }

    /// A maker fill takes the opposite side of the taker only when both sides of the
    /// match are the same token. In a cross-asset match the maker filled the
    /// complementary token, so the reported side stays the taker's. That direction is
    /// the account's resulting exposure, so it is pinned here together with the fill
    /// economics rather than left to the report count alone.
    #[rstest]
    #[case::same_asset(false, OrderSide::Sell)]
    #[case::cross_asset(true, OrderSide::Buy)]
    fn owned_maker_fill_side_follows_the_matched_asset(
        #[case] cross_asset: bool,
        #[case] expected_side: OrderSide,
    ) {
        let (instruments, instrument) = mapped_instrument();
        instruments.insert(
            Ustr::from(COMPLEMENTARY_TOKEN),
            complementary_instrument(&instrument),
        );
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();

        if cross_asset {
            trade.maker_orders[0].asset_id = Ustr::from(COMPLEMENTARY_TOKEN);
        }

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an owned maker order must reconcile");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].order_side, expected_side);
        assert_eq!(reports[0].last_qty.as_decimal(), Decimal::from(25));
        assert_eq!(reports[0].last_px.as_decimal(), Decimal::new(5, 1));
        assert_eq!(reports[0].commission, Money::zero(Currency::pUSD()));
        assert_eq!(reports[0].liquidity_side, LiquiditySide::Maker);
    }

    /// The same cross-asset match under an unscoped request must still report the
    /// account's own maker order, which sits on the complementary token: checking
    /// ownership across the match must not make those fills unreportable.
    #[rstest]
    fn unscoped_cross_asset_match_reports_the_owned_complementary_order() {
        let (instruments, instrument) = mapped_instrument();
        let complementary = complementary_instrument(&instrument);
        instruments.insert(Ustr::from(COMPLEMENTARY_TOKEN), complementary.clone());
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        trade.maker_orders[0].asset_id = Ustr::from(COMPLEMENTARY_TOKEN);
        let owned_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("an unscoped request must report the owned complementary-token order");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned_order_id);
        assert_eq!(reports[0].instrument_id, complementary.id());
    }

    /// Reconciliation for signature type 2 had to be repaired because the venue's
    /// maker address there is not the configured funder, and identifying user fills
    /// by API key was added after address matching proved insufficient. Ownership
    /// must therefore resolve from the API key alone, with no address match.
    #[rstest]
    fn maker_order_owned_by_api_key_alone_is_reconciled() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].owner = USER_API_KEY.to_string();
        let owned_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("a maker order identified by API key must reconcile");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned_order_id);
    }

    /// A configured funder is commonly copied in mixed case while the venue reports
    /// lowercase. The address identity must still resolve, because the API-key
    /// identity stops matching once the CLOB credential is rotated.
    #[rstest]
    fn maker_ownership_resolves_a_mixed_case_configured_address() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        let owned_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());
        let ctx = FillContext {
            user_address: USER_ADDRESS_MIXED_CASE,
            ..fill_context()
        };

        let reports = build_fill_reports_from_trades(
            &[trade],
            &ctx,
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("address case must not decide maker-order ownership");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned_order_id);
    }

    /// The completeness failure aborts NT startup, so it must name both identities
    /// an operator needs to diagnose it, and never the API keys.
    #[rstest]
    fn unowned_maker_trade_failure_names_addresses_without_credentials() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        disown_maker_orders(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("an unowned confirmed maker trade must fail reconciliation");

        let rendered = error.to_string();
        assert!(rendered.contains("no owned maker order"), "{rendered}");
        assert!(rendered.contains(USER_ADDRESS), "{rendered}");
        assert!(rendered.contains(COUNTERPARTY_ADDRESS), "{rendered}");
        assert!(!rendered.contains(USER_API_KEY), "{rendered}");
        assert!(!rendered.contains(COUNTERPARTY_API_KEY), "{rendered}");
    }

    #[rstest]
    fn instrument_scoped_maker_reconciliation_ignores_unrelated_maker_orders() {
        let (instruments, instrument) = mapped_instrument();
        let requested = confirmed_maker_trade_for(&instrument);
        let mut unrelated = requested.clone();
        unrelated.id = "trade-unrelated".to_string();
        disown_maker_orders(&mut unrelated);
        for maker_order in &mut unrelated.maker_orders {
            maker_order.asset_id = Ustr::from(UNMAPPED_TOKEN);
        }

        let reports = build_fill_reports_from_trades(
            &[unrelated, requested],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::Instrument(instrument.id()),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("instrument-scoped reconciliation must ignore unrelated maker orders");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument.id());
    }

    #[rstest]
    fn targeted_maker_fill_rejects_asset_instrument_conflict() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        let target_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());
        trade.maker_orders[0].asset_id = Ustr::from(UNMAPPED_TOKEN);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::Order {
                instrument_id: instrument.id(),
                venue_order_id: target_order_id,
            },
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a targeted maker order on another asset must fail reconciliation");

        assert!(
            error
                .to_string()
                .contains("does not match requested NT instrument")
        );
    }

    /// A match carries counterparties' identifiers too, and `VenueOrderId` panics on
    /// a malformed value, so reconciliation must reject one rather than abort the
    /// process.
    #[rstest]
    #[case::maker_order(|trade: &mut PolymarketTradeReport| trade.maker_orders[1].order_id = String::new())]
    #[case::taker_order(|trade: &mut PolymarketTradeReport| {
        trade.trader_side = PolymarketLiquiditySide::Taker;
        trade.taker_order_id = String::new();
    })]
    fn rejects_a_malformed_venue_order_id(#[case] mutate: fn(&mut PolymarketTradeReport)) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.maker_orders[0].maker_address = USER_ADDRESS.to_string();
        mutate(&mut trade);

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a malformed venue order ID must fail reconciliation");

        assert!(
            error.to_string().contains("invalid venue order ID"),
            "{error}"
        );
    }

    #[rstest]
    fn rejects_confirmed_maker_trade_without_maker_orders() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.maker_orders.clear();

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("a confirmed maker trade must carry at least one maker order");

        assert!(error.to_string().contains("no owned maker order"));
    }

    #[rstest]
    fn malformed_counterparty_maker_order_leaves_owned_report_intact() {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        let owned_order_id = VenueOrderId::from(trade.maker_orders[0].order_id.as_str());
        trade.maker_orders[1].maker_address = COUNTERPARTY_ADDRESS.to_string();
        trade.maker_orders[1].owner = COUNTERPARTY_API_KEY.to_string();
        trade.maker_orders[1].matched_amount = Decimal::MAX;
        trade.maker_orders[1].asset_id = Ustr::from(UNMAPPED_TOKEN);

        let reports = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("counterparty maker data must not fail or alter the owned maker report");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id, owned_order_id);
        assert_eq!(reports[0].last_qty.as_decimal(), Decimal::from(25));
    }

    #[rstest]
    #[case::malformed("not-a-timestamp")]
    #[case::overflow("18446744073709551615")]
    fn rejects_invalid_confirmed_fill_timestamp(#[case] match_time: &str) {
        let (instruments, instrument) = mapped_instrument();
        let mut trade = confirmed_trade_for(&instrument);
        trade.match_time = match_time.to_string();

        let error = build_fill_reports_from_trades(
            &[trade],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::All,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("invalid confirmed-fill timestamps must fail reconciliation");

        assert!(error.to_string().contains("match_time"));
    }

    #[rstest]
    fn scoped_order_reconciliation_ignores_unrelated_unmapped_order() {
        let (instruments, instrument) = mapped_instrument();
        let mapped = open_order_for(&instrument);
        let mut unrelated = mapped.clone();
        unrelated.asset_id = Ustr::from("UNRELATED-UNMAPPED-TOKEN");

        let reports = build_order_reports_from_orders(
            &[unrelated, mapped],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            Some(instrument.id()),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("scoped reconciliation must ignore unrelated venue orders");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument.id());
    }

    #[rstest]
    fn scoped_fill_reconciliation_ignores_unrelated_unmapped_fill() {
        let (instruments, instrument) = mapped_instrument();
        let mapped = confirmed_trade_for(&instrument);
        let mut unrelated = mapped.clone();
        unrelated.asset_id = Ustr::from("UNRELATED-UNMAPPED-TOKEN");

        let reports = build_fill_reports_from_trades(
            &[unrelated, mapped],
            &fill_context(),
            &instruments,
            &FillReconciliationScope::Instrument(instrument.id()),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("scoped reconciliation must ignore unrelated venue fills");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument.id());
    }

    /// The venue bounds the request through `after`, but its documentation does not
    /// say which timestamp that filters on, so the window is enforced locally too.
    /// An obsolete confirmed trade must not be able to fail a bounded
    /// reconciliation it can no longer contribute to.
    #[rstest]
    #[case::before_the_window("1700000000", false)]
    #[case::inside_the_window("1900000000", true)]
    #[case::unparsable_is_kept("not-a-timestamp", true)]
    fn bounds_confirmed_trades_to_the_lookback_window(
        #[case] match_time: &str,
        #[case] expected: bool,
    ) {
        let (_, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.match_time = match_time.to_string();
        let cutoff = Some(UnixNanos::from(1_800_000_000_000_000_000u64));

        assert_eq!(is_within_lookback(&trade, cutoff), expected);
    }

    #[rstest]
    fn keeps_every_confirmed_trade_when_the_lookback_is_unbounded() {
        let (_, instrument) = mapped_instrument();
        let mut trade = confirmed_maker_trade_for(&instrument);
        trade.match_time = "1700000000".to_string();

        assert!(is_within_lookback(&trade, None));
    }

    #[rstest]
    #[case::unbounded(None, None)]
    #[case::one_minute(Some(1), Some(940_000_000_000u64))]
    #[case::saturates_at_epoch(Some(u64::MAX / 60_000_000_000), Some(0))]
    fn resolves_the_reconciliation_lookback_cutoff(
        #[case] lookback_mins: Option<u64>,
        #[case] expected: Option<u64>,
    ) {
        let now = UnixNanos::from(1_000_000_000_000u64);

        let cutoff =
            lookback_cutoff(now, lookback_mins).expect("a representable lookback resolves");

        assert_eq!(cutoff.map(|cutoff| cutoff.as_u64()), expected);
    }

    #[rstest]
    fn rejects_unrepresentable_reconciliation_lookback() {
        let error = lookback_cutoff(UnixNanos::from(1_000_000_000_000u64), Some(u64::MAX))
            .expect_err("an unrepresentable lookback must fail reconciliation");

        assert!(error.to_string().contains("overflows"));
    }

    #[rstest]
    fn rejects_negative_current_position_before_dust_filtering() {
        let position = DataApiPosition {
            asset: "token-1".to_string(),
            condition_id: "condition-1".to_string(),
            size: Decimal::NEGATIVE_ONE,
            avg_price: None,
        };

        let error = build_position_reports(
            &[position],
            &AtomicMap::new(),
            AccountId::from("POLYMARKET-001"),
            None,
            UnixNanos::from(1_000_000_000u64),
        )
        .expect_err("negative current positions must fail reconciliation");

        assert!(error.to_string().contains("negative Data API position"));
    }

    #[rstest]
    fn scoped_position_reconciliation_ignores_unrelated_invalid_position() {
        let (instruments, instrument) = mapped_instrument();
        let unrelated = DataApiPosition {
            asset: "UNRELATED-UNMAPPED-TOKEN".to_string(),
            condition_id: "condition-unrelated".to_string(),
            size: Decimal::NEGATIVE_ONE,
            avg_price: None,
        };
        let mapped = DataApiPosition {
            asset: instrument.raw_symbol().to_string(),
            condition_id: "condition-mapped".to_string(),
            size: Decimal::ONE,
            avg_price: None,
        };

        let reports = build_position_reports(
            &[unrelated, mapped],
            &instruments,
            AccountId::from("POLYMARKET-001"),
            Some(instrument.id()),
            UnixNanos::from(1_000_000_000u64),
        )
        .expect("scoped reconciliation must ignore unrelated positions");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].instrument_id, instrument.id());
    }

    #[rstest]
    fn rejects_unrepresentable_current_position() {
        let error = position_quantity(Decimal::MAX, 6, "condition-1", "token-1")
            .expect_err("an unrepresentable current position must fail reconciliation");

        assert!(
            error
                .to_string()
                .contains("cannot represent Data API position")
        );
    }

    /// An order the response carries no confirmed fill for is uncorroborated, not
    /// unfilled: its fills may simply predate the lookback window. Reporting zero
    /// filled against a venue that reports otherwise understates a live order's
    /// exposure, so the venue's quantity stands.
    #[rstest]
    fn preserves_filled_quantity_for_an_order_without_confirmed_fills() {
        let mut reports = vec![OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            VenueOrderId::from("V-OUTSIDE-WINDOW"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from("4.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &AHashSet::new()).unwrap();

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
        assert_eq!(reports[0].quantity, Quantity::from("10.0000"));
        assert_eq!(reports[0].order_status, OrderStatus::PartiallyFilled);
    }

    /// The venue counts a matched trade toward an order's filled size before that
    /// trade confirms, and the adapter defers fill quantity until confirmation, so an
    /// order with something still settling is held to its confirmed fills rather than
    /// treated as merely uncorroborated.
    #[rstest]
    fn holds_an_order_with_an_unsettled_trade_to_its_confirmed_fills() {
        let venue_order_id = VenueOrderId::from("V-SETTLING");
        let mut reports = vec![OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from("4.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let unsettled = AHashSet::from_iter([venue_order_id]);

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &unsettled).unwrap();

        assert_eq!(reports[0].filled_qty, Quantity::zero(4));
    }

    /// Only a trade that is still settling contributes an unsettled order ID, and
    /// both sides of the match are covered.
    #[rstest]
    fn collects_order_ids_from_unsettled_trades_only() {
        let (_, instrument) = mapped_instrument();
        let mut settling = confirmed_maker_trade_for(&instrument);
        settling.status = PolymarketTradeStatus::Matched;
        let confirmed = confirmed_maker_trade_for(&instrument);

        let unsettled = orders_with_unsettled_trades(&[settling.clone(), confirmed]);

        assert!(unsettled.contains(&VenueOrderId::from(settling.taker_order_id.as_str())));
        assert!(unsettled.contains(&VenueOrderId::from(
            settling.maker_orders[0].order_id.as_str()
        )));
        assert_eq!(unsettled.len(), 1 + settling.maker_orders.len());
    }

    /// Terminal-quantity normalization still applies to an uncorroborated report,
    /// because it only relates the venue's own quantity and filled quantity.
    #[rstest]
    fn normalizes_an_uncorroborated_terminal_report() {
        let mut reports = vec![OrderStatusReport::new(
            AccountId::from("POLY-001"),
            InstrumentId::from("TEST.POLYMARKET"),
            None,
            VenueOrderId::from("V-DUST-UNCORROBORATED"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("100.000"),
            Quantity::from("99.995"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &[], &AHashSet::new()).unwrap();

        assert_eq!(reports[0].quantity, Quantity::from("99.995"));
        assert_eq!(reports[0].filled_qty, Quantity::from("99.995"));
    }

    #[rstest]
    fn caps_order_report_to_confirmed_companion_fills() {
        let account_id = AccountId::from("POLY-001");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let venue_order_id = VenueOrderId::from("V-1");
        let mut reports = vec![OrderStatusReport::new(
            account_id,
            instrument_id,
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0000"),
            Quantity::from("10.0000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let fills = vec![FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::from("T-1"),
            OrderSide::Buy,
            Quantity::from("4.0000"),
            Price::from("0.5000"),
            Money::new(0.0, Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &AHashSet::new()).unwrap();

        assert_eq!(reports[0].filled_qty, Quantity::from("4.0000"));
    }

    #[rstest]
    #[case::below_threshold("99.995", "99.995")]
    #[case::at_threshold("99.990", "100.000")]
    fn normalizes_confirmed_dust_residual_to_order_quantity(
        #[case] confirmed: &str,
        #[case] expected_quantity: &str,
    ) {
        let account_id = AccountId::from("POLY-001");
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let venue_order_id = VenueOrderId::from("V-DUST");
        let mut reports = vec![OrderStatusReport::new(
            account_id,
            instrument_id,
            None,
            venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("100.000"),
            Quantity::from("100.000"),
            UnixNanos::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];
        let fills = vec![FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            TradeId::from("T-DUST"),
            OrderSide::Buy,
            Quantity::from(confirmed),
            Price::from("0.5000"),
            Money::zero(Currency::pUSD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(1),
            None,
        )];

        cap_order_reports_to_confirmed_fills(&mut reports, &fills, &AHashSet::new()).unwrap();

        assert_eq!(reports[0].quantity, Quantity::from(expected_quantity));
        assert_eq!(reports[0].filled_qty, Quantity::from(confirmed));
    }
}
