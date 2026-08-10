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

//! Execution state manager for live trading.
//!
//! This module provides the execution manager for reconciling execution state between
//! the local cache and connected venues, as well as purging old state during live trading.

use std::{cell::RefCell, fmt::Debug, rc::Rc, time::Duration};

use super::recency::RecencyMap;
use indexmap::{IndexMap, IndexSet};
use nautilus_common::{
    cache::Cache,
    clients::{DEFAULT_POSITION_RECONCILIATION_TOLERANCE, ExecutionClient},
    clock::Clock,
    enums::{LogColor, LogLevel},
    live::dst,
    log_info,
    messages::{
        AuthenticatedExecutionMassStatus, ExecutionReport,
        execution::{
            QueryOrder, TradingCommand,
            report::{
                GenerateOrderStatusReport, GenerateOrderStatusReports,
                GeneratePositionStatusReports,
            },
        },
    },
};
use nautilus_core::{
    UUID4, UnixNanos,
    datetime::{mins_to_nanos, mins_to_secs},
};
use nautilus_execution::{
    engine::{ExecutionEngine, ExecutionPositionProjection, PreparedExecutionReconciliation},
    reconciliation::{
        NormalizedExecutionMassStatus, calculate_reconciliation_price,
        create_position_reconciliation_venue_order_id, create_reconciliation_rejected,
        process_mass_status_for_reconciliation, reconcile_order_report,
    },
};
use nautilus_model::{
    enums::{OmsType, OrderSide, OrderStatus, OrderType, PositionSideSpecified, TimeInForce},
    events::{OrderCanceled, OrderEventAny, OrderFilled},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, TradeId, TraderId,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

/// Composite key identifying a position context by instrument and account.
///
/// Used to scope per-position reconciliation state (retry counters, activity
/// throttles, venue report lookups) so that multiple accounts holding the same
/// instrument do not share the same tracking entry.
pub type InstrumentAccountKey = (InstrumentId, AccountId);
type FillKey = (AccountId, InstrumentId, TradeId);

#[expect(clippy::too_many_arguments)]
fn build_cross_zero_leg_report(
    instrument: &InstrumentAny,
    account_id: AccountId,
    instrument_id: InstrumentId,
    order_side: OrderSide,
    quantity: Decimal,
    avg_px: Decimal,
    tag: &str,
    venue_position_id: Option<PositionId>,
    ts_now: UnixNanos,
    venue_ts_last: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let order_qty = Quantity::from_decimal_dp(quantity, instrument.size_precision())?;
    anyhow::ensure!(
        !order_qty.is_zero(),
        "Position correction quantity rounds to zero"
    );
    let fill_price = Price::from_decimal_dp(avg_px, instrument.price_precision())?;
    let venue_order_id = create_position_reconciliation_venue_order_id(
        account_id,
        instrument_id,
        order_side,
        OrderType::Market,
        order_qty,
        Some(fill_price),
        venue_position_id,
        Some(tag),
        venue_ts_last,
    );

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        None,
        venue_order_id,
        order_side,
        OrderType::Market,
        TimeInForce::Gtc,
        OrderStatus::Filled,
        order_qty,
        order_qty,
        ts_now,
        ts_now,
        ts_now,
        None,
    )
    .with_avg_px(avg_px);

    if let Some(venue_position_id) = venue_position_id {
        report = report.with_venue_position_id(venue_position_id);
    }

    Ok(report)
}

/// Execution clients responsible for reporting one cached entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReportClientCoverage {
    Resolved(IndexSet<ClientId>),
    Unresolved,
}

/// Result of a committed execution reconciliation.
#[derive(Debug, Default)]
pub struct ReconciliationResult {
    /// Order events generated during reconciliation.
    pub events: Vec<OrderEventAny>,
}

/// One normalized client snapshot prepared for an all-or-nothing startup transaction.
pub(crate) struct PreparedMassStatusReconciliation {
    prepared: PreparedExecutionReconciliation,
    venue: nautilus_model::identifiers::Venue,
    filtered_reports: usize,
    positions_created: usize,
}

#[derive(Debug, Default)]
pub(crate) struct OpenOrderReconciliationResult {
    pub events: Vec<OrderEventAny>,
    pub targeted_queries: Vec<TargetedOrderQuery>,
}

#[derive(Debug, Clone)]
pub(crate) struct TargetedOrderQuery {
    client_order_id: ClientOrderId,
    responsible_clients: IndexSet<ClientId>,
    command: GenerateOrderStatusReport,
}

impl TargetedOrderQuery {
    #[cfg(feature = "node")]
    pub(crate) const fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }
}

#[derive(Debug)]
pub(crate) struct TargetedOrderReportResult {
    client_order_id: ClientOrderId,
    report: Option<OrderStatusReport>,
    coverage_complete: bool,
}

/// Snapshot and command for one continuous open-order reconciliation check.
#[derive(Debug, Clone)]
pub(crate) struct OpenOrderReportCheck {
    pub command: GenerateOrderStatusReports,
    pub filtered_orders: Vec<OrderAny>,
    pub client_coverage: IndexMap<ClientOrderId, ReportClientCoverage>,
    pub start: Option<UnixNanos>,
}

/// Prepare-time state and command for one continuous position reconciliation check.
#[derive(Debug, Clone)]
pub(crate) struct PositionReportCheck {
    pub command: GeneratePositionStatusReports,
    pub client_coverage: IndexMap<InstrumentAccountKey, ReportClientCoverage>,
    pub activity_revisions: IndexMap<InstrumentAccountKey, u64>,
}

/// Position evidence paired with the execution client capability that produced it.
#[derive(Debug, Clone)]
pub(crate) struct SourcedPositionStatusReport {
    pub source_client_id: ClientId,
    pub report: PositionStatusReport,
}

/// Configuration for execution manager.
#[expect(
    clippy::struct_excessive_bools,
    reason = "config flags mirror the live execution engine configuration surface"
)]
#[derive(Debug, Clone)]
pub struct ExecutionManagerConfig {
    /// The trader ID for generated orders.
    pub trader_id: TraderId,
    /// If reconciliation is active at start-up.
    pub reconciliation: bool,
    /// Number of minutes to look back during reconciliation.
    pub lookback_mins: Option<u64>,
    /// Instrument IDs to include during reconciliation (empty => all).
    pub reconciliation_instrument_ids: IndexSet<InstrumentId>,
    /// Whether to filter position status reports during reconciliation.
    pub filter_position_reports: bool,
    /// Client order IDs excluded from reconciliation.
    pub filtered_client_order_ids: IndexSet<ClientOrderId>,
    /// Whether to generate missing orders from reports.
    pub generate_missing_orders: bool,
    /// The interval (milliseconds) between checking whether in-flight orders have exceeded their threshold.
    pub inflight_check_interval_ms: u32,
    /// Threshold in milliseconds for inflight order checks.
    pub inflight_threshold_ms: u64,
    /// Maximum liveness queries for an inflight order.
    /// Exhaustion does not infer a terminal order state.
    pub inflight_max_retries: u32,
    /// The interval (seconds) between checks for open orders at the venue.
    pub open_check_interval_secs: Option<f64>,
    /// The lookback minutes for open order checks.
    pub open_check_lookback_mins: Option<u64>,
    /// Threshold in nanoseconds before acting on venue discrepancies for open orders.
    pub open_check_threshold_ns: u64,
    /// Maximum retries before resolving an open order missing at the venue.
    pub open_check_missing_retries: u32,
    /// Whether open-order polling should only request open orders from the venue.
    pub open_check_open_only: bool,
    /// The maximum number of single-order queries per consistency check cycle.
    pub max_single_order_queries_per_cycle: u32,
    /// The delay (milliseconds) between consecutive single-order queries.
    pub single_order_query_delay_ms: u32,
    /// The interval (seconds) between checks for open positions at the venue.
    pub position_check_interval_secs: Option<f64>,
    /// The lookback minutes for position consistency checks.
    pub position_check_lookback_mins: u64,
    /// Threshold in nanoseconds before acting on venue discrepancies for positions.
    pub position_check_threshold_ns: u64,
    /// Maximum retries before stopping position discrepancy reconciliation.
    pub position_check_retries: u32,
    /// The time buffer (minutes) before closed orders can be purged.
    pub purge_closed_orders_buffer_mins: Option<u32>,
    /// The time buffer (minutes) before closed positions can be purged.
    pub purge_closed_positions_buffer_mins: Option<u32>,
    /// The time buffer (minutes) before account events can be purged.
    pub purge_account_events_lookback_mins: Option<u32>,
    /// If purge operations should also delete from the backing database.
    pub purge_from_database: bool,
}

impl Default for ExecutionManagerConfig {
    fn default() -> Self {
        Self {
            trader_id: TraderId::default(),
            reconciliation: true,
            lookback_mins: Some(60),
            reconciliation_instrument_ids: IndexSet::new(),
            filter_position_reports: false,
            filtered_client_order_ids: IndexSet::new(),
            generate_missing_orders: true,
            inflight_check_interval_ms: 2_000,
            inflight_threshold_ms: 5_000,
            inflight_max_retries: 5,
            open_check_interval_secs: None,
            open_check_lookback_mins: Some(60),
            open_check_threshold_ns: 5_000_000_000,
            open_check_missing_retries: 5,
            open_check_open_only: true,
            max_single_order_queries_per_cycle: 5,
            single_order_query_delay_ms: 100,
            position_check_interval_secs: None,
            position_check_lookback_mins: 60,
            position_check_threshold_ns: 60_000_000_000,
            position_check_retries: 3,
            purge_closed_orders_buffer_mins: None,
            purge_closed_positions_buffer_mins: None,
            purge_account_events_lookback_mins: None,
            purge_from_database: false,
        }
    }
}

impl ExecutionManagerConfig {
    /// Sets the trader ID on the configuration.
    #[must_use]
    pub fn with_trader_id(mut self, trader_id: TraderId) -> Self {
        self.trader_id = trader_id;
        self
    }
}

/// Information about an inflight order check.
#[derive(Debug, Clone)]
struct InflightCheck {
    pub submitted_at: dst::time::Instant,
    pub retry_count: u32,
    // `Instant` debug output is runtime-specific and intentionally only useful
    // as an opaque monotonic offset.
    pub last_query_at: Option<dst::time::Instant>,
}

/// Manager for execution state.
///
/// The `ExecutionManager` handles:
/// - Startup reconciliation to align state on system start.
/// - Continuous reconciliation of inflight orders.
/// - External order discovery and claiming.
/// - Fill report processing and validation.
/// - Purging of old orders, positions, and account events.
///
/// # Thread Safety
///
/// This struct is **not thread-safe** and is designed for single-threaded use within
/// an async runtime. Internal state is managed using `IndexMap` without synchronization,
/// and the `clock` and `cache` use `Rc<RefCell<>>` which provide runtime borrow checking
/// but no thread-safety guarantees.
///
/// If concurrent access is required, this struct must be wrapped in `Arc<Mutex<>>` or
/// similar synchronization primitives. Alternatively, ensure that all methods are called
/// from the same thread/task in the async runtime.
///
/// **Warning:** Concurrent mutable access to internal `IndexMaps` or concurrent borrows
/// of `RefCell` contents will cause runtime panics.
#[derive(Clone)]
pub struct ExecutionManager {
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    config: ExecutionManagerConfig,
    inflight_checks: IndexMap<ClientOrderId, InflightCheck>,
    processed_fills: RecencyMap<FillKey>,
    missing_order_retries: IndexMap<ClientOrderId, u32>,
    order_query_recency: RecencyMap<ClientOrderId>,
    order_local_activity: RecencyMap<ClientOrderId>,
    // Monotonic (`dst::time`) instants, not `self.clock`; see `record_position_activity`.
    position_local_activity: RecencyMap<InstrumentAccountKey>,
    position_local_activity_revisions: IndexMap<InstrumentAccountKey, u64>,
    position_reconciliation_states: IndexMap<InstrumentAccountKey, u32>,
    position_reconciliation_tolerances: IndexMap<AccountId, Decimal>,
    recent_fills_cache: RecencyMap<FillKey>,
    missing_order_coverage_warnings: IndexSet<ClientOrderId>,
    unresolved_order_coverage: IndexSet<ClientOrderId>,
    targeted_order_queries: IndexSet<ClientOrderId>,
}

impl Debug for ExecutionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ExecutionManager))
            .field("config", &self.config)
            .field("inflight_checks", &self.inflight_checks)
            .field("processed_fills", &self.processed_fills)
            .field("missing_order_retries", &self.missing_order_retries)
            .finish_non_exhaustive()
    }
}

impl ExecutionManager {
    /// Creates a new [`ExecutionManager`] instance.
    pub fn new(
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
        config: ExecutionManagerConfig,
    ) -> Self {
        Self {
            clock,
            cache,
            config,
            inflight_checks: IndexMap::new(),
            processed_fills: RecencyMap::default(),
            missing_order_retries: IndexMap::new(),
            order_query_recency: RecencyMap::default(),
            order_local_activity: RecencyMap::default(),
            position_local_activity: RecencyMap::default(),
            position_local_activity_revisions: IndexMap::new(),
            position_reconciliation_states: IndexMap::new(),
            position_reconciliation_tolerances: IndexMap::new(),
            recent_fills_cache: RecencyMap::default(),
            missing_order_coverage_warnings: IndexSet::new(),
            unresolved_order_coverage: IndexSet::new(),
            targeted_order_queries: IndexSet::new(),
        }
    }

    pub(crate) fn set_position_reconciliation_tolerance(
        &mut self,
        account_id: AccountId,
        tolerance: Decimal,
    ) {
        let tolerance = if tolerance < Decimal::ZERO {
            log::error!(
                "Invalid negative position reconciliation tolerance {tolerance} for \
                 {account_id}; using the default"
            );
            DEFAULT_POSITION_RECONCILIATION_TOLERANCE
        } else {
            tolerance
        };
        self.position_reconciliation_tolerances
            .insert(account_id, tolerance);
    }

    fn position_reconciliation_tolerance(&self, account_id: AccountId) -> Decimal {
        self.position_reconciliation_tolerances
            .get(&account_id)
            .copied()
            .unwrap_or(DEFAULT_POSITION_RECONCILIATION_TOLERANCE)
    }

    /// Reconciles orders and fills from a mass status report.
    ///
    /// Order events are collected, sorted globally by `ts_event`, then processed through
    /// the execution engine to ensure chronological ordering across all orders.
    /// Position events are processed after all order events to ensure fills are applied first.
    ///
    /// # Errors
    ///
    /// Returns an error when the mass status source or any child report lies outside the
    /// registered execution client's account or venue authority.
    pub fn reconcile_execution_mass_status(
        &mut self,
        normalized: NormalizedExecutionMassStatus,
        exec_engine: &Rc<RefCell<ExecutionEngine>>,
    ) -> anyhow::Result<ReconciliationResult> {
        let prepared = self.prepare_execution_mass_status(normalized, exec_engine)?;
        self.commit_execution_mass_statuses(vec![prepared], exec_engine)
    }

    /// Prepares one client snapshot without mutating execution state.
    pub(crate) fn prepare_execution_mass_status(
        &self,
        normalized: NormalizedExecutionMassStatus,
        exec_engine: &Rc<RefCell<ExecutionEngine>>,
    ) -> anyhow::Result<PreparedMassStatusReconciliation> {
        let source_client_id = normalized.source_client_id();
        let source_id = normalized.source_id();
        let mass_status = normalized.into_mass_status()?;
        let venue = mass_status.venue;
        let order_count = mass_status.order_reports().len();
        let fill_count: usize = mass_status.fill_reports().values().map(Vec::len).sum();
        let position_count: usize = mass_status.position_reports().values().map(Vec::len).sum();

        exec_engine
            .borrow()
            .publish_reconciliation_evidence(&mass_status);

        log_info!(
            "Reconciling ExecutionMassStatus for {venue}",
            color = LogColor::Blue
        );
        log_info!(
            "Received {order_count} order(s), {fill_count} fill(s), {position_count} position(s)",
            color = LogColor::Blue
        );

        let (adjusted_orders, adjusted_fills) = self.adjust_mass_status_fills(&mass_status);
        let filtered_venue_order_ids = self
            .cache
            .borrow()
            .orders(None, None, None, None, None)
            .into_iter()
            .filter(|order| {
                self.config
                    .filtered_client_order_ids
                    .contains(&order.client_order_id())
            })
            .flat_map(|order| {
                order
                    .venue_order_ids()
                    .into_iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<IndexSet<_>>();
        let mut scoped_orders = Vec::new();
        let mut excluded_venue_order_ids = IndexSet::new();
        let mut filtered_reports = 0usize;

        for report in adjusted_orders.into_values() {
            if self.should_skip_order_report(&report)
                || filtered_venue_order_ids.contains(&report.venue_order_id)
            {
                excluded_venue_order_ids.insert(report.venue_order_id);
                filtered_reports += 1;
            } else {
                scoped_orders.push(report);
            }
        }

        let mut scoped_fills = Vec::new();
        for fill in adjusted_fills.into_values().flatten() {
            let filtered_client_order = fill.client_order_id.is_some_and(|client_order_id| {
                self.config
                    .filtered_client_order_ids
                    .contains(&client_order_id)
            }) || filtered_venue_order_ids
                .contains(&fill.venue_order_id);
            if excluded_venue_order_ids.contains(&fill.venue_order_id)
                || filtered_client_order
                || !self.should_reconcile_instrument(&fill.instrument_id)
            {
                filtered_reports += 1;
            } else {
                scoped_fills.push(fill);
            }
        }

        let mut scoped_positions = Vec::new();
        for report in mass_status.position_reports().into_values().flatten() {
            if self.config.filter_position_reports
                || !self.should_reconcile_instrument(&report.instrument_id)
            {
                filtered_reports += 1;
            } else {
                scoped_positions.push(report);
            }
        }

        let mut adjusted_mass_status = ExecutionMassStatus::new(
            mass_status.client_id,
            mass_status.account_id,
            mass_status.venue,
            mass_status.ts_init,
            Some(mass_status.report_id),
        );
        adjusted_mass_status.add_order_reports(scoped_orders)?;
        adjusted_mass_status.add_fill_reports(scoped_fills);
        adjusted_mass_status.add_position_reports(scoped_positions.clone());

        let normalized = {
            let engine = exec_engine.borrow();
            engine.normalize_authenticated_execution_mass_status(
                AuthenticatedExecutionMassStatus::new(
                    source_client_id,
                    source_id,
                    adjusted_mass_status,
                ),
            )?
        };
        let projection = {
            let engine = exec_engine.borrow();
            engine.project_execution_reconciliation(normalized.clone())?
        };
        let corrections = self.plan_position_corrections(&scoped_positions, &projection)?;
        let positions_created = corrections.len();
        let position_tolerance = self.position_reconciliation_tolerance(mass_status.account_id);

        let prepared = {
            let engine = exec_engine.borrow();
            engine.prepare_execution_reconciliation(normalized, corrections, position_tolerance)?
        };

        Ok(PreparedMassStatusReconciliation {
            prepared,
            venue,
            filtered_reports,
            positions_created,
        })
    }

    /// Commits prepared client snapshots as one execution transaction.
    pub(crate) fn commit_execution_mass_statuses(
        &mut self,
        prepared: Vec<PreparedMassStatusReconciliation>,
        exec_engine: &Rc<RefCell<ExecutionEngine>>,
    ) -> anyhow::Result<ReconciliationResult> {
        let venues = prepared
            .iter()
            .map(|snapshot| snapshot.venue.to_string())
            .collect::<IndexSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(",");
        let filtered_reports = prepared
            .iter()
            .map(|snapshot| snapshot.filtered_reports)
            .sum::<usize>();
        let positions_created = prepared
            .iter()
            .map(|snapshot| snapshot.positions_created)
            .sum::<usize>();
        let prepared = {
            let engine = exec_engine.borrow();
            engine.combine_execution_reconciliations(
                prepared
                    .into_iter()
                    .map(|snapshot| snapshot.prepared)
                    .collect(),
            )?
        };
        let receipt = exec_engine
            .borrow_mut()
            .commit_execution_reconciliation(prepared)?;
        let external_orders_created = receipt.external_orders.len();
        let events = receipt.events;
        let fills_applied = events
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Filled(_)))
            .count();
        let orders_reconciled = events
            .iter()
            .map(OrderEventAny::client_order_id)
            .collect::<IndexSet<_>>()
            .len();

        for event in &events {
            if let OrderEventAny::Filled(fill) = event {
                let fill_key = (fill.account_id, fill.instrument_id, fill.trade_id);
                if self.is_fill_applied(fill, fill_key) {
                    self.processed_fills.mark(fill_key);
                }
            }
        }

        if filtered_reports > 0 {
            log::debug!("{filtered_reports} reconciliation reports skipped by explicit config");
        }

        log::info!(
            color = LogColor::Blue as u8;
            "Reconciliation complete for [{venues}]: reconciled={orders_reconciled}, external={external_orders_created}, fills={fills_applied}, positions={positions_created}, filtered={filtered_reports}",
        );

        Ok(ReconciliationResult { events })
    }

    fn plan_position_corrections(
        &self,
        reports: &[PositionStatusReport],
        projection: &ExecutionPositionProjection,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut corrections = Vec::new();
        let ts_now = self.clock.borrow().timestamp_ns();

        for report in reports {
            let projected = projection.state_for(report)?;
            let venue_quantity = report.signed_decimal_qty;
            let tolerance = self.position_reconciliation_tolerance(report.account_id);
            if (projected.signed_quantity - venue_quantity).abs() <= tolerance {
                continue;
            }
            anyhow::ensure!(
                self.config.generate_missing_orders,
                "Position {}/{} differs from venue and correction generation is disabled",
                report.account_id,
                report.instrument_id,
            );
            let instrument = self.get_instrument(&report.instrument_id).ok_or_else(|| {
                anyhow::anyhow!("Instrument {} is not loaded", report.instrument_id)
            })?;

            if !venue_quantity.is_zero() {
                let venue_open_price = report.avg_px_open.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Open venue position {}/{} has no average price",
                        report.account_id,
                        report.instrument_id,
                    )
                })?;
                Price::from_decimal_dp(venue_open_price, instrument.price_precision()).map_err(
                    |_| {
                        anyhow::anyhow!(
                            "Venue average price {venue_open_price} is not representable for {}",
                            report.instrument_id
                        )
                    },
                )?;
            }

            let crosses_zero = !projected.signed_quantity.is_zero()
                && !venue_quantity.is_zero()
                && (projected.signed_quantity > Decimal::ZERO) != (venue_quantity > Decimal::ZERO);
            if crosses_zero {
                let close_price = projected.average_open_price.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Projected open position {}/{} has no average price",
                        report.account_id,
                        report.instrument_id,
                    )
                })?;
                let open_price = report
                    .avg_px_open
                    .expect("validated non-zero venue position");
                let close_side = if projected.signed_quantity > Decimal::ZERO {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };
                let open_side = if venue_quantity > Decimal::ZERO {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                corrections.push(build_cross_zero_leg_report(
                    &instrument,
                    report.account_id,
                    report.instrument_id,
                    close_side,
                    projected.signed_quantity.abs(),
                    close_price,
                    "CLOSE",
                    report.venue_position_id,
                    ts_now,
                    report.ts_last,
                )?);
                corrections.push(build_cross_zero_leg_report(
                    &instrument,
                    report.account_id,
                    report.instrument_id,
                    open_side,
                    venue_quantity.abs(),
                    open_price,
                    "OPEN",
                    report.venue_position_id,
                    ts_now,
                    report.ts_last,
                )?);
                continue;
            }

            let quantity_difference = venue_quantity - projected.signed_quantity;
            let order_side = if quantity_difference > Decimal::ZERO {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let order_quantity =
                Quantity::from_decimal_dp(quantity_difference.abs(), instrument.size_precision())?;
            anyhow::ensure!(
                !order_quantity.is_zero(),
                "Position correction for {} rounds to zero",
                report.instrument_id
            );
            let average_price = calculate_reconciliation_price(
                projected.signed_quantity,
                projected.average_open_price,
                venue_quantity,
                report.avg_px_open,
            )
            .or(report.avg_px_open)
            .or(projected.average_open_price)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Position correction for {} has no economic price",
                    report.instrument_id
                )
            })?;
            let fill_price = Price::from_decimal_dp(average_price, instrument.price_precision())?;
            let venue_order_id = create_position_reconciliation_venue_order_id(
                report.account_id,
                report.instrument_id,
                order_side,
                OrderType::Market,
                order_quantity,
                Some(fill_price),
                report.venue_position_id,
                None,
                report.ts_last,
            );
            let mut correction = OrderStatusReport::new(
                report.account_id,
                report.instrument_id,
                None,
                venue_order_id,
                order_side,
                OrderType::Market,
                TimeInForce::Gtc,
                OrderStatus::Filled,
                order_quantity,
                order_quantity,
                ts_now,
                ts_now,
                ts_now,
                None,
            )
            .with_avg_px(average_price);
            if let Some(position_id) = report.venue_position_id {
                correction = correction.with_venue_position_id(position_id);
            }
            corrections.push(correction);
        }

        Ok(corrections)
    }

    /// Returns bounded liveness queries for unresolved inflight orders.
    ///
    /// `QueryOrder` is not authoritative evidence: a missing response may be a
    /// transport or parsing failure. Terminal absence is resolved exclusively by
    /// the bulk-plus-targeted report path, which carries explicit client coverage.
    pub fn check_inflight_orders(&mut self) -> Vec<TradingCommand> {
        let mut queries = Vec::new();
        let now = dst::time::Instant::now();
        let threshold = Duration::from_millis(self.config.inflight_threshold_ms);

        let mut to_check = Vec::new();

        for (client_order_id, check) in &self.inflight_checks {
            if now
                .checked_duration_since(check.submitted_at)
                .is_some_and(|elapsed| elapsed > threshold)
            {
                to_check.push(*client_order_id);
            }
        }

        for client_order_id in to_check {
            if self
                .config
                .filtered_client_order_ids
                .contains(&client_order_id)
            {
                self.clear_recon_tracking(&client_order_id, true);
                continue;
            }

            if self.targeted_order_queries.contains(&client_order_id) {
                continue;
            }

            let Some(order) = self.get_order(client_order_id) else {
                self.clear_recon_tracking(&client_order_id, true);
                continue;
            };
            if !matches!(
                order.status(),
                OrderStatus::Submitted | OrderStatus::PendingUpdate | OrderStatus::PendingCancel
            ) {
                self.clear_recon_tracking(&client_order_id, true);
                continue;
            }

            if let Some(check) = self.inflight_checks.get_mut(&client_order_id) {
                if let Some(last_query_at) = check.last_query_at
                    && now
                        .checked_duration_since(last_query_at)
                        .is_none_or(|elapsed| elapsed < threshold)
                {
                    continue;
                }

                if check.retry_count >= self.config.inflight_max_retries {
                    continue;
                }

                check.retry_count = check.retry_count.saturating_add(1);
                check.last_query_at = Some(now);
                self.order_query_recency.mark(client_order_id);
                if check.retry_count == self.config.inflight_max_retries {
                    log::warn!(
                        "Inflight query budget exhausted for {client_order_id}; awaiting authoritative order-report coverage"
                    );
                }

                let ts_now = self.clock.borrow().timestamp_ns();
                let client_id = self.cache.borrow().client_id(&client_order_id).copied();
                queries.push(TradingCommand::QueryOrder(QueryOrder::new(
                    order.trader_id(),
                    client_id,
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    order.venue_order_id(),
                    UUID4::new(),
                    ts_now,
                    None,
                    None,
                )));
            }
        }

        queries
    }

    fn filtered_open_orders_for_reconciliation(&self) -> Vec<OrderAny> {
        {
            let cache = self.cache.borrow();
            let mut orders = cache.orders_open(None, None, None, None, None);
            orders.extend(cache.orders_inflight(None, None, None, None, None));
            let mut seen_client_order_ids = IndexSet::new();
            orders.retain(|order| seen_client_order_ids.insert(order.client_order_id()));

            if self.config.reconciliation_instrument_ids.is_empty() {
                orders.iter().map(|o| (*o).clone()).collect()
            } else {
                orders
                    .iter()
                    .filter(|o| {
                        self.config
                            .reconciliation_instrument_ids
                            .contains(&o.instrument_id())
                    })
                    .map(|o| (*o).clone())
                    .collect()
            }
        }
    }

    fn open_position_keys_for_reconciliation(&self) -> IndexSet<InstrumentAccountKey> {
        let cache = self.cache.borrow();
        let positions = cache.positions_open(None, None, None, None, None);
        let mut position_keys = IndexSet::new();

        for position in positions {
            if !self.should_reconcile_instrument(&position.instrument_id) {
                continue;
            }

            position_keys.insert((position.instrument_id, position.account_id));
        }

        position_keys
    }

    /// Prepares a bulk open-order report request and snapshots cached open orders.
    pub(crate) fn prepare_open_order_report_check(
        &mut self,
        command_id: UUID4,
        clients: &[&dyn ExecutionClient],
    ) -> OpenOrderReportCheck {
        let filtered_orders = self.filtered_open_orders_for_reconciliation();
        let active_order_ids: IndexSet<ClientOrderId> = filtered_orders
            .iter()
            .map(|order| order.client_order_id())
            .collect();
        self.missing_order_coverage_warnings
            .retain(|client_order_id| active_order_ids.contains(client_order_id));
        self.unresolved_order_coverage
            .retain(|client_order_id| active_order_ids.contains(client_order_id));

        let mut client_coverage = IndexMap::new();

        for order in &filtered_orders {
            let client_order_id = order.client_order_id();
            let coverage = self.resolve_order_report_client_coverage(order, clients);

            match &coverage {
                ReportClientCoverage::Resolved(_) => {
                    if self
                        .unresolved_order_coverage
                        .shift_remove(&client_order_id)
                    {
                        self.missing_order_coverage_warnings
                            .shift_remove(&client_order_id);
                    }
                }
                ReportClientCoverage::Unresolved => {
                    self.unresolved_order_coverage.insert(client_order_id);
                }
            }

            client_coverage.insert(client_order_id, coverage);
        }

        log::debug!(
            "Found {} order{} open in cache",
            filtered_orders.len(),
            if filtered_orders.len() == 1 { "" } else { "s" }
        );

        let ts_now = self.clock.borrow().timestamp_ns();
        let start = self.config.open_check_lookback_mins.map(|mins| {
            let lookback_ns = mins_to_nanos(mins);
            ts_now.saturating_sub_ns(lookback_ns)
        });

        let mut command = GenerateOrderStatusReports::new(
            command_id,
            ts_now,
            self.config.open_check_open_only,
            None,
            start,
            None,
            None,
            None,
        );
        command.log_receipt_level = LogLevel::Debug;

        OpenOrderReportCheck {
            command,
            filtered_orders,
            client_coverage,
            start,
        }
    }

    fn resolve_order_report_client_coverage(
        &self,
        order: &OrderAny,
        clients: &[&dyn ExecutionClient],
    ) -> ReportClientCoverage {
        if let Some(client_id) = self.cache.borrow().client_id(&order.client_order_id()) {
            return ReportClientCoverage::Resolved(IndexSet::from([*client_id]));
        }

        if let Some(account_id) = order.account_id() {
            let account_clients = clients
                .iter()
                .filter(|client| client.account_id() == account_id)
                .map(|client| client.client_id())
                .collect::<IndexSet<_>>();

            if !account_clients.is_empty() {
                return ReportClientCoverage::Resolved(account_clients);
            }
        }

        let venue_clients = clients
            .iter()
            .filter(|client| client.handles_order_venue(order.instrument_id().venue))
            .map(|client| client.client_id())
            .collect::<IndexSet<_>>();

        if venue_clients.is_empty() {
            ReportClientCoverage::Unresolved
        } else {
            ReportClientCoverage::Resolved(venue_clients)
        }
    }

    /// Builds per-order venue queries for fallback open-order reconciliation.
    pub fn check_open_order_queries(&mut self) -> Vec<TradingCommand> {
        self.check_open_order_queries_for_clients(None)
    }

    pub(crate) fn check_open_order_queries_for_clients(
        &mut self,
        client_ids: Option<&IndexSet<ClientId>>,
    ) -> Vec<TradingCommand> {
        let now = dst::time::Instant::now();
        let query_delay = Duration::from_millis(u64::from(self.config.single_order_query_delay_ms));
        let query_limit = self.config.max_single_order_queries_per_cycle as usize;

        if query_limit == 0 {
            return Vec::new();
        }

        let mut filtered_orders = self.filtered_open_orders_for_reconciliation();
        filtered_orders.sort_by_key(|order| {
            let client_order_id = order.client_order_id();
            (
                self.order_query_recency.last_marked(&client_order_id),
                client_order_id,
            )
        });

        let mut queries = Vec::new();

        for order in filtered_orders {
            if queries.len() >= query_limit {
                break;
            }

            let client_order_id = order.client_order_id();
            let client_id = self.cache.borrow().client_id(&client_order_id).copied();

            if let Some(client_ids) = client_ids
                && !client_id.is_some_and(|client_id| client_ids.contains(&client_id))
            {
                continue;
            }

            if self
                .config
                .filtered_client_order_ids
                .contains(&client_order_id)
            {
                continue;
            }

            let threshold = Duration::from_nanos(self.config.open_check_threshold_ns);
            if let Some(elapsed) = self.order_local_activity.elapsed_at(&client_order_id, now)
                && elapsed < threshold
            {
                let elapsed_ms = elapsed.as_millis();
                let threshold_ms = threshold.as_millis();
                log::debug!(
                    "Deferring open order query for {client_order_id}: recent local activity \
                     ({elapsed_ms}ms < threshold={threshold_ms}ms)",
                );
                continue;
            }

            if self
                .order_query_recency
                .within_at(&client_order_id, now, query_delay)
            {
                continue;
            }

            self.order_query_recency.mark(client_order_id);
            let ts_now = self.clock.borrow().timestamp_ns();

            let cmd = TradingCommand::QueryOrder(QueryOrder::new(
                order.trader_id(),
                client_id,
                order.strategy_id(),
                order.instrument_id(),
                client_order_id,
                order.venue_order_id(),
                UUID4::new(),
                ts_now,
                None,
                None,
            ));
            queries.push(cmd);
        }

        queries
    }

    /// Checks open orders consistency between cache and venue.
    ///
    /// This method validates that open orders in the cache match the venue's state,
    /// comparing order status and filled quantities, and generating reconciliation
    /// events for any discrepancies detected.
    ///
    /// # Returns
    ///
    /// A vector of order events generated to reconcile discrepancies.
    pub async fn check_open_orders(
        &mut self,
        clients: &[&dyn ExecutionClient],
    ) -> Vec<OrderEventAny> {
        log::debug!("Checking order consistency between cached-state and venues");

        let check = self.prepare_open_order_report_check(UUID4::new(), clients);
        let mut all_reports = Vec::new();
        let mut queried_clients = IndexSet::new();
        let mut failed_clients = IndexSet::new();

        for client in clients {
            let client_id = client.client_id();
            queried_clients.insert(client_id);

            match client.generate_order_status_reports(&check.command).await {
                Ok(reports) => {
                    all_reports.extend(reports);
                }
                Err(e) => {
                    failed_clients.insert(client_id);
                    log::warn!(
                        "Failed to query order reports from {}: {e}",
                        client.client_id()
                    );
                }
            }
        }

        let result = self.reconcile_open_order_reports(
            &check,
            all_reports,
            &queried_clients,
            &failed_clients,
        );
        let mut events = result.events;

        if !result.targeted_queries.is_empty() {
            let query_delay =
                Duration::from_millis(u64::from(self.config.single_order_query_delay_ms));
            let query_results =
                request_targeted_order_reports(clients, result.targeted_queries, query_delay).await;
            events.extend(self.reconcile_targeted_order_reports(query_results));
        }

        events
    }

    /// Reconciles bulk open-order report responses against a cached order snapshot.
    pub(crate) fn reconcile_open_order_reports(
        &mut self,
        check: &OpenOrderReportCheck,
        all_reports: Vec<OrderStatusReport>,
        queried_clients: &IndexSet<ClientId>,
        failed_clients: &IndexSet<ClientId>,
    ) -> OpenOrderReconciliationResult {
        let mut venue_reported_ids = IndexSet::new();

        for report in &all_reports {
            if let Some(client_order_id) = &report.client_order_id {
                venue_reported_ids.insert(*client_order_id);
                self.missing_order_coverage_warnings
                    .shift_remove(client_order_id);
                // A positive report is proof the venue still knows the order:
                // reset the missing-order ladder so only consecutive misses
                // accumulate (mirrors the Python engine's per-report clear).
                self.missing_order_retries.shift_remove(client_order_id);
            } else {
                let mapped_client_order_id = self
                    .cache
                    .borrow()
                    .client_order_id(&report.venue_order_id)
                    .copied();

                // The mapped order was positively reported: it must receive
                // the full positive-report bookkeeping or the missing-order
                // loop below immediately re-increments the cleared counter.
                if let Some(client_order_id) = mapped_client_order_id {
                    venue_reported_ids.insert(client_order_id);
                    self.missing_order_coverage_warnings
                        .shift_remove(&client_order_id);
                    self.missing_order_retries.shift_remove(&client_order_id);
                }
            }
        }

        let mut events = Vec::new();
        let mut targeted_candidates = Vec::new();

        for report in all_reports {
            if let Some(client_order_id) = &report.client_order_id
                && let Some(order) = self.get_order(*client_order_id)
            {
                // Check for recent local activity to avoid race conditions with in-flight fills
                let threshold = Duration::from_nanos(self.config.open_check_threshold_ns);
                if let Some(elapsed) = self.order_local_activity.elapsed(client_order_id)
                    && elapsed < threshold
                {
                    let elapsed_ms = elapsed.as_millis();
                    let threshold_ms = threshold.as_millis();
                    log::debug!(
                        "Deferring reconciliation for {client_order_id}: recent local activity ({elapsed_ms}ms < threshold={threshold_ms}ms)",
                    );
                    continue;
                }

                let instrument = self.get_instrument(&report.instrument_id);

                if let Some(event) =
                    self.reconcile_order_report(&order, &report, instrument.as_ref())
                {
                    events.push(event);
                }
            }
        }

        // Handle orders missing at venue (skip in open_only mode where the
        // venue response may omit recently closed orders). When a lookback
        // window is set, only consider orders within that window so older
        // GTC orders outside the query range are not falsely marked missing.
        if self.config.open_check_open_only {
            let cached_ids: IndexSet<ClientOrderId> = check
                .filtered_orders
                .iter()
                .map(|o| o.client_order_id())
                .collect();
            let missing_at_venue: IndexSet<ClientOrderId> = cached_ids
                .difference(&venue_reported_ids)
                .copied()
                .collect();

            if !missing_at_venue.is_empty() {
                log::debug!(
                    "{} cached open order{} not present in venue current response",
                    missing_at_venue.len(),
                    if missing_at_venue.len() == 1 {
                        " is"
                    } else {
                        "s are"
                    },
                );

                for client_order_id in missing_at_venue {
                    log::debug!("Cached open order missing from venue response: {client_order_id}");
                }
            }
        } else {
            let candidates: Vec<&OrderAny> = if let Some(cutoff) = check.start {
                check
                    .filtered_orders
                    .iter()
                    .filter(|o| o.ts_last() >= cutoff)
                    .collect()
            } else {
                check.filtered_orders.iter().collect()
            };

            for order in candidates {
                let client_order_id = order.client_order_id();
                if venue_reported_ids.contains(&client_order_id) {
                    continue;
                }

                let coverage = check
                    .client_coverage
                    .get(&client_order_id)
                    .unwrap_or(&ReportClientCoverage::Unresolved);

                let ReportClientCoverage::Resolved(responsible_clients) = coverage else {
                    if self.missing_order_coverage_warnings.insert(client_order_id) {
                        log::warn!(
                            "Skipping order reconciliation for {client_order_id}: responsible execution client coverage is unresolved"
                        );
                    }
                    continue;
                };

                if responsible_clients.is_empty() {
                    if self.missing_order_coverage_warnings.insert(client_order_id) {
                        log::warn!(
                            "Skipping order reconciliation for {client_order_id}: responsible execution client coverage is unresolved"
                        );
                    }
                    continue;
                }

                let missing_clients = responsible_clients
                    .difference(queried_clients)
                    .copied()
                    .collect::<IndexSet<_>>();

                if !missing_clients.is_empty() {
                    if self.missing_order_coverage_warnings.insert(client_order_id) {
                        log::warn!(
                            "Skipping order reconciliation for {client_order_id}: responsible execution clients were not queried: {missing_clients:?}"
                        );
                    }
                    continue;
                }

                let failed_responsible_clients = responsible_clients
                    .intersection(failed_clients)
                    .copied()
                    .collect::<IndexSet<_>>();

                if !failed_responsible_clients.is_empty() {
                    log::warn!(
                        "Skipping order reconciliation for {client_order_id}: failed to query responsible execution clients: {failed_responsible_clients:?}"
                    );
                    continue;
                }

                self.missing_order_coverage_warnings
                    .shift_remove(&client_order_id);
                if let Some(order) = self.prepare_missing_order_query(client_order_id) {
                    targeted_candidates.push((order, responsible_clients.clone()));
                }
            }
        }

        targeted_candidates.sort_by_key(|(order, _)| {
            let client_order_id = order.client_order_id();
            (
                self.order_query_recency.last_marked(&client_order_id),
                client_order_id,
            )
        });

        let query_limit = self.config.max_single_order_queries_per_cycle as usize;
        let mut planned_queries = 0usize;
        let mut cap_deferred_orders = 0usize;
        let mut targeted_queries = Vec::new();

        for (order, responsible_clients) in targeted_candidates {
            let client_order_id = order.client_order_id();

            let required_queries = responsible_clients.len();
            let exceeds_query_limit = planned_queries + required_queries > query_limit;
            let can_run_oversized_group = planned_queries == 0 && query_limit > 0;
            if required_queries == 0 || (exceeds_query_limit && !can_run_oversized_group) {
                cap_deferred_orders += 1;
                continue;
            }

            if required_queries > query_limit {
                log::warn!(
                    "Targeted order query for {client_order_id} requires {required_queries} responsible clients, exceeding the per-cycle limit {query_limit} to avoid indefinite deferral"
                );
            }

            planned_queries += required_queries;
            self.order_query_recency.mark(client_order_id);
            self.targeted_order_queries.insert(client_order_id);
            targeted_queries.push(TargetedOrderQuery {
                client_order_id,
                responsible_clients,
                command: GenerateOrderStatusReport::new(
                    UUID4::new(),
                    self.clock.borrow().timestamp_ns(),
                    Some(order.instrument_id()),
                    Some(client_order_id),
                    order.venue_order_id(),
                    None,
                    None,
                ),
            });
        }

        if cap_deferred_orders > 0 {
            log::warn!(
                "Reached max single-order queries ({query_limit}) this cycle, deferring {cap_deferred_orders} order(s)"
            );
        }

        OpenOrderReconciliationResult {
            events,
            targeted_queries,
        }
    }

    pub(crate) fn reconcile_targeted_order_reports(
        &mut self,
        results: Vec<TargetedOrderReportResult>,
    ) -> Vec<OrderEventAny> {
        let mut events = Vec::new();

        for result in results {
            let client_order_id = result.client_order_id;
            self.targeted_order_queries.shift_remove(&client_order_id);

            if let Some(report) = result.report {
                self.missing_order_retries.shift_remove(&client_order_id);
                self.missing_order_coverage_warnings
                    .shift_remove(&client_order_id);

                let Some(order) = self.get_order(client_order_id) else {
                    continue;
                };
                let instrument = self.get_instrument(&report.instrument_id);

                log::info!(
                    color = LogColor::Blue as u8;
                    "Found {client_order_id} via targeted order status query: {}",
                    report.order_status,
                );

                if let Some(event) =
                    self.reconcile_order_report(&order, &report, instrument.as_ref())
                {
                    events.push(event);
                }
                continue;
            }

            if result.coverage_complete {
                events.extend(self.resolve_missing_order(client_order_id));
            } else {
                log::warn!(
                    "Deferring missing-order resolution for {client_order_id}: targeted order status coverage was incomplete"
                );
            }
        }

        events
    }

    /// Prepares a bulk position report request and records client coverage.
    #[must_use]
    pub(crate) fn prepare_position_report_check(
        &self,
        command_id: UUID4,
        clients: &[&dyn ExecutionClient],
    ) -> PositionReportCheck {
        let position_keys = self.open_position_keys_for_reconciliation();
        let client_coverage = position_keys
            .iter()
            .map(|key| {
                (
                    *key,
                    Self::resolve_position_report_client_coverage(*key, clients),
                )
            })
            .collect();
        let activity_revisions = position_keys
            .iter()
            .map(|key| (*key, self.position_activity_revision(key)))
            .collect();

        log::debug!(
            "Found {} unique instrument/account combination{} with open positions",
            position_keys.len(),
            if position_keys.len() == 1 { "" } else { "s" }
        );

        let mut command = GeneratePositionStatusReports::new(
            command_id,
            self.clock.borrow().timestamp_ns(),
            None, // instrument_id - query all
            None, // start
            None, // end
            None, // params
            None, // correlation_id
        );
        command.log_receipt_level = LogLevel::Debug;

        PositionReportCheck {
            command,
            client_coverage,
            activity_revisions,
        }
    }

    fn resolve_position_report_client_coverage(
        key: InstrumentAccountKey,
        clients: &[&dyn ExecutionClient],
    ) -> ReportClientCoverage {
        let account_clients = clients
            .iter()
            .filter(|client| {
                client.account_id() == key.1 && client.handles_order_venue(key.0.venue)
            })
            .map(|client| client.client_id())
            .collect::<IndexSet<_>>();

        if account_clients.is_empty() {
            ReportClientCoverage::Unresolved
        } else {
            ReportClientCoverage::Resolved(account_clients)
        }
    }

    /// Checks position consistency between cache and venue.
    ///
    /// This method validates that positions in the cache match the venue's state,
    /// detecting position drift and querying for missing fills when discrepancies
    /// are found.
    ///
    /// # Returns
    ///
    /// A vector of fill events generated to reconcile position discrepancies.
    pub async fn check_positions_consistency(
        &mut self,
        clients: &[&dyn ExecutionClient],
        exec_engine: Rc<RefCell<ExecutionEngine>>,
    ) -> Vec<OrderEventAny> {
        let check = self.prepare_position_report_check(UUID4::new(), clients);
        let mut reports = Vec::new();
        let mut queried_clients = IndexSet::new();
        let mut failed_clients = IndexSet::new();

        for client in clients {
            let client_id = client.client_id();
            queried_clients.insert(client_id);
            self.set_position_reconciliation_tolerance(
                client.account_id(),
                client.position_reconciliation_tolerance(),
            );

            match client
                .generate_position_status_reports(&check.command)
                .await
            {
                Ok(client_reports) => {
                    reports.extend(client_reports.into_iter().map(|report| {
                        SourcedPositionStatusReport {
                            source_client_id: client_id,
                            report,
                        }
                    }));
                }
                Err(e) => {
                    failed_clients.insert(client_id);
                    log::warn!(
                        "Failed to query position reports from {}: {e}",
                        client.client_id()
                    );
                }
            }
        }

        let result = self.reconcile_position_reports(
            &check,
            reports,
            &queried_clients,
            &failed_clients,
            clients,
            &mut exec_engine.borrow_mut(),
        );
        match result {
            Ok(events) => events,
            Err(error) => {
                log::error!("Periodic position reconciliation rejected: {error}");
                Vec::new()
            }
        }
    }

    /// Reconciles cached positions against venue position reports.
    pub(crate) fn reconcile_position_reports(
        &mut self,
        check: &PositionReportCheck,
        reports: Vec<SourcedPositionStatusReport>,
        queried_clients: &IndexSet<ClientId>,
        failed_clients: &IndexSet<ClientId>,
        clients: &[&dyn ExecutionClient],
        exec_engine: &mut ExecutionEngine,
    ) -> anyhow::Result<Vec<OrderEventAny>> {
        log::debug!("Checking position consistency between cached-state and venues");

        let clients_by_id = clients
            .iter()
            .map(|client| (client.client_id(), *client))
            .collect::<IndexMap<_, _>>();
        let mut venue_positions =
            IndexMap::<InstrumentAccountKey, Vec<SourcedPositionStatusReport>>::new();

        for sourced in reports {
            let report = &sourced.report;
            if !self.should_reconcile_instrument(&report.instrument_id) {
                continue;
            }
            let source = clients_by_id
                .get(&sourced.source_client_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Position report source {} was not part of the authenticated query",
                        sourced.source_client_id,
                    )
                })?;
            let mut status = ExecutionMassStatus::new(
                sourced.source_client_id,
                report.account_id,
                report.instrument_id.venue,
                report.ts_init,
                None,
            );
            status.add_position_reports(vec![report.clone()]);
            exec_engine.normalize_execution_mass_status_from_client(*source, status)?;

            venue_positions
                .entry((report.instrument_id, report.account_id))
                .or_default()
                .push(sourced);
        }

        let current_position_keys = self.open_position_keys_for_reconciliation();
        let mut keys = check
            .client_coverage
            .keys()
            .copied()
            .collect::<IndexSet<_>>();
        keys.extend(venue_positions.keys().copied());
        let mut prepared = Vec::new();
        let mut reconciled_keys = Vec::new();
        let mut preparation_failures = Vec::new();

        for key in keys {
            if check
                .activity_revisions
                .get(&key)
                .is_some_and(|prepared_revision| {
                    self.position_activity_revision(&key) > *prepared_revision
                })
            {
                log::debug!(
                    "Deferring position reconciliation for {}/{}: local activity recorded during report request",
                    key.0,
                    key.1,
                );
                continue;
            }

            let sourced_reports = venue_positions.get(&key).cloned().unwrap_or_default();
            if !check.client_coverage.contains_key(&key) && current_position_keys.contains(&key) {
                log::debug!(
                    "Deferring position reconciliation for {}/{}: position opened after client coverage was recorded",
                    key.0,
                    key.1,
                );
                continue;
            }

            let (source_client_id, venue_reports) = if sourced_reports.is_empty() {
                let Some(ReportClientCoverage::Resolved(responsible_clients)) =
                    check.client_coverage.get(&key)
                else {
                    log::warn!(
                        "Skipping position reconciliation for {}/{}: responsible execution client coverage is unresolved",
                        key.0,
                        key.1,
                    );
                    continue;
                };
                if responsible_clients.is_empty()
                    || !responsible_clients.is_subset(queried_clients)
                    || !responsible_clients.is_disjoint(failed_clients)
                {
                    log::warn!(
                        "Skipping position reconciliation for {}/{}: authoritative client coverage is incomplete",
                        key.0,
                        key.1,
                    );
                    continue;
                }
                let source_client_id = *responsible_clients
                    .first()
                    .expect("non-empty responsible clients");
                let source = clients_by_id.get(&source_client_id).ok_or_else(|| {
                    anyhow::anyhow!("Missing queried position source {source_client_id}")
                })?;
                let ts_now = self.clock.borrow().timestamp_ns();
                let cached_positions = self
                    .cache
                    .borrow()
                    .positions_open(None, Some(&key.0), None, Some(&key.1), None)
                    .into_iter()
                    .map(|position| (*position).clone())
                    .collect::<Vec<_>>();
                let reports = if source.oms_type() == OmsType::Hedging {
                    cached_positions
                        .into_iter()
                        .map(|position| {
                            PositionStatusReport::new(
                                key.1,
                                key.0,
                                PositionSideSpecified::Flat,
                                Quantity::zero(0),
                                ts_now,
                                ts_now,
                                None,
                                Some(position.id),
                                None,
                            )
                        })
                        .collect()
                } else {
                    vec![PositionStatusReport::new(
                        key.1,
                        key.0,
                        PositionSideSpecified::Flat,
                        Quantity::zero(0),
                        ts_now,
                        ts_now,
                        None,
                        None,
                        None,
                    )]
                };
                (source_client_id, reports)
            } else {
                let source_client_id = sourced_reports[0].source_client_id;
                (
                    source_client_id,
                    sourced_reports
                        .into_iter()
                        .map(|sourced| sourced.report)
                        .collect(),
                )
            };

            let cached_positions = self
                .cache
                .borrow()
                .positions_open(None, Some(&key.0), None, Some(&key.1), None)
                .into_iter()
                .map(|position| (*position).clone())
                .collect::<Vec<_>>();
            let (cached_signed_qty, cached_long_qty, cached_short_qty) =
                Self::position_qty_aggregates(
                    cached_positions.iter().map(Position::signed_decimal_qty),
                )?;
            let (venue_signed_qty, venue_long_qty, venue_short_qty) =
                Self::position_qty_aggregates(
                    venue_reports.iter().map(|report| report.signed_decimal_qty),
                )?;
            let tolerance = self.position_reconciliation_tolerance(key.1);
            let venue_has_side_reports = venue_reports.iter().any(PositionStatusReport::is_long)
                && venue_reports.iter().any(PositionStatusReport::is_short);
            if (cached_signed_qty - venue_signed_qty).abs() <= tolerance
                && (!venue_has_side_reports
                    || ((cached_long_qty - venue_long_qty).abs() <= tolerance
                        && (cached_short_qty - venue_short_qty).abs() <= tolerance))
            {
                self.position_reconciliation_states.shift_remove(&key);
                continue;
            }
            if self.position_local_activity.within(
                &key,
                Duration::from_nanos(self.config.position_check_threshold_ns),
            ) {
                continue;
            }
            if self.position_recon_retry_count(&key) >= self.config.position_check_retries {
                continue;
            }

            let source = clients_by_id.get(&source_client_id).ok_or_else(|| {
                anyhow::anyhow!("Missing queried position source {source_client_id}")
            })?;
            let mut status = ExecutionMassStatus::new(
                source_client_id,
                key.1,
                key.0.venue,
                self.clock.borrow().timestamp_ns(),
                None,
            );
            status.add_position_reports(venue_reports.clone());
            let transaction = (|| {
                let normalized = exec_engine
                    .normalize_execution_mass_status_from_client(*source, status.clone())?;
                let projection = exec_engine.project_execution_reconciliation(normalized)?;
                let corrections = self.plan_position_corrections(&venue_reports, &projection)?;
                let normalized =
                    exec_engine.normalize_execution_mass_status_from_client(*source, status)?;
                exec_engine.prepare_execution_reconciliation(normalized, corrections, tolerance)
            })();
            match transaction {
                Ok(transaction) => {
                    prepared.push(transaction);
                    reconciled_keys.push(key);
                }
                Err(error) => preparation_failures.push((key, error)),
            }
        }

        let active_keys: IndexSet<InstrumentAccountKey> = current_position_keys
            .into_iter()
            .chain(
                venue_positions
                    .iter()
                    .filter(|(_, reports)| {
                        reports
                            .iter()
                            .any(|sourced| sourced.report.signed_decimal_qty != Decimal::ZERO)
                    })
                    .map(|(k, _)| *k),
            )
            .collect();
        self.position_reconciliation_states
            .retain(|k, _| active_keys.contains(k));

        if !preparation_failures.is_empty() {
            let mut messages = Vec::new();
            for (key, error) in preparation_failures {
                let retries = self
                    .position_recon_retry_count(&key)
                    .saturating_add(1)
                    .min(self.config.position_check_retries);
                self.position_reconciliation_states.insert(key, retries);
                messages.push(format!("{}/{}: {error}", key.0, key.1));
            }
            anyhow::bail!(
                "Periodic position reconciliation was not committable: {}",
                messages.join("; ")
            );
        }

        let transaction = exec_engine.combine_execution_reconciliations(prepared)?;
        let events = exec_engine
            .commit_execution_reconciliation(transaction)?
            .events;
        for key in reconciled_keys {
            self.position_reconciliation_states.shift_remove(&key);
        }

        Ok(events)
    }

    /// Registers an order as inflight for tracking.
    pub fn register_inflight(&mut self, client_order_id: ClientOrderId) {
        if self
            .config
            .filtered_client_order_ids
            .contains(&client_order_id)
        {
            return;
        }

        self.inflight_checks.insert(
            client_order_id,
            InflightCheck {
                submitted_at: dst::time::Instant::now(),
                retry_count: 0,
                last_query_at: None,
            },
        );
        self.order_query_recency.remove(&client_order_id);
        self.order_local_activity.remove(&client_order_id);
    }

    /// Records local activity for the specified order.
    ///
    /// Uses a monotonic receipt instant, not venue or domain time, to accurately
    /// track when we last processed activity for this order. This avoids race
    /// conditions where network/queue latency makes events appear "old" even
    /// though they just arrived.
    pub fn record_local_activity(&mut self, client_order_id: ClientOrderId) {
        self.order_local_activity.mark(client_order_id);
    }

    /// Clears reconciliation tracking state for an order.
    pub fn clear_recon_tracking(&mut self, client_order_id: &ClientOrderId, drop_last_query: bool) {
        self.inflight_checks.shift_remove(client_order_id);
        self.missing_order_retries.shift_remove(client_order_id);
        self.missing_order_coverage_warnings
            .shift_remove(client_order_id);
        self.unresolved_order_coverage.shift_remove(client_order_id);
        self.targeted_order_queries.shift_remove(client_order_id);

        if drop_last_query {
            self.order_query_recency.remove(client_order_id);
        }
        self.order_local_activity.remove(client_order_id);
    }

    #[cfg(feature = "node")]
    pub(crate) fn remove_targeted_order_queries(&mut self, client_order_ids: &[ClientOrderId]) {
        for client_order_id in client_order_ids {
            self.targeted_order_queries.shift_remove(client_order_id);
        }
    }

    /// Records position activity for reconciliation tracking, scoped per (instrument, account).
    ///
    /// The activity is stamped from the monotonic `dst::time` clock (real elapsed
    /// time), **not** from `self.clock` and **not** from the venue event's
    /// `ts_event`. The position-discrepancy grace is a real-time settling window:
    /// give the local pipeline a moment to catch up before flagging a
    /// cache-vs-venue gap. That is inherently wall/monotonic time; you want N
    /// real seconds of cover regardless of the trading clock's epoch or speed.
    /// `self.clock` can be driven off wall time (e.g. an accelerated simulated
    /// venue), which would shrink the window by the clock's speed; the venue
    /// `ts_event` lives on yet another axis. Measuring against the same monotonic
    /// clock the reconciliation loop already schedules on keeps the grace honest.
    /// Periodic reconciliation reads this same monotonic activity record before
    /// preparing any correction transaction.
    pub fn record_position_activity(&mut self, instrument_id: InstrumentId, account_id: AccountId) {
        let key = (instrument_id, account_id);
        self.position_local_activity.mark(key);
        let revision = self
            .position_local_activity_revisions
            .entry(key)
            .or_default();
        *revision = revision.saturating_add(1);
    }

    fn position_activity_revision(&self, key: &InstrumentAccountKey) -> u64 {
        self.position_local_activity_revisions
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    /// Returns the current position-reconciliation retry count for the given
    /// `(instrument, account)` key, or zero if no entry exists.
    #[must_use]
    pub fn position_recon_retry_count(&self, key: &InstrumentAccountKey) -> u32 {
        self.position_reconciliation_states
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    /// Returns the current missing-order reconciliation retry count for the
    /// given client order ID, or zero if no entry exists.
    #[must_use]
    pub fn missing_order_retry_count(&self, client_order_id: &ClientOrderId) -> u32 {
        self.missing_order_retries
            .get(client_order_id)
            .copied()
            .unwrap_or(0)
    }

    /// Observes a local order event and updates tracking state.
    ///
    /// This is the `LiveNode` dispatch path for order events: acknowledgement
    /// events clear reconciliation tracking, fills record position
    /// activity, and every event stamps local activity. The stamp must come
    /// AFTER any [`Self::clear_recon_tracking`] call - that call drops the
    /// local-activity mark, which is the sole grace gate protecting a
    /// just-acknowledged order from missing-order reconciliation while the
    /// venue report lags.
    pub fn observe_order_event(&mut self, event: &OrderEventAny) {
        match event {
            OrderEventAny::Filled(fill) => {
                self.record_position_activity(fill.instrument_id, fill.account_id);
            }
            OrderEventAny::Accepted(_)
            | OrderEventAny::Rejected(_)
            | OrderEventAny::Canceled(_)
            | OrderEventAny::Expired(_)
            | OrderEventAny::Denied(_)
            | OrderEventAny::Updated(_)
            | OrderEventAny::ModifyRejected(_)
            | OrderEventAny::CancelRejected(_) => {
                self.clear_recon_tracking(&event.client_order_id(), true);
            }
            _ => {}
        }

        self.record_local_activity(event.client_order_id());
    }

    /// Observes an incoming execution report and updates tracking state.
    ///
    /// This should be called **before** the report is dispatched to the execution
    /// engine, so that the manager's state is current when periodic checks run.
    ///
    /// Updates performed per report variant:
    /// - `Order`: updates reconciliation tracking based on order status
    /// - `Fill`: records order and position activity without marking the fill as processed
    /// - `OrderWithFills`: updates order tracking and records position activity per fill
    /// - `Position`: records position activity
    /// - `MassStatus`: no-op (handled separately via startup reconciliation)
    pub fn observe_execution_report(&mut self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Order(order_report) => {
                self.observe_order_status_report(order_report);
            }
            ExecutionReport::Fill(fill_report) => {
                let client_order_id = fill_report.client_order_id.or_else(|| {
                    self.cache
                        .borrow()
                        .client_order_id(&fill_report.venue_order_id)
                        .copied()
                });

                if let Some(coid) = client_order_id {
                    self.record_local_activity(coid);
                }
                self.record_position_activity(fill_report.instrument_id, fill_report.account_id);
            }
            ExecutionReport::OrderWithFills(order_report, fills) => {
                self.observe_order_status_report(order_report);

                for fill_report in fills {
                    self.record_position_activity(
                        fill_report.instrument_id,
                        fill_report.account_id,
                    );
                }
            }
            ExecutionReport::Position(position_report) => {
                self.record_position_activity(
                    position_report.instrument_id,
                    position_report.account_id,
                );
            }
            ExecutionReport::MassStatus(_) => {
                // Handled separately via reconcile_execution_mass_status
            }
        }
    }

    fn observe_order_status_report(&mut self, report: &OrderStatusReport) {
        let Some(client_order_id) = report.client_order_id else {
            return;
        };

        if !matches!(
            report.order_status,
            OrderStatus::PendingUpdate | OrderStatus::PendingCancel
        ) {
            self.clear_recon_tracking(&client_order_id, report.order_status.is_closed());
        }

        // Dispatch may suppress a terminal report, such as a stale cancel for the
        // old leg of a cancel-replace. Keep the settling grace until the node
        // confirms the cached order closed after dispatch.
        self.record_local_activity(client_order_id);
    }

    /// Checks if a fill has been recently processed (for deduplication).
    #[must_use]
    pub fn is_fill_recently_processed(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        trade_id: TradeId,
    ) -> bool {
        self.recent_fills_cache
            .contains_key(&(account_id, instrument_id, trade_id))
    }

    /// Marks a fill as recently processed with the current monotonic instant.
    pub fn mark_fill_processed(
        &mut self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        trade_id: TradeId,
    ) {
        self.recent_fills_cache
            .mark((account_id, instrument_id, trade_id));
    }

    /// Marks a fill as recently processed when it is present on its canonical order.
    pub fn commit_recent_fill_if_applied(&mut self, fill: &OrderFilled) {
        let fill_key = (fill.account_id, fill.instrument_id, fill.trade_id);
        if self.is_fill_applied(fill, fill_key) {
            self.mark_fill_processed(fill_key.0, fill_key.1, fill_key.2);
        }
    }

    /// Prunes expired fills from the recent fills cache.
    ///
    /// Default TTL is 60 seconds.
    pub fn prune_recent_fills_cache(&mut self, ttl_secs: f64) {
        // Map the f64 TTL to a Duration, reproducing the old
        // (ttl_secs * NANOSECONDS_IN_SECOND) as u64 cast at the boundaries
        // rather than panicking on this pub fn. The as cast saturated:
        //   - negative / NaN            -> 0        (prune everything)
        //   - positive overflow / +inf  -> u64::MAX (keep everything)
        // try_from_secs_f64 returns Err for all three, so branch on the sign
        // to keep the two behaviors distinct.
        let ttl = match Duration::try_from_secs_f64(ttl_secs) {
            Ok(ttl) => ttl,
            Err(_) if ttl_secs > 0.0 => Duration::MAX,
            Err(_) => Duration::ZERO,
        };

        self.recent_fills_cache.prune_older_than(ttl);
    }

    /// Prunes committed mass-reconciliation fills outside the startup report window.
    ///
    /// An unbounded startup lookback requires indefinite retention because no finite
    /// horizon can safely exclude a replayed fill report.
    pub fn prune_processed_fills(&mut self) {
        let Some(lookback_mins) = self.config.lookback_mins else {
            return;
        };

        let ttl = Duration::from_mins(lookback_mins).max(Duration::from_mins(1));
        self.processed_fills.prune_older_than(ttl);
    }

    /// Prunes order activity outside the continuous reconciliation settling window.
    pub fn prune_order_local_activity(&mut self) {
        self.order_local_activity
            .prune_older_than(Duration::from_nanos(self.config.open_check_threshold_ns));
    }

    /// Purges closed orders from the cache that are older than the configured buffer.
    pub fn purge_closed_orders(&mut self) {
        let Some(buffer_mins) = self.config.purge_closed_orders_buffer_mins else {
            return;
        };

        let ts_now = self.clock.borrow().timestamp_ns();
        let buffer_secs = mins_to_secs(u64::from(buffer_mins));

        self.cache
            .borrow_mut()
            .purge_closed_orders(ts_now, buffer_secs);
    }

    /// Purges closed positions from the cache that are older than the configured buffer.
    pub fn purge_closed_positions(&mut self) {
        let Some(buffer_mins) = self.config.purge_closed_positions_buffer_mins else {
            return;
        };

        let ts_now = self.clock.borrow().timestamp_ns();
        let buffer_secs = mins_to_secs(u64::from(buffer_mins));

        self.cache
            .borrow_mut()
            .purge_closed_positions(ts_now, buffer_secs);
    }

    /// Purges old account events from the cache based on the configured lookback.
    pub fn purge_account_events(&mut self) {
        let Some(lookback_mins) = self.config.purge_account_events_lookback_mins else {
            return;
        };

        let ts_now = self.clock.borrow().timestamp_ns();
        let lookback_secs = mins_to_secs(u64::from(lookback_mins));

        self.cache
            .borrow_mut()
            .purge_account_events(ts_now, lookback_secs);
    }

    // Private helper methods

    fn get_order(&self, client_order_id: ClientOrderId) -> Option<OrderAny> {
        self.cache
            .borrow()
            .order(&client_order_id)
            .map(|o| o.clone())
    }

    fn get_order_by_venue_order_id(&self, venue_order_id: VenueOrderId) -> Option<OrderAny> {
        let cache = self.cache.borrow();
        cache
            .client_order_id(&venue_order_id)
            .and_then(|client_order_id| cache.order(client_order_id).map(|o| o.clone()))
    }

    fn get_instrument(&self, instrument_id: &InstrumentId) -> Option<InstrumentAny> {
        self.cache.borrow().instrument(instrument_id).cloned()
    }

    fn should_skip_order_report(&self, report: &OrderStatusReport) -> bool {
        if let Some(client_order_id) = &report.client_order_id
            && self
                .config
                .filtered_client_order_ids
                .contains(client_order_id)
        {
            log::debug!(
                "Skipping order report {client_order_id}: in filtered_client_order_ids list"
            );
            return true;
        }

        if !self.should_reconcile_instrument(&report.instrument_id) {
            log::debug!(
                "Skipping order report for {}: not in reconciliation_instrument_ids",
                report.instrument_id
            );
            return true;
        }

        false
    }

    fn should_reconcile_instrument(&self, instrument_id: &InstrumentId) -> bool {
        self.config.reconciliation_instrument_ids.is_empty()
            || self
                .config
                .reconciliation_instrument_ids
                .contains(instrument_id)
    }

    fn prepare_missing_order_query(&mut self, client_order_id: ClientOrderId) -> Option<OrderAny> {
        let order = self.get_order(client_order_id)?;

        // The order may have closed while the report request was in flight;
        // the check must come before the retry increment or the stale empty
        // response recreates tracking state that nothing prunes afterwards.
        if order.status().is_closed() {
            log::debug!(
                "Skipping missing-order resolution for {client_order_id}: already {}",
                order.status()
            );
            self.clear_recon_tracking(&client_order_id, true);
            return None;
        }

        // Recent local activity is the real-time settling window for missing
        // orders. Venue/domain timestamps can be ahead of the trading clock and
        // must not stall reconciliation.
        if self.order_local_activity.within(
            &client_order_id,
            Duration::from_nanos(self.config.open_check_threshold_ns),
        ) {
            return None;
        }

        let retries = self
            .missing_order_retries
            .entry(client_order_id)
            .or_insert(0);
        *retries = retries.saturating_add(1);

        if *retries < self.config.open_check_missing_retries {
            log::debug!(
                "Order {} not found at venue, retry {}/{}",
                client_order_id,
                retries,
                self.config.open_check_missing_retries
            );
            return None;
        }

        Some(order)
    }

    fn resolve_missing_order(&mut self, client_order_id: ClientOrderId) -> Vec<OrderEventAny> {
        let mut events = Vec::new();

        let Some(order) = self.get_order(client_order_id) else {
            return events;
        };

        if order.status().is_closed() {
            log::debug!(
                "Skipping missing-order resolution for {client_order_id}: already {}",
                order.status()
            );
            self.clear_recon_tracking(&client_order_id, true);
            return events;
        }

        if self.order_local_activity.within(
            &client_order_id,
            Duration::from_nanos(self.config.open_check_threshold_ns),
        ) {
            log::debug!(
                "Deferring missing-order resolution for {client_order_id}: recent local activity"
            );
            return events;
        }

        let retries = self
            .missing_order_retries
            .get(&client_order_id)
            .copied()
            .unwrap_or_default();
        let ts_now = self.clock.borrow().timestamp_ns();

        match order.status() {
            OrderStatus::Accepted | OrderStatus::Submitted => {
                log::warn!(
                    "Order {client_order_id} not found at venue after {retries} retries and a targeted query, marking as REJECTED"
                );

                if let Some(rejected) =
                    create_reconciliation_rejected(&order, Some("NOT_FOUND_AT_VENUE"), ts_now)
                {
                    events.push(rejected);
                }
            }
            OrderStatus::PartiallyFilled => {
                log::warn!(
                    "Order {client_order_id} not found at venue after {retries} retries and a targeted query, marking as CANCELED"
                );
                events.push(OrderEventAny::Canceled(OrderCanceled::new(
                    order.trader_id(),
                    order.strategy_id(),
                    order.instrument_id(),
                    client_order_id,
                    UUID4::new(),
                    ts_now,
                    ts_now,
                    true,
                    order.venue_order_id(),
                    order.account_id(),
                )));
            }
            OrderStatus::PendingUpdate | OrderStatus::PendingCancel => {
                log::debug!(
                    "Deferring resolution for {client_order_id}: still inflight as {}",
                    order.status()
                );
                // Narrow tracking reset mirroring the Python engine:
                // zero the retry ladder and stamp the query time so the
                // inflight checker first observes a full threshold delay
                // and then retries from scratch. The order must stay
                // registered in `inflight_checks` - the inflight checker
                // walks that map, unlike Python which rescans cached
                // inflight orders every cycle - and keeps its
                // local-activity mark.
                self.missing_order_retries.shift_remove(&client_order_id);
                if let Some(check) = self.inflight_checks.get_mut(&client_order_id) {
                    check.retry_count = 0;
                    check.last_query_at = Some(dst::time::Instant::now());
                }
                self.order_query_recency.mark(client_order_id);
                return events;
            }
            status => {
                log::warn!(
                    "Skipping missing-order resolution for {client_order_id}: unexpected status {status}"
                );
            }
        }

        self.clear_recon_tracking(&client_order_id, true);
        events
    }

    fn position_qty_aggregates(
        mut signed_quantities: impl Iterator<Item = Decimal>,
    ) -> anyhow::Result<(Decimal, Decimal, Decimal)> {
        signed_quantities.try_fold(
            (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            |(net, long, short), qty| {
                if qty > Decimal::ZERO {
                    Ok((
                        net.checked_add(qty)
                            .ok_or_else(|| anyhow::anyhow!("Net position quantity overflows"))?,
                        long.checked_add(qty)
                            .ok_or_else(|| anyhow::anyhow!("Long position quantity overflows"))?,
                        short,
                    ))
                } else {
                    Ok((
                        net.checked_add(qty)
                            .ok_or_else(|| anyhow::anyhow!("Net position quantity overflows"))?,
                        long,
                        short
                            .checked_add(qty.abs())
                            .ok_or_else(|| anyhow::anyhow!("Short position quantity overflows"))?,
                    ))
                }
            },
        )
    }

    fn reconcile_order_report(
        &self,
        order: &OrderAny,
        report: &OrderStatusReport,
        instrument: Option<&InstrumentAny>,
    ) -> Option<OrderEventAny> {
        let ts_now = self.clock.borrow().timestamp_ns();
        reconcile_order_report(order, report, instrument, ts_now)
    }

    /// Adjusts fills for instruments with incomplete first lifecycle (partial window).
    ///
    /// When historical fills don't fully explain the current position (e.g., lookback window
    /// started mid-position), this creates synthetic fills to align with the venue position.
    fn adjust_mass_status_fills(
        &self,
        mass_status: &ExecutionMassStatus,
    ) -> (
        IndexMap<VenueOrderId, OrderStatusReport>,
        IndexMap<VenueOrderId, Vec<FillReport>>,
    ) {
        let mut final_orders: IndexMap<VenueOrderId, OrderStatusReport> =
            mass_status.order_reports();
        let mut final_fills: IndexMap<VenueOrderId, Vec<FillReport>> = mass_status.fill_reports();

        let mut instruments_to_adjust = Vec::new();

        for (instrument_id, position_reports) in mass_status.position_reports() {
            if !self.should_reconcile_instrument(&instrument_id) {
                log::debug!(
                    "Skipping fill adjustment for {instrument_id}: not in reconciliation_instrument_ids"
                );
                continue;
            }

            // Skip hedge mode instruments (have venue_position_id) as partial-window
            // adjustment assumes a single net position per instrument
            let is_hedge_mode = position_reports
                .iter()
                .any(|r| r.venue_position_id.is_some());

            if is_hedge_mode {
                log::debug!(
                    "Skipping fill adjustment for {instrument_id}: hedge mode (has venue_position_id)"
                );
                continue;
            }

            let has_retained_position = {
                let cache = self.cache.borrow();
                !cache
                    .positions_open(
                        None,
                        Some(&instrument_id),
                        None,
                        Some(&mass_status.account_id),
                        None,
                    )
                    .is_empty()
            };

            if has_retained_position {
                log::debug!(
                    "Skipping fill adjustment for {instrument_id}: retained open position in cache"
                );
                continue;
            }

            if let Some(instrument) = self.get_instrument(&instrument_id) {
                instruments_to_adjust.push(instrument);
            } else {
                log::debug!(
                    "Skipping fill adjustment for {instrument_id}: instrument not found in cache"
                );
            }
        }

        if instruments_to_adjust.is_empty() {
            return (final_orders, final_fills);
        }

        log_info!(
            "Adjusting fills for {} instrument(s) with position reports",
            instruments_to_adjust.len(),
            color = LogColor::Blue
        );

        for instrument in &instruments_to_adjust {
            let instrument_id = instrument.id();

            match process_mass_status_for_reconciliation(mass_status, instrument, None) {
                Ok(result) => {
                    final_orders.retain(|_, order| order.instrument_id != instrument_id);
                    final_fills.retain(|_, fills| {
                        fills
                            .first()
                            .is_none_or(|f| f.instrument_id != instrument_id)
                    });

                    for (venue_order_id, order) in result.orders {
                        final_orders.insert(venue_order_id, order);
                    }

                    for (venue_order_id, fills) in result.fills {
                        final_fills.insert(venue_order_id, fills);
                    }
                }
                Err(e) => {
                    log::warn!("Failed to adjust fills for {instrument_id}: {e}");
                }
            }
        }

        log_info!(
            "After adjustment: {} order(s), {} fill group(s)",
            final_orders.len(),
            final_fills.len(),
            color = LogColor::Blue
        );

        (final_orders, final_fills)
    }

    fn is_fill_applied(&self, fill: &OrderFilled, fill_key: FillKey) -> bool {
        self.get_order(fill.client_order_id)
            .or_else(|| self.get_order_by_venue_order_id(fill.venue_order_id))
            .is_some_and(|order| {
                order.account_id() == Some(fill_key.0)
                    && order.instrument_id() == fill_key.1
                    && order.trade_ids().contains(&&fill_key.2)
            })
    }
}

pub(crate) async fn request_targeted_order_reports(
    clients: &[&dyn ExecutionClient],
    queries: Vec<TargetedOrderQuery>,
    query_delay: Duration,
) -> Vec<TargetedOrderReportResult> {
    let mut results = Vec::with_capacity(queries.len());
    let mut request_count = 0usize;

    for query in queries {
        let mut report = None;
        let mut coverage_complete = true;

        for client_id in &query.responsible_clients {
            let client_id = *client_id;
            let Some(client) = clients
                .iter()
                .find(|client| client.client_id() == client_id)
            else {
                coverage_complete = false;
                log::warn!(
                    "Cannot run targeted order status query for {}: execution client {client_id} is unavailable",
                    query.client_order_id,
                );
                continue;
            };

            if request_count > 0 && !query_delay.is_zero() {
                dst::time::sleep(query_delay).await;
            }
            request_count += 1;

            match client.generate_order_status_report(&query.command).await {
                Ok(Some(candidate)) if targeted_report_matches(&query, &candidate) => {
                    report = Some(candidate);
                    break;
                }
                Ok(Some(candidate)) => {
                    coverage_complete = false;
                    log::warn!(
                        "Ignoring mismatched targeted order status report from {client_id} for {}: client_order_id={:?}, venue_order_id={}, instrument_id={}",
                        query.client_order_id,
                        candidate.client_order_id,
                        candidate.venue_order_id,
                        candidate.instrument_id,
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    coverage_complete = false;
                    log::warn!(
                        "Failed targeted order status query from {client_id} for {}: {e}",
                        query.client_order_id,
                    );
                }
            }
        }

        results.push(TargetedOrderReportResult {
            client_order_id: query.client_order_id,
            report,
            coverage_complete,
        });
    }

    results
}

fn targeted_report_matches(query: &TargetedOrderQuery, report: &OrderStatusReport) -> bool {
    let instrument_matches = query
        .command
        .instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id);
    let order_matches = report.client_order_id == Some(query.client_order_id)
        || query
            .command
            .venue_order_id
            .is_some_and(|venue_order_id| report.venue_order_id == venue_order_id);

    instrument_matches && order_matches
}

#[cfg(test)]
mod tests {
    use nautilus_common::clock::TestClock;
    use nautilus_core::datetime::NANOSECONDS_IN_SECOND;
    use nautilus_execution::{
        engine::stubs::StubExecutionClient, reconciliation::generate_reconciliation_order_events,
    };
    use nautilus_model::{
        enums::{OmsType, PositionSideSpecified},
        events::order::spec::{OrderPendingUpdateSpec, OrderUpdatedSpec},
        instruments::{
            Instrument,
            stubs::{crypto_perpetual_ethusdt, xbtusd_bitmex},
        },
        orders::{OrderTestBuilder, stubs::TestOrderEventStubs},
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_clear_recon_tracking_removes_targeted_query() {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(clock, cache, ExecutionManagerConfig::default());
        let client_order_id = ClientOrderId::from("O-TARGETED-CLEAR");
        manager.targeted_order_queries.insert(client_order_id);

        manager.clear_recon_tracking(&client_order_id, true);

        assert!(manager.targeted_order_queries.is_empty());
    }

    #[rstest]
    fn test_register_inflight_skips_filtered_order() {
        let client_order_id = ClientOrderId::from("O-FILTERED-REGISTER");
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock,
            cache,
            ExecutionManagerConfig {
                filtered_client_order_ids: IndexSet::from([client_order_id]),
                ..Default::default()
            },
        );

        manager.register_inflight(client_order_id);

        assert!(!manager.inflight_checks.contains_key(&client_order_id));
        assert!(!manager.missing_order_retries.contains_key(&client_order_id));
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_inflight_check_retires_order_filtered_after_registration() {
        let client_order_id = ClientOrderId::from("O-FILTERED-LATE");
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock,
            cache,
            ExecutionManagerConfig {
                inflight_threshold_ms: 100,
                ..Default::default()
            },
        );
        manager.register_inflight(client_order_id);
        manager
            .config
            .filtered_client_order_ids
            .insert(client_order_id);
        dst::time::sleep(Duration::from_millis(101)).await;

        let first = manager.check_inflight_orders();

        assert!(first.is_empty());
        assert!(!manager.inflight_checks.contains_key(&client_order_id));
        assert!(!manager.missing_order_retries.contains_key(&client_order_id));

        dst::time::sleep(Duration::from_millis(101)).await;
        let second = manager.check_inflight_orders();
        assert!(second.is_empty());
        assert!(!manager.inflight_checks.contains_key(&client_order_id));
    }

    #[rstest]
    #[case(false, OrderStatus::PendingUpdate, true, true, true)]
    #[case(false, OrderStatus::Accepted, false, true, true)]
    #[case(false, OrderStatus::Canceled, false, true, false)]
    #[case(true, OrderStatus::PendingCancel, true, true, true)]
    #[case(true, OrderStatus::Accepted, false, true, true)]
    #[case(true, OrderStatus::Filled, false, true, false)]
    fn test_observe_order_status_report_tracking_matrix(
        #[case] with_fills: bool,
        #[case] status: OrderStatus,
        #[case] expect_inflight: bool,
        #[case] expect_activity: bool,
        #[case] expect_last_query: bool,
    ) {
        let client_order_id = ClientOrderId::from("O-STATUS-MATRIX");
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(clock, cache, ExecutionManagerConfig::default());
        manager.register_inflight(client_order_id);
        manager.order_query_recency.mark(client_order_id);
        manager
            .missing_order_coverage_warnings
            .insert(client_order_id);
        manager.unresolved_order_coverage.insert(client_order_id);
        manager.targeted_order_queries.insert(client_order_id);
        let order_report = OrderStatusReport::new(
            AccountId::from("TEST-001"),
            crypto_perpetual_ethusdt().id(),
            Some(client_order_id),
            VenueOrderId::from("V-STATUS-MATRIX"),
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            status,
            Quantity::from("10.0"),
            Quantity::from("0.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            None,
        );
        let report = if with_fills {
            ExecutionReport::OrderWithFills(Box::new(order_report), Vec::new())
        } else {
            ExecutionReport::Order(Box::new(order_report))
        };

        manager.observe_execution_report(&report);

        assert_eq!(
            manager.inflight_checks.contains_key(&client_order_id),
            expect_inflight,
        );
        assert!(!manager.missing_order_retries.contains_key(&client_order_id));
        assert_eq!(
            manager.order_local_activity.contains_key(&client_order_id),
            expect_activity,
        );
        assert_eq!(
            manager.order_query_recency.contains_key(&client_order_id),
            expect_last_query,
        );
        assert_eq!(
            manager
                .missing_order_coverage_warnings
                .contains(&client_order_id),
            expect_inflight,
        );
        assert_eq!(
            manager.unresolved_order_coverage.contains(&client_order_id),
            expect_inflight,
        );
        assert_eq!(
            manager.targeted_order_queries.contains(&client_order_id),
            expect_inflight,
        );
    }

    #[rstest]
    fn test_superseded_cancel_report_preserves_missing_order_grace() {
        let client_order_id = ClientOrderId::from("O-CANCEL-REPLACE");
        let old_venue_order_id = VenueOrderId::from("V-CANCEL-REPLACE-OLD");
        let new_venue_order_id = VenueOrderId::from("V-CANCEL-REPLACE-NEW");
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("TEST");
        let instrument_id = crypto_perpetual_ethusdt().id();
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        insert_accepted_limit_order(
            &cache,
            client_order_id,
            old_venue_order_id,
            instrument_id,
            client_id,
        );

        let order = cache.borrow().order_owned(&client_order_id).unwrap();
        let pending_update = OrderPendingUpdateSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .account_id(account_id)
            .venue_order_id(old_venue_order_id)
            .build();
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::PendingUpdate(pending_update))
            .unwrap();
        let order = cache.borrow().order_owned(&client_order_id).unwrap();
        let updated = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .quantity(order.quantity())
            .venue_order_id(new_venue_order_id)
            .account_id(account_id)
            .build();
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Updated(updated))
            .unwrap();

        let mut manager = ExecutionManager::new(
            clock,
            cache.clone(),
            ExecutionManagerConfig {
                open_check_missing_retries: 1,
                ..Default::default()
            },
        );
        manager.record_local_activity(client_order_id);
        assert!(
            manager
                .prepare_missing_order_query(client_order_id)
                .is_none()
        );

        let report = OrderStatusReport::new(
            account_id,
            instrument_id,
            Some(client_order_id),
            old_venue_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Canceled,
            Quantity::from("10.0"),
            Quantity::from("0.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(2_000),
            UnixNanos::from(3_000),
            None,
        );

        manager.observe_execution_report(&ExecutionReport::Order(Box::new(report.clone())));
        let order = cache.borrow().order_owned(&client_order_id).unwrap();
        let events =
            generate_reconciliation_order_events(&order, &report, None, UnixNanos::from(1_000));

        assert!(events.is_empty());
        assert_eq!(order.status(), OrderStatus::Accepted);
        assert_eq!(order.venue_order_id(), Some(new_venue_order_id));
        assert!(manager.order_local_activity.contains_key(&client_order_id));
        assert!(
            manager
                .prepare_missing_order_query(client_order_id)
                .is_none()
        );
        assert_eq!(manager.missing_order_retry_count(&client_order_id), 0);
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_prune_order_local_activity_uses_open_check_threshold() {
        let old_id = ClientOrderId::from("O-ACTIVITY-OLD");
        let fresh_id = ClientOrderId::from("O-ACTIVITY-FRESH");
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock,
            cache,
            ExecutionManagerConfig {
                open_check_threshold_ns: 100_000_000,
                ..Default::default()
            },
        );
        manager.record_local_activity(old_id);
        dst::time::sleep(Duration::from_millis(101)).await;
        manager.record_local_activity(fresh_id);

        manager.prune_order_local_activity();

        assert!(!manager.order_local_activity.contains_key(&old_id));
        assert!(manager.order_local_activity.contains_key(&fresh_id));
    }

    #[rstest]
    fn test_prepare_open_order_report_check_builds_bulk_command_with_config() {
        let lookback_mins = 5_u64;
        let lookback_ns = lookback_mins * 60 * NANOSECONDS_IN_SECOND;
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock.clone(),
            cache.clone(),
            ExecutionManagerConfig {
                open_check_lookback_mins: Some(lookback_mins),
                open_check_open_only: false,
                reconciliation_instrument_ids: IndexSet::from([crypto_perpetual_ethusdt().id()]),
                ..Default::default()
            },
        );
        let included_id = ClientOrderId::from("O-REPORT-001");
        let excluded_id = ClientOrderId::from("O-REPORT-002");
        let included_instrument_id = crypto_perpetual_ethusdt().id();
        let excluded_instrument_id = xbtusd_bitmex().id();

        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(xbtusd_bitmex()))
            .unwrap();
        insert_accepted_limit_order(
            &cache,
            included_id,
            VenueOrderId::from("V-REPORT-001"),
            included_instrument_id,
            ClientId::from("BINANCE"),
        );
        insert_accepted_limit_order(
            &cache,
            excluded_id,
            VenueOrderId::from("V-REPORT-002"),
            excluded_instrument_id,
            ClientId::from("BITMEX"),
        );
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(lookback_ns * 2), true);

        let ts_now = clock.borrow().timestamp_ns();
        let command_id = UUID4::new();
        let check = manager.prepare_open_order_report_check(command_id, &[]);

        assert_eq!(check.command.command_id, command_id);
        assert_eq!(check.command.ts_init, ts_now);
        assert!(!check.command.open_only);
        assert_eq!(check.command.instrument_id, None);
        assert_eq!(
            check.command.start,
            Some(ts_now.saturating_sub_ns(lookback_ns))
        );
        assert_eq!(check.command.end, None);
        assert_eq!(check.command.log_receipt_level, LogLevel::Debug);
        assert_eq!(check.start, check.command.start);
        assert_eq!(check.filtered_orders.len(), 1);
        assert_eq!(check.filtered_orders[0].client_order_id(), included_id);
    }

    #[rstest]
    fn test_prepare_position_report_check_builds_bulk_command_with_coverage() {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let manager = ExecutionManager::new(
            clock.clone(),
            cache.clone(),
            ExecutionManagerConfig {
                reconciliation_instrument_ids: IndexSet::from([crypto_perpetual_ethusdt().id()]),
                ..Default::default()
            },
        );
        let included_instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let excluded_instrument = InstrumentAny::CryptoPerpetual(xbtusd_bitmex());

        cache
            .borrow_mut()
            .add_instrument(included_instrument.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument(excluded_instrument.clone())
            .unwrap();
        let included_position = insert_open_position(
            &cache,
            &included_instrument,
            PositionId::from("P-REPORT-001"),
            OrderSide::Buy,
            "5.0",
            "3000.00",
        );
        insert_open_position(
            &cache,
            &excluded_instrument,
            PositionId::from("P-REPORT-002"),
            OrderSide::Buy,
            "2.0",
            "40000.00",
        );

        let ts_now = clock.borrow().timestamp_ns();
        let command_id = UUID4::new();
        let check = manager.prepare_position_report_check(command_id, &[]);
        let key = (
            included_position.instrument_id,
            included_position.account_id,
        );

        assert_eq!(check.command.command_id, command_id);
        assert_eq!(check.command.ts_init, ts_now);
        assert_eq!(check.command.instrument_id, None);
        assert_eq!(check.command.start, None);
        assert_eq!(check.command.end, None);
        assert_eq!(check.command.log_receipt_level, LogLevel::Debug);
        assert_eq!(check.client_coverage.len(), 1);
        assert!(check.client_coverage.contains_key(&key));
        assert_eq!(check.activity_revisions.get(&key), Some(&0));
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_position_report_check_defers_activity_recorded_during_delayed_request() {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock.clone(),
            cache.clone(),
            ExecutionManagerConfig {
                position_check_threshold_ns: 5_000_000_000,
                ..Default::default()
            },
        );
        let mut exec_engine = ExecutionEngine::new(clock, cache.clone(), None);
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        let position = insert_open_position(
            &cache,
            &instrument,
            PositionId::from("P-ACTIVITY-DURING-REQUEST"),
            OrderSide::Buy,
            "5.0",
            "3000.00",
        );
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let account_id = position.account_id;
        let client = StubExecutionClient::new(
            ClientId::from("TEST"),
            account_id,
            instrument_id.venue,
            OmsType::Netting,
            None,
        );
        let clients: Vec<&dyn ExecutionClient> = vec![&client];
        let check = manager.prepare_position_report_check(UUID4::new(), &clients);
        let report = PositionStatusReport::new(
            account_id,
            instrument_id,
            PositionSideSpecified::Long,
            Quantity::from("5.0"),
            UnixNanos::from(1_000_000),
            UnixNanos::from(1_000_000),
            None,
            None,
            Some(Decimal::from(3000)),
        );

        let closed_position = close_long_position(
            position,
            &instrument,
            TradeId::from("T-ACTIVITY-DURING-REQUEST"),
        );
        cache
            .borrow_mut()
            .update_position(&closed_position)
            .unwrap();
        manager.record_position_activity(instrument_id, account_id);

        // Client A's report is already captured while client B holds the batch open.
        dst::time::sleep(Duration::from_secs(6)).await;

        let events = manager
            .reconcile_position_reports(
                &check,
                vec![SourcedPositionStatusReport {
                    source_client_id: client.client_id(),
                    report,
                }],
                &IndexSet::from([client.client_id()]),
                &IndexSet::new(),
                &clients,
                &mut exec_engine,
            )
            .unwrap();

        assert!(
            !events.iter().any(|event| {
                matches!(
                    event,
                    OrderEventAny::Filled(fill)
                        if fill.order_side == OrderSide::Buy
                            && fill.last_qty == Quantity::from("5.0")
                )
            }),
            "activity recorded after the request started must defer A's stale report",
        );
    }

    #[rstest]
    fn test_position_report_check_does_not_defer_activity_recorded_before_request() {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut manager = ExecutionManager::new(
            clock.clone(),
            cache.clone(),
            ExecutionManagerConfig {
                position_check_threshold_ns: 0,
                ..Default::default()
            },
        );
        let mut exec_engine = ExecutionEngine::new(clock, cache.clone(), None);
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        let position = insert_open_position(
            &cache,
            &instrument,
            PositionId::from("P-ACTIVITY-BEFORE-REQUEST"),
            OrderSide::Buy,
            "5.0",
            "3000.00",
        );
        cache.borrow_mut().add_instrument(instrument).unwrap();
        let account_id = position.account_id;
        manager.record_position_activity(instrument_id, account_id);
        let client = StubExecutionClient::new(
            ClientId::from("TEST"),
            account_id,
            instrument_id.venue,
            OmsType::Netting,
            None,
        );
        let clients: Vec<&dyn ExecutionClient> = vec![&client];
        let check = manager.prepare_position_report_check(UUID4::new(), &clients);
        let report = PositionStatusReport::new(
            account_id,
            instrument_id,
            PositionSideSpecified::Long,
            Quantity::from("10.0"),
            UnixNanos::from(1_000_000),
            UnixNanos::from(1_000_000),
            None,
            None,
            Some(Decimal::from(3000)),
        );

        let events = manager
            .reconcile_position_reports(
                &check,
                vec![SourcedPositionStatusReport {
                    source_client_id: client.client_id(),
                    report,
                }],
                &IndexSet::from([client.client_id()]),
                &IndexSet::new(),
                &clients,
                &mut exec_engine,
            )
            .unwrap();

        assert!(events.iter().any(|event| {
            matches!(
                event,
                OrderEventAny::Filled(fill)
                    if fill.order_side == OrderSide::Buy
                        && fill.last_qty == Quantity::from("5.0")
            )
        }));
    }

    fn insert_accepted_limit_order(
        cache: &Rc<RefCell<Cache>>,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        client_id: ClientId,
    ) {
        let account_id = AccountId::from("TEST-001");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(client_order_id)
            .instrument_id(instrument_id)
            .quantity(Quantity::from("10.0"))
            .price(Price::from("100.0"))
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, account_id);
        cache
            .borrow_mut()
            .add_order(order, None, Some(client_id), false)
            .unwrap();
        let order = cache.borrow_mut().update_order(&submitted).unwrap();
        let accepted = TestOrderEventStubs::accepted(&order, account_id, venue_order_id);
        cache.borrow_mut().update_order(&accepted).unwrap();
    }

    fn insert_open_position(
        cache: &Rc<RefCell<Cache>>,
        instrument: &InstrumentAny,
        position_id: PositionId,
        side: OrderSide,
        quantity: &str,
        price: &str,
    ) -> Position {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(side)
            .quantity(Quantity::from(quantity))
            .build();
        let fill = TestOrderEventStubs::filled(
            &order,
            instrument,
            Some(TradeId::new("T-REPORT-001")),
            Some(position_id),
            Some(Price::from(price)),
            Some(Quantity::from(quantity)),
            None,
            None,
            None,
            Some(AccountId::from("TEST-001")),
        );
        let order_filled: OrderFilled = fill.into();
        let position = Position::new(instrument, order_filled);
        cache
            .borrow_mut()
            .add_position(&position, OmsType::Hedging)
            .unwrap();
        position
    }

    fn close_long_position(
        mut position: Position,
        instrument: &InstrumentAny,
        trade_id: TradeId,
    ) -> Position {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(OrderSide::Sell)
            .quantity(position.quantity)
            .build();
        let fill = TestOrderEventStubs::filled(
            &order,
            instrument,
            Some(trade_id),
            Some(position.id),
            Some(Price::from("3000.00")),
            Some(position.quantity),
            None,
            None,
            None,
            Some(position.account_id),
        );
        let order_filled: OrderFilled = fill.into();
        position.apply(&order_filled);
        position
    }
}
