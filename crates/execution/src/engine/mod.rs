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

//! Provides a generic `ExecutionEngine` for all environments.
//!
//! The execution engines primary responsibility is to orchestrate interactions
//! between the `ExecutionClient` instances, and the rest of the platform. This
//! includes sending commands to, and receiving events from, the trading venue
//! endpoints via its registered execution clients.

pub mod config;
pub mod position;
pub mod stubs;

use std::{
    cell::{Cell, RefCell, RefMut},
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    rc::Rc,
    sync::LazyLock,
    time::SystemTime,
};

use ahash::AHashSet;
use config::ExecutionEngineConfig;
use futures::future::join_all;
use indexmap::{IndexMap, IndexSet};
use nautilus_common::{
    cache::{Cache, PositionRef},
    clients::ExecutionClient,
    clock::Clock,
    enums::LogColor,
    generators::position_id::PositionIdGenerator,
    log_info,
    logging::{CMD, EVT, RECV, SEND},
    messages::{
        AuthenticatedExecutionMassStatus, AuthenticatedExecutionReport, ExecutionReport,
        ExecutionSourceId,
        execution::{
            BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, ModifyOrder,
            QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList, TradingCommand,
        },
    },
    msgbus::{
        self, MessagingSwitchboard, TypedHandler, TypedIntoHandler, get_message_bus,
        switchboard::{self},
    },
    runner::{
        TradingCommandMessage, capture_trading_cmd, trading_cmd_is_dispatching,
        try_get_trading_cmd_sender,
    },
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{
    UUID4, UnixNanos, WeakCell,
    datetime::{mins_to_nanos, mins_to_secs, secs_to_nanos},
};
use nautilus_model::{
    accounts::Account,
    enums::{
        ContingencyType, OmsType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
        TrailingOffsetType,
    },
    events::{
        OrderAccepted, OrderDenied, OrderDeniedReason, OrderEvent, OrderEventAny, OrderFillVoided,
        OrderFilled, OrderInitialized, PositionChanged, PositionClosed, PositionEvent,
        PositionOpened,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId, Venue,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orderbook::own::{OwnBookOrder, OwnOrderBook, should_handle_own_book_order},
    orders::{Order, OrderAny, OrderError},
    position::{Position, PositionReplayEvent},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Money, Quantity},
};
use position::CorrectedPosition;
pub use position::{PositionStateSnapshot, SnapshotAnchorer};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    client::ExecutionClientAdapter,
    reconciliation::{
        NormalizedExecutionMassStatus, check_position_reconciliation,
        generate_external_order_status_events, generate_reconciliation_order_events,
        generate_reconciliation_order_pre_fill_events,
        generate_reconciliation_order_snapshot_events, is_superseded_cancel_report,
        reconcile_fill_report as reconcile_fill,
    },
};

const TIMER_SNAPSHOT_POSITIONS: &str = "ExecEngine_SNAPSHOT_POSITIONS";
const TIMER_PURGE_CLOSED_ORDERS: &str = "ExecEngine_PURGE_CLOSED_ORDERS";
const TIMER_PURGE_CLOSED_POSITIONS: &str = "ExecEngine_PURGE_CLOSED_POSITIONS";
const TIMER_PURGE_ACCOUNT_EVENTS: &str = "ExecEngine_PURGE_ACCOUNT_EVENTS";
static RECONCILIATION_VENUE_TAG: LazyLock<Ustr> = LazyLock::new(|| Ustr::from("VENUE"));
static RECONCILIATION_CORRECTION_TAG: LazyLock<Ustr> =
    LazyLock::new(|| Ustr::from("RECONCILIATION"));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconciliationOrderSource {
    Venue,
    PositionCorrection,
}

type ReconciliationFillKey = (AccountId, InstrumentId, TradeId);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconciliationFillDisposition {
    ApplyEconomics,
    ProjectOrderOnly,
}

#[derive(Clone)]
struct PreparedReconciliationEvent {
    event: OrderEventAny,
    fill_disposition: ReconciliationFillDisposition,
    oms_type: OmsType,
}

#[derive(Default)]
struct ReconciliationFillLedger {
    raw_fill_keys: AHashSet<ReconciliationFillKey>,
    position_fill_keys: AHashSet<ReconciliationFillKey>,
    missing_order_ids: AHashSet<(AccountId, InstrumentId, ClientOrderId)>,
    missing_venue_order_ids: AHashSet<(AccountId, InstrumentId, VenueOrderId)>,
    netting_lifecycle_starts: HashMap<(AccountId, InstrumentId, StrategyId), UnixNanos>,
    cached_fill_owners: HashMap<ReconciliationFillKey, ClientOrderId>,
}

impl ReconciliationOrderSource {
    fn tag(self) -> Ustr {
        match self {
            Self::Venue => *RECONCILIATION_VENUE_TAG,
            Self::PositionCorrection => *RECONCILIATION_CORRECTION_TAG,
        }
    }

    const fn is_position_correction(self) -> bool {
        matches!(self, Self::PositionCorrection)
    }
}

/// An external order whose construction and identity checks completed without
/// touching the live cache.
struct PreparedExternalOrder {
    initialized: OrderEventAny,
    order: OrderAny,
    venue_order_id: VenueOrderId,
    instrument_id: InstrumentId,
    strategy_id: StrategyId,
    ts_init: UnixNanos,
    reported_status: Option<OrderStatus>,
}

/// One venue-order projection within an authoritative snapshot.
struct PreparedOrderReconciliation {
    source_client_id: ClientId,
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
    venue_order_id_to_claim: Option<VenueOrderId>,
    external_order: Option<PreparedExternalOrder>,
    initial_order: OrderAny,
    report: Option<OrderStatusReport>,
    events: Vec<PreparedReconciliationEvent>,
}

enum PreparedOrderGroup {
    Reconcile(Box<PreparedOrderReconciliation>),
    Filtered(ClientOrderId, VenueOrderId, InstrumentId),
}

/// A snapshot projection that has been validated completely against cloned state.
struct ProjectedExecutionReconciliation {
    order_reconciliations: Vec<PreparedOrderReconciliation>,
    events: Vec<PreparedReconciliationEvent>,
    position_reports: Vec<PositionStatusReport>,
    position_projection: ExecutionPositionProjection,
    filtered_external_orders: Vec<(ClientOrderId, VenueOrderId, InstrumentId)>,
}

impl Debug for ProjectedExecutionReconciliation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedExecutionReconciliation")
            .field("order_groups", &self.order_reconciliations.len())
            .field("events", &self.events.len())
            .field("position_reports", &self.position_reports.len())
            .field("position_projection", &self.position_projection)
            .field(
                "filtered_external_orders",
                &self.filtered_external_orders.len(),
            )
            .finish()
    }
}

#[derive(Clone)]
struct ExecutionReconciliationRecipe {
    normalized: NormalizedExecutionMassStatus,
    corrections: Vec<OrderStatusReport>,
    position_tolerance: Decimal,
}

/// A sealed reconciliation transaction whose evidence is projected again at commit.
///
/// One transaction may contain snapshots from several execution clients. Re-projecting
/// the complete set synchronously under the mutable engine borrow prevents a caller from
/// committing events prepared against execution state that has since changed.
pub struct PreparedExecutionReconciliation {
    recipes: Vec<ExecutionReconciliationRecipe>,
}

impl Debug for PreparedExecutionReconciliation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedExecutionReconciliation")
            .field("snapshots", &self.recipes.len())
            .field(
                "corrections",
                &self
                    .recipes
                    .iter()
                    .map(|recipe| recipe.corrections.len())
                    .sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
}

/// Quantity and open-price state projected from a reconciliation snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProjectedPositionState {
    pub signed_quantity: Decimal,
    pub average_open_price: Option<Decimal>,
}

/// Position state after all prepared order and fill events, without live mutation.
#[derive(Clone, Debug, Default)]
pub struct ExecutionPositionProjection {
    positions: IndexMap<PositionId, Position>,
}

impl ExecutionPositionProjection {
    /// Returns the projected state addressed by a venue position report.
    ///
    /// # Errors
    ///
    /// Returns an error when aggregate position quantity or notional cannot be represented.
    pub fn state_for(
        &self,
        report: &PositionStatusReport,
    ) -> anyhow::Result<ProjectedPositionState> {
        let positions = self.positions.values().filter(|position| {
            position.account_id == report.account_id
                && position.instrument_id == report.instrument_id
                && report
                    .venue_position_id
                    .is_none_or(|position_id| position.id == position_id)
        });
        let mut signed_quantity = Decimal::ZERO;
        let mut weighted_open_price = Decimal::ZERO;
        let mut priced_quantity = Decimal::ZERO;

        for position in positions {
            let quantity = position.signed_decimal_qty();
            signed_quantity = signed_quantity.checked_add(quantity).ok_or_else(|| {
                anyhow::anyhow!(
                    "Projected position quantity overflows for {}/{}",
                    report.account_id,
                    report.instrument_id,
                )
            })?;
            if !quantity.is_zero()
                && let Some(open_price) = Decimal::from_f64_retain(position.avg_px_open)
            {
                let absolute_quantity = quantity.abs();
                let notional = open_price.checked_mul(absolute_quantity).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Projected position notional overflows for {}/{}",
                        report.account_id,
                        report.instrument_id,
                    )
                })?;
                weighted_open_price =
                    weighted_open_price.checked_add(notional).ok_or_else(|| {
                        anyhow::anyhow!(
                            "Projected position notional aggregate overflows for {}/{}",
                            report.account_id,
                            report.instrument_id,
                        )
                    })?;
                priced_quantity =
                    priced_quantity
                        .checked_add(absolute_quantity)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Projected priced quantity overflows for {}/{}",
                                report.account_id,
                                report.instrument_id,
                            )
                        })?;
            }
        }

        let average_open_price = if priced_quantity.is_zero() {
            None
        } else {
            Some(
                weighted_open_price
                    .checked_div(priced_quantity)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Projected average price is not representable for {}/{}",
                            report.account_id,
                            report.instrument_id,
                        )
                    })?,
            )
        };

        Ok(ProjectedPositionState {
            signed_quantity,
            average_open_price,
        })
    }
}

/// External order registered while committing a reconciliation transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconciledExternalOrder {
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub instrument_id: InstrumentId,
    pub strategy_id: StrategyId,
    pub ts_init: UnixNanos,
}

/// Observable result of a committed reconciliation transaction.
#[derive(Debug, Default)]
pub struct ExecutionReconciliationReceipt {
    pub events: Vec<OrderEventAny>,
    pub external_orders: Vec<ReconciledExternalOrder>,
}

/// Central execution engine responsible for orchestrating order routing and execution.
///
/// The execution engine manages the entire order lifecycle from submission to completion,
/// handling routing to appropriate execution clients, position management, and event
/// processing. It supports multiple execution venues through registered clients and
/// provides sophisticated order management capabilities.
pub struct ExecutionEngine {
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    clients: IndexMap<ClientId, ExecutionClientAdapter>,
    execution_sources: HashMap<ClientId, ExecutionSourceId>,
    execution_clients_by_source: HashMap<ExecutionSourceId, ClientId>,
    default_client_id: Option<ClientId>,
    routing_map: HashMap<Venue, ClientId>,
    oms_overrides: HashMap<StrategyId, OmsType>,
    external_order_claims: HashMap<InstrumentId, StrategyId>,
    external_clients: HashSet<ClientId>,
    pos_id_generator: PositionIdGenerator,
    config: ExecutionEngineConfig,
    command_count: Cell<u64>,
    event_count: u64,
    report_count: u64,
    filtered_unclaimed_external_order_count: u64,
    snapshot_anchorer: Option<SnapshotAnchorer>,
}

impl Debug for ExecutionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ExecutionEngine))
            .field("client_count", &self.clients.len())
            .finish()
    }
}

impl ExecutionEngine {
    /// Creates a new [`ExecutionEngine`] instance.
    pub fn new(
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
        config: Option<ExecutionEngineConfig>,
    ) -> Self {
        let trader_id = get_message_bus().borrow().trader_id;
        Self {
            clock: clock.clone(),
            cache,
            clients: IndexMap::new(),
            execution_sources: HashMap::new(),
            execution_clients_by_source: HashMap::new(),
            default_client_id: None,
            routing_map: HashMap::new(),
            oms_overrides: HashMap::new(),
            external_order_claims: HashMap::new(),
            external_clients: config
                .as_ref()
                .and_then(|c| c.external_clients.clone())
                .unwrap_or_default()
                .into_iter()
                .collect(),
            pos_id_generator: PositionIdGenerator::new(trader_id, clock),
            config: config.unwrap_or_default(),
            command_count: Cell::new(0),
            event_count: 0,
            report_count: 0,
            filtered_unclaimed_external_order_count: 0,
            snapshot_anchorer: None,
        }
    }

    /// Registers all message bus handlers for the execution engine.
    pub fn register_msgbus_handlers(engine: &Rc<RefCell<Self>>) {
        let weak = WeakCell::from(Rc::downgrade(engine));

        let weak1 = weak.clone();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            TypedIntoHandler::from(move |cmd: TradingCommand| {
                if let Some(rc) = weak1.upgrade() {
                    rc.borrow().execute(cmd);
                }
            }),
        );

        // Queued endpoint for deferred command execution (re-entrancy safe),
        // with direct dispatch when no sender is installed.
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TypedIntoHandler::from(move |cmd: TradingCommand| {
                let endpoint = MessagingSwitchboard::exec_engine_execute();
                if trading_cmd_is_dispatching() {
                    capture_trading_cmd(TradingCommandMessage::new(endpoint, cmd));
                } else if let Some(sender) = try_get_trading_cmd_sender() {
                    sender.execute(TradingCommandMessage::new(endpoint, cmd));
                } else {
                    msgbus::send_trading_command(endpoint, cmd);
                }
            }),
        );

        let weak2 = weak.clone();
        msgbus::register_order_event_endpoint(
            MessagingSwitchboard::exec_engine_process(),
            TypedIntoHandler::from(move |event: OrderEventAny| {
                if let Some(rc) = weak2.upgrade() {
                    rc.borrow_mut().process(&event);
                }
            }),
        );

        let weak3 = weak;
        msgbus::register_execution_report_endpoint(
            MessagingSwitchboard::exec_engine_reconcile_execution_report(),
            TypedIntoHandler::from(move |report: AuthenticatedExecutionReport| {
                if let Some(rc) = weak3.upgrade()
                    && let Err(error) = rc.borrow_mut().reconcile_execution_report(&report)
                {
                    log::error!("Rejected execution report: {error}");
                }
            }),
        );
    }

    /// Returns the total count of trading commands received by the engine.
    #[must_use]
    pub fn command_count(&self) -> u64 {
        self.command_count.get()
    }

    /// Returns the total count of order events received by the engine.
    #[must_use]
    pub const fn event_count(&self) -> u64 {
        self.event_count
    }

    /// Returns the total count of execution reports received by the engine.
    #[must_use]
    pub const fn report_count(&self) -> u64 {
        self.report_count
    }

    /// Returns the count of unclaimed external venue orders filtered by execution reconciliation.
    #[must_use]
    pub const fn filtered_unclaimed_external_order_count(&self) -> u64 {
        self.filtered_unclaimed_external_order_count
    }

    /// Subscribes to instrument updates for a venue via the message bus.
    ///
    /// When instruments are published by the `DataEngine`, the handler routes
    /// them to the execution client registered for that venue.
    pub fn subscribe_venue_instruments(engine: &Rc<RefCell<Self>>, venue: Venue) {
        let weak = WeakCell::from(Rc::downgrade(engine));
        let pattern = switchboard::get_instruments_pattern(venue);

        let handler = TypedHandler::from(move |instrument: &InstrumentAny| {
            if let Some(rc) = weak.upgrade() {
                let venue = instrument.id().venue;
                let client_id = rc.borrow().routing_map.get(&venue).copied();
                if let Some(client_id) = client_id {
                    let mut engine = rc.borrow_mut();
                    if let Some(adapter) = engine.get_client_adapter_mut(&client_id) {
                        adapter.on_instrument(instrument.clone());
                    }
                }
            }
        });

        msgbus::subscribe_instruments(pattern, handler, None);
        log::info!("Subscribed to instrument updates for venue {venue}");
    }

    #[must_use]
    /// Returns the position ID count for the specified strategy.
    pub fn position_id_count(&self, strategy_id: StrategyId) -> usize {
        self.pos_id_generator.count(strategy_id)
    }

    #[must_use]
    /// Returns a reference to the cache.
    pub fn cache(&self) -> &Rc<RefCell<Cache>> {
        &self.cache
    }

    #[must_use]
    /// Returns a reference to the configuration.
    pub const fn config(&self) -> &ExecutionEngineConfig {
        &self.config
    }

    /// Sets the cache snapshot anchorer.
    ///
    /// The system event-store integration installs this while a run is open. Passing
    /// `None` disables anchor recording for later cache snapshots.
    pub fn set_snapshot_anchorer(&mut self, anchorer: Option<SnapshotAnchorer>) {
        self.snapshot_anchorer = anchorer;
    }

    #[must_use]
    /// Checks the integrity of cached execution data.
    pub fn check_integrity(&self) -> bool {
        self.cache.borrow_mut().check_integrity()
    }

    #[must_use]
    /// Returns true if all registered execution clients are connected.
    pub fn check_connected(&self) -> bool {
        self.clients.values().all(|c| c.is_connected())
    }

    #[must_use]
    /// Returns true if all registered execution clients are disconnected.
    pub fn check_disconnected(&self) -> bool {
        self.clients.values().all(|c| !c.is_connected())
    }

    /// Returns connection status for each registered client.
    #[must_use]
    pub fn client_connection_status(&self) -> Vec<(ClientId, bool)> {
        self.clients
            .values()
            .map(|c| (c.client_id(), c.is_connected()))
            .collect()
    }

    #[must_use]
    /// Checks for residual positions and orders in the cache.
    pub fn check_residuals(&self) -> bool {
        self.cache.borrow().check_residuals()
    }

    #[must_use]
    /// Returns the set of instruments that have external order claims.
    pub fn get_external_order_claims_instruments(&self) -> HashSet<InstrumentId> {
        self.external_order_claims.keys().copied().collect()
    }

    #[must_use]
    /// Returns the configured external client IDs.
    pub fn get_external_client_ids(&self) -> HashSet<ClientId> {
        self.external_clients.clone()
    }

    #[must_use]
    /// Returns any external order claim for the given instrument ID.
    pub fn get_external_order_claim(&self, instrument_id: &InstrumentId) -> Option<StrategyId> {
        self.external_order_claims.get(instrument_id).copied()
    }

    /// Returns the instruments with external order claims owned by `strategy_id`.
    #[must_use]
    pub fn get_external_order_claims_for_strategy(
        &self,
        strategy_id: StrategyId,
    ) -> HashSet<InstrumentId> {
        self.external_order_claims
            .iter()
            .filter_map(|(instrument_id, owner)| (*owner == strategy_id).then_some(*instrument_id))
            .collect()
    }

    /// Registers a new execution client.
    ///
    /// # Errors
    ///
    /// Returns an error if a client with the same ID is already registered.
    pub fn register_client(&mut self, mut client: Box<dyn ExecutionClient>) -> anyhow::Result<()> {
        let client_id = client.client_id();
        let venue = client.venue();

        if self.clients.contains_key(&client_id) {
            anyhow::bail!("Client already registered with ID {client_id}");
        }

        if let Some(existing_client_id) = self.routing_map.get(&venue) {
            anyhow::bail!(
                "Venue {venue} already routed to {existing_client_id}, \
                 cannot register {client_id} for the same venue"
            );
        }

        self.bind_execution_source(client_id, client.as_mut());
        let adapter = ExecutionClientAdapter::new(client);
        self.routing_map.insert(venue, client_id);
        log::debug!("Registered client {client_id}");
        self.clients.insert(client_id, adapter);
        Ok(())
    }

    /// Registers a default execution client for fallback routing.
    pub fn register_default_client(&mut self, mut client: Box<dyn ExecutionClient>) {
        let client_id = client.client_id();
        self.bind_execution_source(client_id, client.as_mut());
        let adapter = ExecutionClientAdapter::new(client);

        self.clients.insert(client_id, adapter);
        self.default_client_id = Some(client_id);
        log::debug!("Registered default client {client_id}");
    }

    fn bind_execution_source(&mut self, client_id: ClientId, client: &mut dyn ExecutionClient) {
        if let Some(previous_source_id) = self.execution_sources.remove(&client_id) {
            self.execution_clients_by_source.remove(&previous_source_id);
        }

        let source_id = loop {
            let candidate = ExecutionSourceId::new();
            if !self.execution_clients_by_source.contains_key(&candidate) {
                break candidate;
            }
        };
        client.bind_execution_source(source_id);
        self.execution_sources.insert(client_id, source_id);
        self.execution_clients_by_source
            .insert(source_id, client_id);
    }

    /// Marks an already-registered client as the default for fallback routing.
    ///
    /// # Errors
    ///
    /// Returns an error if no client is registered with the given ID, or a default
    /// client has already been set.
    pub fn set_default_client(&mut self, client_id: ClientId) -> anyhow::Result<()> {
        if self.default_client_id.is_some() {
            anyhow::bail!("default client already registered");
        }

        if !self.clients.contains_key(&client_id) {
            anyhow::bail!("No client registered with ID {client_id}");
        }
        self.default_client_id = Some(client_id);
        log::debug!("Set client {client_id} as default");
        Ok(())
    }

    #[must_use]
    /// Returns a reference to the execution client registered with the given ID.
    pub fn get_client(&self, client_id: &ClientId) -> Option<&dyn ExecutionClient> {
        self.clients.get(client_id).map(|a| a.client.as_ref())
    }

    fn execution_source_id(&self, client_id: ClientId) -> anyhow::Result<ExecutionSourceId> {
        self.execution_sources
            .get(&client_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Unknown execution report source {client_id}"))
    }

    fn validate_execution_source(
        &self,
        client_id: ClientId,
        source_id: ExecutionSourceId,
    ) -> anyhow::Result<()> {
        let authenticated_client = self
            .execution_clients_by_source
            .get(&source_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Unknown execution source capability"))?;
        anyhow::ensure!(
            authenticated_client == client_id,
            "Execution source capability belongs to {authenticated_client}, not {client_id}"
        );
        Ok(())
    }

    /// Wraps a report with the source capability bound at client registration.
    ///
    /// # Errors
    ///
    /// Returns an error when `client_id` is not registered.
    pub fn authenticate_execution_report(
        &self,
        client_id: ClientId,
        report: ExecutionReport,
    ) -> anyhow::Result<AuthenticatedExecutionReport> {
        Ok(AuthenticatedExecutionReport::new(
            client_id,
            self.execution_source_id(client_id)?,
            report,
        ))
    }

    /// Wraps a mass status with the source capability bound at client registration.
    ///
    /// # Errors
    ///
    /// Returns an error when `client_id` is not registered.
    pub fn authenticate_execution_mass_status(
        &self,
        client_id: ClientId,
        status: ExecutionMassStatus,
    ) -> anyhow::Result<AuthenticatedExecutionMassStatus> {
        Ok(AuthenticatedExecutionMassStatus::new(
            client_id,
            self.execution_source_id(client_id)?,
            status,
        ))
    }

    #[must_use]
    /// Returns a mutable reference to the execution client adapter registered with the given ID.
    pub fn get_client_adapter_mut(
        &mut self,
        client_id: &ClientId,
    ) -> Option<&mut ExecutionClientAdapter> {
        self.clients.get_mut(client_id)
    }

    /// Generates mass status for the given client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not found or mass status generation fails.
    pub async fn generate_mass_status(
        &mut self,
        client_id: &ClientId,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<NormalizedExecutionMassStatus>> {
        let mass_status = if let Some(client) = self.get_client_adapter_mut(client_id) {
            client.generate_mass_status(lookback_mins).await?
        } else {
            anyhow::bail!("Client {client_id} not found");
        };

        mass_status
            .map(|mass_status| {
                let authenticated =
                    self.authenticate_execution_mass_status(*client_id, mass_status)?;
                self.normalize_authenticated_execution_mass_status(authenticated)
            })
            .transpose()
    }

    /// Registers an external order with the execution client for tracking.
    ///
    /// This is called after reconciliation creates an external order, allowing the
    /// execution client to track it for subsequent events (e.g., cancellations).
    pub fn register_external_order(&self, order: &OrderAny, ts_init: UnixNanos) {
        let Some(venue_order_id) = order.venue_order_id() else {
            log::error!(
                "Cannot register external order {} without a venue order ID",
                order.client_order_id(),
            );
            return;
        };
        let venue = order.instrument_id().venue;
        let client_id = self
            .routing_map
            .get(&venue)
            .copied()
            .or(self.default_client_id);

        if let Some(client_id) = client_id {
            if let Err(error) =
                self.register_external_order_with_client(client_id, order, venue_order_id, ts_init)
            {
                log::error!("Cannot register external order with execution client: {error}");
            }
        }
    }

    fn register_external_order_with_client(
        &self,
        client_id: ClientId,
        order: &OrderAny,
        venue_order_id: VenueOrderId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        self.clients
            .get(&client_id)
            .ok_or_else(|| anyhow::anyhow!("Execution client {client_id} is not registered"))?
            .register_external_order(order, venue_order_id, ts_init);
        Ok(())
    }

    #[must_use]
    /// Returns all registered execution client IDs.
    pub fn client_ids(&self) -> Vec<ClientId> {
        self.clients.keys().copied().collect()
    }

    #[must_use]
    /// Returns mutable access to all registered execution clients.
    pub fn get_clients_mut(&mut self) -> Vec<&mut ExecutionClientAdapter> {
        self.clients.values_mut().collect()
    }

    /// Returns all registered execution clients.
    #[must_use]
    pub fn get_all_clients(&self) -> Vec<&dyn ExecutionClient> {
        self.clients.values().map(|a| a.client.as_ref()).collect()
    }

    #[must_use]
    /// Returns execution clients that would handle the given orders.
    ///
    /// This method first attempts to resolve each order's originating client from the cache,
    /// then falls back to venue routing for any orders without a cached client.
    pub fn get_clients_for_orders(&self, orders: &[OrderAny]) -> Vec<&dyn ExecutionClient> {
        let mut client_ids: IndexSet<ClientId> = IndexSet::new();
        let mut venues: IndexSet<Venue> = IndexSet::new();

        // Collect client IDs from cache and venues for fallback
        for order in orders {
            venues.insert(order.instrument_id().venue);
            if let Some(client_id) = self.cache.borrow().client_id(&order.client_order_id()) {
                client_ids.insert(*client_id);
            }
        }

        let mut clients: Vec<&dyn ExecutionClient> = Vec::new();

        // Add clients for cached client IDs (orders go back to originating client)
        for client_id in &client_ids {
            if let Some(adapter) = self.clients.get(client_id)
                && !clients.iter().any(|c| c.client_id() == adapter.client_id)
            {
                clients.push(adapter.client.as_ref());
            }
        }

        // Add clients for venue routing (for orders not in cache)
        for venue in &venues {
            let resolved_id = self
                .routing_map
                .get(venue)
                .copied()
                .or(self.default_client_id);

            if let Some(adapter) = resolved_id.and_then(|id| self.clients.get(&id))
                && !clients.iter().any(|c| c.client_id() == adapter.client_id)
            {
                clients.push(adapter.client.as_ref());
            }
        }

        clients
    }

    /// Sets routing for a specific venue to a given client ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the client ID is not registered.
    pub fn register_venue_routing(
        &mut self,
        client_id: ClientId,
        venue: Venue,
    ) -> anyhow::Result<()> {
        if !self.clients.contains_key(&client_id) {
            anyhow::bail!("No client registered with ID {client_id}");
        }

        if let Some(existing_client_id) = self.routing_map.get(&venue)
            && *existing_client_id != client_id
        {
            anyhow::bail!(
                "Venue {venue} already routed to {existing_client_id}, \
                 cannot re-route to {client_id}"
            );
        }

        self.routing_map.insert(venue, client_id);
        log::info!("Set client {client_id} routing for {venue}");
        Ok(())
    }

    /// Registers the OMS (Order Management System) type for a strategy.
    ///
    /// If an OMS type is already registered for this strategy, it will be overridden.
    pub fn register_oms_type(&mut self, strategy_id: StrategyId, oms_type: OmsType) {
        self.oms_overrides.insert(strategy_id, oms_type);
        log::info!("Registered OMS::{oms_type:?} for {strategy_id}");
    }

    /// Registers external order claims for a strategy.
    ///
    /// Venue-sourced external orders, fills, and materialized reconciliation activity for matching
    /// instruments will be associated with the strategy.
    ///
    /// This operation is atomic: either all instruments are registered or none are.
    ///
    /// # Errors
    ///
    /// Returns an error if any instrument already has a registered claim.
    pub fn register_external_order_claims(
        &mut self,
        strategy_id: StrategyId,
        instrument_ids: &HashSet<InstrumentId>,
    ) -> anyhow::Result<()> {
        // Validate all instruments first
        for instrument_id in instrument_ids {
            if let Some(existing) = self.external_order_claims.get(instrument_id) {
                anyhow::bail!(
                    "External order claim for {instrument_id} already exists for {existing}"
                );
            }
        }

        // If validation passed, insert all claims
        for instrument_id in instrument_ids {
            self.external_order_claims
                .insert(*instrument_id, strategy_id);
        }

        if !instrument_ids.is_empty() {
            log::info!("Registered external order claims for {strategy_id}: {instrument_ids:?}");
        }

        Ok(())
    }

    /// Commits external order claims for `strategy_id` without validation.
    ///
    /// The caller must have preflighted every instrument against
    /// [`Self::get_external_order_claim`]: an existing claim is overwritten
    /// without error. Coordinated live-node callers should use
    /// `LiveNode::register_external_order_claims`, which preflights both the
    /// execution engine and the reconciliation manager before committing;
    /// ordinary callers should use
    /// [`Self::register_external_order_claims`] instead.
    pub fn commit_external_order_claims(
        &mut self,
        strategy_id: StrategyId,
        instrument_ids: &HashSet<InstrumentId>,
    ) {
        self.external_order_claims.extend(
            instrument_ids
                .iter()
                .map(|instrument_id| (*instrument_id, strategy_id)),
        );

        if !instrument_ids.is_empty() {
            log::info!("Registered external order claims for {strategy_id}: {instrument_ids:?}");
        }
    }

    /// Deregisters all external order claims owned by `strategy_id`.
    ///
    /// Coordinated live-node callers should use
    /// `LiveNode::deregister_external_order_claims` so the execution engine and
    /// reconciliation manager remain consistent.
    pub fn deregister_external_order_claims(&mut self, strategy_id: StrategyId) {
        self.external_order_claims
            .retain(|_, owner| *owner != strategy_id);
    }

    /// # Errors
    ///
    /// Returns an error if no client is registered with the given ID.
    pub fn deregister_client(&mut self, client_id: ClientId) -> anyhow::Result<()> {
        if self.clients.shift_remove(&client_id).is_some() {
            if let Some(source_id) = self.execution_sources.remove(&client_id) {
                self.execution_clients_by_source.remove(&source_id);
            }
            if self.default_client_id == Some(client_id) {
                self.default_client_id = None;
            }

            // Remove from routing map if present
            self.routing_map
                .retain(|_, mapped_id| mapped_id != &client_id);
            log::info!("Deregistered client {client_id}");
            Ok(())
        } else {
            anyhow::bail!("No client registered with ID {client_id}")
        }
    }

    /// Connects all registered execution clients concurrently.
    ///
    /// Connection failures are logged but do not prevent the node from running.
    pub async fn connect(&mut self) {
        let futures: Vec<_> = self
            .get_clients_mut()
            .into_iter()
            .map(ExecutionClientAdapter::connect)
            .collect();

        let results = join_all(futures).await;

        for error in results.into_iter().filter_map(Result::err) {
            log::error!("Failed to connect execution client: {error:#}");
        }
    }

    /// Disconnects all registered execution clients concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error if any client fails to disconnect.
    pub async fn disconnect(&mut self) -> anyhow::Result<()> {
        let futures: Vec<_> = self
            .get_clients_mut()
            .into_iter()
            .map(ExecutionClientAdapter::disconnect)
            .collect();

        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();

        if errors.is_empty() {
            Ok(())
        } else {
            let error_msgs: Vec<_> = errors.iter().map(ToString::to_string).collect();
            anyhow::bail!(
                "Failed to disconnect execution clients: {}",
                error_msgs.join("; ")
            )
        }
    }

    /// Sets the `manage_own_order_books` configuration option.
    pub fn set_manage_own_order_books(&mut self, value: bool) {
        self.config.manage_own_order_books = value;
    }

    /// Starts the position snapshot timer if configured.
    #[expect(
        clippy::missing_panics_doc,
        reason = "timer registration is not expected to fail"
    )]
    pub fn start_snapshot_timer(&mut self) {
        if let Some(interval_secs) = self
            .config
            .snapshot_positions_interval_secs
            .filter(|&secs| secs > 0.0)
            && !self
                .clock
                .borrow()
                .timer_names()
                .contains(&TIMER_SNAPSHOT_POSITIONS)
        {
            let interval_ns = match secs_to_nanos(interval_secs) {
                Ok(ns) => ns,
                Err(e) => {
                    log::error!("Cannot start position snapshots timer: {e}");
                    return;
                }
            };
            let clock = self.clock.clone();
            let cache = self.cache.clone();
            let debug = self.config.debug;

            let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event| {
                Self::snapshot_open_positions(&clock, &cache, debug);
            });
            let callback = TimeEventCallback::from(callback_fn);

            log::info!("Starting position snapshots timer at {interval_secs} second intervals");
            self.clock
                .borrow_mut()
                .set_timer_ns(
                    TIMER_SNAPSHOT_POSITIONS,
                    interval_ns,
                    None,
                    None,
                    Some(callback),
                    None,
                    None,
                )
                .expect("Failed to set position snapshots timer");
        }
    }

    /// Stops the position snapshot timer if running.
    pub fn stop_snapshot_timer(&mut self) {
        let timer_registered = self
            .clock
            .borrow()
            .timer_names()
            .contains(&TIMER_SNAPSHOT_POSITIONS);

        if timer_registered {
            log::info!("Canceling position snapshots timer");
            self.clock
                .borrow_mut()
                .cancel_timer(TIMER_SNAPSHOT_POSITIONS);
        }
    }

    /// Starts the purge timers if configured.
    #[expect(
        clippy::missing_panics_doc,
        reason = "timer registration is not expected to fail"
    )]
    pub fn start_purge_timers(&mut self) {
        if let Some(interval_mins) = self
            .config
            .purge_closed_orders_interval_mins
            .filter(|&m| m > 0)
            && !self
                .clock
                .borrow()
                .timer_names()
                .contains(&TIMER_PURGE_CLOSED_ORDERS)
        {
            let interval_ns = mins_to_nanos(u64::from(interval_mins));
            let buffer_mins = self.config.purge_closed_orders_buffer_mins.unwrap_or(0);
            let buffer_secs = mins_to_secs(u64::from(buffer_mins));
            let cache = self.cache.clone();
            let clock = self.clock.clone();

            let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event| {
                let ts_now = clock.borrow().timestamp_ns();
                cache.borrow_mut().purge_closed_orders(ts_now, buffer_secs);
            });
            let callback = TimeEventCallback::from(callback_fn);

            log::info!("Starting purge closed orders timer at {interval_mins} minute intervals");
            self.clock
                .borrow_mut()
                .set_timer_ns(
                    TIMER_PURGE_CLOSED_ORDERS,
                    interval_ns,
                    None,
                    None,
                    Some(callback),
                    None,
                    None,
                )
                .expect("Failed to set purge closed orders timer");
        }

        if let Some(interval_mins) = self
            .config
            .purge_closed_positions_interval_mins
            .filter(|&m| m > 0)
            && !self
                .clock
                .borrow()
                .timer_names()
                .contains(&TIMER_PURGE_CLOSED_POSITIONS)
        {
            let interval_ns = mins_to_nanos(u64::from(interval_mins));
            let buffer_mins = self.config.purge_closed_positions_buffer_mins.unwrap_or(0);
            let buffer_secs = mins_to_secs(u64::from(buffer_mins));
            let cache = self.cache.clone();
            let clock = self.clock.clone();

            let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event| {
                let ts_now = clock.borrow().timestamp_ns();
                cache
                    .borrow_mut()
                    .purge_closed_positions(ts_now, buffer_secs);
            });
            let callback = TimeEventCallback::from(callback_fn);

            log::info!("Starting purge closed positions timer at {interval_mins} minute intervals");
            self.clock
                .borrow_mut()
                .set_timer_ns(
                    TIMER_PURGE_CLOSED_POSITIONS,
                    interval_ns,
                    None,
                    None,
                    Some(callback),
                    None,
                    None,
                )
                .expect("Failed to set purge closed positions timer");
        }

        if let Some(interval_mins) = self
            .config
            .purge_account_events_interval_mins
            .filter(|&m| m > 0)
            && !self
                .clock
                .borrow()
                .timer_names()
                .contains(&TIMER_PURGE_ACCOUNT_EVENTS)
        {
            let interval_ns = mins_to_nanos(u64::from(interval_mins));
            let lookback_mins = self.config.purge_account_events_lookback_mins.unwrap_or(0);
            let lookback_secs = mins_to_secs(u64::from(lookback_mins));
            let cache = self.cache.clone();
            let clock = self.clock.clone();

            let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event| {
                let ts_now = clock.borrow().timestamp_ns();
                cache
                    .borrow_mut()
                    .purge_account_events(ts_now, lookback_secs);
            });
            let callback = TimeEventCallback::from(callback_fn);

            log::info!("Starting purge account events timer at {interval_mins} minute intervals");
            self.clock
                .borrow_mut()
                .set_timer_ns(
                    TIMER_PURGE_ACCOUNT_EVENTS,
                    interval_ns,
                    None,
                    None,
                    Some(callback),
                    None,
                    None,
                )
                .expect("Failed to set purge account events timer");
        }
    }

    /// Stops the purge timers if running.
    pub fn stop_purge_timers(&mut self) {
        let timer_names: Vec<String> = self
            .clock
            .borrow()
            .timer_names()
            .into_iter()
            .map(String::from)
            .collect();

        if timer_names.iter().any(|n| n == TIMER_PURGE_CLOSED_ORDERS) {
            log::info!("Canceling purge closed orders timer");
            self.clock
                .borrow_mut()
                .cancel_timer(TIMER_PURGE_CLOSED_ORDERS);
        }

        if timer_names
            .iter()
            .any(|n| n == TIMER_PURGE_CLOSED_POSITIONS)
        {
            log::info!("Canceling purge closed positions timer");
            self.clock
                .borrow_mut()
                .cancel_timer(TIMER_PURGE_CLOSED_POSITIONS);
        }

        if timer_names.iter().any(|n| n == TIMER_PURGE_ACCOUNT_EVENTS) {
            log::info!("Canceling purge account events timer");
            self.clock
                .borrow_mut()
                .cancel_timer(TIMER_PURGE_ACCOUNT_EVENTS);
        }
    }

    /// Creates snapshots of all open positions.
    pub fn snapshot_open_position_states(&self) {
        Self::snapshot_open_positions(&self.clock, &self.cache, self.config.debug);
    }

    fn snapshot_open_positions(
        clock: &Rc<RefCell<dyn Clock>>,
        cache: &Rc<RefCell<Cache>>,
        debug: bool,
    ) {
        let positions: Vec<Position> = cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .into_iter()
            .map(|p| p.cloned())
            .collect();

        for position in positions {
            Self::publish_position_state_snapshot(clock, cache, debug, &position, true);
        }
    }

    #[expect(clippy::await_holding_refcell_ref)]
    /// Loads persistent state into cache and rebuilds indices.
    ///
    /// # Errors
    ///
    /// Returns an error if any cache operation fails.
    pub async fn load_cache(&mut self) -> anyhow::Result<()> {
        let ts = SystemTime::now(); // dst-ok: init-time log timing, not on DST state path

        {
            let mut cache = self.cache.borrow_mut();
            cache.clear_index();
            cache.cache_general()?;
        }

        self.cache.borrow_mut().cache_all().await?;

        // Snapshot before iterating: `get_or_init_own_order_book` re-enters `self.cache.borrow_mut()`.
        let own_book_entries: Vec<(InstrumentId, OwnBookOrder)> = {
            let mut cache = self.cache.borrow_mut();
            cache.build_index();
            let _ = cache.check_integrity();

            if self.config.manage_own_order_books {
                cache
                    .orders(None, None, None, None, None)
                    .into_iter()
                    .filter(|o| !o.is_closed() && should_handle_own_book_order(o))
                    .map(|o| (o.instrument_id(), o.to_own_book_order()))
                    .collect()
            } else {
                Vec::new()
            }
        };

        for (instrument_id, own_order) in own_book_entries {
            let mut own_book = self.get_or_init_own_order_book(&instrument_id);
            own_book.add(own_order);
        }

        self.set_position_id_counts();

        log::info!(
            "Loaded cache in {}ms",
            SystemTime::now() // dst-ok: init-time log timing, not on DST state path
                .duration_since(ts)
                .map_err(|e| anyhow::anyhow!("Failed to calculate duration: {e}"))?
                .as_millis()
        );

        Ok(())
    }

    /// Flushes the database to persist all cached data.
    pub fn flush_db(&self) {
        self.cache.borrow_mut().flush_db();
    }

    /// Reconciles an execution report from an authenticated client channel.
    ///
    /// # Errors
    ///
    /// Returns an error when the source client is unknown or the report claims
    /// an account or instrument venue outside that client's authority.
    pub fn reconcile_execution_report(
        &mut self,
        authenticated: &AuthenticatedExecutionReport,
    ) -> anyhow::Result<()> {
        let normalized = self.normalize_execution_report(authenticated)?;
        self.reconcile_execution_mass_status(normalized)
    }

    fn normalize_execution_report(
        &self,
        authenticated: &AuthenticatedExecutionReport,
    ) -> anyhow::Result<NormalizedExecutionMassStatus> {
        self.validate_execution_source(authenticated.source_client_id, authenticated.source_id)?;
        let mut mass_status = match &authenticated.report {
            ExecutionReport::MassStatus(status) => status.as_ref().clone(),
            ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _) => {
                ExecutionMassStatus::new(
                    authenticated.source_client_id,
                    report.account_id,
                    report.instrument_id.venue,
                    report.ts_init,
                    Some(report.report_id),
                )
            }
            ExecutionReport::Fill(report) => ExecutionMassStatus::new(
                authenticated.source_client_id,
                report.account_id,
                report.instrument_id.venue,
                report.ts_init,
                Some(report.report_id),
            ),
            ExecutionReport::Position(report) => ExecutionMassStatus::new(
                authenticated.source_client_id,
                report.account_id,
                report.instrument_id.venue,
                report.ts_init,
                Some(report.report_id),
            ),
        };

        match &authenticated.report {
            ExecutionReport::Order(report) => {
                mass_status.add_order_reports(vec![*report.clone()])?;
            }
            ExecutionReport::Fill(report) => mass_status.add_fill_reports(vec![*report.clone()]),
            ExecutionReport::OrderWithFills(report, fills) => {
                mass_status.add_order_reports(vec![*report.clone()])?;
                mass_status.add_fill_reports(fills.clone());
            }
            ExecutionReport::Position(report) => {
                mass_status.add_position_reports(vec![*report.clone()]);
            }
            ExecutionReport::MassStatus(_) => {}
        }

        self.normalize_authenticated_execution_mass_status(AuthenticatedExecutionMassStatus::new(
            authenticated.source_client_id,
            authenticated.source_id,
            mass_status,
        ))
    }

    /// Authenticates and normalizes a mass status before reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when the source client is unknown, any envelope or child
    /// report lies outside that client's authority, or evidence contradicts
    /// another report in the snapshot.
    pub fn normalize_authenticated_execution_mass_status(
        &self,
        authenticated: AuthenticatedExecutionMassStatus,
    ) -> anyhow::Result<NormalizedExecutionMassStatus> {
        self.validate_execution_source(authenticated.source_client_id, authenticated.source_id)?;
        self.validate_execution_mass_status_source(
            authenticated.source_client_id,
            &authenticated.report,
        )?;
        let normalized = NormalizedExecutionMassStatus::normalize(authenticated)?;
        self.validate_normalized_execution_mass_status(&normalized)?;
        Ok(normalized)
    }

    /// Authenticates a mass status against the execution client that produced it.
    ///
    /// This entrypoint is used by report-collection tasks which still hold the
    /// concrete client capability, so provenance does not depend on a client ID
    /// asserted inside the report envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope or any child report lies outside the
    /// source client's authority, evidence conflicts within the snapshot, or a
    /// report conflicts with cached execution identity.
    pub fn normalize_execution_mass_status_from_client(
        &self,
        source: &dyn ExecutionClient,
        status: ExecutionMassStatus,
    ) -> anyhow::Result<NormalizedExecutionMassStatus> {
        Self::validate_execution_mass_status_source_client(source, &status)?;
        let authenticated = self.authenticate_execution_mass_status(source.client_id(), status)?;
        let normalized = NormalizedExecutionMassStatus::normalize(authenticated)?;
        self.validate_normalized_execution_mass_status(&normalized)?;
        Ok(normalized)
    }

    fn validate_normalized_execution_mass_status(
        &self,
        normalized: &NormalizedExecutionMassStatus,
    ) -> anyhow::Result<()> {
        let cache = self.cache.borrow();
        let source_client_id = normalized.source_client_id();
        let mut venue_ids_by_client = IndexMap::<ClientOrderId, IndexSet<VenueOrderId>>::new();

        for report in normalized.order_reports().values() {
            Self::validate_cached_order_evidence(
                &cache,
                source_client_id,
                report.client_order_id,
                report.venue_order_id,
                report.account_id,
                report.instrument_id,
                report.order_side,
                report.venue_position_id,
            )?;
            if let Some(client_order_id) = report.client_order_id {
                venue_ids_by_client
                    .entry(client_order_id)
                    .or_default()
                    .insert(report.venue_order_id);
            }
        }

        for report in normalized.fill_reports().values().flatten() {
            Self::validate_cached_order_evidence(
                &cache,
                source_client_id,
                report.client_order_id,
                report.venue_order_id,
                report.account_id,
                report.instrument_id,
                report.order_side,
                report.venue_position_id,
            )?;
            if let Some(client_order_id) = report.client_order_id {
                venue_ids_by_client
                    .entry(client_order_id)
                    .or_default()
                    .insert(report.venue_order_id);
            }
        }

        for report in normalized.position_reports().values().flatten() {
            anyhow::ensure!(
                cache.instrument(&report.instrument_id).is_some(),
                "Instrument {} is not loaded for reconciliation",
                report.instrument_id
            );
            if let Some(position_id) = report.venue_position_id
                && let Some(position) = cache.position(&position_id)
            {
                anyhow::ensure!(
                    position.account_id == report.account_id
                        && position.instrument_id == report.instrument_id,
                    "Position {position_id} conflicts with cached account or instrument"
                );
            }
        }

        for (client_order_id, venue_order_ids) in venue_ids_by_client {
            if venue_order_ids.len() < 2 {
                continue;
            }
            let order = cache.order(&client_order_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Multiple venue orders claim uncached client order {client_order_id}"
                )
            })?;
            let known_venue_order_ids = order.venue_order_ids();
            anyhow::ensure!(
                venue_order_ids
                    .iter()
                    .all(|venue_order_id| known_venue_order_ids.contains(&venue_order_id)),
                "Client order {client_order_id} is claimed by an unknown replacement lifecycle"
            );
        }

        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    fn validate_cached_order_evidence(
        cache: &Cache,
        source_client_id: ClientId,
        reported_client_order_id: Option<ClientOrderId>,
        venue_order_id: VenueOrderId,
        account_id: AccountId,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        venue_position_id: Option<PositionId>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            cache.instrument(&instrument_id).is_some(),
            "Instrument {instrument_id} is not loaded for reconciliation"
        );

        let by_client =
            reported_client_order_id.and_then(|client_order_id| cache.order(&client_order_id));
        let venue_client_order_id = cache.client_order_id(&venue_order_id).copied();
        let by_venue =
            venue_client_order_id.and_then(|client_order_id| cache.order(&client_order_id));

        if let (Some(by_client), Some(by_venue)) = (&by_client, &by_venue) {
            anyhow::ensure!(
                by_client.client_order_id() == by_venue.client_order_id(),
                "Client and venue order IDs resolve to different cached orders"
            );
        }

        let Some(order) = by_client.or(by_venue) else {
            anyhow::ensure!(
                venue_client_order_id.is_none(),
                "Venue order {venue_order_id} indexes a missing cached order"
            );
            return Ok(());
        };

        let client_order_id = order.client_order_id();
        anyhow::ensure!(
            reported_client_order_id.is_none_or(|reported| reported == client_order_id),
            "Reported client order ID conflicts with cached venue order {venue_order_id}"
        );
        anyhow::ensure!(
            cache.client_id(&client_order_id) == Some(&source_client_id),
            "Order {client_order_id} has no authenticated ownership by {source_client_id}"
        );
        anyhow::ensure!(
            order.account_id().is_none_or(|cached| cached == account_id),
            "Order {client_order_id} belongs to a different account"
        );
        anyhow::ensure!(
            order.instrument_id() == instrument_id && order.order_side() == order_side,
            "Order {client_order_id} conflicts with reported instrument or side"
        );
        if let Some(current_venue_order_id) = order.venue_order_id()
            && current_venue_order_id != venue_order_id
        {
            anyhow::ensure!(
                order
                    .venue_order_ids()
                    .iter()
                    .any(|historical| **historical == venue_order_id),
                "Order {client_order_id} is claimed by unknown venue order {venue_order_id}"
            );
        }
        if let Some(reported_position_id) = venue_position_id
            && let Some(cached_position_id) = cache.position_id(&client_order_id)
        {
            anyhow::ensure!(
                *cached_position_id == reported_position_id,
                "Order {client_order_id} conflicts with reported venue position ID"
            );
        }

        Ok(())
    }

    fn validate_execution_mass_status_source(
        &self,
        source_client_id: ClientId,
        status: &ExecutionMassStatus,
    ) -> anyhow::Result<()> {
        let client = self
            .clients
            .get(&source_client_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown execution report source {source_client_id}"))?;
        Self::validate_execution_mass_status_source_client(client.client.as_ref(), status)
    }

    fn validate_execution_mass_status_source_client(
        client: &dyn ExecutionClient,
        status: &ExecutionMassStatus,
    ) -> anyhow::Result<()> {
        let source_client_id = client.client_id();
        let child_matches = |account_id: AccountId, instrument_id: InstrumentId| {
            account_id == client.account_id() && client.handles_order_venue(instrument_id.venue)
        };

        let matches_source = status.client_id == source_client_id
            && status.account_id == client.account_id()
            && client.handles_order_venue(status.venue)
            && status
                .order_reports()
                .values()
                .all(|report| child_matches(report.account_id, report.instrument_id))
            && status
                .fill_reports()
                .values()
                .flatten()
                .all(|report| child_matches(report.account_id, report.instrument_id))
            && status
                .position_reports()
                .values()
                .flatten()
                .all(|report| child_matches(report.account_id, report.instrument_id));

        anyhow::ensure!(
            matches_source,
            "Execution mass status claims authority outside source client {source_client_id}"
        );
        Ok(())
    }

    fn prepare_external_order_from_status_with_strategy(
        &self,
        report: &OrderStatusReport,
        strategy_id: StrategyId,
        source: ReconciliationOrderSource,
    ) -> anyhow::Result<PreparedExternalOrder> {
        let client_order_id = report
            .client_order_id
            .unwrap_or_else(|| ClientOrderId::from(report.venue_order_id.as_str()));

        let trader_id = get_message_bus().borrow().trader_id;
        let ts_now = self.clock.borrow().timestamp_ns();

        let initialized = OrderInitialized::new_checked(
            trader_id,
            strategy_id,
            report.instrument_id,
            client_order_id,
            report.order_side,
            report.order_type,
            report.quantity,
            report.time_in_force,
            report.post_only,
            report.reduce_only,
            false, // quote_quantity
            true,  // reconciliation
            UUID4::new(),
            ts_now,
            ts_now,
            report.price,
            report.activation_price,
            report.trigger_price,
            report.trigger_type,
            report.limit_offset,
            report.trailing_offset,
            Some(report.trailing_offset_type),
            report.expire_time,
            report.display_qty,
            None, // emulation_trigger
            None, // trigger_instrument_id
            Some(report.contingency_type),
            report.order_list_id,
            report.linked_order_ids.clone(),
            report.parent_order_id,
            None, // exec_algorithm_id
            None, // exec_algorithm_params
            None, // exec_spawn_id
            strategy_id.is_external().then(|| vec![source.tag()]),
        )?;

        self.prepare_external_order(
            initialized,
            report.venue_order_id,
            report.instrument_id,
            strategy_id,
            ts_now,
            Some(report.order_status),
        )
    }

    fn prepare_external_order_from_fills(
        &self,
        fills: &[FillReport],
        strategy_id: StrategyId,
    ) -> anyhow::Result<PreparedExternalOrder> {
        let first = fills
            .first()
            .ok_or_else(|| anyhow::anyhow!("Cannot prepare external order from empty fills"))?;
        let quantity =
            fills
                .iter()
                .try_fold(Quantity::zero(first.last_qty.precision), |total, fill| {
                    total.checked_add(fill.last_qty).ok_or_else(|| {
                        anyhow::anyhow!(
                            "External fill quantity overflows for venue order {}",
                            first.venue_order_id,
                        )
                    })
                })?;
        let client_order_id = first
            .client_order_id
            .unwrap_or_else(|| ClientOrderId::from(first.venue_order_id.as_str()));
        let trader_id = get_message_bus().borrow().trader_id;
        let ts_now = self.clock.borrow().timestamp_ns();
        let initialized = OrderInitialized::new(
            trader_id,
            strategy_id,
            first.instrument_id,
            client_order_id,
            first.order_side,
            OrderType::Market,
            quantity,
            TimeInForce::Ioc,
            false, // post_only
            false, // reduce_only: a fill report does not carry this order instruction
            false,
            true,
            UUID4::new(),
            ts_now,
            ts_now,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TrailingOffsetType::NoTrailingOffset),
            None,
            None,
            None,
            None,
            Some(ContingencyType::NoContingency),
            None,
            None,
            None,
            None,
            None,
            None,
            strategy_id
                .is_external()
                .then(|| vec![ReconciliationOrderSource::Venue.tag()]),
        );

        self.prepare_external_order(
            initialized,
            first.venue_order_id,
            first.instrument_id,
            strategy_id,
            ts_now,
            None,
        )
    }

    fn resolve_external_strategy(&self, instrument_id: &InstrumentId) -> StrategyId {
        self.external_order_claims
            .get(instrument_id)
            .copied()
            .unwrap_or_else(StrategyId::external)
    }

    fn should_filter_unclaimed_external_order(&self, strategy_id: StrategyId) -> bool {
        self.config.filter_unclaimed_external_orders && strategy_id.is_external()
    }

    fn record_filtered_external_order(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
    ) {
        self.filtered_unclaimed_external_order_count += 1;
        if self.filtered_unclaimed_external_order_count == 1 {
            log::info!(
                "Filtering unclaimed external orders; first filtered order {client_order_id} ({venue_order_id}) for {instrument_id}",
            );
        } else {
            log::debug!(
                "Filtered unclaimed external order {client_order_id} ({venue_order_id}) for {instrument_id}",
            );
        }
    }

    /// Constructs an external order without mutating cache or publishing events.
    #[allow(
        clippy::too_many_arguments,
        reason = "external order materialisation threads several ids and a timestamp"
    )]
    fn prepare_external_order(
        &self,
        initialized: OrderInitialized,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        strategy_id: StrategyId,
        ts_now: UnixNanos,
        order_status: Option<OrderStatus>,
    ) -> anyhow::Result<PreparedExternalOrder> {
        let initialized = OrderEventAny::Initialized(initialized);
        let order = OrderAny::from_events(vec![initialized.clone()])?;

        Ok(PreparedExternalOrder {
            initialized,
            order,
            venue_order_id,
            instrument_id,
            strategy_id,
            ts_init: ts_now,
            reported_status: order_status,
        })
    }

    /// Commits a previously prepared external order to cache and adapter routing.
    fn commit_external_order(
        &self,
        prepared: PreparedExternalOrder,
        source_client_id: ClientId,
    ) -> anyhow::Result<OrderAny> {
        let PreparedExternalOrder {
            initialized,
            order,
            venue_order_id,
            instrument_id,
            strategy_id: _,
            ts_init,
            reported_status,
        } = prepared;
        let client_order_id = order.client_order_id();

        {
            let mut cache = self.cache.borrow_mut();
            cache.add_venue_order_id(&client_order_id, &venue_order_id, false)?;
            cache.add_order(order.clone(), None, Some(source_client_id), false)?;
        }

        self.publish_order_event(&initialized);

        match reported_status {
            Some(status) => log::info!(
                "Created external order {client_order_id} ({venue_order_id}) for {instrument_id} [{status}]",
            ),
            None => log::info!(
                "Created external order {client_order_id} ({venue_order_id}) for {instrument_id}",
            ),
        }

        self.register_external_order_with_client(
            source_client_id,
            &order,
            venue_order_id,
            ts_init,
        )?;

        Ok(order)
    }

    /// Compares a venue position snapshot with the projected cache state.
    fn reconcile_position_report_without_raw_publish(&self, report: &PositionStatusReport) {
        let cache = self.cache.borrow();

        let size_precision = cache
            .instrument(&report.instrument_id)
            .map(InstrumentAny::size_precision);

        if report.venue_position_id.is_some() {
            self.reconcile_position_report_hedging(report, &cache);
        } else {
            self.reconcile_position_report_netting(report, &cache, size_precision);
        }
    }

    fn reconcile_position_report_hedging(&self, report: &PositionStatusReport, cache: &Cache) {
        let venue_position_id = report.venue_position_id.as_ref().unwrap();

        log::debug!(
            "Reconciling HEDGE position for {}, venue_position_id={}",
            report.instrument_id,
            venue_position_id
        );

        let Some(position) = cache.position(venue_position_id) else {
            log::error!("Cannot reconcile position: {venue_position_id} not found in cache");
            return;
        };

        let cached_signed_qty = match position.side {
            PositionSide::Long => position.quantity.as_decimal(),
            PositionSide::Short => -position.quantity.as_decimal(),
            _ => Decimal::ZERO,
        };
        let venue_signed_qty = report.signed_decimal_qty;

        if cached_signed_qty != venue_signed_qty {
            log::error!(
                "Position mismatch for {} {}: cached={}, venue={}",
                report.instrument_id,
                venue_position_id,
                cached_signed_qty,
                venue_signed_qty
            );
        }
    }

    fn reconcile_position_report_netting(
        &self,
        report: &PositionStatusReport,
        cache: &Cache,
        size_precision: Option<u8>,
    ) {
        log::debug!("Reconciling NET position for {}", report.instrument_id);

        let positions_open = Self::netting_positions_open_for_report(cache, report);

        let position_refs = positions_open
            .iter()
            .map(|position| &**position)
            .collect::<Vec<_>>();

        if let Some(message) =
            Self::netting_split_position_ownership_message(report, &position_refs)
        {
            log::warn!("{message}");
        }

        // Sum up cached position quantities using domain types to avoid f64 precision loss
        let cached_signed_qty: Decimal = positions_open
            .iter()
            .map(|position| Self::position_signed_decimal_qty(position))
            .sum();

        log::debug!(
            "Position report: venue_signed_qty={}, cached_signed_qty={}",
            report.signed_decimal_qty,
            cached_signed_qty
        );

        let _ = check_position_reconciliation(report, cached_signed_qty, size_precision);
    }

    fn netting_positions_open_for_report<'a>(
        cache: &'a Cache,
        report: &PositionStatusReport,
    ) -> Vec<PositionRef<'a>> {
        cache.positions_open(
            None,
            Some(&report.instrument_id),
            None,
            Some(&report.account_id),
            None,
        )
    }

    fn netting_split_position_ownership_message(
        report: &PositionStatusReport,
        positions_open: &[&Position],
    ) -> Option<String> {
        let mut strategy_ids = positions_open
            .iter()
            .map(|position| position.strategy_id.to_string())
            .collect::<Vec<_>>();
        strategy_ids.sort();
        strategy_ids.dedup();

        if strategy_ids.len() <= 1 {
            return None;
        }

        let position_details = Self::position_details(positions_open.iter().copied());

        Some(format!(
            "NETTING reconciliation found split ownership for account_id={}, instrument_id={}: \
             strategies=[{}], positions=[{}]",
            report.account_id,
            report.instrument_id,
            strategy_ids.join(", "),
            position_details
        ))
    }

    /// Reconciles an execution mass status report atomically at the order-state boundary.
    ///
    /// The complete snapshot is first projected against cloned orders. No cache mutation
    /// occurs unless every venue-order group is applicable.
    fn reconcile_execution_mass_status(
        &mut self,
        normalized: NormalizedExecutionMassStatus,
    ) -> anyhow::Result<()> {
        self.report_count += 1;
        let source_client_id = normalized.source_client_id();
        let mass_status = normalized.into_mass_status();

        log::info!(
            "Reconciling mass status for client={}, account={}, venue={}",
            mass_status.client_id,
            mass_status.account_id,
            mass_status.venue
        );

        self.publish_reconciliation_evidence(&mass_status);
        let prepared =
            self.prepare_execution_mass_status(&mass_status, source_client_id, &AHashSet::new())?;
        Self::ensure_position_projection_matches(&prepared, Decimal::ZERO)?;
        let order_count = prepared.order_reconciliations.len();
        let position_count = prepared.position_reports.len();
        let _ = self.commit_projected_execution_reconciliation(prepared)?;

        log::info!(
            "Mass status reconciliation complete: {order_count} order groups, {position_count} positions",
        );
        Ok(())
    }

    /// Publishes the authenticated venue evidence before any derived reconciliation state.
    pub fn publish_reconciliation_evidence(&self, mass_status: &ExecutionMassStatus) {
        let order_topic = MessagingSwitchboard::reconciliation_raw_order_status_report_topic();
        for report in mass_status.order_reports().values() {
            msgbus::publish_any(order_topic, report);
        }

        let fill_topic = MessagingSwitchboard::reconciliation_raw_fill_report_topic();
        for report in mass_status.fill_reports().values().flatten() {
            msgbus::publish_any(fill_topic, report);
        }

        let position_topic =
            MessagingSwitchboard::reconciliation_raw_position_status_report_topic();
        for report in mass_status.position_reports().values().flatten() {
            msgbus::publish_any(position_topic, report);
        }
    }

    fn prepare_execution_mass_status(
        &self,
        mass_status: &ExecutionMassStatus,
        source_client_id: ClientId,
        correction_venue_order_ids: &AHashSet<VenueOrderId>,
    ) -> anyhow::Result<ProjectedExecutionReconciliation> {
        let order_reports = mass_status.order_reports();
        let fill_reports = mass_status.fill_reports();
        let fill_ledger = self.reconciliation_fill_ledger(mass_status)?;
        let mut order_reconciliations = Vec::new();
        let mut filtered_external_orders = Vec::new();
        let mut paired_venue_ids = AHashSet::new();

        for report in order_reports.values() {
            let fills = fill_reports
                .get(&report.venue_order_id)
                .map_or(&[][..], Vec::as_slice);
            match self.prepare_order_reconciliation(
                Some(report),
                fills,
                source_client_id,
                if correction_venue_order_ids.contains(&report.venue_order_id) {
                    ReconciliationOrderSource::PositionCorrection
                } else {
                    ReconciliationOrderSource::Venue
                },
                &fill_ledger,
            )? {
                PreparedOrderGroup::Reconcile(prepared) => {
                    order_reconciliations.push(*prepared);
                }
                PreparedOrderGroup::Filtered(client_order_id, venue_order_id, instrument_id) => {
                    filtered_external_orders.push((client_order_id, venue_order_id, instrument_id));
                }
            }
            paired_venue_ids.insert(report.venue_order_id);
        }

        for (venue_order_id, fills) in &fill_reports {
            if paired_venue_ids.contains(venue_order_id) || fills.is_empty() {
                continue;
            }
            match self.prepare_order_reconciliation(
                None,
                fills,
                source_client_id,
                ReconciliationOrderSource::Venue,
                &fill_ledger,
            )? {
                PreparedOrderGroup::Reconcile(prepared) => {
                    order_reconciliations.push(*prepared);
                }
                PreparedOrderGroup::Filtered(client_order_id, venue_order_id, instrument_id) => {
                    filtered_external_orders.push((client_order_id, venue_order_id, instrument_id));
                }
            }
        }

        let position_reports = mass_status
            .position_reports()
            .into_values()
            .flatten()
            .collect::<Vec<_>>();
        let mut events = order_reconciliations
            .iter()
            .flat_map(|reconciliation| reconciliation.events.iter().cloned())
            .collect::<Vec<_>>();
        events.sort_by_key(|prepared| prepared.event.ts_event());
        let projected_orders =
            self.project_canonical_order_state(&order_reconciliations, &events)?;
        let position_projection = self.project_position_state(&projected_orders, &events)?;

        Ok(ProjectedExecutionReconciliation {
            order_reconciliations,
            events,
            position_reports,
            position_projection,
            filtered_external_orders,
        })
    }

    fn reconciliation_fill_ledger(
        &self,
        mass_status: &ExecutionMassStatus,
    ) -> anyhow::Result<ReconciliationFillLedger> {
        let mut ledger = ReconciliationFillLedger {
            raw_fill_keys: mass_status
                .fill_reports()
                .into_values()
                .flatten()
                .map(|fill| (fill.account_id, fill.instrument_id, fill.trade_id))
                .collect(),
            ..Default::default()
        };
        let cache = self.cache.borrow();

        for position in cache.positions(None, None, None, None, None) {
            let position_fills =
                position
                    .events
                    .iter()
                    .chain(
                        position
                            .replay_events
                            .iter()
                            .filter_map(|event| match event {
                                PositionReplayEvent::Filled(fill) => Some(fill),
                                PositionReplayEvent::Adjusted(_) => None,
                            }),
                    );
            for fill in position_fills {
                ledger.position_fill_keys.insert((
                    position.account_id,
                    position.instrument_id,
                    fill.trade_id,
                ));
                if cache.order(&fill.client_order_id).is_none() {
                    ledger.missing_order_ids.insert((
                        position.account_id,
                        position.instrument_id,
                        fill.client_order_id,
                    ));
                    ledger.missing_venue_order_ids.insert((
                        position.account_id,
                        position.instrument_id,
                        fill.venue_order_id,
                    ));
                }
            }

            if cache.oms_type(&position.id) == Some(OmsType::Netting) {
                ledger.netting_lifecycle_starts.insert(
                    (
                        position.account_id,
                        position.instrument_id,
                        position.strategy_id,
                    ),
                    position.ts_opened,
                );
            }
        }

        for order in cache.orders(None, None, None, None, None) {
            let Some(account_id) = order.account_id() else {
                continue;
            };
            for trade_id in order.trade_ids() {
                let key = (account_id, order.instrument_id(), *trade_id);
                if let Some(owner) = ledger
                    .cached_fill_owners
                    .insert(key, order.client_order_id())
                {
                    anyhow::ensure!(
                        owner == order.client_order_id(),
                        "Cached fill {trade_id} is owned by both {owner} and {}",
                        order.client_order_id(),
                    );
                }
            }
        }

        Ok(ledger)
    }

    fn project_position_state(
        &self,
        projected_orders: &HashMap<ClientOrderId, OrderAny>,
        events: &[PreparedReconciliationEvent],
    ) -> anyhow::Result<ExecutionPositionProjection> {
        let cache = self.cache.borrow();
        let positions = cache
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|position| (position.id, position.clone()))
            .collect::<IndexMap<_, _>>();
        drop(cache);
        let mut projection = ExecutionPositionProjection { positions };

        for event in events {
            if event.fill_disposition == ReconciliationFillDisposition::ProjectOrderOnly {
                continue;
            }
            let OrderEventAny::Filled(fill) = &event.event else {
                continue;
            };
            if projection.positions.values().any(|position| {
                position.account_id == fill.account_id
                    && position.instrument_id == fill.instrument_id
                    && position.trade_ids.contains(&fill.trade_id)
            }) {
                continue;
            }

            let cached_position_id = self
                .cache
                .borrow()
                .position_id(&fill.client_order_id)
                .copied();
            let position_id = if let Some(position_id) = cached_position_id {
                position_id
            } else if event.oms_type == OmsType::Hedging {
                fill.position_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cannot project hedging fill {} without a venue position ID",
                        fill.trade_id
                    )
                })?
            } else {
                self.determine_netting_position_id(fill)
            };
            let mut fill = fill.clone();
            fill.position_id = Some(position_id);
            let projected_order = projected_orders.get(&fill.client_order_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Projected fill {} has no prepared order {}",
                    fill.trade_id,
                    fill.client_order_id,
                )
            })?;
            self.cache.borrow().try_account(&fill.account_id)?;

            if let Some(position) = projection.positions.get_mut(&position_id) {
                anyhow::ensure!(
                    position.account_id == fill.account_id
                        && position.instrument_id == fill.instrument_id,
                    "Projected fill {} conflicts with position {position_id}",
                    fill.trade_id
                );
                anyhow::ensure!(
                    !projected_order.is_reduce_only()
                        || (position.is_open()
                            && position.is_opposite_side(fill.order_side)
                            && fill.last_qty <= position.quantity),
                    "Reduce-only fill {} cannot be applied to position {position_id}",
                    fill.trade_id,
                );
                position.apply(&fill);
            } else {
                anyhow::ensure!(
                    !projected_order.is_reduce_only(),
                    "Reduce-only fill {} cannot open position {position_id}",
                    fill.trade_id,
                );
                let instrument = self
                    .cache
                    .borrow()
                    .instrument(&fill.instrument_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Instrument {} is not loaded for position projection",
                            fill.instrument_id
                        )
                    })?;
                projection
                    .positions
                    .insert(position_id, Position::new(&instrument, fill));
            }
        }

        Ok(projection)
    }

    fn project_canonical_order_state(
        &self,
        order_reconciliations: &[PreparedOrderReconciliation],
        events: &[PreparedReconciliationEvent],
    ) -> anyhow::Result<HashMap<ClientOrderId, OrderAny>> {
        let mut projected_orders = HashMap::new();
        for reconciliation in order_reconciliations {
            projected_orders
                .entry(reconciliation.client_order_id)
                .or_insert_with(|| reconciliation.initial_order.clone());
        }

        for prepared in events {
            let client_order_id = prepared.event.client_order_id();
            let projected = projected_orders.get_mut(&client_order_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Canonical reconciliation event has no prepared order {client_order_id}"
                )
            })?;
            projected.apply(prepared.event.clone()).map_err(|error| {
                anyhow::anyhow!(
                    "Cannot project canonical reconciliation event for {client_order_id}: {error}; event={}",
                    prepared.event,
                )
            })?;
        }

        for reconciliation in order_reconciliations {
            let Some(report) = reconciliation.report.as_ref() else {
                continue;
            };
            let projected = projected_orders
                .get(&reconciliation.client_order_id)
                .expect("prepared order projection must exist");
            Self::ensure_order_projection_matches(projected, report)?;
        }

        Ok(projected_orders)
    }

    fn ensure_position_projection_matches(
        prepared: &ProjectedExecutionReconciliation,
        tolerance: Decimal,
    ) -> anyhow::Result<()> {
        for report in &prepared.position_reports {
            let projected = prepared.position_projection.state_for(report)?;
            anyhow::ensure!(
                (projected.signed_quantity - report.signed_decimal_qty).abs() <= tolerance,
                "Projected position quantity {} for {}/{} does not match venue quantity {}",
                projected.signed_quantity,
                report.account_id,
                report.instrument_id,
                report.signed_decimal_qty,
            );
        }
        Ok(())
    }

    fn prepare_order_reconciliation(
        &self,
        report: Option<&OrderStatusReport>,
        fills: &[FillReport],
        source_client_id: ClientId,
        source: ReconciliationOrderSource,
        fill_ledger: &ReconciliationFillLedger,
    ) -> anyhow::Result<PreparedOrderGroup> {
        let evidence = report
            .map(|report| {
                (
                    report.client_order_id,
                    report.venue_order_id,
                    report.instrument_id,
                )
            })
            .or_else(|| {
                fills.first().map(|fill| {
                    (
                        fill.client_order_id,
                        fill.venue_order_id,
                        fill.instrument_id,
                    )
                })
            })
            .ok_or_else(|| anyhow::anyhow!("Cannot prepare an empty order reconciliation"))?;
        let (reported_client_order_id, venue_order_id, instrument_id) = evidence;

        let instrument = self
            .cache
            .borrow()
            .instrument(&instrument_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Instrument {instrument_id} is not loaded"))?;
        let cached_order = {
            let cache = self.cache.borrow();
            reported_client_order_id
                .and_then(|client_order_id| cache.order_owned(&client_order_id))
                .or_else(|| {
                    cache
                        .client_order_id(&venue_order_id)
                        .and_then(|client_order_id| cache.order_owned(client_order_id))
                })
        };
        let venue_order_id_to_claim = cached_order.as_ref().and_then(|order| {
            let is_indexed = self
                .cache
                .borrow()
                .client_order_id(&venue_order_id)
                .is_some();
            (!is_indexed
                && order
                    .venue_order_id()
                    .is_none_or(|current| current == venue_order_id))
            .then_some(venue_order_id)
        });
        let mut external_order = None;
        let mut projected = if let Some(order) = cached_order {
            order
        } else {
            let strategy_id = self.resolve_external_strategy(&instrument_id);
            let client_order_id = reported_client_order_id
                .unwrap_or_else(|| ClientOrderId::from(venue_order_id.as_str()));
            if !source.is_position_correction()
                && self.should_filter_unclaimed_external_order(strategy_id)
            {
                return Ok(PreparedOrderGroup::Filtered(
                    client_order_id,
                    venue_order_id,
                    instrument_id,
                ));
            }

            let prepared = match report {
                Some(report) => self.prepare_external_order_from_status_with_strategy(
                    report,
                    strategy_id,
                    source,
                )?,
                None => self.prepare_external_order_from_fills(fills, strategy_id)?,
            };
            let order = prepared.order.clone();
            external_order = Some(prepared);
            order
        };

        let initial_order = projected.clone();
        let source_client = self.clients.get(&source_client_id).ok_or_else(|| {
            anyhow::anyhow!("Execution source client {source_client_id} is not registered")
        })?;
        let reconciliation_oms_type =
            self.resolve_oms_type_for_client(projected.strategy_id(), source_client.as_ref());
        let ts_now = self.clock.borrow().timestamp_ns();
        let mut events = Vec::new();
        match report {
            Some(report) if fills.is_empty() && external_order.is_some() => {
                let generated = generate_external_order_status_events(
                    &projected,
                    report,
                    &report.account_id,
                    &instrument,
                    ts_now,
                );
                self.project_order_events(
                    &mut projected,
                    generated,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
            }
            Some(report) if fills.is_empty() => {
                let generated = generate_reconciliation_order_events(
                    &projected,
                    report,
                    Some(&instrument),
                    ts_now,
                );
                self.project_order_events(
                    &mut projected,
                    generated,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
            }
            Some(report) => {
                let generated = if let Some(prepared) = external_order.as_ref() {
                    vec![OrderEventAny::Accepted(OrderAccepted::new(
                        projected.trader_id(),
                        projected.strategy_id(),
                        projected.instrument_id(),
                        projected.client_order_id(),
                        prepared.venue_order_id,
                        report.account_id,
                        UUID4::new(),
                        report.ts_accepted,
                        ts_now,
                        true,
                    ))]
                } else {
                    generate_reconciliation_order_pre_fill_events(&projected, report, ts_now)
                };
                self.project_order_events(
                    &mut projected,
                    generated,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
                self.project_fill_reports(
                    &mut projected,
                    fills,
                    &instrument,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
                let generated = generate_reconciliation_order_snapshot_events(
                    &projected,
                    report,
                    Some(&instrument),
                    ts_now,
                );
                self.project_order_events(
                    &mut projected,
                    generated,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
            }
            None => {
                if projected.status() == OrderStatus::Initialized {
                    let fill = fills.first().ok_or_else(|| {
                        anyhow::anyhow!("Fill-only reconciliation group is empty")
                    })?;
                    let accepted = OrderAccepted::new(
                        projected.trader_id(),
                        projected.strategy_id(),
                        projected.instrument_id(),
                        projected.client_order_id(),
                        fill.venue_order_id,
                        fill.account_id,
                        UUID4::new(),
                        fill.ts_event,
                        ts_now,
                        true,
                    );
                    self.project_order_events(
                        &mut projected,
                        vec![OrderEventAny::Accepted(accepted)],
                        &mut events,
                        fill_ledger,
                        reconciliation_oms_type,
                    )?;
                }
                self.project_fill_reports(
                    &mut projected,
                    fills,
                    &instrument,
                    &mut events,
                    fill_ledger,
                    reconciliation_oms_type,
                )?;
            }
        }

        if let Some(report) = report {
            Self::ensure_order_projection_matches(&projected, report)?;
        }

        Ok(PreparedOrderGroup::Reconcile(Box::new(
            PreparedOrderReconciliation {
                source_client_id,
                client_order_id: projected.client_order_id(),
                venue_order_id,
                venue_order_id_to_claim,
                external_order,
                initial_order,
                report: report.cloned(),
                events,
            },
        )))
    }

    fn project_fill_reports(
        &self,
        projected: &mut OrderAny,
        fills: &[FillReport],
        instrument: &InstrumentAny,
        events: &mut Vec<PreparedReconciliationEvent>,
        fill_ledger: &ReconciliationFillLedger,
        oms_type: OmsType,
    ) -> anyhow::Result<()> {
        for fill in fills {
            if projected
                .trade_ids()
                .iter()
                .any(|trade_id| **trade_id == fill.trade_id)
            {
                continue;
            }
            let ts_now = self.clock.borrow().timestamp_ns();
            let event = reconcile_fill(
                projected,
                fill,
                instrument,
                ts_now,
                self.config.allow_overfills,
            )
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Fill {} for {} is not applicable to the projected order",
                    fill.trade_id,
                    projected.client_order_id(),
                )
            })?;
            self.project_order_events(projected, vec![event], events, fill_ledger, oms_type)?;
        }
        Ok(())
    }

    fn project_order_events(
        &self,
        projected: &mut OrderAny,
        generated: Vec<OrderEventAny>,
        events: &mut Vec<PreparedReconciliationEvent>,
        fill_ledger: &ReconciliationFillLedger,
        oms_type: OmsType,
    ) -> anyhow::Result<()> {
        for event in generated {
            let fill_disposition = match &event {
                OrderEventAny::Filled(fill) => {
                    self.reconciliation_fill_disposition(projected, fill, fill_ledger)?
                }
                _ => ReconciliationFillDisposition::ApplyEconomics,
            };
            projected.apply(event.clone()).map_err(|error| {
                anyhow::anyhow!(
                    "Cannot project reconciliation event for {}: {error}; event={event}",
                    projected.client_order_id(),
                )
            })?;
            events.push(PreparedReconciliationEvent {
                event,
                fill_disposition,
                oms_type,
            });
        }
        Ok(())
    }

    fn reconciliation_fill_disposition(
        &self,
        projected: &OrderAny,
        fill: &OrderFilled,
        ledger: &ReconciliationFillLedger,
    ) -> anyhow::Result<ReconciliationFillDisposition> {
        let key = (fill.account_id, fill.instrument_id, fill.trade_id);
        if let Some(owner) = ledger.cached_fill_owners.get(&key) {
            anyhow::ensure!(
                *owner == projected.client_order_id(),
                "Fill {} is already owned by cached order {owner}, not {}",
                fill.trade_id,
                projected.client_order_id(),
            );
        }

        let missing_order_history = !ledger.raw_fill_keys.contains(&key)
            && (ledger.missing_order_ids.contains(&(
                fill.account_id,
                fill.instrument_id,
                fill.client_order_id,
            )) || ledger.missing_venue_order_ids.contains(&(
                fill.account_id,
                fill.instrument_id,
                fill.venue_order_id,
            )));
        let predates_netting_lifecycle = ledger
            .netting_lifecycle_starts
            .get(&(fill.account_id, fill.instrument_id, fill.strategy_id))
            .is_some_and(|ts_opened| fill.ts_event < *ts_opened);

        Ok(
            if ledger.position_fill_keys.contains(&key)
                || missing_order_history
                || predates_netting_lifecycle
            {
                ReconciliationFillDisposition::ProjectOrderOnly
            } else {
                ReconciliationFillDisposition::ApplyEconomics
            },
        )
    }

    fn ensure_order_projection_matches(
        projected: &OrderAny,
        report: &OrderStatusReport,
    ) -> anyhow::Result<()> {
        if is_superseded_cancel_report(projected, report)
            || matches!(
                report.order_status,
                OrderStatus::PendingUpdate | OrderStatus::PendingCancel
            )
        {
            return Ok(());
        }

        anyhow::ensure!(
            projected.filled_qty() == report.filled_qty,
            "Projected fill quantity {} for {} does not match venue quantity {}",
            projected.filled_qty(),
            projected.client_order_id(),
            report.filled_qty,
        );
        let status_matches = projected.status() == report.order_status
            || (projected.status() == OrderStatus::Filled
                && matches!(
                    report.order_status,
                    OrderStatus::Canceled | OrderStatus::Expired
                ));
        anyhow::ensure!(
            status_matches,
            "Projected status {} for {} does not match venue status {}",
            projected.status(),
            projected.client_order_id(),
            report.order_status,
        );
        Ok(())
    }

    /// Projects a normalized snapshot without creating a committable transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when any order, fill, or position report cannot be
    /// projected completely against current execution state.
    pub fn project_execution_reconciliation(
        &self,
        normalized: NormalizedExecutionMassStatus,
    ) -> anyhow::Result<ExecutionPositionProjection> {
        let source_client_id = normalized.source_client_id();
        let mass_status = normalized.into_mass_status();
        Ok(self
            .prepare_execution_mass_status(&mass_status, source_client_id, &AHashSet::new())?
            .position_projection)
    }

    /// Prepares a normalized snapshot without mutating execution state.
    ///
    /// # Errors
    ///
    /// Returns an error when correction evidence lies outside the authenticated
    /// position snapshot, normalized identities conflict, a report is not fully
    /// applicable, or the projected positions do not match venue evidence within
    /// `position_tolerance`.
    pub fn prepare_execution_reconciliation(
        &self,
        normalized: NormalizedExecutionMassStatus,
        corrections: Vec<OrderStatusReport>,
        position_tolerance: Decimal,
    ) -> anyhow::Result<PreparedExecutionReconciliation> {
        let recipe = ExecutionReconciliationRecipe {
            normalized,
            corrections,
            position_tolerance,
        };
        self.project_execution_reconciliation_recipes(std::slice::from_ref(&recipe))?;
        Ok(PreparedExecutionReconciliation {
            recipes: vec![recipe],
        })
    }

    /// Combines independently prepared snapshots into one all-or-nothing transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshots overlap, contradict each other, or cannot be
    /// projected together against the current execution state.
    pub fn combine_execution_reconciliations(
        &self,
        prepared: Vec<PreparedExecutionReconciliation>,
    ) -> anyhow::Result<PreparedExecutionReconciliation> {
        let recipes = prepared
            .into_iter()
            .flat_map(|prepared| prepared.recipes)
            .collect::<Vec<_>>();
        self.project_execution_reconciliation_recipes(&recipes)?;
        Ok(PreparedExecutionReconciliation { recipes })
    }

    fn project_normalized_execution_reconciliation(
        &self,
        normalized: NormalizedExecutionMassStatus,
        corrections: Vec<OrderStatusReport>,
        position_tolerance: Decimal,
    ) -> anyhow::Result<ProjectedExecutionReconciliation> {
        let source_client_id = normalized.source_client_id();
        let source_id = normalized.source_id();
        self.validate_execution_source(source_client_id, source_id)?;
        let mut mass_status = normalized.into_mass_status();
        let existing_venue_order_ids = mass_status
            .order_reports()
            .into_keys()
            .collect::<AHashSet<_>>();
        let correction_venue_order_ids = corrections
            .iter()
            .map(|report| report.venue_order_id)
            .collect::<AHashSet<_>>();
        anyhow::ensure!(
            corrections.len() == correction_venue_order_ids.len()
                && correction_venue_order_ids.is_disjoint(&existing_venue_order_ids),
            "Position corrections conflict with authoritative venue order evidence"
        );
        let position_reports = mass_status.position_reports();
        anyhow::ensure!(
            corrections.iter().all(|correction| {
                correction.account_id == mass_status.account_id
                    && position_reports
                        .get(&correction.instrument_id)
                        .is_some_and(|reports| {
                            reports.iter().any(|report| {
                                report.account_id == correction.account_id
                                    && report.venue_position_id == correction.venue_position_id
                            })
                        })
            }),
            "Position correction lies outside authenticated position evidence"
        );
        mass_status.add_order_reports(corrections)?;
        let normalized = NormalizedExecutionMassStatus::normalize(
            AuthenticatedExecutionMassStatus::new(source_client_id, source_id, mass_status),
        )?;
        self.validate_normalized_execution_mass_status(&normalized)?;
        let mass_status = normalized.into_mass_status();
        let prepared = self.prepare_execution_mass_status(
            &mass_status,
            source_client_id,
            &correction_venue_order_ids,
        )?;
        Self::ensure_position_projection_matches(&prepared, position_tolerance)?;
        Ok(prepared)
    }

    fn project_execution_reconciliation_recipes(
        &self,
        recipes: &[ExecutionReconciliationRecipe],
    ) -> anyhow::Result<ProjectedExecutionReconciliation> {
        let mut order_reconciliations = Vec::new();
        let mut position_reports = Vec::new();
        let mut position_tolerances = Vec::new();
        let mut filtered_external_orders = Vec::new();
        let mut venue_order_ids = AHashSet::new();
        let mut external_client_order_ids = AHashSet::new();

        for recipe in recipes {
            let projected = self.project_normalized_execution_reconciliation(
                recipe.normalized.clone(),
                recipe.corrections.clone(),
                recipe.position_tolerance,
            )?;

            for reconciliation in projected.order_reconciliations {
                let venue_order_id = reconciliation.venue_order_id;
                anyhow::ensure!(
                    venue_order_ids.insert(venue_order_id),
                    "Venue order {venue_order_id} appears in more than one reconciliation snapshot"
                );
                if reconciliation.external_order.is_some() {
                    anyhow::ensure!(
                        external_client_order_ids.insert(reconciliation.client_order_id),
                        "External client order {} appears in more than one reconciliation snapshot",
                        reconciliation.client_order_id,
                    );
                }
                order_reconciliations.push(reconciliation);
            }

            position_tolerances.extend(
                projected
                    .position_reports
                    .iter()
                    .cloned()
                    .map(|report| (report, recipe.position_tolerance)),
            );
            position_reports.extend(projected.position_reports);
            filtered_external_orders.extend(projected.filtered_external_orders);
        }

        let mut events = order_reconciliations
            .iter()
            .flat_map(|reconciliation| reconciliation.events.iter().cloned())
            .collect::<Vec<_>>();
        events.sort_by_key(|prepared| prepared.event.ts_event());
        let projected_orders =
            self.project_canonical_order_state(&order_reconciliations, &events)?;
        let position_projection = self.project_position_state(&projected_orders, &events)?;

        for (report, tolerance) in position_tolerances {
            let projected = position_projection.state_for(&report)?;
            anyhow::ensure!(
                (projected.signed_quantity - report.signed_decimal_qty).abs() <= tolerance,
                "Combined projected position quantity {} for {}/{} does not match venue quantity {}",
                projected.signed_quantity,
                report.account_id,
                report.instrument_id,
                report.signed_decimal_qty,
            );
        }

        Ok(ProjectedExecutionReconciliation {
            order_reconciliations,
            events,
            position_reports,
            position_projection,
            filtered_external_orders,
        })
    }

    /// Re-projects and commits a prepared reconciliation transaction.
    ///
    /// # Errors
    ///
    /// Returns an error without mutation when execution state changed after the
    /// transaction was prepared or any evidence is no longer fully applicable.
    pub fn commit_execution_reconciliation(
        &mut self,
        prepared: PreparedExecutionReconciliation,
    ) -> anyhow::Result<ExecutionReconciliationReceipt> {
        let projected = self.project_execution_reconciliation_recipes(&prepared.recipes)?;
        self.commit_projected_execution_reconciliation(projected)
    }

    fn commit_projected_execution_reconciliation(
        &mut self,
        prepared: ProjectedExecutionReconciliation,
    ) -> anyhow::Result<ExecutionReconciliationReceipt> {
        let mut receipt = ExecutionReconciliationReceipt::default();
        for (client_order_id, venue_order_id, instrument_id) in prepared.filtered_external_orders {
            self.record_filtered_external_order(client_order_id, venue_order_id, instrument_id);
        }

        for reconciliation in prepared.order_reconciliations {
            if let Some(venue_order_id) = reconciliation.venue_order_id_to_claim {
                self.cache
                    .borrow_mut()
                    .add_venue_order_id(&reconciliation.client_order_id, &venue_order_id, false)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "Prepared venue-order identity failed at commit for {}: {error}",
                            reconciliation.client_order_id,
                        )
                    })?;
            }
            if let Some(external_order) = reconciliation.external_order {
                let metadata = ReconciledExternalOrder {
                    client_order_id: external_order.order.client_order_id(),
                    venue_order_id: external_order.venue_order_id,
                    instrument_id: external_order.instrument_id,
                    strategy_id: external_order.strategy_id,
                    ts_init: external_order.ts_init,
                };
                self.commit_external_order(external_order, reconciliation.source_client_id)?;
                receipt.external_orders.push(metadata);
            }
        }

        for prepared in prepared.events {
            if self.commit_reconciliation_event(&prepared) {
                receipt.events.push(prepared.event);
            } else {
                anyhow::bail!("Prepared reconciliation event failed at commit");
            }
        }

        for report in prepared.position_reports {
            self.reconcile_position_report_without_raw_publish(&report);
        }

        Ok(receipt)
    }

    fn commit_reconciliation_event(&mut self, prepared: &PreparedReconciliationEvent) -> bool {
        self.handle_event_with_position_application_and_oms(
            &prepared.event,
            prepared.fill_disposition == ReconciliationFillDisposition::ApplyEconomics,
            prepared.oms_type,
        )
    }

    /// Executes a trading command by routing it to the appropriate execution client.
    pub fn execute(&self, command: TradingCommand) {
        self.execute_command(command);
    }

    /// Processes an order event, updating internal state and routing as needed.
    pub fn process(&mut self, event: &OrderEventAny) {
        self.handle_event(event);
    }

    /// Projects a reconciled fill onto its order without applying position or portfolio economics.
    pub fn project_reconciliation_fill(&mut self, fill: &OrderFilled) {
        self.handle_event_with_position_application(&OrderEventAny::Filled(fill.clone()), false);
    }

    /// Starts the execution engine and all registered execution clients.
    pub fn start(&mut self) {
        for client in self.get_clients_mut() {
            if let Err(e) = client.start() {
                log::error!("{e}");
            }
        }

        self.start_snapshot_timer();
        self.start_purge_timers();

        log::info!("Started");
    }

    /// Stops the execution engine and all registered execution clients.
    ///
    /// Adapters are expected to be idempotent on repeated `stop()` calls
    /// (e.g. via an internal `is_stopped` guard); the backtest teardown
    /// sequence calls `stop()` more than once per run.
    pub fn stop(&mut self) {
        for client in self.get_clients_mut() {
            if let Err(e) = client.stop() {
                log::error!("{e}");
            }
        }

        self.stop_snapshot_timer();
        self.stop_purge_timers();

        log::info!("Stopped");
    }

    /// Stops all registered execution clients without stopping the engine itself.
    pub fn stop_clients(&mut self) {
        for client in self.get_clients_mut() {
            if let Err(e) = client.stop() {
                log::error!("{e}");
            }
        }
    }

    /// Resets the execution engine and all registered execution clients to initial state.
    ///
    /// Cancels engine-owned timers (snapshot, purge) but leaves timers owned by
    /// other components on the shared clock untouched.
    pub fn reset(&mut self) {
        for client in self.get_clients_mut() {
            if let Err(e) = client.reset() {
                log::error!("{e}");
            }
        }

        self.cache.borrow_mut().reset();
        self.pos_id_generator.reset();

        self.stop_snapshot_timer();
        self.stop_purge_timers();

        self.command_count.set(0);
        self.event_count = 0;
        self.report_count = 0;
        self.filtered_unclaimed_external_order_count = 0;

        log::info!("Reset");
    }

    /// Disposes of the execution engine, releasing resources from all clients and timers.
    ///
    /// Cancels engine-owned timers (snapshot, purge) but leaves timers owned by
    /// other components on the shared clock untouched.
    pub fn dispose(&mut self) {
        for client in self.get_clients_mut() {
            if let Err(e) = client.dispose() {
                log::error!("{e}");
            }
        }

        self.stop_snapshot_timer();
        self.stop_purge_timers();

        log::info!("Disposed");
    }

    fn execute_command(&self, command: TradingCommand) {
        self.command_count.set(self.command_count.get() + 1);

        if self.config.debug {
            log::debug!("{RECV}{CMD} {command:?}");
        }

        if let Some(cid) = command.client_id()
            && self.external_clients.contains(&cid)
        {
            let topic = format!("commands.trading.{cid}");
            msgbus::publish_any(topic.into(), &command);

            if self.config.debug {
                log::debug!("Skipping execution command for external client {cid}: {command:?}");
            }
            return;
        }

        let client = if let Some(adapter) = self.find_client_for_command(&command) {
            adapter.client.as_ref()
        } else {
            let routing_context = Self::routing_context_for_command(&command);

            log::error!(
                "No execution client found for command: client_id={:?}, {routing_context}, command={command:?}",
                command.client_id(),
            );

            let reason = OrderDeniedReason::NoExecutionClient {
                client_id: command.client_id(),
                routing_context,
            }
            .to_string();

            match command {
                TradingCommand::SubmitOrder(cmd) => {
                    let order = self
                        .cache
                        .borrow()
                        .order(&cmd.client_order_id)
                        .map(|o| o.clone());

                    if let Some(order) = order {
                        self.deny_order(&order, &reason);
                    }
                }
                TradingCommand::SubmitOrderList(cmd) => {
                    let orders: Vec<OrderAny> = self
                        .cache
                        .borrow()
                        .orders_for_ids(&cmd.order_list.client_order_ids, &cmd);

                    for order in &orders {
                        self.deny_order(order, &reason);
                    }
                }
                _ => {}
            }

            return;
        };

        match command {
            TradingCommand::SubmitOrder(cmd) => self.handle_submit_order(client, cmd),
            TradingCommand::SubmitOrderList(cmd) => self.handle_submit_order_list(client, cmd),
            TradingCommand::ModifyOrder(cmd) => self.handle_modify_order(client, cmd),
            TradingCommand::ModifyOrders(cmd) => self.handle_batch_modify_orders(client, cmd),
            TradingCommand::CancelOrder(cmd) => self.handle_cancel_order(client, cmd),
            TradingCommand::CancelOrders(cmd) => self.handle_batch_cancel_orders(client, cmd),
            TradingCommand::CancelAllOrders(cmd) => self.handle_cancel_all_orders(client, cmd),
            TradingCommand::QueryOrder(cmd) => self.handle_query_order(client, cmd),
            TradingCommand::QueryAccount(cmd) => self.handle_query_account(client, cmd),
        }
    }

    fn routing_context_for_command(command: &TradingCommand) -> String {
        match command {
            TradingCommand::SubmitOrder(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::SubmitOrderList(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::ModifyOrder(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::ModifyOrders(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::CancelOrder(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::CancelOrders(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::CancelAllOrders(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::QueryOrder(cmd) => format!("venue={}", cmd.instrument_id.venue),
            TradingCommand::QueryAccount(cmd) => {
                let issuer = cmd.account_id.get_issuer();
                format!("account_id={}, issuer={issuer}", cmd.account_id)
            }
        }
    }

    fn find_client_for_command(&self, command: &TradingCommand) -> Option<&ExecutionClientAdapter> {
        if let Some(client_id) = command.client_id()
            && let Some(adapter) = self.clients.get(&client_id)
        {
            return Some(adapter);
        }

        if let Some(account_id) = self.account_id_for_command(command) {
            let issuer = account_id.get_issuer();
            let issuer_client_id = ClientId::from(issuer.as_str());

            if let Some(adapter) = self.clients.get(&issuer_client_id) {
                return Some(adapter);
            }

            if let Some(client_id) = self.routing_map.get(&issuer)
                && let Some(adapter) = self.clients.get(client_id)
            {
                return Some(adapter);
            }
        }

        if let Some(instrument_id) = Self::instrument_id_for_command(command)
            && let Some(client_id) = self.routing_map.get(&instrument_id.venue)
            && let Some(adapter) = self.clients.get(client_id)
        {
            return Some(adapter);
        }

        self.default_client_id.and_then(|id| self.clients.get(&id))
    }

    fn account_id_for_command(&self, command: &TradingCommand) -> Option<AccountId> {
        match command {
            TradingCommand::QueryAccount(cmd) => Some(cmd.account_id),
            TradingCommand::SubmitOrder(cmd) => self
                .cache
                .borrow()
                .order(&cmd.client_order_id)
                .and_then(|order| order.account_id()),
            TradingCommand::ModifyOrder(cmd) => self
                .cache
                .borrow()
                .order(&cmd.client_order_id)
                .and_then(|order| order.account_id()),
            TradingCommand::CancelOrder(cmd) => self
                .cache
                .borrow()
                .order(&cmd.client_order_id)
                .and_then(|order| order.account_id()),
            TradingCommand::SubmitOrderList(_)
            | TradingCommand::ModifyOrders(_)
            | TradingCommand::CancelOrders(_)
            | TradingCommand::CancelAllOrders(_)
            | TradingCommand::QueryOrder(_) => None,
        }
    }

    const fn instrument_id_for_command(command: &TradingCommand) -> Option<InstrumentId> {
        match command {
            TradingCommand::SubmitOrder(cmd) => Some(cmd.instrument_id),
            TradingCommand::SubmitOrderList(cmd) => Some(cmd.instrument_id),
            TradingCommand::ModifyOrder(cmd) => Some(cmd.instrument_id),
            TradingCommand::ModifyOrders(cmd) => Some(cmd.instrument_id),
            TradingCommand::CancelOrder(cmd) => Some(cmd.instrument_id),
            TradingCommand::CancelOrders(cmd) => Some(cmd.instrument_id),
            TradingCommand::CancelAllOrders(cmd) => Some(cmd.instrument_id),
            TradingCommand::QueryOrder(cmd) => Some(cmd.instrument_id),
            TradingCommand::QueryAccount(_) => None,
        }
    }

    fn handle_submit_order(&self, client: &dyn ExecutionClient, cmd: SubmitOrder) {
        let client_order_id = cmd.client_order_id;
        let cached_order = { self.cache.borrow().order_owned(&client_order_id) };

        let (order, added_to_cache) = match cached_order {
            Some(order) => (order, false),
            None => {
                let Some(order) =
                    self.add_order_from_init(&cmd.order_init, cmd.position_id, cmd.client_id, &cmd)
                else {
                    return;
                };

                (order, true)
            }
        };

        if added_to_cache && self.config.snapshot_orders {
            self.create_order_state_snapshot(&order);
        }

        let order_venue = order.instrument_id().venue;
        let client_venue = client.venue();
        if !client.handles_order_venue(order_venue) {
            let client_id = client.client_id();
            let reason = OrderDeniedReason::ClientVenueMismatch {
                client_id,
                order_venue,
                client_venue,
            }
            .to_string();
            self.deny_order(&order, &reason);
            return;
        }

        if let Some(reason) = self.check_position_id_against_oms(
            cmd.instrument_id,
            cmd.strategy_id,
            cmd.position_id,
            client,
        ) {
            self.deny_order(&order, &reason.to_string());
            return;
        }

        let instrument_id = order.instrument_id();

        if !added_to_cache && self.config.snapshot_orders {
            self.create_order_state_snapshot(&order);
        }

        {
            let cache = self.cache.borrow();
            if cache.instrument(&instrument_id).is_none() {
                log::error!(
                    "Cannot handle submit order: no instrument found for {instrument_id}, {cmd}",
                );
                return;
            }
        }

        if self.config.manage_own_order_books && should_handle_own_book_order(&order) {
            let mut own_book = self.get_or_init_own_order_book(&order.instrument_id());
            own_book.add(order.to_own_book_order());
        }

        log_info!("Submit {order}", color = LogColor::Blue);

        if let Err(e) = client.submit_order(cmd) {
            self.deny_order(
                &order,
                &OrderDeniedReason::SubmitFailed {
                    detail: e.to_string(),
                }
                .to_string(),
            );
        }
    }

    fn handle_submit_order_list(&self, client: &dyn ExecutionClient, cmd: SubmitOrderList) {
        let mut orders = Vec::with_capacity(cmd.order_list.client_order_ids.len());
        let mut added_client_order_ids = AHashSet::new();

        for client_order_id in &cmd.order_list.client_order_ids {
            let cached_order = { self.cache.borrow().order_owned(client_order_id) };

            if let Some(order) = cached_order {
                orders.push(order);
                continue;
            }

            let Some(order_init) = cmd
                .order_inits
                .iter()
                .find(|init| init.client_order_id == *client_order_id)
            else {
                log::error!(
                    "Cannot handle submit order list: order not found in cache and no initialization event for {client_order_id}, {cmd}"
                );
                continue;
            };

            let Some(order) =
                self.add_order_from_init(order_init, cmd.position_id, cmd.client_id, &cmd)
            else {
                continue;
            };

            added_client_order_ids.insert(order.client_order_id());
            orders.push(order);
        }

        if self.config.snapshot_orders {
            for order in &orders {
                if added_client_order_ids.contains(&order.client_order_id()) {
                    self.create_order_state_snapshot(order);
                }
            }
        }

        if orders.len() != cmd.order_list.client_order_ids.len() {
            let reason = OrderDeniedReason::OrderListIncomplete {
                order_list_id: cmd.order_list.id,
            }
            .to_string();

            for order in &orders {
                self.deny_order(order, &reason);
            }
            return;
        }

        let order_list_venue = cmd.instrument_id.venue;
        let client_venue = client.venue();
        if !client.handles_order_venue(order_list_venue) {
            let client_id = client.client_id();
            let reason = OrderDeniedReason::ClientVenueMismatch {
                client_id,
                order_venue: order_list_venue,
                client_venue,
            }
            .to_string();

            for order in &orders {
                self.deny_order(order, &reason);
            }
            return;
        }

        let is_uniform_instrument = orders
            .iter()
            .all(|o| o.instrument_id() == cmd.instrument_id);

        if let Some(position_id) = cmd.position_id
            && !is_uniform_instrument
        {
            let reason = OrderDeniedReason::InvalidPositionId {
                position_id,
                detail: "not valid for a mixed-instrument order list; a position belongs to a single instrument"
                    .to_string(),
            }
            .to_string();

            for order in &orders {
                self.deny_order(order, &reason);
            }
            return;
        }

        if let Some(reason) = self.check_position_id_against_oms(
            cmd.instrument_id,
            cmd.strategy_id,
            cmd.position_id,
            client,
        ) {
            let reason = reason.to_string();
            for order in &orders {
                self.deny_order(order, &reason);
            }
            return;
        }

        if self.config.snapshot_orders {
            for order in &orders {
                if !added_client_order_ids.contains(&order.client_order_id()) {
                    self.create_order_state_snapshot(order);
                }
            }
        }

        {
            let cache = self.cache.borrow();
            if cache.instrument(&cmd.instrument_id).is_none() {
                log::error!(
                    "Cannot handle submit order list: no instrument found for {}, {cmd}",
                    cmd.instrument_id,
                );
                return;
            }
        }

        if self.config.manage_own_order_books {
            for order in &orders {
                if should_handle_own_book_order(order) {
                    let mut own_book = self.get_or_init_own_order_book(&order.instrument_id());
                    own_book.add(order.to_own_book_order());
                }
            }
        }

        log_info!("Submit {}", cmd.order_list, color = LogColor::Blue);

        if let Err(e) = client.submit_order_list(cmd) {
            log::error!("Error submitting order list to client: {e}");
            let reason = OrderDeniedReason::SubmitFailed {
                detail: e.to_string(),
            }
            .to_string();

            for order in &orders {
                self.deny_order(order, &reason);
            }
        }
    }

    fn add_order_from_init(
        &self,
        order_init: &OrderInitialized,
        position_id: Option<PositionId>,
        client_id: Option<ClientId>,
        context: &dyn Display,
    ) -> Option<OrderAny> {
        let client_order_id = order_init.client_order_id;
        let order = match OrderAny::from_events(vec![OrderEventAny::Initialized(
            order_init.clone(),
        )]) {
            Ok(order) => order,
            Err(e) => {
                log::error!(
                    "Cannot reconstruct order from initialization event for {client_order_id}: {e}, {context}"
                );
                return None;
            }
        };

        if let Err(e) =
            self.cache
                .borrow_mut()
                .add_order(order.clone(), position_id, client_id, true)
        {
            log::error!(
                "Cannot add reconstructed order to cache for {client_order_id}: {e}, {context}"
            );
            return None;
        }

        Some(order)
    }

    fn handle_modify_order(&self, client: &dyn ExecutionClient, cmd: ModifyOrder) {
        let venue_str = cmd
            .venue_order_id
            .map_or_else(String::new, |venue_order_id| format!(" {venue_order_id}"));

        log_info!(
            "Modify {}{venue_str}",
            cmd.client_order_id,
            color = LogColor::Blue
        );

        if let Err(e) = client.modify_order(cmd) {
            log::error!("Error modifying order: {e}");
        }
    }

    fn handle_batch_modify_orders(&self, client: &dyn ExecutionClient, cmd: BatchModifyOrders) {
        if let Err(e) = client.batch_modify_orders(cmd) {
            log::error!("Error batch modifying orders: {e}");
        }
    }

    fn handle_cancel_order(&self, client: &dyn ExecutionClient, cmd: CancelOrder) {
        let venue_str = cmd
            .venue_order_id
            .map_or_else(String::new, |venue_order_id| format!(" {venue_order_id}"));

        log_info!(
            "Cancel {}{venue_str}",
            cmd.client_order_id,
            color = LogColor::Blue
        );

        if let Err(e) = client.cancel_order(cmd) {
            log::error!("Error canceling order: {e}");
        }
    }

    fn handle_cancel_all_orders(&self, client: &dyn ExecutionClient, cmd: CancelAllOrders) {
        let side_str = match cmd.order_side {
            OrderSide::NoOrderSide => " ".to_string(),
            order_side => format!(" {order_side} "),
        };

        log_info!("Cancel all{side_str}orders", color = LogColor::Blue);

        if let Err(e) = client.cancel_all_orders(cmd) {
            log::error!("Error canceling all orders: {e}");
        }
    }

    fn handle_batch_cancel_orders(&self, client: &dyn ExecutionClient, cmd: BatchCancelOrders) {
        let client_order_ids: Vec<ClientOrderId> = cmd
            .cancels
            .iter()
            .map(|cancel| cancel.client_order_id)
            .collect();

        log_info!(
            "Batch cancel orders {client_order_ids:?}",
            color = LogColor::Blue
        );

        if let Err(e) = client.batch_cancel_orders(cmd) {
            log::error!("Error batch canceling orders: {e}");
        }
    }

    fn handle_query_account(&self, client: &dyn ExecutionClient, cmd: QueryAccount) {
        log_info!("Query {}", cmd.account_id, color = LogColor::Blue);

        if let Err(e) = client.query_account(cmd) {
            log::warn!("Error querying account: {e}");
        }
    }

    fn handle_query_order(&self, client: &dyn ExecutionClient, cmd: QueryOrder) {
        log_info!("Query {}", cmd.client_order_id, color = LogColor::Blue);

        if let Err(e) = client.query_order(cmd) {
            log::warn!("Error querying order: {e}");
        }
    }

    fn create_order_state_snapshot(&self, order: &OrderAny) {
        if self.config.debug {
            log::debug!("Creating order state snapshot for {order}");
        }

        if self.cache.borrow().has_backing()
            && let Err(e) = self.cache.borrow().snapshot_order_state(order)
        {
            log::warn!("Failed to snapshot order state: {e}");
        }
    }

    fn create_position_state_snapshot(&self, position: &Position, open_only: bool) {
        Self::publish_position_state_snapshot(
            &self.clock,
            &self.cache,
            self.config.debug,
            position,
            open_only,
        );
    }

    fn publish_position_state_snapshot(
        clock: &Rc<RefCell<dyn Clock>>,
        cache: &Rc<RefCell<Cache>>,
        debug: bool,
        position: &Position,
        open_only: bool,
    ) {
        if debug {
            log::debug!("Creating position state snapshot for {position}");
        }

        let ts_snapshot = clock.borrow().timestamp_ns();
        let unrealized_pnl = cache.borrow().calculate_unrealized_pnl(position);

        let snapshot = PositionStateSnapshot {
            position: position.clone(),
            unrealized_pnl,
            ts_snapshot,
        };

        let topic = switchboard::get_snapshot_position_topic(position.id);
        msgbus::publish_any(topic, &snapshot);

        let has_backing = cache.borrow().has_backing();
        if has_backing
            && let Err(e) = cache.borrow_mut().snapshot_position_state(
                position,
                ts_snapshot,
                unrealized_pnl,
                Some(open_only),
            )
        {
            log::warn!("Failed to snapshot position state: {e}");
        }
    }

    fn handle_event(&mut self, event: &OrderEventAny) {
        self.handle_event_with_position_application(event, true);
    }

    fn handle_event_with_position_application(
        &mut self,
        event: &OrderEventAny,
        apply_position: bool,
    ) -> bool {
        let fill_oms_type = match event {
            OrderEventAny::Filled(fill) => self.determine_oms_type(fill),
            _ => OmsType::Unspecified,
        };
        self.handle_event_with_position_application_and_oms(event, apply_position, fill_oms_type)
    }

    fn handle_event_with_position_application_and_oms(
        &mut self,
        event: &OrderEventAny,
        apply_position: bool,
        fill_oms_type: OmsType,
    ) -> bool {
        self.event_count += 1;

        if self.config.debug {
            log::debug!("{RECV}{EVT} {event:?}");
        }

        let event_client_order_id = event.client_order_id();
        let cache = self.cache.borrow();
        let client_order_id = if cache.order_exists(&event_client_order_id) {
            event_client_order_id
        } else {
            let is_leg_fill =
                matches!(event, OrderEventAny::Filled(fill) if self.is_leg_fill(fill));
            if !is_leg_fill {
                log::warn!(
                    "Order with {} not found in the cache to apply {}",
                    event.client_order_id(),
                    event
                );
            }

            // Try to find order by venue order ID if available
            let venue_order_id = if let Some(id) = event.venue_order_id() {
                id
            } else {
                log::error!(
                    "Cannot apply event to any order: {} not found in the cache with no VenueOrderId",
                    event.client_order_id()
                );
                return false;
            };

            // Look up client order ID from venue order ID
            let client_order_id = if let Some(id) = cache.client_order_id(&venue_order_id) {
                *id
            } else {
                if let OrderEventAny::Filled(fill) = event
                    && is_leg_fill
                {
                    log::info!(
                        "Processing leg fill without corresponding order: {} for instrument {}",
                        fill.client_order_id,
                        fill.instrument_id
                    );
                    drop(cache);
                    return self.handle_leg_fill_without_order(fill.clone());
                }

                log::error!(
                    "Cannot apply event to any order: {} and {venue_order_id} not found in the cache",
                    event.client_order_id(),
                );
                return false;
            };

            // Get order using found client order ID
            if cache.order_exists(&client_order_id) {
                log::info!("Order with {client_order_id} was found in the cache");
                client_order_id
            } else {
                if let OrderEventAny::Filled(fill) = event
                    && is_leg_fill
                {
                    log::info!(
                        "Processing leg fill without corresponding order: {} for instrument {}",
                        fill.client_order_id,
                        fill.instrument_id
                    );
                    drop(cache);
                    return self.handle_leg_fill_without_order(fill.clone());
                }

                log::error!(
                    "Cannot apply event to any order: {client_order_id} and {venue_order_id} not found in cache",
                );
                return false;
            }
        };
        let order_before_fill = if matches!(event, OrderEventAny::Filled(_)) {
            cache.order(&client_order_id).map(|o| o.clone())
        } else {
            None
        };

        drop(cache);

        let event = if event_client_order_id == client_order_id {
            event.clone()
        } else {
            event.clone().with_client_order_id(client_order_id)
        };

        match &event {
            OrderEventAny::Filled(fill) => {
                let Some(order_before_fill) = order_before_fill else {
                    log::error!(
                        "Cannot apply fill: order {} not found in the cache",
                        fill.client_order_id()
                    );
                    return false;
                };
                let configured_oms_type = fill_oms_type;
                let position_id =
                    self.determine_position_id(fill, configured_oms_type, Some(&order_before_fill));
                let oms_type = self
                    .cache
                    .borrow()
                    .oms_type(&position_id)
                    .unwrap_or(configured_oms_type);

                let mut fill = fill.clone();
                fill.position_id = Some(position_id);

                let validation = if apply_position {
                    self.validate_fill_for_order(&order_before_fill, &fill)
                } else {
                    self.validate_fill_for_order_projection(&order_before_fill, &fill)
                };

                if validation.is_err() {
                    return false;
                }
                let event = OrderEventAny::Filled(fill.clone());
                let Some(order) = self.update_cached_order(client_order_id, &event, apply_position)
                else {
                    return false;
                };

                let position_events = if apply_position {
                    self.handle_order_fill(&order, fill, oms_type)
                } else {
                    Vec::new()
                };
                self.publish_order_event(&event);
                self.publish_position_events(position_events);
                true
            }
            OrderEventAny::FillVoided(voided) => {
                let mut voided = voided.clone();
                let Some(order_before_void) = self
                    .cache
                    .borrow()
                    .order(&client_order_id)
                    .map(|order| order.clone())
                else {
                    log::error!("Cannot apply fill void: order {client_order_id} not found");
                    return false;
                };
                let original_fill = order_before_void
                    .events()
                    .into_iter()
                    .find_map(|candidate| match candidate {
                        OrderEventAny::Filled(fill) if fill.trade_id == voided.trade_id => {
                            Some(fill.clone())
                        }
                        _ => None,
                    });

                if voided.position_id.is_none() {
                    voided.position_id = original_fill.as_ref().and_then(|fill| fill.position_id);
                }
                let event = OrderEventAny::FillVoided(voided.clone());

                let mut validated_order = order_before_void.clone();
                match validated_order.apply(event.clone()) {
                    Ok(()) => {}
                    Err(OrderError::DuplicateFillVoid(trade_id)) => {
                        log::warn!(
                            "Duplicate fill void rejected at order level: trade_id={trade_id}"
                        );
                        return false;
                    }
                    Err(e) => {
                        log::error!("Cannot apply fill void to order: {e}");
                        return false;
                    }
                }

                let corrected_positions = if apply_position
                    && original_fill
                        .as_ref()
                        .is_some_and(|fill| fill.position_id.is_some())
                {
                    match self.prepare_order_fill_void_positions(&order_before_void, &voided) {
                        Ok(positions) => positions,
                        Err(e) => {
                            log::error!("Cannot apply fill void to positions: {e}");
                            return false;
                        }
                    }
                } else {
                    Vec::new()
                };

                let mut position_events = Vec::new();

                for CorrectedPosition {
                    position,
                    corrected_qty,
                    absorbed_prior_cycles,
                    closed_cycles_pnl,
                } in corrected_positions
                {
                    if let Err(e) = self.cache.borrow_mut().update_position(&position) {
                        log::error!("Cannot apply fill void to position {}: {e}", position.id);
                        return false;
                    }

                    if absorbed_prior_cycles {
                        log::info!(
                            "Settling archived NETTING cycles rebuilt by fill void {} for position {}: realized={closed_cycles_pnl:?}",
                            voided.trade_id,
                            position.id,
                        );

                        self.cache
                            .borrow_mut()
                            .settle_position_snapshots(&position, closed_cycles_pnl);
                    }

                    if self.config.snapshot_positions {
                        self.create_position_state_snapshot(&position, false);
                    }

                    position_events.push(Self::create_fill_void_position_event(
                        &position,
                        &voided,
                        corrected_qty,
                    ));
                }

                if self
                    .update_cached_order(client_order_id, &event, true)
                    .is_none()
                {
                    return false;
                }

                if original_fill.is_some() {
                    let portfolio_endpoint = MessagingSwitchboard::portfolio_update_order();
                    msgbus::send_order_event(portfolio_endpoint, event.clone());
                }
                self.publish_order_event(&event);
                self.publish_position_events(position_events);
                true
            }
            _ => {
                if self
                    .update_cached_order(client_order_id, &event, true)
                    .is_some()
                {
                    self.publish_order_event(&event);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn handle_leg_fill_without_order(&mut self, mut fill: OrderFilled) -> bool {
        let instrument =
            if let Some(instrument) = self.cache.borrow().instrument(&fill.instrument_id) {
                instrument.clone()
            } else {
                log::error!(
                    "Cannot handle leg fill: no instrument found for {}, {fill}",
                    fill.instrument_id,
                );
                return false;
            };

        if let Err(e) = self.cache.borrow().try_account(&fill.account_id) {
            log::error!("Cannot handle leg fill: {e}, {fill}");
            return false;
        }

        let oms_type = self.determine_oms_type(&fill);
        let position_id = self.determine_leg_fill_position_id(&fill, oms_type);
        fill.position_id = Some(position_id);
        let duplicate_position_fill = self.position_contains_trade_id(position_id, fill.trade_id);

        let event = OrderEventAny::Filled(fill.clone());

        if duplicate_position_fill {
            log::warn!(
                "Duplicate leg fill: {} trade_id={} already applied to position {}, skipping",
                fill.client_order_id,
                fill.trade_id,
                position_id
            );
            return false;
        }

        let portfolio_endpoint = MessagingSwitchboard::portfolio_update_order();
        msgbus::send_order_event(portfolio_endpoint, event.clone());
        let position_events = self.handle_position_update(&instrument, fill, oms_type);
        self.publish_order_event(&event);
        self.publish_position_events(position_events);
        true
    }

    fn determine_leg_fill_position_id(
        &mut self,
        fill: &OrderFilled,
        oms_type: OmsType,
    ) -> PositionId {
        let cache = self.cache.borrow();
        let cached_position_id = cache.position_id(&fill.client_order_id()).copied();
        drop(cache);

        if let Some(position_id) = cached_position_id {
            if let Some(fill_position_id) = fill.position_id
                && fill_position_id != position_id
            {
                log::warn!(
                    "Incorrect position ID assigned to leg fill: \
                     cached={position_id}, assigned={fill_position_id}; \
                     re-assigning from cache",
                );
            }

            return position_id;
        }

        match oms_type {
            OmsType::Hedging => fill
                .position_id
                .unwrap_or_else(|| self.pos_id_generator.generate(fill.strategy_id, false)),
            OmsType::Netting => self.determine_netting_position_id(fill),
            _ => self.determine_netting_position_id(fill),
        }
    }

    fn is_leg_fill(&self, fill: &OrderFilled) -> bool {
        if !fill.client_order_id.as_str().contains("-LEG-")
            && !fill.venue_order_id.as_str().contains("-LEG-")
        {
            return false;
        }

        self.cache
            .borrow()
            .instrument(&fill.instrument_id)
            .is_some_and(|instrument| !instrument.is_spread())
    }

    fn determine_oms_type(&self, fill: &OrderFilled) -> OmsType {
        if let Some(oms_type) = self.oms_overrides.get(&fill.strategy_id)
            && *oms_type != OmsType::Unspecified
        {
            return *oms_type;
        }

        if let Some(client_id) = self.routing_map.get(&fill.instrument_id.venue)
            && let Some(client) = self.clients.get(client_id)
        {
            return client.oms_type;
        }

        if let Some(client) = self.default_client_id.and_then(|id| self.clients.get(&id)) {
            return client.oms_type;
        }

        OmsType::Netting // Default fallback
    }

    fn resolve_oms_type_for_client(
        &self,
        strategy_id: StrategyId,
        client: &dyn ExecutionClient,
    ) -> OmsType {
        if let Some(oms_type) = self.oms_overrides.get(&strategy_id)
            && *oms_type != OmsType::Unspecified
        {
            return *oms_type;
        }

        client.oms_type()
    }

    fn check_position_id_against_oms(
        &self,
        instrument_id: InstrumentId,
        strategy_id: StrategyId,
        position_id: Option<PositionId>,
        client: &dyn ExecutionClient,
    ) -> Option<OrderDeniedReason> {
        let position_id = position_id?;

        if self.resolve_oms_type_for_client(strategy_id, client) != OmsType::Netting {
            return None;
        }

        let expected = format!("{instrument_id}-{strategy_id}");
        if position_id.as_str() == expected {
            return None;
        }

        Some(OrderDeniedReason::InvalidPositionId {
            position_id,
            detail: format!(
                "not valid for NETTING OMS; expected '{expected}' (use HEDGING for custom position IDs)"
            ),
        })
    }

    fn determine_position_id(
        &mut self,
        fill: &OrderFilled,
        oms_type: OmsType,
        order: Option<&OrderAny>,
    ) -> PositionId {
        let cache = self.cache.borrow();
        let cached_position_id = cache.position_id(&fill.client_order_id()).copied();
        drop(cache);

        if self.config.debug {
            log::debug!(
                "Determining position ID for {}, position_id={:?}",
                fill.client_order_id(),
                cached_position_id,
            );
        }

        if let Some(position_id) = cached_position_id {
            if let Some(fill_position_id) = fill.position_id
                && fill_position_id != position_id
            {
                log::warn!(
                    "Incorrect position ID assigned to fill: \
                     cached={position_id}, assigned={fill_position_id}; \
                     re-assigning from cache",
                );
            }

            if self.config.debug {
                log::debug!("Assigned {position_id} to {}", fill.client_order_id());
            }

            return position_id;
        }

        let position_id = match oms_type {
            OmsType::Hedging => self.determine_hedging_position_id(fill, order),
            OmsType::Netting => self.determine_netting_position_id(fill),
            _ => self.determine_netting_position_id(fill),
        };

        let order = if let Some(o) = order {
            o.clone()
        } else {
            let cache = self.cache.borrow();
            cache.order(&fill.client_order_id()).map_or_else(
                || {
                    panic!(
                        "Order for {} not found to determine position ID",
                        fill.client_order_id()
                    )
                },
                |o| o.clone(),
            )
        };

        if order.exec_algorithm_id().is_some()
            && let Some(exec_spawn_id) = order.exec_spawn_id()
        {
            let cache = self.cache.borrow();
            let primary = if let Some(p) = cache.order(&exec_spawn_id) {
                p.clone()
            } else {
                log::warn!(
                    "Primary exec spawn order {exec_spawn_id} not found, \
                     skipping position ID propagation"
                );
                return position_id;
            };
            let primary_already_indexed = cache.position_id(&primary.client_order_id()).is_some();
            drop(cache);

            if primary.position_id().is_none() && !primary_already_indexed {
                if let Some(mut primary_mut) = self.cache.borrow_mut().order_mut(&exec_spawn_id) {
                    primary_mut.set_position_id(Some(position_id));
                }
                let _ = self.cache.borrow_mut().add_position_id(
                    &position_id,
                    &primary.instrument_id().venue,
                    &primary.client_order_id(),
                    &primary.strategy_id(),
                );
                log::debug!("Assigned primary order {position_id}");
            }
        }

        position_id
    }

    fn determine_hedging_position_id(
        &mut self,
        fill: &OrderFilled,
        order: Option<&OrderAny>,
    ) -> PositionId {
        // Check if position ID already exists
        if let Some(position_id) = fill.position_id {
            if self.config.debug {
                log::debug!("Already had a position ID of: {position_id}");
            }
            return position_id;
        }

        let cache = self.cache.borrow();

        let cached_order;
        let order: &OrderAny = if let Some(order) = order {
            order
        } else {
            cached_order = cache.order(&fill.client_order_id()).unwrap_or_else(|| {
                panic!(
                    "Order for {} not found to determine position ID",
                    fill.client_order_id()
                )
            });
            &cached_order
        };

        // Check execution spawn orders
        if let Some(spawn_id) = order.exec_spawn_id() {
            let spawn_orders = cache.orders_for_exec_spawn(&spawn_id);
            for spawned_order in spawn_orders {
                if let Some(pos_id) = spawned_order.position_id() {
                    if self.config.debug {
                        log::debug!("Found spawned {} for {}", pos_id, fill.client_order_id());
                    }
                    return pos_id;
                }
            }
        }

        if order.is_reduce_only() {
            let mut candidates = cache
                .positions_open(
                    None,
                    Some(&fill.instrument_id),
                    Some(&fill.strategy_id),
                    Some(&fill.account_id),
                    None,
                )
                .into_iter()
                .filter(|position| position.is_opposite_side(fill.order_side));
            let candidate = candidates.next();

            if let Some(position) = candidate
                && candidates.next().is_none()
                && order.would_reduce_only(position.side, position.quantity)
            {
                if self.config.debug {
                    log::debug!(
                        "Assigned reduce-only fill {} to position {}",
                        fill.client_order_id(),
                        position.id
                    );
                }
                return position.id;
            }
        }

        // Generate new position ID
        let position_id = self.pos_id_generator.generate(fill.strategy_id, false);

        if self.config.debug {
            log::debug!("Generated {} for {}", position_id, fill.client_order_id());
        }
        position_id
    }

    fn determine_netting_position_id(&self, fill: &OrderFilled) -> PositionId {
        PositionId::new(format!("{}-{}", fill.instrument_id, fill.strategy_id))
    }

    fn validate_fill_for_order(&self, order: &OrderAny, fill: &OrderFilled) -> anyhow::Result<()> {
        if order.is_duplicate_fill(fill) {
            log::warn!(
                "Duplicate fill: {} trade_id={} already applied, skipping",
                order.client_order_id(),
                fill.trade_id
            );
            anyhow::bail!("Duplicate fill");
        }

        if let Some(position_id) = fill.position_id
            && self.position_contains_trade_id(position_id, fill.trade_id)
        {
            log::warn!(
                "Duplicate fill: {} trade_id={} already applied to position {}, skipping",
                order.client_order_id(),
                fill.trade_id,
                position_id
            );
            anyhow::bail!("Duplicate position fill");
        }

        self.check_overfill(order, fill)
    }

    fn validate_fill_for_order_projection(
        &self,
        order: &OrderAny,
        fill: &OrderFilled,
    ) -> anyhow::Result<()> {
        if order.is_duplicate_fill(fill) {
            anyhow::bail!("Duplicate fill");
        }

        self.check_overfill(order, fill)
    }

    fn position_contains_trade_id(&self, position_id: PositionId, trade_id: TradeId) -> bool {
        self.cache
            .borrow()
            .position(&position_id)
            .is_some_and(|position| position.trade_ids.contains(&trade_id))
    }

    fn update_cached_order(
        &self,
        client_order_id: ClientOrderId,
        event: &OrderEventAny,
        send_portfolio_update: bool,
    ) -> Option<OrderAny> {
        let result = { self.cache.borrow_mut().update_order(event) };

        let order = match result {
            Ok(order) => order,
            Err(e) => {
                if matches!(
                    e.downcast_ref::<OrderError>(),
                    Some(OrderError::InvalidStateTransition)
                ) {
                    // A non-fill event that fails to apply to an already-closed order is an
                    // expected venue race (e.g. a place reject then a stream cancel for the same
                    // order), not an anomaly. A dropped fill stays at warn even on a closed order,
                    // since it represents real, possibly lost, execution.
                    let already_closed = self
                        .cache
                        .borrow()
                        .order(&client_order_id)
                        .is_some_and(|o| o.is_closed());

                    if already_closed && !matches!(event, OrderEventAny::Filled(_)) {
                        log::debug!("InvalidStateTrigger: {e}, did not apply {event}");
                    } else {
                        log::warn!("InvalidStateTrigger: {e}, did not apply {event}");
                    }
                    return None;
                }

                if let Some(OrderError::DuplicateFill(trade_id)) = e.downcast_ref::<OrderError>() {
                    log::warn!(
                        "Duplicate fill rejected at order level: trade_id={trade_id}, did not apply {event}"
                    );
                    return None;
                }

                if let Some(OrderError::DuplicateFillVoid(trade_id)) =
                    e.downcast_ref::<OrderError>()
                {
                    log::warn!(
                        "Duplicate fill void rejected at order level: trade_id={trade_id}, did not apply {event}"
                    );
                    return None;
                }

                log::error!("Error applying event: {e}, did not apply {event}");

                if matches!(
                    event,
                    OrderEventAny::Denied(_)
                        | OrderEventAny::Rejected(_)
                        | OrderEventAny::Canceled(_)
                        | OrderEventAny::Expired(_)
                ) {
                    log::warn!(
                        "Terminal event {event} failed to apply to {client_order_id}, forcing cleanup from own book"
                    );
                    self.cache
                        .borrow_mut()
                        .force_remove_from_own_order_book(&client_order_id);
                } else {
                    let order = self
                        .cache
                        .borrow()
                        .order(&client_order_id)
                        .map(|o| o.clone());

                    if let Some(order) = order {
                        let should_update_own_book = {
                            let cache = self.cache.borrow();
                            let own_book = cache.own_order_book(&order.instrument_id());
                            (own_book.is_some() && order.is_closed())
                                || should_handle_own_book_order(&order)
                        };

                        if should_update_own_book {
                            self.cache.borrow_mut().update_own_order_book(&order);
                        }
                    }
                }
                return None;
            }
        };

        if self.config.manage_own_order_books && should_handle_own_book_order(&order) {
            let needs_own_book = {
                self.cache
                    .borrow()
                    .own_order_book(&order.instrument_id())
                    .is_none()
            };

            if needs_own_book {
                self.cache.borrow_mut().update_own_order_book(&order);
            }
        }

        if self.config.debug {
            log::debug!("{SEND}{EVT} {event}");
        }

        if self.config.snapshot_orders {
            self.create_order_state_snapshot(&order);
        }

        if send_portfolio_update {
            self.send_order_update_to_portfolio(event);
        }

        Some(order)
    }

    fn send_order_update_to_portfolio(&self, event: &OrderEventAny) {
        let send_to_portfolio = match event {
            OrderEventAny::Filled(fill) => self
                .cache
                .borrow()
                .account(&fill.account_id)
                .is_none_or(|account| !account.is_margin_account()),
            OrderEventAny::Accepted(_)
            | OrderEventAny::Canceled(_)
            | OrderEventAny::Expired(_)
            | OrderEventAny::Rejected(_)
            | OrderEventAny::Updated(_) => true,
            _ => false,
        };

        if send_to_portfolio {
            let portfolio_endpoint = MessagingSwitchboard::portfolio_update_order();
            msgbus::send_order_event(portfolio_endpoint, event.clone());
        }
    }

    fn publish_order_event(&self, event: &OrderEventAny) {
        let topic = switchboard::get_event_order_topic(event.strategy_id());
        msgbus::publish_order_event(topic, event);

        let topic = match event {
            OrderEventAny::Submitted(_) => {
                switchboard::get_order_submitted_topic(event.instrument_id())
            }
            OrderEventAny::Rejected(_) => {
                switchboard::get_order_rejected_topic(event.instrument_id())
            }
            OrderEventAny::PendingUpdate(_) => {
                switchboard::get_order_pending_update_topic(event.instrument_id())
            }
            OrderEventAny::PendingCancel(_) => {
                switchboard::get_order_pending_cancel_topic(event.instrument_id())
            }
            OrderEventAny::ModifyRejected(_) => {
                switchboard::get_order_modify_rejected_topic(event.instrument_id())
            }
            OrderEventAny::CancelRejected(_) => {
                switchboard::get_order_cancel_rejected_topic(event.instrument_id())
            }
            OrderEventAny::Canceled(_) => {
                switchboard::get_order_canceled_topic(event.instrument_id())
            }
            // Keep Filled out of this generic fanout: handle_order_fill publishes the instrument
            // topic, while leg fills stay on the strategy topic.
            _ => return,
        };

        msgbus::publish_order_event(topic, event);
    }

    fn publish_position_events(&self, events: Vec<PositionEvent>) {
        for event in events {
            let strategy_id = match &event {
                PositionEvent::PositionOpened(event) => event.strategy_id,
                PositionEvent::PositionChanged(event) => event.strategy_id,
                PositionEvent::PositionClosed(event) => event.strategy_id,
                PositionEvent::PositionAdjusted(event) => event.strategy_id,
            };
            let topic = switchboard::get_event_position_topic(strategy_id);
            msgbus::publish_position_event(topic, &event);
        }
    }

    fn check_overfill(&self, order: &OrderAny, fill: &OrderFilled) -> anyhow::Result<()> {
        let potential_overfill = order.calculate_overfill(fill.last_qty);

        if potential_overfill.is_positive() {
            if self.config.allow_overfills {
                log::warn!(
                    "Order overfill detected: {} potential_overfill={}, current_filled={}, last_qty={}, quantity={}",
                    order.client_order_id(),
                    potential_overfill,
                    order.filled_qty(),
                    fill.last_qty,
                    order.quantity()
                );
            } else {
                let msg = format!(
                    "Order overfill rejected: {} potential_overfill={}, current_filled={}, last_qty={}, quantity={}. \
                Set `allow_overfills=true` in ExecutionEngineConfig to allow overfills.",
                    order.client_order_id(),
                    potential_overfill,
                    order.filled_qty(),
                    fill.last_qty,
                    order.quantity()
                );
                anyhow::bail!("{msg}");
            }
        }

        Ok(())
    }

    fn handle_order_fill(
        &mut self,
        order: &OrderAny,
        fill: OrderFilled,
        oms_type: OmsType,
    ) -> Vec<PositionEvent> {
        let instrument =
            if let Some(instrument) = self.cache.borrow().instrument(&fill.instrument_id) {
                instrument.clone()
            } else {
                log::error!(
                    "Cannot handle order fill: no instrument found for {}, {fill}",
                    fill.instrument_id,
                );
                return Vec::new();
            };

        let is_margin_account = {
            let cache = self.cache.borrow();
            let account = match cache.try_account(&fill.account_id) {
                Ok(account) => account,
                Err(e) => {
                    log::error!("Cannot handle order fill: {e}, {fill}");
                    return Vec::new();
                }
            };

            account.is_margin_account()
        };

        // Skip portfolio position updates for combo fills (spread instruments)
        // Combo fills are only used for order management, not portfolio updates
        if !instrument.is_spread() && is_margin_account {
            let portfolio_endpoint = MessagingSwitchboard::portfolio_update_order();
            msgbus::send_order_event(portfolio_endpoint, OrderEventAny::Filled(fill.clone()));
        }

        let (position, position_events) = if instrument.is_spread() {
            (None, Vec::new())
        } else {
            let position_events = self.handle_position_update(&instrument, fill.clone(), oms_type);
            let position_id = fill.position_id.unwrap();
            (
                self.cache.borrow().position_owned(&position_id),
                position_events,
            )
        };

        // Handle contingent orders for both spread and non-spread instruments
        // For spread instruments, contingent orders work without position linkage
        if matches!(order.contingency_type(), Some(ContingencyType::Oto)) {
            // For non-spread instruments, link to position if available
            if !instrument.is_spread()
                && let Some(ref pos) = position
                && pos.is_open()
            {
                let position_id = pos.id;

                for client_order_id in order.linked_order_ids().unwrap_or_default() {
                    // Take a scoped write borrow on the contingent's cell. The borrow drops at
                    // the end of `and_then` so the subsequent `add_position_id` on the cache is
                    // free to take `&mut Cache`.
                    let link = self.cache.borrow_mut().order_mut(client_order_id).and_then(
                        |mut contingent_order| {
                            if contingent_order.position_id().is_none() {
                                contingent_order.set_position_id(Some(position_id));
                                Some((
                                    contingent_order.instrument_id().venue,
                                    contingent_order.client_order_id(),
                                    contingent_order.strategy_id(),
                                ))
                            } else {
                                None
                            }
                        },
                    );

                    if let Some((venue, contingent_id, strategy_id)) = link
                        && let Err(e) = self.cache.borrow_mut().add_position_id(
                            &position_id,
                            &venue,
                            &contingent_id,
                            &strategy_id,
                        )
                    {
                        log::error!("Failed to add position ID: {e}");
                    }
                }
            }
            // For spread instruments, contingent orders can still be triggered
            // but without position linkage (since no position is created for spreads)
        }

        let topic = switchboard::get_order_filled_topic(fill.instrument_id);
        let event = OrderEventAny::Filled(fill);
        msgbus::publish_order_event(topic, &event);

        position_events
    }

    fn prepare_order_fill_void_positions(
        &self,
        order: &OrderAny,
        event: &OrderFillVoided,
    ) -> anyhow::Result<Vec<CorrectedPosition>> {
        let source_event_id = order
            .events()
            .into_iter()
            .find_map(|order_event| match order_event {
                OrderEventAny::Filled(fill) if fill.trade_id == event.trade_id => {
                    Some(fill.event_id)
                }
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("fill {} is not in order history", event.trade_id))?;

        let positions: Vec<Position> = {
            let cache = self.cache.borrow();
            cache
                .positions(
                    None,
                    Some(&event.instrument_id),
                    Some(&event.strategy_id),
                    Some(&event.account_id),
                    None,
                )
                .into_iter()
                .map(|position| position.cloned())
                .collect()
        };
        let mut fragments = Vec::new();

        for position in &positions {
            for replay_event in &position.replay_events {
                let PositionReplayEvent::Filled(fill) = replay_event else {
                    continue;
                };

                if fill.client_order_id != event.client_order_id || fill.trade_id != event.trade_id
                {
                    continue;
                }
                let split_rank = if fill.event_id == source_event_id {
                    0
                } else if fill.causation_id == Some(source_event_id) {
                    1
                } else {
                    continue;
                };
                fragments.push((position.id, split_rank, fill.last_qty, fill.commission));
            }
        }
        anyhow::ensure!(
            !fragments.is_empty(),
            "no position fragments found for fill {}",
            event.trade_id
        );
        fragments.sort_by_key(|(_, split_rank, _, _)| *split_rank);

        let mut allocations = IndexMap::<PositionId, (Quantity, Option<Money>)>::new();
        let mut remaining_qty = event.voided_qty;
        for (position_id, _, quantity, _) in fragments.iter().rev() {
            if remaining_qty.is_zero() {
                break;
            }
            let removed = remaining_qty.min(*quantity);
            allocations
                .entry(*position_id)
                .and_modify(|allocation| allocation.0 = allocation.0 + removed)
                .or_insert((removed, None));
            remaining_qty = remaining_qty - removed;
        }
        anyhow::ensure!(
            remaining_qty.is_zero(),
            "position fragments do not cover voided quantity for fill {}",
            event.trade_id
        );

        if let Some(mut remaining_commission) = event.commission_voided {
            for (position_id, _, _, commission) in fragments.iter().rev() {
                if remaining_commission.is_zero() {
                    break;
                }
                let Some(commission) = commission else {
                    continue;
                };
                anyhow::ensure!(
                    commission.currency == remaining_commission.currency,
                    "position commission currency differs for fill {}",
                    event.trade_id
                );
                let removed_raw = remaining_commission.raw.abs().min(commission.raw.abs());
                let removed = Money::from_raw(
                    removed_raw * remaining_commission.raw.signum(),
                    remaining_commission.currency,
                );
                allocations
                    .entry(*position_id)
                    .and_modify(|allocation| {
                        allocation.1 = Some(
                            allocation
                                .1
                                .map_or(removed, |commission| commission + removed),
                        );
                    })
                    .or_insert((Quantity::zero(event.voided_qty.precision), Some(removed)));
                remaining_commission = remaining_commission - removed;
            }
            anyhow::ensure!(
                remaining_commission.is_zero(),
                "position fragments do not cover voided commission for fill {}",
                event.trade_id
            );
        }

        let mut corrected_positions = Vec::new();

        for (position_id, (voided_qty, commission_voided)) in allocations {
            if voided_qty.is_zero() {
                anyhow::bail!(
                    "commission-only position correction requires authoritative reconciliation for fill {}",
                    event.trade_id
                );
            }
            let mut position = self
                .cache
                .borrow()
                .position_owned(&position_id)
                .ok_or_else(|| anyhow::anyhow!("position {position_id} is not cached"))?;
            let previous = position
                .fill_voids
                .iter()
                .rev()
                .find(|record| {
                    record.event.client_order_id == event.client_order_id
                        && record.event.trade_id == event.trade_id
                })
                .map(|record| (record.voided_qty, record.commission_voided));
            if previous == Some((voided_qty, commission_voided)) {
                continue;
            }
            let corrected_qty = previous.map_or(voided_qty, |(prior_qty, _)| {
                voided_qty.saturating_sub(prior_qty)
            });

            // `events` holds the fills since the position was last flat, because `apply_fill`
            // clears it when reopening from flat. A NETTING flip splits one fill across the
            // closing and reopening cycles under the same trade, so compare quantities rather
            // than presence: the correction reaches an earlier cycle once it exceeds what the
            // current cycle originally held. Earlier corrections have already shrunk the
            // fragments in `events` while `voided_qty` stays cumulative, so add back what this
            // position already voided. Read this before `apply_fill_void`, whose rebuild
            // re-derives `events` and can move that boundary.
            let previously_voided = previous
                .map_or(Quantity::zero(position.size_precision), |(prior_qty, _)| {
                    prior_qty
                });
            let current_cycle_qty = position
                .events
                .iter()
                .filter(|fill| {
                    fill.client_order_id == event.client_order_id && fill.trade_id == event.trade_id
                })
                .fold(previously_voided, |total, fill| total + fill.last_qty);
            let absorbed_prior_cycles = voided_qty > current_cycle_qty;
            let closed_cycles_pnl =
                position.apply_fill_void(event.clone(), voided_qty, commission_voided)?;
            corrected_positions.push(CorrectedPosition {
                position,
                corrected_qty,
                absorbed_prior_cycles,
                closed_cycles_pnl,
            });
        }
        Ok(corrected_positions)
    }

    fn create_fill_void_position_event(
        position: &Position,
        fill_voided: &OrderFillVoided,
        corrected_qty: Quantity,
    ) -> PositionEvent {
        let event_id = UUID4::new();
        let ts_init = fill_voided.ts_init;

        if position.is_closed() {
            PositionEvent::PositionClosed(PositionClosed {
                trader_id: position.trader_id,
                strategy_id: position.strategy_id,
                instrument_id: position.instrument_id,
                position_id: position.id,
                account_id: position.account_id,
                opening_order_id: position.opening_order_id,
                closing_order_id: position.closing_order_id,
                entry: position.entry,
                side: position.side,
                signed_qty: position.signed_qty,
                quantity: position.quantity,
                peak_quantity: position.peak_qty,
                last_qty: corrected_qty,
                last_px: fill_voided.last_px,
                currency: position.quote_currency,
                avg_px_open: position.avg_px_open,
                avg_px_close: position.avg_px_close,
                realized_return: position.realized_return,
                realized_pnl: position.realized_pnl,
                unrealized_pnl: Money::zero(position.quote_currency),
                duration: position.duration_ns,
                event_id,
                ts_opened: position.ts_opened,
                ts_closed: position.ts_closed,
                ts_event: fill_voided.ts_event,
                ts_init,
            })
        } else {
            PositionEvent::PositionChanged(PositionChanged {
                trader_id: position.trader_id,
                strategy_id: position.strategy_id,
                instrument_id: position.instrument_id,
                position_id: position.id,
                account_id: position.account_id,
                opening_order_id: position.opening_order_id,
                entry: position.entry,
                side: position.side,
                signed_qty: position.signed_qty,
                quantity: position.quantity,
                peak_quantity: position.peak_qty,
                last_qty: corrected_qty,
                last_px: fill_voided.last_px,
                currency: position.quote_currency,
                avg_px_open: position.avg_px_open,
                avg_px_close: position.avg_px_close,
                realized_return: position.realized_return,
                realized_pnl: position.realized_pnl,
                unrealized_pnl: Money::zero(position.quote_currency),
                event_id,
                ts_opened: position.ts_opened,
                ts_event: fill_voided.ts_event,
                ts_init,
            })
        }
    }

    /// Handle position creation or update for a fill.
    ///
    /// This function mirrors the Python `_handle_position_update` method.
    fn handle_position_update(
        &mut self,
        instrument: &InstrumentAny,
        fill: OrderFilled,
        oms_type: OmsType,
    ) -> Vec<PositionEvent> {
        let position_id = if let Some(position_id) = fill.position_id {
            position_id
        } else {
            log::error!("Cannot handle position update: no position ID found for fill {fill}");
            return Vec::new();
        };

        let position_opt = self.cache.borrow().position_owned(&position_id);

        match position_opt {
            None => {
                if self.reject_reduce_only_position_open(&fill, oms_type) {
                    return Vec::new();
                }

                self.open_position(instrument, None, fill, oms_type)
                    .unwrap_or_default()
            }
            Some(pos) if pos.is_closed() => {
                if self.reject_reduce_only_position_open(&fill, oms_type) {
                    return Vec::new();
                }

                self.open_position(instrument, Some(&pos), fill, oms_type)
                    .unwrap_or_default()
            }
            Some(mut pos) => {
                if self.will_flip_position(&pos, &fill) {
                    self.flip_position(instrument, &mut pos, &fill, oms_type)
                } else {
                    self.update_position(&mut pos, &fill).into_iter().collect()
                }
            }
        }
    }

    fn reject_reduce_only_position_open(&self, fill: &OrderFilled, oms_type: OmsType) -> bool {
        let cache = self.cache.borrow();
        let Some(order) = cache.order_owned(&fill.client_order_id) else {
            return false;
        };

        if !order.is_reduce_only() {
            return false;
        }

        let positions_open = cache.positions_open(
            None,
            Some(&fill.instrument_id),
            None,
            Some(&fill.account_id),
            None,
        );
        let position_id = fill
            .position_id
            .map_or_else(|| "None".to_string(), |position_id| position_id.to_string());
        let matching_position_details = Self::position_details(
            positions_open
                .iter()
                .filter(|position| position.is_opposite_side(fill.order_side))
                .map(|position| &**position),
        );
        let open_position_details =
            Self::position_details(positions_open.iter().map(|position| &**position));

        log::error!(
            "Cannot open {oms_type} position {position_id} from reduce-only fill {} for {}; \
             matching_reduce_positions=[{}], open_positions=[{}]",
            fill.trade_id,
            fill.instrument_id,
            matching_position_details,
            open_position_details
        );

        true
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "takes the opening fill by value to seed the new position"
    )]
    fn open_position(
        &self,
        instrument: &InstrumentAny,
        position: Option<&Position>,
        fill: OrderFilled,
        oms_type: OmsType,
    ) -> anyhow::Result<Vec<PositionEvent>> {
        if let Some(position) = position {
            if Self::is_duplicate_closed_fill(position, &fill) {
                log::warn!(
                    "Ignoring duplicate fill {} for closed position {}; no position reopened (side={:?}, qty={}, px={})",
                    fill.trade_id,
                    position.id,
                    fill.order_side,
                    fill.last_qty,
                    fill.last_px
                );
                return Ok(Vec::new());
            }
            self.reopen_position(position, oms_type)?;
        }

        // The prior-position clone exists only to carry replay state across the reopen
        let prior_position = if self.config.carry_replay_events_on_reopen {
            position.cloned().or_else(|| {
                fill.position_id
                    .and_then(|position_id| self.cache.borrow().position_owned(&position_id))
            })
        } else {
            None
        };
        let mut position = Position::new(instrument, fill.clone());
        if let Some(prior) = prior_position
            && prior.id == position.id
        {
            let current_replay = position.replay_events.clone();
            position.replay_events = prior.replay_events;
            position.replay_events.extend(current_replay);
            position.fill_voids = prior.fill_voids;
        }
        self.cache.borrow_mut().add_position(&position, oms_type)?;

        if self.config.snapshot_positions {
            self.create_position_state_snapshot(&position, true);
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let event = PositionOpened::create(&position, &fill, UUID4::new(), ts_init);

        Ok(vec![PositionEvent::PositionOpened(event)])
    }

    fn is_duplicate_closed_fill(position: &Position, fill: &OrderFilled) -> bool {
        position.replay_events.iter().any(|event| {
            matches!(
                event,
                PositionReplayEvent::Filled(replayed) if replayed.trade_id == fill.trade_id
            )
        })
    }

    fn reopen_position(&self, position: &Position, oms_type: OmsType) -> anyhow::Result<()> {
        if oms_type == OmsType::Netting {
            if position.is_open() {
                anyhow::bail!(
                    "Cannot reopen position {} (oms_type=NETTING): reopening is only valid for closed positions in NETTING mode",
                    position.id
                );
            }
            // Snapshot closed position if reopening (NETTING mode)
            self.snapshot_position(position)?;
        } else {
            // HEDGING mode
            log::warn!(
                "Received fill for closed position {} in HEDGING mode; creating new position and ignoring previous state",
                position.id
            );
        }
        Ok(())
    }

    /// Archives the closed `position` and anchors the frame when an anchorer is installed.
    ///
    /// An installed anchorer needs the encoded frame, so this takes the eager path. Without one
    /// the cache defers the encode unless a backing database has to persist the frame.
    fn snapshot_position(&self, position: &Position) -> anyhow::Result<()> {
        let mut cache = self.cache.borrow_mut();

        let Some(anchorer) = &self.snapshot_anchorer else {
            return cache.snapshot_position(position);
        };

        let snapshot_ref = cache.snapshot_position_encoded(position)?;
        drop(cache);

        if let Err(e) = anchorer(snapshot_ref) {
            log::warn!("Failed to record cache snapshot anchor: {e}");
        }

        Ok(())
    }

    fn update_position(
        &self,
        position: &mut Position,
        fill: &OrderFilled,
    ) -> Option<PositionEvent> {
        // Apply the fill to the position
        position.apply(fill);

        // Check if position is closed after applying the fill
        let is_closed = position.is_closed();

        // Update position in cache - this should handle the closed state tracking
        if let Err(e) = self.cache.borrow_mut().update_position(position) {
            log::error!("Failed to update position: {e:?}");
            return None;
        }

        // Verify cache state after update
        let cache = self.cache.borrow();

        drop(cache);

        // Create position state snapshot if enabled
        if self.config.snapshot_positions {
            self.create_position_state_snapshot(position, false);
        }

        let ts_init = self.clock.borrow().timestamp_ns();

        if is_closed {
            let event = PositionClosed::create(position, fill, UUID4::new(), ts_init);
            Some(PositionEvent::PositionClosed(event))
        } else {
            let event = PositionChanged::create(position, fill, UUID4::new(), ts_init);
            Some(PositionEvent::PositionChanged(event))
        }
    }

    fn will_flip_position(&self, position: &Position, fill: &OrderFilled) -> bool {
        position.is_opposite_side(fill.order_side) && (fill.last_qty.raw > position.quantity.raw)
    }

    fn position_signed_decimal_qty(position: &Position) -> Decimal {
        match position.side {
            PositionSide::Long => position.quantity.as_decimal(),
            PositionSide::Short => -position.quantity.as_decimal(),
            _ => Decimal::ZERO,
        }
    }

    fn position_details<'a>(positions: impl IntoIterator<Item = &'a Position>) -> String {
        positions
            .into_iter()
            .map(|position| {
                format!(
                    "{} strategy_id={} signed_qty={}",
                    position.id,
                    position.strategy_id,
                    Self::position_signed_decimal_qty(position)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn flip_position(
        &mut self,
        instrument: &InstrumentAny,
        position: &mut Position,
        fill: &OrderFilled,
        oms_type: OmsType,
    ) -> Vec<PositionEvent> {
        let mut position_events = Vec::new();
        let difference = match position.side {
            PositionSide::Long => Quantity::from_raw(
                fill.last_qty.raw - position.quantity.raw,
                position.size_precision,
            ),
            PositionSide::Short => Quantity::from_raw(
                position.quantity.raw.abs_diff(fill.last_qty.raw), // Equivalent to Python's abs(position.quantity - fill.last_qty)
                position.size_precision,
            ),
            _ => fill.last_qty,
        };

        // Split commission between two positions
        let fill_percent = position.quantity.as_decimal() / fill.last_qty.as_decimal();
        let (commission1, commission2) = if let Some(commission) = fill.commission {
            let commission_currency = commission.currency;
            let commission1 =
                Money::from_decimal(commission.as_decimal() * fill_percent, commission_currency)
                    .expect("Invalid split commission");
            let commission2 = commission - commission1;
            (Some(commission1), Some(commission2))
        } else {
            log::warn!(
                "Commission is not available for position flip, splitting with no commission"
            );
            (None, None)
        };

        let mut fill_split1: Option<OrderFilled> = None;

        if position.is_open() {
            let mut split = OrderFilled::new(
                fill.trader_id,
                fill.strategy_id,
                fill.instrument_id,
                fill.client_order_id,
                fill.venue_order_id,
                fill.account_id,
                fill.trade_id,
                fill.order_side,
                fill.order_type,
                position.quantity,
                fill.last_px,
                fill.currency,
                fill.liquidity_side,
                fill.event_id,
                fill.ts_event,
                fill.ts_init,
                fill.reconciliation,
                fill.position_id,
                commission1,
                fill.info.clone(),
            );
            split.causation_id = fill.causation_id;
            fill_split1 = Some(split);

            if let Some(position_event) =
                self.update_position(position, fill_split1.as_ref().unwrap())
            {
                position_events.push(position_event);
            }

            // Snapshot closed position before reusing ID (NETTING mode)
            if oms_type == OmsType::Netting
                && let Err(e) = self.snapshot_position(position)
            {
                log::warn!("Failed to snapshot position during flip: {e:?}");
            }
        }

        // Guard against flipping a position with a zero fill size
        if difference.raw == 0 {
            log::warn!(
                "Zero fill size during position flip calculation, this could be caused by a mismatch between instrument `size_precision` and a quantity `size_precision`"
            );
            return position_events;
        }

        let position_id_flip = if oms_type == OmsType::Hedging
            && let Some(position_id) = fill.position_id
            && position_id.is_virtual()
        {
            // Generate new position ID for flipped virtual position (Hedging OMS only)
            Some(self.pos_id_generator.generate(fill.strategy_id, true))
        } else {
            // Default: use the same position ID as the fill (Python behavior)
            fill.position_id
        };

        let mut fill_split2 = OrderFilled::new(
            fill.trader_id,
            fill.strategy_id,
            fill.instrument_id,
            fill.client_order_id,
            fill.venue_order_id,
            fill.account_id,
            fill.trade_id,
            fill.order_side,
            fill.order_type,
            difference,
            fill.last_px,
            fill.currency,
            fill.liquidity_side,
            UUID4::new(),
            fill.ts_event,
            fill.ts_init,
            fill.reconciliation,
            position_id_flip,
            commission2,
            fill.info.clone(),
        );
        fill_split2.causation_id = Some(fill.event_id);

        if oms_type == OmsType::Hedging
            && let Some(position_id) = fill.position_id
            && position_id.is_virtual()
        {
            log::warn!("Closing position {fill_split1:?}");
            log::warn!("Flipping position {fill_split2:?}");
        }

        // Open flipped position
        match self.open_position(instrument, None, fill_split2, oms_type) {
            Ok(opened_events) => position_events.extend(opened_events),
            Err(e) => log::error!("Failed to open flipped position: {e:?}"),
        }

        position_events
    }

    /// Sets the internal position ID generator counts based on existing cached positions.
    pub fn set_position_id_counts(&mut self) {
        let cache = self.cache.borrow();
        let positions = cache.positions(None, None, None, None, None);

        // Count positions per instrument_id using a HashMap
        let mut counts: HashMap<StrategyId, usize> = HashMap::new();

        for position in positions {
            *counts.entry(position.strategy_id).or_insert(0) += 1;
        }

        self.pos_id_generator.reset();

        for (strategy_id, count) in counts {
            self.pos_id_generator.set_count(count, strategy_id);
            log::info!("Set PositionId count for {strategy_id} to {count}");
        }
    }

    fn deny_order(&self, order: &OrderAny, reason: &str) {
        let denied = OrderDenied::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            reason.into(),
            UUID4::new(),
            self.clock.borrow().timestamp_ns(),
            self.clock.borrow().timestamp_ns(),
        );

        let event = OrderEventAny::Denied(denied);
        let order = match self.cache.borrow_mut().update_order(&event) {
            Ok(order) => order,
            Err(e) => {
                log::error!("Failed to apply denied event to order: {e}");
                return;
            }
        };

        let topic = switchboard::get_event_order_topic(order.strategy_id());
        msgbus::publish_order_event(topic, &event);

        if self.config.snapshot_orders {
            self.create_order_state_snapshot(&order);
        }
    }

    fn get_or_init_own_order_book(&self, instrument_id: &InstrumentId) -> RefMut<'_, OwnOrderBook> {
        let mut cache = self.cache.borrow_mut();
        if cache.own_order_book_mut(instrument_id).is_none() {
            let own_book = OwnOrderBook::new(*instrument_id);
            cache.add_own_order_book(own_book).unwrap();
        }

        RefMut::map(cache, |c| c.own_order_book_mut(instrument_id).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::clock::TestClock;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType, PositionSideSpecified},
        events::order::spec::OrderFilledSpec,
        identifiers::{AccountId, ClientOrderId, TradeId, VenueOrderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
        orders::builder::OrderTestBuilder,
        types::Price,
    };
    use rstest::*;

    use super::*;

    #[rstest]
    fn netting_positions_open_for_report_scopes_positions_by_account() {
        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let account1_id = AccountId::from("SIM-001");
        let account2_id = AccountId::from("SIM-002");
        let position1 = position_for_account(
            &instrument,
            account1_id,
            StrategyId::from("S-001"),
            PositionId::from("P-ACC-1"),
            OrderSide::Buy,
            Quantity::from(1_000),
        );
        let position2 = position_for_account(
            &instrument,
            account2_id,
            StrategyId::from("S-002"),
            PositionId::from("P-ACC-2"),
            OrderSide::Buy,
            Quantity::from(2_000),
        );
        let mut cache = Cache::default();
        cache.add_position(&position1, OmsType::Netting).unwrap();
        cache.add_position(&position2, OmsType::Netting).unwrap();

        let report = PositionStatusReport::new(
            account1_id,
            instrument.id(),
            PositionSideSpecified::Long,
            Quantity::from(1_000),
            UnixNanos::from(1_000_000),
            UnixNanos::from(1_000_000),
            None,
            None,
            None,
        );

        let positions_open = ExecutionEngine::netting_positions_open_for_report(&cache, &report);
        let signed_qty: Decimal = positions_open
            .iter()
            .map(|position| ExecutionEngine::position_signed_decimal_qty(position))
            .sum();

        assert_eq!(positions_open.len(), 1);
        assert_eq!(positions_open[0].id, position1.id);
        assert_eq!(signed_qty, Decimal::from(1_000));
    }

    #[rstest]
    fn netting_split_position_ownership_message_reports_only_split_ownership() {
        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let account_id = AccountId::from("SIM-001");
        let external_position = position_for_account(
            &instrument,
            account_id,
            StrategyId::from("EXTERNAL"),
            PositionId::from("P-EXTERNAL"),
            OrderSide::Buy,
            Quantity::from(1_000),
        );
        let strategy_position = position_for_account(
            &instrument,
            account_id,
            StrategyId::from("S-001"),
            PositionId::from("P-STRATEGY"),
            OrderSide::Buy,
            Quantity::from(500),
        );
        let same_strategy_position = position_for_account(
            &instrument,
            account_id,
            StrategyId::from("EXTERNAL"),
            PositionId::from("P-EXTERNAL-2"),
            OrderSide::Buy,
            Quantity::from(250),
        );
        let report = PositionStatusReport::new(
            account_id,
            instrument.id(),
            PositionSideSpecified::Long,
            Quantity::from(1_500),
            UnixNanos::from(1_000_000),
            UnixNanos::from(1_000_000),
            None,
            None,
            None,
        );

        let message = ExecutionEngine::netting_split_position_ownership_message(
            &report,
            &[&external_position, &strategy_position],
        )
        .expect("split ownership should produce a warning message");

        assert!(message.contains("account_id=SIM-001"));
        assert!(message.contains(&format!("instrument_id={}", instrument.id())));
        assert!(message.contains("EXTERNAL"));
        assert!(message.contains("S-001"));
        assert!(message.contains("P-EXTERNAL"));
        assert!(message.contains("P-STRATEGY"));
        assert!(message.contains("signed_qty=1000"));
        assert!(message.contains("signed_qty=500"));
        assert!(
            ExecutionEngine::netting_split_position_ownership_message(
                &report,
                &[&external_position, &same_strategy_position],
            )
            .is_none()
        );
    }

    #[rstest]
    fn materialize_external_order_rejects_venue_id_owned_by_another_order() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let venue_order_id = VenueOrderId::from("V-SHARED");
        let owner_id = ClientOrderId::from("O-OWNER");
        cache
            .borrow_mut()
            .add_venue_order_id(&owner_id, &venue_order_id, false)
            .unwrap();
        let engine = ExecutionEngine::new(
            Rc::new(RefCell::new(TestClock::new())),
            Rc::clone(&cache),
            None,
        );
        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let claimant_id = ClientOrderId::from("O-CLAIMANT");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .client_order_id(claimant_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100_000))
            .price(Price::from("1.00000"))
            .build();
        let OrderEventAny::Initialized(initialized) = order.last_event().clone() else {
            panic!("Expected initialized order");
        };

        let prepared = engine
            .prepare_external_order(
                initialized,
                venue_order_id,
                instrument.id(),
                order.strategy_id(),
                UnixNanos::default(),
                None,
            )
            .unwrap();
        let result = engine.commit_external_order(prepared, ClientId::from("SIM"));

        assert!(result.is_none());
        assert!(!cache.borrow().order_exists(&claimant_id));
        assert_eq!(
            cache.borrow().client_order_id(&venue_order_id),
            Some(&owner_id)
        );
        assert_eq!(cache.borrow().venue_order_id(&claimant_id), None);
    }

    fn position_for_account(
        instrument: &InstrumentAny,
        account_id: AccountId,
        strategy_id: StrategyId,
        position_id: PositionId,
        order_side: OrderSide,
        quantity: Quantity,
    ) -> Position {
        let client_order_id = ClientOrderId::from(format!("O-{position_id}"));
        let fill = OrderFilledSpec::builder()
            .strategy_id(strategy_id)
            .instrument_id(instrument.id())
            .client_order_id(client_order_id)
            .venue_order_id(VenueOrderId::from(format!("V-{position_id}")))
            .account_id(account_id)
            .trade_id(TradeId::new(format!("T-{position_id}")))
            .order_side(order_side)
            .last_qty(quantity)
            .last_px(Price::from("1.0"))
            .currency(instrument.quote_currency())
            .liquidity_side(LiquiditySide::Maker)
            .position_id(position_id)
            .commission(Money::from("2 USD"))
            .build();

        Position::new(instrument, fill)
    }
}
