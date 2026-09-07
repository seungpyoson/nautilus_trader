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

//! Recovery through the actual LiveNode run channels while execution connects.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    live::{get_data_event_sender, get_exec_event_sender},
    logging::logger::LoggerConfig,
    messages::{
        DataEvent, ExecutionEvent,
        book::{BookFeed, BookFeedAction, BookFeedBudget, L2BookUpdate},
    },
};
use nautilus_core::{Params, UnixNanos};
use nautilus_live::node::{LiveNode, NodeRunMode};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AssetClass, OmsType, OrderStatus, OrderType},
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, InstrumentId, Symbol, TraderId, Venue},
    instruments::{BinaryOption, Instrument, InstrumentAny},
    orders::{Order, OrderTestBuilder, stubs::TestOrderEventStubs},
    types::{AccountBalance, Currency, MarginBalance, Quantity},
};
use rstest::rstest;
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug)]
enum RecoveryPhase {
    None,
    Connecting,
    Readiness,
}

#[derive(Clone, Copy, Debug)]
enum StartupEntry {
    Start,
    Run,
}

#[derive(Clone, Copy, Debug)]
enum IngressEnd {
    Stop,
    Deadline,
}

#[derive(Debug)]
struct ReplenishingIngress {
    active: AtomicBool,
    started: AtomicBool,
    callbacks: AtomicU64,
    end: IngressEnd,
}

fn id() -> InstrumentId {
    InstrumentId::from("FIXTURE.FIXTURE")
}

fn instrument(ts_event: u64) -> InstrumentAny {
    BinaryOption::builder()
        .instrument_id(id())
        .raw_symbol(Symbol::from("FIXTURE"))
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USD())
        .activation_ns(0.into())
        .expiration_ns(1_000_000_000_000_000_000_u64.into())
        .price_precision(2)
        .size_precision(2)
        .price_increment("0.01".into())
        .size_increment("0.01".into())
        .ts_event(ts_event.into())
        .ts_init(0.into())
        .build()
        .unwrap()
        .into()
}

fn snapshot(
    feed: &Arc<BookFeed>,
    quantity: i64,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
) {
    sender
        .send(DataEvent::BookFeed(
            feed.event(
                BookFeedAction::Update {
                    instrument_id: id(),
                    update: L2BookUpdate::Snapshot {
                        bids: vec![(Decimal::new(40, 2), Decimal::new(quantity, 0))],
                        asks: vec![(Decimal::new(60, 2), Decimal::new(10, 0))],
                    },
                    sequence: 1,
                    ts_event: 0.into(),
                },
                0.into(),
            )
            .unwrap(),
        ))
        .unwrap();
}

#[derive(Debug, Clone)]
struct Shared {
    ready: Arc<AtomicBool>,
    transport: Rc<Cell<bool>>,
    feed: Rc<RefCell<Option<Arc<BookFeed>>>>,
    budget: Arc<BookFeedBudget>,
    phase: RecoveryPhase,
    instrument_refreshes: u64,
    instrument_updates: Rc<RefCell<Vec<(UnixNanos, bool)>>>,
    initial_ready_observed: Rc<Cell<bool>>,
    replacement_queued: Arc<AtomicBool>,
    engine_ready_at_disconnect: Rc<Cell<bool>>,
    ingress: Option<Arc<ReplenishingIngress>>,
    startup_order: Option<OrderEventAny>,
    initial_instruments: u64,
    exec_connect_delay: Option<Duration>,
}

impl Shared {
    fn new(phase: RecoveryPhase, instrument_refreshes: u64) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            transport: Rc::new(Cell::new(false)),
            feed: Rc::new(RefCell::new(None)),
            budget: BookFeedBudget::new(32.try_into().unwrap()),
            phase,
            instrument_refreshes,
            instrument_updates: Rc::new(RefCell::new(Vec::new())),
            initial_ready_observed: Rc::new(Cell::new(false)),
            replacement_queued: Arc::new(AtomicBool::new(false)),
            engine_ready_at_disconnect: Rc::new(Cell::new(false)),
            ingress: None,
            startup_order: None,
            initial_instruments: 1,
            exec_connect_delay: Some(Duration::from_millis(50)),
        }
    }

    fn create_feed(&self) -> Arc<BookFeed> {
        let (feed, _failure) =
            BookFeed::new(vec![id()], self.budget.clone(), self.ready.clone()).unwrap();
        *self.feed.borrow_mut() = Some(feed.clone());
        feed
    }

    fn close_feed(&self) {
        if let Some(feed) = self.feed.borrow_mut().take() {
            feed.lock().invalidate();
            get_data_event_sender()
                .send(DataEvent::BookFeed(
                    feed.event(BookFeedAction::Close, 0.into()).unwrap(),
                ))
                .unwrap();
        }
    }
}

#[derive(Debug)]
struct Config;
impl ClientConfig for Config {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
struct DataFactory(Shared);
#[derive(Debug)]
struct ExecFactory(Shared);
impl DataClientFactory for DataFactory {
    fn create(
        &self,
        _: &str,
        _: &dyn ClientConfig,
        _: CacheView,
        _: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        Ok(Box::new(FixtureDataClient(self.0.clone())))
    }

    fn name(&self) -> &'static str {
        "FIXTURE-DATA"
    }

    fn config_type(&self) -> &'static str {
        "Config"
    }
}

impl ExecutionClientFactory for ExecFactory {
    fn create(
        &self,
        _: TraderId,
        _: &str,
        _: &dyn ClientConfig,
        _: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        Ok(Box::new(FixtureExecutionClient {
            shared: self.0.clone(),
            connected: false,
        }))
    }

    fn name(&self) -> &'static str {
        "FIXTURE-EXEC"
    }

    fn config_type(&self) -> &'static str {
        "Config"
    }
}

struct FixtureDataClient(Shared);
#[async_trait(?Send)]
impl DataClient for FixtureDataClient {
    fn client_id(&self) -> ClientId {
        "FIXTURE-DATA".into()
    }

    fn venue(&self) -> Option<Venue> {
        Some("FIXTURE".into())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        if self.0.ingress.as_ref().is_some_and(|ingress| {
            matches!(ingress.end, IngressEnd::Deadline)
                && self.0.replacement_queued.load(Ordering::Acquire)
        }) {
            return false;
        }
        self.0.transport.get() && self.0.ready.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.0.transport.get()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        for timestamp in 0..self.0.initial_instruments {
            get_data_event_sender()
                .send(DataEvent::Instrument(instrument(timestamp)))
                .unwrap();
        }
        snapshot(&self.0.create_feed(), 10, &get_data_event_sender());
        self.0.transport.set(true);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.0
            .engine_ready_at_disconnect
            .set(self.0.ready.load(Ordering::Acquire));
        self.0.close_feed();
        self.0.transport.set(false);
        Ok(())
    }
}

struct FixtureExecutionClient {
    shared: Shared,
    connected: bool,
}
#[async_trait(?Send)]
impl ExecutionClient for FixtureExecutionClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> ClientId {
        "FIXTURE-EXEC".into()
    }

    fn account_id(&self) -> AccountId {
        "FIXTURE-001".into()
    }

    fn venue(&self) -> Venue {
        "FIXTURE".into()
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Hedging
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        _: Vec<AccountBalance>,
        _: Vec<MarginBalance>,
        _: bool,
        _: UnixNanos,
        _: Option<Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let ready = self.shared.ready.load(Ordering::Acquire);

        assert!(
            ready,
            "Initial snapshot must have been applied before execution connects"
        );
        self.shared.initial_ready_observed.set(ready);

        if let Some(event) = &self.shared.startup_order {
            get_exec_event_sender()
                .send(ExecutionEvent::Order(event.clone()))
                .unwrap();
        }

        if !matches!(self.shared.phase, RecoveryPhase::None) {
            self.shared.close_feed();
            let feed = self.shared.create_feed();
            let queued = Arc::clone(&self.shared.replacement_queued);
            let sender = get_data_event_sender();
            let instrument_refreshes = self.shared.instrument_refreshes;
            let delay = match self.shared.phase {
                RecoveryPhase::Connecting => 10,
                RecoveryPhase::Readiness => 100,
                RecoveryPhase::None => unreachable!(),
            };

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;

                for timestamp in 1..instrument_refreshes {
                    sender
                        .send(DataEvent::Instrument(instrument(timestamp)))
                        .unwrap();
                }
                snapshot(&feed, 20, &sender);

                if instrument_refreshes > 0 {
                    sender
                        .send(DataEvent::Instrument(instrument(instrument_refreshes)))
                        .unwrap();
                }
                queued.store(true, Ordering::Release);
            });
        }

        if let Some(delay) = self.shared.exec_connect_delay {
            tokio::time::sleep(delay).await;
        }
        self.connected = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    fn on_instrument(&mut self, update: InstrumentAny) {
        if let Some(ingress) = &self.shared.ingress
            && self.connected
        {
            ingress.started.store(true, Ordering::Release);
            let count = ingress.callbacks.fetch_add(1, Ordering::Relaxed);

            if ingress.active.load(Ordering::Acquire) {
                get_data_event_sender()
                    .send(DataEvent::Instrument(instrument(count + 3)))
                    .unwrap();
            }
            return;
        }
        self.shared
            .instrument_updates
            .borrow_mut()
            .push((update.ts_event(), self.connected));
    }
}

fn build_node(shared: &Shared, timeout_seconds: u64) -> LiveNode {
    LiveNode::builder(TraderId::from("FIXTURE-001"), Environment::Sandbox)
        .unwrap()
        .with_name("STARTUP-BOOK-FEED")
        .with_reconciliation(false)
        .with_timeout_connection(timeout_seconds)
        .with_timeout_disconnection_secs(1)
        .with_delay_post_stop_secs(0)
        .with_logging(LoggerConfig {
            bypass_logging: true,
            ..Default::default()
        })
        .add_data_client(
            None,
            Box::new(DataFactory(shared.clone())),
            Box::new(Config),
        )
        .unwrap()
        .add_exec_client(
            None,
            Box::new(ExecFactory(shared.clone())),
            Box::new(Config),
        )
        .unwrap()
        .build()
        .unwrap()
}

#[rstest]
#[case::control(RecoveryPhase::None, 0)]
#[case::during_execution_connect(RecoveryPhase::Connecting, 0)]
#[case::during_readiness_wait(RecoveryPhase::Readiness, 0)]
#[case::instruments_during_execution_connect(RecoveryPhase::Connecting, 2)]
#[case::instruments_during_readiness_wait(RecoveryPhase::Readiness, 2)]
#[case::instrument_backlog_spans_batches(RecoveryPhase::Connecting, 4_097)]
#[tokio::test]
async fn startup_applies_replacement_book_before_readiness(
    #[case] phase: RecoveryPhase,
    #[case] instrument_refreshes: u64,
) {
    let shared = Shared::new(phase, instrument_refreshes);
    let mut node = build_node(&shared, 1);
    let observer = node.handle();
    let cache = node.kernel().cache();
    let mut observed_bid = None;
    let result = {
        let run = node.run_with_mode(NodeRunMode::Hosted);
        tokio::pin!(run);
        let observe = async {
            loop {
                if observer.is_running() {
                    observed_bid = cache
                        .borrow()
                        .order_book(&id())
                        .and_then(|book| book.best_bid_size());
                    observer.stop();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                biased;
                result = &mut run => result,
                () = observe => run.await,
            }
        })
        .await
        .unwrap()
    };
    node.dispose();
    assert!(shared.initial_ready_observed.get());
    assert_eq!(
        shared.replacement_queued.load(Ordering::Acquire),
        !matches!(phase, RecoveryPhase::None)
    );
    assert!(result.is_ok(), "Startup recovery failed: {result:?}");
    assert!(shared.engine_ready_at_disconnect.get());
    let expected = match phase {
        RecoveryPhase::None => Quantity::from("10.00"),
        _ => Quantity::from("20.00"),
    };
    assert_eq!(observed_bid, Some(expected));
    let expected_updates: Vec<_> = std::iter::once((0.into(), false))
        .chain((1..=instrument_refreshes).map(|timestamp| (timestamp.into(), true)))
        .collect();
    assert_eq!(*shared.instrument_updates.borrow(), expected_updates);
}

#[rstest]
#[case::stop(IngressEnd::Stop)]
#[case::deadline(IngressEnd::Deadline)]
#[tokio::test]
async fn startup_control_does_not_wait_for_replenishing_data_to_end(#[case] end: IngressEnd) {
    let ingress = Arc::new(ReplenishingIngress {
        active: AtomicBool::new(true),
        started: AtomicBool::new(false),
        callbacks: AtomicU64::new(0),
        end,
    });
    let mut shared = Shared::new(RecoveryPhase::Connecting, 2);
    shared.ingress = Some(ingress.clone());
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("FIXTURE-001"))
        .instrument_id(id())
        .quantity("1.00".into())
        .price("0.40".into())
        .build();
    let order_id = order.client_order_id();
    shared.startup_order = Some(TestOrderEventStubs::submitted(&order, "FIXTURE-001".into()));
    let mut node = build_node(&shared, 1);
    node.kernel()
        .cache()
        .borrow_mut()
        .add_order(order, None, None, false)
        .unwrap();
    let handle = node.handle();
    let watchdog_ingress = ingress.clone();
    let (returned_tx, returned_rx) = std::sync::mpsc::channel();
    // A separate thread can release the old synchronous drain without relying
    // on the runtime that it starves. Passing requires returning before release.
    let watchdog = std::thread::spawn(move || {
        let start = std::time::Instant::now();

        while !watchdog_ingress.started.load(Ordering::Acquire) {
            if start.elapsed() > Duration::from_secs(2) {
                watchdog_ingress.active.store(false, Ordering::Release);
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let release_after = match end {
            IngressEnd::Stop => {
                handle.stop();
                Duration::from_millis(500)
            }
            IngressEnd::Deadline => Duration::from_millis(1_600),
        };

        if returned_rx.recv_timeout(release_after).is_err() {
            watchdog_ingress.active.store(false, Ordering::Release);
        }
    });
    let result = node.run_with_mode(NodeRunMode::Hosted).await;
    let returned_while_ingress_active = ingress.active.load(Ordering::Acquire);
    let _ = returned_tx.send(());
    watchdog.join().unwrap();
    let order_status = node
        .kernel()
        .cache()
        .borrow()
        .order(&order_id)
        .unwrap()
        .status();
    node.dispose();

    assert_eq!(order_status, OrderStatus::Submitted);
    assert!(ingress.callbacks.load(Ordering::Relaxed) > 0);
    assert!(
        returned_while_ingress_active,
        "Startup only returned after event replenishment ended"
    );

    match end {
        IngressEnd::Stop => assert!(result.is_ok(), "{result:?}"),
        IngressEnd::Deadline => {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("readiness timeout")
            );
        }
    }
}

#[rstest]
#[case::start_zero_ready(StartupEntry::Start, 0, 1, true)]
#[case::run_zero_ready(StartupEntry::Run, 0, 1, true)]
#[case::start_one_second_control(StartupEntry::Start, 1, 1, true)]
#[case::run_one_second_control(StartupEntry::Run, 1, 1, true)]
#[case::start_zero_full_batch(StartupEntry::Start, 0, 1_023, true)]
#[case::run_zero_full_batch(StartupEntry::Run, 0, 1_023, true)]
#[case::start_zero_requires_another_batch(StartupEntry::Start, 0, 1_024, false)]
#[case::run_zero_requires_another_batch(StartupEntry::Run, 0, 1_024, false)]
#[tokio::test]
async fn startup_budget_admits_ready_data_without_waiting(
    #[case] entry: StartupEntry,
    #[case] timeout_seconds: u64,
    #[case] instrument_count: u64,
    #[case] succeeds: bool,
) {
    let mut shared = Shared::new(RecoveryPhase::None, 0);
    shared.initial_instruments = instrument_count;
    shared.exec_connect_delay = None;
    let mut node = build_node(&shared, timeout_seconds);
    let observer = node.handle();
    let cache = node.kernel().cache();
    let mut observed_bid = None;
    let mut running_seen = false;
    let result = tokio::time::timeout(Duration::from_secs(4), async {
        match entry {
            StartupEntry::Start => {
                let result = node.start().await;
                running_seen = observer.is_running();

                if running_seen {
                    observed_bid = cache
                        .borrow()
                        .order_book(&id())
                        .and_then(|book| book.best_bid_size());
                    node.stop().await.unwrap();
                }
                result
            }
            StartupEntry::Run => {
                let run = node.run_with_mode(NodeRunMode::Hosted);
                tokio::pin!(run);
                let observe = async {
                    loop {
                        if observer.is_running() {
                            running_seen = true;
                            observed_bid = cache
                                .borrow()
                                .order_book(&id())
                                .and_then(|book| book.best_bid_size());
                            observer.stop();
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                };

                tokio::select! {
                    biased;
                    result = &mut run => result,
                    () = observe => run.await,
                }
            }
        }
    })
    .await
    .unwrap();
    node.dispose();

    assert_eq!(result.is_ok(), succeeds, "{entry:?}: {result:?}");
    assert_eq!(running_seen, succeeds);
    assert_eq!(shared.initial_ready_observed.get(), succeeds);

    if succeeds {
        assert_eq!(observed_bid, Some(Quantity::from("10.00")));
        let expected: Vec<_> = (0..instrument_count)
            .map(|timestamp| (timestamp.into(), false))
            .collect();
        assert_eq!(*shared.instrument_updates.borrow(), expected);
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("readiness timeout")
        );
        assert!(observed_bid.is_none());
    }
}
