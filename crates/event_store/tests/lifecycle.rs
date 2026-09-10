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

//! Kernel-level integration tests for the event-store run lifecycle (Phase 7).
//!
//! Exercises the SPEC contract end-to-end through [`NautilusKernel`]: kernel boot
//! recovers crashed predecessors and a kernel that drops without explicit teardown
//! still seals the run via [`Drop`].

use std::{
    cell::RefCell,
    path::PathBuf,
    rc::Rc,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use ahash::AHashMap;
use bytes::Bytes;
use indexmap::IndexMap;
use nautilus_common::{
    cache::{
        Cache,
        database::{CacheDatabaseAdapter, CacheMap},
    },
    clock::{Clock, TestClock},
    signal::Signal,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_event_store::{
    AppendEntry, EventStore, EventStoreConfig, EventStoreEntry, EventStoreLifecycle, Headers,
    PAYLOAD_TYPE_ACCOUNT_STATE, RedbBackend, RegisteredComponents, RetentionMode, RunIdentity,
    RunManifest, RunStatus, SnapshotAnchor, Topic, apply_cache_replay_entry,
    capture::builtins::{PAYLOAD_TYPE_ORDER_FILL_VOIDED, encode_order_event_any},
    compute_entry_hash, compute_snapshot_content_hash, encode_account_state, recover_predecessors,
};
use nautilus_execution::engine::{
    EventApplicationOutcome, ExecutionEngine, config::ExecutionEngineConfig,
    stubs::StubExecutionClient,
};
use nautilus_model::{
    accounts::{AccountAny, CashAccount},
    data::{
        Bar, CustomData, DataType, FundingRateUpdate, QuoteTick, TradeTick,
        greeks::{GreeksData, YieldCurveData},
    },
    enums::{OmsType, OrderSide, OrderType},
    events::{
        AccountState, OrderEventAny, OrderSnapshot, account::stubs::cash_account_state_million_usd,
        order::spec::OrderFillVoidedSpec, position::snapshot::PositionSnapshot,
    },
    identifiers::{
        AccountId, ActorId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        TraderId, Venue, VenueOrderId,
    },
    instruments::{
        CurrencyPair, Instrument, InstrumentAny, SyntheticInstrument, stubs::audusd_sim,
    },
    orderbook::OrderBook,
    orders::{Order, OrderAny, builder::OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    stubs::TestDefault,
    types::{Currency, Money, Quantity},
};
use nautilus_system::{KernelEventStore, NautilusKernelBuilder};
use rstest::rstest;
use tempfile::TempDir;
use ustr::Ustr;

static KERNEL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_kernel_test() -> MutexGuard<'static, ()> {
    KERNEL_TEST_LOCK.lock().expect("kernel test lock")
}

fn event_store_factory(
    config: EventStoreConfig,
) -> impl FnOnce(UUID4, Rc<RefCell<dyn Clock>>) -> anyhow::Result<Box<dyn KernelEventStore>> + 'static
{
    move |instance_id, clock| {
        Ok(Box::new(EventStoreLifecycle::boot(
            Some(config),
            instance_id,
            clock,
        )?))
    }
}

fn config_with(base_dir: PathBuf) -> EventStoreConfig {
    EventStoreConfig {
        base_dir,
        identity: RunIdentity {
            binary_hash: "deadbeef".to_string(),
            schema_version: 1,
            crate_versions: "feedface".to_string(),
            feature_flags: Vec::new(),
            adapter_versions: IndexMap::new(),
            config_hash: "cafebabe".to_string(),
            seed: None,
        },
        retention: RetentionMode::Full,
        replay_from_run_id: None,
        data_markers: None,
        channel_capacity: 64,
        max_batch_entries: 1,
        max_batch_latency: Duration::from_millis(2),
        halt_threshold: Duration::from_secs(2),
        run_started_timeout: Duration::from_secs(2),
    }
}

fn running_manifest(config: &EventStoreConfig, instance_id: UUID4, run_id: &str) -> RunManifest {
    RunManifest {
        run_id: run_id.to_string(),
        parent_run_id: None,
        instance_id: instance_id.to_string(),
        binary_hash: config.identity.binary_hash.clone(),
        schema_version: config.identity.schema_version,
        crate_versions: config.identity.crate_versions.clone(),
        feature_flags: config.identity.feature_flags.clone(),
        adapter_versions: config.identity.adapter_versions.clone(),
        config_hash: config.identity.config_hash.clone(),
        registered_components: RegisteredComponents::default(),
        seed: config.identity.seed,
        start_ts_init: UnixNanos::from(1),
        end_ts_init: None,
        high_watermark: 0,
        status: RunStatus::Running,
    }
}

fn append_account_state(seq: u64, state: &AccountState) -> AppendEntry {
    let encoded = encode_account_state(state).expect("encode account state");
    AppendEntry::without_indices(captured_entry(
        seq,
        "events.account.SIM",
        PAYLOAD_TYPE_ACCOUNT_STATE,
        encoded.payload,
    ))
}

fn captured_entry(seq: u64, topic: &str, payload_type: &str, payload: Bytes) -> EventStoreEntry {
    let topic = Topic::from(topic);
    let ts = UnixNanos::from(seq);
    let headers = Headers::empty();
    let hash = compute_entry_hash(
        seq,
        ts,
        ts,
        topic.as_ref(),
        payload_type,
        &payload,
        &headers,
    );
    EventStoreEntry::new(
        hash,
        seq,
        headers,
        topic,
        Ustr::from(payload_type),
        payload,
        ts,
        ts,
    )
}

fn setup_netting_snapshot_engine(
    execution_engine: &mut ExecutionEngine,
    instrument: &CurrencyPair,
) {
    let stub_client = StubExecutionClient::new(
        ClientId::from("STUB"),
        AccountId::test_default(),
        Venue::test_default(),
        OmsType::Netting,
        None,
    );
    execution_engine
        .register_client(Box::new(stub_client))
        .expect("register stub client");
    execution_engine
        .cache()
        .borrow_mut()
        .add_instrument(instrument.clone().into())
        .expect("add instrument");
    execution_engine
        .cache()
        .borrow_mut()
        .add_account(CashAccount::default().into())
        .expect("add account");
}

#[expect(clippy::too_many_arguments)]
fn process_filled_order(
    execution_engine: &mut ExecutionEngine,
    trader_id: TraderId,
    strategy_id: StrategyId,
    instrument: &CurrencyPair,
    client_order_id: &str,
    venue_order_id: &str,
    trade_id: &str,
    side: OrderSide,
    quantity: u64,
    position_id: PositionId,
) {
    let order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id)
        .strategy_id(strategy_id)
        .instrument_id(instrument.id)
        .client_order_id(ClientOrderId::from(client_order_id))
        .side(side)
        .quantity(Quantity::from(quantity))
        .build();

    execution_engine
        .cache()
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("STUB")), true)
        .expect("add order");
    execution_engine.process(&TestOrderEventStubs::submitted(
        &order,
        AccountId::test_default(),
    ));
    execution_engine.process(&TestOrderEventStubs::accepted(
        &order,
        AccountId::test_default(),
        VenueOrderId::from(venue_order_id),
    ));

    let accepted_order = execution_engine
        .cache()
        .borrow()
        .order_owned(&order.client_order_id())
        .expect("accepted order");
    let instrument_any: InstrumentAny = instrument.clone().into();
    execution_engine.process(&TestOrderEventStubs::filled(
        &accepted_order,
        &instrument_any,
        Some(TradeId::new(trade_id)),
        Some(position_id),
        None,
        None,
        None,
        None,
        None,
        Some(AccountId::test_default()),
    ));
}

#[rstest]
#[case::archived_correction(true, 0)]
#[case::current_cycle_with_history(true, 4)]
#[case::missing_prior_history(false, 0)]
#[case::current_cycle_without_history(false, 4)]
fn replay_fill_corrections_match_live_accounting(
    #[case] carry_history: bool,
    #[case] corrected_order: usize,
) {
    let _guard = lock_kernel_test();
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(TestClock::new())),
        Rc::new(RefCell::new(Cache::default())),
        Some(ExecutionEngineConfig {
            carry_replay_events_on_reopen: carry_history,
            ..Default::default()
        }),
    );
    let instrument = audusd_sim();
    let trader_id = TraderId::test_default();
    let strategy_id = StrategyId::test_default();
    let position_id = PositionId::new(format!("{}-{strategy_id}", instrument.id));
    setup_netting_snapshot_engine(&mut engine, &instrument);

    // Two closed cycles and an open position. All fills are at 1 USD with a 2 USD fee.
    // Voiding 40k from the first buy leaves +60,-40,0,-40,+60k: one rebuilt closed cycle.
    for (index, (side, quantity)) in [
        (OrderSide::Buy, 100_000),
        (OrderSide::Sell, 100_000),
        (OrderSide::Buy, 40_000),
        (OrderSide::Sell, 40_000),
        (OrderSide::Buy, 100_000),
    ]
    .into_iter()
    .enumerate()
    {
        process_filled_order(
            &mut engine,
            trader_id,
            strategy_id,
            &instrument,
            &format!("O-CORRECTION-{index}"),
            &format!("V-CORRECTION-{index}"),
            &format!("T-CORRECTION-{index}"),
            side,
            quantity,
            position_id,
        );
    }

    // Seed replay from actual native state to isolate correction equivalence. This does not
    // claim that replaying the preceding fills can reconstruct the original OMS policy.
    let mut replay = Cache::default();
    let order_id = ClientOrderId::new(format!("O-CORRECTION-{corrected_order}"));
    let (original_fill, original_archives) = {
        let cache = engine.cache().borrow();
        replay
            .add_instrument(instrument.into())
            .expect("instrument");

        for order in cache.orders(None, None, None, None, None) {
            replay
                .add_order(order.cloned(), None, None, true)
                .expect("seed native order");
        }
        let position = cache.position_owned(&position_id).expect("native position");
        replay
            .add_position(&position, OmsType::Netting)
            .expect("seed native position");
        let archives = cache
            .position_snapshot_bytes(&position_id)
            .expect("archives");
        assert_eq!(archives.len(), 2);
        for (index, bytes) in archives.iter().enumerate() {
            replay
                .restore_snapshot_blob(
                    &format!("cache://position-snapshots/{position_id}/{index}"),
                    Bytes::copy_from_slice(bytes),
                )
                .expect("seed native archive");
        }
        let order = cache.order(&order_id).expect("source order");
        let fill = order
            .events()
            .into_iter()
            .find_map(|event| match event {
                OrderEventAny::Filled(fill) => Some(fill.clone()),
                _ => None,
            })
            .expect("source fill");
        (fill, archives)
    };

    for (index, (quantity, refund, valid)) in [
        (40_000, "0.80 USD", true),
        (50_000, "1.00 USD", true),
        (50_000, "1.00 USD", false), // Duplicate cumulative correction.
        (20_000, "0.40 USD", false), // Stale cumulative quantity.
    ]
    .into_iter()
    .enumerate()
    {
        let fill = &original_fill;
        let event = OrderEventAny::FillVoided(
            OrderFillVoidedSpec::builder()
                .trader_id(fill.trader_id)
                .strategy_id(fill.strategy_id)
                .instrument_id(fill.instrument_id)
                .client_order_id(fill.client_order_id)
                .venue_order_id(fill.venue_order_id)
                .account_id(fill.account_id)
                .trade_id(fill.trade_id)
                .voided_qty(Quantity::from(quantity))
                .commission_voided(Money::from(refund))
                .order_side(fill.order_side)
                .order_type(fill.order_type)
                .last_px(fill.last_px)
                .currency(fill.currency)
                .liquidity_side(fill.liquidity_side)
                .maybe_position_id(fill.position_id)
                .build(),
        );
        let before_order = rmp_serde::to_vec_named(&replay.order_owned(&order_id)).unwrap();
        let before_position =
            rmp_serde::to_vec_named(&replay.position_owned(&position_id)).unwrap();
        let before_archives = replay.position_snapshot_bytes(&position_id);
        let applicable = carry_history || corrected_order == 4;
        let accepted = applicable && valid;
        assert_eq!(
            engine.process_with_outcome(&event),
            if accepted {
                EventApplicationOutcome::Applied
            } else {
                EventApplicationOutcome::Incomplete
            },
        );
        let encoded = encode_order_event_any(&event).expect("capture correction");
        let entry = captured_entry(
            index as u64 + 1,
            "events.order.correction",
            PAYLOAD_TYPE_ORDER_FILL_VOIDED,
            encoded.payload,
        );
        let result = apply_cache_replay_entry(&mut replay, &entry);
        if accepted {
            assert!(result.expect("replay correction"));
        } else {
            assert!(result.is_err());
            assert_eq!(
                rmp_serde::to_vec_named(&replay.order_owned(&order_id)).unwrap(),
                before_order
            );
            assert_eq!(
                rmp_serde::to_vec_named(&replay.position_owned(&position_id)).unwrap(),
                before_position
            );
            assert_eq!(
                replay.position_snapshot_bytes(&position_id),
                before_archives
            );
        }

        let live = engine.cache().borrow();
        assert_eq!(
            rmp_serde::to_vec_named(&live.order_owned(&order_id)).unwrap(),
            rmp_serde::to_vec_named(&replay.order_owned(&order_id)).unwrap(),
        );
        assert_eq!(
            rmp_serde::to_vec_named(&live.position_owned(&position_id)).unwrap(),
            rmp_serde::to_vec_named(&replay.position_owned(&position_id)).unwrap(),
        );
        let live_frames = live.position_snapshots(Some(&position_id), None);
        let mut replay_frames = replay.position_snapshots(Some(&position_id), None);
        assert_eq!(live_frames.len(), replay_frames.len());
        for (live_frame, replay_frame) in live_frames.iter().zip(&mut replay_frames) {
            // Archive IDs are newly generated by each cache; all other state must match.
            replay_frame.id = live_frame.id;
            assert_eq!(
                rmp_serde::to_vec_named(&live_frame).unwrap(),
                rmp_serde::to_vec_named(&replay_frame).unwrap()
            );
        }
        let archived_pnl = live_frames
            .iter()
            .fold(Money::zero(Currency::USD()), |total, frame| {
                total + frame.realized_pnl.expect("archived PnL")
            });
        let position = live.position(&position_id).expect("corrected position");
        let expected_total = if !applicable {
            "-10.00 USD"
        } else if index == 0 {
            "-9.20 USD"
        } else {
            "-9.00 USD"
        };
        assert_eq!(
            archived_pnl + position.realized_pnl.expect("current PnL"),
            Money::from(expected_total)
        );
        assert_eq!(
            position.quantity,
            Quantity::from(if !applicable {
                100_000
            } else if index == 0 {
                60_000
            } else {
                50_000
            })
        );

        if applicable && corrected_order == 0 {
            // At 50k the corrected history no longer goes flat; the current position
            // must absorb all PnL and the archive must disappear.
            assert_eq!(live_frames.len(), usize::from(index == 0));
            assert_eq!(
                archived_pnl,
                Money::from(if index == 0 { "-5.20 USD" } else { "0 USD" })
            );
        } else {
            assert_eq!(
                live.position_snapshot_bytes(&position_id),
                Some(original_archives.clone())
            );
        }
    }
}

#[rstest]
fn kernel_drop_after_start_seals_run_as_ended() {
    let _guard = lock_kernel_test();

    // Imperative `engine.run()` followed by drop is the dominant backtest pattern;
    // BacktestEngine::end() never calls finalize_stop, and many callers skip
    // dispose(). The kernel's Drop impl is the last-chance seal site, so a normal
    // backtest exit must seal the run as Ended without leaving Running on disk.
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let advanced_ts = UnixNanos::from(1_234_567_890_u64);

    let run_id = {
        let mut kernel = NautilusKernelBuilder::default()
            .with_instance_id(instance_id)
            .with_event_store(event_store_factory(config_with(tmp.path().to_path_buf())))
            .build()
            .expect("kernel");

        kernel.start().expect("start kernel");

        // Advance the kernel's TestClock so the drop-seal ts is distinguishable from 0.
        {
            let mut clock_borrow = kernel.clock.borrow_mut();
            let test_clock = (&mut *clock_borrow as &mut dyn std::any::Any)
                .downcast_mut::<TestClock>()
                .expect("kernel clock is a TestClock in Backtest environment");
            test_clock.advance_time(advanced_ts, true);
        }

        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .expect("run open after start")
            .to_string()
    };

    let manifests = RedbBackend::list_runs(tmp.path(), &instance_id.to_string()).expect("list");
    let manifest = manifests
        .into_iter()
        .find(|m| m.run_id == run_id)
        .expect("manifest present");
    assert_eq!(
        manifest.status,
        RunStatus::Ended,
        "kernel Drop must seal the run on graceful exit",
    );
    assert_eq!(
        manifest.high_watermark, 2,
        "RunStarted at seq=1 plus RunEnded at seq=2 produces exactly 2 entries",
    );
    assert_eq!(
        manifest.end_ts_init,
        Some(advanced_ts),
        "drop-seal must stamp end_ts_init from the kernel's clock, not a separate object",
    );

    // A second-boot recovery sweep must not chain to a run that closed cleanly.
    let outcome =
        recover_predecessors(tmp.path(), &instance_id.to_string()).expect("recovery sweep");
    assert!(
        outcome.recovered.is_empty(),
        "Ended runs are not predecessors to recover, was {:?}",
        outcome.recovered,
    );
    assert!(outcome.parent_run_id.is_none());
}

#[rstest]
fn kernel_start_installs_snapshot_anchorer_for_execution_snapshots() {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let mut config = config_with(tmp.path().to_path_buf());

    // This test drives the async writer through a synchronous anchor round-trip mid-run
    // and then asserts the run sealed. On a loaded CI box the writer thread can be
    // scheduled late, so the default 2s ceilings would misread a slow writer as a
    // fail-stop, skip the seal, and fail the durability assertions. Generous-but-finite
    // ceilings tolerate scheduling jitter while still surfacing a genuinely stuck writer.
    config.halt_threshold = Duration::from_secs(30);
    config.run_started_timeout = Duration::from_secs(30);
    let instrument = audusd_sim();
    let trader_id = TraderId::test_default();
    let strategy_id = StrategyId::test_default();
    let position_id = PositionId::new(format!("{}-{strategy_id}", instrument.id));

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_exec_engine_config(ExecutionEngineConfig {
            snapshot_positions: true,
            ..Default::default()
        })
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    {
        let mut exec_engine = kernel.exec_engine.borrow_mut();
        setup_netting_snapshot_engine(&mut exec_engine, &instrument);
    }

    kernel.start().expect("start kernel");
    let run_id = kernel
        .event_store()
        .expect("event store")
        .run_id()
        .expect("run open after start")
        .to_string();

    {
        let mut exec_engine = kernel.exec_engine.borrow_mut();
        process_filled_order(
            &mut exec_engine,
            trader_id,
            strategy_id,
            &instrument,
            "O-KERNEL-ANCHOR-1",
            "V-KERNEL-ANCHOR-1",
            "T-KERNEL-ANCHOR-1",
            OrderSide::Buy,
            100_000,
            position_id,
        );
        process_filled_order(
            &mut exec_engine,
            trader_id,
            strategy_id,
            &instrument,
            "O-KERNEL-ANCHOR-2",
            "V-KERNEL-ANCHOR-2",
            "T-KERNEL-ANCHOR-2",
            OrderSide::Sell,
            150_000,
            position_id,
        );
    }

    let snapshot = {
        let cache = kernel.cache.borrow();
        let frames = cache
            .position_snapshot_bytes(&position_id)
            .expect("position snapshot");
        assert_eq!(frames.len(), 1);
        frames[0].clone()
    };

    kernel.dispose();

    let reader = RedbBackend::open_sealed(&config.base_dir, &instance_id.to_string(), &run_id)
        .expect("open sealed run");
    let anchor = reader
        .latest_snapshot_anchor()
        .expect("latest snapshot anchor")
        .expect("anchor present");
    let durable_high_watermark = reader.high_watermark().expect("high watermark");

    assert_eq!(
        anchor.blob_ref,
        format!("cache://position-snapshots/{}/0", position_id.as_str()),
    );
    assert_eq!(
        anchor.content_hash,
        compute_snapshot_content_hash(&snapshot),
    );
    assert_eq!(
        anchor.coverage,
        nautilus_event_store::SnapshotCoverage::Partial
    );
    assert!(anchor.high_watermark >= 1);
    assert!(
        anchor.high_watermark <= durable_high_watermark,
        "anchor high_watermark {} exceeded durable high_watermark {}",
        anchor.high_watermark,
        durable_high_watermark,
    );
    assert!(
        reader
            .scan_seq(anchor.high_watermark)
            .expect("anchor high-watermark seq")
            .is_some(),
        "anchor must point to an existing durable event",
    );
}

#[rstest]
#[case::crashed_full_log(false, false, false)]
#[case::configured_full_log(true, false, false)]
#[case::crashed_partial(false, true, false)]
#[case::configured_partial(true, true, false)]
#[case::crashed_legacy(false, true, true)]
#[case::configured_legacy(true, true, true)]
fn kernel_start_requires_complete_cache_replay(
    #[case] configured_replay: bool,
    #[case] partial_snapshot: bool,
    #[case] legacy: bool,
    #[values(false, true)] from_database: bool,
) {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let mut config = config_with(tmp.path().to_path_buf());
    let parent_run_id = "parent-run";
    if configured_replay {
        config.replay_from_run_id = Some(parent_run_id.to_string());
    }
    let instrument = InstrumentAny::CurrencyPair(audusd_sim());
    let position_id = PositionId::new("P-KERNEL-RESTORE-1");
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100_000))
        .build();
    let fill = TestOrderEventStubs::filled(
        &order,
        &instrument,
        Some(TradeId::new("T-KERNEL-RESTORE-1")),
        Some(position_id),
        None,
        None,
        None,
        None,
        None,
        Some(AccountId::test_default()),
    );
    let position = Position::new(&instrument, fill.into());
    let mut snapshot_cache = Cache::default();
    let snapshot_ref = snapshot_cache
        .snapshot_position_encoded(&position)
        .expect("snapshot");
    let anchored_state = cash_account_state_million_usd("100 USD", "0 USD", "100 USD");
    let replayed_state = cash_account_state_million_usd("200 USD", "0 USD", "200 USD");
    let mut unrelated_state = cash_account_state_million_usd("700 USD", "0 USD", "700 USD");
    unrelated_state.account_id = AccountId::new("SIM-UNRELATED");

    let source_path = {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, parent_run_id))
            .expect("open");
        backend
            .append_batch(&[
                append_account_state(1, &anchored_state),
                append_account_state(2, &unrelated_state),
            ])
            .expect("append prefix");

        if partial_snapshot {
            backend
                .record_snapshot_anchor(SnapshotAnchor::new(
                    2,
                    snapshot_ref.blob_ref.clone(),
                    compute_snapshot_content_hash(snapshot_ref.blob.as_ref()),
                    nautilus_event_store::SnapshotCoverage::Partial,
                ))
                .expect("anchor position archive");
        }
        backend
            .append_batch(&[append_account_state(3, &replayed_state)])
            .expect("append tail");
        if configured_replay {
            backend.seal(RunStatus::Ended).expect("seal source");
        }
        backend.current_path().expect("source path").to_path_buf()
    };

    if legacy {
        let bytes = nautilus_event_store::codec::encode_to_vec(&(
            2_u64,
            snapshot_ref.blob_ref.as_str(),
            compute_snapshot_content_hash(snapshot_ref.blob.as_ref()),
        ))
        .expect("legacy snapshot metadata");
        let table: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new("snapshot_anchor");
        let db = redb::Database::create(&source_path).expect("open source");
        let txn = db.begin_write().expect("begin legacy metadata write");
        {
            txn.open_table(table)
                .expect("anchor table")
                .insert("latest", bytes.as_slice())
                .expect("legacy metadata");
        }
        txn.commit().expect("commit legacy metadata");
    }

    let attempts = if partial_snapshot { 2 } else { 1 };
    for _ in 0..attempts {
        let mut builder = NautilusKernelBuilder::default()
            .with_instance_id(instance_id)
            .with_event_store(event_store_factory(config.clone()));

        if from_database {
            builder = builder.with_cache_database(Box::new(StubCacheDatabase::with_blob(
                snapshot_ref.blob_ref.clone(),
                snapshot_ref.blob.clone(),
            )));
        }
        let built = builder.build();

        if partial_snapshot && !configured_replay {
            let error = built.expect_err("unrestorable source must block construction");
            let expected = if legacy {
                "decode snapshot anchor"
            } else {
                "anchored cache checkpoint"
            };
            assert!(format!("{error:#}").contains(expected), "{error:#}");
            let manifests = RedbBackend::list_runs(&config.base_dir, &instance_id.to_string())
                .expect("list sources");
            assert_eq!(manifests.len(), 1);
            assert_eq!(manifests[0].run_id, parent_run_id);
            assert_eq!(manifests[0].status, RunStatus::Running);
            continue;
        }
        let mut kernel = built.expect("kernel");
        if !from_database {
            kernel
                .cache
                .borrow_mut()
                .add(&snapshot_ref.blob_ref, snapshot_ref.blob.clone())
                .expect("seed archive blob");
        }

        let result = kernel.start();
        let cache = kernel.cache.borrow();
        let event_store = kernel.event_store().expect("event store");
        assert_eq!(event_store.parent_run_id(), Some(parent_run_id));
        assert!(cache.position_snapshot_bytes(&position_id).is_none());

        if partial_snapshot {
            let error = result.expect_err("partial snapshot must block startup");
            let expected = if legacy {
                "decode snapshot anchor"
            } else {
                "partial"
            };
            assert!(format!("{error:#}").contains(expected), "{error:#}");
            assert!(cache.account_owned(&anchored_state.account_id).is_none());
            assert!(cache.account_owned(&unrelated_state.account_id).is_none());
            assert!(event_store.run_id().is_none());
        } else {
            result.expect("anchor-free startup");
            assert_eq!(
                cache
                    .account_owned(&anchored_state.account_id)
                    .expect("account")
                    .events(),
                vec![anchored_state.clone(), replayed_state.clone()],
            );
            assert_eq!(
                cache
                    .account_owned(&unrelated_state.account_id)
                    .expect("unrelated account")
                    .events(),
                vec![unrelated_state.clone()],
            );
            assert!(event_store.run_id().is_some());
        }
    }
}

#[rstest]
fn kernel_start_configured_replay_does_not_start_execution_clients() {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let replay_run_id = "seed-run";
    let mut config = config_with(tmp.path().to_path_buf());
    config.replay_from_run_id = Some(replay_run_id.to_string());
    let replayed_state = cash_account_state_million_usd("200 USD", "0 USD", "200 USD");

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, replay_run_id))
            .expect("open replay source");
        backend
            .append_batch(&[append_account_state(1, &replayed_state)])
            .expect("append replay state");
        backend.seal(RunStatus::Ended).expect("seal replay source");
    }

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_event_store(event_store_factory(config))
        .build()
        .expect("kernel");
    {
        let stub_client = StubExecutionClient::new(
            ClientId::from("STUB"),
            AccountId::test_default(),
            Venue::test_default(),
            OmsType::Netting,
            None,
        );
        kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(stub_client))
            .expect("register stub client");
    }

    assert!(
        !kernel.exec_engine.borrow().check_connected(),
        "stub client must start disconnected",
    );

    kernel.start().expect("start kernel");

    {
        let cache = kernel.cache.borrow();
        let account = cache
            .account_owned(&replayed_state.account_id)
            .expect("replayed account");

        assert_eq!(account.events(), vec![replayed_state]);
    }

    assert!(kernel.is_event_store_replay());
    assert!(
        !kernel.exec_engine.borrow().check_connected(),
        "configured event-store replay must not start execution clients",
    );
    assert!(
        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .is_some(),
        "replay-only startup still opens a child run for inspection",
    );
}

#[rstest]
fn kernel_start_configured_replay_requires_load_state() {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let replay_run_id = "healthy-seed-run";
    let mut config = config_with(tmp.path().to_path_buf());
    config.replay_from_run_id = Some(replay_run_id.to_string());
    let replayed_state = cash_account_state_million_usd("250 USD", "0 USD", "250 USD");

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, replay_run_id))
            .expect("open replay source");
        backend
            .append_batch(&[append_account_state(1, &replayed_state)])
            .expect("append replay state");
        backend.seal(RunStatus::Ended).expect("seal replay source");
    }

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_load_state(false)
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    kernel
        .start()
        .expect_err("startup must reject invalid persisted state");

    let manifests =
        RedbBackend::list_runs(&config.base_dir, &instance_id.to_string()).expect("list runs");

    assert!(
        kernel
            .cache
            .borrow()
            .account_owned(&replayed_state.account_id)
            .is_none()
    );
    assert!(
        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .is_none()
    );
    assert!(kernel.is_event_store_replay_configured());
    assert!(!kernel.is_event_store_replay());
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].run_id, replay_run_id);
    assert_eq!(manifests[0].status, RunStatus::Ended);
}

#[rstest]
fn kernel_start_configured_replay_overrides_recovered_parent() {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let replay_run_id = "seed-run";
    let recovered_parent_run_id = "recovered-parent-run";
    let mut config = config_with(tmp.path().to_path_buf());
    config.replay_from_run_id = Some(replay_run_id.to_string());
    let parent_state = cash_account_state_million_usd("900 USD", "0 USD", "900 USD");
    let replayed_state = cash_account_state_million_usd("300 USD", "0 USD", "300 USD");

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, replay_run_id))
            .expect("open replay source");
        backend
            .append_batch(&[append_account_state(1, &replayed_state)])
            .expect("append replay state");
        backend.seal(RunStatus::Ended).expect("seal replay source");
    }

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(
                &config,
                instance_id,
                recovered_parent_run_id,
            ))
            .expect("open recovered parent source");
        backend
            .append_batch(&[append_account_state(1, &parent_state)])
            .expect("append parent state");
    }

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    kernel.start().expect("start kernel");

    {
        let cache = kernel.cache.borrow();
        let account = cache
            .account_owned(&replayed_state.account_id)
            .expect("replayed account");

        assert_eq!(account.events(), vec![replayed_state]);
    }

    let event_store = kernel.event_store().expect("event store");
    assert_eq!(event_store.parent_run_id(), Some(replay_run_id));
    let opened_run_id = event_store
        .run_id()
        .expect("configured replay must open a fresh run")
        .to_string();

    kernel.dispose();

    let manifests =
        RedbBackend::list_runs(&config.base_dir, &instance_id.to_string()).expect("list runs");
    let opened_manifest = manifests
        .iter()
        .find(|manifest| manifest.run_id == opened_run_id)
        .expect("opened run manifest");
    let recovered_parent_manifest = manifests
        .iter()
        .find(|manifest| manifest.run_id == recovered_parent_run_id)
        .expect("recovered parent manifest");

    assert_eq!(
        opened_manifest.parent_run_id.as_deref(),
        Some(replay_run_id)
    );
    assert_eq!(
        recovered_parent_manifest.status,
        RunStatus::CrashedRecovered,
    );
}

#[rstest]
fn kernel_start_missing_configured_replay_run_does_not_open_new_run() {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let mut config = config_with(tmp.path().to_path_buf());
    config.replay_from_run_id = Some("missing-run".to_string());

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    kernel
        .start()
        .expect_err("startup must reject invalid persisted state");

    let manifests =
        RedbBackend::list_runs(&config.base_dir, &instance_id.to_string()).expect("list runs");

    assert!(
        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .is_none()
    );
    assert!(kernel.is_event_store_replay_configured());
    assert!(!kernel.is_event_store_replay());
    assert!(manifests.is_empty());
}

#[rstest]
#[case::load_state(true)]
#[case::skip_load_state(false)]
fn kernel_start_quarantined_configured_replay_run_does_not_open_new_run(#[case] load_state: bool) {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let replay_run_id = "quarantined-run";
    let mut config = config_with(tmp.path().to_path_buf());
    config.replay_from_run_id = Some(replay_run_id.to_string());
    let quarantined_state = cash_account_state_million_usd("100 USD", "0 USD", "100 USD");

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, replay_run_id))
            .expect("open quarantined source");
        backend
            .append_batch(&[append_account_state(1, &quarantined_state)])
            .expect("append quarantined state");
        backend
            .seal(RunStatus::Quarantined)
            .expect("seal quarantined source");
    }

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_load_state(load_state)
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    kernel
        .start()
        .expect_err("startup must reject invalid persisted state");

    let manifests =
        RedbBackend::list_runs(&config.base_dir, &instance_id.to_string()).expect("list runs");

    assert!(
        kernel
            .cache
            .borrow()
            .account_owned(&quarantined_state.account_id)
            .is_none()
    );
    assert!(
        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .is_none()
    );
    assert!(kernel.is_event_store_replay_configured());
    assert!(!kernel.is_event_store_replay());
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].run_id, replay_run_id);
    assert_eq!(manifests[0].status, RunStatus::Quarantined);
}

#[rstest]
#[case::missing_blob(false, false)]
#[case::hash_mismatch(true, true)]
fn kernel_start_rejects_partial_snapshot_with_missing_or_corrupt_blob(
    #[case] seed_blob: bool,
    #[case] bad_hash: bool,
) {
    let _guard = lock_kernel_test();
    let tmp = TempDir::new().expect("tempdir");
    let instance_id = UUID4::new();
    let mut config = config_with(tmp.path().to_path_buf());
    let parent_run_id = "parent-run";
    config.replay_from_run_id = Some(parent_run_id.to_string());
    let instrument = audusd_sim();
    let instrument_any = InstrumentAny::CurrencyPair(instrument.clone());
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100_000))
        .build();
    let fill = TestOrderEventStubs::filled(
        &order,
        &instrument_any,
        Some(TradeId::new("T-KERNEL-RESTORE-FAILURE-1")),
        Some(PositionId::new("P-KERNEL-RESTORE-FAILURE-1")),
        None,
        None,
        None,
        None,
        None,
        Some(AccountId::test_default()),
    );
    let position = Position::new(&instrument_any, fill.into());
    let mut snapshot_cache = Cache::default();
    let snapshot_ref = snapshot_cache
        .snapshot_position_encoded(&position)
        .expect("snapshot position");
    let anchored_state = cash_account_state_million_usd("100 USD", "0 USD", "100 USD");
    let content_hash = if bad_hash {
        "blake3:bad".to_string()
    } else {
        compute_snapshot_content_hash(snapshot_ref.blob.as_ref())
    };

    {
        let mut backend = RedbBackend::new(config.base_dir.clone());
        backend
            .open_run(running_manifest(&config, instance_id, parent_run_id))
            .expect("open parent run");
        backend
            .append_batch(&[append_account_state(1, &anchored_state)])
            .expect("append anchored state");
        backend
            .record_snapshot_anchor(SnapshotAnchor::new(
                1,
                snapshot_ref.blob_ref.clone(),
                content_hash,
                nautilus_event_store::SnapshotCoverage::Partial,
            ))
            .expect("record snapshot anchor");
        backend.seal(RunStatus::Ended).expect("seal source");
    }

    let mut kernel = NautilusKernelBuilder::default()
        .with_instance_id(instance_id)
        .with_event_store(event_store_factory(config.clone()))
        .build()
        .expect("kernel");

    if seed_blob {
        kernel
            .cache
            .borrow_mut()
            .add(&snapshot_ref.blob_ref, snapshot_ref.blob)
            .expect("seed cache-owned snapshot blob");
    }

    kernel
        .start()
        .expect_err("startup must reject invalid persisted state");

    let manifests =
        RedbBackend::list_runs(&config.base_dir, &instance_id.to_string()).expect("list runs");

    assert_eq!(
        kernel.event_store().expect("event store").parent_run_id(),
        Some(parent_run_id)
    );
    assert!(
        kernel
            .event_store()
            .expect("event store")
            .run_id()
            .is_none()
    );
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].run_id, parent_run_id);
}

#[rstest]
#[case::healthy(false)]
#[case::load_error(true)]
fn snapshot_blob_restore_from_database(#[case] load_error: bool) {
    let instrument = audusd_sim();
    let instrument_any = InstrumentAny::CurrencyPair(instrument.clone());
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument.id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100_000))
        .build();
    let fill = TestOrderEventStubs::filled(
        &order,
        &instrument_any,
        Some(TradeId::new("T-KERNEL-DB-LOAD-ERR-1")),
        Some(PositionId::new("P-KERNEL-DB-LOAD-ERR-1")),
        None,
        None,
        None,
        None,
        None,
        Some(AccountId::test_default()),
    );
    let position = Position::new(&instrument_any, fill.into());
    let mut snapshot_cache = Cache::default();
    let snapshot_ref = snapshot_cache
        .snapshot_position_encoded(&position)
        .expect("snapshot position");

    let database = if load_error {
        StubCacheDatabase::failing_load("db unavailable")
    } else {
        StubCacheDatabase::with_blob(snapshot_ref.blob_ref.clone(), snapshot_ref.blob.clone())
    };
    let mut cache = Cache::new(None, Some(Box::new(database)));
    let anchor = SnapshotAnchor::new(
        0,
        snapshot_ref.blob_ref,
        compute_snapshot_content_hash(snapshot_ref.blob.as_ref()),
        nautilus_event_store::SnapshotCoverage::Partial,
    );
    let result = nautilus_event_store::restore_cache_snapshot_blob(&mut cache, Some(&anchor));

    if load_error {
        assert!(
            result
                .expect_err("database failure")
                .to_string()
                .contains("db unavailable")
        );
        assert!(cache.position_snapshot_bytes(&position.id).is_none());
    } else {
        result.expect("load archive blob");
        assert_eq!(
            cache.position_snapshot_bytes(&position.id).expect("frame"),
            vec![snapshot_ref.blob.to_vec()]
        );
    }
}

struct StubCacheDatabase {
    general: AHashMap<String, Bytes>,
    load_error: Option<String>,
}

impl StubCacheDatabase {
    fn with_blob(blob_ref: String, blob: Bytes) -> Self {
        let mut general = AHashMap::new();
        general.insert(blob_ref, blob);
        Self {
            general,
            load_error: None,
        }
    }

    fn failing_load(message: &str) -> Self {
        Self {
            general: AHashMap::new(),
            load_error: Some(message.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for StubCacheDatabase {
    fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        Ok(CacheMap::default())
    }

    fn load(&self) -> anyhow::Result<AHashMap<String, Bytes>> {
        if let Some(message) = &self.load_error {
            anyhow::bail!("{message}");
        }
        Ok(self.general.clone())
    }

    async fn load_currencies(&self) -> anyhow::Result<AHashMap<Ustr, Currency>> {
        Ok(AHashMap::new())
    }

    async fn load_instruments(&self) -> anyhow::Result<AHashMap<InstrumentId, InstrumentAny>> {
        Ok(AHashMap::new())
    }

    async fn load_synthetics(&self) -> anyhow::Result<AHashMap<InstrumentId, SyntheticInstrument>> {
        Ok(AHashMap::new())
    }

    async fn load_accounts(&self) -> anyhow::Result<AHashMap<AccountId, AccountAny>> {
        Ok(AHashMap::new())
    }

    async fn load_orders(&self) -> anyhow::Result<AHashMap<ClientOrderId, OrderAny>> {
        Ok(AHashMap::new())
    }

    async fn load_positions(&self) -> anyhow::Result<AHashMap<PositionId, Position>> {
        Ok(AHashMap::new())
    }

    fn load_index_order_position(&self) -> anyhow::Result<AHashMap<ClientOrderId, PositionId>> {
        Ok(AHashMap::new())
    }

    fn load_index_order_client(&self) -> anyhow::Result<AHashMap<ClientOrderId, ClientId>> {
        Ok(AHashMap::new())
    }

    async fn load_currency(&self, _code: &Ustr) -> anyhow::Result<Option<Currency>> {
        Ok(None)
    }

    async fn load_instrument(
        &self,
        _instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        Ok(None)
    }

    async fn load_synthetic(
        &self,
        _instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<SyntheticInstrument>> {
        Ok(None)
    }

    async fn load_account(&self, _account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        Ok(None)
    }

    async fn load_order(
        &self,
        _client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderAny>> {
        Ok(None)
    }

    async fn load_position(&self, _position_id: &PositionId) -> anyhow::Result<Option<Position>> {
        Ok(None)
    }

    fn load_actor(&self, _actor_id: &ActorId) -> anyhow::Result<AHashMap<String, Bytes>> {
        Ok(AHashMap::new())
    }

    fn load_strategy(&self, _strategy_id: &StrategyId) -> anyhow::Result<AHashMap<String, Bytes>> {
        Ok(AHashMap::new())
    }

    fn load_signals(&self, _name: &str) -> anyhow::Result<Vec<Signal>> {
        Ok(Vec::new())
    }

    fn load_custom_data(&self, _data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        Ok(Vec::new())
    }

    fn load_order_snapshot(
        &self,
        _client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderSnapshot>> {
        Ok(None)
    }

    fn load_position_snapshot(
        &self,
        _position_id: &PositionId,
    ) -> anyhow::Result<Option<PositionSnapshot>> {
        Ok(None)
    }

    fn load_quotes(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        Ok(Vec::new())
    }

    fn load_trades(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        Ok(Vec::new())
    }

    fn load_funding_rates(
        &self,
        _instrument_id: &InstrumentId,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        Ok(Vec::new())
    }

    fn load_bars(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        Ok(Vec::new())
    }

    fn add(&self, _key: String, _value: Bytes) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_currency(&self, _currency: &Currency) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_instrument(&self, _instrument: &InstrumentAny) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_synthetic(&self, _synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_account(&self, _account: &AccountAny) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_order(&self, _order: &OrderAny, _client_id: Option<ClientId>) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_order_snapshot(&self, _snapshot: &OrderSnapshot) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_position(&self, _position: &Position) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_position_snapshot(&self, _snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_order_book(&self, _order_book: &OrderBook) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_signal(&self, _signal: &Signal) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_custom_data(&self, _data: &CustomData) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_funding_rate(&self, _funding_rate: &FundingRateUpdate) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_greeks(&self, _greeks: &GreeksData) -> anyhow::Result<()> {
        Ok(())
    }

    fn add_yield_curve(&self, _yield_curve: &YieldCurveData) -> anyhow::Result<()> {
        Ok(())
    }

    fn delete_actor(&self, _actor_id: &ActorId) -> anyhow::Result<()> {
        Ok(())
    }

    fn delete_strategy(&self, _component_id: &StrategyId) -> anyhow::Result<()> {
        Ok(())
    }

    fn delete_order(&self, _client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        Ok(())
    }

    fn delete_position(&self, _position_id: &PositionId) -> anyhow::Result<()> {
        Ok(())
    }

    fn delete_account_event(&self, _account_id: &AccountId, _event_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn index_venue_order_id(
        &self,
        _client_order_id: ClientOrderId,
        _venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn index_order_position(
        &self,
        _client_order_id: ClientOrderId,
        _position_id: PositionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_actor(
        &self,
        _actor_id: &ActorId,
        _state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_strategy(
        &self,
        _strategy_id: &StrategyId,
        _state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_account(&self, _account: &AccountAny) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_order(&self, _order_event: &OrderEventAny) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_position(&self, _position: &Position) -> anyhow::Result<()> {
        Ok(())
    }

    fn snapshot_order_state(&self, _order: &OrderAny) -> anyhow::Result<()> {
        Ok(())
    }

    fn snapshot_position_state(
        &self,
        _position: &Position,
        _ts_snapshot: UnixNanos,
        _unrealized_pnl: Option<Money>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn heartbeat(&self, _timestamp: UnixNanos) -> anyhow::Result<()> {
        Ok(())
    }
}
