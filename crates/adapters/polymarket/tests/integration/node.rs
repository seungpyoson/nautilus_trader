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

//! Full-`LiveNode` smoke tests for the Polymarket execution adapter.

use std::{
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::FutureExt;
use nautilus_common::{
    actor::DataActor,
    enums::Environment,
    msgbus::{
        self, MessagingSwitchboard,
        stubs::{get_any_saving_handler, get_typed_message_saving_handler},
    },
    testing::wait_until_async,
};
use nautilus_core::{UnixNanos, datetime::unix_nanos_to_iso8601, time::get_atomic_clock_realtime};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveExecutionEngineConfig, LiveNodeConfig},
    execution::submission::{SubmissionRecoveryExhausted, SubmissionRecoverySource},
    node::{LiveNode, NodeState},
};
use nautilus_model::{
    data::InstrumentClose,
    enums::OrderStatus,
    events::{
        OrderAccepted, OrderCanceled, OrderFillVoided, OrderFilled, OrderRejected, PositionEvent,
    },
    identifiers::{ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    instruments::Instrument,
    orders::Order,
    types::Quantity,
};
use nautilus_polymarket::{
    common::consts::POLYMARKET,
    config::{PolymarketDataClientConfig, PolymarketInstrumentProviderConfig},
    factories::{PolymarketDataClientFactory, PolymarketExecutionClientFactory},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::json;

use crate::{
    harness,
    mock_venue::{TestServerState, execution_config, load_json, start_mock_server},
};

const CLIENT_ORDER_ID: &str = "O-1";
const DEADLINE: Duration = Duration::from_secs(5);
const RUN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default)]
struct LifecycleProbe {
    accepted: Arc<AtomicBool>,
    rejected: Arc<AtomicBool>,
    canceled: Arc<AtomicBool>,
    filled: Arc<AtomicBool>,
    fill_voided: Arc<AtomicBool>,
}

#[derive(Debug)]
struct SubmitLimitOnStart {
    core: StrategyCore,
    instrument_id: InstrumentId,
    client_id: Option<ClientId>,
    cancel_on_accept: bool,
    probe: LifecycleProbe,
}

impl SubmitLimitOnStart {
    fn new(
        instrument_id: InstrumentId,
        client_id: ClientId,
        cancel_on_accept: bool,
        probe: LifecycleProbe,
    ) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(harness::STRATEGY_ID)),
                ..Default::default()
            }),
            instrument_id,
            client_id: Some(client_id),
            cancel_on_accept,
            probe,
        }
    }
}

impl DataActor for SubmitLimitOnStart {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.submit_order(
            harness::limit_order(self.instrument_id, CLIENT_ORDER_ID),
            None,
            self.client_id,
            None,
        )?;
        Ok(())
    }
}

nautilus_strategy!(SubmitLimitOnStart, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.probe.accepted.store(true, Ordering::Relaxed);

        if self.cancel_on_accept {
            self.cancel_order(event.client_order_id, self.client_id, None)
                .expect("cancel_order failed");
        }
    }

    fn on_order_rejected(&mut self, _event: OrderRejected) {
        self.probe.rejected.store(true, Ordering::Relaxed);
    }

    fn on_order_canceled(&mut self, _event: &OrderCanceled) {
        self.probe.canceled.store(true, Ordering::Relaxed);
    }

    fn on_order_filled(&mut self, _event: &OrderFilled) {
        self.probe.filled.store(true, Ordering::Relaxed);
    }

    fn on_order_fill_voided(&mut self, _event: &OrderFillVoided) {
        self.probe.fill_voided.store(true, Ordering::Relaxed);
    }
});

#[derive(Debug)]
struct SubmitOutcomePairOnStart {
    core: StrategyCore,
    instrument_ids: [InstrumentId; 2],
}

impl DataActor for SubmitOutcomePairOnStart {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for (index, instrument_id) in self.instrument_ids.into_iter().enumerate() {
            self.submit_order(
                harness::limit_order(instrument_id, &format!("LATE-{index}")),
                None,
                Some(ClientId::from(POLYMARKET)),
                None,
            )?;
        }

        Ok(())
    }
}

nautilus_strategy!(SubmitOutcomePairOnStart, {});

async fn start_accepting_mock() -> (SocketAddr, TestServerState) {
    let state = TestServerState::default();
    state.configure_default_order_success().await;
    let addr = start_mock_server(state.clone()).await;
    (addr, state)
}

fn build_node(name: &str, addr: SocketAddr) -> LiveNode {
    build_node_with_execution_config(
        name,
        addr,
        LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
    )
}

fn build_node_with_execution_config(
    name: &str,
    addr: SocketAddr,
    exec_engine: LiveExecutionEngineConfig,
) -> LiveNode {
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from(harness::TRADER_ID),
        exec_engine,
        delay_post_stop: Duration::from_millis(50),
        ..Default::default()
    };
    let node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name(name)
        .add_exec_client(
            None,
            Box::new(PolymarketExecutionClientFactory),
            Box::new(execution_config(addr)),
        )
        .unwrap()
        .build()
        .unwrap();
    let instrument = harness::instrument();
    node.kernel()
        .cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    node.kernel()
        .exec_engine
        .borrow_mut()
        .get_client_adapter_mut(&ClientId::from(POLYMARKET))
        .expect("Polymarket execution client")
        .on_instrument(instrument);
    node
}

#[rstest]
#[tokio::test]
#[expect(
    clippy::await_holding_refcell_ref,
    reason = "the node is stopped before the standalone report request"
)]
async fn startup_mass_status_loads_position_in_past_end_open_market() {
    let state = TestServerState::default();
    let market = load_json("gamma_market_past_end_date_open.json");
    let condition_id = market["conditionId"].as_str().unwrap();
    let tokens: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap()).unwrap();
    let instrument_id = InstrumentId::from(format!("{condition_id}-{}.POLYMARKET", tokens[0]));
    *state.positions_response_override.lock().await = Some(json!([{
        "token_id": tokens[0],
        "condition_id": condition_id,
        "current_size": "7.1234",
        "avg_price": "0.321",
    }]));
    *state.gamma_response.lock().await = Some(json!([market]));
    *state.orders_response_override.lock().await = Some(json!({"data": [], "next_cursor": "LTE="}));
    *state.trades_response_override.lock().await = Some(json!({"data": [], "next_cursor": "LTE="}));
    let addr = start_mock_server(state.clone()).await;
    let base_url = format!("http://{addr}");

    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from(harness::TRADER_ID),
        timeout_connection: DEADLINE,
        timeout_reconciliation: DEADLINE,
        delay_post_stop: Duration::from_millis(50),
        ..Default::default()
    };

    let data_config = PolymarketDataClientConfig {
        base_url_http: Some(base_url.clone()),
        base_url_gamma: Some(base_url.clone()),
        base_url_data_api: Some(base_url),
        base_url_ws: Some(format!("ws://{addr}/ws")),
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            load_ids: Some(vec![instrument_id]),
            ..Default::default()
        }),
        resolve_poll_enabled: false,
        ..Default::default()
    };

    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("PolymarketPastEndStartup")
        .add_data_client(
            None,
            Box::new(PolymarketDataClientFactory),
            Box::new(data_config),
        )
        .unwrap()
        .add_exec_client(
            None,
            Box::new(PolymarketExecutionClientFactory),
            Box::new(execution_config(addr)),
        )
        .unwrap()
        .build()
        .unwrap();

    let handle = node.handle();

    let driver = async move {
        wait_until_async(
            || async { matches!(handle.state(), NodeState::Running | NodeState::Stopped) },
            DEADLINE,
        )
        .await;
        let running = handle.state();
        handle.stop();
        running
    };

    let (result, running) =
        tokio::time::timeout(RUN_TIMEOUT, async { tokio::join!(node.run(), driver) })
            .await
            .unwrap();
    result.expect("startup mass status");
    let cached = node
        .kernel()
        .cache
        .borrow()
        .instrument(&instrument_id)
        .cloned()
        .unwrap();
    let mass_status = node
        .kernel()
        .exec_engine
        .borrow_mut()
        .generate_mass_status(&ClientId::from(POLYMARKET), None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(running, NodeState::Running);
    assert!(
        cached.expiration_ns().unwrap()
            < nautilus_core::time::get_atomic_clock_realtime().get_time_ns()
    );
    assert_eq!(mass_status.position_reports().len(), 1);
    let positions = &mass_status.position_reports()[&instrument_id];
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].instrument_id, instrument_id);
    assert_eq!(positions[0].quantity, Quantity::from("7.1234"));
    assert_eq!(
        positions[0].avg_px_open,
        Some(Decimal::from_str_exact("0.321").unwrap())
    );
    assert_eq!(state.orders_get_count.load(Ordering::Acquire), 2);
}

#[rstest]
#[tokio::test]
async fn stalled_submit_survives_inflight_exhaustion_and_accepts_late_response(
    #[values(false, true)] explicit_client: bool,
) {
    let (addr, state) = start_accepting_mock().await;
    state.order_response_gate.enable();
    let probe = LifecycleProbe::default();

    let mut node = build_node_with_execution_config(
        "PolymarketNodeStalledSubmit",
        addr,
        LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 10,
            inflight_check_threshold_ms: 20,
            inflight_check_retries: 1,
            ..Default::default()
        },
    );

    let mut strategy = SubmitLimitOnStart::new(
        InstrumentId::from(harness::INSTRUMENT_ID),
        ClientId::from(POLYMARKET),
        false,
        probe.clone(),
    );

    if !explicit_client {
        strategy.client_id = None;
    }

    node.add_strategy(strategy).unwrap();
    let (handler, diagnostics) = get_any_saving_handler::<SubmissionRecoveryExhausted>(None);
    let topic = MessagingSwitchboard::submission_recovery_exhausted_topic();
    msgbus::subscribe_any(topic.into(), handler.clone(), None);
    let handle = node.handle();
    let gate = state.order_response_gate.clone();
    let accepted = probe.accepted.clone();
    let rejected = probe.rejected.clone();

    let exhaustion = diagnostics.clone();

    let driver = async move {
        wait_until_async(|| async { gate.started() == 1 }, DEADLINE).await;
        wait_until_async(|| async { !exhaustion.get_messages().is_empty() }, DEADLINE).await;
        assert!(!rejected.load(Ordering::Relaxed));
        gate.release();
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        handle.stop();
    };

    let (result, ()) =
        tokio::time::timeout(RUN_TIMEOUT, async { tokio::join!(node.run(), driver) })
            .await
            .unwrap();
    result.unwrap();

    msgbus::unsubscribe_any(topic.into(), &handler);
    let diagnostics = diagnostics.get_messages();
    assert_eq!(diagnostics.len(), 1);
    let diagnostic = &diagnostics[0];
    assert_eq!(diagnostic.trader_id, TraderId::from(harness::TRADER_ID));
    assert_eq!(diagnostic.client_id, Some(ClientId::from(POLYMARKET)));
    assert_eq!(
        diagnostic.strategy_id,
        StrategyId::from(harness::STRATEGY_ID)
    );
    assert_eq!(
        diagnostic.instrument_id,
        InstrumentId::from(harness::INSTRUMENT_ID)
    );
    assert_eq!(
        diagnostic.client_order_id,
        ClientOrderId::from(CLIENT_ORDER_ID)
    );
    assert_eq!(diagnostic.source, SubmissionRecoverySource::Inflight);
    assert_eq!(diagnostic.retry_count, 1);

    let post_count = *state.order_post_count.lock().await;
    let open_order_ids = state.open_order_ids.lock().await.clone();
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Accepted);
    assert_eq!(
        order.venue_order_id(),
        Some(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID.into())
    );
    assert!(!probe.rejected.load(Ordering::Relaxed));
    assert_eq!(post_count, 1);
    assert_eq!(open_order_ids.len(), 1);
    assert!(open_order_ids.contains(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID));
}

fn add_submit_strategy(node: &mut LiveNode, cancel_on_accept: bool, probe: LifecycleProbe) {
    node.add_strategy(SubmitLimitOnStart::new(
        InstrumentId::from(harness::INSTRUMENT_ID),
        ClientId::from(POLYMARKET),
        cancel_on_accept,
        probe,
    ))
    .unwrap();
}

#[rstest]
#[tokio::test]
async fn inflight_query_resolves_signed_order_while_submit_response_is_stalled() {
    let (addr, state) = start_accepting_mock().await;
    state.order_response_gate.enable();
    state
        .order_response_uses_request_hash
        .store(true, Ordering::Release);
    let probe = LifecycleProbe::default();

    let mut node = build_node_with_execution_config(
        "PolymarketNodeQuerySubmit",
        addr,
        LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 10,
            inflight_check_threshold_ms: 50,
            inflight_check_retries: 5,
            ..Default::default()
        },
    );

    add_submit_strategy(&mut node, false, probe.clone());
    let handle = node.handle();
    let accepted = probe.accepted.clone();
    let driver_state = state.clone();

    let driver = tokio::spawn(async move {
        wait_until_async(
            || async { driver_state.order_response_gate.started() == 1 },
            DEADLINE,
        )
        .await;
        let venue_order_id = driver_state
            .open_order_ids
            .lock()
            .await
            .iter()
            .next()
            .cloned()
            .unwrap();
        let mut venue_order = load_json("http_open_order.json");
        venue_order["id"] = serde_json::Value::from(venue_order_id.clone());
        venue_order["asset_id"] = serde_json::Value::from(crate::mock_venue::TEST_TOKEN_ID);
        venue_order["market"] = serde_json::Value::from(crate::mock_venue::TEST_CONDITION_ID);
        venue_order["size_matched"] = serde_json::Value::from("0.0000");
        *driver_state.single_order_response.lock().await = Some(venue_order);
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        driver_state.order_response_gate.release();
        handle.stop();
        venue_order_id
    });

    tokio::time::timeout(RUN_TIMEOUT, node.run())
        .await
        .unwrap()
        .unwrap();
    let venue_order_id = driver.await.unwrap();
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Accepted);
    assert_eq!(order.venue_order_id(), Some(venue_order_id.as_str().into()));
    assert!(!probe.rejected.load(Ordering::Relaxed));
}

#[rstest]
#[tokio::test]
async fn node_boots_connects_and_stops_clean() {
    let (addr, state) = start_accepting_mock().await;
    let mut node = build_node("PolymarketNodeSmoke", addr);
    let handle = node.handle();
    let stop_handle = handle.clone();
    let socket_count = state.user_socket_count.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(
            || {
                let socket_count = socket_count.clone();
                async move { socket_count.load(Ordering::Acquire) == 1 }
            },
            DEADLINE,
        )
        .await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert_eq!(handle.state(), NodeState::Stopped);
    assert_eq!(
        state.startup_request_paths.lock().await.as_slice(),
        ["/version", "/ws", "/balance-allowance"],
    );
}

#[rstest]
#[tokio::test]
async fn submit_routes_through_node_to_accepted() {
    let (addr, _state) = start_accepting_mock().await;
    let probe = LifecycleProbe::default();
    let mut node = build_node("PolymarketNodeSubmit", addr);
    add_submit_strategy(&mut node, false, probe.clone());
    let handle = node.handle();
    let stop_handle = handle.clone();
    let accepted = probe.accepted.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert!(
        probe.accepted.load(Ordering::Relaxed),
        "strategy never received OrderAccepted",
    );
    assert_eq!(handle.state(), NodeState::Stopped);
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Accepted);
    assert_eq!(
        order.venue_order_id(),
        Some(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID.into()),
    );
}

#[rstest]
#[tokio::test]
async fn submit_venue_error_routes_through_node_to_rejected() {
    let (addr, state) = start_accepting_mock().await;
    *state.order_response.lock().await = Some(load_json("http_order_response_failed.json"));
    let probe = LifecycleProbe::default();
    let mut node = build_node("PolymarketNodeReject", addr);
    add_submit_strategy(&mut node, false, probe.clone());
    let handle = node.handle();
    let stop_handle = handle.clone();
    let rejected = probe.rejected.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(|| async { rejected.load(Ordering::Relaxed) }, DEADLINE).await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert!(
        probe.rejected.load(Ordering::Relaxed),
        "strategy never received OrderRejected",
    );
    assert_eq!(handle.state(), NodeState::Stopped);
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Rejected);
    assert_eq!(order.venue_order_id(), None);
}

#[rstest]
#[tokio::test]
async fn cancel_routes_through_node() {
    let (addr, state) = start_accepting_mock().await;
    let probe = LifecycleProbe::default();
    let mut node = build_node("PolymarketNodeCancel", addr);
    add_submit_strategy(&mut node, true, probe.clone());
    let handle = node.handle();
    let stop_handle = handle.clone();
    let accepted = probe.accepted.clone();
    let canceled = probe.canceled.clone();
    let driver_state = state.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        wait_until_async(
            || {
                let state = driver_state.clone();
                async move { *state.cancel_delete_count.lock().await == 1 }
            },
            DEADLINE,
        )
        .await;
        driver_state
            .feed_user("ws_user_order_cancellation.json")
            .await;
        wait_until_async(|| async { canceled.load(Ordering::Relaxed) }, DEADLINE).await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert_eq!(*state.cancel_delete_count.lock().await, 1);
    assert!(
        probe.canceled.load(Ordering::Relaxed),
        "strategy never received OrderCanceled",
    );
    assert_eq!(handle.state(), NodeState::Stopped);
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Canceled);
    assert_eq!(
        order.venue_order_id(),
        Some(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID.into()),
    );
}

#[rstest]
#[tokio::test]
async fn fill_routes_through_node_execution_manager() {
    let (addr, state) = start_accepting_mock().await;
    let probe = LifecycleProbe::default();
    let mut node = build_node("PolymarketNodeFill", addr);
    add_submit_strategy(&mut node, false, probe.clone());
    let handle = node.handle();
    let stop_handle = handle.clone();
    let accepted = probe.accepted.clone();
    let filled = probe.filled.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        state.feed_user("ws_user_trade_full.json").await;
        wait_until_async(|| async { filled.load(Ordering::Relaxed) }, DEADLINE).await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert!(
        probe.filled.load(Ordering::Relaxed),
        "strategy never received OrderFilled",
    );
    assert_eq!(handle.state(), NodeState::Stopped);
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Filled);
    assert_eq!(order.filled_qty().as_decimal(), Decimal::from(100));
    assert_eq!(
        order.venue_order_id(),
        Some(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID.into()),
    );
    let positions = cache.positions_open(
        None,
        Some(&InstrumentId::from(harness::INSTRUMENT_ID)),
        None,
        None,
        None,
    );
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, Quantity::from("100.0000"));
}

#[rstest]
#[case::ordinary(false)]
#[case::first_position_after_terminal(true)]
#[tokio::test]
async fn native_resolution_closes_both_outcomes_after_terminal(
    #[case] terminal_before_first_fill: bool,
) {
    let state = TestServerState::default();
    state.configure_default_order_success().await;
    state
        .order_response_uses_request_hash
        .store(true, Ordering::Release);
    *state.orders_response_override.lock().await = Some(json!({"data": [], "next_cursor": "LTE="}));
    *state.trades_response_override.lock().await = Some(json!({"data": [], "next_cursor": "LTE="}));
    let mut market = load_json("gamma_market.json");
    let condition_id = market["conditionId"].as_str().unwrap().to_string();
    let tokens: [String; 2] =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap()).unwrap();
    let outcomes: [String; 2] = serde_json::from_str(market["outcomes"].as_str().unwrap()).unwrap();
    let instrument_ids = tokens
        .each_ref()
        .map(|token| InstrumentId::from(format!("{condition_id}-{token}.POLYMARKET")));
    let addr = start_mock_server(state.clone()).await;
    let base_url = format!("http://{addr}");
    let data_config = PolymarketDataClientConfig {
        base_url_http: Some(base_url.clone()),
        base_url_gamma: Some(base_url.clone()),
        base_url_data_api: Some(base_url),
        base_url_ws: Some(format!("ws://{addr}/ws")),
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            load_ids: Some(instrument_ids.to_vec()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(data_config.resolve_poll_enabled);
    assert!(!data_config.subscribe_new_markets);
    let poll_bound = Duration::from_secs(data_config.resolve_poll_interval_secs) + DEADLINE;
    let grace = Duration::from_secs(data_config.resolve_poll_grace_secs);
    let expiration = UnixNanos::from(
        get_atomic_clock_realtime().get_time_ns().as_u64()
            - u64::try_from(grace.as_nanos()).unwrap(),
    );
    market["endDate"] = json!(unix_nanos_to_iso8601(expiration));
    *state.gamma_response.lock().await = Some(json!([market.clone()]));
    let mut resolved = market;
    resolved["closed"] = json!(true);
    resolved["acceptingOrders"] = json!(false);
    resolved["outcomePrices"] = json!("[\"1\",\"0\"]");

    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from(harness::TRADER_ID),
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let run_bound = poll_bound * (u32::try_from(instrument_ids.len()).unwrap() + 1)
        + RUN_TIMEOUT
        + config.delay_post_stop
        + config.timeout_disconnection;
    assert!(Duration::from_secs(data_config.resolve_poll_max_wait_secs) > grace + run_bound);
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("PolymarketLatePositionResolution")
        .add_data_client(
            None,
            Box::new(PolymarketDataClientFactory),
            Box::new(data_config),
        )
        .unwrap()
        .add_exec_client(
            None,
            Box::new(PolymarketExecutionClientFactory),
            Box::new(execution_config(addr)),
        )
        .unwrap()
        .build()
        .unwrap();
    node.add_strategy(SubmitOutcomePairOnStart {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(harness::STRATEGY_ID)),
            ..Default::default()
        }),
        instrument_ids,
    })
    .unwrap();
    let (close_handler, closes) = get_any_saving_handler::<InstrumentClose>(None);
    let close_topic = "data.close.POLYMARKET.*".into();
    msgbus::subscribe_instrument_close(close_topic, close_handler.clone(), Some(1));
    let (position_handler, positions) = get_typed_message_saving_handler::<PositionEvent>(None);
    let position_topic = "events.position.*".into();
    msgbus::subscribe_position_events(position_topic, position_handler.clone(), Some(1));
    let cache = node.kernel().cache();
    let handle = node.handle();

    let proof = async {
        wait_until_async(
            || async {
                instrument_ids.iter().enumerate().all(|(index, _)| {
                    cache
                        .borrow()
                        .order(&ClientOrderId::from(format!("LATE-{index}")))
                        .is_some_and(|order| order.status() == OrderStatus::Accepted)
                })
            },
            DEADLINE,
        )
        .await;
        assert!(positions.get_messages().is_empty());

        if terminal_before_first_fill {
            *state.gamma_response.lock().await = Some(json!([resolved.clone()]));
            wait_until_async(
                || async {
                    instrument_ids.iter().all(|id| {
                        cache
                            .borrow()
                            .instrument(id)
                            .and_then(|instrument| instrument.info())
                            .is_some_and(|info| {
                                info.get("closed").and_then(|v| v.as_bool()) == Some(true)
                            })
                    })
                },
                poll_bound,
            )
            .await;
            // This proves terminal discovery, not private metadata retirement completion.
            assert!(positions.get_messages().is_empty());
            assert!(closes.get_messages().is_empty());
        }

        for (index, instrument_id) in instrument_ids.iter().enumerate() {
            let order_id = ClientOrderId::from(format!("LATE-{index}"));
            let venue_order_id = cache
                .borrow()
                .order(&order_id)
                .unwrap()
                .venue_order_id()
                .unwrap();
            let mut fill = load_json("ws_user_trade_full.json");
            fill["event_type"] = json!("trade");
            fill["id"] = json!(format!("late-position-{index}"));
            fill["market"] = json!(condition_id);
            fill["asset_id"] = json!(tokens[index]);
            fill["outcome"] = json!(outcomes[index]);
            fill["taker_order_id"] = json!(venue_order_id.as_str());
            fill["maker_orders"][0]["asset_id"] = json!(tokens[index]);
            fill["maker_orders"][0]["outcome"] = json!(outcomes[index]);
            let clock = get_atomic_clock_realtime();
            fill["timestamp"] = json!(clock.get_time_ms().to_string());
            fill["match_time"] = json!(unix_nanos_to_iso8601(clock.get_time_ns()));
            fill["last_update"] = fill["match_time"].clone();
            state.send_user(fill.clone()).await;
            wait_until_async(
                || async {
                    positions.get_messages().iter().any(|event| {
                        matches!(event, PositionEvent::PositionOpened(open) if open.instrument_id == *instrument_id)
                    })
                },
                DEADLINE,
            )
            .await;

            if !terminal_before_first_fill && index == 0 {
                assert!(closes.get_messages().is_empty());
                assert_eq!(
                    cache
                        .borrow()
                        .positions_open(None, None, None, None, None)
                        .len(),
                    1,
                );
                *state.gamma_response.lock().await = Some(json!([resolved.clone()]));
            }

            wait_until_async(
                || async {
                    positions.get_messages().iter().any(|event| {
                        matches!(event, PositionEvent::PositionClosed(closed) if closed.instrument_id == *instrument_id)
                    })
                },
                poll_bound,
            )
            .await;
            let instrument_closes = closes
                .get_messages()
                .into_iter()
                .filter(|close| close.instrument_id == *instrument_id)
                .collect::<Vec<_>>();
            assert_eq!(instrument_closes.len(), 1);
            assert_eq!(
                instrument_closes[0].close_price.as_decimal(),
                [Decimal::ONE, Decimal::ZERO][index],
            );
            assert!(
                cache
                    .borrow()
                    .positions_open(None, None, None, None, None)
                    .is_empty(),
            );
            assert!(cache.borrow().instrument_close(instrument_id).is_some());

            if index == 0 {
                // This replay precedes the opposite fill on the same FIFO user socket.
                // Final event counts must stay exact; admission can defer the replay after close.
                state.send_user(fill).await;
            }
        }
    };
    let driver = async {
        let result = AssertUnwindSafe(proof).catch_unwind().await;
        handle.stop();
        result
    };
    let run = tokio::time::timeout(run_bound, async { tokio::join!(node.run(), driver) }).await;
    msgbus::unsubscribe_instrument_close(close_topic, &close_handler);
    msgbus::unsubscribe_position_events(position_topic, &position_handler);
    let (node_result, proof_result) =
        run.expect("native resolution run exceeded configured bounds");
    node_result.expect("native node shutdown");

    if let Err(panic) = proof_result {
        std::panic::resume_unwind(panic);
    }

    assert_eq!(handle.state(), NodeState::Stopped);
    assert_eq!(closes.get_messages().len(), instrument_ids.len());
    let events = positions.get_messages();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, PositionEvent::PositionOpened(_)))
            .count(),
        instrument_ids.len(),
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, PositionEvent::PositionClosed(_)))
            .count(),
        instrument_ids.len(),
    );
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty(),
    );
    assert_eq!(
        cache
            .borrow()
            .positions_closed(None, None, None, None, None)
            .len(),
        instrument_ids.len(),
    );
}

#[rstest]
#[tokio::test]
async fn failed_fill_routes_void_through_node_execution_manager() {
    let (addr, state) = start_accepting_mock().await;
    let probe = LifecycleProbe::default();
    let mut node = build_node("PolymarketNodeFillVoid", addr);
    add_submit_strategy(&mut node, false, probe.clone());
    let handle = node.handle();
    let stop_handle = handle.clone();
    let accepted = probe.accepted.clone();
    let filled = probe.filled.clone();
    let fill_voided = probe.fill_voided.clone();

    tokio::spawn(async move {
        wait_until_async(|| async { stop_handle.is_running() }, DEADLINE).await;
        wait_until_async(|| async { accepted.load(Ordering::Relaxed) }, DEADLINE).await;
        state.feed_user("ws_user_trade_full.json").await;
        wait_until_async(|| async { filled.load(Ordering::Relaxed) }, DEADLINE).await;
        let mut rest_failed = load_json("http_trade_report.json");
        rest_failed["id"] = serde_json::json!("trade-0xfull");
        rest_failed["status"] = serde_json::json!("FAILED");
        rest_failed["size"] = serde_json::json!("100.0000");
        rest_failed["taker_order_id"] =
            serde_json::json!(crate::mock_venue::DEFAULT_ACCEPTED_ORDER_ID);
        *state.trades_response_override.lock().await = Some(serde_json::json!({
            "data": [rest_failed],
            "next_cursor": "LTE="
        }));
        state.feed_user("ws_user_trade_full_failed.json").await;
        wait_until_async(|| async { fill_voided.load(Ordering::Relaxed) }, DEADLINE).await;
        stop_handle.stop();
    });

    let result = tokio::time::timeout(RUN_TIMEOUT, node.run()).await;

    assert!(result.is_ok(), "node.run() did not complete within timeout");
    assert!(result.unwrap().is_ok(), "node.run() returned an error");
    assert!(
        probe.filled.load(Ordering::Relaxed),
        "strategy never received OrderFilled",
    );
    assert!(
        probe.fill_voided.load(Ordering::Relaxed),
        "strategy never received OrderFillVoided",
    );
    assert_eq!(handle.state(), NodeState::Stopped);
    let cache = node.kernel().cache.borrow();
    let order = cache.order(&ClientOrderId::from(CLIENT_ORDER_ID)).unwrap();
    assert_eq!(order.status(), OrderStatus::Voided);
    assert_eq!(order.filled_qty().as_decimal(), Decimal::ZERO);
    assert_eq!(order.voided_qty().as_decimal(), Decimal::from(100));
    assert!(
        cache
            .positions_open(
                None,
                Some(&InstrumentId::from(harness::INSTRUMENT_ID)),
                None,
                None,
                None,
            )
            .is_empty(),
        "voided fill left an open position",
    );
}
