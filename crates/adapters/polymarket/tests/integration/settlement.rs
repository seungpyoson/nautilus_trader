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

//! Settlement correctness diagnostic for the bounded NT fork patch based on 55dd0a6.
//!
//! Adapted from the preserved reconciliation-enabled baseline diagnostic. Real Polymarket clients
//! run inside owned and hosted `LiveNode` modes against a loopback venue. After resolution the
//! venue still reports ten shares at an average entry price of 0.40; after redemption it reports
//! empty inventory. Settlement must occur once at resolution and remain unchanged by either
//! inventory report, with and without an external-order ownership claim.
//!
//! The test sets the position reconciliation grace period to zero so recent settlement activity
//! cannot hide a missing reconciliation freeze. Both phases wait for successive inventory queries:
//! the node serializes report tasks, so the next query follows handling of the preceding result.
//! Only public test credentials are used. Order submission endpoints reject and count requests.
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{Json, Response},
    routing::{get, post},
};
use nautilus_common::{
    actor::DataActor,
    enums::Environment,
    msgbus::{self, TypedHandler},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveExecutionEngineConfig, LiveNodeConfig},
    node::NodeRunMode,
};
use nautilus_model::{
    accounts::Account,
    data::InstrumentClose,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType},
    events::{OrderFilled, PositionClosed, PositionEvent},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId, TraderId,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_network::retry::RetryConfig;
use nautilus_polymarket::{
    common::consts::POLYMARKET_CLIENT_ID,
    config::{
        PolymarketDataClientConfig, PolymarketExecutionClientConfig,
        PolymarketInstrumentProviderConfig,
    },
    factories::{PolymarketDataClientFactory, PolymarketExecutionClientFactory},
    http::{gamma::PolymarketGammaHttpClient, query::GetGammaMarketsParams},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde_json::{Value, json};

#[derive(Clone)]
struct VenueState {
    market: Value,
    holding: Value,
    position_reads: Arc<AtomicUsize>,
    order_submissions: Arc<AtomicUsize>,
    flat: Arc<AtomicBool>,
}

async fn local_markets(State(state): State<VenueState>) -> Json<Value> {
    Json(json!({"markets": [state.market]}))
}

async fn local_positions(State(state): State<VenueState>) -> Json<Value> {
    state.position_reads.fetch_add(1, Ordering::SeqCst);
    if state.flat.load(Ordering::SeqCst) {
        Json(json!([]))
    } else {
        Json(json!([state.holding]))
    }
}

async fn local_empty_page() -> Json<Value> {
    Json(json!({"data": [], "next_cursor": "LTE="}))
}

async fn reject_order_submission(State(state): State<VenueState>) -> StatusCode {
    state.order_submissions.fetch_add(1, Ordering::SeqCst);
    StatusCode::METHOD_NOT_ALLOWED
}

async fn local_user_socket(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            match message {
                Message::Ping(payload) => {
                    socket.send(Message::Pong(payload)).await.unwrap();
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    })
}

async fn start_venue(
    winner: bool,
) -> (
    String,
    VenueState,
    tokio::task::JoinHandle<()>,
    InstrumentAny,
) {
    let mut market: Value =
        serde_json::from_str(include_str!("../../test_data/gamma_market.json")).unwrap();
    market["closed"] = json!(true);
    market["outcomePrices"] = json!(if winner {
        "[\"1\",\"0\"]"
    } else {
        "[\"0\",\"1\"]"
    });
    let condition_id = market["conditionId"].as_str().unwrap().to_string();
    let token_ids: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap()).unwrap();
    // Data API row shape consumed by the pinned adapter (`DataApiPosition`): asset, conditionId,
    // size, avgPrice. This is the pre-redemption inventory (phase A).
    let holding = json!({
        "asset": token_ids[0],
        "conditionId": condition_id,
        "size": 10,
        "avgPrice": 0.4,
    });
    let state = VenueState {
        market,
        holding,
        position_reads: Arc::new(AtomicUsize::new(0)),
        order_submissions: Arc::new(AtomicUsize::new(0)),
        flat: Arc::new(AtomicBool::new(false)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/markets/keyset", get(local_markets))
        .route("/positions", get(local_positions))
        .route("/data/orders", get(local_empty_page))
        .route("/data/trades", get(local_empty_page))
        .route("/order", post(reject_order_submission))
        .route("/orders", post(reject_order_submission))
        .route("/version", get(|| async { Json(json!({"version": 2})) }))
        .route(
            "/balance-allowance",
            get(|| async { Json(json!({"balance": "100000000", "allowances": {}})) }),
        )
        .route("/ws", get(local_user_socket))
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let gamma =
        PolymarketGammaHttpClient::new(Some(base_url.clone()), 5, RetryConfig::default()).unwrap();
    let parsed = gamma
        .request_instruments_by_params(GetGammaMarketsParams {
            condition_ids: Some(vec![
                state.market["conditionId"].as_str().unwrap().to_string(),
            ]),
            ..Default::default()
        })
        .await
        .unwrap();
    let instrument = parsed
        .into_iter()
        .find(|i| i.raw_symbol().as_str() == token_ids[0])
        .unwrap();
    assert!(
        instrument.expiration_ns().unwrap()
            < nautilus_core::time::get_atomic_clock_realtime().get_time_ns()
    );
    (base_url, state, task, instrument)
}

fn execution_config(base_url: &str) -> PolymarketExecutionClientConfig {
    PolymarketExecutionClientConfig {
        private_key: Some(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".into(),
        ),
        api_key: Some("00000000-0000-0000-0000-000000000001".into()),
        api_secret: Some("dGVzdF9zZWNyZXRfa2V5XzMyYnl0ZXNfcGFkMTIzNDU=".into()),
        passphrase: Some("test_pass".into()),
        base_url_http: Some(base_url.to_string()),
        base_url_data_api: Some(base_url.to_string()),
        base_url_ws: Some(format!("{}/ws", base_url.replacen("http:", "ws:", 1))),
        max_retries: 0,
        heartbeat_enabled: false,
        http_timeout_secs: 5,
        ..Default::default()
    }
}

#[derive(Debug, Default)]
struct NodeProbe {
    resolution_count: AtomicUsize,
    position_closed_count: AtomicUsize,
    close_price: std::sync::Mutex<Option<Price>>,
}

#[derive(Debug)]
struct ObserveRecon {
    core: StrategyCore,
    instrument_id: InstrumentId,
    probe: Arc<NodeProbe>,
}

impl DataActor for ObserveRecon {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_instrument_close(self.instrument_id, Some(*POLYMARKET_CLIENT_ID), None);
        Ok(())
    }

    fn on_instrument_close(&mut self, close: &InstrumentClose) -> anyhow::Result<()> {
        assert_eq!(close.instrument_id, self.instrument_id);
        *self.probe.close_price.lock().unwrap() = Some(close.close_price);
        self.probe.resolution_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

nautilus_strategy!(ObserveRecon, {
    fn on_position_closed(&mut self, _event: PositionClosed) {
        self.probe
            .position_closed_count
            .fetch_add(1, Ordering::SeqCst);
    }
});

async fn run_recon_case(case: &str, winner: bool, claim: bool, mode: NodeRunMode) {
    let (base_url, venue, server, instrument) = start_venue(winner).await;
    let trader_id = TraderId::from("TRADER-001");
    let strategy_id = StrategyId::from("REDEMPTION-001");
    let account_id = AccountId::from("POLYMARKET-001");
    let instrument_id = instrument.id();
    let position_id = PositionId::from(format!("{instrument_id}-{strategy_id}").as_str());
    let currency = Currency::pUSD();
    let opening_ns = UnixNanos::from(instrument.expiration_ns().unwrap().as_u64() - 60_000_000_000);
    let mut opening_fill = OrderFilled::new(
        trader_id,
        strategy_id,
        instrument_id,
        ClientOrderId::from("OPEN"),
        VenueOrderId::from("OPEN"),
        account_id,
        TradeId::from("OPEN"),
        OrderSide::Buy,
        OrderType::Limit,
        Quantity::from("10.00"),
        Price::from("0.40"),
        currency,
        LiquiditySide::Taker,
        UUID4::new(),
        opening_ns,
        opening_ns,
        false,
        None,
        Some(Money::new(0.10, currency)),
        None,
    );
    opening_fill.position_id = Some(position_id);
    let position = Position::new(&instrument, opening_fill);
    let ws_url = format!("{}/ws", base_url.replacen("http:", "ws:", 1));
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id,
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            generate_missing_orders: true,
            filter_position_reports: false,
            filter_unclaimed_external_orders: false,
            inflight_check_interval_ms: 2_000,
            open_check_interval_secs: Some(1.0),
            position_check_interval_secs: Some(1.0),
            position_check_threshold_ms: 0,
            ..Default::default()
        },
        timeout_reconciliation: Duration::from_secs(20),
        delay_post_stop: Duration::from_millis(50),
        ..Default::default()
    };
    let data_config = PolymarketDataClientConfig {
        instrument_config: Some(PolymarketInstrumentProviderConfig {
            load_ids: Some(vec![instrument_id]),
            ..Default::default()
        }),
        base_url_http: Some(base_url.clone()),
        base_url_gamma: Some(base_url.clone()),
        base_url_data_api: Some(base_url.clone()),
        base_url_ws: Some(ws_url.clone()),
        base_url_rtds: Some(ws_url),
        resolve_poll_enabled: false,
        auto_load_debounce_ms: 1,
        ..Default::default()
    };
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("ContractSettlementProof")
        .add_data_client(
            None,
            Box::new(PolymarketDataClientFactory),
            Box::new(data_config),
        )
        .unwrap()
        .add_exec_client(
            None,
            Box::new(PolymarketExecutionClientFactory),
            Box::new(execution_config(&base_url)),
        )
        .unwrap()
        .build()
        .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_position(&position, OmsType::Netting)
        .unwrap();
    node.kernel()
        .exec_engine
        .borrow_mut()
        .get_client_adapter_mut(&POLYMARKET_CLIENT_ID)
        .unwrap()
        .on_instrument(instrument);

    // Capture every position event on the bus (owner and EXTERNAL).
    let events = Rc::new(RefCell::new(Vec::<PositionEvent>::new()));
    let event_count = Arc::new(AtomicUsize::new(0));
    let captured = events.clone();
    let counter = event_count.clone();
    let handler = TypedHandler::from(move |event: &PositionEvent| {
        captured.borrow_mut().push(event.clone());
        counter.fetch_add(1, Ordering::SeqCst);
    });
    msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), None);

    let probe = Arc::new(NodeProbe::default());
    node.add_strategy(ObserveRecon {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            external_order_instrument_ids: if claim {
                Some(vec![instrument_id])
            } else {
                None
            },
            ..Default::default()
        }),
        instrument_id,
        probe: probe.clone(),
    })
    .unwrap();
    if claim {
        // The strategy-config field alone does not register a claim on this revision (the
        // `Strategy::external_order_instrument_ids` default returns None unless overridden), so the
        // countercheck registers the claim through the public LiveNode API, which is the same call
        // `add_strategy` makes when a strategy overrides that method.
        node.register_external_order_claims(strategy_id, &[instrument_id])
            .unwrap();
    }

    let stop_handle = node.handle();
    let monitor_probe = probe.clone();
    let reads = venue.position_reads.clone();
    let flat = venue.flat.clone();
    let monitor_events = event_count.clone();
    let monitor = tokio::spawn(async move {
        let resolution_seen = tokio::time::timeout(Duration::from_secs(8), async {
            while monitor_probe.resolution_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let reads_at_resolution = reads.load(Ordering::SeqCst);
        // The second query proves the first post-resolution report task completed
        let holding_read_after_resolution = tokio::time::timeout(Duration::from_secs(8), async {
            while reads.load(Ordering::SeqCst) < reads_at_resolution + 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let events_before_flat = monitor_events.load(Ordering::SeqCst);
        let reads_before_flat = reads.load(Ordering::SeqCst);
        // Phase B: venue inventory disappears (redemption).
        flat.store(true, Ordering::SeqCst);
        let flat_inventory_read = tokio::time::timeout(Duration::from_secs(8), async {
            while reads.load(Ordering::SeqCst) < reads_before_flat + 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let reads_total = reads.load(Ordering::SeqCst);
        stop_handle.stop();
        (
            resolution_seen,
            reads_at_resolution,
            holding_read_after_resolution,
            events_before_flat,
            reads_before_flat,
            flat_inventory_read,
            reads_total,
        )
    });
    let run = tokio::time::timeout(Duration::from_secs(40), node.run_with_mode(mode)).await;
    let observed = monitor.await.unwrap();
    let original = node
        .kernel()
        .cache
        .borrow()
        .position_owned(&position_id)
        .unwrap();
    let all_positions: Vec<Value> = node
        .kernel()
        .cache
        .borrow()
        .positions(None, Some(&instrument_id), None, None, None)
        .into_iter()
        .map(|p| {
            json!({
                "id": p.id.to_string(), "strategy": p.strategy_id.to_string(),
                "side": format!("{:?}", p.side), "signed_qty": p.signed_qty,
                "is_open": p.is_open(), "avg_px_open": p.avg_px_open,
                "avg_px_close": p.avg_px_close, "realized_pnl": p.realized_pnl.map(|m| m.to_string()),
            })
        })
        .collect();
    let orders: Vec<Value> = node
        .kernel()
        .cache
        .borrow()
        .orders(None, None, None, None, None)
        .into_iter()
        .map(|o| {
            json!({
                "client_order_id": o.client_order_id().to_string(),
                "strategy": o.strategy_id().to_string(),
                "side": format!("{:?}", o.order_side()),
                "status": format!("{:?}", o.status()),
                "qty": o.quantity().to_string(),
                "avg_px": o.avg_px(),
            })
        })
        .collect();
    let event_summary: Vec<Value> = events
        .borrow()
        .iter()
        .map(|e| {
            let (kind, position_id, strategy) = match e {
                PositionEvent::PositionOpened(x) => ("PositionOpened", x.position_id, x.strategy_id),
                PositionEvent::PositionChanged(x) => ("PositionChanged", x.position_id, x.strategy_id),
                PositionEvent::PositionClosed(x) => ("PositionClosed", x.position_id, x.strategy_id),
                PositionEvent::PositionAdjusted(x) => ("PositionAdjusted", x.position_id, x.strategy_id),
            };
            json!({"event": kind, "position_id": position_id.to_string(), "strategy": strategy.to_string()})
        })
        .collect();
    let pnl = node
        .kernel()
        .portfolio
        .borrow_mut()
        .realized_pnl(&instrument_id);
    let expected_pnl = if winner {
        Money::new(5.90, currency)
    } else {
        Money::new(-4.10, currency)
    };
    let balance = node
        .kernel()
        .cache
        .borrow()
        .account(&account_id)
        .unwrap()
        .balance_total(Some(currency));
    let order_submissions = venue.order_submissions.load(Ordering::SeqCst);
    println!(
        "{}",
        json!({
            "case": case, "node_mode": format!("{mode:?}"),
            "winner": winner, "claim_registered": claim,
            "reconciliation_enabled": true, "position_check_interval_secs": 1.0,
            "position_check_threshold_ms": 0,
            "resolution_seen": observed.0, "adapter_close_price": probe.close_price.lock().unwrap().map(|p| p.to_string()),
            "venue_reads_at_resolution": observed.1,
            "holding_still_reported_and_read_after_resolution": observed.2,
            "position_events_before_venue_flat": observed.3,
            "venue_reads_before_flat": observed.4,
            "flat_inventory_read_after_redemption": observed.5,
            "venue_reads_total": observed.6,
            "owner_position_closed_callbacks": probe.position_closed_count.load(Ordering::SeqCst),
            "original_open": original.is_open(),
            "positions": all_positions, "orders": orders, "position_events": event_summary,
            "realized_pnl": pnl.map(|p| p.to_string()), "expected_settlement_pnl": expected_pnl.to_string(),
            "venue_reported_balance": balance.map(|value| value.to_string()),
            "venue_order_submissions": order_submissions,
        })
    );
    server.abort();
    let _ = server.await;
    msgbus::unsubscribe_position_events("events.position.*".into(), &handler);
    run.expect("LiveNode deadline").expect("LiveNode run");
    // Correctness assertions: settle at resolution and retain that result across redemption
    assert!(observed.0, "resolution must reach the strategy");
    assert!(
        observed.2,
        "venue must have been read again while still reporting the holding"
    );
    assert_eq!(
        observed.3, 1,
        "owner closes once while the venue still reports the holding"
    );
    assert!(observed.5, "the venue must be read after redemption");
    assert!(!original.is_open(), "the owned position must remain closed");
    assert_eq!(probe.position_closed_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        event_summary.len(),
        1,
        "redemption must not create another position event"
    );
    assert_eq!(event_summary[0]["event"], "PositionClosed");
    assert_eq!(event_summary[0]["strategy"], strategy_id.to_string());
    assert!(
        all_positions
            .iter()
            .all(|position| position["is_open"] == false)
    );
    assert_eq!(
        orders.len(),
        1,
        "only the owner settlement order is generated"
    );
    assert_eq!(orders[0]["strategy"], strategy_id.to_string());
    assert_eq!(
        pnl,
        Some(expected_pnl),
        "payout minus original cost and opening commission"
    );
    assert_eq!(
        balance,
        Some(Money::new(100.0, currency)),
        "venue reports remain cash authority"
    );
    assert_eq!(order_submissions, 0, "settlement must not submit orders");
}

#[tokio::test(flavor = "current_thread")]
async fn owned_unclaimed_winner() {
    run_recon_case("owned_unclaimed_winner", true, false, NodeRunMode::Owned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn owned_claimed_winner() {
    run_recon_case("owned_claimed_winner", true, true, NodeRunMode::Owned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn owned_unclaimed_loser() {
    run_recon_case("owned_unclaimed_loser", false, false, NodeRunMode::Owned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn owned_claimed_loser() {
    run_recon_case("owned_claimed_loser", false, true, NodeRunMode::Owned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_unclaimed_winner() {
    run_recon_case("hosted_unclaimed_winner", true, false, NodeRunMode::Hosted).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_claimed_winner() {
    run_recon_case("hosted_claimed_winner", true, true, NodeRunMode::Hosted).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_unclaimed_loser() {
    run_recon_case("hosted_unclaimed_loser", false, false, NodeRunMode::Hosted).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_claimed_loser() {
    run_recon_case("hosted_claimed_loser", false, true, NodeRunMode::Hosted).await;
}
