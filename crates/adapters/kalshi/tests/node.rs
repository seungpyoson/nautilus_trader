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

//! Exercises the Kalshi factory through LiveNode, its runner, data engine and actor cache.
//! Both backends run sequentially because a node owns process-global logging state.

#![cfg(not(feature = "turmoil"))]

mod common;
#[path = "common/native.rs"]
mod native;

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, routing::get};
use native::{TICKER, config, factory};
use nautilus_common::{
    actor::{DataActor, DataActorCore, data_actor::DataActorConfig},
    enums::Environment,
    logging::logger::LoggerConfig,
    nautilus_actor,
};
use nautilus_live::node::{LiveNode, NodeState};
use nautilus_model::{
    data::OrderBookDeltas,
    enums::{BookAction, BookType, RecordFlag},
    identifiers::{ActorId, ClientId, InstrumentId, TraderId},
    instruments::InstrumentAny,
    types::Quantity,
};
use nautilus_network::websocket::TransportBackend;
use rstest::rstest;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    InitialSnapshot,
    Delta,
    Clear,
    Definition,
    ReplacementSnapshot,
    Stopping,
}

#[derive(Debug)]
struct BookObserver {
    core: DataActorCore,
    instrument_id: InstrumentId,
    phase: Phase,
    advance: tokio::sync::mpsc::UnboundedSender<()>,
    events: Rc<RefCell<Vec<&'static str>>>,
}

nautilus_actor!(BookObserver);

impl BookObserver {
    fn request_definition(&mut self) -> anyhow::Result<()> {
        self.request_instrument(
            self.instrument_id,
            None,
            None,
            Some(ClientId::from("KALSHI")),
            None,
        )?;
        Ok(())
    }
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions must fail the test because the actor runtime logs returned callback errors"
)]
impl DataActor for BookObserver {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let instrument = self.cache().instrument(&self.instrument_id).unwrap();
        assert_revision(&instrument, 0);
        self.events.borrow_mut().push("started");
        self.subscribe_book_deltas(
            self.instrument_id,
            BookType::L2_MBP,
            None,
            Some(ClientId::from("KALSHI")),
            true,
            None,
        );
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        assert_eq!(deltas.instrument_id, self.instrument_id);
        assert!(RecordFlag::F_LAST.matches(deltas.flags));
        let book = self.cache().order_book(&self.instrument_id).unwrap();

        match self.phase {
            Phase::InitialSnapshot => {
                assert!(RecordFlag::F_SNAPSHOT.matches(deltas.flags));
                assert_eq!(book.best_bid_size(), Some(Quantity::from("333.00")));
                self.events.borrow_mut().push("initial snapshot");
                self.phase = Phase::Delta;
                self.advance.send(())?;
            }
            Phase::Delta => {
                assert!(!RecordFlag::F_SNAPSHOT.matches(deltas.flags));
                assert_eq!(book.best_bid_size(), Some(Quantity::from("333.25")));
                self.events.borrow_mut().push("delta");
                self.phase = Phase::Clear;
                self.request_definition()?;
            }
            Phase::Clear => {
                assert_eq!(deltas.deltas.len(), 1);
                assert_eq!(deltas.deltas[0].action, BookAction::Clear);
                assert!(!RecordFlag::F_SNAPSHOT.matches(deltas.flags));
                assert!(!book.has_bid() && !book.has_ask());
                self.events.borrow_mut().push("clear");
                self.phase = Phase::Definition;
            }
            Phase::ReplacementSnapshot => {
                assert!(RecordFlag::F_SNAPSHOT.matches(deltas.flags));
                assert_eq!(book.best_bid_size(), Some(Quantity::from("10.00")));
                assert_revision(&self.cache().instrument(&self.instrument_id).unwrap(), 1);
                self.events.borrow_mut().push("replacement snapshot");
                self.phase = Phase::Stopping;
                self.request_definition()?;
            }
            phase => panic!("Unexpected book event during {phase:?}"),
        }
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        assert_eq!(self.phase, Phase::Definition);
        assert_revision(instrument, 1);
        assert_revision(&self.cache().instrument(&self.instrument_id).unwrap(), 1);
        let book = self.cache().order_book(&self.instrument_id).unwrap();
        assert!(!book.has_bid() && !book.has_ask());
        self.events.borrow_mut().push("definition");
        self.phase = Phase::ReplacementSnapshot;
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        assert_eq!(self.phase, Phase::Stopping);
        self.events.borrow_mut().push("stopped");
        Ok(())
    }
}

fn assert_revision(instrument: &InstrumentAny, revision: usize) {
    let InstrumentAny::BinaryOption(instrument) = instrument else {
        panic!("Expected a binary option");
    };
    let info = instrument.info.as_ref().unwrap();
    let raw: Value = serde_json::from_str(info["kalshi_market_json"].as_str().unwrap()).unwrap();
    assert_eq!(raw["title"], format!("Node definition {revision}"));
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn livenode_bootstrap_refresh_recovery_and_pending_request_shutdown() {
    for (index, backend) in [TransportBackend::Tungstenite, TransportBackend::Sockudo]
        .into_iter()
        .enumerate()
    {
        tokio::time::timeout(Duration::from_secs(20), run_node(index, backend))
            .await
            .unwrap_or_else(|e| panic!("LiveNode scenario timed out for {backend:?}: {e}"));
    }
}

async fn run_node(index: usize, backend: TransportBackend) {
    let requests = Arc::new(AtomicUsize::new(0));
    let (pending, mut pending_requests) = tokio::sync::mpsc::unbounded_channel();
    let app = {
        let requests = Arc::clone(&requests);
        Router::new().route(
            "/trade-api/v2/markets/{ticker}",
            get(move || {
                let requests = Arc::clone(&requests);
                let pending = pending.clone();
                async move {
                    let revision = requests.fetch_add(1, Ordering::SeqCst);
                    let mut market = serde_json::from_slice::<Value>(include_bytes!(
                        "../test_data/markets.json"
                    ))
                    .unwrap()["markets"][0]
                        .clone();
                    market["ticker"] = json!(TICKER);
                    market["title"] = json!(format!("Node definition {revision}"));
                    market["price_ranges"] = json!([{
                        "start":"0.0000", "end":"1.0000",
                        "step": if revision == 0 { "0.0100" } else { "0.0200" },
                    }]);

                    if revision == 2 {
                        let (resume, release) = tokio::sync::oneshot::channel::<()>();
                        pending.send(resume).unwrap();
                        release.await.expect("Test dropped the withheld response");
                    }
                    Json(json!({"market":market}))
                }
            }),
        )
    };
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = listener.local_addr().unwrap();
    let (advance, stages) = tokio::sync::mpsc::unbounded_channel();
    let ws_server = tokio::spawn(common::testing::serve_metadata_orderbooks(listener, stages));
    let mut config = config(
        format!("http://{http_address}/trade-api/v2"),
        format!("ws://{ws_address}/trade-api/ws/v2"),
        backend,
    );
    // A normal HTTP timeout must outlast the test so it cannot satisfy shutdown
    config.http.request_timeout_ms = 30_000.try_into().unwrap();
    config.http.operation_timeout_ms = 30_000.try_into().unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let instrument_id = InstrumentId::from("FED-23DEC-T3.00.KALSHI");
    let mut node = LiveNode::builder(
        TraderId::from(format!("TESTER-{index:03}")),
        Environment::Sandbox,
    )
    .unwrap()
    .with_name(format!("KALSHI-NODE-{index}"))
    .with_reconciliation(false)
    .with_timeout_connection(5)
    .with_timeout_disconnection_secs(2)
    .with_delay_post_stop_secs(0)
    .with_logging(LoggerConfig {
        bypass_logging: true,
        ..Default::default()
    })
    .add_data_client(None, Box::new(factory()), Box::new(config))
    .unwrap()
    .build()
    .unwrap();
    node.add_actor(BookObserver {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::from(format!("OBSERVER-{index}"))),
            log_events: false,
            log_commands: false,
        }),
        instrument_id,
        phase: Phase::InitialSnapshot,
        advance,
        events: Rc::clone(&events),
    })
    .unwrap();
    let handle = node.handle();
    let stop_handle = handle.clone();

    let stopper = tokio::spawn(async move {
        let response = pending_requests.recv().await.unwrap();
        assert!(stop_handle.is_running());
        stop_handle.stop();
        response
    });

    let result = tokio::time::timeout(Duration::from_secs(10), node.run()).await;
    assert!(
        result.is_ok(),
        "Node stalled for {backend:?}: state={:?}, events={:?}, requests={}",
        node.state(),
        events.borrow(),
        requests.load(Ordering::SeqCst),
    );
    result.unwrap().unwrap();
    let mut response = stopper.await.unwrap();
    assert_eq!(handle.state(), NodeState::Stopped);
    assert!(node.kernel().data_engine().check_disconnected());
    assert_eq!(
        *events.borrow(),
        [
            "started",
            "initial snapshot",
            "delta",
            "clear",
            "definition",
            "replacement snapshot",
            "stopped",
        ]
    );
    tokio::time::timeout(Duration::from_secs(2), ws_server)
        .await
        .expect("WebSocket fixture remained connected after node shutdown")
        .unwrap();

    // Canceling HTTP drops Axum's handler, so it cannot signal response completion
    tokio::time::timeout(Duration::from_secs(2), response.closed())
        .await
        .expect("Pending HTTP handler was not canceled after node shutdown");
    assert!(response.send(()).is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    assert_revision(
        node.kernel()
            .cache()
            .borrow()
            .instrument(&instrument_id)
            .unwrap(),
        1,
    );
    node.dispose();
    http_server.abort();
    let _ = http_server.await;
}
