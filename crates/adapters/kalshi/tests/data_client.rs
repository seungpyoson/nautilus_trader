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

//! Native factory and DataClient lifecycle over OS sockets, with generated credentials.
//! The public capture and schema examples are mutated for a correlated, fixed market.
//! Turmoil covers the transport separately; NT's global task runtime uses OS sockets here.

#![cfg(not(feature = "turmoil"))]

mod common;
#[path = "common/engine.rs"]
mod engine;
use engine::EngineReceiver;
#[path = "common/native.rs"]
mod native;

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, extract::Path, routing::get};
use futures_util::{SinkExt, StreamExt};
use native::{TICKER, config, factory};
use nautilus_common::{
    cache::{Cache, CacheView},
    clients::DataClient,
    clock::TestClock,
    factories::DataClientFactory,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{
            DataResponse, RequestInstrument, RequestInstruments, SubscribeBookDeltas,
            UnsubscribeBookDeltas,
        },
    },
};
use nautilus_core::Params;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    enums::{BookAction, BookType, RecordFlag},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::InstrumentAny,
    orderbook::OrderBook,
    types::Quantity,
};
use nautilus_network::websocket::TransportBackend;
use rstest::rstest;
use serde_json::{Value, json};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn subscription_budget_preserves_recovery_and_intent(
    #[case] backend: TransportBackend,
    #[values(6, 7, 8)] count: usize,
) {
    tokio::time::timeout(
        Duration::from_secs(10),
        run_subscription_budget(backend, count),
    )
    .await
    .unwrap();
}

async fn run_subscription_budget(backend: TransportBackend, count: usize) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/trade-api/v2/markets/{ticker}",
        get(|| async {
            let mut market =
                serde_json::from_slice::<Value>(include_bytes!("../test_data/markets.json"))
                    .unwrap()["markets"][0]
                    .clone();
            market["ticker"] = json!(native::TICKER);
            Json(json!({"market":market}))
        }),
    );

    let http_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = connections.clone();

    let ws_server = tokio::spawn(async move {
        let mut workers = tokio::task::JoinSet::new();

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);

            workers.spawn(async move {
     let mut websocket=accept_async(stream).await.unwrap();
     let request:Value=serde_json::from_str(&websocket.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
     let ack=json!({"type":"subscribed","id":request["id"],"msg":{"channel":"orderbook_delta","sid":2}});
     websocket.send(Message::text(ack.to_string())).await.unwrap();
     let mut snapshot:Value=serde_json::from_str(include_str!("../test_data/ws_orderbook_snapshot.json")).unwrap();
     snapshot["msg"]["no_dollars_fp"][0][0]=json!("0.4600");
     snapshot["msg"]["no_dollars_fp"][1][0]=json!("0.4400");
     websocket.send(Message::text(snapshot.to_string())).await.unwrap();

     while let Some(message)=websocket.next().await {
      match message {Ok(Message::Close(_))=>{let _=websocket.flush().await;break},Err(_)=>break,_=>{}}
     }
    });
        }
    });
    let cache = Rc::new(RefCell::new(Cache::default()));
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut engine = EngineReceiver::new(receiver, cache.clone());
    replace_data_event_sender(sender);
    let mut config = native::config(
        format!("http://{http_address}/trade-api/v2"),
        format!("ws://{ws_address}/trade-api/ws/v2"),
        backend,
    );
    config.websocket.max_pending_frames = 4.try_into().unwrap();
    config.websocket.transport.reconnect_max_attempts = None;
    let mut client = native::factory()
        .create(
            "KALSHI",
            &config,
            CacheView::new(cache.clone()),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .unwrap();
    client.start().unwrap();
    client.connect().await.unwrap();
    engine.flush();
    assert!(client.is_connected(), "initial bootstrap confirmed");
    let id = InstrumentId::from("FED-23DEC-T3.00.KALSHI");

    for i in 0..count {
        if i.is_multiple_of(2) {
            client.subscribe_book_deltas(subscription(id)).unwrap();
        } else {
            client
                .unsubscribe_book_deltas(&UnsubscribeBookDeltas {
                    instrument_id: id,
                    client_id: Some(ClientId::from("KALSHI")),
                    venue: Some(Venue::new("KALSHI")),
                    command_id: Default::default(),
                    ts_init: 1.into(),
                    correlation_id: None,
                    params: None,
                })
                .unwrap();
        }
    }

    // Leave the production engine receiver paused while the background stream processes commands
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ready_before_drain = client.is_connected();
    engine.flush();
    tokio::time::sleep(Duration::from_millis(400)).await;
    engine.flush();

    let request = RequestInstruments::new(
        None,
        None,
        Some(ClientId::from("KALSHI")),
        Some(Venue::new("KALSHI")),
        Default::default(),
        1.into(),
        None,
    );

    let mut snapshots = 0;
    client.request_instruments(request.clone()).unwrap();

    loop {
        let event = engine.recv().await.unwrap();

        if let DataEvent::Data(Data::Deltas(deltas)) = &event
            && deltas.deltas.len() > 1
        {
            snapshots += 1;
        }

        if let DataEvent::Response(DataResponse::Instruments(response)) = event {
            assert_eq!(response.correlation_id, request.request_id);
            break;
        }
    }

    while !client.is_connected() {
        engine.flush();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    engine.flush();

    while let Ok(event) = engine.try_recv() {
        if let DataEvent::Data(Data::Deltas(deltas)) = event
            && deltas.deltas.len() > 1
        {
            snapshots += 1;
        }
    }

    if count == 6 {
        assert!(ready_before_drain);
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    } else {
        assert!(!ready_before_drain);
        assert!(
            connections.load(Ordering::SeqCst) > 1,
            "Overflow must reconnect"
        );
    }

    // Recovered publication must retain the final subscribe or unsubscribe intent
    if !count.is_multiple_of(2) {
        assert!(
            snapshots > 0,
            "Subscribed recovery must publish a fresh snapshot"
        );
        client
            .unsubscribe_book_deltas(&UnsubscribeBookDeltas {
                instrument_id: id,
                client_id: Some(ClientId::from("KALSHI")),
                venue: Some(Venue::new("KALSHI")),
                command_id: Default::default(),
                ts_init: 1.into(),
                correlation_id: None,
                params: None,
            })
            .unwrap();
    } else if count > 6 {
        assert_eq!(
            snapshots, 0,
            "Unsubscribed recovery must not publish snapshots"
        );
    }
    client.subscribe_book_deltas(subscription(id)).unwrap();
    let replay = next_deltas(&mut engine).await;
    assert!(
        replay.deltas.len() > 1,
        "Late subscribe must replay the recovered engine book"
    );
    assert!(client.is_connected());
    let recovered_connections = connections.load(Ordering::SeqCst);
    drop(engine);

    while client.request_instruments(request.clone()).is_ok() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!client.is_connected());
    assert_eq!(connections.load(Ordering::SeqCst), recovered_connections);
    client.disconnect().await.unwrap();
    drop(client);
    http_server.abort();
    let _ = http_server.await;
    ws_server.abort();
    let _ = ws_server.await;
}

fn subscription(id: InstrumentId) -> SubscribeBookDeltas {
    SubscribeBookDeltas {
        instrument_id: id,
        book_type: BookType::L2_MBP,
        client_id: Some(ClientId::from("KALSHI")),
        venue: Some(Venue::new("KALSHI")),
        command_id: Default::default(),
        ts_init: 1.into(),
        depth: None,
        managed: true,
        correlation_id: None,
        params: None,
    }
}

async fn connect_observing_instruments(
    client: &mut dyn DataClient,
    receiver: &mut EngineReceiver,
    cache: &Rc<RefCell<Cache>>,
) {
    // LiveNode queues definitions and snapshots, then the engine confirms book readiness
    client.connect().await.unwrap();
    assert!(!client.is_connected());
    let event = receiver.recv().await.unwrap();
    let DataEvent::Instrument(instrument) = event else {
        panic!("Expected bootstrap instrument");
    };
    cache.borrow_mut().add_instrument(instrument).unwrap();
    receiver.flush();
    assert!(client.is_connected());
}

async fn next_deltas(receiver: &mut EngineReceiver) -> OrderBookDeltas {
    match receiver.recv().await.unwrap() {
        DataEvent::Data(Data::Deltas(deltas)) => (*deltas).clone(),
        event => panic!("Unexpected data event: {event:?}"),
    }
}

#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn native_factory_bootstrap_recovery_reset_and_drop(#[case] backend: TransportBackend) {
    tokio::time::timeout(Duration::from_secs(20), run_lifecycle(backend))
        .await
        .unwrap();
}

async fn run_lifecycle(backend: TransportBackend) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let app = Router::new().route(
        "/trade-api/v2/markets/{ticker}",
        get(move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let mut market =
                    serde_json::from_slice::<Value>(include_bytes!("../test_data/markets.json"))
                        .unwrap()["markets"][0]
                        .clone();
                market["ticker"] = json!(TICKER);
                Json(json!({"market":market}))
            }
        }),
    );

    let http_server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = listener.local_addr().unwrap();
    let (advance, mut stages) = tokio::sync::mpsc::unbounded_channel::<()>();

    let ws_server = tokio::spawn(async move {
        for connection in 1..=3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            let request = websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            let request: Value = serde_json::from_str(&request).unwrap();
            assert_eq!(request["params"]["market_tickers"], json!([TICKER]));
            assert_eq!(request["params"]["use_yes_price"], true);
            assert_eq!(request["id"], if connection == 2 { 2 } else { 1 });
            let mut ack: Value =
                serde_json::from_str(include_str!("../test_data/ws_subscribed.json")).unwrap();
            ack["id"] = request["id"].clone();
            ack["msg"]["sid"] = json!(2);
            websocket
                .send(Message::text(ack.to_string()))
                .await
                .unwrap();
            let mut snapshot: Value =
                serde_json::from_str(include_str!("../test_data/ws_orderbook_snapshot.json"))
                    .unwrap();
            snapshot["msg"]["no_dollars_fp"][0][0] = json!("0.4600");
            snapshot["msg"]["no_dollars_fp"][1][0] = json!("0.4400");

            if connection > 1 {
                snapshot["msg"]["yes_dollars_fp"][1][1] = json!("8.50");
            }
            websocket
                .send(Message::text(snapshot.to_string()))
                .await
                .unwrap();

            if connection <= 2 {
                stages.recv().await.unwrap();
                let mut delta: Value =
                    serde_json::from_str(include_str!("../test_data/ws_orderbook_delta.json"))
                        .unwrap();
                delta["msg"]["price_dollars"] = json!("0.2200");
                delta["msg"]["delta_fp"] = json!("0.25");
                websocket
                    .send(Message::text(delta.to_string()))
                    .await
                    .unwrap();

                if connection == 1 {
                    stages.recv().await.unwrap();
                    delta["seq"] = json!(4);
                    delta["msg"]["delta_fp"] = json!("-999.00");
                    websocket
                        .send(Message::text(delta.to_string()))
                        .await
                        .unwrap();

                    // Keep the transport open until the native validator has cleared the book
                    stages.recv().await.unwrap();
                    continue;
                }
            }

            while let Some(message) = websocket.next().await {
                match message {
                    Ok(Message::Close(_)) => {
                        let _ = websocket.flush().await;
                        break;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    });
    let cache = Rc::new(RefCell::new(Cache::default()));
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = EngineReceiver::new(receiver, Rc::clone(&cache));
    replace_data_event_sender(sender);
    let config = config(
        format!("http://{http_address}/trade-api/v2"),
        format!("ws://{ws_address}/trade-api/ws/v2"),
        backend,
    );
    let mut client = factory()
        .create(
            "KALSHI",
            &config,
            CacheView::new(Rc::clone(&cache)),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .unwrap();
    client.start().unwrap();
    connect_observing_instruments(client.as_mut(), &mut receiver, &cache).await;
    assert!(client.is_connected());
    let id = InstrumentId::from("FED-23DEC-T3.00.KALSHI");
    let mut downstream = OrderBook::new(id, BookType::L2_MBP);
    let mut wrong = subscription(id);
    wrong.book_type = BookType::L3_MBO;
    assert!(client.subscribe_book_deltas(wrong).is_err());
    assert!(
        client
            .subscribe_book_deltas(subscription(InstrumentId::from("OTHER.KALSHI")))
            .is_err()
    );
    client.subscribe_book_deltas(subscription(id)).unwrap();
    client.subscribe_book_deltas(subscription(id)).unwrap();
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.00")));
    advance.send(()).unwrap();
    let update = next_deltas(&mut receiver).await;
    assert_eq!(update.deltas[0].action, BookAction::Update);
    downstream.apply_deltas(&update).unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.25")));
    advance.send(()).unwrap();
    let clear = next_deltas(&mut receiver).await;
    assert_eq!(clear.flags, RecordFlag::F_LAST as u8);
    assert_eq!(clear.deltas[0].action, BookAction::Clear);
    downstream.apply_deltas(&clear).unwrap();
    assert!(!downstream.has_bid());
    advance.send(()).unwrap();
    let recovered = next_deltas(&mut receiver).await;
    assert!(RecordFlag::F_SNAPSHOT.matches(recovered.flags));
    downstream.apply_deltas(&recovered).unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("8.50")));

    client
        .unsubscribe_book_deltas(&UnsubscribeBookDeltas {
            instrument_id: id,
            client_id: Some(ClientId::from("KALSHI")),
            venue: Some(Venue::new("KALSHI")),
            command_id: Default::default(),
            ts_init: 1.into(),
            correlation_id: None,
            params: None,
        })
        .unwrap();
    advance.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.recv())
            .await
            .is_err()
    );
    client.subscribe_book_deltas(subscription(id)).unwrap();
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("8.75")));

    client.stop().unwrap();
    assert!(client.is_disconnected());
    client.disconnect().await.unwrap();
    client.disconnect().await.unwrap();
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert!(!downstream.has_bid());
    client.reset().unwrap();
    client.start().unwrap();
    connect_observing_instruments(client.as_mut(), &mut receiver, &cache).await;
    client.subscribe_book_deltas(subscription(id)).unwrap();
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("8.50")));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    drop(client);
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert!(!downstream.has_bid());
    assert!(!downstream.has_ask());
    ws_server.await.unwrap();
    http_server.abort();
    let _ = http_server.await;
}

#[rstest]
fn factory_rejects_invalid_selection_and_commands_without_network_work() {
    let factory = factory();
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut config = config(
        "http://127.0.0.1:1/trade-api/v2".into(),
        "ws://127.0.0.1:1/trade-api/ws/v2".into(),
        TransportBackend::Tungstenite,
    );
    config.market_tickers.push(TICKER.into());
    assert!(
        factory
            .create(
                "KALSHI",
                &config,
                CacheView::new(Rc::clone(&cache)),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .is_err()
    );
    config.market_tickers.pop();
    let mut client = factory
        .create(
            "KALSHI",
            &config,
            CacheView::new(cache),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .unwrap();
    assert!(client.is_disconnected());
    assert!(
        client
            .subscribe_book_deltas(subscription(InstrumentId::from("FED-23DEC-T3.00.KALSHI")))
            .is_err()
    );
    client.stop().unwrap();
    client.stop().unwrap();
    client.dispose().unwrap();
    assert!(client.is_disconnected());
}

#[derive(Clone, Copy, Debug)]
enum RequestShutdown {
    InvalidDefinition,
    Deadline,
    Stop,
    Drop,
}

#[rstest]
#[case::tungstenite_invalid(TransportBackend::Tungstenite, RequestShutdown::InvalidDefinition)]
#[case::sockudo_invalid(TransportBackend::Sockudo, RequestShutdown::InvalidDefinition)]
#[case::tungstenite_deadline(TransportBackend::Tungstenite, RequestShutdown::Deadline)]
#[case::sockudo_deadline(TransportBackend::Sockudo, RequestShutdown::Deadline)]
#[case::tungstenite_stop(TransportBackend::Tungstenite, RequestShutdown::Stop)]
#[case::sockudo_stop(TransportBackend::Sockudo, RequestShutdown::Stop)]
#[case::tungstenite_drop(TransportBackend::Tungstenite, RequestShutdown::Drop)]
#[case::sockudo_drop(TransportBackend::Sockudo, RequestShutdown::Drop)]
#[tokio::test]
async fn fresh_metadata_requests_share_book_ownership_and_cancellation(
    #[case] backend: TransportBackend,
    #[case] shutdown: RequestShutdown,
) {
    tokio::time::timeout(Duration::from_secs(20), run_requests(backend, shutdown))
        .await
        .unwrap();
}

async fn run_requests(backend: TransportBackend, shutdown: RequestShutdown) {
    let revision = Arc::new(AtomicUsize::new(0));
    let block = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let (arrived, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let app = {
        let revision = Arc::clone(&revision);
        let block = Arc::clone(&block);
        let release = Arc::clone(&release);
        let count = Arc::clone(&count);
        Router::new().route(
            "/trade-api/v2/markets/{ticker}",
            get(move || {
                let revision = Arc::clone(&revision);
                let block = Arc::clone(&block);
                let release = Arc::clone(&release);
                let count = Arc::clone(&count);
                let arrived = arrived.clone();
                async move {
                    let revision = revision.load(Ordering::SeqCst);
                    let mut market = serde_json::from_slice::<Value>(include_bytes!(
                        "../test_data/markets.json"
                    ))
                    .unwrap()["markets"][0]
                        .clone();
                    market["ticker"] = json!(TICKER);
                    market["title"] = json!(format!("Definition {revision}"));
                    market["price_ranges"] = json!([{
                        "start":"0.0000", "end":"1.0000",
                        "step": match revision { 0 | 1 => "0.0100", 2 => "0.0200", _ => "0.0370" },
                    }]);
                    let seen = count.fetch_add(1, Ordering::SeqCst) + 1;
                    arrived.send(seen).unwrap();

                    if block.load(Ordering::SeqCst) {
                        release.notified().await;
                    }
                    Json(json!({"market":market}))
                }
            }),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = listener.local_addr().unwrap();

    let http_server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = listener.local_addr().unwrap();
    let (advance, stages) = tokio::sync::mpsc::unbounded_channel();

    let ws_server = tokio::spawn(common::testing::serve_metadata_orderbooks(listener, stages));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = EngineReceiver::new(receiver, Rc::clone(&cache));
    replace_data_event_sender(sender);
    let config = config(
        format!("http://{http_address}/trade-api/v2"),
        format!("ws://{ws_address}/trade-api/ws/v2"),
        backend,
    );
    let mut client = factory()
        .create(
            "KALSHI",
            &config,
            CacheView::new(Rc::clone(&cache)),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .unwrap();
    client.start().unwrap();
    connect_observing_instruments(client.as_mut(), &mut receiver, &cache).await;
    assert_eq!(arrivals.recv().await, Some(1));
    let id = InstrumentId::from("FED-23DEC-T3.00.KALSHI");
    client.subscribe_book_deltas(subscription(id)).unwrap();
    let mut downstream = OrderBook::new(id, BookType::L2_MBP);
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    let mut params = Params::new();
    params.insert("force_instrument_update".into(), json!(true));
    params.insert("update_catalog".into(), json!(false));
    params.insert("only_last".into(), json!(true));
    let first = RequestInstrument::new(
        id,
        None,
        None,
        None,
        Default::default(),
        1.into(),
        Some(params.clone()),
    );
    let second = RequestInstruments::new(
        None,
        None,
        Some(ClientId::from("KALSHI")),
        Some(Venue::new("KALSHI")),
        Default::default(),
        2.into(),
        Some(params.clone()),
    );

    revision.store(1, Ordering::SeqCst);
    block.store(true, Ordering::SeqCst);
    client.request_instrument(first.clone()).unwrap();
    assert_eq!(arrivals.recv().await, Some(2));

    // A pending metadata response must not cancel the HTTP future or stall book publication
    advance.send(()).unwrap();
    downstream
        .apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.25")));
    revision.store(2, Ordering::SeqCst);
    client.request_instruments(second.clone()).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    block.store(false, Ordering::SeqCst);
    release.notify_one();
    let DataEvent::Instrument(definition) = receiver.recv().await.unwrap() else {
        panic!("Expected first refreshed definition before its response");
    };
    assert_definition_revision(&definition, 1);
    let DataEvent::Response(DataResponse::Instrument(response)) = receiver.recv().await.unwrap()
    else {
        panic!("Expected single instrument response");
    };
    assert_eq!(response.correlation_id, first.request_id);
    assert_eq!(response.client_id, ClientId::from("KALSHI"));
    assert_eq!(response.instrument_id, id);
    assert_eq!(response.params, Some(params.clone()));
    assert!(response.start.is_none() && response.end.is_none());
    assert_definition_revision(&response.data, 1);
    cache.borrow_mut().add_instrument(response.data).unwrap();

    // The second request must fetch again, then clear old liquidity before the changed definition
    assert_eq!(arrivals.recv().await, Some(3));
    let clear = next_deltas(&mut receiver).await;
    assert_eq!(clear.deltas[0].action, BookAction::Clear);
    assert_eq!(clear.flags, RecordFlag::F_LAST as u8);
    downstream.apply_deltas(&clear).unwrap();
    assert!(!downstream.has_bid());
    assert!(!client.is_connected());
    let DataEvent::Instrument(definition) = receiver.recv().await.unwrap() else {
        panic!("Expected changed definition after clear");
    };
    assert_definition_revision(&definition, 2);
    let DataEvent::Response(DataResponse::Instruments(response)) = receiver.recv().await.unwrap()
    else {
        panic!("Expected instrument selection response");
    };
    assert_eq!(response.correlation_id, second.request_id);
    assert_eq!(response.client_id, ClientId::from("KALSHI"));
    assert_eq!(response.venue, Venue::new("KALSHI"));
    assert_eq!(response.params, Some(params));
    assert!(response.start.is_none() && response.end.is_none());
    assert_eq!(response.data.len(), 1);
    assert_definition_revision(&response.data[0], 2);
    let snapshot = next_deltas(&mut receiver).await;
    assert!(RecordFlag::F_SNAPSHOT.matches(snapshot.flags));
    downstream.apply_deltas(&snapshot).unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("10.00")));
    assert!(client.is_connected());
    assert_eq!(cache.borrow().instrument(&id), Some(&definition));

    revision.store(3, Ordering::SeqCst);
    block.store(
        !matches!(shutdown, RequestShutdown::InvalidDefinition),
        Ordering::SeqCst,
    );
    client.request_instrument(first.clone()).unwrap();
    assert_eq!(arrivals.recv().await, Some(4));

    match shutdown {
        RequestShutdown::InvalidDefinition | RequestShutdown::Deadline => {
            if matches!(shutdown, RequestShutdown::Deadline) {
                // Force the pending GET past its one-second attempt budget.
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
            revision.store(2, Ordering::SeqCst);
            block.store(false, Ordering::SeqCst);
            release.notify_one();
            client.request_instrument(first.clone()).unwrap();
            assert_eq!(arrivals.recv().await, Some(5));
            let DataEvent::Instrument(definition) = receiver.recv().await.unwrap() else {
                panic!("Rejected refresh must preserve the last validated definition and book");
            };
            assert_definition_revision(&definition, 2);
            let DataEvent::Response(DataResponse::Instrument(response)) =
                receiver.recv().await.unwrap()
            else {
                panic!("The next request must succeed after a refresh failure");
            };
            assert_eq!(response.correlation_id, first.request_id);
            assert_definition_revision(&response.data, 2);
            cache.borrow_mut().add_instrument(response.data).unwrap();
            assert!(client.is_connected());
            client.disconnect().await.unwrap();
            downstream
                .apply_deltas(&next_deltas(&mut receiver).await)
                .unwrap();
        }
        RequestShutdown::Stop => {
            client.stop().unwrap();
            client.disconnect().await.unwrap();
            assert!(client.request_instrument(first).is_err());
            downstream
                .apply_deltas(&next_deltas(&mut receiver).await)
                .unwrap();
        }
        RequestShutdown::Drop => {
            drop(client);
            downstream
                .apply_deltas(&next_deltas(&mut receiver).await)
                .unwrap();
        }
    }
    block.store(false, Ordering::SeqCst);
    release.notify_one();
    assert!(!downstream.has_bid());
    assert!(!downstream.has_ask());
    ws_server.await.unwrap();
    assert!(receiver.try_recv().is_err());
    let expected = match shutdown {
        RequestShutdown::InvalidDefinition | RequestShutdown::Deadline => 5,
        RequestShutdown::Stop | RequestShutdown::Drop => 4,
    };
    assert_eq!(count.load(Ordering::SeqCst), expected);
    http_server.abort();
    let _ = http_server.await;
}

fn assert_definition_revision(instrument: &InstrumentAny, revision: usize) {
    let InstrumentAny::BinaryOption(instrument) = instrument else {
        panic!("Expected a binary option");
    };
    let info = instrument.info.as_ref().unwrap();
    let raw: Value = serde_json::from_str(info["kalshi_market_json"].as_str().unwrap()).unwrap();
    assert_eq!(raw["title"], format!("Definition {revision}"));
}

#[derive(Clone, Copy, Debug)]
enum RecoveryScenario {
    SelectionRefresh,
    Exhaustion,
}

#[rstest]
#[case::tungstenite_refresh(TransportBackend::Tungstenite, RecoveryScenario::SelectionRefresh)]
#[case::sockudo_refresh(TransportBackend::Sockudo, RecoveryScenario::SelectionRefresh)]
#[case::tungstenite_exhaustion(TransportBackend::Tungstenite, RecoveryScenario::Exhaustion)]
#[case::sockudo_exhaustion(TransportBackend::Sockudo, RecoveryScenario::Exhaustion)]
#[tokio::test]
async fn selection_refresh_and_transport_exhaustion_reach_the_data_client(
    #[case] backend: TransportBackend,
    #[case] scenario: RecoveryScenario,
) {
    tokio::time::timeout(Duration::from_secs(20), run_recovery(backend, scenario))
        .await
        .unwrap();
}

async fn run_recovery(backend: TransportBackend, scenario: RecoveryScenario) {
    let request = RequestInstruments::new(
        None,
        None,
        Some(ClientId::from("KALSHI")),
        Some(Venue::new("KALSHI")),
        Default::default(),
        1.into(),
        None,
    );
    let tickers = match scenario {
        RecoveryScenario::SelectionRefresh => vec!["MARKET-A", "MARKET-B", "MARKET-C"],
        RecoveryScenario::Exhaustion => vec!["MARKET-A"],
    };
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let app = Router::new().route(
        "/trade-api/v2/markets/{ticker}",
        get(move |Path(ticker): Path<String>| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);

                if matches!(scenario, RecoveryScenario::SelectionRefresh) {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                }
                let mut market =
                    serde_json::from_slice::<Value>(include_bytes!("../test_data/markets.json"))
                        .unwrap()["markets"][0]
                        .clone();
                market["ticker"] = json!(ticker);
                Json(json!({"market": market}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = listener.local_addr().unwrap();

    let http_server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = listener.local_addr().unwrap();
    let (advance, stage) = tokio::sync::oneshot::channel();
    let selection = tickers.clone();

    let ws_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        let request: Value = serde_json::from_str(
            &websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request["params"]["market_tickers"], json!(selection));
        websocket
            .send(Message::text(
                json!({
                    "id": request["id"], "type": "subscribed",
                    "msg": {"channel": "orderbook_delta", "sid": 2},
                })
                .to_string(),
            ))
            .await
            .unwrap();

        for (index, ticker) in selection.iter().enumerate() {
            let mut snapshot: Value =
                serde_json::from_str(include_str!("../test_data/ws_orderbook_snapshot.json"))
                    .unwrap();
            snapshot["seq"] = json!(index + 2);
            snapshot["msg"]["market_ticker"] = json!(ticker);
            snapshot["msg"]["market_id"] =
                json!(format!("00000000-0000-4000-8000-{:012}", index + 1));
            websocket
                .send(Message::text(snapshot.to_string()))
                .await
                .unwrap();
        }
        stage.await.unwrap();

        match scenario {
            RecoveryScenario::SelectionRefresh => {
                let mut delta: Value =
                    serde_json::from_str(include_str!("../test_data/ws_orderbook_delta.json"))
                        .unwrap();
                delta["seq"] = json!(selection.len() + 2);
                delta["msg"]["market_ticker"] = json!(selection[0]);
                delta["msg"]["market_id"] = json!("00000000-0000-4000-8000-000000000001");
                delta["msg"]["price_dollars"] = json!("0.2200");
                delta["msg"]["delta_fp"] = json!("0.25");
                websocket
                    .send(Message::text(delta.to_string()))
                    .await
                    .unwrap();

                while let Some(message) = websocket.next().await {
                    match message {
                        Ok(Message::Close(_)) => {
                            let _ = websocket.flush().await;
                            break;
                        }
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
            RecoveryScenario::Exhaustion => {
                drop(listener);
                websocket.close(None).await.unwrap();
            }
        }
    });
    let cache = Rc::new(RefCell::new(Cache::default()));
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut receiver = EngineReceiver::new(receiver, Rc::clone(&cache));
    replace_data_event_sender(sender);
    let mut config = config(
        format!("http://{http_address}/trade-api/v2"),
        format!("ws://{ws_address}/trade-api/ws/v2"),
        backend,
    );
    config.market_tickers = tickers.iter().map(|ticker| ticker.to_string()).collect();
    config.http.request_timeout_ms = 1500.try_into().unwrap();
    config.http.operation_timeout_ms = 1500.try_into().unwrap();
    config.websocket.transport.reconnect_max_attempts = Some(1);
    let mut client = factory()
        .create(
            "KALSHI",
            &config,
            CacheView::new(Rc::clone(&cache)),
            Rc::new(RefCell::new(TestClock::new())),
        )
        .unwrap();
    client.start().unwrap();
    client.connect().await.unwrap();

    for _ in &tickers {
        let DataEvent::Instrument(instrument) = receiver.recv().await.unwrap() else {
            panic!("Expected every selected instrument at bootstrap");
        };
        cache.borrow_mut().add_instrument(instrument).unwrap();
    }
    receiver.flush();
    assert!(client.is_connected());
    let id = InstrumentId::from("MARKET-A.KALSHI");
    client.subscribe_book_deltas(subscription(id)).unwrap();
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    book.apply_deltas(&next_deltas(&mut receiver).await)
        .unwrap();

    match scenario {
        RecoveryScenario::SelectionRefresh => {
            let started = tokio::time::Instant::now();
            client.request_instruments(request.clone()).unwrap();

            for _ in &tickers {
                let DataEvent::Instrument(instrument) = receiver.recv().await.unwrap() else {
                    panic!("Selection refresh ended before every market completed its own budget");
                };
                cache.borrow_mut().add_instrument(instrument).unwrap();
            }
            let DataEvent::Response(DataResponse::Instruments(response)) =
                receiver.recv().await.unwrap()
            else {
                panic!("Expected complete selection response");
            };
            assert_eq!(response.correlation_id, request.request_id);
            assert_eq!(response.data.len(), tickers.len());
            assert!(started.elapsed() > Duration::from_millis(1500));
            assert_eq!(count.load(Ordering::SeqCst), 2 * tickers.len());
            advance.send(()).unwrap();
            book.apply_deltas(&next_deltas(&mut receiver).await)
                .unwrap();
            assert_eq!(book.best_bid_size(), Some(Quantity::from("333.25")));
            assert!(client.is_connected());
        }
        RecoveryScenario::Exhaustion => {
            advance.send(()).unwrap();
            book.apply_deltas(&next_deltas(&mut receiver).await)
                .unwrap();
            assert!(!book.has_bid() && !book.has_ask());
            assert!(!client.is_connected());

            while client.subscribe_book_deltas(subscription(id)).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(
                receiver.try_recv().is_err(),
                "Exhaustion must clear a published book only once"
            );
        }
    }
    client.disconnect().await.unwrap();
    ws_server.await.unwrap();
    http_server.abort();
    let _ = http_server.await;
}
