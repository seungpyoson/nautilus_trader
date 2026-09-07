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

//! Public client tests using generated credentials and documented protocol fixtures.
//! The same scenarios run on OS sockets or Turmoil's simulated network.

mod common;

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    sync::LazyLock,
    time::Duration,
};

use aws_lc_rs::{
    encoding::{AsDer, Pkcs8V1Der},
    rsa::{KeyPair, KeySize},
    signature::{KeyPair as _, RSA_PSS_2048_8192_SHA256, UnparsedPublicKey},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use nautilus_kalshi::{
    KalshiCredential, KalshiOrderbookMessage, KalshiWebSocketClient, KalshiWebSocketConfig,
    KalshiWebSocketError, KalshiWebSocketEvent,
};
use nautilus_network::{
    mode::ReconnectRequestOutcome,
    net::TcpListener,
    websocket::{TransportBackend, WebSocketConfig},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Callback, ErrorResponse, Request, Response},
    },
};
use zeroize::Zeroizing;

const TICKER: &str = "FED-23DEC-T3.00";
static TEST_KEY: LazyLock<KeyPair> = LazyLock::new(|| KeyPair::generate(KeySize::Rsa2048).unwrap());

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Reconnect,
    MalformedFrame,
    MissingAcknowledgement,
    MissingSnapshot,
}

#[derive(Debug)]
struct VerifyAuthentication;

impl Callback for VerifyAuthentication {
    #[expect(
        clippy::panic_in_result_fn,
        reason = "invalid authentication must fail the test"
    )]
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(request.uri().path(), "/trade-api/ws/v2");
        assert_eq!(request.headers()["KALSHI-ACCESS-KEY"], "synthetic");
        let timestamp = request.headers()["KALSHI-ACCESS-TIMESTAMP"]
            .to_str()
            .unwrap();
        let signature = STANDARD
            .decode(request.headers()["KALSHI-ACCESS-SIGNATURE"].as_bytes())
            .unwrap();
        UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, TEST_KEY.public_key().as_ref())
            .verify(
                format!("{timestamp}GET/trade-api/ws/v2").as_bytes(),
                &signature,
            )
            .unwrap();
        Ok(response)
    }
}

#[cfg(not(feature = "turmoil"))]
#[rstest]
#[case::tungstenite_reconnect(TransportBackend::Tungstenite, Scenario::Reconnect)]
#[case::sockudo_reconnect(TransportBackend::Sockudo, Scenario::Reconnect)]
#[case::tungstenite_timeout(TransportBackend::Tungstenite, Scenario::MissingAcknowledgement)]
#[case::sockudo_timeout(TransportBackend::Sockudo, Scenario::MissingAcknowledgement)]
#[case::tungstenite_malformed(TransportBackend::Tungstenite, Scenario::MalformedFrame)]
#[case::sockudo_malformed(TransportBackend::Sockudo, Scenario::MalformedFrame)]
#[case::tungstenite_snapshot_timeout(TransportBackend::Tungstenite, Scenario::MissingSnapshot)]
#[case::sockudo_snapshot_timeout(TransportBackend::Sockudo, Scenario::MissingSnapshot)]
#[tokio::test]
async fn authenticated_orderbook_client_lifecycle(
    #[case] backend: TransportBackend,
    #[case] scenario: Scenario,
) {
    run_scenario(backend, scenario).await;
}

#[cfg(feature = "turmoil")]
#[rstest]
#[case::tungstenite_reconnect(TransportBackend::Tungstenite, Scenario::Reconnect)]
#[case::sockudo_reconnect(TransportBackend::Sockudo, Scenario::Reconnect)]
#[case::tungstenite_timeout(TransportBackend::Tungstenite, Scenario::MissingAcknowledgement)]
#[case::sockudo_timeout(TransportBackend::Sockudo, Scenario::MissingAcknowledgement)]
#[case::tungstenite_malformed(TransportBackend::Tungstenite, Scenario::MalformedFrame)]
#[case::sockudo_malformed(TransportBackend::Sockudo, Scenario::MalformedFrame)]
#[case::tungstenite_snapshot_timeout(TransportBackend::Tungstenite, Scenario::MissingSnapshot)]
#[case::sockudo_snapshot_timeout(TransportBackend::Sockudo, Scenario::MissingSnapshot)]
fn simulated_authenticated_orderbook_client_lifecycle(
    #[case] backend: TransportBackend,
    #[case] scenario: Scenario,
) {
    let mut simulation = turmoil::Builder::new()
        .rng_seed(0x57EB_3019)
        .simulation_duration(Duration::from_secs(20))
        .build();
    simulation.client("client", async move {
        run_scenario(backend, scenario).await;
        Ok(())
    });
    simulation.run().unwrap();
}

async fn run_scenario(backend: TransportBackend, scenario: Scenario) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut connections = Vec::new();

        for request_id in 1..=2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_hdr_async(stream, VerifyAuthentication)
                .await
                .unwrap();
            let command = websocket
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&command).unwrap(),
                json!({
                    "id":request_id,"cmd":"subscribe",
                    "params":{"channels":["orderbook_delta"],"market_tickers":[TICKER],"use_yes_price":true}
                })
            );

            if matches!(scenario, Scenario::MissingAcknowledgement) {
                drop(listener);
                // The active socket stays silent while replacement connections are refused.
                // Terminal client shutdown must release this socket and wake its consumer.
                let _ = websocket.next().await;
                return;
            }
            let mut ack: Value =
                serde_json::from_str(include_str!("../test_data/ws_subscribed.json")).unwrap();
            ack["id"] = json!(request_id);
            ack["msg"]["sid"] = json!(2);
            websocket
                .send(Message::text(ack.to_string()))
                .await
                .unwrap();

            if matches!(scenario, Scenario::MissingSnapshot) {
                drop(listener);
                let _ = websocket.next().await;
                return;
            }
            websocket
                .send(Message::text(include_str!(
                    "../test_data/ws_orderbook_snapshot.json"
                )))
                .await
                .unwrap();

            if matches!(scenario, Scenario::MalformedFrame) && request_id == 1 {
                websocket
                    .send(Message::text(r#"{"type":"orderbook_delta"}"#))
                    .await
                    .unwrap();
            }
            connections.push(websocket);
        }
        let current = connections.last_mut().unwrap();

        while let Some(message) = current.next().await {
            if matches!(message.unwrap(), Message::Close(_)) {
                current.flush().await.unwrap();
                break;
            }
        }
    });
    let bootstrap_timeout_ms = match scenario {
        Scenario::Reconnect | Scenario::MalformedFrame => 2_000,
        Scenario::MissingAcknowledgement | Scenario::MissingSnapshot => 100,
    };
    let mut client = connect_client(backend, address, bootstrap_timeout_ms).await;

    match scenario {
        Scenario::Reconnect | Scenario::MalformedFrame => {
            assert_bootstrap(&mut client, 0).await;

            if matches!(scenario, Scenario::MalformedFrame) {
                assert!(matches!(
                    next(&mut client).await,
                    Err(KalshiWebSocketError::Json(_))
                ));
            } else {
                assert_eq!(client.invalidate(), ReconnectRequestOutcome::Accepted);
            }
            assert!(matches!(
                next(&mut client).await.unwrap(),
                Some(KalshiWebSocketEvent::Disconnected {
                    connection_epoch: 0
                })
            ));
            assert_bootstrap(&mut client, 1).await;
            client.disconnect().await;
            client.disconnect().await;
            assert!(next(&mut client).await.unwrap().is_none());
            assert_eq!(client.invalidate(), ReconnectRequestOutcome::Closed);
        }
        Scenario::MissingAcknowledgement | Scenario::MissingSnapshot => {
            if matches!(scenario, Scenario::MissingSnapshot) {
                assert!(matches!(
                    next(&mut client).await.unwrap(),
                    Some(KalshiWebSocketEvent::Subscribed { .. })
                ));
            }
            assert!(matches!(
                next(&mut client).await,
                Err(KalshiWebSocketError::BootstrapTimeout)
            ));
            // Loss may already have reached the terminal state before the next poll.
            if let Some(event) = next(&mut client).await.unwrap() {
                assert!(matches!(
                    event,
                    KalshiWebSocketEvent::Disconnected {
                        connection_epoch: 0
                    }
                ));
                assert!(next(&mut client).await.unwrap().is_none());
            }
            client.disconnect().await;
        }
    }
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

async fn connect_client(
    backend: TransportBackend,
    address: SocketAddr,
    bootstrap_timeout_ms: u32,
) -> KalshiWebSocketClient {
    connect_client_with_spacing(backend, address, bootstrap_timeout_ms, 1).await
}

async fn connect_client_with_spacing(
    backend: TransportBackend,
    address: SocketAddr,
    bootstrap_timeout_ms: u32,
    command_spacing_ms: u32,
) -> KalshiWebSocketClient {
    let der: Pkcs8V1Der<'_> = TEST_KEY.as_der().unwrap();
    let pem = Zeroizing::new(pem::encode(&pem::Pem::new("PRIVATE KEY", der.as_ref())));
    let credential = KalshiCredential::from_pem("synthetic", pem.as_bytes()).unwrap();
    let config = KalshiWebSocketConfig {
        transport: WebSocketConfig::builder()
            .url(format!("ws://{address}/trade-api/ws/v2"))
            .backend(backend)
            .connect_timeout_ms(200)
            .heartbeat_interval_secs(10)
            .reconnect_delay_initial_ms(10)
            .reconnect_delay_max_ms(10)
            .reconnect_jitter_ms(0)
            .reconnect_max_attempts(2)
            .build()
            .unwrap(),
        connect_deadline_ms: NonZeroU32::new(2_000).unwrap(),
        bootstrap_timeout_ms: NonZeroU32::new(bootstrap_timeout_ms).unwrap(),
        max_frame_bytes: NonZeroUsize::new(4096).unwrap(),
        max_pending_frames: NonZeroUsize::new(64).unwrap(),
        connection_spacing_ms: NonZeroU32::new(1).unwrap(),
        command_spacing_ms: NonZeroU32::new(command_spacing_ms).unwrap(),
    };
    KalshiWebSocketClient::connect(config, vec![TICKER.to_string()], &credential)
        .await
        .unwrap()
}

#[cfg(not(feature = "turmoil"))]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn cancelling_event_poll_preserves_reconnect_subscription(#[case] backend: TransportBackend) {
    run_cancelled_subscription(backend).await;
}

#[cfg(feature = "turmoil")]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
fn simulated_cancelling_event_poll_preserves_reconnect_subscription(
    #[case] backend: TransportBackend,
) {
    let mut simulation = turmoil::Builder::new()
        .rng_seed(0x57EB_3019)
        .simulation_duration(Duration::from_secs(20))
        .build();
    simulation.client("client", async move {
        run_cancelled_subscription(backend).await;
        Ok(())
    });
    simulation.run().unwrap();
}

async fn run_cancelled_subscription(backend: TransportBackend) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (advance, stages) = tokio::sync::mpsc::unbounded_channel();
        let (connected, mut connections) = tokio::sync::mpsc::unbounded_channel();

        let server = tokio::spawn(
            common::testing::serve_metadata_orderbooks_observing_connections(
                listener,
                stages,
                Some(connected),
            ),
        );
        let mut client = connect_client_with_spacing(backend, address, 5000, 3000).await;
        assert_bootstrap(&mut client, 0).await;
        assert_eq!(connections.recv().await, Some(1));
        advance.send(()).unwrap();
        assert!(matches!(
            next(&mut client).await.unwrap(),
            Some(KalshiWebSocketEvent::Book { .. })
        ));
        assert_eq!(client.invalidate(), ReconnectRequestOutcome::Accepted);
        assert!(matches!(
            next(&mut client).await.unwrap(),
            Some(KalshiWebSocketEvent::Disconnected { .. })
        ));
        assert_eq!(connections.recv().await, Some(2));
        // Repeated cancellations span transport reconnect and the command quota wait.
        for _ in 0..50 {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), client.next_event())
                    .await
                    .is_err()
            );
        }
        assert_bootstrap(&mut client, 1).await;
        client.disconnect().await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}

async fn next(
    client: &mut KalshiWebSocketClient,
) -> Result<Option<KalshiWebSocketEvent>, KalshiWebSocketError> {
    tokio::time::timeout(Duration::from_secs(5), client.next_event())
        .await
        .unwrap()
}

async fn assert_bootstrap(client: &mut KalshiWebSocketClient, expected_epoch: u64) {
    let acknowledgement = next(client).await.unwrap();
    assert!(
        matches!(acknowledgement, Some(KalshiWebSocketEvent::Subscribed {connection_epoch,subscription_id}) if connection_epoch == expected_epoch && subscription_id.get() == 2),
        "Expected subscription for epoch {expected_epoch}, received {acknowledgement:?}"
    );
    let event = next(client).await.unwrap().unwrap();
    let KalshiWebSocketEvent::Book {
        connection_epoch,
        message: KalshiOrderbookMessage::Snapshot { sid, seq, snapshot },
    } = event
    else {
        panic!("expected the documented snapshot");
    };
    assert_eq!(connection_epoch, expected_epoch);
    assert_eq!(sid.get(), 2);
    assert_eq!(seq.get(), 2);
    assert_eq!(snapshot.market_ticker, TICKER);
    assert_eq!(snapshot.yes.len(), 2);
    assert_eq!(snapshot.no.len(), 2);
}

#[cfg(not(feature = "turmoil"))]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn metadata_refresh_recovers_before_previous_socket_closes(
    #[case] backend: TransportBackend,
) {
    run_metadata_reconnect(backend).await;
}

#[cfg(feature = "turmoil")]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
fn simulated_metadata_refresh_recovers_before_previous_socket_closes(
    #[case] backend: TransportBackend,
) {
    let mut simulation = turmoil::Builder::new()
        .rng_seed(0x57EB_3019)
        .simulation_duration(Duration::from_secs(20))
        .build();
    simulation.client("client", async move {
        run_metadata_reconnect(backend).await;
        Ok(())
    });
    simulation.run().unwrap();
}

async fn run_metadata_reconnect(backend: TransportBackend) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (advance, stages) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(common::testing::serve_metadata_orderbooks(listener, stages));
    let mut client = connect_client(backend, address, 2_000).await;
    assert_bootstrap(&mut client, 0).await;
    advance.send(()).unwrap();
    assert!(matches!(
        next(&mut client).await.unwrap(),
        Some(KalshiWebSocketEvent::Book {
            connection_epoch: 0,
            message: KalshiOrderbookMessage::Delta { .. },
        })
    ));
    assert_eq!(client.invalidate(), ReconnectRequestOutcome::Accepted);
    assert!(matches!(
        next(&mut client).await.unwrap(),
        Some(KalshiWebSocketEvent::Disconnected {
            connection_epoch: 0
        })
    ));
    assert_bootstrap(&mut client, 1).await;
    client.disconnect().await;
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[cfg(not(feature = "turmoil"))]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn backlog_overflow_requires_a_new_connection_and_snapshot(
    #[case] backend: TransportBackend,
) {
    run_backlog_overflow(backend).await;
}

#[cfg(feature = "turmoil")]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
fn simulated_backlog_overflow_requires_a_new_connection_and_snapshot(
    #[case] backend: TransportBackend,
) {
    let mut simulation = turmoil::Builder::new()
        .rng_seed(0x57EB_3019)
        .min_message_latency(Duration::ZERO)
        .max_message_latency(Duration::ZERO)
        .simulation_duration(Duration::from_secs(20))
        .build();
    simulation.client("client", async move {
        run_backlog_overflow(backend).await;
        Ok(())
    });
    simulation.run().unwrap();
}

async fn run_backlog_overflow(backend: TransportBackend) {
    tokio::time::timeout(Duration::from_secs(10), async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (burst, completed) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut first = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _request = first.next().await.unwrap().unwrap();
            let ack = r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":2}}"#;
            for _ in 0..1024 {
                first.send(Message::text(ack)).await.unwrap();
            }
            burst.send(()).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut replacement = tokio_tungstenite::accept_async(stream).await.unwrap();
            let command: Value = serde_json::from_str(
                &replacement
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(command["id"], 2);
            replacement
                .send(Message::text(
                    r#"{"id":2,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":2}}"#,
                ))
                .await
                .unwrap();
            replacement
                .send(Message::text(include_str!(
                    "../test_data/ws_orderbook_snapshot.json"
                )))
                .await
                .unwrap();

            while let Some(message) = replacement.next().await {
                if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                    let _ = replacement.flush().await;
                    break;
                }
            }
        });
        let mut client = connect_client(backend, address, 2000).await;
        completed.await.unwrap();
        // Let the transport drain the completed burst while the application consumer is paused
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            next(&mut client).await,
            Err(KalshiWebSocketError::BacklogOverflow { limit: 64 })
        ));
        assert!(matches!(
            next(&mut client).await.unwrap(),
            Some(KalshiWebSocketEvent::Disconnected { .. })
        ));
        assert_bootstrap(&mut client, 1).await;
        client.disconnect().await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}
