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

//! The same reconnect scenario runs against OS sockets and the Turmoil network simulator.

#![cfg(not(all(feature = "simulation", madsim)))]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use nautilus_network::{
    net::TcpListener,
    transport::{Message, TransportError},
    websocket::{HeadersProvider, TransportBackend, WebSocketClient, WebSocketConfig},
};
use rstest::rstest;
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message as ServerMessage,
        handshake::server::{Callback, ErrorResponse, Request, Response},
    },
};

#[derive(Debug)]
struct HeaderCheck(usize);

impl Callback for HeaderCheck {
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertion failures must fail the test"
    )]
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(request.uri().path(), "/fixture");
        assert_eq!(request.headers()["x-timestamp"], self.0.to_string());
        assert_eq!(
            request.headers()["x-signature"],
            format!("signature-{}", self.0)
        );
        Ok(response)
    }
}

#[cfg(not(feature = "turmoil"))]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[cfg_attr(
    feature = "transport-sockudo",
    case::sockudo(TransportBackend::Sockudo)
)]
#[tokio::test]
async fn reconnect_generates_fresh_headers_and_skips_failed_generation(
    #[case] backend: TransportBackend,
) {
    check_reconnect_headers(backend).await;
}

#[cfg(feature = "turmoil")]
#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[cfg_attr(
    feature = "transport-sockudo",
    case::sockudo(TransportBackend::Sockudo)
)]
fn simulated_reconnect_generates_fresh_headers_and_skips_failed_generation(
    #[case] backend: TransportBackend,
) {
    let mut simulation = turmoil::Builder::new()
        .rng_seed(0x57EB_3018)
        .simulation_duration(Duration::from_secs(20))
        .build();
    simulation.client("client", async move {
        check_reconnect_headers(backend).await;
        Ok(())
    });
    simulation.run().unwrap();
}

async fn check_reconnect_headers(backend: TransportBackend) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut connections = Vec::new();

        for generation in [0, 2] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_hdr_async(stream, HeaderCheck(generation))
                .await
                .unwrap();
            websocket
                .send(ServerMessage::text(format!("generation-{generation}")))
                .await
                .unwrap();
            connections.push(websocket);
        }

        // Keep both sockets alive until the client shuts down, so only the explicit request
        // below initiates reconnection. Reading the replacement also completes its close handshake.
        let replacement = connections.last_mut().unwrap();

        while let Some(message) = replacement.next().await {
            if matches!(message.unwrap(), ServerMessage::Close(_)) {
                replacement.flush().await.unwrap();
                break;
            }
        }
    });
    let generations = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&generations);
    let provider: HeadersProvider = Arc::new(move || {
        let generation = count.fetch_add(1, Ordering::SeqCst);

        if generation == 1 {
            return Err(TransportError::Other("fixture signing failure".to_string()));
        }
        Ok(vec![
            ("x-timestamp".to_string(), generation.to_string()),
            ("x-signature".to_string(), format!("signature-{generation}")),
        ])
    });
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let config = WebSocketConfig::builder()
        .url(format!("ws://{address}/fixture"))
        .backend(backend)
        .connect_timeout_ms(2_000)
        .reconnect_delay_initial_ms(10)
        .reconnect_delay_max_ms(10)
        .reconnect_jitter_ms(0)
        .reconnect_max_attempts(3)
        .build()
        .unwrap();
    let client = timeout(
        Duration::from_secs(5),
        WebSocketClient::epoch_builder()
            .config(config)
            .headers_provider(provider)
            .epoch_handler(Arc::new(move |epoch, message| {
                if let Message::Text(text) = message
                    && text.starts_with(b"generation-")
                {
                    received_tx
                        .send((epoch, String::from_utf8(text.to_vec()).unwrap()))
                        .unwrap();
                }
            }))
            .connect(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), received_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        (0, "generation-0".to_string())
    );
    assert!(client.request_reconnect());
    assert_eq!(
        timeout(Duration::from_secs(5), received_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        (1, "generation-2".to_string())
    );
    assert_eq!(generations.load(Ordering::SeqCst), 3);
    assert_eq!(client.connection_epoch(), 1);
    assert!(
        timeout(Duration::from_millis(20), client.wait_until_closed())
            .await
            .is_err()
    );
    client.disconnect().await;
    timeout(Duration::from_secs(5), client.wait_until_closed())
        .await
        .unwrap();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}
