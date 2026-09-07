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

//! Correlated metadata-refresh orderbooks shared by native and simulated client tests.

use futures_util::{SinkExt, StreamExt};
use nautilus_network::net::TcpListener;
use serde_json::{Value, json};
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub(crate) async fn serve_metadata_orderbooks(
    listener: TcpListener,
    stages: tokio::sync::mpsc::UnboundedReceiver<()>,
) {
    serve_metadata_orderbooks_observing_connections(listener, stages, None).await;
}

pub(crate) async fn serve_metadata_orderbooks_observing_connections(
    listener: TcpListener,
    mut stages: tokio::sync::mpsc::UnboundedReceiver<()>,
    connected: Option<tokio::sync::mpsc::UnboundedSender<u64>>,
) {
    let mut connections = Vec::new();

    for connection in 1..=2 {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        if let Some(connected) = &connected {
            connected.send(connection).unwrap();
        }
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
        assert_eq!(request["id"], connection);
        let mut ack: Value =
            serde_json::from_str(include_str!("../../test_data/ws_subscribed.json")).unwrap();
        ack["id"] = request["id"].clone();
        ack["msg"]["sid"] = json!(2);
        websocket
            .send(Message::text(ack.to_string()))
            .await
            .unwrap();
        let mut snapshot: Value =
            serde_json::from_str(include_str!("../../test_data/ws_orderbook_snapshot.json"))
                .unwrap();
        snapshot["msg"]["no_dollars_fp"][0][0] = json!("0.4600");
        snapshot["msg"]["no_dollars_fp"][1][0] = json!("0.4400");

        if connection == 2 {
            snapshot["msg"]["yes_dollars_fp"][1][1] = json!("10.00");
        }
        websocket
            .send(Message::text(snapshot.to_string()))
            .await
            .unwrap();

        if connection == 1 {
            stages.recv().await.unwrap();
            let mut delta: Value =
                serde_json::from_str(include_str!("../../test_data/ws_orderbook_delta.json"))
                    .unwrap();
            delta["msg"]["price_dollars"] = json!("0.2200");
            delta["msg"]["delta_fp"] = json!("0.25");
            websocket
                .send(Message::text(delta.to_string()))
                .await
                .unwrap();
        }

        // The transport establishes its replacement before closing the previous socket
        connections.push(websocket);
    }
    let current = connections.last_mut().unwrap();

    while let Some(message) = current.next().await {
        match message {
            Ok(Message::Close(_)) => {
                let _ = current.flush().await;
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
}
