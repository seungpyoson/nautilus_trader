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

use std::{cell::RefCell, rc::Rc, sync::LazyLock};

use aws_lc_rs::{
    encoding::{AsDer, Pkcs8V1Der},
    rsa::{KeyPair, KeySize},
};
use nautilus_common::cache::Cache;
use rstest::rstest;
use zeroize::Zeroizing;

use super::*;

fn config() -> KalshiDataClientConfig {
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../examples/data_reader.json")).unwrap();
    serde_json::from_value(value["client"].take()).unwrap()
}

fn client(config: KalshiDataClientConfig) -> anyhow::Result<KalshiDataClient> {
    static KEY: LazyLock<KeyPair> = LazyLock::new(|| KeyPair::generate(KeySize::Rsa2048).unwrap());
    let der: Pkcs8V1Der<'_> = KEY.as_der().unwrap();
    let pem = Zeroizing::new(pem::encode(&pem::Pem::new("PRIVATE KEY", der.as_ref())));
    KalshiDataClient::new(
        ClientId::from("KALSHI"),
        config,
        KalshiCredential::from_pem("synthetic", pem.as_bytes()).unwrap(),
        CacheView::new(Rc::new(RefCell::new(Cache::default()))),
    )
}

#[rstest]
#[tokio::test]
async fn cancelled_disconnect_retains_task_until_join_or_drop() {
    let mut client = client(config()).unwrap();
    let (finished, completion) = tokio::sync::oneshot::channel::<()>();
    client.task = Some(tokio::spawn(async move {
        let _finished = finished;
        std::future::pending::<()>().await;
    }));
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), client.disconnect())
            .await
            .is_err()
    );
    assert!(
        client.task.is_some(),
        "cancelled join lost ownership of the task"
    );
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .unwrap()
            .is_err()
    );
}

#[rstest]
#[case::disabled(None, None, None, false)]
#[case::derived(Some(10), None, None, true)]
#[case::explicit(None, Some(30), None, true)]
#[case::idle(None, None, Some(30000), true)]
#[case::zero_interval(Some(0), None, None, false)]
#[case::zero_timeout(None, Some(0), None, false)]
#[case::zero_idle(None, None, Some(0), false)]
fn construction_requires_finite_liveness_before_network_work(
    #[case] interval: Option<u64>,
    #[case] timeout: Option<u64>,
    #[case] idle: Option<u64>,
    #[case] accepted: bool,
) {
    let mut config = config();
    config.websocket.transport.heartbeat_interval_secs = interval;
    config.websocket.transport.heartbeat_timeout_secs = timeout;
    config.websocket.transport.idle_timeout_ms = idle;
    assert_eq!(client(config).is_ok(), accepted);
}

#[rstest]
fn construction_accepts_continuous_transport_recovery() {
    let mut config = config();
    config.websocket.transport.reconnect_max_attempts = None;
    assert!(client(config).is_ok());
}

#[rstest]
#[case::quotes("quotes")]
#[case::trades("trades")]
#[case::depth("depth")]
#[case::snapshot("snapshot")]
fn unsupported_data_operations_return_errors(#[case] operation: &str) {
    use nautilus_common::messages::data::{
        RequestBookSnapshot, SubscribeBookDepth10, SubscribeQuotes, SubscribeTrades,
    };
    let mut client = client(config()).unwrap();
    let client: &mut dyn DataClient = &mut client;
    let value = serde_json::json!({
        "instrument_id": "FED-23DEC-T3.00.KALSHI",
        "client_id": "KALSHI", "venue": "KALSHI",
        "command_id": "01234567-89ab-4cde-8012-3456789abcde",
        "request_id": "01234567-89ab-4cde-8012-3456789abcde",
        "ts_init": 1, "book_type": "L2_MBP", "managed": true,
        "depth": null, "start": null, "end": null,
    });
    let result = match operation {
        "quotes" => {
            client.subscribe_quotes(serde_json::from_value::<SubscribeQuotes>(value).unwrap())
        }
        "trades" => {
            client.subscribe_trades(serde_json::from_value::<SubscribeTrades>(value).unwrap())
        }
        "depth" => client
            .subscribe_book_depth10(serde_json::from_value::<SubscribeBookDepth10>(value).unwrap()),
        "snapshot" => client
            .request_book_snapshot(serde_json::from_value::<RequestBookSnapshot>(value).unwrap()),
        _ => unreachable!(),
    };
    assert!(result.is_err(), "unsupported {operation} returned success");
}
