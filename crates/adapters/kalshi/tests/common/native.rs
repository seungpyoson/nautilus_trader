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

//! Shared native-client configuration and generated credentials for local mock venues.

use aws_lc_rs::{
    encoding::{AsDer, Pkcs8V1Der},
    rsa::{KeyPair, KeySize},
};
use nautilus_kalshi::{
    KalshiCredential, KalshiDataClientConfig, KalshiDataClientFactory, KalshiHttpConfig,
    KalshiWebSocketConfig,
};
use nautilus_network::websocket::{TransportBackend, WebSocketConfig};
use zeroize::Zeroizing;

pub(crate) const TICKER: &str = "FED-23DEC-T3.00";

pub(crate) fn config(
    http: String,
    websocket: String,
    backend: TransportBackend,
) -> KalshiDataClientConfig {
    KalshiDataClientConfig {
        market_tickers: vec![TICKER.to_string()],
        http: KalshiHttpConfig {
            base_url: http,
            request_timeout_ms: 1000.try_into().unwrap(),
            operation_timeout_ms: 2000.try_into().unwrap(),
            max_response_bytes: 65536.try_into().unwrap(),
            max_pages: 2.try_into().unwrap(),
            max_markets: 2.try_into().unwrap(),
            page_size: 2.try_into().unwrap(),
            request_spacing_ms: 1.try_into().unwrap(),
            max_retries: 0,
        },
        websocket: KalshiWebSocketConfig {
            transport: WebSocketConfig::builder()
                .url(websocket)
                .backend(backend)
                .connect_timeout_ms(1000)
                .heartbeat_interval_secs(10)
                .reconnect_delay_initial_ms(10)
                .reconnect_delay_max_ms(10)
                .reconnect_jitter_ms(0)
                .reconnect_max_attempts(3)
                .build()
                .unwrap(),
            connect_deadline_ms: 2000.try_into().unwrap(),
            bootstrap_timeout_ms: 2000.try_into().unwrap(),
            max_frame_bytes: 4096.try_into().unwrap(),
            max_pending_frames: 64.try_into().unwrap(),
            connection_spacing_ms: 1.try_into().unwrap(),
            command_spacing_ms: 1.try_into().unwrap(),
        },
        bootstrap_timeout_ms: 5000.try_into().unwrap(),
        shutdown_timeout_ms: 2000.try_into().unwrap(),
    }
}

pub(crate) fn factory() -> KalshiDataClientFactory {
    let key = KeyPair::generate(KeySize::Rsa2048).unwrap();
    let der: Pkcs8V1Der<'_> = key.as_der().unwrap();
    let pem = Zeroizing::new(pem::encode(&pem::Pem::new("PRIVATE KEY", der.as_ref())));
    KalshiDataClientFactory::new(KalshiCredential::from_pem("synthetic", pem.as_bytes()).unwrap())
}
