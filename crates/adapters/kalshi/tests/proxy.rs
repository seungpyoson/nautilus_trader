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

//! Authenticated loopback exceptions cannot be routed through a proxy.
#![cfg(not(feature = "turmoil"))]

#[path = "common/native.rs"]
mod native;

use std::{cell::RefCell, rc::Rc, time::Duration};

use aws_lc_rs::{
    encoding::{AsDer, Pkcs8V1Der},
    rsa::{KeyPair, KeySize},
};
use nautilus_common::{
    cache::{Cache, CacheView},
    clock::TestClock,
    factories::DataClientFactory,
};
use nautilus_kalshi::{KalshiCredential, KalshiWebSocketClient, KalshiWebSocketError};
use nautilus_network::websocket::TransportBackend;
use rstest::rstest;
use zeroize::Zeroizing;

#[rstest]
#[case::tungstenite(TransportBackend::Tungstenite)]
#[case::sockudo(TransportBackend::Sockudo)]
#[tokio::test]
async fn proxied_plaintext_authentication_is_rejected_before_io(
    #[case] backend: TransportBackend,
    #[values("127.0.0.2", "[::1]")] host: &str,
    #[values("http", "https", "socks5")] proxy_scheme: &str,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    let mut config = native::config(
        "http://127.0.0.1:1/trade-api/v2".to_string(),
        format!("ws://{host}:43119/trade-api/ws/v2"),
        backend,
    );
    config.websocket.transport.proxy_url = Some(format!("{proxy_scheme}://{proxy}"));
    let factory_result = native::factory().create(
        "KALSHI",
        &config,
        CacheView::new(Rc::new(RefCell::new(Cache::default()))),
        Rc::new(RefCell::new(TestClock::new())),
    );
    assert!(
        factory_result.is_err(),
        "Factory must reject plaintext proxy configuration"
    );

    let key = KeyPair::generate(KeySize::Rsa2048).unwrap();
    let der: Pkcs8V1Der<'_> = key.as_der().unwrap();
    let pem = Zeroizing::new(pem::encode(&pem::Pem::new("PRIVATE KEY", der.as_ref())));
    let credential = KalshiCredential::from_pem("synthetic", pem.as_bytes()).unwrap();
    let result = KalshiWebSocketClient::connect(
        config.websocket.clone(),
        config.market_tickers.clone(),
        &credential,
    )
    .await;
    assert!(matches!(
        result,
        Err(KalshiWebSocketError::Configuration(_))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );

    // TLS remains permitted with a proxy, with validation before any connection
    config.websocket.transport.url = format!("wss://{host}:43119/trade-api/ws/v2");
    assert!(
        native::factory()
            .create(
                "KALSHI",
                &config,
                CacheView::new(Rc::new(RefCell::new(Cache::default()))),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .is_ok()
    );
}
