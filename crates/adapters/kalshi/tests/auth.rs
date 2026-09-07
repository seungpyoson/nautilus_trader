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

use std::{collections::HashMap, sync::LazyLock, time::Duration};

use aws_lc_rs::{
    encoding::{AsDer, Pkcs8V1Der},
    rsa::{KeyPair, KeySize},
    signature::{
        KeyPair as _, RSA_PKCS1_2048_8192_SHA256, RSA_PSS_2048_8192_SHA256, UnparsedPublicKey,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_kalshi::{KalshiAuthError, KalshiCredential};
use nautilus_network::http::Url;
use rstest::rstest;
use zeroize::Zeroizing;

static TEST_KEY: LazyLock<KeyPair> = LazyLock::new(|| KeyPair::generate(KeySize::Rsa2048).unwrap());

fn private_key_pem(pkcs1: bool) -> Zeroizing<String> {
    let der: Pkcs8V1Der<'_> = TEST_KEY.as_der().unwrap();
    let (tag, bytes) = if pkcs1 {
        let info = pkcs8::PrivateKeyInfo::try_from(der.as_ref()).unwrap();
        ("RSA PRIVATE KEY", info.private_key)
    } else {
        ("PRIVATE KEY", der.as_ref())
    };
    Zeroizing::new(pem::encode(&pem::Pem::new(tag, bytes)))
}

#[rstest]
#[case::pkcs1(true)]
#[case::pkcs8(false)]
fn signature_binds_milliseconds_method_and_encoded_path(#[case] pkcs1: bool) {
    let input = private_key_pem(pkcs1);
    let credential = KalshiCredential::from_pem("synthetic-key-id", input.as_bytes()).unwrap();
    let url = Url::parse("wss://example.test/trade-api/ws/v2?ignored=query").unwrap();
    let headers: HashMap<_, _> = credential
        .sign_get(&url, UnixNanos::from(1_669_149_841_123_456_789_u64))
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(headers.len(), 3);
    assert_eq!(headers["KALSHI-ACCESS-KEY"], "synthetic-key-id");
    assert_eq!(headers["KALSHI-ACCESS-TIMESTAMP"], "1669149841123");
    let signature = STANDARD
        .decode(&headers["KALSHI-ACCESS-SIGNATURE"])
        .unwrap();
    let verifier =
        UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, TEST_KEY.public_key().as_ref());
    verifier
        .verify(b"1669149841123GET/trade-api/ws/v2", &signature)
        .unwrap();

    for incorrect in [
        "1669149841123456789GET/trade-api/ws/v2",
        "1669149841123POST/trade-api/ws/v2",
        "1669149841123GET/trade-api/ws/v2?ignored=query",
        "1669149841123GET/trade-api/ws/v3",
    ] {
        assert!(verifier.verify(incorrect.as_bytes(), &signature).is_err());
    }
    assert!(
        UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, TEST_KEY.public_key().as_ref())
            .verify(b"1669149841123GET/trade-api/ws/v2", &signature)
            .is_err()
    );
    assert!(!format!("{credential:?}").contains("synthetic-key-id"));
    assert!(!format!("{credential:?}").contains("PRIVATE"));
}

#[rstest]
fn signature_preserves_percent_encoded_path_without_query_or_fragment() {
    let input = private_key_pem(false);
    let credential = KalshiCredential::from_pem("synthetic-key-id", input.as_bytes()).unwrap();
    let url =
        Url::parse("https://example.test/trade-api/v2/markets/A%2FB?limit=1#fragment").unwrap();
    let headers: HashMap<_, _> = credential
        .sign_get(&url, UnixNanos::from(1_000_000_u64))
        .unwrap()
        .into_iter()
        .collect();
    let signature = STANDARD
        .decode(&headers["KALSHI-ACCESS-SIGNATURE"])
        .unwrap();
    let verifier =
        UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, TEST_KEY.public_key().as_ref());
    verifier
        .verify(b"1GET/trade-api/v2/markets/A%2FB", &signature)
        .unwrap();
    assert!(
        verifier
            .verify(b"1GET/trade-api/v2/markets/A/B", &signature)
            .is_err()
    );
}

#[rstest]
#[tokio::test]
async fn provider_generates_signatures_with_current_time() {
    let input = private_key_pem(false);
    let credential = KalshiCredential::from_pem("synthetic-key-id", input.as_bytes()).unwrap();
    let provider = credential
        .websocket_headers_provider(Url::parse("wss://example.test/trade-api/ws/v2").unwrap())
        .unwrap();
    let before = get_atomic_clock_realtime().get_time_ms();
    let first: HashMap<_, _> = provider().unwrap().into_iter().collect();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let second: HashMap<_, _> = provider().unwrap().into_iter().collect();
    let after = get_atomic_clock_realtime().get_time_ms();
    let verifier =
        UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, TEST_KEY.public_key().as_ref());

    for headers in [&first, &second] {
        let timestamp = headers["KALSHI-ACCESS-TIMESTAMP"].parse::<u64>().unwrap();
        assert!((before..=after).contains(&timestamp));
        let message = format!("{timestamp}GET/trade-api/ws/v2");
        verifier
            .verify(
                message.as_bytes(),
                &STANDARD
                    .decode(&headers["KALSHI-ACCESS-SIGNATURE"])
                    .unwrap(),
            )
            .unwrap();
    }
    assert_ne!(
        first["KALSHI-ACCESS-SIGNATURE"],
        second["KALSHI-ACCESS-SIGNATURE"]
    );
    assert!(
        second["KALSHI-ACCESS-TIMESTAMP"].parse::<u64>().unwrap()
            > first["KALSHI-ACCESS-TIMESTAMP"].parse::<u64>().unwrap()
    );
}

#[rstest]
#[case("")]
#[case(" key")]
#[case("key\r\nx-injected: value")]
fn invalid_key_id_is_rejected_without_echoing_input(#[case] key_id: &str) {
    let error = KalshiCredential::from_pem(key_id, b"not a key").unwrap_err();
    assert!(matches!(error, KalshiAuthError::InvalidKeyId));
    assert_eq!(error.to_string(), "Invalid Kalshi key ID");
}

#[rstest]
fn invalid_ambiguous_and_oversized_keys_are_rejected() {
    let input = private_key_pem(false);
    assert!(matches!(
        KalshiCredential::from_pem("synthetic", &vec![0; 65_537]),
        Err(KalshiAuthError::KeyTooLarge)
    ));

    for input in [
        Zeroizing::new("not a key".to_string()),
        Zeroizing::new(input.repeat(2)),
        Zeroizing::new(pem::encode(&pem::Pem::new("PUBLIC KEY", [1, 2, 3]))),
    ] {
        assert!(matches!(
            KalshiCredential::from_pem("synthetic", input.as_bytes()),
            Err(KalshiAuthError::InvalidPrivateKey)
        ));
    }
}

#[rstest]
#[case("https://example.test/trade-api/ws/v2")]
#[case("wss://user:password@example.test/trade-api/ws/v2")]
#[case("wss://example.test/trade-api/ws/v2#fragment")]
#[case("ws://example.test/trade-api/ws/v2")]
#[case("ws://192.0.2.1/trade-api/ws/v2")]
#[case("ws://[2001:db8::1]/trade-api/ws/v2")]
#[case("ws://localhost/trade-api/ws/v2")]
fn invalid_websocket_target_is_rejected(#[case] target: &str) {
    let input = private_key_pem(false);
    let credential = KalshiCredential::from_pem("synthetic", input.as_bytes()).unwrap();
    assert!(matches!(
        credential.websocket_headers_provider(Url::parse(target).unwrap()),
        Err(KalshiAuthError::InvalidUrl)
    ));
}

#[rstest]
#[case("ws://127.0.0.1/trade-api/ws/v2")]
#[case("ws://[::1]/trade-api/ws/v2")]
#[case("wss://example.test/trade-api/ws/v2")]
fn encrypted_or_literal_loopback_websocket_targets_are_accepted(#[case] target: &str) {
    let input = private_key_pem(false);
    let credential = KalshiCredential::from_pem("synthetic", input.as_bytes()).unwrap();
    assert!(
        credential
            .websocket_headers_provider(Url::parse(target).unwrap())
            .is_ok()
    );
}
