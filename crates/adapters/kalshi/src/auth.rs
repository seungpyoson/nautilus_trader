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

//! Kalshi GET authentication from caller-injected RSA credentials.

use std::{fmt::Debug, sync::Arc};

use aws_lc_rs::{rand::SystemRandom, rsa::KeyPair, signature::RSA_PSS_SHA256};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::HeaderValue;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_network::{http::Url, transport::TransportError, websocket::HeadersProvider};
use thiserror::Error;
use zeroize::Zeroizing;

const MAX_PRIVATE_KEY_PEM_BYTES: usize = 64 * 1024;

/// Authentication failures, without credential material in diagnostics.
#[derive(Debug, Error)]
pub enum KalshiAuthError {
    /// The injected key ID is empty or cannot be represented as an HTTP header.
    #[error("Invalid Kalshi key ID")]
    InvalidKeyId,
    /// The injected PEM exceeds the supported input bound.
    #[error("Kalshi private key PEM exceeds 64 KiB")]
    KeyTooLarge,
    /// The input does not contain exactly one unencrypted RSA private key.
    #[error("Invalid Kalshi RSA private key PEM")]
    InvalidPrivateKey,
    /// The request URL is not an absolute HTTP or WebSocket URL without user information.
    #[error("Invalid Kalshi authentication URL")]
    InvalidUrl,
    /// The signing primitive failed.
    #[error("Kalshi RSA-PSS signing failed")]
    SigningFailed,
}

/// A caller-injected key ID and parsed RSA private key for data requests.
///
/// Accepts PKCS#1 and PKCS#8 PEM encodings. Decoded private-key buffers are zeroized after parsing;
/// the caller owns the lifetime of its input buffer. Clones share the parsed key. This type does
/// not read files, environment variables or credential stores, and cannot serialize its key.
#[derive(Clone)]
pub struct KalshiCredential {
    key_id: String,
    key_pair: Arc<KeyPair>,
}

impl Debug for KalshiCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(KalshiCredential))
            .finish_non_exhaustive()
    }
}

impl KalshiCredential {
    /// Parses an injected key once for reuse by connection attempts.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid key ID, oversized PEM, or unsupported private key.
    pub fn from_pem(key_id: &str, private_key_pem: &[u8]) -> Result<Self, KalshiAuthError> {
        if key_id.is_empty() || key_id.trim() != key_id || HeaderValue::from_str(key_id).is_err() {
            return Err(KalshiAuthError::InvalidKeyId);
        }

        if private_key_pem.len() > MAX_PRIVATE_KEY_PEM_BYTES {
            return Err(KalshiAuthError::KeyTooLarge);
        }
        let blocks = pem::parse_many(private_key_pem)
            .map_err(|_| KalshiAuthError::InvalidPrivateKey)?
            .into_iter()
            .map(|block| {
                (
                    block.tag().to_string(),
                    Zeroizing::new(block.into_contents()),
                )
            })
            .collect::<Vec<_>>();
        let [(tag, der)] = blocks.as_slice() else {
            return Err(KalshiAuthError::InvalidPrivateKey);
        };
        let key_pair = match tag.as_str() {
            "RSA PRIVATE KEY" => KeyPair::from_der(der),
            "PRIVATE KEY" => KeyPair::from_pkcs8(der),
            _ => return Err(KalshiAuthError::InvalidPrivateKey),
        }
        .map_err(|_| KalshiAuthError::InvalidPrivateKey)?;
        Ok(Self {
            key_id: key_id.to_string(),
            key_pair: Arc::new(key_pair),
        })
    }

    /// Signs the exact encoded URL path for a GET request using a millisecond timestamp.
    ///
    /// The signature uses RSA-PSS with SHA-256, MGF1-SHA256 and a 32-byte salt. Query parameters,
    /// fragments and the origin are excluded from the canonical message, as required by Kalshi.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request URL or a signing failure.
    pub fn sign_get(
        &self,
        url: &Url,
        timestamp: UnixNanos,
    ) -> Result<Vec<(String, String)>, KalshiAuthError> {
        if !matches!(url.scheme(), "https" | "http" | "wss" | "ws")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(KalshiAuthError::InvalidUrl);
        }
        let timestamp = (timestamp.as_u64() / 1_000_000).to_string();
        let message = format!("{timestamp}GET{}", url.path());
        let mut signature = vec![0; self.key_pair.public_modulus_len()];
        self.key_pair
            .sign(
                &RSA_PSS_SHA256,
                &SystemRandom::new(),
                message.as_bytes(),
                &mut signature,
            )
            .map_err(|_| KalshiAuthError::SigningFailed)?;
        Ok(vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), timestamp),
            (
                "KALSHI-ACCESS-SIGNATURE".to_string(),
                STANDARD.encode(signature),
            ),
        ])
    }

    /// Creates a provider which signs afresh before every WebSocket connection attempt.
    ///
    /// # Errors
    ///
    /// Returns an error unless the target uses TLS or a literal loopback address,
    /// and is a WebSocket URL without user information or a fragment.
    pub fn websocket_headers_provider(&self, url: Url) -> Result<HeadersProvider, KalshiAuthError> {
        if !matches!(url.scheme(), "wss" | "ws")
            || (url.scheme() == "ws"
                && !url.host_str().is_some_and(|host| {
                    host.trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
                }))
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(KalshiAuthError::InvalidUrl);
        }
        let credential = self.clone();
        Ok(Arc::new(move || {
            credential
                .sign_get(&url, get_atomic_clock_realtime().get_time_ns())
                .map_err(|e| TransportError::Other(e.to_string()))
        }))
    }
}
