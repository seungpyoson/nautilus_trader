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

//! Kalshi protocol contracts for a native Rust data adapter.
//!
//! This crate discovers public market metadata and drives authenticated, fixed-selection orderbook
//! subscriptions with transport recovery and continuity checks. Its native data client publishes
//! instrument definitions and typed L2 inputs for engine-owned additive quantity conversion.
//! It contains no execution client or live-readiness authority.

mod auth;
mod book;
mod common;
mod config;
mod data;
mod factories;
mod http;
mod instruments;
mod metadata;
mod orderbook;
mod publication;
mod requests;
mod stream;
mod websocket;

#[cfg(test)]
mod tests;

pub use auth::{KalshiAuthError, KalshiCredential};
pub use common::{KALSHI, KALSHI_CLIENT_ID, KALSHI_VENUE};
pub use config::KalshiDataClientConfig;
pub use data::KalshiDataClient;
pub use factories::KalshiDataClientFactory;
pub use http::{
    KalshiHttpClient, KalshiHttpConfig, KalshiHttpError, KalshiMarketFilter,
    KalshiMarketFilterStatus,
};
pub use instruments::{KalshiInstrumentError, parse_instrument};
pub use metadata::{
    KalshiMarketMetadata, KalshiMarketStatus, KalshiMarketType, KalshiMarketsPage,
    KalshiMetadataError, KalshiPriceRange, decode_market_response, decode_markets_response,
};
pub use orderbook::{
    KalshiBookLevel, KalshiMarketSide, KalshiOrderbookDelta, KalshiOrderbookMessage,
    KalshiOrderbookSnapshot, decode_orderbook_message,
};
pub use stream::{KalshiOrderbookStream, KalshiStreamError, KalshiStreamState};
pub use websocket::{
    KalshiWebSocketClient, KalshiWebSocketConfig, KalshiWebSocketError, KalshiWebSocketEvent,
};
