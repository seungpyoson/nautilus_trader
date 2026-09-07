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

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::{KalshiHttpConfig, KalshiWebSocketConfig};

/// Explicit policy for a fixed-selection, L2 market data client.
///
/// Metadata is fetched at each connection and on explicit instrument requests. The WebSocket
/// reconnect controller restores the same selection; changed book rules require fresh snapshots.
/// Changing the selection requires disconnecting and recreating the data client.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KalshiDataClientConfig {
    /// Metadata tickers to bootstrap and stream, without ticker-based routing rules.
    pub market_tickers: Vec<String>,
    /// Public metadata transport policy.
    pub http: KalshiHttpConfig,
    /// Authenticated orderbook transport policy.
    pub websocket: KalshiWebSocketConfig,
    /// Total budget for metadata, connection and initial snapshots.
    pub bootstrap_timeout_ms: NonZeroU32,
    /// Maximum time to join the stream task before aborting and awaiting it.
    pub shutdown_timeout_ms: NonZeroU32,
}
