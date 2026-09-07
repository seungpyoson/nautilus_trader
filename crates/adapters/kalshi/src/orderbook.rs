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

use std::num::NonZeroU64;

use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{DecimalString, invalid, present_value, validate_identity};

/// A YES or NO side in the Kalshi orderbook protocol.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum KalshiMarketSide {
    /// The YES side.
    Yes,
    /// The NO side.
    No,
}

/// A price and absolute contract quantity in a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KalshiBookLevel {
    /// The exact dollar price.
    pub price_dollars: Decimal,
    /// The exact, nonnegative number of contracts.
    pub quantity: Decimal,
}

/// A complete snapshot for one market within a subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KalshiOrderbookSnapshot {
    /// The venue market ticker, without an inferred asset or market family.
    pub market_ticker: String,
    /// The venue market UUID.
    pub market_id: Uuid,
    /// The YES price levels; an omitted wire field denotes an empty side.
    pub yes: Vec<KalshiBookLevel>,
    /// The NO price levels; an omitted wire field denotes an empty side.
    pub no: Vec<KalshiBookLevel>,
}

/// An additive quantity change for one market price level.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KalshiOrderbookDelta {
    /// The venue market ticker.
    pub market_ticker: String,
    /// The venue market UUID.
    pub market_id: Uuid,
    /// The price level in dollars.
    pub price_dollars: Decimal,
    /// The signed change in contracts, not a replacement absolute quantity.
    pub delta: Decimal,
    /// The side to update.
    pub side: KalshiMarketSide,
    /// The optional source event timestamp in milliseconds, without a fallback.
    pub ts_ms: Option<u64>,
}

/// A decoded orderbook frame retaining subscription identity and sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KalshiOrderbookMessage {
    /// A snapshot replacing the complete book for its market.
    Snapshot {
        /// The server-assigned subscription ID.
        sid: NonZeroU64,
        /// The sequence number in the subscription.
        seq: NonZeroU64,
        /// The snapshot content.
        snapshot: KalshiOrderbookSnapshot,
    },
    /// An additive update requiring a valid snapshot and sequence continuity.
    Delta {
        /// The server-assigned subscription ID.
        sid: NonZeroU64,
        /// The sequence number in the subscription.
        seq: NonZeroU64,
        /// The update content.
        delta: KalshiOrderbookDelta,
    },
}

// Private wire models prevent callers from constructing a decoded frame through
// another deserializer with different field or precision rules.
#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum WireMessage {
    #[serde(rename = "subscribed")]
    Subscribed {
        #[serde(default, deserialize_with = "present_value")]
        id: Option<u64>,
        msg: WireSubscribed,
    },
    #[serde(rename = "orderbook_snapshot")]
    Snapshot {
        sid: NonZeroU64,
        seq: NonZeroU64,
        msg: WireSnapshot,
    },
    #[serde(rename = "orderbook_delta")]
    Delta {
        sid: NonZeroU64,
        seq: NonZeroU64,
        msg: WireDelta,
    },
    #[serde(rename = "ok")]
    Ok {
        #[serde(default, rename = "id", deserialize_with = "present_value")]
        _id: Option<u64>,
        #[serde(default, deserialize_with = "present_value")]
        sid: Option<NonZeroU64>,
        #[serde(default, deserialize_with = "present_value")]
        seq: Option<NonZeroU64>,
        #[serde(default, deserialize_with = "present_value")]
        msg: Option<SubscriptionMarkets>,
    },
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        #[serde(default, rename = "id", deserialize_with = "present_value")]
        _id: Option<u64>,
        sid: NonZeroU64,
        seq: NonZeroU64,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(default, rename = "id", deserialize_with = "present_value")]
        _id: Option<u64>,
        #[serde(default, deserialize_with = "present_value")]
        sid: Option<NonZeroU64>,
        #[serde(default, deserialize_with = "present_value")]
        seq: Option<NonZeroU64>,
        msg: WireError,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubscriptionMarkets {
    #[serde(default, deserialize_with = "present_value")]
    pub(crate) market_tickers: Option<Vec<String>>,
    #[serde(default, deserialize_with = "present_value")]
    pub(crate) market_ids: Option<Vec<Uuid>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireError {
    code: i64,
    #[serde(rename = "msg")]
    _message: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSubscribed {
    channel: String,
    sid: NonZeroU64,
}

pub(crate) enum StreamMessage {
    Subscribed {
        id: Option<u64>,
        channel: String,
        sid: NonZeroU64,
    },
    Book(KalshiOrderbookMessage),
    Ok {
        sid: Option<NonZeroU64>,
        seq: Option<NonZeroU64>,
        markets: Option<SubscriptionMarkets>,
    },
    Unsubscribed {
        sid: NonZeroU64,
        seq: NonZeroU64,
    },
    Error {
        sid: Option<NonZeroU64>,
        seq: Option<NonZeroU64>,
        code: i64,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSnapshot {
    market_ticker: String,
    market_id: Uuid,
    #[serde(default)]
    yes_dollars_fp: Vec<(DecimalString, DecimalString)>,
    #[serde(default)]
    no_dollars_fp: Vec<(DecimalString, DecimalString)>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDelta {
    market_ticker: String,
    market_id: Uuid,
    price_dollars: DecimalString,
    delta_fp: DecimalString,
    side: KalshiMarketSide,
    #[serde(default, deserialize_with = "present_value")]
    ts_ms: Option<u64>,
    #[serde(default, rename = "ts", deserialize_with = "present_value")]
    _deprecated_ts: Option<String>,
    #[serde(
        default,
        rename = "client_order_id",
        deserialize_with = "present_value"
    )]
    _client_order_id: Option<String>,
    #[serde(default, rename = "subaccount", deserialize_with = "present_value")]
    _subaccount: Option<u64>,
}

fn decode_levels(
    levels: Vec<(DecimalString, DecimalString)>,
) -> Result<Vec<KalshiBookLevel>, serde_json::Error> {
    levels
        .into_iter()
        .map(|(price, quantity)| {
            if price.0 < Decimal::ZERO || quantity.0 < Decimal::ZERO {
                return Err(invalid("snapshot price and quantity must be nonnegative"));
            }
            Ok(KalshiBookLevel {
                price_dollars: price.0,
                quantity: quantity.0,
            })
        })
        .collect()
}

/// Decodes one Kalshi orderbook snapshot or delta frame.
///
/// Omitted snapshot side arrays denote empty sides under the official protocol.
/// Explicit nulls, malformed fields and inexact decimals are rejected. The result
/// retains the market identity, subscription ID, sequence and optional source time;
/// it does not establish stream continuity, market binding or publication authority.
/// The caller must enforce its configured frame-size limit before decoding.
///
/// # Errors
///
/// Returns an error for malformed JSON, an unsupported message type, missing or
/// invalid identities, invalid level pairs, or unrepresentable numeric values.
pub fn decode_orderbook_message(bytes: &[u8]) -> Result<KalshiOrderbookMessage, serde_json::Error> {
    match decode_stream_message(bytes)? {
        StreamMessage::Book(message) => Ok(message),
        _ => Err(invalid("expected an orderbook snapshot or delta")),
    }
}

pub(crate) fn decode_stream_message(bytes: &[u8]) -> Result<StreamMessage, serde_json::Error> {
    match serde_json::from_slice::<WireMessage>(bytes)? {
        WireMessage::Subscribed { id, msg } => Ok(StreamMessage::Subscribed {
            id,
            channel: msg.channel,
            sid: msg.sid,
        }),
        WireMessage::Snapshot { sid, seq, msg } => {
            validate_identity(&msg.market_ticker, msg.market_id)?;
            let yes = decode_levels(msg.yes_dollars_fp)?;
            let no = decode_levels(msg.no_dollars_fp)?;
            Ok(StreamMessage::Book(KalshiOrderbookMessage::Snapshot {
                sid,
                seq,
                snapshot: KalshiOrderbookSnapshot {
                    market_ticker: msg.market_ticker,
                    market_id: msg.market_id,
                    yes,
                    no,
                },
            }))
        }
        WireMessage::Delta { sid, seq, msg } => {
            validate_identity(&msg.market_ticker, msg.market_id)?;
            if msg.price_dollars.0 < Decimal::ZERO {
                return Err(invalid("delta price must be nonnegative"));
            }
            Ok(StreamMessage::Book(KalshiOrderbookMessage::Delta {
                sid,
                seq,
                delta: KalshiOrderbookDelta {
                    market_ticker: msg.market_ticker,
                    market_id: msg.market_id,
                    price_dollars: msg.price_dollars.0,
                    delta: msg.delta_fp.0,
                    side: msg.side,
                    ts_ms: msg.ts_ms,
                },
            }))
        }
        WireMessage::Ok { sid, seq, msg, .. } => Ok(StreamMessage::Ok {
            sid,
            seq,
            markets: msg,
        }),
        WireMessage::Unsubscribed { sid, seq, .. } => Ok(StreamMessage::Unsubscribed { sid, seq }),
        WireMessage::Error { sid, seq, msg, .. } => Ok(StreamMessage::Error {
            sid,
            seq,
            code: msg.code,
        }),
    }
}
