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

use std::{collections::BTreeSet, num::NonZeroUsize};

use jiff::Timestamp;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;
use thiserror::Error;

use crate::common::{DecimalString, invalid, validate_ticker};

/// The source market payout classification, without an inferred fallback.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum KalshiMarketType {
    /// A binary payout.
    Binary,
    /// A scalar payout.
    Scalar,
    /// An unrecognized payout classification requiring explicit handling.
    #[serde(other)]
    Unknown,
}

/// The source market lifecycle state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum KalshiMarketStatus {
    /// The market has been initialized.
    Initialized,
    /// The market is inactive.
    Inactive,
    /// The market is active.
    Active,
    /// Trading has closed.
    Closed,
    /// The result has been determined.
    Determined,
    /// The result is disputed.
    Disputed,
    /// The result has been amended.
    Amended,
    /// The result is finalized.
    Finalized,
    /// An unrecognized state which cannot imply active trading.
    #[serde(other)]
    Unknown,
}

/// An exact, metadata-defined price band in dollars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KalshiPriceRange {
    /// The start of the source price band.
    pub start: Decimal,
    /// The end of the source price band.
    pub end: Decimal,
    /// The source tick size within this band.
    pub step: Decimal,
}

/// A market definition projection, separate from REST quotes and trading permission.
///
/// The original market object is retained without interpreting unrelated numeric
/// fields through floating point. Settlement details and future source fields stay
/// available in `raw`. Consumers must explicitly validate their supported product
/// capabilities before constructing or publishing an instrument.
#[derive(Clone, Debug)]
pub struct KalshiMarketMetadata {
    /// The canonical venue market ticker, also used for subscriptions.
    pub ticker: String,
    /// The source event identity.
    pub event_ticker: String,
    /// The source payout classification.
    pub market_type: KalshiMarketType,
    /// The source lifecycle state, independent of query filter names.
    pub status: KalshiMarketStatus,
    /// The source YES outcome description.
    pub yes_sub_title: String,
    /// The source NO outcome description.
    pub no_sub_title: String,
    /// The source market creation time.
    pub created_time: Timestamp,
    /// The source time of the last non-trading metadata update.
    pub updated_time: Timestamp,
    /// The source opening time.
    pub open_time: Timestamp,
    /// The source trading close time.
    pub close_time: Timestamp,
    /// The source latest expiration time, kept separate from trading close.
    pub latest_expiration_time: Timestamp,
    /// The settlement delay after determination, in seconds.
    pub settlement_timer_seconds: u64,
    /// The source notional value of one contract at settlement.
    pub notional_value_dollars: Decimal,
    /// The human-readable grid label; it never selects pricing behavior.
    pub price_level_structure: String,
    /// The authoritative source price bands, without reduction to a single tick.
    pub price_ranges: Vec<KalshiPriceRange>,
    /// The explicit venue routing shard; absence never defaults to zero.
    pub exchange_index: u32,
    /// Whether the venue allows this market to close early.
    pub can_close_early: bool,
    /// The source primary settlement rules, which may be empty for composite markets.
    pub rules_primary: String,
    /// The source secondary settlement rules.
    pub rules_secondary: String,
    /// The complete original market JSON object, including product and settlement details.
    pub raw: Box<RawValue>,
}

/// One bounded discovery response, without automatic pagination or partial success.
#[derive(Clone, Debug)]
pub struct KalshiMarketsPage {
    /// The decoded market definitions in source order.
    pub markets: Vec<KalshiMarketMetadata>,
    /// The opaque source cursor; an empty string denotes the last page.
    pub cursor: String,
}

/// Metadata failures which cannot produce a partial instrument definition.
#[derive(Debug, Error)]
pub enum KalshiMetadataError {
    /// A response exceeds its configured byte bound.
    #[error("Kalshi metadata response exceeds configured byte limit")]
    ResponseTooLarge,
    /// A page exceeds its configured market count bound.
    #[error("Kalshi metadata response exceeds configured market count limit")]
    TooManyMarkets,
    /// A source field is missing, malformed or inconsistent.
    #[error("Invalid Kalshi metadata: {0}")]
    Decode(#[from] serde_json::Error),
    /// A page repeats a canonical market ticker.
    #[error("Kalshi metadata response repeats a market ticker")]
    DuplicateMarket,
    /// A single-market response does not match its request.
    #[error("Kalshi metadata response does not match the requested ticker")]
    MarketMismatch,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePage {
    markets: Vec<Box<RawValue>>,
    cursor: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireResponse {
    market: Box<RawValue>,
}

// This projection requires every field it uses; unrelated quote/statistic fields
// are retained in RawValue and cannot supply metadata or readiness defaults.
#[derive(Deserialize)]
struct WireMetadata {
    ticker: String,
    event_ticker: String,
    market_type: KalshiMarketType,
    status: KalshiMarketStatus,
    yes_sub_title: String,
    no_sub_title: String,
    created_time: Timestamp,
    updated_time: Timestamp,
    open_time: Timestamp,
    close_time: Timestamp,
    latest_expiration_time: Timestamp,
    settlement_timer_seconds: u64,
    notional_value_dollars: DecimalString,
    price_level_structure: String,
    price_ranges: Vec<WireRange>,
    exchange_index: u32,
    can_close_early: bool,
    rules_primary: String,
    rules_secondary: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRange {
    start: DecimalString,
    end: DecimalString,
    step: DecimalString,
}

fn decode_definition(raw: Box<RawValue>) -> Result<KalshiMarketMetadata, serde_json::Error> {
    let wire: WireMetadata = serde_json::from_str(raw.get())?;
    validate_ticker(&wire.ticker)?;
    validate_ticker(&wire.event_ticker)?;

    for timestamp in [
        wire.created_time,
        wire.updated_time,
        wire.open_time,
        wire.close_time,
        wire.latest_expiration_time,
    ] {
        if u64::try_from(timestamp.as_nanosecond()).is_err() {
            return Err(invalid("market time is outside the NT nanosecond range"));
        }
    }

    if wire.open_time >= wire.close_time || wire.close_time > wire.latest_expiration_time {
        return Err(invalid(
            "market opening, closing and expiration times are inconsistent",
        ));
    }

    if wire.notional_value_dollars.0 <= Decimal::ZERO || wire.price_ranges.is_empty() {
        return Err(invalid(
            "market requires a positive notional value and price ranges",
        ));
    }

    let mut ranges: Vec<KalshiPriceRange> = Vec::with_capacity(wire.price_ranges.len());

    for range in wire.price_ranges {
        let (start, end, step) = (range.start.0, range.end.0, range.step.0);

        if start < Decimal::ZERO
            || start >= end
            || end > wire.notional_value_dollars.0
            || step <= Decimal::ZERO
            || step > end - start
            || ranges.last().is_some_and(|previous| previous.end > start)
        {
            return Err(invalid(
                "market price ranges are invalid, overlapping or unsorted",
            ));
        }

        ranges.push(KalshiPriceRange { start, end, step });
    }

    Ok(KalshiMarketMetadata {
        ticker: wire.ticker,
        event_ticker: wire.event_ticker,
        market_type: wire.market_type,
        status: wire.status,
        yes_sub_title: wire.yes_sub_title,
        no_sub_title: wire.no_sub_title,
        created_time: wire.created_time,
        updated_time: wire.updated_time,
        open_time: wire.open_time,
        close_time: wire.close_time,
        latest_expiration_time: wire.latest_expiration_time,
        settlement_timer_seconds: wire.settlement_timer_seconds,
        notional_value_dollars: wire.notional_value_dollars.0,
        price_level_structure: wire.price_level_structure,
        price_ranges: ranges,
        exchange_index: wire.exchange_index,
        can_close_early: wire.can_close_early,
        rules_primary: wire.rules_primary,
        rules_secondary: wire.rules_secondary,
        raw,
    })
}

/// Decodes one discovery page and rejects the whole page on a definition failure.
///
/// The caller owns bounded pagination, source capture and instrument publication.
/// Market metadata does not contain a UUID; stream UUIDs must come from the venue's
/// authenticated snapshot for the selected ticker.
///
/// # Errors
///
/// Returns an error for exceeded bounds, invalid definitions, a missing cursor,
/// or duplicate market tickers. A missing exchange index is always an error.
pub fn decode_markets_response(
    bytes: &[u8],
    max_response_bytes: NonZeroUsize,
    max_markets: NonZeroUsize,
) -> Result<KalshiMarketsPage, KalshiMetadataError> {
    if bytes.len() > max_response_bytes.get() {
        return Err(KalshiMetadataError::ResponseTooLarge);
    }

    let page: WirePage = serde_json::from_slice(bytes)?;

    if page.markets.len() > max_markets.get() {
        return Err(KalshiMetadataError::TooManyMarkets);
    }

    let mut tickers = BTreeSet::new();
    let mut markets = Vec::with_capacity(page.markets.len());

    for raw in page.markets {
        let market = decode_definition(raw)?;

        if !tickers.insert(market.ticker.clone()) {
            return Err(KalshiMetadataError::DuplicateMarket);
        }

        markets.push(market);
    }

    Ok(KalshiMarketsPage {
        markets,
        cursor: page.cursor,
    })
}

/// Decodes a single-market response bound to the requested ticker.
///
/// # Errors
///
/// Returns an error for an exceeded byte bound, an invalid definition or a ticker
/// which does not match the request exactly.
pub fn decode_market_response(
    bytes: &[u8],
    expected_ticker: &str,
    max_response_bytes: NonZeroUsize,
) -> Result<KalshiMarketMetadata, KalshiMetadataError> {
    if bytes.len() > max_response_bytes.get() {
        return Err(KalshiMetadataError::ResponseTooLarge);
    }

    let response: WireResponse = serde_json::from_slice(bytes)?;
    let market = decode_definition(response.market)?;

    if market.ticker != expected_ticker {
        return Err(KalshiMetadataError::MarketMismatch);
    }

    Ok(market)
}
