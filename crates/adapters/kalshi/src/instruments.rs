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

use nautilus_core::{Params, UnixNanos, correctness::CorrectnessError};
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, PriceGrid},
    types::{Currency, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::{KALSHI_VENUE, KalshiMarketMetadata, KalshiMarketStatus, KalshiMarketType};

/// An unsupported or invalid native instrument definition.
#[derive(Debug, Error)]
pub enum KalshiInstrumentError {
    /// The initial data slice supports only binary, one-dollar contracts.
    #[error("only binary Kalshi markets with a one-dollar notional are supported")]
    UnsupportedProduct,
    /// Unknown lifecycle states cannot be mapped implicitly.
    #[error("unknown Kalshi market lifecycle state")]
    UnknownStatus,
    /// The source bands cannot be represented without an assumption or rounding.
    #[error("unsupported Kalshi price ranges: require contiguous, aligned bands from zero to one")]
    UnsupportedPriceGrid,
    /// Lifecycle timestamps must be valid NT timestamps with a positive trading interval.
    #[error("invalid Kalshi market lifecycle timestamps")]
    InvalidTime,
    /// A native model correctness check failed.
    #[error(transparent)]
    InvalidDefinition(#[from] CorrectnessError),
}

/// Converts decoded metadata into a native instrument for the data-only slice.
///
/// Identity is the venue ticker on `KALSHI`; routing comes from `exchange_index`.
/// Contract activation and trading expiration use `open_time` and `close_time`.
/// The original JSON preserves settlement times and rules separately. Fees and
/// margins retain the model defaults and do not describe executable economics.
///
/// The supported grid is restricted to prices strictly between zero and one.
/// Outer endpoint eligibility is not established by the captured schema. Adjacent
/// source bands must meet at an aligned price; each shared boundary appears once.
/// Lifecycle state alone never authorizes publication or trading.
///
/// # Errors
///
/// Returns an error for unsupported products, unknown lifecycle states, inexact
/// grids, invalid timestamps, or invalid native instrument fields.
pub fn parse_instrument(
    market: &KalshiMarketMetadata,
    ts_init: UnixNanos,
) -> Result<BinaryOption, KalshiInstrumentError> {
    if market.market_type != KalshiMarketType::Binary
        || market.notional_value_dollars != Decimal::ONE
    {
        return Err(KalshiInstrumentError::UnsupportedProduct);
    }

    if market.status == KalshiMarketStatus::Unknown {
        return Err(KalshiInstrumentError::UnknownStatus);
    }

    if market.open_time >= market.close_time || market.close_time > market.latest_expiration_time {
        return Err(KalshiInstrumentError::InvalidTime);
    }
    let timestamp = |value: jiff::Timestamp| {
        u64::try_from(value.as_nanosecond())
            .map(UnixNanos::from)
            .map_err(|_| KalshiInstrumentError::InvalidTime)
    };
    timestamp(market.created_time)?;
    timestamp(market.latest_expiration_time)?;
    let grid = parse_grid(market)?;
    let symbol = Symbol::new_checked(&market.ticker)?;
    let mut info = Params::new();
    info.insert("exchange_index".into(), Value::from(market.exchange_index));
    info.insert(
        "event_ticker".into(),
        Value::from(market.event_ticker.clone()),
    );
    info.insert("kalshi_market_json".into(), Value::from(market.raw.get()));

    BinaryOption::builder()
        .instrument_id(InstrumentId::new(symbol, *KALSHI_VENUE))
        .raw_symbol(symbol)
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USD())
        .activation_ns(timestamp(market.open_time)?)
        .expiration_ns(timestamp(market.close_time)?)
        .price_precision(grid.precision())
        .size_precision(2)
        .price_increment(grid.min_increment())
        .size_increment(Quantity::from("0.01"))
        .min_price(grid.min_price())
        .max_price(grid.max_price())
        .price_grid(grid)
        .outcome("YES".into())
        .info(info)
        .ts_event(timestamp(market.updated_time)?)
        .ts_init(ts_init)
        .build()
        .map_err(Into::into)
}

fn parse_grid(market: &KalshiMarketMetadata) -> Result<PriceGrid, KalshiInstrumentError> {
    let precision = market
        .price_ranges
        .iter()
        .flat_map(|range| [range.start, range.end, range.step])
        .map(|value| value.normalize().scale())
        .max()
        .ok_or(KalshiInstrumentError::UnsupportedPriceGrid)?;
    if precision > u32::from(FIXED_PRECISION) {
        return Err(KalshiInstrumentError::UnsupportedPriceGrid);
    }
    let precision = precision as u8;
    let exact_price = |value: Decimal| {
        let price = Price::from_decimal_dp(value, precision)?;
        if price.as_decimal() != value {
            return Err(KalshiInstrumentError::UnsupportedPriceGrid);
        }
        Ok(price)
    };
    let mut previous_end = Decimal::ZERO;
    let mut ranges = Vec::with_capacity(market.price_ranges.len());

    for range in &market.price_ranges {
        if range.start != previous_end
            || range.start >= range.end
            || range.end > Decimal::ONE
            || range.step <= Decimal::ZERO
            || range.step > range.end - range.start
            || (range.end - range.start) % range.step != Decimal::ZERO
        {
            return Err(KalshiInstrumentError::UnsupportedPriceGrid);
        }
        let first = if range.start.is_zero() {
            range.step
        } else {
            range.start
        };
        let last = range.end - range.step;
        if first <= last {
            ranges.push((
                exact_price(first)?,
                exact_price(last)?,
                exact_price(range.step)?,
            ));
        }
        previous_end = range.end;
    }

    if previous_end != Decimal::ONE || ranges.is_empty() {
        return Err(KalshiInstrumentError::UnsupportedPriceGrid);
    }
    PriceGrid::new(ranges).map_err(Into::into)
}
