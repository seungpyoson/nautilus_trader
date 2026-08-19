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

//! Exact value validation for Polymarket execution reports.

use anyhow::{Context, ensure};
use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_SECOND};
use nautilus_model::{
    identifiers::{TradeId, VenueOrderId},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

pub(crate) fn positive_quantity(
    value: Decimal,
    precision: u8,
    field: &str,
) -> anyhow::Result<Quantity> {
    ensure!(
        value > Decimal::ZERO,
        "{field} must be positive, was {value}"
    );

    let quantity = Quantity::from_decimal_dp(value, precision)
        .with_context(|| format!("{field} {value} is not representable as Quantity"))?;
    ensure!(
        quantity.as_decimal() == value,
        "{field} {value} is not exactly representable at precision {precision}"
    );

    Ok(quantity)
}

pub(crate) fn positive_quantity_rounded(
    value: Decimal,
    precision: u8,
    field: &str,
) -> anyhow::Result<Quantity> {
    ensure!(
        value > Decimal::ZERO,
        "{field} must be positive, was {value}"
    );
    let quantity = Quantity::from_decimal_dp(value, precision)
        .with_context(|| format!("{field} {value} is not representable as Quantity"))?;
    ensure!(
        !quantity.is_zero(),
        "{field} {value} rounds to zero at precision {precision}"
    );
    Ok(quantity)
}

pub(crate) fn non_negative_quantity(
    value: Decimal,
    precision: u8,
    field: &str,
) -> anyhow::Result<Quantity> {
    ensure!(
        value >= Decimal::ZERO,
        "{field} must be non-negative, was {value}"
    );

    let quantity = Quantity::from_decimal_dp(value, precision)
        .with_context(|| format!("{field} {value} is not representable as Quantity"))?;
    ensure!(
        quantity.as_decimal() == value,
        "{field} {value} is not exactly representable at precision {precision}"
    );

    Ok(quantity)
}

pub(crate) fn binary_price(value: Decimal, precision: u8, field: &str) -> anyhow::Result<Price> {
    ensure!(
        value > Decimal::ZERO && value < Decimal::ONE,
        "{field} must satisfy 0 < price < 1, was {value}"
    );

    let price = Price::from_decimal_dp(value, precision)
        .with_context(|| format!("{field} {value} is not representable as Price"))?;
    ensure!(
        price.as_decimal() == value,
        "{field} {value} is not exactly representable at precision {precision}"
    );

    Ok(price)
}

/// Validates a fill price while preserving its exact venue-reported scale.
///
/// Historical fills can retain a price from an earlier tick regime, so validating them against
/// the instrument's current tick precision would reject valid economic evidence.
pub(crate) fn exact_binary_price(value: Decimal, field: &str) -> anyhow::Result<Price> {
    ensure!(
        value > Decimal::ZERO && value < Decimal::ONE,
        "{field} must satisfy 0 < price < 1, was {value}"
    );

    let normalized = value.normalize();
    let price = Price::from_decimal(normalized)
        .with_context(|| format!("{field} {value} is not exactly representable as Price"))?;
    ensure!(
        price.as_decimal() == value,
        "{field} {value} is not exactly representable as Price"
    );

    Ok(price)
}

pub(crate) fn venue_order_id(value: &str, field: &str) -> anyhow::Result<VenueOrderId> {
    VenueOrderId::new_checked(value)
        .with_context(|| format!("{field} {value:?} is not a valid venue order ID"))
}

pub(crate) fn trade_id(value: &str, field: &str) -> anyhow::Result<TradeId> {
    TradeId::new_checked(value)
        .with_context(|| format!("{field} {value:?} is not a valid trade ID"))
}

pub(crate) fn unix_seconds(value: u64, field: &str) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(NANOSECONDS_IN_SECOND)
        .map(UnixNanos::from)
        .with_context(|| format!("{field} {value} overflows Unix nanoseconds"))
}

pub(crate) fn condition_id(value: &str, field: &str) -> anyhow::Result<String> {
    let (prefix, hex) = value
        .split_at_checked(2)
        .with_context(|| format!("{field} {value:?} must start with a 0x prefix"))?;
    ensure!(
        prefix.eq_ignore_ascii_case("0x"),
        "{field} {value:?} must start with a 0x prefix"
    );
    ensure!(
        hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{field} {value:?} must contain exactly 32 hexadecimal bytes"
    );
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

pub(crate) fn token_id(value: &str, field: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "{field} {value:?} must be a non-empty decimal integer"
    );
    Ok(())
}
