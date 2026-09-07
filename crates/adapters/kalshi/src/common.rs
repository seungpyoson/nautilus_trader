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

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de};
use uuid::Uuid;

/// Canonical adapter identity.
pub const KALSHI: &str = "KALSHI";

/// Canonical venue instance.
pub static KALSHI_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(KALSHI));

/// Default client identity; factory callers may supply a different client name.
pub static KALSHI_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(KALSHI));

pub(crate) struct DecimalString(pub(crate) Decimal);

pub(crate) fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl<'de> Deserialize<'de> for DecimalString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let unsigned = value.strip_prefix('-').unwrap_or(&value);
        let mut parts = unsigned.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();

        if integer.is_empty()
            || !integer.bytes().all(|c| c.is_ascii_digit())
            || fraction.is_some_and(|p| p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()))
            || parts.next().is_some()
        {
            return Err(de::Error::custom("expected a fixed-point decimal string"));
        }

        Decimal::from_str_exact(&value)
            .map(Self)
            .map_err(|_| de::Error::custom("decimal is not exactly representable"))
    }
}

pub(crate) fn invalid(message: &'static str) -> serde_json::Error {
    <serde_json::Error as de::Error>::custom(message)
}

pub(crate) fn validate_ticker(ticker: &str) -> Result<(), serde_json::Error> {
    if ticker.is_empty() || ticker.trim() != ticker || ticker.chars().any(char::is_control) {
        return Err(invalid(
            "market ticker must be nonempty without surrounding whitespace or controls",
        ));
    }
    Ok(())
}

pub(crate) fn validate_identity(ticker: &str, market_id: Uuid) -> Result<(), serde_json::Error> {
    validate_ticker(ticker)?;

    if market_id.is_nil() {
        return Err(invalid("market ID must not be nil"));
    }
    Ok(())
}
