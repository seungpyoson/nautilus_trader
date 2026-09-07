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

//! Instrument-owned price grids with exact, bounded price-level navigation.

use std::fmt::Display;

use nautilus_core::correctness::{CorrectnessError, CorrectnessResult, check_predicate_true};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

use super::tick_scheme::TickSchemeRule;
use crate::types::{
    Price,
    fixed::{FIXED_PRECISION, check_fixed_precision},
    price::{PRICE_RAW_MAX, PRICE_RAW_MIN},
};

/// A finite union of disjoint arithmetic progressions of valid prices.
///
/// Each triple is `(first, last, step)`, with both endpoints included and on the
/// grid. Ranges are ordered and cannot overlap. Gaps and negative prices are
/// supported. All values have the same precision. Construction and deserialization
/// validate these invariants; navigation uses integer arithmetic without expanding
/// the ranges into individual ticks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<(Price, Price, Price)>",
    into = "Vec<(Price, Price, Price)>"
)]
pub struct PriceGrid {
    ranges: Vec<(Price, Price, Price)>,
}

#[allow(
    clippy::useless_conversion,
    reason = "PriceRaw is i64 or i128 depending on the high-precision feature"
)]
impl PriceGrid {
    /// Creates a price grid from inclusive `(first, last, step)` triples.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, unordered, overlapping, inexact, or invalid ranges.
    pub fn new(ranges: Vec<(Price, Price, Price)>) -> CorrectnessResult<Self> {
        check_predicate_true(
            !ranges.is_empty(),
            "price grid must contain at least one range",
        )?;
        let precision = ranges[0].0.precision;
        check_fixed_precision(precision)?;
        let mut previous_last = None;

        for &(first, last, step) in &ranges {
            for price in [first, last, step] {
                check_predicate_true(
                    price.precision == precision
                        && (PRICE_RAW_MIN..=PRICE_RAW_MAX).contains(&price.raw),
                    "price grid values must be valid prices with matching precision",
                )?;
                let checked = Price::from_decimal_dp(price.as_decimal(), precision)?;
                check_predicate_true(
                    checked.raw == price.raw,
                    "price grid values must be exact at the declared precision",
                )?;
            }
            check_predicate_true(first <= last, "price grid first must not exceed last")?;
            check_predicate_true(step.raw > 0, "price grid step must be positive")?;
            check_predicate_true(
                previous_last.is_none_or(|previous| first > previous),
                "price grid ranges must be ordered and disjoint",
            )?;
            let width = i128::from(last.raw).checked_sub(i128::from(first.raw));
            check_predicate_true(
                width.is_some_and(|width| width % i128::from(step.raw) == 0),
                "price grid last must be reachable exactly from first",
            )?;
            previous_last = Some(last);
        }

        Ok(Self { ranges })
    }

    /// Returns the inclusive `(first, last, step)` ranges.
    #[must_use]
    pub fn ranges(&self) -> &[(Price, Price, Price)] {
        &self.ranges
    }

    /// Returns the precision shared by all grid prices.
    #[must_use]
    pub fn precision(&self) -> u8 {
        self.ranges[0].0.precision
    }

    /// Returns the smallest declared step, without replacing the full grid.
    #[must_use]
    pub fn min_increment(&self) -> Price {
        self.ranges
            .iter()
            .map(|range| range.2)
            .fold(self.ranges[0].2, std::cmp::min)
    }

    /// Returns the least grid price.
    #[must_use]
    pub fn min_price(&self) -> Price {
        self.ranges[0].0
    }

    /// Returns the greatest grid price.
    #[must_use]
    pub fn max_price(&self) -> Price {
        self.ranges[self.ranges.len() - 1].1
    }

    /// Converts a decimal only if it is exactly representable and on this grid.
    #[must_use]
    pub fn price_from_decimal(&self, value: Decimal) -> Option<Price> {
        let price = Price::from_decimal_dp(value, self.precision()).ok()?;
        (price.as_decimal() == value && self.contains(price)).then_some(price)
    }

    /// Returns whether the exact price is on this grid.
    #[must_use]
    pub fn contains(&self, value: Price) -> bool {
        Self::is_compatible_price(value)
            && self.ranges.iter().any(|&(first, last, step)| {
                value >= first
                    && value <= last
                    && (i128::from(value.raw) - i128::from(first.raw)) % i128::from(step.raw) == 0
            })
    }

    /// Returns the price `n` ticks below the greatest grid price at or below `value`.
    #[must_use]
    pub fn next_bid_price(&self, value: Price, n: u32) -> Option<Price> {
        if !Self::is_compatible_price(value) {
            return None;
        }
        let mut remaining = i128::from(n);

        for &(first, last, step) in self.ranges.iter().rev() {
            if value < first {
                continue;
            }
            let first_raw = i128::from(first.raw);
            let step_raw = i128::from(step.raw);
            let available = (i128::from(value.min(last).raw) - first_raw) / step_raw;
            if remaining <= available {
                let raw = first_raw + (available - remaining) * step_raw;
                return Price::from_raw_checked(raw.try_into().ok()?, self.precision()).ok();
            }
            remaining -= available + 1;
        }
        None
    }

    /// Returns the price `n` ticks above the least grid price at or above `value`.
    #[must_use]
    pub fn next_ask_price(&self, value: Price, n: u32) -> Option<Price> {
        if !Self::is_compatible_price(value) {
            return None;
        }
        let mut remaining = i128::from(n);

        for &(first, last, step) in &self.ranges {
            if value > last {
                continue;
            }
            let last_raw = i128::from(last.raw);
            let step_raw = i128::from(step.raw);
            let available = (last_raw - i128::from(value.max(first).raw)) / step_raw;
            if remaining <= available {
                let raw = last_raw - (available - remaining) * step_raw;
                return Price::from_raw_checked(raw.try_into().ok()?, self.precision()).ok();
            }
            remaining -= available + 1;
        }
        None
    }

    fn is_compatible_price(value: Price) -> bool {
        value.precision <= FIXED_PRECISION && (PRICE_RAW_MIN..=PRICE_RAW_MAX).contains(&value.raw)
    }

    fn directional_bound(value: f64, strategy: RoundingStrategy) -> Option<Price> {
        let value = value.to_string().parse::<Decimal>().ok()?;
        let bound = value.round_dp_with_strategy(FIXED_PRECISION.into(), strategy);
        Price::from_decimal_dp(bound, FIXED_PRECISION).ok()
    }
}

impl TryFrom<Vec<(Price, Price, Price)>> for PriceGrid {
    type Error = CorrectnessError;

    fn try_from(ranges: Vec<(Price, Price, Price)>) -> Result<Self, Self::Error> {
        Self::new(ranges)
    }
}

impl From<PriceGrid> for Vec<(Price, Price, Price)> {
    fn from(grid: PriceGrid) -> Self {
        grid.ranges
    }
}

impl Display for PriceGrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PriceGrid({} ranges)", self.ranges.len())
    }
}

impl TickSchemeRule for PriceGrid {
    fn next_bid_price(&self, value: f64, n: i32, precision: u8) -> Option<Price> {
        if precision != self.precision() {
            return None;
        }
        self.next_bid_price(
            Self::directional_bound(value, RoundingStrategy::ToNegativeInfinity)?,
            n.try_into().ok()?,
        )
    }

    fn next_ask_price(&self, value: f64, n: i32, precision: u8) -> Option<Price> {
        if precision != self.precision() {
            return None;
        }
        self.next_ask_price(
            Self::directional_bound(value, RoundingStrategy::ToPositiveInfinity)?,
            n.try_into().ok()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::types::{ERROR_PRICE, PRICE_ERROR, PRICE_UNDEF};

    #[fixture]
    fn grid() -> PriceGrid {
        PriceGrid::new(vec![
            ("0.0000".into(), "0.0099".into(), "0.0001".into()),
            ("0.0100".into(), "0.9890".into(), "0.0010".into()),
            ("0.9900".into(), "1.0000".into(), "0.0001".into()),
        ])
        .unwrap()
    }

    #[rstest]
    #[case("0.0100", 0, Some("0.0100"), Some("0.0100"))]
    #[case("0.0100", 1, Some("0.0099"), Some("0.0110"))]
    #[case("0.0105", 0, Some("0.0100"), Some("0.0110"))]
    #[case("0.9900", 1, Some("0.9890"), Some("0.9901"))]
    #[case("0.9900", 980, Some("0.0100"), None)]
    #[case("0.0100", 981, None, Some("0.9901"))]
    #[case("-1", 0, None, Some("0.0000"))]
    #[case("2", 0, Some("1.0000"), None)]
    #[case("0.5", u32::MAX, None, None)]
    fn test_navigation(
        grid: PriceGrid,
        #[case] value: &str,
        #[case] n: u32,
        #[case] bid: Option<&str>,
        #[case] ask: Option<&str>,
    ) {
        assert_eq!(grid.next_bid_price(value.into(), n), bid.map(Price::from));
        assert_eq!(grid.next_ask_price(value.into(), n), ask.map(Price::from));
    }

    #[rstest]
    fn test_navigation_matches_enumerated_prices() {
        let grid = PriceGrid::new(vec![
            ("-0.03".into(), "0.07".into(), "0.05".into()),
            ("0.12".into(), "0.14".into(), "0.02".into()),
            ("0.20".into(), "0.20".into(), "0.03".into()),
        ])
        .unwrap();
        let ticks: Vec<Price> = ["-0.03", "0.02", "0.07", "0.12", "0.14", "0.20"]
            .map(Price::from)
            .into();

        for cents in -10..30 {
            let value = Price::from_decimal_dp(Decimal::new(cents, 2), 2).unwrap();
            assert_eq!(grid.contains(value), ticks.contains(&value));
            for n in 0..8 {
                let bid = ticks.iter().rev().filter(|&&tick| tick <= value).nth(n);
                let ask = ticks.iter().filter(|&&tick| tick >= value).nth(n);
                assert_eq!(grid.next_bid_price(value, n as u32), bid.copied());
                assert_eq!(grid.next_ask_price(value, n as u32), ask.copied());
            }
        }
    }

    #[rstest]
    fn test_decimal_conversion_rejects_rounding_and_off_grid(grid: PriceGrid) {
        assert_eq!(
            grid.price_from_decimal(dec!(0.011000)),
            Some("0.0110".into())
        );
        assert_eq!(grid.price_from_decimal(dec!(0.0105)), None);
        assert_eq!(grid.price_from_decimal(dec!(0.01100000001)), None);
        assert_eq!(grid.price_from_decimal(Decimal::MAX), None);
        assert_eq!(grid.price_from_decimal(dec!(-0.0001)), None);
    }

    #[rstest]
    fn test_existing_tick_rule_interface(grid: PriceGrid) {
        let rule: &dyn TickSchemeRule = &grid;
        assert_eq!(rule.next_bid_price(0.01, 1, 4), Some("0.0099".into()));
        assert_eq!(rule.next_ask_price(0.99, 1, 4), Some("0.9901".into()));
        assert_eq!(rule.next_bid_price(0.01, -1, 4), None);
        assert_eq!(rule.next_ask_price(0.99, 1, 3), None);
        assert_eq!(rule.next_bid_price(f64::NAN, 0, 4), None);
        assert_eq!(rule.next_ask_price(f64::INFINITY, 0, 4), None);
    }

    #[rstest]
    #[case(0.009_999_999_9, "0.0099", "0.0100")]
    #[case(0.010_000_000_1, "0.0100", "0.0110")]
    fn test_tick_rule_preserves_direction_near_grid_levels(
        grid: PriceGrid,
        #[case] value: f64,
        #[case] bid: &str,
        #[case] ask: &str,
    ) {
        let rule: &dyn TickSchemeRule = &grid;
        assert_eq!(rule.next_bid_price(value, 0, 4), Some(bid.into()));
        assert_eq!(rule.next_ask_price(value, 0, 4), Some(ask.into()));
    }

    #[rstest]
    fn test_serialization_preserves_ranges_and_precision(grid: PriceGrid) {
        let json = serde_json::to_string(&grid).unwrap();
        let restored: PriceGrid = serde_json::from_str(&json).unwrap();
        assert_eq!(grid, restored);
        assert_eq!(restored.precision(), 4);
        assert_eq!(restored.min_increment().to_string(), "0.0001");
        assert_eq!(
            restored.next_bid_price("0.99".into(), 1),
            Some("0.9890".into())
        );
    }

    #[rstest]
    #[case("[]")]
    #[case(r#"[["0.00","1.00","0.00"]]"#)]
    #[case(r#"[["0.00","1.00","-0.01"]]"#)]
    #[case(r#"[["1.00","0.00","0.01"]]"#)]
    #[case(r#"[["0.00","1.00","0.03"]]"#)]
    #[case(r#"[["0.00","1.00","0.1"]]"#)]
    #[case(r#"[["0.00","1.00","0.01"],["0.50","2.00","0.01"]]"#)]
    #[case(r#"[["0.00","1.00","0.01"],["1.00","2.00","0.01"]]"#)]
    #[case(r#"[["2.00","3.00","0.01"],["0.00","1.00","0.01"]]"#)]
    fn test_deserialization_validates_grid(#[case] json: &str) {
        assert!(serde_json::from_str::<PriceGrid>(json).is_err());
    }

    #[rstest]
    fn test_constructor_rejects_invalid_price_representation() {
        let invalid = Price {
            raw: 1,
            precision: 2,
        };
        assert!(PriceGrid::new(vec![(invalid, "1.00".into(), "0.01".into())]).is_err());
        let invalid = Price {
            raw: 0,
            precision: u8::MAX,
        };
        assert!(PriceGrid::new(vec![(invalid, invalid, invalid)]).is_err());
    }

    #[rstest]
    #[case(ERROR_PRICE)]
    #[case(Price::from_raw(PRICE_ERROR, 0))]
    #[case(Price::from_raw(PRICE_UNDEF, 0))]
    #[case(Price { raw: 0, precision: FIXED_PRECISION + 1 })]
    fn test_queries_reject_incompatible_prices(grid: PriceGrid, #[case] value: Price) {
        assert!(!grid.contains(value));
        assert_eq!(grid.next_bid_price(value, 0), None);
        assert_eq!(grid.next_ask_price(value, 0), None);
    }
}
