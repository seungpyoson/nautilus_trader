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

//! Checked report construction shared by Polymarket reconciliation paths.

use std::collections::BTreeMap;

use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_SECOND};
use nautilus_model::types::{Price, Quantity};
use rust_decimal::Decimal;
use thiserror::Error;

/// Why a venue value could not safely become report authority.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq, Hash)]
pub(crate) enum ReportValueError {
    /// The value cannot be represented by the target fixed-point type.
    #[error("value is not representable at the requested precision")]
    Unrepresentable,
    /// A quantity that must be positive was zero, negative, or rounded to zero.
    #[error("quantity is not strictly positive")]
    NonPositive,
    /// A quantity that may be zero was negative.
    #[error("quantity is negative")]
    Negative,
    /// A binary-option price did not lie strictly between zero and one.
    #[error("price is outside the open interval (0, 1)")]
    OutsideBinaryPriceRange,
    /// A venue timestamp could not be represented as Unix nanoseconds.
    #[error("timestamp is invalid or overflows Unix nanoseconds")]
    InvalidTimestamp,
    /// A positive position was below the venue's reportable dust threshold.
    #[error("position is below the reportable dust threshold")]
    PositionDust,
    /// An identifier supplied by the venue failed model validation.
    #[error("identifier is invalid")]
    InvalidIdentifier,
}

/// Venue row type whose report could not be constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReportRow {
    Order,
    Fill,
    Position,
}

/// Field whose venue value could not be represented safely.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReportField {
    OriginalQuantity,
    FilledQuantity,
    Price,
    Timestamp,
    MatchedQuantity,
    PositionQuantity,
    AveragePrice,
    Expiration,
    InstrumentIdentity,
    VenueOrderIdentity,
    TradeIdentity,
}

/// Typed reason a venue row did not produce an authority-bearing report.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq, Hash)]
pub(crate) enum ReportOmission {
    #[error("{row:?} row has no loaded instrument")]
    UnmappedInstrument { row: ReportRow },
    #[error("{row:?} rows contain a duplicate authority identity")]
    DuplicateIdentity { row: ReportRow },
    #[error("confirmed maker trade has no order owned by the account")]
    UnownedMakerTrade,
    #[error("{row:?} row has invalid {field:?}: {reason}")]
    InvalidValue {
        row: ReportRow,
        field: ReportField,
        reason: ReportValueError,
    },
}

/// Reports and omissions produced by one checked venue-row pass.
#[derive(Debug)]
pub(crate) struct ReportBatch<T> {
    pub reports: Vec<T>,
    pub omissions: Vec<ReportOmission>,
}

impl<T> Default for ReportBatch<T> {
    fn default() -> Self {
        Self {
            reports: Vec::new(),
            omissions: Vec::new(),
        }
    }
}

impl<T> ReportBatch<T> {
    pub(crate) fn push_report(&mut self, report: T) {
        self.reports.push(report);
    }

    pub(crate) fn push_omission(&mut self, omission: ReportOmission) {
        self.omissions.push(omission);
    }

    pub(crate) fn push_result(
        &mut self,
        result: Result<T, ReportBuildError>,
    ) -> anyhow::Result<()> {
        match result {
            Ok(report) => self.push_report(report),
            Err(ReportBuildError::Omitted(omission)) => self.push_omission(omission),
            Err(ReportBuildError::Fatal(e)) => return Err(e),
        }
        Ok(())
    }

    pub(crate) fn push_omittable(&mut self, result: Result<T, ReportOmission>) {
        match result {
            Ok(report) => self.push_report(report),
            Err(omission) => self.push_omission(omission),
        }
    }
}

/// Report conversion failure, separating expected row omission from fatal batch failure.
#[derive(Debug, Error)]
pub(crate) enum ReportBuildError {
    #[error(transparent)]
    Omitted(#[from] ReportOmission),
    #[error(transparent)]
    Fatal(anyhow::Error),
}

impl ReportBuildError {
    pub(crate) fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Omitted(omission) => anyhow::Error::new(omission),
            Self::Fatal(e) => e,
        }
    }

    pub(crate) fn with_context(self, context: String) -> Self {
        match self {
            Self::Omitted(omission) => Self::Omitted(omission),
            Self::Fatal(e) => Self::Fatal(e.context(context)),
        }
    }
}

pub(crate) fn positive_quantity(
    value: Decimal,
    precision: u8,
) -> Result<Quantity, ReportValueError> {
    if value <= Decimal::ZERO {
        return Err(ReportValueError::NonPositive);
    }

    let quantity = Quantity::from_decimal_dp(value, precision)
        .map_err(|_| ReportValueError::Unrepresentable)?;
    if quantity.is_zero() {
        Err(ReportValueError::NonPositive)
    } else {
        Ok(quantity)
    }
}

pub(crate) fn non_negative_quantity(
    value: Decimal,
    precision: u8,
) -> Result<Quantity, ReportValueError> {
    if value < Decimal::ZERO {
        return Err(ReportValueError::Negative);
    }

    let quantity = Quantity::from_decimal_dp(value, precision)
        .map_err(|_| ReportValueError::Unrepresentable)?;

    if value > Decimal::ZERO && quantity.is_zero() {
        Err(ReportValueError::Unrepresentable)
    } else {
        Ok(quantity)
    }
}

pub(crate) fn binary_price(value: Decimal, precision: u8) -> Result<Price, ReportValueError> {
    if value <= Decimal::ZERO || value >= Decimal::ONE {
        return Err(ReportValueError::OutsideBinaryPriceRange);
    }

    let price =
        Price::from_decimal_dp(value, precision).map_err(|_| ReportValueError::Unrepresentable)?;
    if price.as_decimal() <= Decimal::ZERO || price.as_decimal() >= Decimal::ONE {
        Err(ReportValueError::OutsideBinaryPriceRange)
    } else {
        Ok(price)
    }
}

pub(crate) fn unix_seconds(value: u64) -> Result<UnixNanos, ReportValueError> {
    value
        .checked_mul(NANOSECONDS_IN_SECOND)
        .map(UnixNanos::from)
        .ok_or(ReportValueError::InvalidTimestamp)
}

pub(crate) fn omission_summary(omissions: &[ReportOmission]) -> String {
    let mut counts = BTreeMap::new();
    for omission in omissions {
        *counts.entry(omission.to_string()).or_insert(0_usize) += 1;
    }
    counts
        .into_iter()
        .map(|(reason, count)| format!("{count} × {reason}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use nautilus_core::datetime::NANOSECONDS_IN_SECOND;
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    #[rstest]
    #[case::zero(dec!(0))]
    #[case::negative(dec!(-1))]
    #[case::rounds_to_zero(dec!(0.0000004))]
    fn positive_quantity_rejects_non_positive_values(#[case] value: rust_decimal::Decimal) {
        assert_eq!(
            positive_quantity(value, 6),
            Err(ReportValueError::NonPositive),
        );
    }

    #[rstest]
    fn non_negative_quantity_rejects_positive_value_that_rounds_to_zero() {
        assert_eq!(
            non_negative_quantity(dec!(0.0000004), 6),
            Err(ReportValueError::Unrepresentable),
        );
    }

    #[rstest]
    #[case::zero(dec!(0))]
    #[case::one(dec!(1))]
    #[case::negative(dec!(-0.1))]
    #[case::above_one(dec!(1.1))]
    fn binary_price_rejects_values_outside_open_unit_interval(
        #[case] value: rust_decimal::Decimal,
    ) {
        assert_eq!(
            binary_price(value, 4),
            Err(ReportValueError::OutsideBinaryPriceRange),
        );
    }

    #[rstest]
    fn unix_seconds_rejects_overflow() {
        assert_eq!(
            unix_seconds(u64::MAX),
            Err(ReportValueError::InvalidTimestamp),
        );
        assert_eq!(
            unix_seconds(1)
                .expect("one second is representable")
                .as_u64(),
            NANOSECONDS_IN_SECOND,
        );
    }

    #[rstest]
    fn report_batch_keeps_reports_and_typed_omissions_separate() {
        let mut batch = ReportBatch::default();
        batch.push_report(7_u8);
        batch.push_omission(ReportOmission::InvalidValue {
            row: ReportRow::Order,
            field: ReportField::OriginalQuantity,
            reason: ReportValueError::NonPositive,
        });

        assert_eq!(batch.reports, vec![7]);
        assert_eq!(batch.omissions.len(), 1);
        assert!(!batch.omissions.is_empty());
    }
}
