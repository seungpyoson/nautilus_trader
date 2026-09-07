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

use std::hash::{Hash, Hasher};

use nautilus_core::{
    Params, UnixNanos,
    correctness::{CorrectnessResult, check_equal_u8, check_predicate_true},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{
    Instrument, PriceGrid, TickSchemeRule,
    any::InstrumentAny,
    tick_scheme::{check_tick_scheme, tick_scheme_rule_from_name},
};
use crate::{
    enums::{AssetClass, InstrumentClass, OptionKind},
    identifiers::{InstrumentId, Symbol},
    types::{
        currency::Currency,
        money::Money,
        price::{Price, check_positive_price},
        quantity::{Quantity, check_positive_quantity},
    },
};

/// Represents a generic binary option instrument.
#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(remote = "Self")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct BinaryOption {
    /// The instrument ID.
    pub id: InstrumentId,
    /// The raw/local/native symbol for the instrument, assigned by the venue.
    pub raw_symbol: Symbol,
    /// The binary option asset class.
    pub asset_class: AssetClass,
    /// The binary option contract currency.
    pub currency: Currency,
    /// UNIX timestamp (nanoseconds) for contract activation.
    pub activation_ns: UnixNanos,
    /// UNIX timestamp (nanoseconds) for contract expiration.
    pub expiration_ns: UnixNanos,
    /// The price decimal precision.
    pub price_precision: u8,
    /// The trading size decimal precision.
    pub size_precision: u8,
    /// The minimum price increment (tick size).
    pub price_increment: Price,
    /// The minimum size increment.
    pub size_increment: Quantity,
    /// The initial (order) margin requirement in percentage of order value.
    pub margin_init: Decimal,
    /// The maintenance (position) margin in percentage of position value.
    pub margin_maint: Decimal,
    /// The fee rate for liquidity makers as a percentage of order value.
    pub maker_fee: Decimal,
    /// The fee rate for liquidity takers as a percentage of order value.
    pub taker_fee: Decimal,
    /// The binary outcome of the market.
    pub outcome: Option<Ustr>,
    /// The market description.
    pub description: Option<Ustr>,
    /// The maximum allowable order quantity.
    pub max_quantity: Option<Quantity>,
    /// The minimum allowable order quantity.
    pub min_quantity: Option<Quantity>,
    /// The maximum allowable order notional value.
    pub max_notional: Option<Money>,
    /// The minimum allowable order notional value.
    pub min_notional: Option<Money>,
    /// The maximum allowable quoted price.
    pub max_price: Option<Price>,
    /// The minimum allowable quoted price.
    pub min_price: Option<Price>,
    /// The registered variable tick scheme name.
    pub tick_scheme: Option<Ustr>,
    /// The exact price grid supplied by venue metadata, exclusive with `tick_scheme`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_grid: Option<PriceGrid>,
    /// Additional instrument metadata as a JSON-serializable dictionary.
    pub info: Option<Params>,
    /// UNIX timestamp (nanoseconds) when the data event occurred.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the data object was initialized.
    pub ts_init: UnixNanos,
}

#[bon::bon]
impl BinaryOption {
    #[expect(clippy::too_many_arguments)]
    fn new_checked(
        instrument_id: InstrumentId,
        raw_symbol: Symbol,
        asset_class: AssetClass,
        currency: Currency,
        activation_ns: UnixNanos,
        expiration_ns: UnixNanos,
        price_precision: u8,
        size_precision: u8,
        price_increment: Price,
        size_increment: Quantity,
        outcome: Option<Ustr>,
        description: Option<Ustr>,
        max_quantity: Option<Quantity>,
        min_quantity: Option<Quantity>,
        max_notional: Option<Money>,
        min_notional: Option<Money>,
        max_price: Option<Price>,
        min_price: Option<Price>,
        margin_init: Option<Decimal>,
        margin_maint: Option<Decimal>,
        maker_fee: Option<Decimal>,
        taker_fee: Option<Decimal>,
        tick_scheme: Option<Ustr>,
        price_grid: Option<PriceGrid>,
        info: Option<Params>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> CorrectnessResult<Self> {
        check_equal_u8(
            price_precision,
            price_increment.precision,
            stringify!(price_precision),
            stringify!(price_increment.precision),
        )?;
        check_equal_u8(
            size_precision,
            size_increment.precision,
            stringify!(size_precision),
            stringify!(size_increment.precision),
        )?;
        check_positive_price(price_increment, stringify!(price_increment))?;
        check_positive_quantity(size_increment, stringify!(size_increment))?;
        check_tick_scheme(tick_scheme)?;

        let instrument = Self {
            id: instrument_id,
            raw_symbol,
            asset_class,
            currency,
            activation_ns,
            expiration_ns,
            price_precision,
            size_precision,
            price_increment,
            size_increment,
            margin_init: margin_init.unwrap_or_default(),
            margin_maint: margin_maint.unwrap_or_default(),
            maker_fee: maker_fee.unwrap_or_default(),
            taker_fee: taker_fee.unwrap_or_default(),
            outcome,
            description,
            max_quantity,
            min_quantity,
            max_notional,
            min_notional,
            max_price,
            min_price,
            tick_scheme,
            price_grid,
            info,
            ts_event,
            ts_init,
        };
        instrument.validate_price_grid()?;
        Ok(instrument)
    }

    /// Returns a fluent builder for a [`BinaryOption`] instance.
    ///
    /// Required fields are enforced at compile time; optional fields can be omitted and use the
    /// same defaults as checked construction. The same correctness checks run on `build`.
    ///
    /// # Errors
    ///
    /// Returns an error if any input validation fails.
    #[builder(start_fn = builder, finish_fn = build)]
    pub fn build_checked(
        instrument_id: InstrumentId,
        raw_symbol: Symbol,
        asset_class: AssetClass,
        currency: Currency,
        activation_ns: UnixNanos,
        expiration_ns: UnixNanos,
        price_precision: u8,
        size_precision: u8,
        price_increment: Price,
        size_increment: Quantity,
        outcome: Option<Ustr>,
        description: Option<Ustr>,
        max_quantity: Option<Quantity>,
        min_quantity: Option<Quantity>,
        max_notional: Option<Money>,
        min_notional: Option<Money>,
        max_price: Option<Price>,
        min_price: Option<Price>,
        margin_init: Option<Decimal>,
        margin_maint: Option<Decimal>,
        maker_fee: Option<Decimal>,
        taker_fee: Option<Decimal>,
        tick_scheme: Option<Ustr>,
        price_grid: Option<PriceGrid>,
        info: Option<Params>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> CorrectnessResult<Self> {
        Self::new_checked(
            instrument_id,
            raw_symbol,
            asset_class,
            currency,
            activation_ns,
            expiration_ns,
            price_precision,
            size_precision,
            price_increment,
            size_increment,
            outcome,
            description,
            max_quantity,
            min_quantity,
            max_notional,
            min_notional,
            max_price,
            min_price,
            margin_init,
            margin_maint,
            maker_fee,
            taker_fee,
            tick_scheme,
            price_grid,
            info,
            ts_event,
            ts_init,
        )
    }

    fn validate_price_grid(&self) -> CorrectnessResult<()> {
        if let Some(grid) = &self.price_grid {
            check_predicate_true(
                self.tick_scheme.is_none(),
                "price_grid and tick_scheme cannot both be set",
            )?;
            check_equal_u8(
                self.price_precision,
                grid.precision(),
                "price_precision",
                "price_grid precision",
            )?;
            check_predicate_true(
                self.price_increment == grid.min_increment()
                    && self.price_increment.precision == grid.precision(),
                "price_increment must match the smallest price grid step",
            )?;
        }
        Ok(())
    }
}

impl Serialize for BinaryOption {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for BinaryOption {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let instrument = Self::deserialize(deserializer)?;
        instrument
            .validate_price_grid()
            .map_err(serde::de::Error::custom)?;
        Ok(instrument)
    }
}

impl PartialEq<Self> for BinaryOption {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for BinaryOption {}

impl Hash for BinaryOption {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Instrument for BinaryOption {
    fn price_grid(&self) -> Option<&PriceGrid> {
        self.price_grid.as_ref()
    }

    fn tick_scheme_rule(&self) -> Option<&dyn TickSchemeRule> {
        self.price_grid
            .as_ref()
            .map(|grid| grid as &dyn TickSchemeRule)
            .or_else(|| {
                self.tick_scheme
                    .and_then(|name| tick_scheme_rule_from_name(name.as_str()))
            })
    }

    fn tick_scheme(&self) -> Option<Ustr> {
        self.tick_scheme
    }
    fn into_any(self) -> InstrumentAny {
        InstrumentAny::BinaryOption(self)
    }

    fn id(&self) -> InstrumentId {
        self.id
    }

    fn raw_symbol(&self) -> Symbol {
        self.raw_symbol
    }

    fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    fn instrument_class(&self) -> InstrumentClass {
        InstrumentClass::BinaryOption
    }

    fn underlying(&self) -> Option<Ustr> {
        None
    }

    fn base_currency(&self) -> Option<Currency> {
        None
    }

    fn quote_currency(&self) -> Currency {
        self.currency
    }

    fn settlement_currency(&self) -> Currency {
        self.currency
    }

    fn isin(&self) -> Option<Ustr> {
        None
    }

    fn exchange(&self) -> Option<Ustr> {
        None
    }

    fn option_kind(&self) -> Option<OptionKind> {
        None
    }

    fn is_inverse(&self) -> bool {
        false
    }

    fn price_precision(&self) -> u8 {
        self.price_precision
    }

    fn size_precision(&self) -> u8 {
        self.size_precision
    }

    fn price_increment(&self) -> Price {
        self.price_increment
    }

    fn size_increment(&self) -> Quantity {
        self.size_increment
    }

    fn multiplier(&self) -> Quantity {
        Quantity::from(1)
    }

    fn lot_size(&self) -> Option<Quantity> {
        Some(Quantity::from(1))
    }

    fn max_quantity(&self) -> Option<Quantity> {
        self.max_quantity
    }

    fn min_quantity(&self) -> Option<Quantity> {
        self.min_quantity
    }

    fn max_price(&self) -> Option<Price> {
        self.max_price
    }

    fn min_price(&self) -> Option<Price> {
        self.min_price
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }

    fn margin_init(&self) -> Decimal {
        self.margin_init
    }

    fn margin_maint(&self) -> Decimal {
        self.margin_maint
    }

    fn maker_fee(&self) -> Decimal {
        self.maker_fee
    }

    fn taker_fee(&self) -> Decimal {
        self.taker_fee
    }

    fn strike_price(&self) -> Option<Price> {
        None
    }

    fn activation_ns(&self) -> Option<UnixNanos> {
        Some(self.activation_ns)
    }

    fn expiration_ns(&self) -> Option<UnixNanos> {
        Some(self.expiration_ns)
    }

    fn max_notional(&self) -> Option<Money> {
        self.max_notional
    }

    fn min_notional(&self) -> Option<Money> {
        self.min_notional
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use crate::{
        enums::{AssetClass, InstrumentClass},
        identifiers::{InstrumentId, Symbol},
        instruments::{BinaryOption, Instrument, InstrumentAny, PriceGrid, stubs::*},
        types::{Currency, Money, Price, Quantity},
    };

    #[rstest]
    fn test_trait_accessors(binary_option: BinaryOption) {
        assert_eq!(binary_option.asset_class(), AssetClass::Alternative);
        assert_eq!(
            binary_option.instrument_class(),
            InstrumentClass::BinaryOption
        );
        assert_eq!(binary_option.quote_currency(), Currency::USDC());
        assert!(!binary_option.is_inverse());
        assert_eq!(binary_option.price_precision(), 3);
        assert_eq!(binary_option.size_precision(), 2);
        assert!(binary_option.activation_ns().is_some());
        assert!(binary_option.expiration_ns().is_some());
    }

    #[rstest]
    fn test_new_checked_price_precision_mismatch() {
        let result = BinaryOption::new_checked(
            InstrumentId::from("TEST.POLYMARKET"),
            Symbol::from("TEST"),
            AssetClass::Alternative,
            Currency::USDC(),
            0.into(),
            0.into(),
            4, // mismatch
            2,
            Price::from("0.001"),
            Quantity::from("0.01"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.into(),
            0.into(),
        );
        assert!(result.is_err());
    }

    #[rstest]
    fn test_serialization_roundtrip(binary_option: BinaryOption) {
        let json = serde_json::to_string(&binary_option).unwrap();
        let deserialized: BinaryOption = serde_json::from_str(&json).unwrap();
        assert_eq!(binary_option, deserialized);
    }

    #[rstest]
    fn test_price_grid_survives_instrument_any_serialization(mut binary_option: BinaryOption) {
        binary_option.price_grid = Some(
            PriceGrid::new(vec![
                ("0.000".into(), "0.009".into(), "0.001".into()),
                ("0.010".into(), "1.000".into(), "0.010".into()),
            ])
            .unwrap(),
        );
        let original = InstrumentAny::BinaryOption(binary_option);
        let json = serde_json::to_string(&original).unwrap();
        let restored: InstrumentAny = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.price_grid(), original.price_grid());
        assert_eq!(restored.next_bid_price(0.01, 1), Some("0.009".into()));
        assert_eq!(restored.next_ask_price(0.01, 1), Some("0.020".into()));
        assert_eq!(restored.next_ask_price(0.01, -1), None);
    }

    #[rstest]
    #[case("0.016", false)]
    #[case("0.105", true)]
    #[case("0.002", true)]
    #[case("0.0151", false)]
    fn test_instrument_any_normalization_uses_full_price_grid(
        mut binary_option: BinaryOption,
        #[case] value: &str,
        #[case] accepted: bool,
    ) {
        binary_option.price_increment = "0.002".into();
        binary_option.price_grid = Some(
            PriceGrid::new(vec![
                ("0.002".into(), "0.008".into(), "0.002".into()),
                ("0.015".into(), "0.995".into(), "0.010".into()),
            ])
            .unwrap(),
        );
        let json = serde_json::to_string(&InstrumentAny::BinaryOption(binary_option)).unwrap();
        let instrument: InstrumentAny = serde_json::from_str(&json).unwrap();
        let result = instrument.try_normalize_price(value.into());
        assert_eq!(result.is_ok(), accepted);
        if let Ok(price) = result {
            assert_eq!(price, Price::from(value));
            assert_eq!(price.precision, instrument.price_precision());
        }
    }

    #[rstest]
    #[case("tick_scheme", serde_json::json!("FIXED_PRECISION_3"))]
    #[case("price_precision", serde_json::json!(4))]
    #[case("price_increment", serde_json::json!("0.010"))]
    fn test_deserialization_rejects_conflicting_grid_fields(
        binary_option: BinaryOption,
        #[case] field: &str,
        #[case] value: serde_json::Value,
    ) {
        let mut json = serde_json::to_value(binary_option).unwrap();
        json["price_grid"] = serde_json::json!([["0.000", "1.000", "0.001"]]);
        json[field] = value;
        assert!(serde_json::from_value::<BinaryOption>(json).is_err());
    }

    #[rstest]
    fn test_builder_rejects_conflicting_named_scheme(binary_option: BinaryOption) {
        let result = BinaryOption::builder()
            .instrument_id(binary_option.id)
            .raw_symbol(binary_option.raw_symbol)
            .asset_class(binary_option.asset_class)
            .currency(binary_option.currency)
            .activation_ns(binary_option.activation_ns)
            .expiration_ns(binary_option.expiration_ns)
            .price_precision(3)
            .size_precision(2)
            .price_increment("0.001".into())
            .size_increment("0.01".into())
            .tick_scheme("FIXED_PRECISION_3".into())
            .price_grid(
                PriceGrid::new(vec![("0.000".into(), "1.000".into(), "0.001".into())]).unwrap(),
            )
            .ts_event(0.into())
            .ts_init(0.into())
            .build();
        assert!(result.is_err());
    }

    #[rstest]
    fn test_builder_matches_new_checked() {
        let positional = BinaryOption::new_checked(
            InstrumentId::from("TEST.POLYMARKET"),
            Symbol::from("TEST"),
            AssetClass::Alternative,
            Currency::USDC(),
            1.into(),
            2.into(),
            3,
            2,
            Price::from("0.001"),
            Quantity::from("0.01"),
            Some("Yes".into()),
            Some("Will it happen?".into()),
            Some(Quantity::from("10000.00")),
            Some(Quantity::from("5.00")),
            Some(Money::from("100000 USDC")),
            Some(Money::from("10 USDC")),
            Some(Price::from("0.999")),
            Some(Price::from("0.001")),
            Some(dec!(0.01)),
            Some(dec!(0.02)),
            Some(dec!(0.0002)),
            Some(dec!(0.0004)),
            None,
            None,
            None,
            3.into(),
            4.into(),
        )
        .unwrap();

        let built = BinaryOption::builder()
            .instrument_id(InstrumentId::from("TEST.POLYMARKET"))
            .raw_symbol(Symbol::from("TEST"))
            .asset_class(AssetClass::Alternative)
            .currency(Currency::USDC())
            .activation_ns(1.into())
            .expiration_ns(2.into())
            .price_precision(3)
            .size_precision(2)
            .price_increment(Price::from("0.001"))
            .size_increment(Quantity::from("0.01"))
            .outcome("Yes".into())
            .description("Will it happen?".into())
            .max_quantity(Quantity::from("10000.00"))
            .min_quantity(Quantity::from("5.00"))
            .max_notional(Money::from("100000 USDC"))
            .min_notional(Money::from("10 USDC"))
            .max_price(Price::from("0.999"))
            .min_price(Price::from("0.001"))
            .margin_init(dec!(0.01))
            .margin_maint(dec!(0.02))
            .maker_fee(dec!(0.0002))
            .taker_fee(dec!(0.0004))
            .ts_event(3.into())
            .ts_init(4.into())
            .build()
            .unwrap();

        assert_eq!(
            serde_json::to_value(&positional).unwrap(),
            serde_json::to_value(&built).unwrap(),
        );
    }
}
