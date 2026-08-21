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

//! Polymarket simulation models.

use anyhow::Context;
use nautilus_execution::models::fee::FeeModel;
#[cfg(feature = "python")]
use nautilus_execution::python::fee::PyFeeModel;
use nautilus_model::{
    enums::LiquiditySide,
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    execution::{parse::compute_fee_equivalent, report_validation::validate_fee_schedule},
    http::models::FeeSchedule,
};

/// Polymarket fee model for binary-option backtests.
///
/// Taker fills pay the market's fee-equivalent amount. Maker fills receive a
/// per-fill approximation of the daily maker rebate by applying the market's
/// configured rebate rate to that fee-equivalent amount.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.polymarket")
)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.polymarket",
        extends = PyFeeModel,
        skip_from_py_object
    )
)]
pub struct PolymarketFeeModel;

impl FeeModel for PolymarketFeeModel {
    fn get_commission(
        &self,
        order: &OrderAny,
        fill_quantity: Quantity,
        fill_px: Price,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Money> {
        let InstrumentAny::BinaryOption(binary) = instrument else {
            anyhow::bail!("PolymarketFeeModel requires a binary option instrument");
        };

        let liquidity_side = match order.liquidity_side() {
            Some(LiquiditySide::Maker) => LiquiditySide::Maker,
            Some(LiquiditySide::Taker) => LiquiditySide::Taker,
            Some(LiquiditySide::NoLiquiditySide) | None => {
                anyhow::bail!("Liquidity side not set")
            }
        };

        let fees_enabled = binary
            .info
            .as_ref()
            .and_then(|info| info.get("fees_enabled"))
            .and_then(serde_json::Value::as_bool);
        let schedule = binary
            .info
            .as_ref()
            .and_then(|info| info.get("fee_schedule"))
            .map(|value| serde_json::from_value::<FeeSchedule>(value.clone()))
            .transpose()
            .context("invalid Polymarket fee schedule")?;
        let schedule = match (fees_enabled, schedule) {
            (Some(false), None) => return Ok(Money::zero(instrument.quote_currency())),
            (Some(true), Some(schedule)) => schedule,
            _ => anyhow::bail!("inconsistent fee metadata for PolymarketFeeModel"),
        };

        let (_, fee_exponent) = validate_fee_schedule(&schedule, "PolymarketFeeModel")?;

        let fill_price = fill_px.as_decimal();
        if !(Decimal::ZERO..=Decimal::ONE).contains(&fill_price) {
            anyhow::bail!("PolymarketFeeModel requires a fill price in [0, 1]");
        }

        let fee_equivalent = compute_fee_equivalent(
            schedule.rate,
            fee_exponent,
            fill_quantity.as_decimal(),
            fill_price,
        )?;
        let commission = match liquidity_side {
            LiquiditySide::Maker => -fee_equivalent
                .checked_mul(schedule.rebate_rate)
                .context("commission calculation overflow")?
                .round_dp(5),
            LiquiditySide::Taker => fee_equivalent.round_dp(5),
            LiquiditySide::NoLiquiditySide => unreachable!(),
        };

        Money::from_decimal(commission, instrument.quote_currency()).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_execution::models::fee::{FeeModel, FeeModelHandle};
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType},
        instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
        orders::{OrderAny, builder::OrderTestBuilder, stubs::TestOrderStubs},
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::PolymarketFeeModel;
    use crate::http::{
        models::{FeeSchedule, GammaMarket},
        parse::{create_instrument_from_def, parse_gamma_market},
    };

    #[rstest]
    #[case(dec!(0.07), dec!(0.20), dec!(-0.35))]
    #[case(dec!(0.05), dec!(0.15), dec!(-0.1875))]
    #[case(dec!(0.04), dec!(0.25), dec!(-0.25))]
    #[case(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)]
    fn test_maker_rebate_rates(
        #[case] fee_rate: Decimal,
        #[case] rebate_rate: Decimal,
        #[case] expected: Decimal,
    ) {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(fee_rate, rebate_rate))).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Maker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), expected);
        assert_eq!(commission.currency, instrument.quote_currency());
    }

    #[rstest]
    fn test_taker_fee() {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(dec!(0.05), dec!(0.15)))).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Taker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), dec!(1.25));
        assert_eq!(commission.currency, instrument.quote_currency());
    }

    #[rstest]
    fn test_taker_fee_uses_market_fee_exponent() {
        let instrument = instrument_with_schedule(Some(FeeSchedule {
            exponent: dec!(2),
            rate: dec!(0.04),
            taker_only: true,
            rebate_rate: dec!(0.25),
        }))
        .unwrap();
        let order = fill_order(&instrument, LiquiditySide::Taker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), dec!(0.25));
    }

    #[rstest]
    #[case("0.01", dec!(0.00000))]
    #[case("0.02", dec!(0.00001))]
    fn test_taker_fee_rounds_to_five_decimal_places(
        #[case] fill_quantity: &str,
        #[case] expected: Decimal,
    ) {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(dec!(0.05), dec!(0.15)))).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Taker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from(fill_quantity),
                Price::from("0.01"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), expected);
    }

    #[rstest]
    #[case("0.06", dec!(0.00000))]
    #[case("0.07", dec!(-0.00001))]
    fn test_maker_rebate_rounds_to_five_decimal_places(
        #[case] fill_quantity: &str,
        #[case] expected: Decimal,
    ) {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(dec!(0.05), dec!(0.15)))).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Maker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from(fill_quantity),
                Price::from("0.01"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), expected);
    }

    #[rstest]
    fn test_missing_fee_schedule_returns_zero() {
        let instrument = instrument_with_schedule(None).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Maker);

        let commission = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), Decimal::ZERO);
        assert_eq!(commission.currency, instrument.quote_currency());
    }

    #[rstest]
    fn test_enabled_market_without_schedule_is_not_implicitly_fee_free() {
        let mut market: GammaMarket =
            serde_json::from_str(include_str!("../test_data/gamma_market.json")).unwrap();
        market.fees_enabled = Some(true);
        market.fee_schedule = None;
        let definition = parse_gamma_market(&market).unwrap().remove(0);
        let instrument = create_instrument_from_def(&definition, UnixNanos::default()).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Taker);

        let error = PolymarketFeeModel
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap_err();

        assert!(error.to_string().contains("inconsistent fee metadata"));
    }

    #[rstest]
    fn test_runtime_handle_dispatches_polymarket_model() {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(dec!(0.07), dec!(0.20)))).unwrap();
        let order = fill_order(&instrument, LiquiditySide::Maker);
        let model = FeeModelHandle::new(PolymarketFeeModel);

        let commission = model
            .get_commission(
                &order,
                Quantity::from("100"),
                Price::from("0.50"),
                &instrument,
            )
            .unwrap();

        assert_eq!(commission.as_decimal(), dec!(-0.35));
    }

    #[rstest]
    fn test_requires_binary_option() {
        let instrument = InstrumentAny::CurrencyPair(audusd_sim());
        let order = fill_order(&instrument, LiquiditySide::Taker);

        let result = PolymarketFeeModel.get_commission(
            &order,
            Quantity::from("100"),
            Price::from("0.50"),
            &instrument,
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "PolymarketFeeModel requires a binary option instrument"
        );
    }

    #[rstest]
    fn test_requires_liquidity_side() {
        let instrument =
            instrument_with_schedule(Some(fee_schedule(dec!(0.05), dec!(0.15)))).unwrap();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .price(Price::from("0.50"))
            .quantity(Quantity::from("100"))
            .build();
        let order = TestOrderStubs::make_accepted_order(&order);

        let result = PolymarketFeeModel.get_commission(
            &order,
            Quantity::from("100"),
            Price::from("0.50"),
            &instrument,
        );

        assert_eq!(result.unwrap_err().to_string(), "Liquidity side not set");
    }

    #[rstest]
    #[case(
        dec!(1),
        dec!(-0.01),
        dec!(0.15),
        true,
        "fee rate must be in [0, 1], was -0.01"
    )]
    #[case(
        dec!(1),
        dec!(0.05),
        dec!(-0.01),
        true,
        "rebate rate must be in [0, 1], was -0.01"
    )]
    #[case(
        dec!(1),
        dec!(0.05),
        dec!(1.01),
        true,
        "rebate rate must be in [0, 1], was 1.01"
    )]
    #[case(
        dec!(1),
        dec!(0.05),
        dec!(0.15),
        false,
        "requires a taker-only fee schedule"
    )]
    fn test_requires_supported_schedule(
        #[case] exponent: Decimal,
        #[case] rate: Decimal,
        #[case] rebate_rate: Decimal,
        #[case] taker_only: bool,
        #[case] expected: &str,
    ) {
        let schedule = FeeSchedule {
            exponent,
            rate,
            taker_only,
            rebate_rate,
        };
        let result = instrument_with_schedule(Some(schedule));

        assert!(result.unwrap_err().to_string().contains(expected));
    }

    fn fee_schedule(rate: Decimal, rebate_rate: Decimal) -> FeeSchedule {
        FeeSchedule {
            exponent: Decimal::ONE,
            rate,
            taker_only: true,
            rebate_rate,
        }
    }

    fn instrument_with_schedule(schedule: Option<FeeSchedule>) -> anyhow::Result<InstrumentAny> {
        let mut market: GammaMarket = serde_json::from_str(include_str!(
            "../test_data/gamma_market_sports_market_money_line.json"
        ))
        .unwrap();
        market.fees_enabled = Some(schedule.is_some());
        market.fee_schedule = schedule;
        let def = parse_gamma_market(&market).unwrap().remove(0);
        create_instrument_from_def(&def, UnixNanos::default())
    }

    fn fill_order(instrument: &InstrumentAny, liquidity_side: LiquiditySide) -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .price(Price::from("0.50"))
            .quantity(Quantity::from("100"))
            .build();
        TestOrderStubs::make_filled_order(&order, instrument, liquidity_side)
    }
}
