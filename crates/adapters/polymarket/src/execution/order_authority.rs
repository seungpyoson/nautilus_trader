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

//! Exact signed-order authority for Polymarket execution.

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    events::OrderEventAny,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::{DUST_SNAP_THRESHOLD_DEC, USDC_DECIMALS},
        enums::PolymarketOrderSide,
    },
    execution::order_builder::compute_maker_taker_amounts,
    http::models::PolymarketOrder,
};

/// Provider surface carrying an order snapshot.
///
/// WebSocket snapshots carry the live signed expiration field. REST has historically returned a
/// positive expiration for non-GTD orders, so only that surface canonicalizes the field after all
/// signed economics bind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderReportSurface {
    Rest,
    WebSocket,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum FillGrowthPolicy {
    #[default]
    Fixed,
    /// Durable order semantics prove an immediate quote BUY, but its exact signed budget is not
    /// available after restoration. Provider quantity is preserved and any growth fails closed.
    QuoteImmediateBuyUnproven,
    /// A locally signed quote-quantity market BUY. Realized shares may exceed the estimate, but
    /// aggregate notional cannot exceed the exact signed quote amount.
    QuoteImmediateBuy { signed_quote_budget: Decimal },
}

impl FillGrowthPolicy {
    #[cfg(test)]
    pub(crate) fn quote_immediate_buy(signed_quote_budget: Decimal) -> Self {
        Self::QuoteImmediateBuy {
            signed_quote_budget,
        }
    }

    pub(crate) fn snap_fill_qty(self, submitted_qty: Quantity, fill_qty: Quantity) -> Quantity {
        match self {
            Self::Fixed => {
                let diff = submitted_qty.as_decimal() - fill_qty.as_decimal();
                if diff < Decimal::ZERO && diff.abs() < DUST_SNAP_THRESHOLD_DEC {
                    log::debug!("Snapping overfill {fill_qty} -> {submitted_qty} (dust={diff})");
                    submitted_qty
                } else {
                    fill_qty
                }
            }
            Self::QuoteImmediateBuyUnproven | Self::QuoteImmediateBuy { .. } => fill_qty,
        }
    }
}

/// Immutable economic authority captured from the exact order sent to the venue.
///
/// The CLOB signs maker/taker amount legs rather than an explicit price. Retaining both the
/// submitted price and the exact signed legs lets order snapshots bind to the submitted form while
/// fills are constrained by the executable ratio encoded by the signature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SignedOrderTerms {
    order_side: OrderSide,
    order_type: OrderType,
    time_in_force: TimeInForce,
    base_quantity: Quantity,
    submitted_price: Decimal,
    execution_price_bound: Decimal,
    maker_amount: Decimal,
    taker_amount: Decimal,
    expire_time: Option<UnixNanos>,
}

/// Retained economic authority for one venue order.
///
/// `Proven` contains the exact locally signed maker/taker legs. A restored Market order cannot
/// reconstruct those legs from durable NT order state, so it is represented explicitly as
/// `RestoredMarketUnproven` and cannot authorize fills, growth, or terminal Filled snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderAuthority {
    state: OrderAuthorityState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderAuthorityState {
    Proven(SignedOrderTerms),
    RestoredMarketUnproven {
        order_side: OrderSide,
        time_in_force: TimeInForce,
        base_quantity: Quantity,
        submitted_price: Option<Price>,
        expire_time: Option<UnixNanos>,
        growth_policy: FillGrowthPolicy,
    },
}

impl OrderAuthority {
    fn proven(terms: SignedOrderTerms) -> Self {
        Self {
            state: OrderAuthorityState::Proven(terms),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        order_side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        base_quantity: Quantity,
        submitted_price: Price,
        expire_time: Option<UnixNanos>,
    ) -> Self {
        Self::proven(SignedOrderTerms::for_test(
            order_side,
            order_type,
            time_in_force,
            base_quantity,
            submitted_price,
            expire_time,
        ))
    }

    #[cfg(test)]
    pub(crate) fn for_test_signed_amounts(
        order_side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        submitted_price: Price,
        maker_amount: Decimal,
        taker_amount: Decimal,
        expire_time: Option<UnixNanos>,
    ) -> Self {
        Self::proven(SignedOrderTerms::for_test_signed_amounts(
            order_side,
            order_type,
            time_in_force,
            submitted_price,
            maker_amount,
            taker_amount,
            expire_time,
        ))
    }

    pub(crate) fn from_signed_order(
        order: &PolymarketOrder,
        order_type: OrderType,
        time_in_force: TimeInForce,
        submitted_price: Decimal,
        expire_time: Option<UnixNanos>,
    ) -> anyhow::Result<Self> {
        SignedOrderTerms::from_signed_order(
            order,
            order_type,
            time_in_force,
            submitted_price,
            expire_time,
        )
        .map(Self::proven)
    }

    pub(crate) fn from_cached_order(order: &OrderAny) -> anyhow::Result<Self> {
        match order.order_type() {
            OrderType::Limit => SignedOrderTerms::from_cached_limit_order(order).map(Self::proven),
            OrderType::Market => Ok(Self {
                state: OrderAuthorityState::RestoredMarketUnproven {
                    order_side: order.order_side(),
                    time_in_force: order.time_in_force(),
                    base_quantity: order.quantity(),
                    submitted_price: order.price(),
                    expire_time: order.expire_time(),
                    growth_policy: match order.events().first() {
                        Some(OrderEventAny::Initialized(initialized))
                            if initialized.quote_quantity =>
                        {
                            FillGrowthPolicy::QuoteImmediateBuyUnproven
                        }
                        _ => FillGrowthPolicy::Fixed,
                    },
                },
            }),
            order_type => anyhow::bail!(
                "cached order type {order_type} is unsupported by Polymarket authority restoration"
            ),
        }
    }

    /// Validates durable NT order semantics before retained authority is paired with cache state.
    pub(crate) fn validate_cached_order(self, order: &OrderAny) -> anyhow::Result<()> {
        match self.state {
            OrderAuthorityState::Proven(terms) => terms.validate_cached_order(order),
            OrderAuthorityState::RestoredMarketUnproven {
                order_side,
                time_in_force,
                base_quantity,
                submitted_price,
                expire_time,
                growth_policy,
            } => {
                anyhow::ensure!(
                    order.order_side() == order_side,
                    "cached order side {} does not match restored side {order_side}",
                    order.order_side(),
                );
                anyhow::ensure!(
                    order.order_type() == OrderType::Market,
                    "cached order type {} does not match restored Market authority",
                    order.order_type(),
                );
                anyhow::ensure!(
                    order.time_in_force() == time_in_force,
                    "cached order time in force {} does not match restored time in force {time_in_force}",
                    order.time_in_force(),
                );
                anyhow::ensure!(
                    order.price() == submitted_price,
                    "cached order price {:?} does not match restored price {submitted_price:?}",
                    order.price(),
                );
                anyhow::ensure!(
                    order.expire_time() == expire_time,
                    "cached order expiration {:?} does not match restored expiration {expire_time:?}",
                    order.expire_time(),
                );
                validate_cached_quantity(order.quantity(), base_quantity, growth_policy)
            }
        }
    }

    pub(crate) fn base_quantity(self) -> Quantity {
        match self.state {
            OrderAuthorityState::Proven(terms) => terms.base_quantity,
            OrderAuthorityState::RestoredMarketUnproven { base_quantity, .. } => base_quantity,
        }
    }

    pub(crate) fn growth_policy(self) -> FillGrowthPolicy {
        match self.state {
            OrderAuthorityState::Proven(terms)
                if terms.order_side == OrderSide::Buy
                    && terms.order_type == OrderType::Market
                    && matches!(terms.time_in_force, TimeInForce::Ioc | TimeInForce::Fok) =>
            {
                FillGrowthPolicy::QuoteImmediateBuy {
                    signed_quote_budget: terms.maker_amount
                        / Decimal::from(10u64.pow(USDC_DECIMALS)),
                }
            }
            OrderAuthorityState::RestoredMarketUnproven { growth_policy, .. } => growth_policy,
            _ => FillGrowthPolicy::Fixed,
        }
    }

    pub(crate) fn restored_original_quantity(self, order: &OrderAny) -> Quantity {
        match self.state {
            OrderAuthorityState::RestoredMarketUnproven {
                growth_policy: FillGrowthPolicy::QuoteImmediateBuyUnproven,
                ..
            } => order
                .events()
                .iter()
                .find_map(|event| match event {
                    OrderEventAny::Updated(updated)
                        if updated.venue_order_id.is_none() && !updated.is_quote_quantity =>
                    {
                        Some(updated.quantity)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| order.quantity()),
            _ => order.quantity(),
        }
    }

    pub(crate) fn validate_fill(self, report: &FillReport) -> anyhow::Result<()> {
        match self.state {
            OrderAuthorityState::Proven(terms) => terms.validate_fill(report),
            OrderAuthorityState::RestoredMarketUnproven { .. } => anyhow::bail!(
                "exact signed Market order terms are unavailable for fill on order {}",
                report.venue_order_id,
            ),
        }
    }

    pub(crate) fn snap_fill_qty(self, fill_qty: Quantity) -> Quantity {
        self.growth_policy()
            .snap_fill_qty(self.base_quantity(), fill_qty)
    }

    pub(crate) fn bind_order_report(
        self,
        report: &mut OrderStatusReport,
        raw_original_size: Option<Decimal>,
        surface: OrderReportSurface,
    ) -> anyhow::Result<()> {
        match self.state {
            OrderAuthorityState::Proven(terms) => {
                terms.bind_order_report(report, raw_original_size, surface)
            }
            OrderAuthorityState::RestoredMarketUnproven {
                order_side,
                time_in_force,
                base_quantity,
                submitted_price: _,
                expire_time,
                growth_policy: _,
            } => {
                anyhow::ensure!(
                    raw_original_size.is_none(),
                    "exact signed Market order size is unavailable for order report {}",
                    report.venue_order_id,
                );
                anyhow::ensure!(
                    report.order_side == order_side,
                    "order report side {} does not match restored side {order_side}",
                    report.order_side,
                );
                anyhow::ensure!(
                    report.time_in_force == time_in_force,
                    "order report time in force {} does not match restored time in force {time_in_force}",
                    report.time_in_force,
                );
                anyhow::ensure!(
                    report.quantity.as_decimal() == base_quantity.as_decimal(),
                    "order report quantity {} does not match restored base quantity {base_quantity}",
                    report.quantity,
                );
                anyhow::ensure!(
                    report.order_status != OrderStatus::Filled,
                    "exact signed Market terms are unavailable for terminal order report {}",
                    report.venue_order_id,
                );
                bind_report_expiration(report, expire_time, time_in_force, surface)?;
                report.order_type = OrderType::Market;
                report.price = None;
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_restored_market_unproven(
        order_side: OrderSide,
        time_in_force: TimeInForce,
        base_quantity: Quantity,
        growth_policy: FillGrowthPolicy,
    ) -> Self {
        Self {
            state: OrderAuthorityState::RestoredMarketUnproven {
                order_side,
                time_in_force,
                base_quantity,
                submitted_price: None,
                expire_time: None,
                growth_policy,
            },
        }
    }
}

impl SignedOrderTerms {
    #[cfg(test)]
    pub(crate) fn for_test(
        order_side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        base_quantity: Quantity,
        submitted_price: Price,
        expire_time: Option<UnixNanos>,
    ) -> Self {
        let side = PolymarketOrderSide::try_from(order_side).unwrap();
        let (maker_amount, taker_amount) = compute_maker_taker_amounts(
            submitted_price.as_decimal(),
            base_quantity.as_decimal(),
            side,
            u32::from(submitted_price.precision),
        );
        Self::from_amounts(
            side,
            order_type,
            time_in_force,
            submitted_price.as_decimal(),
            maker_amount,
            taker_amount,
            expire_time,
        )
        .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn for_test_signed_amounts(
        order_side: OrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        submitted_price: Price,
        maker_amount: Decimal,
        taker_amount: Decimal,
        expire_time: Option<UnixNanos>,
    ) -> Self {
        Self::from_amounts(
            PolymarketOrderSide::try_from(order_side).unwrap(),
            order_type,
            time_in_force,
            submitted_price.as_decimal(),
            maker_amount,
            taker_amount,
            expire_time,
        )
        .unwrap()
    }

    pub(crate) fn from_signed_order(
        order: &PolymarketOrder,
        order_type: OrderType,
        time_in_force: TimeInForce,
        submitted_price: Decimal,
        expire_time: Option<UnixNanos>,
    ) -> anyhow::Result<Self> {
        Self::from_amounts(
            order.side,
            order_type,
            time_in_force,
            submitted_price,
            order.maker_amount,
            order.taker_amount,
            expire_time,
        )
    }

    /// Reconstructs exact Limit-order terms from durable NT order semantics.
    ///
    /// A Market order does not retain its locally calculated crossing price or signed legs in NT
    /// events, so restoration must leave that proof unavailable and reject later economic
    /// authority until it can be authoritatively rehydrated.
    fn from_cached_limit_order(order: &OrderAny) -> anyhow::Result<Self> {
        anyhow::ensure!(
            order.order_type() == OrderType::Limit,
            "cached order type {} is not Limit",
            order.order_type(),
        );
        let price = order
            .price()
            .ok_or_else(|| anyhow::anyhow!("cached Limit order has no price"))?;
        let side = PolymarketOrderSide::try_from(order.order_side())
            .map_err(|e| anyhow::anyhow!("cached order side is unsupported: {e}"))?;
        let (maker_amount, taker_amount) = compute_maker_taker_amounts(
            price.as_decimal(),
            order.quantity().as_decimal(),
            side,
            u32::from(price.precision),
        );
        Self::from_amounts(
            side,
            OrderType::Limit,
            order.time_in_force(),
            price.as_decimal(),
            maker_amount,
            taker_amount,
            order.expire_time(),
        )
    }

    fn from_amounts(
        side: PolymarketOrderSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        submitted_price: Decimal,
        maker_amount: Decimal,
        taker_amount: Decimal,
        expire_time: Option<UnixNanos>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            submitted_price > Decimal::ZERO,
            "signed order price must be positive"
        );
        anyhow::ensure!(
            maker_amount > Decimal::ZERO,
            "signed maker amount must be positive"
        );
        anyhow::ensure!(
            taker_amount > Decimal::ZERO,
            "signed taker amount must be positive"
        );

        let scale = Decimal::from(10u64.pow(USDC_DECIMALS));
        let (base_amount, quote_amount) = match side {
            PolymarketOrderSide::Buy => (taker_amount, maker_amount),
            PolymarketOrderSide::Sell => (maker_amount, taker_amount),
        };
        let base_quantity = base_amount
            .checked_div(scale)
            .ok_or_else(|| anyhow::anyhow!("signed base quantity conversion failed"))?;
        let execution_price_bound = quote_amount
            .checked_div(base_amount)
            .ok_or_else(|| anyhow::anyhow!("signed execution price conversion failed"))?;
        anyhow::ensure!(
            base_quantity > Decimal::ZERO,
            "signed base quantity must be positive"
        );
        anyhow::ensure!(
            execution_price_bound > Decimal::ZERO && execution_price_bound < Decimal::ONE,
            "signed execution price {execution_price_bound} is outside the binary price domain"
        );

        let base_quantity = Quantity::from_decimal_dp(base_quantity, USDC_DECIMALS as u8)?;

        Ok(Self {
            order_side: OrderSide::from(side),
            order_type,
            time_in_force,
            base_quantity,
            submitted_price,
            execution_price_bound,
            maker_amount,
            taker_amount,
            expire_time,
        })
    }

    pub(crate) fn provider_original_size(self) -> Decimal {
        let scale = Decimal::from(10u64.pow(USDC_DECIMALS));
        let raw = match (self.order_side, self.order_type, self.time_in_force) {
            (OrderSide::Buy, OrderType::Market, TimeInForce::Ioc | TimeInForce::Fok) => {
                self.maker_amount
            }
            (OrderSide::Buy, _, _) => self.taker_amount,
            _ => self.maker_amount,
        };
        raw / scale
    }

    pub(crate) fn validate_fill(self, report: &FillReport) -> anyhow::Result<()> {
        anyhow::ensure!(
            report.order_side == self.order_side,
            "fill side {} does not match signed side {} for order {}",
            report.order_side,
            self.order_side,
            report.venue_order_id,
        );
        let fill_price = report.last_px.as_decimal();
        match self.order_side {
            OrderSide::Buy => anyhow::ensure!(
                fill_price <= self.execution_price_bound,
                "fill price {fill_price} exceeds signed BUY bound {} for order {}",
                self.execution_price_bound,
                report.venue_order_id,
            ),
            OrderSide::Sell => anyhow::ensure!(
                fill_price >= self.execution_price_bound,
                "fill price {fill_price} is below signed SELL bound {} for order {}",
                self.execution_price_bound,
                report.venue_order_id,
            ),
            side => anyhow::bail!("signed order side {side} is unsupported by Polymarket"),
        }
        Ok(())
    }

    fn validate_cached_order(self, order: &OrderAny) -> anyhow::Result<()> {
        anyhow::ensure!(
            order.order_side() == self.order_side,
            "cached order side {} does not match signed side {}",
            order.order_side(),
            self.order_side,
        );
        anyhow::ensure!(
            order.order_type() == self.order_type,
            "cached order type {} does not match signed type {}",
            order.order_type(),
            self.order_type,
        );
        anyhow::ensure!(
            order.time_in_force() == self.time_in_force,
            "cached order time in force {} does not match signed time in force {}",
            order.time_in_force(),
            self.time_in_force,
        );
        anyhow::ensure!(
            order.expire_time() == self.expire_time,
            "cached order expiration {:?} does not match signed expiration {:?}",
            order.expire_time(),
            self.expire_time,
        );
        if self.order_type == OrderType::Limit {
            anyhow::ensure!(
                order.price().map(|price| price.as_decimal()) == Some(self.submitted_price),
                "cached order price {:?} does not match signed price {}",
                order.price(),
                self.submitted_price,
            );
        } else {
            anyhow::ensure!(
                order.price().is_none(),
                "cached Market order carries unexpected price {:?}",
                order.price(),
            );
        }
        validate_cached_quantity(
            order.quantity(),
            self.base_quantity,
            OrderAuthority::proven(self).growth_policy(),
        )
    }

    pub(crate) fn bind_order_report(
        self,
        report: &mut OrderStatusReport,
        raw_original_size: Option<Decimal>,
        surface: OrderReportSurface,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            report.order_side == self.order_side,
            "order report side {} does not match signed side {}",
            report.order_side,
            self.order_side,
        );
        anyhow::ensure!(
            report.time_in_force == self.time_in_force,
            "order report time in force {} does not match signed time in force {}",
            report.time_in_force,
            self.time_in_force,
        );
        anyhow::ensure!(
            report.quantity.as_decimal() == self.base_quantity.as_decimal(),
            "order report quantity {} does not match signed base quantity {}",
            report.quantity,
            self.base_quantity,
        );
        anyhow::ensure!(
            report.price.as_ref().map(Price::as_decimal) == Some(self.submitted_price),
            "order report price {:?} does not match submitted price {}",
            report.price,
            self.submitted_price,
        );
        if let Some(raw_original_size) = raw_original_size {
            anyhow::ensure!(
                raw_original_size == self.provider_original_size(),
                "order report original size {raw_original_size} does not match signed provider size {}",
                self.provider_original_size(),
            );
        }

        bind_report_expiration(report, self.expire_time, self.time_in_force, surface)?;
        report.order_type = self.order_type;
        if self.order_type == OrderType::Market {
            report.price = None;
        }
        Ok(())
    }
}

fn validate_cached_quantity(
    quantity: Quantity,
    base_quantity: Quantity,
    growth_policy: FillGrowthPolicy,
) -> anyhow::Result<()> {
    match growth_policy {
        FillGrowthPolicy::QuoteImmediateBuy { .. }
        | FillGrowthPolicy::QuoteImmediateBuyUnproven => anyhow::ensure!(
            quantity >= base_quantity,
            "cached order quantity {quantity} is below retained base quantity {base_quantity}",
        ),
        FillGrowthPolicy::Fixed => anyhow::ensure!(
            quantity == base_quantity,
            "cached order quantity {quantity} does not match retained quantity {base_quantity}",
        ),
    }
    Ok(())
}

fn bind_report_expiration(
    report: &mut OrderStatusReport,
    expected: Option<UnixNanos>,
    time_in_force: TimeInForce,
    surface: OrderReportSurface,
) -> anyhow::Result<()> {
    match (surface, time_in_force) {
        (OrderReportSurface::WebSocket, _) | (_, TimeInForce::Gtd) => anyhow::ensure!(
            report.expire_time == expected,
            "order report expiration {:?} does not match submitted expiration {expected:?}",
            report.expire_time,
        ),
        (OrderReportSurface::Rest, _) => {
            // CLOB REST has historically returned positive expirations for GTC orders. It is not
            // signed authority, so canonicalize it to retained local state after all other terms
            // bind.
            report.expire_time = expected;
        }
    }
    Ok(())
}
