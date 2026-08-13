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

//! Trade evidence: the one validated form of a Polymarket trade leg.
//!
//! Polymarket states the same trade many times before it settles, over two transports, and its own
//! settlement status is not monotonic: `RETRYING` is reachable from `MATCHED` and `MINED` and can
//! return to them, and only `CONFIRMED` and `FAILED` are terminal. The venue is therefore free to
//! restate, reorder, and replay; this adapter is not, because every statement it accepts moves real
//! positions.
//!
//! [`TradeEvidence`] is what an accepted statement looks like: one leg, with complete identity and
//! complete economics. A statement that cannot produce it never reaches the ledger in
//! [`super::evidence_ledger`], so refusing incomplete evidence is a property of the type rather
//! than a check each transport has to remember.
//!
//! A record exists once the leg is applied, or once it is refused by a terminal `FAILED`. There is
//! no state for a leg the venue has merely mentioned, which is what makes "reverse only what was
//! applied" structural: the reversal is built from the event the leg emitted, so a leg that emitted
//! nothing has nothing to reverse.

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide},
    events::{OrderFillVoided, OrderFilled},
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    reports::FillReport,
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::{
        enums::{PolymarketLiquiditySide, PolymarketOrderSide, PolymarketTradeStatus},
        models::PolymarketMakerOrder,
    },
    execution::parse::{
        ReportParseError, compute_commission, determine_order_side, make_composite_trade_id,
        parse_fill_values,
    },
};

/// How this adapter reads a Polymarket settlement status.
///
/// This is the only place the five venue statuses are interpreted, so a status the venue adds
/// later fails to compile here instead of being read as one of the existing three by accident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// The venue is still working on the trade: it can still settle and it can still fail.
    Pending,
    /// The venue has settled the trade.
    Confirmed,
    /// The venue has permanently failed the trade, with no further retries.
    Failed,
}

impl Settlement {
    /// Returns how this adapter reads `status`.
    pub(crate) const fn of(status: PolymarketTradeStatus) -> Self {
        match status {
            PolymarketTradeStatus::Matched
            | PolymarketTradeStatus::Mined
            | PolymarketTradeStatus::Retrying => Self::Pending,
            PolymarketTradeStatus::Confirmed => Self::Confirmed,
            PolymarketTradeStatus::Failed => Self::Failed,
        }
    }
}

/// What this adapter has done about one trade leg.
///
/// The venue's status is not monotonic; this is. `Confirmed` and `Voided` are terminal, so a
/// statement arriving after them can restate the venue's own view without reversing ours.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EvidenceState {
    /// The leg has been accepted, and its fill has either been emitted or is queued for emission
    /// once the order it fills is registered.
    Applied,
    /// The venue has settled the leg. Terminal.
    Confirmed,
    /// The leg will never be a fill: either the venue failed it after it was applied, or the venue
    /// failed it before anything was applied and the record stands as a refusal. Terminal.
    Voided,
}

/// One Polymarket trade leg with complete identity and complete economics.
///
/// A venue trade that fills several of the account's maker orders produces one of these per owned
/// maker order, each carrying the composite trade ID the engine indexes that fill under, so every
/// leg settles and fails on its own.
///
/// That independence holds per distinct composite, not per maker order. A [`TradeId`] is bounded to
/// 36 characters, so [`make_composite_trade_id`] truncates: it takes the first 27 characters of the
/// venue trade ID and the last 8 of the venue order ID. Two maker orders inside one trade whose IDs
/// share those last 8 characters therefore key a single record for two legs, and a failure of one
/// would carry the other. Venue order IDs are long hex strings, so this is remote rather than
/// impossible, and the bound belongs to the ID format rather than to this type. It is pinned by
/// `test_maker_legs_sharing_an_id_suffix_collapse_to_one_composite`.
#[derive(Clone, Debug, PartialEq)]
pub struct TradeEvidence {
    /// The engine's identity for this fill: the venue trade ID for a taker leg, and the venue
    /// trade ID combined with the maker order ID for a maker leg.
    pub(crate) trade_id: TradeId,
    pub(crate) venue_order_id: VenueOrderId,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) order_side: OrderSide,
    pub(crate) liquidity_side: LiquiditySide,
    pub(crate) last_qty: Quantity,
    pub(crate) last_px: Price,
    pub(crate) commission: Money,
    /// The venue's own time for this trade, never substituted with a local clock reading.
    pub(crate) ts_event: UnixNanos,
    pub(crate) state: EvidenceState,
}

/// The venue's identity for one trade leg.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LegIdentity {
    pub trade_id: TradeId,
    pub venue_order_id: VenueOrderId,
    pub instrument_id: InstrumentId,
    pub order_side: OrderSide,
    pub liquidity_side: LiquiditySide,
}

/// The venue's economics for one trade leg, as stated before precision conversion.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LegEconomics {
    pub size: Decimal,
    pub price: Decimal,
    pub size_precision: u8,
    pub price_precision: u8,
    pub fee_rate: Decimal,
    pub fee_exponent: f64,
    pub currency: Currency,
}

/// The [`FillReport`] fields that belong to this client rather than to the venue.
#[derive(Clone, Copy, Debug)]
pub struct FillDelivery {
    pub account_id: AccountId,
    pub client_order_id: Option<ClientOrderId>,
    pub ts_init: UnixNanos,
}

impl TradeEvidence {
    /// Builds evidence from what the venue stated about one trade leg.
    ///
    /// This is the boundary both transports pass through: a quantity that is not positive after
    /// precision conversion, a price outside the open unit interval, or a timestamp the venue did
    /// not state all refuse the leg here, rather than reaching the ledger as a partial fill.
    ///
    /// # Errors
    ///
    /// Returns an error when the venue's economics or timestamp are not representable.
    pub(crate) fn build(
        identity: LegIdentity,
        economics: LegEconomics,
        ts_event: Option<UnixNanos>,
    ) -> Result<Self, ReportParseError> {
        let ts_event = ts_event.ok_or(ReportParseError::Timestamp)?;
        let (last_qty, last_px) = parse_fill_values(
            economics.size,
            economics.price,
            economics.size_precision,
            economics.price_precision,
        )?;
        let commission = compute_commission(
            economics.fee_rate,
            economics.fee_exponent,
            economics.size,
            economics.price,
            identity.liquidity_side,
        );

        Ok(Self {
            trade_id: identity.trade_id,
            venue_order_id: identity.venue_order_id,
            instrument_id: identity.instrument_id,
            order_side: identity.order_side,
            liquidity_side: identity.liquidity_side,
            last_qty,
            last_px,
            commission: Money::new(commission, economics.currency),
            ts_event,
            state: EvidenceState::Applied,
        })
    }

    /// Builds the [`FillReport`] this evidence supports.
    pub fn to_fill_report(&self, delivery: FillDelivery) -> FillReport {
        FillReport {
            account_id: delivery.account_id,
            instrument_id: self.instrument_id,
            venue_order_id: self.venue_order_id,
            trade_id: self.trade_id,
            order_side: self.order_side,
            last_qty: self.last_qty,
            last_px: self.last_px,
            commission: self.commission,
            liquidity_side: self.liquidity_side,
            avg_px: None,
            report_id: UUID4::new(),
            ts_event: self.ts_event,
            ts_init: delivery.ts_init,
            client_order_id: delivery.client_order_id,
            venue_position_id: None,
        }
    }

    /// Returns whether `other` states the same leg of the same trade as this evidence.
    ///
    /// `ts_event` is excluded because the venue restamps its message time on every settlement
    /// update, so a `CONFIRMED` message never repeats its own `MATCHED` timestamp. `state` is this
    /// adapter's bookkeeping rather than something the venue stated. Every other field is
    /// destructured, so a field added later joins the comparison instead of slipping past it.
    pub(crate) fn states_same_leg(&self, other: &Self) -> bool {
        let Self {
            trade_id,
            venue_order_id,
            instrument_id,
            order_side,
            liquidity_side,
            last_qty,
            last_px,
            commission,
            ts_event: _,
            state: _,
        } = self;

        *trade_id == other.trade_id
            && *venue_order_id == other.venue_order_id
            && *instrument_id == other.instrument_id
            && *order_side == other.order_side
            && *liquidity_side == other.liquidity_side
            && *last_qty == other.last_qty
            && *last_px == other.last_px
            && *commission == other.commission
    }

    /// Rebuilds the evidence behind a fill this client emitted in an earlier session.
    pub(crate) fn from_applied_fill(filled: &OrderFilled) -> Self {
        Self {
            trade_id: filled.trade_id,
            venue_order_id: filled.venue_order_id,
            instrument_id: filled.instrument_id,
            order_side: filled.order_side,
            liquidity_side: filled.liquidity_side,
            last_qty: filled.last_qty,
            last_px: filled.last_px,
            commission: filled
                .commission
                .unwrap_or_else(|| Money::new(0.0, filled.currency)),
            ts_event: filled.ts_event,
            state: EvidenceState::Applied,
        }
    }

    /// Rebuilds the evidence behind a fill this client voided in an earlier session.
    pub(crate) fn from_voided_fill(voided: &OrderFillVoided) -> Self {
        Self {
            trade_id: voided.trade_id,
            venue_order_id: voided.venue_order_id,
            instrument_id: voided.instrument_id,
            order_side: voided.order_side,
            liquidity_side: voided.liquidity_side,
            last_qty: voided.voided_qty,
            last_px: voided.last_px,
            commission: voided
                .commission_voided
                .unwrap_or_else(|| Money::new(0.0, voided.currency)),
            ts_event: voided.ts_event,
            state: EvidenceState::Voided,
        }
    }
}

/// Builds evidence for one owned maker order inside a venue trade.
///
/// Shared by the REST fill reports and the WebSocket user stream, which state a maker leg in the
/// same [`PolymarketMakerOrder`] shape. Maker fills never pay commission under Polymarket's fee
/// rules, so no fee schedule reaches this leg.
#[expect(clippy::too_many_arguments)]
pub fn maker_leg_evidence(
    maker_order: &PolymarketMakerOrder,
    trade_id: &str,
    trader_side: PolymarketLiquiditySide,
    trade_side: PolymarketOrderSide,
    taker_asset_id: &str,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    currency: Currency,
    liquidity_side: LiquiditySide,
    ts_event: Option<UnixNanos>,
) -> Result<TradeEvidence, ReportParseError> {
    let identity = LegIdentity {
        trade_id: make_composite_trade_id(trade_id, &maker_order.order_id),
        venue_order_id: VenueOrderId::from(maker_order.order_id.as_str()),
        instrument_id,
        order_side: determine_order_side(
            trader_side,
            trade_side,
            taker_asset_id,
            maker_order.asset_id.as_str(),
        ),
        liquidity_side,
    };
    let economics = LegEconomics {
        size: maker_order.matched_amount,
        price: maker_order.price,
        size_precision,
        price_precision,
        fee_rate: Decimal::ZERO,
        fee_exponent: 1.0,
        currency,
    };

    TradeEvidence::build(identity, economics, ts_event)
}

/// Builds evidence for the account's taker side of a venue trade.
///
/// Shared by the REST fill reports and the WebSocket user stream, which state the same trade in
/// different payload shapes and must derive the same economics from either one.
#[expect(clippy::too_many_arguments)]
pub fn taker_leg_evidence(
    trade_id: &str,
    taker_order_id: &str,
    trade_side: PolymarketOrderSide,
    instrument_id: InstrumentId,
    size: Decimal,
    price: Decimal,
    price_precision: u8,
    size_precision: u8,
    currency: Currency,
    fee_rate: Decimal,
    fee_exponent: f64,
    ts_event: Option<UnixNanos>,
) -> Result<TradeEvidence, ReportParseError> {
    let identity = LegIdentity {
        trade_id: TradeId::from(trade_id),
        venue_order_id: VenueOrderId::from(taker_order_id),
        instrument_id,
        order_side: OrderSide::from(trade_side),
        liquidity_side: LiquiditySide::Taker,
    };
    let economics = LegEconomics {
        size,
        price,
        size_precision,
        price_precision,
        fee_rate,
        fee_exponent,
        currency,
    };

    TradeEvidence::build(identity, economics, ts_event)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use ustr::Ustr;

    use super::*;
    use crate::{common::enums::PolymarketOutcome, execution::get_pusd_currency};

    pub(crate) fn test_identity() -> LegIdentity {
        LegIdentity {
            trade_id: TradeId::from("T-1"),
            venue_order_id: VenueOrderId::from("V-1"),
            instrument_id: InstrumentId::from("0xTOKEN.POLYMARKET"),
            order_side: OrderSide::Buy,
            liquidity_side: LiquiditySide::Taker,
        }
    }

    pub(crate) fn test_economics() -> LegEconomics {
        LegEconomics {
            size: dec!(25),
            price: dec!(0.5),
            size_precision: 6,
            price_precision: 4,
            fee_rate: Decimal::ZERO,
            fee_exponent: 1.0,
            currency: get_pusd_currency(),
        }
    }

    fn test_maker_order() -> PolymarketMakerOrder {
        PolymarketMakerOrder {
            asset_id: Ustr::from("0xTOKEN"),
            maker_address: "0xmaker".to_string(),
            matched_amount: dec!(25),
            order_id: "0xmakerorder".to_string(),
            outcome: PolymarketOutcome::yes(),
            owner: "owner".to_string(),
            price: dec!(0.5),
            side: None,
        }
    }

    #[rstest]
    #[case(PolymarketTradeStatus::Matched, Settlement::Pending)]
    #[case(PolymarketTradeStatus::Mined, Settlement::Pending)]
    #[case(PolymarketTradeStatus::Retrying, Settlement::Pending)]
    #[case(PolymarketTradeStatus::Confirmed, Settlement::Confirmed)]
    #[case(PolymarketTradeStatus::Failed, Settlement::Failed)]
    fn test_settlement_reading(
        #[case] status: PolymarketTradeStatus,
        #[case] expected: Settlement,
    ) {
        assert_eq!(Settlement::of(status), expected);
    }

    #[rstest]
    fn test_build_refuses_non_positive_quantity() {
        let mut economics = test_economics();
        economics.size = dec!(0.0000004);

        let error = TradeEvidence::build(test_identity(), economics, Some(UnixNanos::from(1u64)))
            .expect_err("quantity truncates to zero");

        assert_eq!(error, ReportParseError::Quantity);
    }

    #[rstest]
    #[case(dec!(0))]
    #[case(dec!(1))]
    fn test_build_refuses_price_outside_the_unit_interval(#[case] price: Decimal) {
        let mut economics = test_economics();
        economics.price = price;

        let error = TradeEvidence::build(test_identity(), economics, Some(UnixNanos::from(1u64)))
            .expect_err("price outside the open unit interval");

        assert_eq!(error, ReportParseError::Price);
    }

    #[rstest]
    fn test_build_refuses_a_timestamp_the_venue_did_not_state() {
        let error = TradeEvidence::build(test_identity(), test_economics(), None)
            .expect_err("timestamp the venue did not state");

        assert_eq!(error, ReportParseError::Timestamp);
    }

    #[rstest]
    fn test_fill_report_carries_the_evidence() {
        let evidence = TradeEvidence::build(
            test_identity(),
            test_economics(),
            Some(UnixNanos::from(1_000)),
        )
        .expect("valid evidence");
        let delivery = FillDelivery {
            account_id: AccountId::from("POLY-001"),
            client_order_id: Some(ClientOrderId::from("O-1")),
            ts_init: UnixNanos::from(2_000),
        };

        let report = evidence.to_fill_report(delivery);

        assert_eq!(report.trade_id, evidence.trade_id);
        assert_eq!(report.venue_order_id, evidence.venue_order_id);
        assert_eq!(report.instrument_id, evidence.instrument_id);
        assert_eq!(report.order_side, evidence.order_side);
        assert_eq!(report.liquidity_side, evidence.liquidity_side);
        assert_eq!(report.last_qty, evidence.last_qty);
        assert_eq!(report.last_px, evidence.last_px);
        assert_eq!(report.commission, evidence.commission);
        assert_eq!(report.ts_event, evidence.ts_event);
        assert_eq!(report.ts_init, delivery.ts_init);
        assert_eq!(report.client_order_id, delivery.client_order_id);
    }

    #[rstest]
    fn test_restamped_confirmation_states_the_same_leg() {
        let applied = TradeEvidence::build(
            test_identity(),
            test_economics(),
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid evidence");
        let confirmed = TradeEvidence::build(
            test_identity(),
            test_economics(),
            Some(UnixNanos::from(9u64)),
        )
        .expect("valid evidence");

        assert!(applied.states_same_leg(&confirmed));
    }

    #[rstest]
    #[case::quantity(dec!(30), dec!(0.5))]
    #[case::price(dec!(25), dec!(0.6))]
    fn test_restated_economics_do_not_state_the_same_leg(
        #[case] size: Decimal,
        #[case] price: Decimal,
    ) {
        let applied = TradeEvidence::build(
            test_identity(),
            test_economics(),
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid evidence");
        let mut economics = test_economics();
        economics.size = size;
        economics.price = price;
        let restated =
            TradeEvidence::build(test_identity(), economics, Some(UnixNanos::from(1u64)))
                .expect("valid");

        assert!(!applied.states_same_leg(&restated));
    }

    #[rstest]
    fn test_maker_leg_is_keyed_by_the_composite_trade_id() {
        let maker_order = test_maker_order();

        let evidence = maker_leg_evidence(
            &maker_order,
            "T-1",
            PolymarketLiquiditySide::Maker,
            PolymarketOrderSide::Buy,
            "0xOTHER",
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            4,
            6,
            get_pusd_currency(),
            LiquiditySide::Maker,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid maker evidence");

        assert_eq!(
            evidence.trade_id,
            make_composite_trade_id("T-1", &maker_order.order_id)
        );
        assert_eq!(
            evidence.venue_order_id,
            VenueOrderId::from(maker_order.order_id.as_str())
        );
        assert_eq!(evidence.liquidity_side, LiquiditySide::Maker);
        assert_eq!(evidence.commission, Money::new(0.0, get_pusd_currency()));
    }

    #[rstest]
    fn test_maker_legs_of_one_trade_are_separate_evidence() {
        // The composite keys on the last 8 characters of the order ID, so two legs of one trade
        // are separate evidence only when they differ there.
        let mut first = test_maker_order();
        first.order_id = "0xmakerorder-aaaaaaaa".to_string();
        let mut second = test_maker_order();
        second.order_id = "0xmakerorder-bbbbbbbb".to_string();

        let left = maker_leg_evidence(
            &first,
            "T-1",
            PolymarketLiquiditySide::Maker,
            PolymarketOrderSide::Buy,
            "0xOTHER",
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            4,
            6,
            get_pusd_currency(),
            LiquiditySide::Maker,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid maker evidence");
        let right = maker_leg_evidence(
            &second,
            "T-1",
            PolymarketLiquiditySide::Maker,
            PolymarketOrderSide::Buy,
            "0xOTHER",
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            4,
            6,
            get_pusd_currency(),
            LiquiditySide::Maker,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid maker evidence");

        assert_ne!(left.trade_id, right.trade_id);
        assert!(!left.states_same_leg(&right));
    }

    /// Pins the truncation bound documented on [`TradeEvidence`]: two maker orders whose IDs share
    /// their last 8 characters key one record for two legs, which is why per-leg independence is
    /// stated per distinct composite. Widening the composite format should fail here.
    #[rstest]
    fn test_maker_legs_sharing_an_id_suffix_collapse_to_one_composite() {
        let mut first = test_maker_order();
        first.order_id = "0xfirstmaker-shared01".to_string();
        let mut second = test_maker_order();
        second.order_id = "0xsecondmaker-shared01".to_string();

        let left = maker_leg_evidence(
            &first,
            "T-1",
            PolymarketLiquiditySide::Maker,
            PolymarketOrderSide::Buy,
            "0xOTHER",
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            4,
            6,
            get_pusd_currency(),
            LiquiditySide::Maker,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid maker evidence");
        let right = maker_leg_evidence(
            &second,
            "T-1",
            PolymarketLiquiditySide::Maker,
            PolymarketOrderSide::Buy,
            "0xOTHER",
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            4,
            6,
            get_pusd_currency(),
            LiquiditySide::Maker,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid maker evidence");

        assert_eq!(left.trade_id, right.trade_id);
        assert!(!left.states_same_leg(&right));
    }

    #[rstest]
    fn test_taker_leg_is_keyed_by_the_venue_trade_id() {
        let evidence = taker_leg_evidence(
            "T-1",
            "V-1",
            PolymarketOrderSide::Buy,
            InstrumentId::from("0xTOKEN.POLYMARKET"),
            dec!(25),
            dec!(0.5),
            4,
            6,
            get_pusd_currency(),
            Decimal::ZERO,
            1.0,
            Some(UnixNanos::from(1u64)),
        )
        .expect("valid taker evidence");

        assert_eq!(evidence.trade_id, TradeId::from("T-1"));
        assert_eq!(evidence.liquidity_side, LiquiditySide::Taker);
        assert_eq!(evidence.order_side, OrderSide::Buy);
    }
}
