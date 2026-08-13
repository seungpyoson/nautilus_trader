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

//! The ledger of accepted trade legs, and the single transition table over them.
//!
//! Every venue statement about a trade, from either transport, is answered here. Holding the whole
//! table in one place is what makes the answers monotonic and idempotent: `Confirmed` and `Voided`
//! are terminal, a repeated statement changes nothing, a confirmation never applies the fill a
//! second time, and `OrderFillVoided` is reachable only from a leg that was applied.
//!
//! | current     | `MATCHED` / `MINED` / `RETRYING` | `CONFIRMED`                  | `FAILED`             |
//! | ----------- | -------------------------------- | ---------------------------- | -------------------- |
//! | (none)      | apply, emit `OrderFilled`        | apply and settle, emit once  | refuse, emit nothing |
//! | `Applied`   | no change                        | settle, emit nothing         | reverse              |
//! | `Confirmed` | no change                        | no change                    | no change, logged    |
//! | `Voided`    | no change                        | no change, logged            | no change            |

use nautilus_common::cache::fifo::FifoCacheMap;
use nautilus_model::{
    events::{OrderFillVoided, OrderFilled},
    identifiers::{TradeId, VenueOrderId},
    types::Quantity,
};

use super::trade_evidence::{EvidenceState, Settlement, TradeEvidence};

/// What a venue statement means for the caller once the ledger has recorded it.
#[derive(Debug)]
pub(crate) enum EvidenceOutcome {
    /// The leg is newly accepted: its fill must be applied exactly once.
    Apply,
    /// The statement changes nothing that leaves this adapter.
    Ignore,
    /// The leg was applied and the venue has now failed it: give the quantity back, and reverse
    /// the event if one was emitted.
    Void(Box<VoidedLeg>),
}

/// A leg that must be reversed.
#[derive(Debug)]
pub(crate) struct VoidedLeg {
    pub venue_order_id: VenueOrderId,
    /// The quantity applied against the order, which is what has to be given back.
    pub last_qty: Quantity,
    /// The event to reverse. `None` when the fill went out as a report, or when it was still
    /// queued for emission, because neither left an `OrderFilled` for the engine to reverse.
    pub filled: Option<OrderFilled>,
}

/// One accepted leg and the event it produced.
#[derive(Clone, Debug)]
struct EvidenceRecord {
    evidence: TradeEvidence,
    filled: Option<OrderFilled>,
}

/// The state of every trade leg this adapter has accepted.
///
/// Keyed by the trade ID the engine indexes the fill under, so a venue trade that fills several
/// owned maker orders is several records, each settling and failing on its own.
#[derive(Debug, Default)]
pub(crate) struct TradeEvidenceLedger {
    records: FifoCacheMap<TradeId, EvidenceRecord, 10_000>,
}

impl TradeEvidenceLedger {
    /// Records what the venue has stated about one trade leg, and returns what it means.
    ///
    /// `candidate` is the evidence the statement itself supports. It is `None` when the payload
    /// could not produce complete evidence, which only a failure can act on: failing a leg that
    /// was already applied needs the leg's identity, not the failing payload's economics.
    pub(crate) fn observe(
        &mut self,
        trade_id: TradeId,
        candidate: Option<TradeEvidence>,
        settlement: Settlement,
    ) -> EvidenceOutcome {
        let Some(record) = self.records.get_mut(&trade_id) else {
            return self.accept_first(trade_id, candidate, settlement);
        };

        match (record.evidence.state, settlement) {
            (EvidenceState::Applied, Settlement::Pending) => EvidenceOutcome::Ignore,
            (EvidenceState::Applied, Settlement::Confirmed) => confirm(record, candidate.as_ref()),
            (EvidenceState::Applied, Settlement::Failed) => void(record),
            (EvidenceState::Confirmed, Settlement::Failed) => {
                log::error!(
                    "Venue failed trade {trade_id} after confirming it, keeping the confirmed fill"
                );
                EvidenceOutcome::Ignore
            }
            (EvidenceState::Voided, Settlement::Confirmed) => {
                log::error!(
                    "Venue confirmed trade {trade_id} after failing it, leaving the trade unfilled"
                );
                EvidenceOutcome::Ignore
            }
            (EvidenceState::Confirmed | EvidenceState::Voided, _) => EvidenceOutcome::Ignore,
        }
    }

    /// Records the event an applied leg produced, so a later failure reverses exactly what was
    /// applied.
    ///
    /// A leg voided while its fill was queued keeps no event: the void has already been decided,
    /// and the fill it would reverse never reached the engine.
    pub(crate) fn record_fill_event(&mut self, filled: OrderFilled) {
        let Some(record) = self.records.get_mut(&filled.trade_id) else {
            return;
        };

        if record.evidence.state == EvidenceState::Voided {
            return;
        }

        record.filled = Some(filled);
    }

    /// Records the quantity a queued leg actually applied, once the order it fills is registered.
    ///
    /// A leg is queued before the order's registered size is known, so the dust snap the drain
    /// performs is the first moment the applied quantity is settled. The evidence has to say the
    /// same number: a later `CONFIRMED` is compared against it, and a failure reverses it.
    pub(crate) fn record_applied_quantity(&mut self, trade_id: &TradeId, last_qty: Quantity) {
        let Some(record) = self.records.get_mut(trade_id) else {
            return;
        };

        record.evidence.last_qty = last_qty;
    }

    /// Returns whether the leg has been voided, and so must never reach the engine as a fill.
    pub(crate) fn is_voided(&self, trade_id: &TradeId) -> bool {
        self.state_of(trade_id) == Some(EvidenceState::Voided)
    }

    /// Returns whether the venue has confirmed the leg.
    pub(crate) fn is_confirmed(&self, trade_id: &TradeId) -> bool {
        self.state_of(trade_id) == Some(EvidenceState::Confirmed)
    }

    /// Returns the evidence held for a leg.
    pub(crate) fn evidence(&self, trade_id: &TradeId) -> Option<&TradeEvidence> {
        self.records.get(trade_id).map(|record| &record.evidence)
    }

    /// Restores a leg this client applied in an earlier session, so the venue can still fail it.
    pub(crate) fn restore_applied(&mut self, filled: &OrderFilled) {
        self.records.insert(
            filled.trade_id,
            EvidenceRecord {
                evidence: TradeEvidence::from_applied_fill(filled),
                filled: Some(filled.clone()),
            },
        );
    }

    /// Restores a leg this client voided in an earlier session, so it can never be applied again.
    pub(crate) fn restore_voided(&mut self, voided: &OrderFillVoided) {
        self.records.insert(
            voided.trade_id,
            EvidenceRecord {
                evidence: TradeEvidence::from_voided_fill(voided),
                filled: None,
            },
        );
    }

    /// Drops every record.
    pub(crate) fn clear(&mut self) {
        self.records.clear();
    }

    /// Restores a leg as already settled, for tests that need a confirmed trade without replaying
    /// the statements that confirmed it.
    #[cfg(test)]
    pub(crate) fn restore_confirmed(&mut self, filled: &OrderFilled) {
        let mut evidence = TradeEvidence::from_applied_fill(filled);
        evidence.state = EvidenceState::Confirmed;
        self.records.insert(
            filled.trade_id,
            EvidenceRecord {
                evidence,
                filled: Some(filled.clone()),
            },
        );
    }

    fn state_of(&self, trade_id: &TradeId) -> Option<EvidenceState> {
        self.records
            .get(trade_id)
            .map(|record| record.evidence.state)
    }

    fn accept_first(
        &mut self,
        trade_id: TradeId,
        candidate: Option<TradeEvidence>,
        settlement: Settlement,
    ) -> EvidenceOutcome {
        let Some(mut evidence) = candidate else {
            log::warn!("Skipping incomplete evidence for trade {trade_id}");
            return EvidenceOutcome::Ignore;
        };

        // A failure arriving before anything was applied has nothing to reverse, and the record it
        // leaves is a standing refusal: a replayed `MATCHED` for a permanently failed trade must
        // not become a fill.
        evidence.state = match settlement {
            Settlement::Pending => EvidenceState::Applied,
            Settlement::Confirmed => EvidenceState::Confirmed,
            Settlement::Failed => EvidenceState::Voided,
        };
        let outcome = match settlement {
            Settlement::Pending | Settlement::Confirmed => EvidenceOutcome::Apply,
            Settlement::Failed => EvidenceOutcome::Ignore,
        };
        self.records.insert(
            trade_id,
            EvidenceRecord {
                evidence,
                filled: None,
            },
        );

        outcome
    }
}

/// Settles an applied leg, refusing a confirmation that restates its economics.
///
/// A `CONFIRMED` payload that disagrees with what was applied is two answers to one question, not
/// an update: applying the difference would move a position the venue never moved. The leg stays
/// applied and the disagreement is refused.
fn confirm(record: &mut EvidenceRecord, candidate: Option<&TradeEvidence>) -> EvidenceOutcome {
    let Some(candidate) = candidate else {
        log::warn!(
            "Skipping incomplete confirmation for trade {}",
            record.evidence.trade_id
        );
        return EvidenceOutcome::Ignore;
    };

    if !record.evidence.states_same_leg(candidate) {
        log::error!(
            "Refusing conflicting confirmation for trade {}: applied {} @ {} ({}), confirmation states {} @ {} ({})",
            record.evidence.trade_id,
            record.evidence.last_qty,
            record.evidence.last_px,
            record.evidence.commission,
            candidate.last_qty,
            candidate.last_px,
            candidate.commission,
        );
        return EvidenceOutcome::Ignore;
    }

    record.evidence.state = EvidenceState::Confirmed;

    EvidenceOutcome::Ignore
}

/// Reverses an applied leg the venue has permanently failed.
fn void(record: &mut EvidenceRecord) -> EvidenceOutcome {
    record.evidence.state = EvidenceState::Voided;

    EvidenceOutcome::Void(Box::new(VoidedLeg {
        venue_order_id: record.evidence.venue_order_id,
        last_qty: record.evidence.last_qty,
        filled: record.filled.take(),
    }))
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType},
        identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, StrategyId, TraderId},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use ustr::Ustr;

    use super::*;
    use crate::execution::{
        get_pusd_currency,
        trade_evidence::{LegEconomics, LegIdentity},
    };

    fn test_identity() -> LegIdentity {
        LegIdentity {
            trade_id: TradeId::from("T-1"),
            venue_order_id: VenueOrderId::from("V-1"),
            instrument_id: InstrumentId::from("0xTOKEN.POLYMARKET"),
            order_side: OrderSide::Buy,
            liquidity_side: LiquiditySide::Taker,
        }
    }

    fn test_economics() -> LegEconomics {
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

    fn test_evidence() -> TradeEvidence {
        TradeEvidence::build(test_identity(), test_economics(), Some(UnixNanos::from(1)))
            .expect("valid evidence")
    }

    fn restated_evidence(size: Decimal, price: Decimal) -> TradeEvidence {
        let mut economics = test_economics();
        economics.size = size;
        economics.price = price;

        TradeEvidence::build(test_identity(), economics, Some(UnixNanos::from(9)))
            .expect("valid evidence")
    }

    fn test_filled(evidence: &TradeEvidence) -> OrderFilled {
        OrderFilled::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-1"),
            evidence.instrument_id,
            ClientOrderId::from("O-1"),
            evidence.venue_order_id,
            AccountId::from("POLY-001"),
            evidence.trade_id,
            evidence.order_side,
            OrderType::Limit,
            evidence.last_qty,
            evidence.last_px,
            get_pusd_currency(),
            evidence.liquidity_side,
            UUID4::new(),
            evidence.ts_event,
            UnixNanos::from(2),
            false,
            Some(PositionId::from("P-1")),
            Some(evidence.commission),
            None,
        )
    }

    /// A ledger holding one leg that was applied and whose fill event was emitted.
    fn applied_ledger() -> (TradeEvidenceLedger, TradeEvidence) {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();
        let outcome = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
        );

        assert!(matches!(outcome, EvidenceOutcome::Apply));
        ledger.record_fill_event(test_filled(&evidence));

        (ledger, evidence)
    }

    #[rstest]
    #[case(Settlement::Pending, EvidenceState::Applied)]
    #[case(Settlement::Confirmed, EvidenceState::Confirmed)]
    fn test_first_statement_applies_the_fill(
        #[case] settlement: Settlement,
        #[case] expected: EvidenceState,
    ) {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();

        let outcome = ledger.observe(evidence.trade_id, Some(evidence.clone()), settlement);

        assert!(matches!(outcome, EvidenceOutcome::Apply));
        assert_eq!(
            ledger.evidence(&evidence.trade_id).map(|e| e.state),
            Some(expected)
        );
    }

    #[rstest]
    fn test_first_statement_failure_emits_nothing_and_refuses_a_replayed_fill() {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();

        let failed = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Failed,
        );
        let replayed = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
        );

        assert!(matches!(failed, EvidenceOutcome::Ignore));
        assert!(matches!(replayed, EvidenceOutcome::Ignore));
        assert!(ledger.is_voided(&evidence.trade_id));
    }

    #[rstest]
    fn test_failure_of_an_unknown_leg_records_nothing() {
        let mut ledger = TradeEvidenceLedger::default();
        let trade_id = TradeId::from("T-UNKNOWN");

        let outcome = ledger.observe(trade_id, None, Settlement::Failed);

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.evidence(&trade_id).is_none());
    }

    #[rstest]
    fn test_incomplete_first_statement_records_nothing() {
        let mut ledger = TradeEvidenceLedger::default();
        let trade_id = TradeId::from("T-1");

        let outcome = ledger.observe(trade_id, None, Settlement::Pending);

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.evidence(&trade_id).is_none());
    }

    #[rstest]
    fn test_applied_ignores_a_repeated_pending_statement() {
        let (mut ledger, evidence) = applied_ledger();

        let outcome = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
        );

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert_eq!(
            ledger.evidence(&evidence.trade_id).map(|e| e.state),
            Some(EvidenceState::Applied)
        );
    }

    #[rstest]
    fn test_applied_confirms_without_a_second_fill() {
        let (mut ledger, evidence) = applied_ledger();

        let outcome = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Confirmed,
        );

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.is_confirmed(&evidence.trade_id));
    }

    #[rstest]
    #[case::quantity(dec!(30), dec!(0.5))]
    #[case::price(dec!(25), dec!(0.6))]
    fn test_conflicting_confirmation_is_refused_and_the_leg_stays_applied(
        #[case] size: Decimal,
        #[case] price: Decimal,
    ) {
        let (mut ledger, evidence) = applied_ledger();

        let outcome = ledger.observe(
            evidence.trade_id,
            Some(restated_evidence(size, price)),
            Settlement::Confirmed,
        );

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert_eq!(
            ledger.evidence(&evidence.trade_id).map(|e| e.state),
            Some(EvidenceState::Applied)
        );
        assert_eq!(
            ledger.evidence(&evidence.trade_id).map(|e| e.last_qty),
            Some(evidence.last_qty)
        );
    }

    #[rstest]
    fn test_confirmation_the_venue_restamped_is_not_a_conflict() {
        let (mut ledger, evidence) = applied_ledger();

        let outcome = ledger.observe(
            evidence.trade_id,
            Some(restated_evidence(dec!(25), dec!(0.5))),
            Settlement::Confirmed,
        );

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.is_confirmed(&evidence.trade_id));
    }

    #[rstest]
    fn test_applied_failure_voids_the_emitted_fill_exactly_once() {
        let (mut ledger, evidence) = applied_ledger();

        let voided = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Failed,
        );
        let repeated = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Failed,
        );

        match voided {
            EvidenceOutcome::Void(leg) => {
                assert_eq!(leg.venue_order_id, evidence.venue_order_id);
                assert_eq!(leg.last_qty, evidence.last_qty);
                assert_eq!(
                    leg.filled.map(|filled| filled.trade_id),
                    Some(evidence.trade_id)
                );
            }
            other => panic!("expected a void, was {other:?}"),
        }
        assert!(matches!(repeated, EvidenceOutcome::Ignore));
        assert!(ledger.is_voided(&evidence.trade_id));
    }

    #[rstest]
    fn test_failure_of_a_queued_fill_reverses_no_event() {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();
        ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
        );

        let voided = ledger.observe(evidence.trade_id, None, Settlement::Failed);

        match voided {
            EvidenceOutcome::Void(leg) => {
                assert!(leg.filled.is_none());
                assert_eq!(leg.last_qty, evidence.last_qty);
            }
            other => panic!("expected a void, was {other:?}"),
        }
    }

    #[rstest]
    fn test_a_voided_leg_keeps_no_late_fill_event() {
        let (mut ledger, evidence) = applied_ledger();
        ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Failed,
        );

        ledger.record_fill_event(test_filled(&evidence));
        let repeated = ledger.observe(evidence.trade_id, None, Settlement::Failed);

        assert!(matches!(repeated, EvidenceOutcome::Ignore));
        assert!(ledger.is_voided(&evidence.trade_id));
    }

    #[rstest]
    #[case(Settlement::Pending)]
    #[case(Settlement::Confirmed)]
    #[case(Settlement::Failed)]
    fn test_confirmed_is_terminal(#[case] settlement: Settlement) {
        let (mut ledger, evidence) = applied_ledger();
        ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Confirmed,
        );

        let outcome = ledger.observe(evidence.trade_id, Some(evidence.clone()), settlement);

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.is_confirmed(&evidence.trade_id));
    }

    #[rstest]
    #[case(Settlement::Pending)]
    #[case(Settlement::Confirmed)]
    #[case(Settlement::Failed)]
    fn test_voided_is_terminal(#[case] settlement: Settlement) {
        let (mut ledger, evidence) = applied_ledger();
        ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Failed,
        );

        let outcome = ledger.observe(evidence.trade_id, Some(evidence.clone()), settlement);

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.is_voided(&evidence.trade_id));
    }

    #[rstest]
    fn test_restored_fill_can_still_be_voided_without_the_failing_economics() {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();
        ledger.restore_applied(&test_filled(&evidence));

        let outcome = ledger.observe(evidence.trade_id, None, Settlement::Failed);

        match outcome {
            EvidenceOutcome::Void(leg) => {
                assert_eq!(leg.last_qty, evidence.last_qty);
                assert!(leg.filled.is_some());
            }
            other => panic!("expected a void, was {other:?}"),
        }
    }

    #[rstest]
    fn test_restored_void_refuses_a_replayed_fill() {
        let mut ledger = TradeEvidenceLedger::default();
        let evidence = test_evidence();
        let filled = test_filled(&evidence);
        let voided = OrderFillVoided::new(
            filled.trader_id,
            filled.strategy_id,
            filled.instrument_id,
            filled.client_order_id,
            filled.venue_order_id,
            filled.account_id,
            Ustr::from("T-1-FAILED"),
            filled.trade_id,
            filled.last_qty,
            filled.commission,
            filled.order_side,
            filled.order_type,
            filled.last_px,
            filled.currency,
            filled.liquidity_side,
            filled.position_id,
            None,
            None,
            UUID4::new(),
            filled.ts_event,
            filled.ts_init,
            false,
            false,
        );
        ledger.restore_voided(&voided);

        let outcome = ledger.observe(
            evidence.trade_id,
            Some(evidence.clone()),
            Settlement::Pending,
        );

        assert!(matches!(outcome, EvidenceOutcome::Ignore));
        assert!(ledger.is_voided(&evidence.trade_id));
    }

    #[rstest]
    fn test_clear_drops_every_record() {
        let (mut ledger, evidence) = applied_ledger();

        ledger.clear();

        assert!(ledger.evidence(&evidence.trade_id).is_none());
    }
}
