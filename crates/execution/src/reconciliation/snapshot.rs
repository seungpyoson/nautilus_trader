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

//! Canonical execution snapshot normalization.

use anyhow::{Context, ensure};
use indexmap::IndexMap;
use nautilus_common::messages::{AuthenticatedExecutionMassStatus, ExecutionSourceId};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, PositionId, TradeId, Venue, VenueOrderId},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::Quantity,
};

type FillKey = (AccountId, InstrumentId, TradeId);

/// A mass-status snapshot whose evidence has one canonical identity and state.
///
/// Construction is restricted to the execution engine so authenticated source
/// validation always precedes normalization. Equivalent repeated evidence is
/// collapsed; contradictory evidence rejects the whole snapshot.
#[derive(Clone, Debug)]
pub struct NormalizedExecutionMassStatus {
    source_client_id: ClientId,
    source_id: ExecutionSourceId,
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    report_id: UUID4,
    ts_init: UnixNanos,
    order_reports: IndexMap<VenueOrderId, OrderStatusReport>,
    fill_reports: IndexMap<VenueOrderId, Vec<FillReport>>,
    position_reports: IndexMap<InstrumentId, Vec<PositionStatusReport>>,
}

impl NormalizedExecutionMassStatus {
    pub(crate) fn normalize(
        authenticated: AuthenticatedExecutionMassStatus,
    ) -> anyhow::Result<Self> {
        let source_client_id = authenticated.source_client_id;
        let source_id = authenticated.source_id;
        let mass_status = authenticated.report;
        let mut order_reports = mass_status.order_reports();
        for (venue_order_id, report) in &order_reports {
            ensure!(
                *venue_order_id == report.venue_order_id,
                "Order group key {venue_order_id} conflicts with report venue order {}",
                report.venue_order_id
            );
        }
        let fill_reports = normalize_fills(mass_status.fill_reports(), &mut order_reports)?;
        let position_reports = normalize_positions(mass_status.position_reports())?;

        Ok(Self {
            source_client_id,
            source_id,
            client_id: mass_status.client_id,
            account_id: mass_status.account_id,
            venue: mass_status.venue,
            report_id: mass_status.report_id,
            ts_init: mass_status.ts_init,
            order_reports,
            fill_reports,
            position_reports,
        })
    }

    /// Returns the authenticated client that produced the snapshot.
    #[must_use]
    pub const fn source_client_id(&self) -> ClientId {
        self.source_client_id
    }

    /// Returns the source capability bound to the registered client.
    #[must_use]
    pub const fn source_id(&self) -> ExecutionSourceId {
        self.source_id
    }

    /// Returns the venue declared by the normalized snapshot.
    #[must_use]
    pub const fn venue(&self) -> Venue {
        self.venue
    }

    /// Returns normalized order reports.
    #[must_use]
    pub const fn order_reports(&self) -> &IndexMap<VenueOrderId, OrderStatusReport> {
        &self.order_reports
    }

    /// Returns normalized fill reports.
    #[must_use]
    pub const fn fill_reports(&self) -> &IndexMap<VenueOrderId, Vec<FillReport>> {
        &self.fill_reports
    }

    /// Returns normalized position reports.
    #[must_use]
    pub const fn position_reports(&self) -> &IndexMap<InstrumentId, Vec<PositionStatusReport>> {
        &self.position_reports
    }

    /// Rebuilds the model snapshot without reopening normalization invariants.
    #[must_use]
    pub fn into_mass_status(self) -> ExecutionMassStatus {
        let mut mass_status = ExecutionMassStatus::new(
            self.client_id,
            self.account_id,
            self.venue,
            self.ts_init,
            Some(self.report_id),
        );
        mass_status
            .add_order_reports(self.order_reports.into_values().collect())
            .expect("normalized order reports are unique");
        mass_status.add_fill_reports(self.fill_reports.into_values().flatten().collect());
        mass_status.add_position_reports(self.position_reports.into_values().flatten().collect());
        mass_status
    }
}

fn normalize_fills(
    grouped_fills: IndexMap<VenueOrderId, Vec<FillReport>>,
    order_reports: &mut IndexMap<VenueOrderId, OrderStatusReport>,
) -> anyhow::Result<IndexMap<VenueOrderId, Vec<FillReport>>> {
    let mut seen = IndexMap::<FillKey, FillReport>::new();

    for (group_venue_order_id, fills) in grouped_fills {
        for fill in fills {
            ensure!(
                fill.venue_order_id == group_venue_order_id,
                "Fill group key {group_venue_order_id} conflicts with report venue order {}",
                fill.venue_order_id
            );

            let key = (fill.account_id, fill.instrument_id, fill.trade_id);
            if let Some(previous) = seen.get_mut(&key) {
                *previous = merge_repeated_fill(previous, &fill)?;
            } else {
                seen.insert(key, fill);
            }
        }
    }

    let mut normalized = IndexMap::<VenueOrderId, Vec<FillReport>>::new();
    for fill in seen.into_values() {
        normalized
            .entry(fill.venue_order_id)
            .or_default()
            .push(fill);
    }

    for (venue_order_id, fills) in &mut normalized {
        fills.sort_by_key(|fill| fill.ts_event);
        normalize_order_fill_identity(
            *venue_order_id,
            order_reports.get_mut(venue_order_id),
            fills,
        )
        .with_context(|| format!("Invalid evidence for venue order {venue_order_id}"))?;
    }

    Ok(normalized)
}

fn normalize_order_fill_identity(
    venue_order_id: VenueOrderId,
    mut order: Option<&mut OrderStatusReport>,
    fills: &mut [FillReport],
) -> anyhow::Result<()> {
    let Some(first_fill) = fills.first() else {
        return Ok(());
    };

    let account_id = first_fill.account_id;
    let instrument_id = first_fill.instrument_id;
    let order_side = first_fill.order_side;
    let client_order_id = unique_optional(fills.iter().filter_map(|fill| fill.client_order_id))
        .context("Conflicting client order IDs across fills")?;
    let venue_position_id = unique_optional(fills.iter().filter_map(|fill| fill.venue_position_id))
        .context("Conflicting venue position IDs across fills")?;

    ensure!(
        fills.iter().all(|fill| {
            fill.venue_order_id == venue_order_id
                && fill.account_id == account_id
                && fill.instrument_id == instrument_id
                && fill.order_side == order_side
        }),
        "Fills for one venue order disagree on account, instrument, or side"
    );

    if let Some(report) = order.as_deref_mut() {
        let reported_fill_quantity = fills.iter().try_fold(
            Quantity::zero(report.filled_qty.precision),
            |total, fill| {
                total.checked_add(fill.last_qty).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Companion fill quantity overflows for venue order {venue_order_id}"
                    )
                })
            },
        )?;
        ensure!(
            reported_fill_quantity <= report.filled_qty,
            "Companion fills exceed the reported cumulative fill quantity"
        );
        ensure!(
            report.venue_order_id == venue_order_id
                && report.account_id == account_id
                && report.instrument_id == instrument_id
                && report.order_side == order_side,
            "Order and fill evidence disagree on account, instrument, or side"
        );
        report.client_order_id = merge_optional(report.client_order_id, client_order_id)
            .context("Order and fill evidence disagree on client order ID")?;
        report.venue_position_id = merge_optional(report.venue_position_id, venue_position_id)
            .context("Order and fill evidence disagree on venue position ID")?;
    }

    let client_order_id = order
        .as_deref()
        .and_then(|report| report.client_order_id)
        .or(client_order_id);
    let venue_position_id = order
        .as_deref()
        .and_then(|report| report.venue_position_id)
        .or(venue_position_id);

    for fill in fills {
        fill.client_order_id = merge_optional(fill.client_order_id, client_order_id)
            .context("Fill evidence disagrees on client order ID")?;
        fill.venue_position_id = merge_optional(fill.venue_position_id, venue_position_id)
            .context("Fill evidence disagrees on venue position ID")?;
    }

    Ok(())
}

fn normalize_positions(
    grouped_positions: IndexMap<InstrumentId, Vec<PositionStatusReport>>,
) -> anyhow::Result<IndexMap<InstrumentId, Vec<PositionStatusReport>>> {
    let mut normalized = IndexMap::new();
    let mut hedge_positions = IndexMap::<PositionId, PositionStatusReport>::new();

    for (group_instrument_id, reports) in grouped_positions {
        for report in &reports {
            ensure!(
                report.instrument_id == group_instrument_id,
                "Position group key {group_instrument_id} conflicts with report instrument {}",
                report.instrument_id
            );
        }

        let hedge_mode = reports
            .first()
            .and_then(|report| report.venue_position_id)
            .is_some();
        ensure!(
            reports
                .iter()
                .all(|report| report.venue_position_id.is_some() == hedge_mode),
            "Mixed netting and hedging reports for {group_instrument_id}"
        );

        let reports = if hedge_mode {
            let mut by_position = IndexMap::<PositionId, PositionStatusReport>::new();
            for report in reports {
                let position_id = report
                    .venue_position_id
                    .context("Hedge report missing venue position ID")?;
                if let Some(previous) = hedge_positions.get(&position_id) {
                    ensure!(
                        equivalent_position(previous, &report),
                        "Conflicting reports reuse venue position ID {position_id}"
                    );
                    continue;
                }
                hedge_positions.insert(position_id, report.clone());
                by_position.insert(position_id, report);
            }
            by_position.into_values().collect()
        } else {
            let mut reports = reports.into_iter();
            let Some(mut canonical) = reports.next() else {
                normalized.insert(group_instrument_id, Vec::new());
                continue;
            };
            for report in reports {
                ensure!(
                    equivalent_position(&canonical, &report),
                    "Conflicting netting reports for {group_instrument_id}"
                );
                if report.ts_last > canonical.ts_last {
                    canonical = report;
                }
            }
            vec![canonical]
        };

        normalized.insert(group_instrument_id, reports);
    }

    Ok(normalized)
}

fn merge_repeated_fill(lhs: &FillReport, rhs: &FillReport) -> anyhow::Result<FillReport> {
    let client_order_id = merge_optional(lhs.client_order_id, rhs.client_order_id)
        .context("Conflicting repeated fill client order IDs")?;
    let venue_position_id = merge_optional(lhs.venue_position_id, rhs.venue_position_id)
        .context("Conflicting repeated fill venue position IDs")?;
    let avg_px = merge_optional(lhs.avg_px, rhs.avg_px)
        .context("Conflicting repeated fill average prices")?;
    let mut merged = lhs.clone();
    merged.client_order_id = client_order_id;
    merged.venue_position_id = venue_position_id;
    merged.avg_px = avg_px;
    let canonical_lhs = merged.clone();
    let mut canonical_rhs = rhs.clone();
    canonical_rhs.client_order_id = client_order_id;
    canonical_rhs.venue_position_id = venue_position_id;
    canonical_rhs.avg_px = avg_px;
    canonical_rhs.report_id = canonical_lhs.report_id;
    canonical_rhs.ts_init = canonical_lhs.ts_init;
    ensure!(
        canonical_lhs == canonical_rhs,
        "Conflicting repeated fill {} for {}",
        merged.trade_id,
        merged.instrument_id
    );
    Ok(merged)
}

fn equivalent_position(lhs: &PositionStatusReport, rhs: &PositionStatusReport) -> bool {
    let mut rhs = rhs.clone();
    rhs.report_id = lhs.report_id;
    rhs.ts_last = lhs.ts_last;
    rhs.ts_init = lhs.ts_init;
    lhs == &rhs
}

fn unique_optional<T: Copy + Eq>(values: impl IntoIterator<Item = T>) -> anyhow::Result<Option<T>> {
    let mut values = values.into_iter();
    let first = values.next();
    ensure!(
        values.all(|value| Some(value) == first),
        "Optional identity values conflict"
    );
    Ok(first)
}

fn merge_optional<T: Copy + Eq>(lhs: Option<T>, rhs: Option<T>) -> anyhow::Result<Option<T>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) if lhs != rhs => anyhow::bail!("Optional identity values conflict"),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{
            LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSideSpecified, TimeInForce,
        },
        identifiers::{ClientOrderId, PositionId},
        types::{Currency, Money, Price, Quantity, quantity::QUANTITY_RAW_MAX},
    };

    use super::*;

    fn account_id() -> AccountId {
        AccountId::from("SIM-001")
    }

    fn client_id() -> ClientId {
        ClientId::from("SIM")
    }

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT.SIM")
    }

    fn venue_order_id() -> VenueOrderId {
        VenueOrderId::from("V-1")
    }

    fn mass_status() -> ExecutionMassStatus {
        ExecutionMassStatus::new(
            client_id(),
            account_id(),
            Venue::from("SIM"),
            UnixNanos::from(10),
            Some(UUID4::new()),
        )
    }

    fn fill_report(trade_id: &str, last_qty: &str) -> FillReport {
        FillReport::new(
            account_id(),
            instrument_id(),
            venue_order_id(),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from(last_qty),
            Price::from("100.00"),
            Money::new(0.10, Currency::USD()),
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(5),
            UnixNanos::from(10),
            Some(UUID4::new()),
        )
    }

    fn order_report() -> OrderStatusReport {
        OrderStatusReport::new(
            account_id(),
            instrument_id(),
            None,
            venue_order_id(),
            OrderSide::Buy,
            OrderType::Market,
            TimeInForce::Gtc,
            OrderStatus::Filled,
            Quantity::from("2"),
            Quantity::from("2"),
            UnixNanos::from(1),
            UnixNanos::from(5),
            UnixNanos::from(10),
            Some(UUID4::new()),
        )
    }

    fn position_report(
        instrument_id: InstrumentId,
        position_id: Option<PositionId>,
        quantity: &str,
        ts_last: u64,
    ) -> PositionStatusReport {
        PositionStatusReport::new(
            account_id(),
            instrument_id,
            PositionSideSpecified::Long,
            Quantity::from(quantity),
            UnixNanos::from(ts_last),
            UnixNanos::from(ts_last + 1),
            Some(UUID4::new()),
            position_id,
            Some(rust_decimal::Decimal::from(100)),
        )
    }

    fn normalize(
        mass_status: ExecutionMassStatus,
    ) -> anyhow::Result<NormalizedExecutionMassStatus> {
        NormalizedExecutionMassStatus::normalize(AuthenticatedExecutionMassStatus::new(
            client_id(),
            ExecutionSourceId::new(),
            mass_status,
        ))
    }

    #[test]
    fn equivalent_repeated_fill_is_collapsed_and_enriched() {
        let mut first = fill_report("T-1", "1");
        first.client_order_id = Some(ClientOrderId::from("O-1"));
        let mut repeated = first.clone();
        repeated.report_id = UUID4::new();
        repeated.ts_init = UnixNanos::from(20);
        repeated.venue_position_id = Some(PositionId::from("P-1"));
        first.venue_position_id = None;
        let mut mass_status = mass_status();
        mass_status.add_fill_reports(vec![first, repeated]);

        let normalized = normalize(mass_status).unwrap();
        let fills = normalized.fill_reports().get(&venue_order_id()).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].client_order_id, Some(ClientOrderId::from("O-1")));
        assert_eq!(fills[0].venue_position_id, Some(PositionId::from("P-1")));
    }

    #[test]
    fn conflicting_repeated_fill_rejects_snapshot() {
        let mut mass_status = mass_status();
        mass_status.add_fill_reports(vec![fill_report("T-1", "1"), fill_report("T-1", "2")]);

        assert!(normalize(mass_status).is_err());
    }

    #[test]
    fn companion_fill_quantity_overflow_rejects_snapshot() {
        let half_plus_one = Quantity::from_raw((QUANTITY_RAW_MAX / 2) + 1, 0);
        let mut order = order_report();
        order.quantity = Quantity::from_raw(QUANTITY_RAW_MAX, 0);
        order.filled_qty = Quantity::from_raw(QUANTITY_RAW_MAX, 0);
        let mut first = fill_report("T-OVERFLOW-1", "1");
        first.last_qty = half_plus_one;
        let mut second = fill_report("T-OVERFLOW-2", "1");
        second.last_qty = half_plus_one;
        let mut mass_status = mass_status();
        mass_status.add_order_reports(vec![order]).unwrap();
        mass_status.add_fill_reports(vec![first, second]);

        let result = normalize(mass_status);

        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("overflows"), "{error:#}");
    }

    #[test]
    fn order_and_fill_identity_is_canonicalized_once() {
        let mut order = order_report();
        order.venue_position_id = Some(PositionId::from("P-1"));
        let mut fill = fill_report("T-1", "2");
        fill.client_order_id = Some(ClientOrderId::from("O-1"));
        let mut mass_status = mass_status();
        mass_status.add_order_reports(vec![order]).unwrap();
        mass_status.add_fill_reports(vec![fill]);

        let normalized = normalize(mass_status).unwrap();
        let order = normalized.order_reports().get(&venue_order_id()).unwrap();
        let fill = &normalized.fill_reports().get(&venue_order_id()).unwrap()[0];

        assert_eq!(order.client_order_id, Some(ClientOrderId::from("O-1")));
        assert_eq!(fill.venue_position_id, Some(PositionId::from("P-1")));
    }

    #[test]
    fn order_and_fill_side_conflict_rejects_snapshot() {
        let order = order_report();
        let mut fill = fill_report("T-1", "2");
        fill.order_side = OrderSide::Sell;
        let mut mass_status = mass_status();
        mass_status.add_order_reports(vec![order]).unwrap();
        mass_status.add_fill_reports(vec![fill]);

        assert!(normalize(mass_status).is_err());
    }

    #[test]
    fn equivalent_netting_reports_are_collapsed_by_economic_state() {
        let mut mass_status = mass_status();
        mass_status.add_position_reports(vec![
            position_report(instrument_id(), None, "2", 5),
            position_report(instrument_id(), None, "2", 8),
        ]);

        let normalized = normalize(mass_status).unwrap();
        let positions = normalized.position_reports().get(&instrument_id()).unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].ts_last, UnixNanos::from(8));
    }

    #[test]
    fn conflicting_netting_reports_reject_snapshot() {
        let mut mass_status = mass_status();
        mass_status.add_position_reports(vec![
            position_report(instrument_id(), None, "2", 5),
            position_report(instrument_id(), None, "3", 8),
        ]);

        assert!(normalize(mass_status).is_err());
    }

    #[test]
    fn mixed_netting_and_hedging_reports_reject_snapshot() {
        let mut mass_status = mass_status();
        mass_status.add_position_reports(vec![
            position_report(instrument_id(), None, "2", 5),
            position_report(instrument_id(), Some(PositionId::from("P-1")), "2", 5),
        ]);

        assert!(normalize(mass_status).is_err());
    }

    #[test]
    fn reused_hedge_position_id_with_conflicting_instrument_rejects_snapshot() {
        let position_id = Some(PositionId::from("P-1"));
        let mut mass_status = mass_status();
        mass_status.add_position_reports(vec![
            position_report(instrument_id(), position_id, "2", 5),
            position_report(InstrumentId::from("ETHUSDT.SIM"), position_id, "2", 5),
        ]);

        assert!(normalize(mass_status).is_err());
    }
}
