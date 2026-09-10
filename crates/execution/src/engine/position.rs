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

//! Position types the [`ExecutionEngine`](super::ExecutionEngine) publishes or carries between
//! the stages of a fill correction.

use std::rc::Rc;

use indexmap::IndexMap;
use nautilus_common::cache::{Cache, CacheSnapshotRef};
use nautilus_core::UnixNanos;
use nautilus_model::{
    events::{OrderEventAny, OrderFillVoided},
    identifiers::PositionId,
    orders::{Order, OrderAny},
    position::{Position, PositionReplayEvent},
    types::{Money, Quantity},
};

/// Position state snapshot published to the `snapshots.position.{position_id}` topic.
#[derive(Debug, Clone)]
pub struct PositionStateSnapshot {
    /// The position state at the time of the snapshot.
    pub position: Position,
    /// The unrealized PnL for the position, when a current quote is available.
    pub unrealized_pnl: Option<Money>,
    /// UNIX timestamp (nanoseconds) when the snapshot was taken.
    pub ts_snapshot: UnixNanos,
}

/// Callback that anchors cache snapshot metadata in an external store.
pub type SnapshotAnchorer = Rc<dyn Fn(CacheSnapshotRef) -> anyhow::Result<()>>;

/// A position rebuilt by a fill void, with the quantity that correction removed.
///
/// `absorbed_prior_cycles` is set when the voided fill sits outside the position's current
/// NETTING cycle, so the rebuild spans earlier cycles and the archive frames describing them no
/// longer match the corrected history. `closed_cycles_pnl` then holds the realized PnL of
/// whatever cycles the corrected history does close before the current one, which settles those
/// frames. It is `None` when the corrected history never goes flat, leaving no archived cycle.
///
/// Known limitation: the flag is decided from quantities, so a second correction to the same
/// trade that revises only the voided commission leaves it unset, and the settled frame keeps
/// the realized PnL banked by the first. No in-tree emitter produces that shape, since
/// reconciliation always advances the quantity and the adapters void a fill once.
#[derive(Debug)]
pub struct CorrectedPosition {
    pub(super) position: Position,
    pub(super) corrected_qty: Quantity,
    pub(super) absorbed_prior_cycles: bool,
    pub(super) closed_cycles_pnl: Option<Money>,
}

impl CorrectedPosition {
    /// Applies the corrected position and any rebuilt archive cycles to the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot update the corrected position.
    pub fn apply_to_cache(&self, cache: &mut Cache) -> anyhow::Result<()> {
        cache.update_position(&self.position)?;
        if self.absorbed_prior_cycles {
            cache.settle_position_snapshots(&self.position, self.closed_cycles_pnl);
        }
        Ok(())
    }
}

/// Reconstructs positions affected by a fill void without changing the cache.
///
/// Live execution and event-store replay use the same fragment allocation and native
/// position arithmetic. Only affected positions are cloned for correction.
/// Archive settlement requires retained replay history covering every archived cycle for
/// the position; this helper does not establish that coverage.
///
/// # Errors
///
/// Returns an error when the source fill or position history is missing, the allocation
/// exceeds known fragments, or a native position rejects the correction.
pub fn prepare_fill_void_positions(
    cache: &Cache,
    order: &OrderAny,
    event: &OrderFillVoided,
) -> anyhow::Result<Vec<CorrectedPosition>> {
    let source_event_id = order
        .events()
        .into_iter()
        .find_map(|order_event| match order_event {
            OrderEventAny::Filled(fill) if fill.trade_id == event.trade_id => Some(fill.event_id),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("fill {} is not in order history", event.trade_id))?;

    let mut fragments = Vec::new();

    for position in cache.positions(
        None,
        Some(&event.instrument_id),
        Some(&event.strategy_id),
        Some(&event.account_id),
        None,
    ) {
        for replay_event in &position.replay_events {
            let PositionReplayEvent::Filled(fill) = replay_event else {
                continue;
            };

            if fill.client_order_id != event.client_order_id || fill.trade_id != event.trade_id {
                continue;
            }
            let split_rank = if fill.event_id == source_event_id {
                0
            } else if fill.causation_id == Some(source_event_id) {
                1
            } else {
                continue;
            };
            fragments.push((position.id, split_rank, fill.last_qty, fill.commission));
        }
    }
    anyhow::ensure!(
        !fragments.is_empty(),
        "no position fragments found for fill {}",
        event.trade_id
    );
    fragments.sort_by_key(|(_, split_rank, _, _)| *split_rank);

    let mut allocations = IndexMap::<PositionId, (Quantity, Option<Money>)>::new();
    let mut remaining_qty = event.voided_qty;
    for (position_id, _, quantity, _) in fragments.iter().rev() {
        if remaining_qty.is_zero() {
            break;
        }
        let removed = remaining_qty.min(*quantity);
        allocations
            .entry(*position_id)
            .and_modify(|allocation| allocation.0 = allocation.0 + removed)
            .or_insert((removed, None));
        remaining_qty = remaining_qty - removed;
    }
    anyhow::ensure!(
        remaining_qty.is_zero(),
        "position fragments do not cover voided quantity for fill {}",
        event.trade_id
    );

    if let Some(mut remaining_commission) = event.commission_voided {
        for (position_id, _, _, commission) in fragments.iter().rev() {
            if remaining_commission.is_zero() {
                break;
            }
            let Some(commission) = commission else {
                continue;
            };
            anyhow::ensure!(
                commission.currency == remaining_commission.currency,
                "position commission currency differs for fill {}",
                event.trade_id
            );
            let removed_raw = remaining_commission.raw.abs().min(commission.raw.abs());
            let removed = Money::from_raw(
                removed_raw * remaining_commission.raw.signum(),
                remaining_commission.currency,
            );
            allocations
                .entry(*position_id)
                .and_modify(|allocation| {
                    allocation.1 = Some(
                        allocation
                            .1
                            .map_or(removed, |commission| commission + removed),
                    );
                })
                .or_insert((Quantity::zero(event.voided_qty.precision), Some(removed)));
            remaining_commission = remaining_commission - removed;
        }
        anyhow::ensure!(
            remaining_commission.is_zero(),
            "position fragments do not cover voided commission for fill {}",
            event.trade_id
        );
    }

    let mut corrected_positions = Vec::new();

    for (position_id, (voided_qty, commission_voided)) in allocations {
        if voided_qty.is_zero() {
            anyhow::bail!(
                "commission-only position correction requires authoritative reconciliation for fill {}",
                event.trade_id
            );
        }
        let mut position = cache
            .position_owned(&position_id)
            .ok_or_else(|| anyhow::anyhow!("position {position_id} is not cached"))?;
        let previous = position
            .fill_voids
            .iter()
            .rev()
            .find(|record| {
                record.event.client_order_id == event.client_order_id
                    && record.event.trade_id == event.trade_id
            })
            .map(|record| (record.voided_qty, record.commission_voided));
        if previous == Some((voided_qty, commission_voided)) {
            continue;
        }
        let corrected_qty = previous.map_or(voided_qty, |(prior_qty, _)| {
            voided_qty.saturating_sub(prior_qty)
        });

        // `events` holds the fills since the position was last flat, because `apply_fill`
        // clears it when reopening from flat. A NETTING flip splits one fill across the
        // closing and reopening cycles under the same trade, so compare quantities rather
        // than presence: the correction reaches an earlier cycle once it exceeds what the
        // current cycle originally held. Earlier corrections have already shrunk the
        // fragments in `events` while `voided_qty` stays cumulative, so add back what this
        // position already voided. Read this before `apply_fill_void`, whose rebuild
        // re-derives `events` and can move that boundary.
        let previously_voided = previous
            .map_or(Quantity::zero(position.size_precision), |(prior_qty, _)| {
                prior_qty
            });
        let current_cycle_qty = position
            .events
            .iter()
            .filter(|fill| {
                fill.client_order_id == event.client_order_id && fill.trade_id == event.trade_id
            })
            .fold(previously_voided, |total, fill| total + fill.last_qty);
        let absorbed_prior_cycles = voided_qty > current_cycle_qty;
        let closed_cycles_pnl =
            position.apply_fill_void(event.clone(), voided_qty, commission_voided)?;
        corrected_positions.push(CorrectedPosition {
            position,
            corrected_qty,
            absorbed_prior_cycles,
            closed_cycles_pnl,
        });
    }
    Ok(corrected_positions)
}
