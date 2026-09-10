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

//! Position snapshot storage for the platform [`Cache`].
//!
//! Three mechanisms share the "position snapshot" name and live here together:
//!
//! - The NETTING archive ([`Cache::snapshot_position`]), which preserves each closed position
//!   cycle before its ID is reused, and backs cross-cycle realized PnL.
//! - The durable correction boundary ([`Cache::snapshot_position_encoded`],
//!   [`Cache::restore_snapshot_blob`]), which produces and restores the encoded frames an event
//!   store anchors.
//! - The routine state snapshot ([`Cache::snapshot_position_state`]), which writes position state
//!   to the backing database, defaulting to open positions.

use std::{cell::OnceCell, str::FromStr};

use ahash::AHashSet;
use bytes::Bytes;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    identifiers::{AccountId, InstrumentId, PositionId},
    position::Position,
    types::Money,
};
use serde::{Deserialize, Serialize};

use super::Cache;

/// Cache-owned reference to a snapshot blob.
///
/// The cache writes and later fetches the blob; external systems persist this opaque reference
/// and may hash the bytes before recording a durable anchor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheSnapshotRef {
    /// Opaque cache-owned snapshot location.
    pub blob_ref: String,
    /// Snapshot bytes stored under [`Self::blob_ref`].
    pub blob: Bytes,
}

impl CacheSnapshotRef {
    /// Creates a new [`CacheSnapshotRef`].
    #[must_use]
    pub fn new(blob_ref: impl Into<String>, blob: impl Into<Bytes>) -> Self {
        Self {
            blob_ref: blob_ref.into(),
            blob: blob.into(),
        }
    }
}

/// Distinguishes a snapshot of one cycle from a correction's aggregate of earlier cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PositionSnapshotKind {
    Cycle,
    RebuiltPriorCycles,
}

/// One frame in a position's NETTING snapshot history.
///
/// Encoding is deferred until its bytes are needed. Restored frames preserve their exact bytes,
/// including their kind, because event-store anchors record the content hash.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PositionSnapshotFrame {
    kind: PositionSnapshotKind,
    #[serde(flatten)]
    position: Position,
    #[serde(skip)]
    encoded: OnceCell<Bytes>,
}

impl PositionSnapshotFrame {
    fn new(position: Position, kind: PositionSnapshotKind) -> Self {
        Self {
            kind,
            position,
            encoded: OnceCell::new(),
        }
    }

    fn encoded(&self) -> anyhow::Result<Bytes> {
        if let Some(encoded) = self.encoded.get() {
            return Ok(encoded.clone());
        }

        let encoded = Bytes::from(serde_json::to_vec(self)?);
        let _ = self.encoded.set(encoded.clone());

        Ok(encoded)
    }
}

impl Cache {
    /// Creates a snapshot of the `position` by cloning it, assigning a new ID, and storing it
    /// in the position snapshots.
    ///
    /// The copy excludes `replay_events` and `fill_voids`, which no snapshot consumer reads,
    /// so snapshot size stays independent of the fills applied to the position ID. The copy
    /// encodes only when a consumer asks for the bytes, so this call stays off the encode path
    /// unless a backing database has to persist the frame.
    ///
    /// # Errors
    ///
    /// Returns an error if serializing or storing the position snapshot fails.
    pub fn snapshot_position(&mut self, position: &Position) -> anyhow::Result<()> {
        let (blob_ref, snapshot) =
            self.build_position_snapshot(position, PositionSnapshotKind::Cycle);

        if self.database.is_some() {
            self.persist_position_snapshot(&blob_ref, &snapshot)?;
        }
        self.store_position_snapshot(position.id, snapshot);

        Ok(())
    }

    /// Creates a snapshot of the `position` and returns its encoded cache-owned reference.
    ///
    /// Behaves as [`Self::snapshot_position`] but encodes the frame eagerly, for callers that
    /// record the bytes or their content hash against a durable anchor.
    ///
    /// # Errors
    ///
    /// Returns an error if serializing or storing the position snapshot fails.
    pub fn snapshot_position_encoded(
        &mut self,
        position: &Position,
    ) -> anyhow::Result<CacheSnapshotRef> {
        let (blob_ref, snapshot) =
            self.build_position_snapshot(position, PositionSnapshotKind::Cycle);
        let encoded = self.persist_position_snapshot(&blob_ref, &snapshot)?;

        self.store_position_snapshot(position.id, snapshot);

        Ok(CacheSnapshotRef::new(blob_ref, encoded))
    }

    /// Replaces every NETTING archive frame held for `position` with the cycles a correction
    /// rebuilt, worth `closed_cycles_pnl`.
    ///
    /// A correction that reaches an earlier cycle moves the boundaries the existing frames
    /// describe, so they cannot be reconciled and are settled into one frame instead. Pass
    /// `None` when the corrected history never goes flat, which leaves no archived cycle at all.
    /// As with [`Self::purge_position`], the durable `cache://position-snapshots/...` entries
    /// stay in general cache state.
    ///
    /// Requires `closed_cycles_pnl` to account for every frame held, since this removes all of
    /// them. The position's replay log must therefore span every archived cycle for the ID. Any
    /// future retention cap on the log has to preserve that at trim time, either by folding the
    /// trimmed cycles' realized PnL into a baseline the rebuild adds to its banked total, or by
    /// purging the frames those cycles produced in the same operation. Settling cannot detect the
    /// shortfall, because frames carry no cycle identity to match against the retained log.
    ///
    /// Frame indices restart from zero, but each frame's reference includes its unique snapshot
    /// ID, so replacing frames cannot reuse a previously issued durable reference.
    pub fn settle_position_snapshots(
        &mut self,
        position: &Position,
        closed_cycles_pnl: Option<Money>,
    ) {
        self.position_snapshots.remove(&position.id);
        self.bump_position_snapshot_revision(position.id);

        if let Some(closed_cycles_pnl) = closed_cycles_pnl {
            let (_, mut settled) =
                self.build_position_snapshot(position, PositionSnapshotKind::RebuiltPriorCycles);
            settled.position.realized_pnl = Some(closed_cycles_pnl);
            self.store_position_snapshot(position.id, settled);
        }
    }

    /// Records that the frames held for `position_id` were replaced rather than appended to.
    ///
    /// Consumers cache per-position aggregates keyed off the frame count, which settling and
    /// purging can leave unchanged while the frames behind it differ.
    pub(super) fn bump_position_snapshot_revision(&mut self, position_id: PositionId) {
        *self
            .position_snapshot_revisions
            .entry(position_id)
            .or_default() += 1;
    }

    fn build_position_snapshot(
        &self,
        position: &Position,
        kind: PositionSnapshotKind,
    ) -> (String, PositionSnapshotFrame) {
        let position_id = position.id;

        let mut copied_position = position.clone_for_snapshot();
        let snapshot_uuid = UUID4::new();
        let new_id = format!("{}-{snapshot_uuid}", position_id.as_str());
        copied_position.id = PositionId::new(new_id);
        let blob_ref = position_snapshot_blob_ref(
            &position_id,
            self.position_snapshot_count(&position_id),
            snapshot_uuid,
        );

        (blob_ref, PositionSnapshotFrame::new(copied_position, kind))
    }

    fn persist_position_snapshot(
        &mut self,
        blob_ref: &str,
        snapshot: &PositionSnapshotFrame,
    ) -> anyhow::Result<Bytes> {
        let encoded = snapshot.encoded()?;
        self.add(blob_ref, encoded.clone())?;

        Ok(encoded)
    }

    /// Stores the frame after any persist step, so a failed write does not advance the count.
    fn store_position_snapshot(
        &mut self,
        position_id: PositionId,
        snapshot: PositionSnapshotFrame,
    ) {
        log::debug!("Snapshot {}", snapshot.position);

        self.position_snapshots
            .entry(position_id)
            .or_default()
            .push(snapshot);
    }

    /// Returns the last archived amount when that frame represents the supplied closed cycle.
    ///
    /// Opening-fill identity survives later fee or funding changes within the same cycle.
    /// The saved frame may have been taken while that cycle was still open.
    /// Rebuilt prior-cycle totals never represent the current cycle, even when their copied
    /// position fields or monetary amounts coincide. Reads the cached frame without cloning it.
    ///
    /// # Errors
    ///
    /// Returns an error when a potentially matching cycle lacks its opening fill identity,
    /// or when a matching cycle lacks the current or archived PnL required for deduplication.
    pub fn position_snapshot_pnl_for_current_cycle(
        &self,
        position: &Position,
    ) -> anyhow::Result<Option<Money>> {
        let Some(frame) = self
            .position_snapshots
            .get(&position.id)
            .and_then(|frames| frames.last())
        else {
            return Ok(None);
        };

        if position.is_open() || frame.kind == PositionSnapshotKind::RebuiltPriorCycles {
            return Ok(None);
        }
        let opening_fill = position.events.first().ok_or_else(|| {
            anyhow::anyhow!(
                "closed position {} has no opening fill identity",
                position.id
            )
        })?;
        let archived_opening_fill = frame.position.events.first().ok_or_else(|| {
            anyhow::anyhow!(
                "position archive {} has no opening fill identity",
                position.id
            )
        })?;
        let same_cycle = opening_fill.event_id == archived_opening_fill.event_id
            && position.account_id == frame.position.account_id
            && position.instrument_id == frame.position.instrument_id
            && position.strategy_id == frame.position.strategy_id;

        if !same_cycle {
            return Ok(None);
        }
        anyhow::ensure!(
            position.realized_pnl.is_some(),
            "closed position {} has no realized PnL for its archived cycle",
            position.id,
        );
        let archived_pnl = frame.position.realized_pnl.ok_or_else(|| {
            anyhow::anyhow!("position archive {} has no realized PnL", position.id)
        })?;
        Ok(Some(archived_pnl))
    }

    fn position_snapshot_frame(&self, blob_ref: &str) -> Option<&PositionSnapshotFrame> {
        let (position_id, snapshot_index, snapshot_uuid) =
            parse_position_snapshot_blob_ref(blob_ref).ok()?;

        self.position_snapshots
            .get(&position_id)
            .and_then(|frames| frames.get(snapshot_index))
            .filter(|frame| {
                position_snapshot_uuid(&position_id, &frame.position).ok() == Some(snapshot_uuid)
            })
    }

    /// Loads the cache-owned snapshot blob stored under `blob_ref`.
    ///
    /// The cache first checks in-memory snapshot state. When the blob is not present and a
    /// database adapter exists, the generic cache entries are loaded and checked for the same
    /// opaque reference.
    ///
    /// # Errors
    ///
    /// Returns an error if loading generic cache entries from the backing database fails.
    pub fn load_snapshot_blob(&mut self, blob_ref: &str) -> anyhow::Result<Option<Bytes>> {
        if let Some(blob) = self.snapshot_blob(blob_ref) {
            return Ok(Some(blob));
        }

        if self.database.is_some() {
            self.cache_general()?;
        }

        Ok(self.snapshot_blob(blob_ref))
    }

    /// Restores the cache-owned snapshot blob stored under `blob_ref`.
    ///
    /// Only cache-owned `cache://position-snapshots/...` blobs are currently supported.
    ///
    /// # Errors
    ///
    /// Returns an error if the blob reference is unsupported, malformed, skips earlier
    /// snapshot frames, conflicts with an existing frame, or does not decode to the expected
    /// position snapshot. Index-only references are rejected because replacement frames can reuse
    /// an index. Legacy blobs without a frame kind are rejected because a cycle and a
    /// rebuilt prior-cycle total cannot be distinguished from position fields alone.
    /// Previously loaded bytes remain authoritative after their frame is removed. This method
    /// does not load backing database entries; anchored restore loads and checks those first.
    pub fn restore_snapshot_blob(&mut self, blob_ref: &str, blob: Bytes) -> anyhow::Result<()> {
        let (position_id, snapshot_index, snapshot_uuid) =
            parse_position_snapshot_blob_ref(blob_ref)?;
        let restored: PositionSnapshotFrame = serde_json::from_slice(&blob)?;
        anyhow::ensure!(
            position_snapshot_uuid(&position_id, &restored.position)? == snapshot_uuid,
            "position snapshot id {} does not match blob_ref snapshot {snapshot_uuid}",
            restored.position.id,
        );

        if let Some(existing) = self.general.get(blob_ref) {
            anyhow::ensure!(
                existing == &blob,
                "position snapshot {blob_ref} already exists with different bytes",
            );
        }

        let frames = self.position_snapshots.entry(position_id).or_default();
        match frames.get(snapshot_index) {
            Some(existing) if existing.encoded()? == blob => {}
            Some(_) => {
                anyhow::bail!(
                    "position snapshot frame {snapshot_index} for {position_id} already exists with different bytes"
                );
            }
            None if frames.len() == snapshot_index => {
                let _ = restored.encoded.set(blob.clone());
                frames.push(restored);
            }
            None => {
                anyhow::bail!(
                    "position snapshot blob_ref {blob_ref} skips missing frame {}",
                    frames.len()
                );
            }
        }

        self.general.insert(blob_ref.to_string(), blob);
        Ok(())
    }

    /// Returns the immutable reference of the frame at `index` without encoding it.
    ///
    /// The index selects a currently held frame; it is not a durable identity. Retain the
    /// returned reference when recording an anchor or transferring a frame to another cache.
    /// Use [`Self::load_snapshot_blob`] to read the bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame's snapshot ID is invalid.
    pub fn position_snapshot_blob_ref(
        &self,
        position_id: &PositionId,
        index: usize,
    ) -> anyhow::Result<Option<String>> {
        let Some(frame) = self
            .position_snapshots
            .get(position_id)
            .and_then(|frames| frames.get(index))
        else {
            return Ok(None);
        };
        let snapshot_uuid = position_snapshot_uuid(position_id, &frame.position)?;
        Ok(Some(position_snapshot_blob_ref(
            position_id,
            index,
            snapshot_uuid,
        )))
    }

    fn snapshot_blob(&self, blob_ref: &str) -> Option<Bytes> {
        if let Some(blob) = self.general.get(blob_ref) {
            return Some(blob.clone());
        }

        self.position_snapshot_frame(blob_ref)?
            .encoded()
            .inspect_err(|e| log::warn!("Failed to encode position snapshot {blob_ref}: {e}"))
            .ok()
    }

    /// Creates a snapshot of the `position` state in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshotting the position state fails.
    pub fn snapshot_position_state(
        &mut self,
        position: &Position,
        ts_snapshot: UnixNanos,
        unrealized_pnl: Option<Money>,
        open_only: Option<bool>,
    ) -> anyhow::Result<()> {
        let open_only = open_only.unwrap_or(true);

        if open_only && !position.is_open() {
            return Ok(());
        }

        if let Some(database) = &mut self.database {
            database
                .snapshot_position_state(position, ts_snapshot, unrealized_pnl)
                .map_err(|e| {
                    log::error!(
                        "Failed to snapshot position state for {}: {e:?}",
                        position.id
                    );
                    e
                })?;
        } else {
            log::warn!(
                "Cannot snapshot position state for {} (no database configured)",
                position.id
            );
        }

        Ok(())
    }

    /// Gets the serialized position snapshot frames for the `position_id`.
    ///
    /// Each element in the returned vector is one JSON-encoded frame with an explicit kind
    /// and flattened [`Position`] fields,
    /// in the order they were taken. Frames that fail to serialize are skipped with a warning.
    #[must_use]
    pub fn position_snapshot_bytes(&self, position_id: &PositionId) -> Option<Vec<Vec<u8>>> {
        self.position_snapshots.get(position_id).map(|frames| {
            frames
                .iter()
                .filter_map(|frame| match frame.encoded() {
                    Ok(encoded) => Some(encoded.to_vec()),
                    Err(e) => {
                        log::warn!("Failed to encode position snapshot: {e}");
                        None
                    }
                })
                .collect()
        })
    }

    /// Returns the number of stored snapshot frames for the `position_id`.
    ///
    /// Returns `0` when no frames are stored. Does not allocate or copy frame bytes.
    #[must_use]
    pub fn position_snapshot_count(&self, position_id: &PositionId) -> usize {
        self.position_snapshots.get(position_id).map_or(0, Vec::len)
    }

    /// Returns how many times the frames stored for the `position_id` were replaced.
    ///
    /// Pair this with [`Self::position_snapshot_count`] to detect frame changes: settling or
    /// purging can replace the frames without moving the count, so the count alone is not
    /// enough to tell whether cached per-position aggregates are still current.
    #[must_use]
    pub fn position_snapshot_revision(&self, position_id: &PositionId) -> u64 {
        self.position_snapshot_revisions
            .get(position_id)
            .copied()
            .unwrap_or(0)
    }

    /// Returns all position snapshots with the given optional filters.
    ///
    /// When `position_id` is `Some`, only snapshots for that position are returned.
    /// When `account_id` is `Some`, snapshots are filtered to that account.
    #[must_use]
    pub fn position_snapshots(
        &self,
        position_id: Option<&PositionId>,
        account_id: Option<&AccountId>,
    ) -> Vec<Position> {
        let frames: Box<dyn Iterator<Item = &PositionSnapshotFrame> + '_> = match position_id {
            Some(pid) => match self.position_snapshots.get(pid) {
                Some(v) => Box::new(v.iter()),
                None => Box::new(std::iter::empty()),
            },
            None => Box::new(self.position_snapshots.values().flat_map(|v| v.iter())),
        };

        let mut results: Vec<Position> = frames.map(|frame| frame.position.clone()).collect();

        if let Some(aid) = account_id {
            results.retain(|p| p.account_id == *aid);
        }

        results
    }

    /// Returns position snapshots for `position_id` starting from the `skip`th frame.
    ///
    /// Use this to read only newly appended snapshots when the caller already processed
    /// earlier frames. Returns an empty vector when at most `skip` frames are stored.
    #[must_use]
    pub fn position_snapshots_from(&self, position_id: &PositionId, skip: usize) -> Vec<Position> {
        let Some(frames) = self.position_snapshots.get(position_id) else {
            return Vec::new();
        };

        frames
            .iter()
            .skip(skip)
            .map(|frame| frame.position.clone())
            .collect()
    }

    /// Gets position snapshot IDs for the `instrument_id`.
    #[must_use]
    pub fn position_snapshot_ids(&self, instrument_id: &InstrumentId) -> AHashSet<PositionId> {
        // Get snapshot position IDs that match the instrument
        let mut result = AHashSet::new();

        for (position_id, _) in &self.position_snapshots {
            // Check if this position is for the requested instrument
            if let Some(position_cell) = self.positions.get(position_id)
                && position_cell.borrow().instrument_id == *instrument_id
            {
                result.insert(*position_id);
            }
        }
        result
    }
}

fn position_snapshot_blob_ref(
    position_id: &PositionId,
    index: usize,
    snapshot_uuid: UUID4,
) -> String {
    format!("cache://position-snapshots/{position_id}/{index}/{snapshot_uuid}")
}

fn parse_position_snapshot_blob_ref(blob_ref: &str) -> anyhow::Result<(PositionId, usize, UUID4)> {
    let Some(rest) = blob_ref.strip_prefix("cache://position-snapshots/") else {
        anyhow::bail!("unsupported cache snapshot blob_ref {blob_ref}");
    };

    let Some((position_index, snapshot_uuid)) = rest.rsplit_once('/') else {
        anyhow::bail!("malformed position snapshot blob_ref {blob_ref}");
    };
    let Some((position_id, snapshot_index)) = position_index.rsplit_once('/') else {
        anyhow::bail!("malformed position snapshot blob_ref {blob_ref}");
    };

    if position_id.is_empty() {
        anyhow::bail!("position snapshot blob_ref {blob_ref} has empty position id");
    }

    let snapshot_index = snapshot_index.parse::<usize>().map_err(|e| {
        anyhow::anyhow!("position snapshot blob_ref {blob_ref} has invalid frame index: {e}")
    })?;

    let snapshot_uuid = UUID4::from_str(snapshot_uuid).map_err(|e| {
        anyhow::anyhow!("position snapshot blob_ref {blob_ref} has invalid snapshot UUID: {e}")
    })?;

    Ok((PositionId::new(position_id), snapshot_index, snapshot_uuid))
}

fn position_snapshot_uuid(position_id: &PositionId, snapshot: &Position) -> anyhow::Result<UUID4> {
    let expected_prefix = format!("{}-", position_id.as_str());

    let Some(snapshot_uuid) = snapshot.id.as_str().strip_prefix(&expected_prefix) else {
        anyhow::bail!(
            "position snapshot id {} does not match blob_ref position {position_id}",
            snapshot.id
        );
    };

    UUID4::from_str(snapshot_uuid).map_err(|_| {
        anyhow::anyhow!(
            "position snapshot id {} does not match blob_ref position {position_id}",
            snapshot.id
        )
    })
}
