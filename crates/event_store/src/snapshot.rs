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

//! Snapshot anchors recorded by the event store.
//!
//! Cache snapshots remain owned by the cache backing store. The event store records only
//! the durable log high-watermark and declared coverage, plus enough metadata to fetch
//! and validate the blob. Only a full-cache checkpoint permits prefix skipping.

use serde::{Deserialize, Serialize};

use crate::error::EventStoreError;

/// Cache state represented by an anchored snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotCoverage {
    /// A subset of cache state, such as one archived position cycle.
    Partial,
    /// All cache state represented by the prefix through the anchor watermark,
    /// including archived position cycles and correction history.
    FullCache,
}

/// A pointer from a cache snapshot blob to a durable event-store high-watermark.
///
/// `blob_ref` and `content_hash` are intentionally opaque strings. The cache owns the
/// storage backend and hash algorithm; the event store only persists the metadata needed
/// to find the blob and prove the fetched bytes match the snapshot that was anchored.
/// The producer must declare coverage independently of byte integrity.
///
/// Stored anchors without coverage are rejected by the positional codec. They cannot
/// safely authorize prefix skipping or retention because their coverage is unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotAnchor {
    /// Durable event-store `seq` when the snapshot was anchored.
    pub high_watermark: u64,
    /// Cache-owned reference to the snapshot blob.
    pub blob_ref: String,
    /// Cache-owned content hash for the snapshot blob.
    pub content_hash: String,
    /// State covered by the snapshot. A matching hash alone does not establish coverage.
    pub coverage: SnapshotCoverage,
}

impl SnapshotAnchor {
    /// Creates a new [`SnapshotAnchor`].
    #[must_use]
    pub fn new(
        high_watermark: u64,
        blob_ref: impl Into<String>,
        content_hash: impl Into<String>,
        coverage: SnapshotCoverage,
    ) -> Self {
        Self {
            high_watermark,
            blob_ref: blob_ref.into(),
            content_hash: content_hash.into(),
            coverage,
        }
    }

    /// Returns whether the producer declares a complete cache checkpoint.
    #[must_use]
    pub const fn covers_full_cache(&self) -> bool {
        matches!(self.coverage, SnapshotCoverage::FullCache)
    }
}

/// Computes the default cache snapshot content hash recorded in a [`SnapshotAnchor`].
#[must_use]
pub fn compute_snapshot_content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub(crate) fn validate_new_anchor(
    anchor: &SnapshotAnchor,
    durable_high_watermark: u64,
    latest: Option<&SnapshotAnchor>,
) -> Result<(), EventStoreError> {
    if anchor.high_watermark > durable_high_watermark {
        return Err(EventStoreError::Backend(format!(
            "snapshot anchor high_watermark {} exceeds durable high_watermark {}",
            anchor.high_watermark, durable_high_watermark,
        )));
    }

    if let Some(latest) = latest
        && anchor.high_watermark < latest.high_watermark
    {
        return Err(EventStoreError::Backend(format!(
            "snapshot anchor high_watermark {} is older than latest anchor {}",
            anchor.high_watermark, latest.high_watermark,
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn anchor_new_sets_all_fields() {
        let anchor = SnapshotAnchor::new(
            7,
            "cache://snapshots/run-1/7",
            "blake3:abc",
            SnapshotCoverage::Partial,
        );

        assert_eq!(anchor.high_watermark, 7);
        assert_eq!(anchor.blob_ref, "cache://snapshots/run-1/7");
        assert_eq!(anchor.content_hash, "blake3:abc");
    }

    #[rstest]
    fn compute_snapshot_content_hash_prefixes_blake3_digest() {
        assert_eq!(
            compute_snapshot_content_hash(b"snapshot"),
            format!("blake3:{}", blake3::hash(b"snapshot").to_hex()),
        );
    }

    #[rstest]
    fn validate_rejects_anchor_past_durable_watermark() {
        let anchor = SnapshotAnchor::new(8, "blob", "hash", SnapshotCoverage::Partial);
        let err = validate_new_anchor(&anchor, 7, None).expect_err("must reject");

        match err {
            EventStoreError::Backend(msg) => {
                assert!(
                    msg.contains("exceeds durable high_watermark"),
                    "msg was: {msg}",
                );
            }
            other => panic!("expected Backend, was {other:?}"),
        }
    }

    #[rstest]
    fn validate_rejects_anchor_older_than_latest() {
        let latest = SnapshotAnchor::new(9, "latest", "hash-latest", SnapshotCoverage::Partial);
        let anchor = SnapshotAnchor::new(8, "older", "hash-older", SnapshotCoverage::Partial);
        let err = validate_new_anchor(&anchor, 10, Some(&latest)).expect_err("must reject");

        match err {
            EventStoreError::Backend(msg) => {
                assert!(msg.contains("older than latest anchor"), "msg was: {msg}");
            }
            other => panic!("expected Backend, was {other:?}"),
        }
    }

    #[rstest]
    fn validate_accepts_equal_or_newer_anchor() {
        let latest = SnapshotAnchor::new(9, "latest", "hash-latest", SnapshotCoverage::Partial);
        let same = SnapshotAnchor::new(9, "same", "hash-same", SnapshotCoverage::Partial);
        let newer = SnapshotAnchor::new(10, "newer", "hash-newer", SnapshotCoverage::Partial);

        validate_new_anchor(&same, 10, Some(&latest)).expect("same hwm accepted");
        validate_new_anchor(&newer, 10, Some(&latest)).expect("newer hwm accepted");
    }
}
