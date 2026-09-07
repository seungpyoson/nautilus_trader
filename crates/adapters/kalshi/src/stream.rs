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

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
};

use thiserror::Error;
use uuid::Uuid;

use crate::{
    common::validate_ticker,
    orderbook::{
        KalshiOrderbookMessage, StreamMessage, SubscriptionMarkets, decode_stream_message,
    },
};

/// Protocol synchronization state, independent of book validity or live readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KalshiStreamState {
    /// At least one selected market still requires its initial snapshot.
    AwaitingSnapshot,
    /// Every selected market has a snapshot and subscription continuity holds.
    Streaming,
    /// A protocol failure or disconnect requires a new transport subscription.
    Invalidated,
    /// The subscription was stopped and cannot accept further frames.
    Stopped,
}

/// Failures which prevent accepting a frame into the orderbook stream.
#[derive(Debug, Error)]
pub enum KalshiStreamError {
    /// The selected market identities are empty, invalid or ambiguous.
    #[error("Invalid Kalshi stream configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// The frame exceeds the configured byte limit.
    #[error("Kalshi frame length {length} exceeds limit {limit}")]
    FrameTooLarge {
        /// The received frame length in bytes.
        length: usize,
        /// The configured maximum frame length in bytes.
        limit: usize,
    },
    /// The frame does not satisfy the supported wire contract.
    #[error("Invalid Kalshi frame: {0}")]
    Decode(#[from] serde_json::Error),
    /// The frame is not scoped to this subscription.
    #[error("Kalshi frame has a missing or mismatched subscription ID")]
    SubscriptionMismatch,
    /// A subscription control response lacks its sequence number.
    #[error("Kalshi subscription response has no sequence number")]
    MissingSequence,
    /// The next message is missing, duplicated or out of order.
    #[error("Kalshi sequence mismatch: expected {expected}, received {received}")]
    SequenceMismatch {
        /// The next required subscription sequence number.
        expected: u64,
        /// The received subscription sequence number.
        received: u64,
    },
    /// The previous sequence number has no representable successor.
    #[error("Kalshi subscription sequence is exhausted")]
    SequenceExhausted,
    /// The frame's ticker and UUID do not match a selected market.
    #[error("Kalshi frame does not match a selected market identity")]
    MarketMismatch,
    /// This market has not received its initial snapshot.
    #[error("Kalshi delta arrived before its market snapshot")]
    DeltaBeforeSnapshot,
    /// The venue reports different membership for the fixed subscription.
    #[error("Kalshi subscription market membership changed")]
    MembershipChanged,
    /// A venue error invalidates the subscription, including unknown error codes.
    #[error("Kalshi subscription failed with venue error code {code}")]
    Venue {
        /// The venue error code, without retaining the response text.
        code: i64,
    },
    /// A stopped or invalidated stream cannot accept another frame.
    #[error("Kalshi stream is inactive: {0:?}")]
    Inactive(KalshiStreamState),
}

#[derive(Debug)]
struct ActiveStream {
    markets: BTreeMap<String, Option<Uuid>>,
    last_sequence: Option<NonZeroU64>,
}

impl ActiveStream {
    fn advance_sequence(&mut self, sequence: NonZeroU64) -> Result<(), KalshiStreamError> {
        if let Some(last) = self.last_sequence {
            let expected = last
                .get()
                .checked_add(1)
                .ok_or(KalshiStreamError::SequenceExhausted)?;

            if sequence.get() != expected {
                return Err(KalshiStreamError::SequenceMismatch {
                    expected,
                    received: sequence.get(),
                });
            }
        }

        self.last_sequence = Some(sequence);
        Ok(())
    }

    fn check_membership(&self, reported: SubscriptionMarkets) -> Result<(), KalshiStreamError> {
        if let Some(tickers) = reported.market_tickers {
            let actual: BTreeSet<_> = tickers.iter().collect();
            let expected: BTreeSet<_> = self.markets.keys().collect();

            if tickers.len() != expected.len() || actual != expected {
                return Err(KalshiStreamError::MembershipChanged);
            }
        }

        if let Some(ids) = reported.market_ids {
            let actual: BTreeSet<_> = ids.iter().copied().collect();
            let expected: BTreeSet<_> = self.markets.values().filter_map(|id| *id).collect();

            if expected.len() != self.markets.len()
                || ids.len() != expected.len()
                || actual != expected
            {
                return Err(KalshiStreamError::MembershipChanged);
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum StreamStatus {
    Active(ActiveStream),
    Invalidated,
    Stopped,
}

pub(crate) fn validate_market_selection(markets: &[String]) -> Result<(), KalshiStreamError> {
    if markets.is_empty() {
        return Err(KalshiStreamError::InvalidConfiguration(
            "no selected markets",
        ));
    }
    let mut seen = BTreeSet::new();

    for ticker in markets {
        validate_ticker(ticker)
            .map_err(|_| KalshiStreamError::InvalidConfiguration("invalid market identity"))?;

        if !seen.insert(ticker) {
            return Err(KalshiStreamError::InvalidConfiguration(
                "duplicate market ticker",
            ));
        }
    }
    Ok(())
}

/// Validates one fixed orderbook subscription within one transport connection.
///
/// The caller supplies metadata-derived tickers and a confirmed subscription ID
/// on an authenticated connection. A market's first snapshot pins its UUID, which
/// REST market metadata does not supply. The UUID must remain stable and unique
/// within this subscription. The caller routes every frame here, including
/// sequenced control responses. Connection acknowledgements and subscription
/// intent remain the transport handler's responsibility through `SubscriptionState`.
///
/// This type retains sequence and snapshot progress only, never orderbook levels.
/// Accepted messages still require metadata precision/grid checks and Nautilus book
/// processing. `Streaming` therefore does not grant publication or live readiness.
///
/// Invalidation and stop are terminal. The transport handler must invalidate on
/// disconnect, request recovery through Nautilus transport lifecycle, and create a
/// new instance after a fresh subscription. Old connection frames must never be
/// routed to the replacement. Membership changes also require a new instance.
#[derive(Debug)]
pub struct KalshiOrderbookStream {
    subscription_id: NonZeroU64,
    max_frame_bytes: NonZeroUsize,
    status: StreamStatus,
}

impl KalshiOrderbookStream {
    /// Creates a stream awaiting snapshots for the supplied market identities.
    ///
    /// The initial sequence is established by the first sequenced frame, without
    /// assuming the venue starts at one or uses a separate counter for each market.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty selection, invalid tickers, or repeated tickers.
    pub fn new(
        subscription_id: NonZeroU64,
        markets: Vec<String>,
        max_frame_bytes: NonZeroUsize,
    ) -> Result<Self, KalshiStreamError> {
        validate_market_selection(&markets)?;
        let progress = markets.into_iter().map(|ticker| (ticker, None)).collect();

        Ok(Self {
            subscription_id,
            max_frame_bytes,
            status: StreamStatus::Active(ActiveStream {
                markets: progress,
                last_sequence: None,
            }),
        })
    }

    /// Returns the synchronization state across all selected markets.
    #[must_use]
    pub fn state(&self) -> KalshiStreamState {
        match &self.status {
            StreamStatus::Active(active) => {
                if active.markets.values().all(Option::is_some) {
                    KalshiStreamState::Streaming
                } else {
                    KalshiStreamState::AwaitingSnapshot
                }
            }
            StreamStatus::Invalidated => KalshiStreamState::Invalidated,
            StreamStatus::Stopped => KalshiStreamState::Stopped,
        }
    }

    /// Invalidates this subscription on disconnect or downstream processing failure.
    pub fn invalidate(&mut self) {
        if !matches!(self.status, StreamStatus::Stopped) {
            self.status = StreamStatus::Invalidated;
        }
    }

    /// Stops this stream and discards all sequence and snapshot progress.
    pub fn stop(&mut self) {
        self.status = StreamStatus::Stopped;
    }

    /// Checks frame size, decodes once, and validates subscription and market continuity.
    ///
    /// Returns a book message after validation, or `None` for a control response.
    /// A delta can pass only after its own market's snapshot, even while other
    /// selected markets still await theirs. Reduced `ok` responses can omit their
    /// membership lists, but must retain subscription and sequence identities here.
    /// Connection-level `ok` responses belong in the transport handler.
    ///
    /// # Errors
    ///
    /// Returns an error for inactive streams, oversized or invalid frames,
    /// mismatched identities, sequence failures, deltas before snapshots, changed
    /// membership or venue errors. Every failure invalidates an active stream;
    /// a later snapshot cannot revive it.
    pub fn handle_frame(
        &mut self,
        bytes: &[u8],
    ) -> Result<Option<KalshiOrderbookMessage>, KalshiStreamError> {
        if !matches!(self.status, StreamStatus::Active(_)) {
            return Err(KalshiStreamError::Inactive(self.state()));
        }

        let result = if bytes.len() > self.max_frame_bytes.get() {
            Err(KalshiStreamError::FrameTooLarge {
                length: bytes.len(),
                limit: self.max_frame_bytes.get(),
            })
        } else {
            decode_stream_message(bytes)
                .map_err(KalshiStreamError::from)
                .and_then(|message| self.handle_message(message))
        };

        if result.is_err() {
            self.invalidate();
        }

        result
    }

    pub(crate) fn handle_message(
        &mut self,
        message: StreamMessage,
    ) -> Result<Option<KalshiOrderbookMessage>, KalshiStreamError> {
        let (sid, sequence) = match &message {
            StreamMessage::Subscribed { .. } => {
                return Err(KalshiStreamError::SubscriptionMismatch);
            }
            StreamMessage::Book(
                KalshiOrderbookMessage::Snapshot { sid, seq, .. }
                | KalshiOrderbookMessage::Delta { sid, seq, .. },
            )
            | StreamMessage::Unsubscribed { sid, seq } => (Some(*sid), Some(*seq)),
            StreamMessage::Ok { sid, seq, .. } | StreamMessage::Error { sid, seq, .. } => {
                (*sid, *seq)
            }
        };

        // Unscoped connection errors also invalidate this subscription
        if let StreamMessage::Error { code, .. } = &message
            && sid.is_none()
        {
            return Err(KalshiStreamError::Venue { code: *code });
        }

        if sid != Some(self.subscription_id) {
            return Err(KalshiStreamError::SubscriptionMismatch);
        }

        let StreamStatus::Active(active) = &mut self.status else {
            return Err(KalshiStreamError::Inactive(self.state()));
        };

        if let StreamMessage::Error { code, .. } = message {
            return Err(KalshiStreamError::Venue { code });
        }

        active.advance_sequence(sequence.ok_or(KalshiStreamError::MissingSequence)?)?;

        match message {
            StreamMessage::Subscribed { .. } => Err(KalshiStreamError::SubscriptionMismatch),
            StreamMessage::Book(book) => {
                let (ticker, market_id, is_snapshot) = match &book {
                    KalshiOrderbookMessage::Snapshot { snapshot, .. } => {
                        (&snapshot.market_ticker, snapshot.market_id, true)
                    }
                    KalshiOrderbookMessage::Delta { delta, .. } => {
                        (&delta.market_ticker, delta.market_id, false)
                    }
                };
                let known_id = *active
                    .markets
                    .get(ticker)
                    .ok_or(KalshiStreamError::MarketMismatch)?;

                if let Some(known_id) = known_id {
                    if known_id != market_id {
                        return Err(KalshiStreamError::MarketMismatch);
                    }
                } else {
                    if !is_snapshot {
                        return Err(KalshiStreamError::DeltaBeforeSnapshot);
                    }

                    if active.markets.values().any(|id| *id == Some(market_id)) {
                        return Err(KalshiStreamError::MarketMismatch);
                    }

                    active.markets.insert(ticker.clone(), Some(market_id));
                }

                Ok(Some(book))
            }
            StreamMessage::Ok { markets, .. } => {
                if let Some(markets) = markets {
                    active.check_membership(markets)?;
                }

                Ok(None)
            }
            StreamMessage::Unsubscribed { .. } => {
                self.stop();
                Ok(None)
            }
            StreamMessage::Error { code, .. } => Err(KalshiStreamError::Venue { code }),
        }
    }
}
