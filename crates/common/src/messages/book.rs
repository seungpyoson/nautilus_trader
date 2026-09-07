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

//! Engine-owned L2 ingestion for feeds which publish signed quantity changes.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures::channel::oneshot;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{enums::OrderSideSpecified, identifiers::InstrumentId};
use rust_decimal::Decimal;

/// A complete L2 snapshot or a signed change to one price level.
#[derive(Debug)]
pub enum L2BookUpdate {
    Snapshot {
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    },
    Change {
        side: OrderSideSpecified,
        price: Decimal,
        quantity_change: Decimal,
    },
}

/// Operations on one fixed-selection feed generation.
#[derive(Debug)]
pub enum BookFeedAction {
    Update {
        instrument_id: InstrumentId,
        update: L2BookUpdate,
        sequence: u64,
        ts_event: UnixNanos,
    },
    Subscribe(InstrumentId),
    Unsubscribe(InstrumentId),
    Close,
}

/// Shared generation fence, containing no book levels.
///
/// The engine holds the fence while applying and publishing an operation. Invalidating
/// the generation waits for that operation and prevents any later queued operation.
#[derive(Debug)]
pub struct BookFeed {
    pub id: UUID4,
    pub instruments: Vec<InstrumentId>,
    budget: Arc<BookFeedBudget>,
    state: Mutex<BookFeedState>,
}

/// Locked lifecycle state for a feed generation.
#[derive(Debug)]
pub struct BookFeedState {
    active: bool,
    close_sent: bool,
    ready: Arc<AtomicBool>,
    failure: Option<oneshot::Sender<()>>,
}

/// Bounds retained inputs across every connection generation of one client.
#[derive(Debug)]
pub struct BookFeedBudget {
    limit: NonZeroUsize,
    pending: AtomicUsize,
}

impl BookFeedBudget {
    /// Creates a budget shared by every generation of one data client.
    #[must_use]
    pub fn new(limit: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            pending: AtomicUsize::new(0),
        })
    }
}

impl BookFeedState {
    /// Returns whether this generation can still accept inputs.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Publishes engine readiness while the generation remains active.
    pub fn set_ready(&self, ready: bool) {
        if self.active {
            self.ready.store(ready, Ordering::Release);
        }
    }

    /// Fences pending inputs and notifies the producer once.
    pub fn invalidate(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        self.ready.store(false, Ordering::Release);

        if let Some(sender) = self.failure.take() {
            let _ = sender.send(());
        }
    }
}

impl BookFeed {
    /// Creates a fixed-selection generation without retaining any book levels.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate selection, or a full shared input budget.
    pub fn new(
        instruments: Vec<InstrumentId>,
        budget: Arc<BookFeedBudget>,
        ready: Arc<AtomicBool>,
    ) -> anyhow::Result<(Arc<Self>, oneshot::Receiver<()>)> {
        anyhow::ensure!(!instruments.is_empty(), "Book feed selection is empty");
        anyhow::ensure!(
            budget.pending.load(Ordering::Acquire) < budget.limit.get(),
            "Book feed engine backlog exceeded"
        );
        let unique: std::collections::HashSet<_> = instruments.iter().collect();
        anyhow::ensure!(
            unique.len() == instruments.len(),
            "Duplicate book feed instrument"
        );
        let (sender, receiver) = oneshot::channel();
        Ok((
            Arc::new(Self {
                id: UUID4::new(),
                instruments,
                budget,
                state: Mutex::new(BookFeedState {
                    active: true,
                    close_sent: false,
                    ready,
                    failure: Some(sender),
                }),
            }),
            receiver,
        ))
    }

    /// Locks the generation across engine application and publication.
    ///
    /// # Panics
    ///
    /// Panics if a previous holder panicked while holding the lifecycle lock.
    pub fn lock(&self) -> MutexGuard<'_, BookFeedState> {
        self.state
            .lock()
            .expect("book feed lifecycle lock poisoned")
    }

    /// Enqueues a bounded operation. A close remains available after failure or overflow.
    ///
    /// # Errors
    ///
    /// Returns an error for a retired generation, an exceeded input budget, or a repeated close.
    pub fn event(
        self: &Arc<Self>,
        action: BookFeedAction,
        ts_init: UnixNanos,
    ) -> anyhow::Result<BookFeedEvent> {
        let counted = !matches!(action, BookFeedAction::Close);
        if counted {
            let mut state = self.lock();
            anyhow::ensure!(state.active, "Book feed generation is invalid");

            if self
                .budget
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    if pending < self.budget.limit.get() {
                        Some(pending + 1)
                    } else {
                        None
                    }
                })
                .is_err()
            {
                state.invalidate();
                anyhow::bail!("Book feed engine backlog exceeded");
            }
        } else {
            let mut state = self.lock();
            anyhow::ensure!(!state.close_sent, "Book feed close already queued");
            state.close_sent = true;
        }
        Ok(BookFeedEvent {
            feed: Arc::clone(self),
            action,
            ts_init,
            counted,
        })
    }
}

/// An internal engine command with a generation fence and a retained-operation budget.
#[derive(Debug)]
pub struct BookFeedEvent {
    pub feed: Arc<BookFeed>,
    pub action: BookFeedAction,
    pub ts_init: UnixNanos,
    counted: bool,
}

impl Drop for BookFeedEvent {
    fn drop(&mut self) {
        if self.counted {
            self.feed.budget.pending.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn backlog_budget_survives_generation_replacement_and_releases_on_drop() {
        let id = InstrumentId::from("TEST.KALSHI");
        let budget = BookFeedBudget::new(NonZeroUsize::new(2).unwrap());
        let ready = Arc::new(AtomicBool::new(false));
        let (first, _) = BookFeed::new(vec![id], Arc::clone(&budget), Arc::clone(&ready)).unwrap();
        let pending_first = first
            .event(BookFeedAction::Subscribe(id), 0.into())
            .unwrap();
        first.lock().invalidate();
        let (second, mut failure) =
            BookFeed::new(vec![id], Arc::clone(&budget), Arc::clone(&ready)).unwrap();
        let pending_second = second
            .event(BookFeedAction::Subscribe(id), 0.into())
            .unwrap();
        assert!(
            second
                .event(BookFeedAction::Subscribe(id), 0.into())
                .is_err()
        );
        assert_eq!(failure.try_recv().unwrap(), Some(()));
        assert!(!second.lock().is_active());
        assert!(BookFeed::new(vec![id], Arc::clone(&budget), Arc::clone(&ready)).is_err());
        let close = second.event(BookFeedAction::Close, 0.into()).unwrap();
        assert!(second.event(BookFeedAction::Close, 0.into()).is_err());
        drop(pending_first);
        let (third, _) = BookFeed::new(vec![id], Arc::clone(&budget), ready).unwrap();
        let pending_third = third
            .event(BookFeedAction::Subscribe(id), 0.into())
            .unwrap();
        drop((pending_second, pending_third, close));
        assert_eq!(budget.pending.load(Ordering::Acquire), 0);
    }

    #[rstest]
    fn retired_generation_cannot_change_replacement_readiness() {
        let id = InstrumentId::from("TEST.KALSHI");
        let budget = BookFeedBudget::new(NonZeroUsize::new(2).unwrap());
        let ready = Arc::new(AtomicBool::new(false));
        let (first, _) = BookFeed::new(vec![id], Arc::clone(&budget), Arc::clone(&ready)).unwrap();
        first.lock().invalidate();
        let (second, _) = BookFeed::new(vec![id], budget, Arc::clone(&ready)).unwrap();
        second.lock().set_ready(true);
        first.lock().invalidate();
        assert!(ready.load(Ordering::Acquire));
        first.lock().set_ready(false);
        assert!(ready.load(Ordering::Acquire));
        assert!(
            first
                .event(BookFeedAction::Subscribe(id), 0.into())
                .is_err()
        );
    }
}
