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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use ahash::AHashSet;
use futures::channel::oneshot;
use nautilus_common::messages::{
    DataEvent,
    book::{BookFeed, BookFeedAction, BookFeedBudget, L2BookUpdate},
    data::DataResponse,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{BinaryOption, InstrumentAny},
};

use crate::{KalshiWebSocketEvent, book::BookDefinitions};

/// Publishes typed inputs to the engine without retaining any mutable book levels.
#[derive(Debug)]
pub(crate) struct Publication {
    books: BookDefinitions,
    subscriptions: AHashSet<InstrumentId>,
    queued_snapshots: AHashSet<InstrumentId>,
    feed: Option<Arc<BookFeed>>,
    failure: Option<oneshot::Receiver<()>>,
    budget: Arc<BookFeedBudget>,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ready: Arc<AtomicBool>,
    definitions: tokio::sync::watch::Sender<Vec<BinaryOption>>,
}

impl Publication {
    pub(crate) fn new(
        instruments: &[BinaryOption],
        sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
        ready: Arc<AtomicBool>,
        budget: Arc<BookFeedBudget>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            books: BookDefinitions::new(instruments)?,
            subscriptions: AHashSet::new(),
            queued_snapshots: AHashSet::new(),
            feed: None,
            failure: None,
            budget,
            sender,
            ready,
            definitions: tokio::sync::watch::Sender::new(instruments.to_vec()),
        })
    }

    /// Bootstrap delivery is distinct from engine readiness, which only the engine confirms.
    pub(crate) fn is_ready(&self) -> bool {
        !self.engine_failed() && self.queued_snapshots.len() == self.definitions.borrow().len()
    }

    pub(crate) fn engine_failed(&self) -> bool {
        self.feed
            .as_ref()
            .is_some_and(|feed| !feed.lock().is_active())
    }

    pub(crate) fn definitions(&self) -> tokio::sync::watch::Receiver<Vec<BinaryOption>> {
        self.definitions.subscribe()
    }

    pub(crate) fn publish_instruments(&self, instruments: &[BinaryOption]) -> anyhow::Result<()> {
        for instrument in instruments {
            self.sender
                .send(DataEvent::Instrument(InstrumentAny::BinaryOption(
                    instrument.clone(),
                )))
                .map_err(|_| anyhow::anyhow!("NT data receiver closed"))?;
        }
        Ok(())
    }

    pub(crate) fn refresh(
        &mut self,
        updates: &[BinaryOption],
        ts_init: UnixNanos,
    ) -> anyhow::Result<bool> {
        let mut definitions = self.definitions.borrow().clone();
        let mut seen = AHashSet::new();
        for update in updates {
            anyhow::ensure!(seen.insert(update.id), "Duplicate Kalshi instrument update");
            let definition = definitions
                .iter_mut()
                .find(|definition| definition.id == update.id)
                .ok_or_else(|| anyhow::anyhow!("Unselected Kalshi instrument update"))?;
            *definition = update.clone();
        }
        let books = BookDefinitions::new(&definitions)?;
        let changed = !self.books.same_definitions(&books);
        if changed {
            self.invalidate(ts_init)?;
            self.books = books;
        }
        self.definitions.send_replace(definitions);
        self.publish_instruments(updates)?;
        Ok(changed)
    }

    pub(crate) fn respond(&self, response: DataResponse) -> anyhow::Result<()> {
        self.sender
            .send(DataEvent::Response(response))
            .map_err(|_| anyhow::anyhow!("NT data receiver closed"))
    }

    pub(crate) fn receiver_closed(&self) -> impl Future<Output = ()> + use<> {
        let sender = self.sender.clone();
        async move { sender.closed().await }
    }

    pub(crate) async fn failed(&mut self) {
        match self.failure.as_mut() {
            Some(failure) => {
                let _ = failure.await;
            }
            None => futures::future::pending().await,
        }
    }

    fn begin_feed(&mut self, ts_init: UnixNanos) -> anyhow::Result<()> {
        if self.feed.is_some() {
            return Ok(());
        }
        let ids = self
            .definitions
            .borrow()
            .iter()
            .map(|instrument| instrument.id)
            .collect();
        let (feed, failure) =
            BookFeed::new(ids, Arc::clone(&self.budget), Arc::clone(&self.ready))?;
        self.failure = Some(failure);
        self.feed = Some(feed);

        for id in &self.subscriptions {
            self.emit(BookFeedAction::Subscribe(*id), ts_init)?;
        }
        Ok(())
    }

    fn emit(&self, action: BookFeedAction, ts_init: UnixNanos) -> anyhow::Result<()> {
        let event = self
            .feed
            .as_ref()
            .expect("feed started")
            .event(action, ts_init)?;
        self.sender
            .send(DataEvent::BookFeed(event))
            .map_err(|_| anyhow::anyhow!("NT data receiver closed"))
    }

    pub(crate) fn subscribe(&mut self, id: InstrumentId, ts_init: UnixNanos) -> anyhow::Result<()> {
        if self.subscriptions.insert(id) && self.feed.is_some() {
            self.emit(BookFeedAction::Subscribe(id), ts_init)?;
        }
        Ok(())
    }

    pub(crate) fn unsubscribe(
        &mut self,
        id: InstrumentId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        if self.subscriptions.remove(&id) && self.feed.is_some() {
            self.emit(BookFeedAction::Unsubscribe(id), ts_init)?;
        }
        Ok(())
    }

    pub(crate) fn invalidate(&mut self, ts_init: UnixNanos) -> anyhow::Result<()> {
        self.ready.store(false, Ordering::Release);
        self.queued_snapshots.clear();
        self.failure.take();
        if let Some(feed) = self.feed.take() {
            feed.lock().invalidate();
            self.sender
                .send(DataEvent::BookFeed(
                    feed.event(BookFeedAction::Close, ts_init)?,
                ))
                .map_err(|_| anyhow::anyhow!("NT data receiver closed"))?;
        }
        Ok(())
    }

    pub(crate) fn handle(
        &mut self,
        event: KalshiWebSocketEvent,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        match event {
            KalshiWebSocketEvent::Subscribed { .. } | KalshiWebSocketEvent::Disconnected { .. } => {
                self.invalidate(ts_init)
            }
            KalshiWebSocketEvent::Book { message, .. } => {
                let action = self.books.prepare(&message, ts_init)?;
                self.begin_feed(ts_init)?;

                if let BookFeedAction::Update {
                    instrument_id,
                    update: L2BookUpdate::Snapshot { .. },
                    ..
                } = &action
                {
                    self.queued_snapshots.insert(*instrument_id);
                }
                self.emit(action, ts_init)
            }
        }
    }
}
