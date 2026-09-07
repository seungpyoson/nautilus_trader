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

//! Synchronous test driver for the production adapter-to-engine message boundary.
//! No conversion or book mutation is emulated here.

use std::{
    cell::RefCell,
    num::NonZeroUsize,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_common::{
    cache::Cache,
    clock::TestClock,
    messages::{DataEvent, book::BookFeedBudget},
    msgbus::{self, MessageBus, TypedHandler, switchboard},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_data::engine::DataEngine;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    identifiers::{InstrumentId, TraderId},
    instruments::{BinaryOption, InstrumentAny},
};

use crate::{KalshiOrderbookMessage, KalshiWebSocketEvent};

pub(super) struct Publication {
    pub(super) inner: crate::publication::Publication,
    pub(super) engine: DataEngine,
    pub(super) cache: Rc<RefCell<Cache>>,
    pub(super) receiver: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ready: Arc<AtomicBool>,
}

impl Publication {
    pub(super) fn new(
        instruments: &[BinaryOption],
        sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
        ready: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let (input, receiver) = tokio::sync::mpsc::unbounded_channel();
        let inner = crate::publication::Publication::new(
            instruments,
            input,
            Arc::clone(&ready),
            BookFeedBudget::new(NonZeroUsize::new(64 + 2 * instruments.len()).unwrap()),
        )?;
        MessageBus::new(TraderId::new("TEST-001"), UUID4::new(), None, None).register_message_bus();
        let cache = Rc::new(RefCell::new(Cache::default()));
        for instrument in instruments {
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument.clone()))?;
            let output = sender.clone();
            let handler = TypedHandler::from(move |deltas: &OrderBookDeltas| {
                output
                    .send(DataEvent::Data(Data::Deltas(deltas.clone().into())))
                    .unwrap();
            });
            msgbus::subscribe_book_deltas(
                switchboard::get_book_deltas_topic(instrument.id).into(),
                handler,
                None,
            );
        }
        let engine = DataEngine::new(
            Rc::new(RefCell::new(TestClock::new())),
            Rc::clone(&cache),
            None,
        );
        Ok(Self {
            inner,
            engine,
            cache,
            receiver,
            sender,
            ready,
        })
    }

    pub(super) fn pump(&mut self) {
        while let Ok(event) = self.receiver.try_recv() {
            match event {
                DataEvent::BookFeed(event) => self.engine.process(&event),
                DataEvent::Instrument(instrument) => {
                    self.engine.process(&instrument);
                    self.sender.send(DataEvent::Instrument(instrument)).unwrap();
                }
                other => {
                    self.sender.send(other).unwrap();
                }
            }
        }
    }

    pub(super) fn handle(
        &mut self,
        event: KalshiWebSocketEvent,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        let result = self.inner.handle(event, ts_init);
        self.pump();
        let failed = result.is_err() || self.inner.engine_failed();
        if failed {
            self.invalidate(ts_init)?;
            anyhow::bail!("Engine rejected book update");
        }
        Ok(())
    }

    pub(super) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(super) fn subscribe(&mut self, id: InstrumentId, ts_init: UnixNanos) -> anyhow::Result<()> {
        self.inner.subscribe(id, ts_init)?;
        self.pump();
        Ok(())
    }

    pub(super) fn unsubscribe(&mut self, id: InstrumentId) {
        self.inner.unsubscribe(id, 0.into()).unwrap();
        self.pump();
    }

    pub(super) fn invalidate(&mut self, ts_init: UnixNanos) -> anyhow::Result<()> {
        self.inner.invalidate(ts_init)?;
        self.pump();
        Ok(())
    }

    pub(super) fn definitions(&self) -> tokio::sync::watch::Receiver<Vec<BinaryOption>> {
        self.inner.definitions()
    }

    pub(super) fn refresh(
        &mut self,
        instruments: &[BinaryOption],
        ts_init: UnixNanos,
    ) -> anyhow::Result<bool> {
        let changed = self.inner.refresh(instruments, ts_init)?;
        self.pump();
        Ok(changed)
    }
}

/// Runs the existing conversion cases against the actual engine-owned cache.
pub(super) struct Books {
    pub(super) publication: Publication,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
}

impl Books {
    pub(super) fn new(instruments: &[BinaryOption]) -> anyhow::Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut publication =
            Publication::new(instruments, sender, Arc::new(AtomicBool::new(false)))?;
        for instrument in instruments {
            publication.subscribe(instrument.id, 0.into())?;
        }
        Ok(Self {
            publication,
            receiver,
        })
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "unexpected output must fail the test rather than count as a validated book rejection"
    )]
    pub(super) fn apply(
        &mut self,
        message: &KalshiOrderbookMessage,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderBookDeltas> {
        self.publication.handle(
            KalshiWebSocketEvent::Book {
                connection_epoch: 0,
                message: message.clone(),
            },
            ts_init,
        )?;

        match self.receiver.try_recv()? {
            DataEvent::Data(Data::Deltas(deltas)) => Ok((*deltas).clone()),
            other => panic!("Unexpected publication: {other:?}"),
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        self.publication.is_ready()
    }

    pub(super) fn snapshot(&self, id: InstrumentId, ts_init: UnixNanos) -> Option<OrderBookDeltas> {
        if !self.is_ready() {
            return None;
        }
        let cache = self.publication.cache.borrow();
        let book = cache.order_book(&id)?;
        Some(book.to_deltas(book.ts_last, ts_init))
    }
}
