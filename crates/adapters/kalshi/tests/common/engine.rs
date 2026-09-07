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

//! Drives production engine ingestion on the test thread, mirroring the live runner.

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clock::TestClock,
    messages::DataEvent,
    msgbus::{self, MessageBus, TypedHandler, switchboard},
};
use nautilus_core::UUID4;
use nautilus_data::engine::DataEngine;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    identifiers::{InstrumentId, TraderId},
    instruments::Instrument,
};

pub(super) struct EngineReceiver {
    source: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    output: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    engine: DataEngine,
    observed: HashSet<InstrumentId>,
}

impl EngineReceiver {
    pub(super) fn new(
        source: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
        cache: Rc<RefCell<Cache>>,
    ) -> Self {
        MessageBus::new(TraderId::new("TEST-001"), UUID4::new(), None, None).register_message_bus();
        let engine = DataEngine::new(Rc::new(RefCell::new(TestClock::new())), cache, None);
        let (sender, output) = tokio::sync::mpsc::unbounded_channel();
        Self {
            source,
            output,
            sender,
            engine,
            observed: HashSet::new(),
        }
    }

    fn process(&mut self, event: DataEvent) {
        match event {
            DataEvent::BookFeed(event) => self.engine.process(&event),
            DataEvent::Instrument(instrument) => {
                if self.observed.insert(instrument.id()) {
                    let sender = self.sender.clone();
                    let handler = TypedHandler::from(move |deltas: &OrderBookDeltas| {
                        sender
                            .send(DataEvent::Data(Data::Deltas(deltas.clone().into())))
                            .unwrap();
                    });
                    msgbus::subscribe_book_deltas(
                        switchboard::get_book_deltas_topic(instrument.id()).into(),
                        handler,
                        None,
                    );
                }
                self.engine.process(&instrument);
                self.sender.send(DataEvent::Instrument(instrument)).unwrap();
            }
            other => {
                self.sender.send(other).unwrap();
            }
        }
    }

    pub(super) fn flush(&mut self) {
        while let Ok(event) = self.source.try_recv() {
            self.process(event);
        }
    }

    pub(super) fn try_recv(&mut self) -> Result<DataEvent, tokio::sync::mpsc::error::TryRecvError> {
        loop {
            if let Ok(event) = self.output.try_recv() {
                return Ok(event);
            }
            let event = self.source.try_recv()?;
            self.process(event);
        }
    }

    pub(super) async fn recv(&mut self) -> Option<DataEvent> {
        loop {
            if let Ok(event) = self.output.try_recv() {
                return Some(event);
            }
            let event = self.source.recv().await?;
            self.process(event);
        }
    }
}
