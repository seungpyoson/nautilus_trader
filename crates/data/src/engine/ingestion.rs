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

use std::sync::Arc;

use ahash::AHashSet;
use nautilus_common::{
    messages::book::{BookFeed, BookFeedAction, BookFeedEvent, L2BookUpdate},
    msgbus::{self, TypedHandler, switchboard},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    orderbook::{OrderBook, analysis::book_check_integrity},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use super::DataEngine;

/// Feed lifetime and snapshot readiness; all price levels live only in the cache.
#[derive(Debug)]
pub(super) struct IngestedBooks {
    feed: Arc<BookFeed>,
    snapshots: AHashSet<InstrumentId>,
    subscribers: AHashSet<InstrumentId>,
}

impl DataEngine {
    pub(super) fn process_book_feed(&mut self, event: &BookFeedEvent) {
        let mut state = event.feed.lock();
        if matches!(event.action, BookFeedAction::Close) {
            state.invalidate();
            self.close_book_feed(event.feed.id, event.ts_init);
            return;
        }

        if !state.is_active() {
            self.close_book_feed(event.feed.id, event.ts_init);
            return;
        }

        if let Err(e) = self.apply_book_feed(event) {
            state.invalidate();
            self.close_book_feed(event.feed.id, event.ts_init);
            log::warn!("Engine book feed invalidated: {e}");
            return;
        }
        let books = &self.book_feeds[&event.feed.id];
        state.set_ready(books.snapshots.len() == books.feed.instruments.len());
    }

    fn open_book_feed(&mut self, feed: &Arc<BookFeed>) -> anyhow::Result<()> {
        if self.book_feeds.contains_key(&feed.id) {
            return Ok(());
        }
        {
            let cache = self.cache.borrow();

            for id in &feed.instruments {
                anyhow::ensure!(
                    cache.instrument(id).is_some(),
                    "Book feed instrument is missing"
                );
                anyhow::ensure!(
                    !self.book_feed_owners.contains_key(id),
                    "Book already belongs to another feed"
                );
                anyhow::ensure!(
                    cache
                        .order_book(id)
                        .is_none_or(|book| book.book_type == BookType::L2_MBP),
                    "Book feed requires L2_MBP"
                );
            }
        }
        self.book_feeds.insert(
            feed.id,
            IngestedBooks {
                feed: Arc::clone(feed),
                snapshots: AHashSet::new(),
                subscribers: AHashSet::new(),
            },
        );

        for id in &feed.instruments {
            self.cache
                .borrow_mut()
                .add_order_book(OrderBook::new(*id, BookType::L2_MBP))?;
            // An ingested feed applies on the engine thread before publication. Ordinary
            // bus updaters must not apply those published absolute deltas a second time.
            if let Some(updater) = self.book_updaters.get(id) {
                let handler = TypedHandler::<OrderBookDeltas>::new(updater.clone());
                msgbus::unsubscribe_book_deltas(
                    switchboard::get_book_deltas_topic(*id).into(),
                    &handler,
                );
            }
            self.book_feed_owners.insert(*id, feed.id);
        }
        Ok(())
    }

    fn apply_book_feed(&mut self, event: &BookFeedEvent) -> anyhow::Result<()> {
        self.open_book_feed(&event.feed)?;
        let id = match &event.action {
            BookFeedAction::Update { instrument_id, .. }
            | BookFeedAction::Subscribe(instrument_id)
            | BookFeedAction::Unsubscribe(instrument_id) => *instrument_id,
            BookFeedAction::Close => unreachable!("close handled before application"),
        };
        anyhow::ensure!(
            self.book_feed_owners.get(&id) == Some(&event.feed.id),
            "Instrument is outside the book feed selection"
        );

        match &event.action {
            BookFeedAction::Subscribe(_) => {
                let books = self
                    .book_feeds
                    .get_mut(&event.feed.id)
                    .expect("feed opened");

                if books.subscribers.insert(id) && books.snapshots.contains(&id) {
                    let snapshot = {
                        let cache = self.cache.borrow();
                        let book = cache.order_book(&id).expect("feed owns cache book");
                        book.to_deltas(book.ts_last, event.ts_init)
                    };
                    // Replay is publication only; the authoritative book is already current
                    msgbus::publish_deltas(switchboard::get_book_deltas_topic(id), &snapshot);
                }
            }
            BookFeedAction::Unsubscribe(_) => {
                self.book_feeds
                    .get_mut(&event.feed.id)
                    .expect("feed opened")
                    .subscribers
                    .remove(&id);
            }
            BookFeedAction::Update {
                update,
                sequence,
                ts_event,
                ..
            } => {
                let is_snapshot = matches!(update, L2BookUpdate::Snapshot { .. });
                anyhow::ensure!(
                    is_snapshot || self.book_feeds[&event.feed.id].snapshots.contains(&id),
                    "Signed book change requires a snapshot"
                );
                let deltas = {
                    let mut cache = self.cache.borrow_mut();
                    let instrument = cache
                        .instrument(&id)
                        .expect("feed instrument exists")
                        .clone();
                    let book = cache.order_book_mut(&id).expect("feed owns cache book");
                    let deltas = normalize_update(
                        &instrument,
                        book,
                        update,
                        *sequence,
                        *ts_event,
                        event.ts_init,
                    )?;
                    book.apply_deltas(&deltas)?;
                    book_check_integrity(book)?;
                    deltas
                };
                let books = self
                    .book_feeds
                    .get_mut(&event.feed.id)
                    .expect("feed opened");
                books.snapshots.insert(id);

                if self.config.emit_quotes_from_book {
                    let quote = self
                        .cache
                        .borrow()
                        .order_book(&id)
                        .and_then(super::book::derive_quote_from_book);
                    if let Some(quote) = quote {
                        super::book::publish_quote_if_changed(&self.cache, quote);
                    }
                }

                if books.subscribers.contains(&id) {
                    msgbus::publish_deltas(switchboard::get_book_deltas_topic(id), &deltas);
                }
            }
            BookFeedAction::Close => unreachable!("close handled before application"),
        }
        Ok(())
    }

    fn close_book_feed(&mut self, feed_id: UUID4, ts_init: UnixNanos) {
        let Some(books) = self.book_feeds.remove(&feed_id) else {
            return;
        };

        for id in &books.feed.instruments {
            if self.book_feed_owners.get(id) != Some(&feed_id) {
                continue;
            }
            let mut clear = OrderBookDelta::clear(*id, 0, ts_init, ts_init);
            clear.flags = RecordFlag::F_LAST as u8;
            let deltas = OrderBookDeltas::new(*id, vec![clear]);
            if let Some(book) = self.cache.borrow_mut().order_book_mut(id)
                && let Err(e) = book.apply_deltas(&deltas)
            {
                log::error!("Failed to clear engine book: {e}");
            }

            if books.snapshots.contains(id) && books.subscribers.contains(id) {
                msgbus::publish_deltas(switchboard::get_book_deltas_topic(*id), &deltas);
            }
            self.book_feed_owners.remove(id);
            if self.is_underlying_wanted_for_deltas(id)
                && let Err(e) = self.setup_book_updater(id, BookType::L2_MBP, true, None)
            {
                log::error!("Failed to restore engine book updater: {e}");
            }
        }
    }

    pub(super) fn close_book_feeds(&mut self) {
        let feeds: Vec<_> = self
            .book_feeds
            .values()
            .map(|books| Arc::clone(&books.feed))
            .collect();
        let now = self.clock.borrow().timestamp_ns();

        for feed in feeds {
            feed.lock().invalidate();
            self.close_book_feed(feed.id, now);
        }
    }
}

fn price(instrument: &InstrumentAny, value: Decimal) -> anyhow::Result<Price> {
    let price = Price::from_decimal_dp(value, instrument.price_precision())?;
    anyhow::ensure!(price.as_decimal() == value, "Inexact book price");
    instrument.try_normalize_price(price).map_err(Into::into)
}

fn quantity(instrument: &InstrumentAny, value: Decimal) -> anyhow::Result<Quantity> {
    anyhow::ensure!(
        value >= Decimal::ZERO && value % instrument.size_increment().as_decimal() == Decimal::ZERO,
        "Book quantity is negative or outside the instrument increment"
    );
    let quantity = Quantity::from_decimal_dp(value, instrument.size_precision())?;
    anyhow::ensure!(quantity.as_decimal() == value, "Inexact book quantity");
    Ok(quantity)
}

fn normalize_update(
    instrument: &InstrumentAny,
    book: &OrderBook,
    update: &L2BookUpdate,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let id = instrument.id();
    let deltas = match update {
        L2BookUpdate::Snapshot { bids, asks } => {
            let mut deltas = vec![OrderBookDelta::clear(id, sequence, ts_event, ts_init)];

            for (side, levels) in [(OrderSide::Buy, bids), (OrderSide::Sell, asks)] {
                let mut prices = AHashSet::new();

                for (value, size) in levels {
                    let price = price(instrument, *value)?;
                    let size = quantity(instrument, *size)?;
                    anyhow::ensure!(prices.insert(price), "Duplicate snapshot price level");

                    if !size.is_zero() {
                        deltas.push(OrderBookDelta::new(
                            id,
                            BookAction::Add,
                            BookOrder::new(side, price, size, 0),
                            RecordFlag::F_SNAPSHOT as u8,
                            sequence,
                            ts_event,
                            ts_init,
                        ));
                    }
                }
            }
            deltas.last_mut().expect("snapshot includes clear").flags |= RecordFlag::F_LAST as u8;
            deltas
        }
        L2BookUpdate::Change {
            side,
            price: value,
            quantity_change,
        } => {
            let price = price(instrument, *value)?;
            let previous = book.get_quantity_at_level(
                price,
                side.opposite().as_order_side(),
                instrument.size_precision(),
            );
            let absolute = previous
                .as_decimal()
                .checked_add(*quantity_change)
                .ok_or_else(|| anyhow::anyhow!("Signed book quantity overflow"))?;
            let size = quantity(instrument, absolute)?;
            let action = if size.is_zero() {
                BookAction::Delete
            } else if previous.is_zero() {
                BookAction::Add
            } else {
                BookAction::Update
            };
            vec![OrderBookDelta::new(
                id,
                action,
                BookOrder::new(side.as_order_side(), price, size, 0),
                RecordFlag::F_LAST as u8,
                sequence,
                ts_event,
                ts_init,
            )]
        }
    };
    Ok(OrderBookDeltas::new(id, deltas))
}
