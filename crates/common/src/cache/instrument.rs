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

use std::sync::{Arc, Weak};

use ahash::AHashMap;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{Instrument, InstrumentAny},
};
use parking_lot::RwLock;

type SymbolIndex = AHashMap<(Venue, Symbol), AHashMap<InstrumentId, Weak<InstrumentAny>>>;

/// A thread-safe read view of the cache's current instrument definitions.
///
/// This view does not retain definitions after the cache removes them. Reads return owned
/// snapshots; subsequent updates, purge, reload and reset are visible to subsequent reads.
#[derive(Clone, Debug, Default)]
pub struct InstrumentReadView {
    index: Arc<RwLock<SymbolIndex>>,
}

impl InstrumentReadView {
    /// Returns the uniquely identified instrument for a venue's raw symbol.
    ///
    /// Returns `None` if absent or ambiguous. A raw symbol need not equal the canonical
    /// instrument ID's symbol, and must not select arbitrarily between cached instruments.
    #[must_use]
    pub fn instrument(&self, venue: Venue, raw_symbol: Symbol) -> Option<InstrumentAny> {
        let index = self.index.read();
        let instruments = index.get(&(venue, raw_symbol))?;
        if instruments.len() != 1 {
            return None;
        }
        instruments.values().next()?.upgrade().map(|i| (*i).clone())
    }
}

/// Owns instrument definitions and maintains their read view in the same mutation.
#[derive(Debug, Default)]
pub(super) struct InstrumentStore {
    instruments: AHashMap<InstrumentId, Arc<InstrumentAny>>,
    view: InstrumentReadView,
}

impl InstrumentStore {
    pub(super) fn read_view(&self) -> InstrumentReadView {
        self.view.clone()
    }

    pub(super) fn get(&self, id: &InstrumentId) -> Option<&InstrumentAny> {
        self.instruments.get(id).map(Arc::as_ref)
    }

    pub(super) fn contains_key(&self, id: &InstrumentId) -> bool {
        self.instruments.contains_key(id)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &InstrumentId> {
        self.instruments.keys()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &InstrumentAny> {
        self.instruments.values().map(Arc::as_ref)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&InstrumentId, &InstrumentAny)> {
        self.instruments
            .iter()
            .map(|(id, instrument)| (id, instrument.as_ref()))
    }

    pub(super) fn insert(&mut self, id: InstrumentId, instrument: InstrumentAny) {
        let instrument = Arc::new(instrument);
        let mut index = self.view.index.write();
        if let Some(previous) = self.instruments.insert(id, Arc::clone(&instrument)) {
            remove_index_entry(&mut index, &previous);
        }
        index
            .entry((id.venue, instrument.raw_symbol()))
            .or_default()
            .insert(id, Arc::downgrade(&instrument));
    }

    pub(super) fn remove(&mut self, id: &InstrumentId) {
        let mut index = self.view.index.write();
        if let Some(instrument) = self.instruments.remove(id) {
            remove_index_entry(&mut index, &instrument);
        }
    }

    pub(super) fn clear(&mut self) {
        let mut index = self.view.index.write();
        index.clear();
        self.instruments.clear();
    }

    pub(super) fn replace(&mut self, instruments: AHashMap<InstrumentId, InstrumentAny>) {
        let mut index = self.view.index.write();
        index.clear();
        self.instruments = AHashMap::with_hasher(instruments.hasher().clone());
        for (id, instrument) in instruments {
            let instrument = Arc::new(instrument);
            index
                .entry((id.venue, instrument.raw_symbol()))
                .or_default()
                .insert(id, Arc::downgrade(&instrument));
            self.instruments.insert(id, instrument);
        }
    }
}

impl From<AHashMap<InstrumentId, InstrumentAny>> for InstrumentStore {
    fn from(instruments: AHashMap<InstrumentId, InstrumentAny>) -> Self {
        let mut store = Self::default();
        store.replace(instruments);
        store
    }
}

impl Drop for InstrumentStore {
    fn drop(&mut self) {
        self.clear();
    }
}

fn remove_index_entry(index: &mut SymbolIndex, instrument: &InstrumentAny) {
    let key = (instrument.id().venue, instrument.raw_symbol());
    if let Some(instruments) = index.get_mut(&key) {
        instruments.remove(&instrument.id());
        if instruments.is_empty() {
            index.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{instruments::stubs::binary_option, types::Price};
    use rstest::rstest;

    use super::*;
    use crate::cache::{Cache, CacheConfig};

    fn instrument(id: &str, raw_symbol: &str) -> InstrumentAny {
        let mut option = binary_option();
        option.id = InstrumentId::from(id);
        option.raw_symbol = Symbol::from(raw_symbol);
        InstrumentAny::BinaryOption(option)
    }

    #[rstest]
    fn read_view_tracks_updates_without_changing_held_snapshots() {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();
        let original = instrument("condition-token.POLYMARKET", "token");
        let id = original.id();
        let venue = id.venue;
        let symbol = original.raw_symbol();
        assert!(view.instrument(venue, symbol).is_none());
        cache.add_instrument(original.clone()).unwrap();
        let snapshot = view.instrument(venue, symbol).unwrap();

        let InstrumentAny::BinaryOption(mut updated) = original else {
            unreachable!();
        };
        updated.raw_symbol = Symbol::from("replacement");
        updated.price_increment = Price::from("0.01");
        cache.add_instrument(updated.into()).unwrap();

        assert_eq!(snapshot.raw_symbol(), symbol);
        assert!(view.instrument(venue, symbol).is_none());
        let current = view.instrument(venue, Symbol::from("replacement")).unwrap();
        assert_eq!(current.id(), id);
        assert_eq!(current.price_increment(), Price::from("0.01"));
        assert_eq!(
            cache.instrument(&id).unwrap().price_increment(),
            current.price_increment()
        );
        assert_eq!(view.index.read().len(), 1);
    }

    #[rstest]
    fn read_view_rejects_ambiguous_symbols_and_separates_venues() {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();
        let first = instrument("one-token.POLYMARKET", "token");
        let second = instrument("two-token.POLYMARKET", "token");
        let other = instrument("token.OTHER", "token");
        for instrument in [&first, &second, &other] {
            cache.add_instrument(instrument.clone()).unwrap();
        }
        assert!(
            view.instrument(first.id().venue, first.raw_symbol())
                .is_none()
        );
        assert_eq!(
            view.instrument(other.id().venue, other.raw_symbol())
                .unwrap()
                .id(),
            other.id()
        );
        cache.purge_instrument(second.id());
        assert_eq!(
            view.instrument(first.id().venue, first.raw_symbol())
                .unwrap()
                .id(),
            first.id()
        );
    }

    #[rstest]
    fn read_view_releases_definitions_and_index_entries_on_purge() {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();

        for n in 0..64 {
            let instrument =
                instrument(&format!("condition-{n}.POLYMARKET"), &format!("token-{n}"));
            cache.add_instrument(instrument.clone()).unwrap();
            let key = (instrument.id().venue, instrument.raw_symbol());
            let weak = view.index.read()[&key][&instrument.id()].clone();
            assert!(weak.upgrade().is_some());
            cache.purge_instrument(instrument.id());
            assert!(view.instrument(key.0, key.1).is_none());
            assert!(weak.upgrade().is_none());
            assert!(view.index.read().is_empty());
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn read_view_follows_native_reset_policy(#[case] drop_instruments: bool) {
        let mut cache = Cache::new(
            Some(CacheConfig {
                drop_instruments_on_reset: drop_instruments,
                ..Default::default()
            }),
            None,
        );
        let view = cache.instrument_read_view();
        let instrument = instrument("token.POLYMARKET", "token");
        cache.add_instrument(instrument.clone()).unwrap();
        cache.reset();
        assert_eq!(
            view.instrument(instrument.id().venue, instrument.raw_symbol())
                .is_none(),
            drop_instruments
        );
        assert_eq!(view.index.read().is_empty(), drop_instruments);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn read_view_follows_bulk_reload(#[case] all: bool) {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();
        let instrument = instrument("token.POLYMARKET", "token");
        cache.add_instrument(instrument.clone()).unwrap();
        if all {
            futures::executor::block_on(cache.cache_all()).unwrap();
        } else {
            futures::executor::block_on(cache.cache_instruments()).unwrap();
        }
        assert!(
            view.instrument(instrument.id().venue, instrument.raw_symbol())
                .is_none()
        );
        assert!(view.index.read().is_empty());
        cache.add_instrument(instrument.clone()).unwrap();
        assert!(
            view.instrument(instrument.id().venue, instrument.raw_symbol())
                .is_some()
        );
    }

    #[rstest]
    fn read_view_follows_nonempty_replacement_without_changing_handle() {
        let mut store = InstrumentStore::default();
        let view = store.read_view();
        let old = instrument("old.POLYMARKET", "old");
        let new = instrument("new.POLYMARKET", "new");
        store.insert(old.id(), old.clone());
        store.replace(AHashMap::from_iter([(new.id(), new.clone())]));
        assert!(view.instrument(old.id().venue, old.raw_symbol()).is_none());
        assert_eq!(
            view.instrument(new.id().venue, new.raw_symbol())
                .unwrap()
                .id(),
            new.id()
        );
        assert_eq!(view.index.read().len(), 1);
    }

    #[rstest]
    fn read_view_observes_native_mutations_from_another_thread() {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();
        let instrument = instrument("token.POLYMARKET", "token");
        let id = instrument.id();
        let symbol = instrument.raw_symbol();
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for () in request_rx {
                result_tx
                    .send(view.instrument(id.venue, symbol).is_some())
                    .unwrap();
            }
        });
        cache.add_instrument(instrument).unwrap();
        request_tx.send(()).unwrap();
        assert!(result_rx.recv().unwrap());
        cache.purge_instrument(id);
        request_tx.send(()).unwrap();
        assert!(!result_rx.recv().unwrap());
        drop(request_tx);
        reader.join().unwrap();
    }

    #[rstest]
    fn read_view_does_not_keep_cache_history_alive() {
        let mut cache = Cache::default();
        let view = cache.instrument_read_view();
        let instrument = instrument("token.POLYMARKET", "token");
        cache.add_instrument(instrument.clone()).unwrap();
        let key = (instrument.id().venue, instrument.raw_symbol());
        let weak = view.index.read()[&key][&instrument.id()].clone();
        drop(cache);
        assert!(view.instrument(key.0, key.1).is_none());
        assert!(weak.upgrade().is_none());
        assert!(view.index.read().is_empty());
    }
}
