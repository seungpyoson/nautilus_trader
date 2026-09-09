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

use std::fmt::Debug;

use nautilus_common::cache::InstrumentReadView;
use nautilus_model::{
    identifiers::{Symbol, Venue},
    instruments::InstrumentAny,
};
use ustr::Ustr;

/// Resolves venue token IDs for execution reports on both HTTP and WebSocket tasks.
pub(crate) trait TokenInstrumentLookup: Debug + Send + Sync {
    fn get_cloned(&self, token: &Ustr) -> Option<InstrumentAny>;
}

/// Borrows NT's canonical definitions without retaining an adapter-owned history.
#[derive(Clone, Debug)]
pub(crate) struct PolymarketInstrumentLookup {
    view: InstrumentReadView,
    venue: Venue,
}

impl PolymarketInstrumentLookup {
    pub(crate) fn new(view: InstrumentReadView, venue: Venue) -> Self {
        Self { view, venue }
    }
}

impl TokenInstrumentLookup for PolymarketInstrumentLookup {
    fn get_cloned(&self, token: &Ustr) -> Option<InstrumentAny> {
        let symbol = Symbol::new_checked(token.as_str()).ok()?;
        self.view.instrument(self.venue, symbol)
    }
}

pub(super) fn instrument_neg_risk(instrument: &InstrumentAny) -> bool {
    match instrument {
        InstrumentAny::BinaryOption(option) => option
            .info
            .as_ref()
            .and_then(|info| info.get_bool("neg_risk"))
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::collections::AtomicMap;

    use super::*;

    // Static parser fixtures provide only the definitions used by that decoding case
    impl TokenInstrumentLookup for AtomicMap<Ustr, InstrumentAny> {
        fn get_cloned(&self, token: &Ustr) -> Option<InstrumentAny> {
            AtomicMap::get_cloned(self, token)
        }
    }
}
