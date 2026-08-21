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

//! Trackers for in-flight submit and cancel commands whose venue order ID is not yet known.

use std::sync::{Arc, Mutex};

use ahash::{AHashMap, AHashSet};
use nautilus_core::MUTEX_POISONED;
use nautilus_model::identifiers::{ClientOrderId, VenueOrderId};

#[derive(Clone, Copy, Debug)]
struct PendingSubmit {
    client_order_id: ClientOrderId,
}

/// Maps an in-flight submit's expected venue order ID to its local identity.
///
/// Exact economic authority is retained exclusively by `OrderFillTrackerMap` before the POST.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingSubmitTracker {
    venue_to_submit: Arc<Mutex<AHashMap<VenueOrderId, PendingSubmit>>>,
}

impl PendingSubmitTracker {
    pub(crate) fn clear(&self) {
        self.venue_to_submit.lock().expect(MUTEX_POISONED).clear();
    }

    pub(crate) fn remove(&self, venue_order_id: &VenueOrderId) {
        self.venue_to_submit
            .lock()
            .expect(MUTEX_POISONED)
            .remove(venue_order_id);
    }

    pub(crate) fn insert(&self, venue_order_id: VenueOrderId, client_order_id: ClientOrderId) {
        self.venue_to_submit
            .lock()
            .expect(MUTEX_POISONED)
            .insert(venue_order_id, PendingSubmit { client_order_id });
    }

    pub(crate) fn client_order_id(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.venue_to_submit
            .lock()
            .expect(MUTEX_POISONED)
            .get(venue_order_id)
            .map(|submit| submit.client_order_id)
    }
}

/// Tracks client order IDs whose cancel was deferred because the venue order ID was not yet
/// known, so the cancel can be issued once the submit response lands.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingCancelTracker {
    client_order_ids: Arc<Mutex<AHashSet<ClientOrderId>>>,
}

impl PendingCancelTracker {
    pub(crate) fn clear(&self) {
        self.client_order_ids.lock().expect(MUTEX_POISONED).clear();
    }

    pub(crate) fn insert(&self, client_order_id: ClientOrderId) {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_id);
    }

    pub(crate) fn remove(&self, client_order_id: &ClientOrderId) -> bool {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .remove(client_order_id)
    }

    pub(crate) fn contains(&self, client_order_id: &ClientOrderId) -> bool {
        self.client_order_ids
            .lock()
            .expect(MUTEX_POISONED)
            .contains(client_order_id)
    }
}
