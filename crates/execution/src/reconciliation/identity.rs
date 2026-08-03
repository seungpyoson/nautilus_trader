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

use nautilus_common::cache::Cache;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    orders::{Order, OrderAny},
};

/// Identity carried by an execution report when resolving its cached order.
#[derive(Clone, Copy, Debug)]
pub struct ReconciliationReportIdentity {
    pub instrument_id: InstrumentId,
    pub client_order_id: Option<ClientOrderId>,
    pub venue_order_id: VenueOrderId,
    pub order_side: OrderSide,
}

/// Result of resolving an execution report against both cache identity indexes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportOrderResolution {
    Matched(ClientOrderId),
    External,
    Conflict,
}

/// Resolves a report through both client and venue indexes before validating order identity.
#[must_use]
pub fn resolve_report_order(
    cache: &Cache,
    report: ReconciliationReportIdentity,
) -> ReportOrderResolution {
    let by_client = report
        .client_order_id
        .and_then(|id| cache.order(&id).map(|order| order.clone()));
    let by_venue = cache
        .client_order_id(&report.venue_order_id)
        .and_then(|id| cache.order(id).map(|order| order.clone()));

    let order = match (by_client, by_venue) {
        (Some(client_order), Some(venue_order))
            if client_order.client_order_id() != venue_order.client_order_id() =>
        {
            return ReportOrderResolution::Conflict;
        }
        (Some(order), _) | (None, Some(order)) => order,
        (None, None) => return ReportOrderResolution::External,
    };

    if report_identity_matches_order(&order, report) {
        ReportOrderResolution::Matched(order.client_order_id())
    } else {
        ReportOrderResolution::Conflict
    }
}

/// Returns whether a report names the immutable cached identity of an order.
#[must_use]
pub fn report_identity_matches_order(
    order: &OrderAny,
    report: ReconciliationReportIdentity,
) -> bool {
    let venue_id_matches = order.venue_order_id().is_none()
        || order.venue_order_id() == Some(report.venue_order_id)
        || order
            .venue_order_ids()
            .iter()
            .any(|venue_order_id| **venue_order_id == report.venue_order_id);
    let client_id_matches = report
        .client_order_id
        .is_none_or(|client_order_id| client_order_id == order.client_order_id());

    order.instrument_id() == report.instrument_id
        && order.order_side() == report.order_side
        && client_id_matches
        && venue_id_matches
}
