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

use std::cmp::Ordering;

use nautilus_common::cache::Cache;
use nautilus_model::{
    enums::{OrderSide, OrderStatus},
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
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

/// Result of reducing identity-valid reports for one cached order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReconciliationReportSelection<'a> {
    /// No identity-valid report was available.
    None,
    /// One report is the deterministic most-advanced snapshot.
    Selected(&'a OrderStatusReport),
    /// Equally authoritative reports disagree about reconciliation state.
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

/// Selects the deterministic most-advanced snapshot for one cached order.
///
/// Filled quantity is monotonic, then the current venue ID, venue status progress,
/// and venue event time. Equally authoritative reports must describe the same
/// reconciliation state; otherwise the selection fails closed.
#[must_use]
pub fn select_reconciliation_order_report<'a>(
    order: &OrderAny,
    reports: &'a [OrderStatusReport],
) -> ReconciliationReportSelection<'a> {
    let mut selected: Option<&OrderStatusReport> = None;

    for candidate in reports {
        let Some(current) = selected else {
            selected = Some(candidate);
            continue;
        };

        match compare_order_report_progress(order.venue_order_id(), candidate, current) {
            Ordering::Greater => selected = Some(candidate),
            Ordering::Less => {}
            Ordering::Equal if reconciliation_order_report_state_matches(candidate, current) => {}
            Ordering::Equal => return ReconciliationReportSelection::Conflict,
        }
    }

    selected.map_or(
        ReconciliationReportSelection::None,
        ReconciliationReportSelection::Selected,
    )
}

/// Compares mutable order-report progress without using response order.
#[must_use]
pub fn compare_order_report_progress(
    current_venue_order_id: Option<VenueOrderId>,
    a: &OrderStatusReport,
    b: &OrderStatusReport,
) -> Ordering {
    if a.filled_qty > b.filled_qty {
        return Ordering::Greater;
    }
    if a.filled_qty < b.filled_qty {
        return Ordering::Less;
    }

    let current_venue = (Some(a.venue_order_id) == current_venue_order_id)
        .cmp(&(Some(b.venue_order_id) == current_venue_order_id));
    if current_venue != Ordering::Equal {
        return current_venue;
    }

    let status = order_status_priority(a.order_status).cmp(&order_status_priority(b.order_status));
    if status != Ordering::Equal {
        return status;
    }

    a.ts_last.cmp(&b.ts_last)
}

const fn order_status_priority(status: OrderStatus) -> u8 {
    match status {
        OrderStatus::Initialized | OrderStatus::Submitted | OrderStatus::Emulated => 0,
        OrderStatus::Released | OrderStatus::Denied => 1,
        OrderStatus::Accepted | OrderStatus::PendingUpdate | OrderStatus::PendingCancel => 2,
        OrderStatus::Triggered => 3,
        OrderStatus::PartiallyFilled => 4,
        OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected => 5,
        OrderStatus::Filled | OrderStatus::Voided => 6,
    }
}

fn reconciliation_order_report_state_matches(a: &OrderStatusReport, b: &OrderStatusReport) -> bool {
    let mut normalized = a.clone();
    normalized.client_order_id = b.client_order_id;
    normalized.report_id = b.report_id;
    normalized.ts_init = b.ts_init;
    normalized == *b
}
