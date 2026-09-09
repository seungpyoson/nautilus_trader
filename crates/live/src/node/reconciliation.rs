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

//! Bounded diagnostics from the latest native startup reconciliation attempt.

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    identifiers::{AccountId, ClientId, Venue},
    reports::ExecutionMassStatus,
};

use crate::execution::manager::ReconciliationSummary;

/// How the startup reconciliation attempt ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupReconciliationOutcome {
    /// Reconciliation was disabled and wallet order initialization finished.
    Disabled,
    /// The native startup pass, portfolio initialization and bounded pending-message pass finished.
    ///
    /// Individual clients may still have unavailable or unresolved reports.
    Finished,
    /// Stop or shutdown was requested before the pre-trader startup boundary.
    ///
    /// The bounded pending-message pass may have been skipped or interrupted.
    Interrupted,
    /// Startup reconciliation or portfolio initialization returned an error.
    Failed,
}

/// Collection outcome for one requested execution client.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MassStatusCollection {
    /// No request was made, for example because a previous client failed.
    #[default]
    NotRequested,
    /// The client returned no mass status.
    Unavailable,
    /// The client returned an error.
    Failed,
    /// The startup deadline expired before this client's request completed.
    TimedOut,
    /// A mass status was returned; its metadata and application evidence are available.
    Received,
}

/// Metadata and application evidence for a returned mass status, without report histories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectedMassStatusSummary {
    /// Source identity declared by the returned report.
    pub client_id: ClientId,
    /// Account declared by the returned report.
    pub account_id: AccountId,
    /// Venue declared by the returned report.
    pub venue: Venue,
    /// Identity of the returned report.
    pub report_id: UUID4,
    /// Timestamp at which the adapter initialized its report.
    pub ts_init: UnixNanos,
    /// Lower historical bound declared by the adapter.
    pub lookback_start: Option<UnixNanos>,
    /// The adapter's completeness declaration, not independent query-class coverage proof.
    pub reports_complete: bool,
    /// Native application evidence when this report was reconciled.
    ///
    /// Later reports and queued events can change the cache before summary publication.
    pub application: ReconciliationSummary,
}

impl From<&ExecutionMassStatus> for CollectedMassStatusSummary {
    fn from(report: &ExecutionMassStatus) -> Self {
        Self {
            client_id: report.client_id,
            account_id: report.account_id,
            venue: report.venue,
            report_id: report.report_id,
            ts_init: report.ts_init,
            lookback_start: report.lookback_start(),
            reports_complete: report.reports_complete(),
            application: ReconciliationSummary::default(),
        }
    }
}

/// Outcome for one client registered when the native startup attempt began.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientReconciliationSummary {
    /// Requested execution client.
    pub client_id: ClientId,
    /// Account of the requested client.
    pub account_id: AccountId,
    /// Venue of the requested client.
    pub venue: Venue,
    /// Terminal collection disposition for this client.
    pub collection: MassStatusCollection,
    /// Returned report metadata and application evidence, if any.
    pub report: Option<CollectedMassStatusSummary>,
}

/// Bounded diagnostic snapshot from the latest native startup reconciliation attempt.
///
/// Published after the bounded pending-message pass and its abort check, before actors and
/// strategies start. Reconciliation failures publish before error cleanup instead.
/// It survives stop for diagnosis and is cleared when the node enters Starting. Failures before
/// that transition and cancellation of the reconciliation future do not publish a new result.
/// This is not a live readiness permit: the pending-message pass does not establish queue
/// quiescence or acknowledge successful application. Collection declarations and per-report
/// observations do not establish query-class coverage, omitted inventory, final cache agreement,
/// historical economics, persistence, or fresh valuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupReconciliationSummary {
    /// How the native attempt ended, distinct from individual report reconciliation.
    pub outcome: StartupReconciliationOutcome,
    /// Configured historical lookback requested from clients.
    pub requested_lookback_mins: Option<u64>,
    /// Native timestamp when this attempt began.
    pub ts_started: UnixNanos,
    /// Native timestamp when the terminal summary was published.
    pub ts_finished: UnixNanos,
    /// One result per client registered at the beginning of this attempt.
    pub clients: Vec<ClientReconciliationSummary>,
}
