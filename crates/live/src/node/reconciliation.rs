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

use std::num::NonZeroU64;

use nautilus_core::{UUID4, UnixNanos, datetime::NANOSECONDS_IN_SECOND};
use nautilus_model::{
    identifiers::{AccountId, ClientId, Venue},
    reports::{
        ExecutionMassStatus,
        mass_status::{
            ConditionalOrderCoverage, ExecutionMassStatusCoverage, ExecutionReportCoverage,
            ExecutionReportScope,
        },
    },
};

use crate::{
    execution::manager::{ReconciliationInventorySummary, ReconciliationSummary},
    runner::ExecutionApplicationSummary,
};

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
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Explicit source scope and selection; unknown declarations do not establish coverage.
    pub coverage: ExecutionMassStatusCoverage,
    /// Native application evidence when this report was reconciled.
    ///
    /// Later reports and queued events can change the cache before summary publication.
    pub application: ReconciliationSummary,
    /// Final native inventory observations after the bounded pending-message pass.
    ///
    /// Unavailable when startup failed or was interrupted before the final observation.
    pub final_inventory: Option<ReconciliationInventorySummary>,
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
            coverage: report.coverage().clone(),
            application: ReconciliationSummary::default(),
            final_inventory: None,
        }
    }
}

impl CollectedMassStatusSummary {
    /// Returns whether the source declares complete account-wide collection for this request.
    ///
    /// Requires current orders (including applicable conditional orders), current positions and
    /// fill history covering the requested lookback through report initialization. The adapter
    /// must also declare successful collection. Unknown or instrument-scoped declarations fail.
    /// This does not establish valid source identity, native application or startup readiness.
    #[must_use]
    pub fn has_complete_account_collection(&self, lookback_mins: Option<u64>) -> bool {
        if !self.reports_complete
            || !matches!(
                self.coverage.orders,
                ExecutionReportCoverage::CurrentOpen {
                    scope: ExecutionReportScope::Account
                }
            )
            || !matches!(
                self.coverage.positions,
                ExecutionReportCoverage::CurrentOpen {
                    scope: ExecutionReportScope::Account
                }
            )
            || self.coverage.conditional_orders == ConditionalOrderCoverage::Unknown
        {
            return false;
        }
        let ExecutionReportCoverage::History {
            scope: ExecutionReportScope::Account,
            start,
            end,
        } = &self.coverage.fills
        else {
            return false;
        };
        let requested_start = match lookback_mins {
            Some(mins) => {
                let Some(duration) = mins
                    .checked_mul(60)
                    .and_then(|secs| secs.checked_mul(NANOSECONDS_IN_SECOND))
                else {
                    return false;
                };
                Some(UnixNanos::from(
                    self.ts_init.as_u64().saturating_sub(duration),
                ))
            }
            None => None,
        };
        let start_covered = match (*start, requested_start) {
            (None, _) => true,
            (Some(actual), Some(requested)) => actual <= requested,
            (Some(_), None) => false,
        };
        start_covered && end.is_none_or(|actual| actual >= self.ts_init)
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
/// quiescence. Pending execution acknowledgements cover the bounded pass only.
/// Collection declarations and per-report observations do not establish agreement with all
/// retained inventory, complete historical economics, persistence, or fresh valuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupReconciliationSummary {
    /// Kernel instance identity for this attempt.
    ///
    /// A configured identity can be reused by another node; it is not a process-lifetime nonce.
    pub instance_id: UUID4,
    /// Monotonic publication sequence within this node handle's lifetime, including failures.
    ///
    /// Assigned when publishing the terminal attempt and preserved across stop/start. `None`
    /// means the sequence was exhausted or the summary has not been published. A sequence is
    /// not proof of successful reconciliation or fresh prices, and is not comparable across
    /// independently constructed nodes, even when they use the same configured instance ID.
    pub completion_sequence: Option<NonZeroU64>,
    /// How the native attempt ended, distinct from individual report reconciliation.
    pub outcome: StartupReconciliationOutcome,
    /// Configured historical lookback requested from clients.
    pub requested_lookback_mins: Option<u64>,
    /// Native timestamp when this attempt began.
    pub ts_started: UnixNanos,
    /// Native timestamp when the terminal summary was published.
    pub ts_finished: UnixNanos,
    /// Acknowledgements for direct execution messages in the bounded pending-message pass.
    pub pending_execution: ExecutionApplicationSummary,
    /// One result per client registered at the beginning of this attempt.
    pub clients: Vec<ClientReconciliationSummary>,
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    fn account_collection() -> CollectedMassStatusSummary {
        let mut report = ExecutionMassStatus::new(
            ClientId::from("SIM"),
            AccountId::from("SIM-001"),
            Venue::from("SIM"),
            UnixNanos::from(10_000_000_000_000),
            None,
        );
        report.set_coverage(ExecutionMassStatusCoverage {
            orders: ExecutionReportCoverage::CurrentOpen {
                scope: ExecutionReportScope::Account,
            },
            positions: ExecutionReportCoverage::CurrentOpen {
                scope: ExecutionReportScope::Account,
            },
            fills: ExecutionReportCoverage::History {
                scope: ExecutionReportScope::Account,
                start: None,
                end: None,
            },
            conditional_orders: ConditionalOrderCoverage::Included,
        });
        CollectedMassStatusSummary::from(&report)
    }

    #[rstest]
    #[case::unbounded(None)]
    #[case::bounded(Some(60))]
    fn test_complete_empty_account_collection(#[case] lookback: Option<u64>) {
        let summary = account_collection();
        assert!(summary.has_complete_account_collection(lookback));
        // A collection declaration does not manufacture application evidence.
        assert!(!summary.application.all_received_reports_reconciled());
    }

    #[rstest]
    fn test_complete_account_collection_requires_success_and_declared_coverage() {
        let mut summary = account_collection();
        summary.reports_complete = false;
        assert!(!summary.has_complete_account_collection(None));
        summary.reports_complete = true;
        summary.coverage = ExecutionMassStatusCoverage::default();
        assert!(!summary.has_complete_account_collection(None));
    }

    #[rstest]
    #[case::unknown(ExecutionReportCoverage::Unknown)]
    #[case::instrument(ExecutionReportCoverage::CurrentOpen { scope: ExecutionReportScope::Instruments(vec![InstrumentId::from("TEST.SIM")]) })]
    #[case::empty_instruments(ExecutionReportCoverage::CurrentOpen { scope: ExecutionReportScope::Instruments(Vec::new()) })]
    #[case::history_only(ExecutionReportCoverage::History { scope: ExecutionReportScope::Account, start: None, end: None })]
    fn test_current_account_inventory_requires_each_source(
        #[case] incomplete: ExecutionReportCoverage,
    ) {
        let mut summary = account_collection();
        summary.coverage.orders = incomplete.clone();
        assert!(!summary.has_complete_account_collection(None));
        let mut summary = account_collection();
        summary.coverage.positions = incomplete;
        assert!(!summary.has_complete_account_collection(None));
    }

    #[rstest]
    #[case::unknown(ConditionalOrderCoverage::Unknown, false)]
    #[case::included(ConditionalOrderCoverage::Included, true)]
    #[case::not_applicable(ConditionalOrderCoverage::NotApplicable, true)]
    fn test_conditional_order_coverage(
        #[case] coverage: ConditionalOrderCoverage,
        #[case] complete: bool,
    ) {
        let mut summary = account_collection();
        summary.coverage.conditional_orders = coverage;
        assert_eq!(summary.has_complete_account_collection(None), complete);
    }

    #[rstest]
    #[case::exact(Some(6_400_000_000_000), Some(10_000_000_000_000), Some(60), true)]
    #[case::narrow_start(Some(6_400_000_000_001), None, Some(60), false)]
    #[case::narrow_end(None, Some(9_999_999_999_999), Some(60), false)]
    #[case::bounded_for_unbounded(Some(0), None, None, false)]
    #[case::invalid_interval(Some(10_000_000_000_001), Some(10_000_000_000_000), Some(0), false)]
    #[case::overflow(None, None, Some(u64::MAX), false)]
    fn test_fill_window_covers_requested_history(
        #[case] start: Option<u64>,
        #[case] end: Option<u64>,
        #[case] lookback: Option<u64>,
        #[case] complete: bool,
    ) {
        let mut summary = account_collection();
        summary.coverage.fills = ExecutionReportCoverage::History {
            scope: ExecutionReportScope::Account,
            start: start.map(UnixNanos::from),
            end: end.map(UnixNanos::from),
        };
        assert_eq!(summary.has_complete_account_collection(lookback), complete);
    }

    #[rstest]
    #[case::unknown(ExecutionReportCoverage::Unknown)]
    #[case::current_only(ExecutionReportCoverage::CurrentOpen { scope: ExecutionReportScope::Account })]
    #[case::scoped(ExecutionReportCoverage::History { scope: ExecutionReportScope::Instruments(vec![InstrumentId::from("TEST.SIM")]), start: None, end: None })]
    fn test_fill_history_requires_explicit_account_scope(
        #[case] coverage: ExecutionReportCoverage,
    ) {
        let mut summary = account_collection();
        summary.coverage.fills = coverage;
        assert!(!summary.has_complete_account_collection(None));
    }
}
