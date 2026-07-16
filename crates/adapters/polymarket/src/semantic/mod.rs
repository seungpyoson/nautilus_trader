// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

mod capabilities;
mod collector;
mod decode;
mod generated;
mod hooks;
mod sensitive;

pub use capabilities::{AutonomousEntryCapability, CapabilityUnavailable, CurrentV2Capabilities};
pub use collector::{CollectorError, CollectorKind, CollectorPlan, FixedCollector};
pub use decode::{
    AssociatedTradeObservation, AssociatedTradesObservation, DiagnosticClass,
    ExactOrderObservation, PostOrderObservation, SemanticDiagnostic, decode_associated_trades,
    decode_exact_order, decode_post_order,
};
pub use generated::*;
pub use hooks::{FinalizedBlockRef, PreDispatchHook, PreSendHook, SemanticHookError};
pub use sensitive::{
    RedactedMetadata, SemanticCredential, SensitiveProviderBytes, SensitiveSignedRequest,
    SensitiveValueError,
};
