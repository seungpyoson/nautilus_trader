// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use super::{CURRENT_V2_UNAVAILABLE, CapabilityEvidence};

/// The registered provider capability state for the reviewed V2 revisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CurrentV2Capabilities;

impl CurrentV2Capabilities {
    /// Returns the closed capability state generated from registered evidence.
    #[must_use]
    pub const fn current_v2() -> Self {
        Self
    }

    /// Returns every independent unavailable capability.
    #[must_use]
    pub const fn unavailable(&self) -> &'static [CapabilityEvidence] {
        &CURRENT_V2_UNAVAILABLE
    }

    /// Always fails because the reviewed V2 evidence satisfies no autonomous-entry gate.
    pub const fn require_autonomous_entry(
        &self,
    ) -> Result<AutonomousEntryCapability, CapabilityUnavailable> {
        Err(CapabilityUnavailable)
    }
}

/// The complete fail-closed current-V2 capability result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityUnavailable;

impl CapabilityUnavailable {
    #[must_use]
    pub const fn unavailable(&self) -> &'static [CapabilityEvidence] {
        &CURRENT_V2_UNAVAILABLE
    }
}

/// One-use autonomous-entry authority, unconstructable for current V2.
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::AutonomousEntryCapability;
/// let _ = AutonomousEntryCapability { _private: () };
/// ```
#[allow(missing_debug_implementations)]
pub struct AutonomousEntryCapability {
    _private: (),
}
