// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use super::{RedactedMetadata, SensitiveSignedRequest};

/// An opaque finalized block identity for pre-dispatch validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizedBlockRef {
    number: u64,
    hash: [u8; 32],
}

impl FinalizedBlockRef {
    #[must_use]
    pub const fn new(number: u64, hash: [u8; 32]) -> Self {
        Self { number, hash }
    }

    #[must_use]
    pub const fn number(self) -> u64 {
        self.number
    }

    #[must_use]
    pub const fn hash(self) -> [u8; 32] {
        self.hash
    }
}

/// A fail-closed semantic hook result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticHookError {
    Denied,
    Unavailable,
}

/// Synchronous validation immediately before a signed request can be sent.
pub trait PreSendHook: Send + Sync {
    fn before_send(&self, request: &SensitiveSignedRequest) -> Result<(), SemanticHookError>;
}

/// Synchronous validation immediately before a finalized effect can be dispatched.
pub trait PreDispatchHook: Send + Sync {
    fn before_dispatch(
        &self,
        request: &RedactedMetadata,
        block: FinalizedBlockRef,
    ) -> Result<(), SemanticHookError>;
}
