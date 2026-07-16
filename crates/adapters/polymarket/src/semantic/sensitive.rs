// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use aws_lc_rs::digest;
use zeroize::Zeroizing;

use super::{SemanticLimits, SemanticRoute};

/// Safe metadata derived from a validated sensitive value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedactedMetadata {
    route: SemanticRoute,
    validated_len: usize,
    sha256: [u8; 32],
}

impl RedactedMetadata {
    /// Returns the registered semantic route.
    #[must_use]
    pub const fn route(self) -> SemanticRoute {
        self.route
    }

    /// Returns the validated byte length.
    #[must_use]
    pub const fn validated_len(self) -> usize {
        self.validated_len
    }

    /// Returns the SHA-256 digest without exposing the source bytes.
    #[must_use]
    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

/// A fail-closed sensitive-value validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SensitiveValueError {
    Empty,
    Capacity,
}

/// Validated raw provider bytes with no observable raw-byte API.
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveProviderBytes;
/// let value: SensitiveProviderBytes = panic!();
/// let _ = format!("{value:?}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveProviderBytes;
/// let value: SensitiveProviderBytes = panic!();
/// let _ = format!("{value}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveProviderBytes;
/// let value: SensitiveProviderBytes = panic!();
/// let _ = serde_json::to_vec(&value);
/// ```
#[allow(missing_debug_implementations)]
pub struct SensitiveProviderBytes {
    bytes: Zeroizing<Vec<u8>>,
    limits: SemanticLimits,
    metadata: RedactedMetadata,
}

impl SensitiveProviderBytes {
    /// Checks the response byte limit before hashing or copying the input.
    pub fn checked(
        route: SemanticRoute,
        bytes: &[u8],
        limits: SemanticLimits,
    ) -> Result<Self, SensitiveValueError> {
        checked_bytes(bytes, limits.response_body_bytes())?;
        Ok(Self {
            metadata: metadata(route, bytes),
            bytes: Zeroizing::new(bytes.to_vec()),
            limits,
        })
    }

    /// Returns only safe metadata.
    #[must_use]
    pub const fn metadata(&self) -> &RedactedMetadata {
        &self.metadata
    }

    pub(crate) const fn limits(&self) -> SemanticLimits {
        self.limits
    }

    pub(crate) fn decode_with<T>(&self, decode: impl FnOnce(&[u8]) -> T) -> T {
        decode(self.bytes.as_slice())
    }
}

/// Validated signed request bytes with no observable raw-byte API.
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveSignedRequest;
/// let value: SensitiveSignedRequest = panic!();
/// let _ = format!("{value:?}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveSignedRequest;
/// let value: SensitiveSignedRequest = panic!();
/// let _ = format!("{value}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SensitiveSignedRequest;
/// let value: SensitiveSignedRequest = panic!();
/// let _ = serde_json::to_vec(&value);
/// ```
#[allow(missing_debug_implementations)]
pub struct SensitiveSignedRequest {
    _bytes: Zeroizing<Vec<u8>>,
    metadata: RedactedMetadata,
}

impl SensitiveSignedRequest {
    /// Checks the request byte limit before hashing or copying the input.
    pub fn checked(
        route: SemanticRoute,
        bytes: &[u8],
        limits: SemanticLimits,
    ) -> Result<Self, SensitiveValueError> {
        checked_bytes(bytes, limits.request_body_bytes())?;
        Ok(Self {
            metadata: metadata(route, bytes),
            _bytes: Zeroizing::new(bytes.to_vec()),
        })
    }

    /// Returns only safe metadata.
    #[must_use]
    pub const fn metadata(&self) -> &RedactedMetadata {
        &self.metadata
    }
}

/// Validated zeroizing provider credentials with no projection API.
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SemanticCredential;
/// let value: SemanticCredential = panic!();
/// let _ = format!("{value:?}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SemanticCredential;
/// let value: SemanticCredential = panic!();
/// let _ = format!("{value}");
/// ```
///
/// ```compile_fail
/// # use nautilus_polymarket::semantic::SemanticCredential;
/// let value: SemanticCredential = panic!();
/// let _ = serde_json::to_vec(&value);
/// ```
#[allow(missing_debug_implementations)]
pub struct SemanticCredential {
    _api_key: Zeroizing<Vec<u8>>,
    _secret: Zeroizing<Vec<u8>>,
    _passphrase: Zeroizing<Vec<u8>>,
}

impl SemanticCredential {
    /// Checks every field before copying any credential bytes.
    pub fn checked(
        api_key: &[u8],
        secret: &[u8],
        passphrase: &[u8],
        limits: SemanticLimits,
    ) -> Result<Self, SensitiveValueError> {
        let limit = limits.string_bytes();
        checked_bytes(api_key, limit)?;
        checked_bytes(secret, limit)?;
        checked_bytes(passphrase, limit)?;

        Ok(Self {
            _api_key: Zeroizing::new(api_key.to_vec()),
            _secret: Zeroizing::new(secret.to_vec()),
            _passphrase: Zeroizing::new(passphrase.to_vec()),
        })
    }
}

const fn checked_bytes(bytes: &[u8], limit: usize) -> Result<(), SensitiveValueError> {
    if bytes.is_empty() {
        return Err(SensitiveValueError::Empty);
    }
    if bytes.len() > limit {
        return Err(SensitiveValueError::Capacity);
    }
    Ok(())
}

fn metadata(route: SemanticRoute, bytes: &[u8]) -> RedactedMetadata {
    let digest = digest::digest(&digest::SHA256, bytes);
    let mut sha256 = [0_u8; 32];
    sha256.copy_from_slice(digest.as_ref());
    RedactedMetadata {
        route,
        validated_len: bytes.len(),
        sha256,
    }
}
