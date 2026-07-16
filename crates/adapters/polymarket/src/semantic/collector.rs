// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use super::SemanticLimits;

/// Identifies the semantic collection whose capacity is being checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectorKind {
    RequestItems,
    ResponseItems,
    TransactionHashes,
    TradeIds,
    AssociatedTrades,
}

/// A fail-closed collector validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectorError {
    ZeroCapacity,
    ItemCapacity,
    ByteCapacity,
    ArithmeticOverflow,
    Incomplete,
    Contradictory,
}

/// A validated, allocation-free plan for a fixed-capacity collector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectorPlan {
    item_capacity: usize,
    byte_capacity: usize,
    exact: bool,
}

impl CollectorPlan {
    /// Validates bounded capacities against caller-supplied semantic limits.
    pub fn bounded(
        kind: CollectorKind,
        item_capacity: usize,
        byte_capacity: usize,
        limits: SemanticLimits,
    ) -> Result<Self, CollectorError> {
        if item_capacity == 0 || byte_capacity == 0 {
            return Err(CollectorError::ZeroCapacity);
        }

        if item_capacity > item_limit(kind, limits) {
            return Err(CollectorError::ItemCapacity);
        }

        if byte_capacity > byte_limit(kind, limits) {
            return Err(CollectorError::ByteCapacity);
        }

        Ok(Self {
            item_capacity,
            byte_capacity,
            exact: false,
        })
    }

    /// Validates capacities which must be filled exactly before completion.
    pub fn exact(
        kind: CollectorKind,
        expected_items: usize,
        expected_bytes: usize,
        limits: SemanticLimits,
    ) -> Result<Self, CollectorError> {
        let mut plan = Self::bounded(kind, expected_items, expected_bytes, limits)?;
        plan.exact = true;
        Ok(plan)
    }

    /// Performs the collector's sole allocation after validation has succeeded.
    #[must_use]
    pub fn allocate<T>(self) -> FixedCollector<T> {
        FixedCollector {
            items: Vec::with_capacity(self.item_capacity),
            plan: self,
            observed_bytes: 0,
        }
    }
}

/// A fixed-capacity collector which validates before retaining each item.
#[derive(Debug)]
pub struct FixedCollector<T> {
    items: Vec<T>,
    plan: CollectorPlan,
    observed_bytes: usize,
}

impl<T> FixedCollector<T> {
    /// Retains an item only after count and byte checks succeed.
    pub fn try_push(&mut self, item: T, encoded_bytes: usize) -> Result<(), CollectorError> {
        if self.items.len() >= self.plan.item_capacity {
            return Err(CollectorError::ItemCapacity);
        }
        let observed_bytes = self
            .observed_bytes
            .checked_add(encoded_bytes)
            .ok_or(CollectorError::ArithmeticOverflow)?;
        if observed_bytes > self.plan.byte_capacity {
            return Err(CollectorError::ByteCapacity);
        }

        self.items.push(item);
        self.observed_bytes = observed_bytes;
        Ok(())
    }

    /// Returns the retained item count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether no items have been retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Completes only if all exact-plan declarations agree with observations.
    pub fn finish(self) -> Result<Vec<T>, CollectorError> {
        if self.plan.exact && self.items.len() != self.plan.item_capacity {
            return Err(CollectorError::Incomplete);
        }

        if self.plan.exact && self.observed_bytes != self.plan.byte_capacity {
            return Err(CollectorError::Contradictory);
        }
        Ok(self.items)
    }
}

const fn item_limit(kind: CollectorKind, limits: SemanticLimits) -> usize {
    match kind {
        CollectorKind::RequestItems => limits.request_items(),
        CollectorKind::ResponseItems => limits.response_items(),
        CollectorKind::TransactionHashes => limits.transaction_hashes(),
        CollectorKind::TradeIds => limits.trade_ids(),
        CollectorKind::AssociatedTrades => limits.associated_trades(),
    }
}

const fn byte_limit(kind: CollectorKind, limits: SemanticLimits) -> usize {
    match kind {
        CollectorKind::RequestItems => limits.request_body_bytes(),
        CollectorKind::ResponseItems
        | CollectorKind::TransactionHashes
        | CollectorKind::TradeIds
        | CollectorKind::AssociatedTrades => limits.response_body_bytes(),
    }
}
