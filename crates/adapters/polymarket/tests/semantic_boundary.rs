// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use nautilus_polymarket::semantic::{
    CollectorError, CollectorKind, CollectorPlan, SemanticLimitError, SemanticLimitKind,
    SemanticLimitValues, SemanticLimits,
};

fn limit_values(transaction_hashes: usize) -> SemanticLimitValues {
    SemanticLimitValues {
        request_body_bytes: 128,
        request_items: 8,
        response_body_bytes: 4096,
        response_items: 8,
        transaction_hashes,
        trade_ids: 8,
        associated_trades: 8,
        string_bytes: 256,
        decimal_bytes: 128,
        log_items: 8,
    }
}

fn limits(transaction_hashes: usize) -> SemanticLimits {
    SemanticLimits::checked(limit_values(transaction_hashes)).unwrap()
}

#[test]
fn semantic_limits_reject_each_zero_field() {
    let mut values = limit_values(96);
    values.request_body_bytes = 0;
    assert_eq!(
        SemanticLimits::checked(values),
        Err(SemanticLimitError::Zero(
            SemanticLimitKind::RequestBodyBytes
        ))
    );
}

#[test]
fn transaction_hash_plan_accepts_capacity_minus_one_and_capacity() {
    let limits = limits(96);

    assert!(
        CollectorPlan::bounded(CollectorKind::TransactionHashes, 95, 95 * 32, limits).is_ok()
    );
    assert!(
        CollectorPlan::bounded(CollectorKind::TransactionHashes, 96, 96 * 32, limits).is_ok()
    );
}

#[test]
fn transaction_hash_plan_rejects_capacity_plus_one() {
    let error = CollectorPlan::bounded(
        CollectorKind::TransactionHashes,
        97,
        96 * 32,
        limits(96),
    )
    .unwrap_err();

    assert_eq!(error, CollectorError::ItemCapacity);
}

#[test]
fn bounded_collector_checks_bytes_before_retaining_item() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate();

    collector.try_push("first", 4).unwrap();
    assert_eq!(collector.len(), 1);
    assert_eq!(
        collector.try_push("rejected", 1),
        Err(CollectorError::ByteCapacity)
    );
    assert_eq!(collector.len(), 1);
}

#[test]
fn bounded_collector_checks_items_before_retaining_item() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 1, 4, limits(96)).unwrap();
    let mut collector = plan.allocate();

    collector.try_push("first", 1).unwrap();
    assert_eq!(
        collector.try_push("rejected", 1),
        Err(CollectorError::ItemCapacity)
    );
    assert_eq!(collector.len(), 1);
}

#[test]
fn exact_collector_rejects_incomplete_item_count() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate();
    collector.try_push(1_u8, 2).unwrap();

    assert_eq!(collector.finish(), Err(CollectorError::Incomplete));
}

#[test]
fn exact_collector_rejects_contradictory_byte_total() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate();
    collector.try_push(1_u8, 1).unwrap();
    collector.try_push(2_u8, 2).unwrap();

    assert_eq!(collector.finish(), Err(CollectorError::Contradictory));
}

#[test]
fn exact_collector_finishes_only_at_declared_totals() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate();
    collector.try_push(1_u8, 2).unwrap();
    collector.try_push(2_u8, 2).unwrap();

    assert_eq!(collector.finish().unwrap(), vec![1, 2]);
}

#[test]
fn collector_rejects_byte_addition_overflow() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 2, 4096, limits(96)).unwrap();
    let mut collector = plan.allocate();
    collector.try_push(1_u8, 1).unwrap();

    assert_eq!(
        collector.try_push(2_u8, usize::MAX),
        Err(CollectorError::ArithmeticOverflow)
    );
    assert_eq!(collector.len(), 1);
}
