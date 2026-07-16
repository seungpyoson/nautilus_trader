// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use nautilus_polymarket::semantic::{
    CollectorError, CollectorKind, CollectorPlan, FinalizedBlockRef, PreDispatchHook, PreSendHook,
    SemanticCredential, SemanticHookError, SemanticLimitError, SemanticLimitKind,
    SemanticLimitValues, SemanticLimits, SemanticRoute, SensitiveProviderBytes,
    SensitiveSignedRequest,
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

    assert!(CollectorPlan::bounded(CollectorKind::TransactionHashes, 95, 95 * 32, limits).is_ok());
    assert!(CollectorPlan::bounded(CollectorKind::TransactionHashes, 96, 96 * 32, limits).is_ok());
}

#[test]
fn transaction_hash_plan_rejects_capacity_plus_one() {
    let error = CollectorPlan::bounded(CollectorKind::TransactionHashes, 97, 96 * 32, limits(96))
        .unwrap_err();

    assert_eq!(error, CollectorError::ItemCapacity);
}

#[test]
fn request_plan_accepts_capacity_edges_and_rejects_capacity_plus_one() {
    let limits = limits(96);

    assert!(CollectorPlan::bounded(CollectorKind::RequestItems, 7, 127, limits).is_ok());
    assert!(CollectorPlan::bounded(CollectorKind::RequestItems, 8, 128, limits).is_ok());
    assert_eq!(
        CollectorPlan::bounded(CollectorKind::RequestItems, 9, 128, limits).unwrap_err(),
        CollectorError::ItemCapacity
    );
    assert_eq!(
        CollectorPlan::bounded(CollectorKind::RequestItems, 8, 129, limits).unwrap_err(),
        CollectorError::ByteCapacity
    );
}

#[test]
fn response_plan_accepts_capacity_edges_and_rejects_capacity_plus_one() {
    let limits = limits(96);

    assert!(CollectorPlan::bounded(CollectorKind::ResponseItems, 7, 4095, limits).is_ok());
    assert!(CollectorPlan::bounded(CollectorKind::ResponseItems, 8, 4096, limits).is_ok());
    assert_eq!(
        CollectorPlan::bounded(CollectorKind::ResponseItems, 9, 4096, limits).unwrap_err(),
        CollectorError::ItemCapacity
    );
    assert_eq!(
        CollectorPlan::bounded(CollectorKind::ResponseItems, 8, 4097, limits).unwrap_err(),
        CollectorError::ByteCapacity
    );
}

#[test]
fn collector_plan_rejects_zero_capacity() {
    let limits = limits(96);

    assert_eq!(
        CollectorPlan::bounded(CollectorKind::ResponseItems, 0, 1, limits).unwrap_err(),
        CollectorError::ZeroCapacity
    );
    assert_eq!(
        CollectorPlan::bounded(CollectorKind::ResponseItems, 1, 0, limits).unwrap_err(),
        CollectorError::ZeroCapacity
    );
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

#[test]
fn sensitive_values_expose_only_redacted_metadata() {
    let sentinel = b"credential-success-failure-malformed-sentinel";
    let limits = limits(96);
    let provider =
        SensitiveProviderBytes::checked(SemanticRoute::GetExactOrder, sentinel.as_slice(), limits)
            .expect("fixture is within caller-supplied limits");
    let request =
        SensitiveSignedRequest::checked(SemanticRoute::PostOrder, sentinel.as_slice(), limits)
            .expect("fixture is within caller-supplied limits");
    let _credential = SemanticCredential::checked(
        b"credential-sentinel",
        b"secret-sentinel",
        b"passphrase-sentinel",
        limits,
    )
    .expect("fixtures are within caller-supplied limits");

    assert_eq!(provider.metadata().route(), SemanticRoute::GetExactOrder);
    assert_eq!(provider.metadata().validated_len(), sentinel.len());
    assert_eq!(provider.metadata().sha256().len(), 32);
    assert_eq!(request.metadata().route(), SemanticRoute::PostOrder);
    assert!(!format!("{:?}", provider.metadata()).contains("sentinel"));
    assert!(!format!("{:?}", request.metadata()).contains("sentinel"));
}

#[test]
fn sensitive_values_reject_oversized_input() {
    let limits = limits(96);
    let oversized_request = [0_u8; 129];
    let oversized_response = [0_u8; 4097];
    let oversized_credential = [0_u8; 257];

    assert!(
        SensitiveSignedRequest::checked(
            SemanticRoute::PostOrder,
            oversized_request.as_slice(),
            limits,
        )
        .is_err()
    );
    assert!(
        SensitiveProviderBytes::checked(
            SemanticRoute::GetExactOrder,
            oversized_response.as_slice(),
            limits,
        )
        .is_err()
    );
    assert!(
        SemanticCredential::checked(
            oversized_credential.as_slice(),
            b"secret",
            b"passphrase",
            limits,
        )
        .is_err()
    );
}

struct RejectPreSend;

impl PreSendHook for RejectPreSend {
    fn before_send(&self, request: &SensitiveSignedRequest) -> Result<(), SemanticHookError> {
        assert_eq!(request.metadata().route(), SemanticRoute::PostOrder);
        Err(SemanticHookError::Denied)
    }
}

struct RejectPreDispatch;

impl PreDispatchHook for RejectPreDispatch {
    fn before_dispatch(
        &self,
        request: &nautilus_polymarket::semantic::RedactedMetadata,
        block: FinalizedBlockRef,
    ) -> Result<(), SemanticHookError> {
        assert_eq!(request.route(), SemanticRoute::PostOrder);
        assert_eq!(block.number(), 42);
        Err(SemanticHookError::Unavailable)
    }
}

#[test]
fn semantic_hooks_are_synchronous_and_fail_closed() {
    let request = SensitiveSignedRequest::checked(
        SemanticRoute::PostOrder,
        b"signed-request-sentinel",
        limits(96),
    )
    .unwrap();
    let block = FinalizedBlockRef::new(42, [7_u8; 32]);

    assert_eq!(
        RejectPreSend.before_send(&request),
        Err(SemanticHookError::Denied)
    );
    assert_eq!(
        RejectPreDispatch.before_dispatch(request.metadata(), block),
        Err(SemanticHookError::Unavailable)
    );
}
