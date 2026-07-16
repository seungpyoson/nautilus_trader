// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use nautilus_polymarket::semantic::{
    AssociatedTradeStatus, CURRENT_V2_UNAVAILABLE, CollectorError, CollectorKind, CollectorPlan,
    CurrentV2Capabilities, DiagnosticClass, ExactOrderStatus, FinalizedBlockRef, PostOrderStatus,
    PreDispatchHook, PreSendHook, RedactedMetadata, SemanticCredential, SemanticDiagnostic,
    SemanticHookError, SemanticLimitError, SemanticLimitKind, SemanticLimitValues, SemanticLimits,
    SemanticRoute, SensitiveProviderBytes, SensitiveSignedRequest, decode_associated_trades,
    decode_exact_order, decode_post_order,
};
use rstest::rstest;

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

#[rstest]
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

#[rstest]
fn transaction_hash_plan_accepts_capacity_minus_one_and_capacity() {
    let limits = limits(96);

    assert!(CollectorPlan::bounded(CollectorKind::TransactionHashes, 95, 95 * 32, limits).is_ok());
    assert!(CollectorPlan::bounded(CollectorKind::TransactionHashes, 96, 96 * 32, limits).is_ok());
}

#[rstest]
fn transaction_hash_plan_rejects_capacity_plus_one() {
    let error = CollectorPlan::bounded(CollectorKind::TransactionHashes, 97, 96 * 32, limits(96))
        .unwrap_err();

    assert_eq!(error, CollectorError::ItemCapacity);
}

#[rstest]
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

#[rstest]
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

#[rstest]
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

#[rstest]
fn bounded_collector_checks_bytes_before_retaining_item() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();

    collector.try_push("first", 4).unwrap();
    assert_eq!(collector.len(), 1);
    assert_eq!(
        collector.try_push("rejected", 1),
        Err(CollectorError::ByteCapacity)
    );
    assert_eq!(collector.len(), 1);
}

#[rstest]
fn bounded_collector_checks_items_before_retaining_item() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 1, 4, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();

    collector.try_push("first", 1).unwrap();
    assert_eq!(
        collector.try_push("rejected", 1),
        Err(CollectorError::ItemCapacity)
    );
    assert_eq!(collector.len(), 1);
}

#[rstest]
fn exact_collector_rejects_incomplete_item_count() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();
    collector.try_push(1_u8, 2).unwrap();

    assert_eq!(collector.finish(), Err(CollectorError::Incomplete));
}

#[rstest]
fn exact_collector_rejects_contradictory_byte_total() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();
    collector.try_push(1_u8, 1).unwrap();
    collector.try_push(2_u8, 2).unwrap();

    assert_eq!(collector.finish(), Err(CollectorError::Contradictory));
}

#[rstest]
fn exact_collector_finishes_only_at_declared_totals() {
    let plan = CollectorPlan::exact(CollectorKind::ResponseItems, 2, 4, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();
    collector.try_push(1_u8, 2).unwrap();
    collector.try_push(2_u8, 2).unwrap();

    assert_eq!(collector.finish().unwrap(), vec![1, 2]);
}

#[rstest]
fn collector_rejects_byte_addition_overflow() {
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, 2, 4096, limits(96)).unwrap();
    let mut collector = plan.allocate().unwrap();
    collector.try_push(1_u8, 1).unwrap();

    assert_eq!(
        collector.try_push(2_u8, usize::MAX),
        Err(CollectorError::ArithmeticOverflow)
    );
    assert_eq!(collector.len(), 1);
}

#[rstest]
fn collector_reports_unrepresentable_allocation_capacity() {
    let values = SemanticLimitValues {
        request_body_bytes: usize::MAX,
        request_items: usize::MAX,
        response_body_bytes: usize::MAX,
        response_items: usize::MAX,
        transaction_hashes: usize::MAX,
        trade_ids: usize::MAX,
        associated_trades: usize::MAX,
        string_bytes: usize::MAX,
        decimal_bytes: usize::MAX,
        log_items: usize::MAX,
    };
    let limits = SemanticLimits::checked(values).unwrap();
    let plan = CollectorPlan::bounded(CollectorKind::ResponseItems, usize::MAX, usize::MAX, limits)
        .unwrap();

    assert_eq!(
        plan.allocate::<u16>().unwrap_err(),
        CollectorError::AllocationCapacity
    );
}

#[rstest]
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

#[rstest]
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
        request: &RedactedMetadata,
        block: FinalizedBlockRef,
    ) -> Result<(), SemanticHookError> {
        assert_eq!(request.route(), SemanticRoute::PostOrder);
        assert_eq!(block.number(), 42);
        Err(SemanticHookError::Unavailable)
    }
}

#[rstest]
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

fn provider_bytes(
    route: SemanticRoute,
    json: &str,
    limits: SemanticLimits,
) -> SensitiveProviderBytes {
    SensitiveProviderBytes::checked(route, json.as_bytes(), limits).unwrap()
}

fn decode_error<T>(result: Result<T, SemanticDiagnostic>) -> SemanticDiagnostic {
    match result {
        Ok(_) => panic!("fixture unexpectedly decoded"),
        Err(diagnostic) => diagnostic,
    }
}

fn post_json(status: &str, hashes: &str, order_id: &str, making: &str) -> String {
    format!(
        r#"{{"success":true,"errorMsg":"","orderID":"{order_id}","transactionsHashes":{hashes},"tradeIDs":["trade-1"],"status":"{status}","takingAmount":"1.25","makingAmount":"{making}"}}"#
    )
}

fn exact_json(status: &str, id: &str) -> String {
    format!(
        r#"{{"associate_trades":["trade-1"],"id":"{id}","status":"{status}","owner":"owner","maker_address":"maker","market":"market","asset_id":"asset","side":"BUY","original_size":"2.0","size_matched":"1.0","price":"0.5","outcome":"YES","created_at":42,"expiration":"0","order_type":"GTC"}}"#
    )
}

fn trade_json(status: &str, id: &str, size: &str) -> String {
    format!(
        r#"{{"id":"{id}","taker_order_id":"order-1","market":"market","asset_id":"asset","side":"BUY","size":"{size}","fee_rate_bps":"0","price":"0.5","status":"{status}","match_time":"1","last_update":"2","outcome":"YES","bucket_index":0,"owner":"owner","maker_address":"maker","maker_orders":[],"transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000000","trader_side":"TAKER"}}"#
    )
}

#[rstest]
fn post_decoder_accepts_every_generated_status() {
    let hash = r#"["0x0000000000000000000000000000000000000000000000000000000000000000"]"#;
    for wire in ["live", "matched", "delayed", "unmatched"] {
        let json = post_json(wire, hash, "order-1", "2.5");
        let observation =
            decode_post_order(provider_bytes(SemanticRoute::PostOrder, &json, limits(96))).unwrap();
        assert_eq!(
            observation.status(),
            PostOrderStatus::try_from(wire).unwrap()
        );
    }
}

#[rstest]
fn exact_order_decoder_accepts_every_generated_status_and_matching_id() {
    for wire in [
        "ORDER_STATUS_LIVE",
        "ORDER_STATUS_INVALID",
        "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
        "ORDER_STATUS_CANCELED",
        "ORDER_STATUS_MATCHED",
    ] {
        let json = exact_json(wire, "order-1");
        let observation = decode_exact_order(
            provider_bytes(SemanticRoute::GetExactOrder, &json, limits(96)),
            "order-1",
        )
        .unwrap();
        assert_eq!(
            observation.status(),
            ExactOrderStatus::try_from(wire).unwrap()
        );
    }
}

#[rstest]
fn associated_trade_decoder_accepts_every_generated_status() {
    for wire in ["MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"] {
        let json = format!("[{}]", trade_json(wire, "trade-1", "1.0"));
        let observation = decode_associated_trades(
            provider_bytes(SemanticRoute::GetAssociatedTrades, &json, limits(96)),
            "order-1",
        )
        .unwrap();
        assert_eq!(
            observation.trades()[0].status(),
            AssociatedTradeStatus::try_from(wire).unwrap()
        );
    }
}

#[rstest]
fn route_decoders_reject_unknown_cross_route_and_malformed_data_safely() {
    let hash = r#"["0x0000000000000000000000000000000000000000000000000000000000000000"]"#;
    for json in [
        post_json("ORDER_STATUS_LIVE", hash, "order-1", "2.5"),
        post_json("unknown", hash, "order-1", "2.5"),
        post_json("live", hash, "order-1", "not-a-decimal"),
        post_json("live", r#"["0x00"]"#, "order-1", "2.5"),
    ] {
        let diagnostic = decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &json,
            limits(96),
        )));
        assert_eq!(diagnostic.route(), SemanticRoute::PostOrder);
        assert!(!format!("{diagnostic:?}").contains("order-1"));
    }

    let malformed = provider_bytes(SemanticRoute::PostOrder, "{", limits(96));
    assert_eq!(
        decode_error(decode_post_order(malformed)).class(),
        DiagnosticClass::Malformed
    );
}

#[rstest]
fn route_decoders_reject_incomplete_extra_oversized_and_contradictory_data() {
    let missing =
        r#"{"success":true,"errorMsg":"","status":"live","takingAmount":"1","makingAmount":"1"}"#;
    let extra = r#"{"success":true,"errorMsg":"","orderID":"order-1","transactionsHashes":[],"tradeIDs":[],"status":"live","takingAmount":"1","makingAmount":"1","extra":true}"#;
    for json in [missing.to_string(), extra.to_string()] {
        assert!(
            decode_post_order(provider_bytes(SemanticRoute::PostOrder, &json, limits(96))).is_err()
        );
    }

    let duplicate_hashes = r#"["0x0000000000000000000000000000000000000000000000000000000000000000","0x0000000000000000000000000000000000000000000000000000000000000000"]"#;
    let duplicate = post_json("live", duplicate_hashes, "order-1", "2.5");
    assert_eq!(
        decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &duplicate,
            limits(96),
        )))
        .class(),
        DiagnosticClass::Contradictory
    );

    let three_hashes = r#"["0x0000000000000000000000000000000000000000000000000000000000000000","0x0100000000000000000000000000000000000000000000000000000000000000","0x0200000000000000000000000000000000000000000000000000000000000000"]"#;
    let oversized = post_json("live", three_hashes, "order-1", "2.5");
    assert_eq!(
        decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &oversized,
            limits(2),
        )))
        .class(),
        DiagnosticClass::Oversized
    );

    let mismatch = exact_json("ORDER_STATUS_LIVE", "other-order");
    assert_eq!(
        decode_error(decode_exact_order(
            provider_bytes(SemanticRoute::GetExactOrder, &mismatch, limits(96)),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Contradictory
    );

    let conflicting = format!(
        "[{},{}]",
        trade_json("MATCHED", "trade-1", "1.0"),
        trade_json("MATCHED", "trade-1", "2.0")
    );
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(SemanticRoute::GetAssociatedTrades, &conflicting, limits(96),),
            "order-1"
        ))
        .class(),
        DiagnosticClass::Contradictory
    );
}

#[rstest]
fn every_decoder_rejects_wrong_route_unknown_status_and_scalar_cap_plus_one() {
    let post = post_json("live", "[]", "order-1", "1");
    let wrong_route = provider_bytes(SemanticRoute::PostOrder, &post, limits(96));
    assert_eq!(
        decode_error(decode_exact_order(wrong_route, "order-1")).class(),
        DiagnosticClass::WrongRoute
    );

    let exact_unknown = exact_json("MATCHED", "order-1");
    assert_eq!(
        decode_error(decode_exact_order(
            provider_bytes(SemanticRoute::GetExactOrder, &exact_unknown, limits(96),),
            "order-1",
        ))
        .class(),
        DiagnosticClass::UnknownStatus
    );

    let trade_unknown = format!("[{}]", trade_json("ORDER_STATUS_LIVE", "trade-1", "1"));
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &trade_unknown,
                limits(96),
            ),
            "order-1"
        ))
        .class(),
        DiagnosticClass::UnknownStatus
    );

    let oversized_id = "x".repeat(257);
    let oversized_string = post_json("live", "[]", &oversized_id, "1");
    assert_eq!(
        decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &oversized_string,
            limits(96),
        )))
        .class(),
        DiagnosticClass::Oversized
    );

    let oversized_decimal = "1".repeat(129);
    let oversized_number = post_json("live", "[]", "order-1", &oversized_decimal);
    assert_eq!(
        decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &oversized_number,
            limits(96),
        )))
        .class(),
        DiagnosticClass::Oversized
    );

    for exact_bad_value in [
        exact_json("ORDER_STATUS_LIVE", "order-1")
            .replace(r#""side":"BUY""#, r#""side":"SIDEWAYS""#),
        exact_json("ORDER_STATUS_LIVE", "order-1")
            .replace(r#""order_type":"GTC""#, r#""order_type":"BOGUS""#),
    ] {
        assert_eq!(
            decode_error(decode_exact_order(
                provider_bytes(SemanticRoute::GetExactOrder, &exact_bad_value, limits(96),),
                "order-1",
            ))
            .class(),
            DiagnosticClass::UnknownStatus
        );
    }

    let unrelated_trade = format!(
        "[{}]",
        trade_json("MATCHED", "trade-1", "1").replace("order-1", "order-2")
    );
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &unrelated_trade,
                limits(96),
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Contradictory
    );

    for trade_bad_value in [
        trade_json("MATCHED", "trade-1", "1")
            .replace(r#""trader_side":"TAKER""#, r#""trader_side":"BOGUS""#),
        trade_json("MATCHED", "trade-1", "1").replace(r#""side":"BUY""#, r#""side":"SIDEWAYS""#),
    ] {
        let body = format!("[{trade_bad_value}]");
        assert_eq!(
            decode_error(decode_associated_trades(
                provider_bytes(SemanticRoute::GetAssociatedTrades, &body, limits(96)),
                "order-1",
            ))
            .class(),
            DiagnosticClass::UnknownStatus
        );
    }

    let maker = r#"{"order_id":"order-1","owner":"owner","maker_address":"maker","matched_amount":"1","price":"0.5","fee_rate_bps":"0","asset_id":"asset","outcome":"YES","side":"BUY"}"#;
    let maker_associated = trade_json("MATCHED", "trade-1", "1")
        .replace("order-1", "order-2")
        .replace(r#""trader_side":"TAKER""#, r#""trader_side":"MAKER""#)
        .replace(
            r#""maker_orders":[]"#,
            &format!(r#""maker_orders":[{maker}]"#),
        );
    let body = format!("[{maker_associated}]");
    assert!(
        decode_associated_trades(
            provider_bytes(SemanticRoute::GetAssociatedTrades, &body, limits(96)),
            "order-1",
        )
        .is_ok()
    );

    let crossed_taker =
        maker_associated.replace(r#""trader_side":"MAKER""#, r#""trader_side":"TAKER""#);
    let crossed_taker = format!("[{crossed_taker}]");
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &crossed_taker,
                limits(96),
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Contradictory
    );

    let crossed_maker = trade_json("MATCHED", "trade-1", "1")
        .replace(r#""trader_side":"TAKER""#, r#""trader_side":"MAKER""#);
    let crossed_maker = format!("[{crossed_maker}]");
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &crossed_maker,
                limits(96),
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Contradictory
    );

    let invalid_maker = maker.replace(r#""side":"BUY""#, r#""side":"SIDEWAYS""#);
    let invalid_maker_side = trade_json("MATCHED", "trade-1", "1")
        .replace("order-1", "order-2")
        .replace(
            r#""maker_orders":[]"#,
            &format!(r#""maker_orders":[{invalid_maker}]"#),
        );
    let invalid_maker_side = format!("[{invalid_maker_side}]");
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &invalid_maker_side,
                limits(96),
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::UnknownStatus
    );
}

#[rstest]
fn exact_and_trade_decoders_reject_route_specific_negative_matrix() {
    let exact = exact_json("ORDER_STATUS_LIVE", "order-1");
    let oversized_string = "x".repeat(257);
    let oversized_decimal = "1".repeat(129);
    let oversized_associated_trades = (0..9)
        .map(|index| format!(r#""trade-{index}""#))
        .collect::<Vec<_>>()
        .join(",");
    let exact_cases = [
        exact.replace(r#""owner":"owner","#, ""),
        exact.replace("}", r#","extra":true}"#),
        exact.replace(r#""price":"0.5""#, r#""price":"not-decimal""#),
        exact.replace(
            r#""owner":"owner""#,
            &format!(r#""owner":"{oversized_string}""#),
        ),
        exact.replace(
            r#""original_size":"2.0""#,
            &format!(r#""original_size":"{oversized_decimal}""#),
        ),
        exact.replace(
            r#"["trade-1"]"#,
            &format!("[{oversized_associated_trades}]"),
        ),
    ];

    for json in exact_cases {
        assert!(
            decode_exact_order(
                provider_bytes(SemanticRoute::GetExactOrder, &json, limits(96)),
                "order-1",
            )
            .is_err()
        );
    }
    assert!(
        decode_exact_order(
            provider_bytes(SemanticRoute::GetExactOrder, "{", limits(96)),
            "order-1",
        )
        .is_err()
    );

    let trade = trade_json("MATCHED", "trade-1", "1");
    let trade_cases = [
        trade.replace(r#""owner":"owner","#, ""),
        trade.replace("}", r#","extra":true}"#),
        trade.replace(r#""size":"1""#, r#""size":"not-decimal""#),
        trade.replace(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x00",
        ),
    ];

    for json in trade_cases {
        let body = format!("[{json}]");
        assert!(
            decode_associated_trades(
                provider_bytes(SemanticRoute::GetAssociatedTrades, &body, limits(96)),
                "order-1",
            )
            .is_err()
        );
    }

    let valid_trade_body = format!("[{}]", trade_json("MATCHED", "trade-1", "1"));
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(SemanticRoute::PostOrder, &valid_trade_body, limits(96)),
            "order-1",
        ))
        .class(),
        DiagnosticClass::WrongRoute
    );

    for oversized_trade in [
        trade_json("MATCHED", "trade-1", "1").replace(
            r#""owner":"owner""#,
            &format!(r#""owner":"{oversized_string}""#),
        ),
        trade_json("MATCHED", "trade-1", "1")
            .replace(r#""size":"1""#, &format!(r#""size":"{oversized_decimal}""#)),
    ] {
        let body = format!("[{oversized_trade}]");
        assert_eq!(
            decode_error(decode_associated_trades(
                provider_bytes(SemanticRoute::GetAssociatedTrades, &body, limits(96)),
                "order-1",
            ))
            .class(),
            DiagnosticClass::Oversized
        );
    }

    assert!(
        decode_associated_trades(
            provider_bytes(SemanticRoute::GetAssociatedTrades, "[", limits(96)),
            "order-1",
        )
        .is_err()
    );

    let oversized = (0..9)
        .map(|index| trade_json("MATCHED", &format!("trade-{index}"), "1"))
        .collect::<Vec<_>>()
        .join(",");
    let mut values = limit_values(96);
    values.response_body_bytes = 16_384;
    let expanded_body_limit = SemanticLimits::checked(values).unwrap();
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &format!("[{oversized}]"),
                expanded_body_limit,
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Oversized
    );
}

#[rstest]
fn decoders_reject_source_bound_numeric_contradictions() {
    let negative_post = post_json("live", "[]", "order-1", "1")
        .replace(r#""takingAmount":"1.25""#, r#""takingAmount":"-1.25""#);
    assert_eq!(
        decode_error(decode_post_order(provider_bytes(
            SemanticRoute::PostOrder,
            &negative_post,
            limits(96),
        )))
        .class(),
        DiagnosticClass::Contradictory
    );

    for exact in [
        exact_json("ORDER_STATUS_LIVE", "order-1").replace(r#""price":"0.5""#, r#""price":"-0.5""#),
        exact_json("ORDER_STATUS_LIVE", "order-1")
            .replace(r#""size_matched":"1.0""#, r#""size_matched":"3.0""#),
    ] {
        assert_eq!(
            decode_error(decode_exact_order(
                provider_bytes(SemanticRoute::GetExactOrder, &exact, limits(96)),
                "order-1",
            ))
            .class(),
            DiagnosticClass::Contradictory
        );
    }

    let negative_trade = format!("[{}]", trade_json("MATCHED", "trade-1", "-1"));
    assert_eq!(
        decode_error(decode_associated_trades(
            provider_bytes(
                SemanticRoute::GetAssociatedTrades,
                &negative_trade,
                limits(96),
            ),
            "order-1",
        ))
        .class(),
        DiagnosticClass::Contradictory
    );
}

#[rstest]
fn current_v2_capabilities_are_all_explicitly_unavailable() {
    let capabilities = CurrentV2Capabilities::current_v2();
    let unavailable = capabilities.unavailable();

    assert_eq!(unavailable.len(), 3);
    assert_eq!(unavailable, &CURRENT_V2_UNAVAILABLE);
}

#[rstest]
fn observations_and_larger_capacity_cannot_authorize_autonomous_entry() {
    let mut above_retired_ceiling = limit_values(96);
    above_retired_ceiling.response_items = 128;
    let _limits = SemanticLimits::checked(above_retired_ceiling).unwrap();
    let non_capability_observations = [
        "cancellation",
        "not-found",
        "elapsed-time",
        "unsigned-expiry",
        "fill-or-kill-text",
        "quiet-chain",
        "sequential-response-hashes",
        "status-observation",
    ];

    for _observation in non_capability_observations {
        let result = CurrentV2Capabilities::current_v2().require_autonomous_entry();
        assert!(result.is_err());
    }
}
