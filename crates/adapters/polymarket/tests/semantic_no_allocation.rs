// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use nautilus_polymarket::semantic::{
    CollectorKind, CollectorPlan, SemanticCredential, SemanticLimitValues, SemanticLimits,
    SemanticRoute, SensitiveProviderBytes, SensitiveSignedRequest, decode_associated_trades,
    decode_post_order,
};
use rstest::rstest;

struct CountingAllocator;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn valid_values() -> SemanticLimitValues {
    SemanticLimitValues {
        request_body_bytes: 128,
        request_items: 8,
        response_body_bytes: 4096,
        response_items: 8,
        transaction_hashes: 96,
        trade_ids: 8,
        associated_trades: 8,
        string_bytes: 256,
        decimal_bytes: 128,
        log_items: 8,
    }
}

#[rstest]
fn rejected_capacity_checks_allocate_nothing() {
    let limits = SemanticLimits::checked(valid_values()).unwrap();
    let mut invalid_values = valid_values();
    invalid_values.response_body_bytes = 0;
    let oversized_request = [0_u8; 129];
    let oversized_response = [0_u8; 4097];
    let oversized_credential = [0_u8; 257];
    let mut one_trade_id_values = valid_values();
    one_trade_id_values.trade_ids = 1;
    let one_trade_id_limits = SemanticLimits::checked(one_trade_id_values).unwrap();
    let post_cap_plus_one = SensitiveProviderBytes::checked(
        SemanticRoute::PostOrder,
        br#"{"success":true,"errorMsg":"","orderID":"order-1","tradeIDs":["one","two"],"status":"live","takingAmount":"1","makingAmount":"1"}"#,
        one_trade_id_limits,
    )
    .unwrap();
    let mut one_trade_values = valid_values();
    one_trade_values.associated_trades = 1;
    let one_trade_limits = SemanticLimits::checked(one_trade_values).unwrap();
    let trade_cap_plus_one = SensitiveProviderBytes::checked(
        SemanticRoute::GetAssociatedTrades,
        br#"[{},{}]"#,
        one_trade_limits,
    )
    .unwrap();
    let huge_limits = SemanticLimits::checked(SemanticLimitValues {
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
    })
    .unwrap();
    let huge_plan = CollectorPlan::bounded(
        CollectorKind::ResponseItems,
        usize::MAX,
        usize::MAX,
        huge_limits,
    )
    .unwrap();

    ALLOCATIONS.store(0, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    let invalid_limits = SemanticLimits::checked(invalid_values);
    let invalid_plan = CollectorPlan::bounded(CollectorKind::TransactionHashes, 97, 4096, limits);
    let invalid_request = SensitiveSignedRequest::checked(
        SemanticRoute::PostOrder,
        oversized_request.as_slice(),
        limits,
    );
    let invalid_response = SensitiveProviderBytes::checked(
        SemanticRoute::GetExactOrder,
        oversized_response.as_slice(),
        limits,
    );
    let invalid_credential = SemanticCredential::checked(
        oversized_credential.as_slice(),
        b"secret",
        b"passphrase",
        limits,
    );
    let invalid_post_array = decode_post_order(post_cap_plus_one);
    let invalid_trade_array = decode_associated_trades(trade_cap_plus_one, "order-1");
    let invalid_allocation = huge_plan.allocate::<u16>();
    ENABLED.store(false, Ordering::SeqCst);

    assert!(invalid_limits.is_err());
    assert!(invalid_plan.is_err());
    assert!(invalid_request.is_err());
    assert!(invalid_response.is_err());
    assert!(invalid_credential.is_err());
    assert!(invalid_post_array.is_err());
    assert!(invalid_trade_array.is_err());
    assert!(invalid_allocation.is_err());
    assert_eq!(ALLOCATIONS.load(Ordering::SeqCst), 0);
}
