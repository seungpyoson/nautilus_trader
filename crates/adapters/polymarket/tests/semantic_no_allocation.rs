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
    CollectorKind, CollectorPlan, SemanticLimitValues, SemanticLimits,
};

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

#[test]
fn rejected_capacity_checks_allocate_nothing() {
    let limits = SemanticLimits::checked(valid_values()).unwrap();
    let mut invalid_values = valid_values();
    invalid_values.response_body_bytes = 0;

    ALLOCATIONS.store(0, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    let invalid_limits = SemanticLimits::checked(invalid_values);
    let invalid_plan = CollectorPlan::bounded(CollectorKind::TransactionHashes, 97, 4096, limits);
    ENABLED.store(false, Ordering::SeqCst);

    assert!(invalid_limits.is_err());
    assert!(invalid_plan.is_err());
    assert_eq!(ALLOCATIONS.load(Ordering::SeqCst), 0);
}
