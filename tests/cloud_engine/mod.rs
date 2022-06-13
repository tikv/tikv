// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_patterns)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

use std::sync::atomic::AtomicU16;

mod backup;
mod delete_range;
mod gc;

static NODE_ALLOCATOR: AtomicU16 = AtomicU16::new(0);

pub(crate) fn alloc_node_id() -> u16 {
    NODE_ALLOCATOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}
