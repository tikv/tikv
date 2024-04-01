// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
thread_local! {
    pub static PERF_CONTEXT: RefCell<PerfContext> = RefCell::new(PerfContext::default());
}

// A thread local context for gathering performance counter efficiently
// and transparently.
#[derive(Default)]
pub struct PerfContext {
    // Total number of deletes skipped over during iteration
    pub(crate) internal_delete_skipped_count: usize,
}
