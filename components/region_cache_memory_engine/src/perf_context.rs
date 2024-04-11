// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
thread_local! {
    pub static PERF_CONTEXT: RefCell<PerfContext> = RefCell::new(PerfContext::default());
}

// A thread local context for gathering performance counter efficiently
// and transparently.
#[derive(Default)]
pub struct PerfContext {
    // Comment from `RocksDB`
    // total number of internal keys skipped over during iteration.
    // There are several reasons for it:
    // 1. when calling Next(), the iterator is in the position of the previous key, so that we'll
    //    need to skip it. It means this counter will always be incremented in Next().
    // 2. when calling Next(), we need to skip internal entries for the previous keys that are
    //    overwritten.
    // 3. when calling Next(), Seek() or SeekToFirst(), after previous key before calling Next(),
    //    the seek key in Seek() or the beginning for SeekToFirst(), there may be one or more
    //    deleted keys before the next valid key that the operation should place the iterator to.
    //    We need to skip both of the tombstone and updates hidden by the tombstones. The
    //    tombstones are not included in this counter, while previous updates hidden by the
    //    tombstones will be included here.
    // 4. symmetric cases for Prev() and SeekToLast()
    // internal_recent_skipped_count is not included in this counter.
    pub(crate) internal_key_skipped_count: usize,
    // Total number of deletes skipped over during iteration. There may be one or more deleted keys
    // before the next valid key but every deleted key is counted once.
    pub(crate) internal_delete_skipped_count: usize,
}

#[macro_export]
macro_rules! perf_counter_add {
    ($metric:ident, $value:expr) => {
        PERF_CONTEXT.with(|perf_context| {
            perf_context.borrow_mut().$metric += 1;
        });
    };
}
