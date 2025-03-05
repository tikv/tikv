// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::info;

use crate::execute::hooking::{CId, ExecHooks, SubcompactionStartCtx};

/// A hook that skips small compaction.
///
/// The size threshold is compared with "original KV size":
/// the length of each key value pairs in its original form.
pub struct SkipSmallCompaction {
    size_threshold: u64,
}

impl SkipSmallCompaction {
    /// Creates a new `SkipSmallCompaction` hook.
    pub fn new(size_threshold: u64) -> Self {
        Self { size_threshold }
    }
}

impl ExecHooks for SkipSmallCompaction {
    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        if cx.subc.size < self.size_threshold {
            info!("Skipped a small compaction."; 
                "size" => cx.subc.size, "threshold" => self.size_threshold);
            cx.skip();
        }
    }
}
