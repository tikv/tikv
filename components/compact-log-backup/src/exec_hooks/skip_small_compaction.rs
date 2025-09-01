// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::info;

use crate::{
    execute::hooking::{CId, ExecHooks, SubcompactionStartCtx},
    util,
};

/// A hook that skips small compaction.
///
/// The size threshold is compared with "original KV size":
/// the length of each key value pairs in its original form.
pub struct SkipSmallCompaction {
    size_threshold_default: u64,
    size_threshold_write: u64,
}

impl SkipSmallCompaction {
    /// Creates a new `SkipSmallCompaction` hook.
    pub fn new(size_threshold_default: u64, size_threshold_write: u64) -> Self {
        Self {
            size_threshold_default,
            size_threshold_write,
        }
    }

    fn get_threshold_by_cf(&self, cf: &str) -> u64 {
        if util::is_write_cf(cf) {
            return self.size_threshold_write;
        }
        self.size_threshold_default
    }
}

impl ExecHooks for SkipSmallCompaction {
    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        let size_threshold = self.get_threshold_by_cf(cx.subc.cf);
        if cx.subc.size < size_threshold {
            info!("Skipped a small compaction.";
                "size" => cx.subc.size, "threshold" => size_threshold);
            cx.skip();
        }
    }
}
