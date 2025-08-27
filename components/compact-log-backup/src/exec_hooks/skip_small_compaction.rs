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
}

impl ExecHooks for SkipSmallCompaction {
    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        let size_threshold = if util::is_write_cf(cx.subc.cf) {
            self.size_threshold_write
        } else {
            self.size_threshold_default
        };
        if cx.subc.size < size_threshold {
            let mut hs = [0; 6];
            for input in &cx.subc.inputs {
                if input.key_value_size < 128 {
                    hs[0] += 1;
                } else if input.key_value_size < 512 {
                    hs[1] += 1;
                } else if input.key_value_size < 2048 {
                    hs[2] += 1;
                } else if input.key_value_size < 32768 {
                    hs[3] += 1;
                } else if input.key_value_size < 524288 {
                    hs[4] += 1;
                } else {
                    hs[5] += 1;
                }
            }
            info!("Skipped a small compaction.";
                "h1" => hs[0],
                "h2" => hs[1],
                "h3" => hs[2],
                "h4" => hs[3],
                "h5" => hs[4],
                "h6" => hs[5],
                "size" => cx.subc.size,
                "threshold" => size_threshold);
            cx.skip();
        }
    }
}
