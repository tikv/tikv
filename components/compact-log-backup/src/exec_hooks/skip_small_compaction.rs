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
    old_size_threshold_default: u64,
    old_size_threshold_write: u64,
    compaction_until_ts: u64,
}

impl SkipSmallCompaction {
    /// Creates a new `SkipSmallCompaction` hook.
    pub fn new(
        size_threshold_default: u64,
        size_threshold_write: u64,
        old_size_threshold_default: u64,
        old_size_threshold_write: u64,
        compaction_until_ts: u64,
    ) -> Self {
        Self {
            size_threshold_default,
            size_threshold_write,
            old_size_threshold_default,
            old_size_threshold_write,
            compaction_until_ts,
        }
    }
}

impl ExecHooks for SkipSmallCompaction {
    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        let (size_threshold, old_size_threshold) = if util::is_write_cf(cx.subc.cf) {
            (self.size_threshold_write, self.old_size_threshold_write)
        } else {
            (self.size_threshold_default, self.old_size_threshold_default)
        };
        let buckets: [u64; 17] = [
            32,
            64,
            128,
            256,
            512,
            1024,
            2048,
            4096,
            8192,
            16384,
            32768,
            65536,
            131072,
            262144,
            524288,
            1048576,
            107374182400,
        ];
        if cx.subc.size < size_threshold {
            const DAY_2_OLDER: u64 = 46000000000000;
            if self
                .compaction_until_ts
                .saturating_sub(cx.subc.input_max_ts)
                > DAY_2_OLDER
                && cx.subc.size < old_size_threshold
            {
                info!("No skipped the old compaction.";
                    "size" => cx.subc.size,
                    "threshold" => old_size_threshold,
                );
            } else {
                const HEADER_SIZE_PER_ENTRY: u64 = std::mem::size_of::<u32>() as u64 * 2;
                let mut hs = [0; 17];
                for input in &cx.subc.inputs {
                    let file_real_size =
                        input.key_value_size + HEADER_SIZE_PER_ENTRY * input.num_of_entries;
                    for (i, bucket_size) in buckets.iter().enumerate() {
                        if file_real_size < *bucket_size {
                            hs[i] += 1;
                            break;
                        }
                    }
                }
                info!("Skipped a small compaction.";
                   "hs" => hs.map(|v| v.to_string()).join(","),
                    "size" => cx.subc.size,
                    "threshold" => size_threshold,
                );
                cx.skip();
            }
        }
    }
}
