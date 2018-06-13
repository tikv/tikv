// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::PerfContext;

/// Store statistics we need. Data comes from RocksDB's `PerfContext` via `PerfContextCollector`.
/// Depends on different contexts, it may either store absolute values (i.e. statistics since
/// thread creation), or relative values (i.e. statistics delta).
#[derive(Default, Clone, Debug, Copy)]
pub struct PerfStatistics {
    /// Whether values stored in this instance is absolute or not (i.e. relative).
    /// By default, `PerfStatistics` stores (relative) 0 values.
    /// TODO: Use const generics (RFC #2000) once it is available to make it safe in compile time
    /// instead of runtime.
    absolute: bool,

    pub internal_key_skipped_count: usize,
    pub internal_delete_skipped_count: usize,
    pub block_cache_hit_count: usize,
    pub block_read_count: usize,
    pub block_read_byte: usize,
}

impl PerfStatistics {
    /// Add statistics of another instance into the current one. This operation is valid
    /// only if both instances store relative values.
    pub fn add(&mut self, other: &Self) {
        assert!(!self.absolute);
        assert!(!other.absolute);
        self.internal_key_skipped_count = self.internal_key_skipped_count
            .saturating_add(other.internal_key_skipped_count);
        self.internal_delete_skipped_count = self.internal_delete_skipped_count
            .saturating_add(other.internal_delete_skipped_count);
        self.block_cache_hit_count = self.block_cache_hit_count
            .saturating_add(other.block_cache_hit_count);
        self.block_read_count = self.block_read_count.saturating_add(other.block_read_count);
        self.block_read_byte = self.block_read_byte.saturating_add(other.block_read_byte);
    }

    /// Calculates delta statistics from two instances storing absolute values. `other`
    /// must contain statistics later then `self`, otherwise the behavior is undefined.
    fn get_delta(&self, other: &Self) -> PerfStatistics {
        assert!(self.absolute);
        assert!(other.absolute);
        PerfStatistics {
            absolute: false,
            internal_key_skipped_count: other.internal_key_skipped_count
                - self.internal_key_skipped_count,
            internal_delete_skipped_count: other.internal_delete_skipped_count
                - self.internal_delete_skipped_count,
            block_cache_hit_count: other.block_cache_hit_count - self.block_cache_hit_count,
            block_read_count: other.block_read_count - self.block_read_count,
            block_read_byte: other.block_read_byte - self.block_read_byte,
        }
    }

    /// Create an instance which stores absolute statistics values, retrieved at creation.
    fn new(perf_context: &PerfContext) -> PerfStatistics {
        PerfStatistics {
            absolute: true,
            internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            block_read_count: perf_context.block_read_count() as usize,
            block_read_byte: perf_context.block_read_byte() as usize,
        }
    }

    pub fn is_absolute(&self) -> bool {
        self.absolute
    }
}

/// Helper to collect RocksDB's `PerfContext`. This struct records the statistics at creation,
/// and updates delta to the `collect_target` when it is dropped.
#[must_use = "You must keep the collector live in the scope, otherwise it won't collect anything!"]
pub struct PerfContextCollector<'a> {
    collect_target: &'a mut PerfStatistics,
    perf_context: PerfContext,
    /// Absolute statistics values retrieved at creation.
    perf: PerfStatistics,
}

impl<'a> PerfContextCollector<'a> {
    pub fn new(collect_target: &'a mut PerfStatistics) -> PerfContextCollector<'a> {
        let perf_context = PerfContext::get();
        let perf = PerfStatistics::new(&perf_context);
        PerfContextCollector {
            collect_target,
            perf_context,
            perf,
        }
    }

    /// Get delta statistics since instance creation.
    pub fn collect(&self) -> PerfStatistics {
        let current_perf = PerfStatistics::new(&self.perf_context);
        self.perf.get_delta(&current_perf)
    }
}

impl<'a> Drop for PerfContextCollector<'a> {
    fn drop(&mut self) {
        let statistics = self.collect();
        self.collect_target.add(&statistics);
    }
}

/// RocksDB's `PerfContext` is thread local. If we send `PerfContextCollector` to another thread,
/// we will get incorrect delta values, so it is `!Send` and `!Sync`.
impl<'a> !Send for PerfContextCollector<'a> {}

impl<'a> !Sync for PerfContextCollector<'a> {}

/// A wrapper of `PerfContextCollector` to deal with optional targets. If the collecting target
/// is `None`, there will be no overheads.
pub enum OptionTargetPerfContextCollector<'a> {
    SomeTarget(PerfContextCollector<'a>),
    EmptyTarget,
}

impl<'a> OptionTargetPerfContextCollector<'a> {
    pub fn new(
        collect_target: Option<&'a mut PerfStatistics>,
    ) -> OptionTargetPerfContextCollector<'a> {
        match collect_target {
            Some(collect_target) => OptionTargetPerfContextCollector::SomeTarget(
                PerfContextCollector::new(collect_target),
            ),
            None => OptionTargetPerfContextCollector::EmptyTarget,
        }
    }
}
