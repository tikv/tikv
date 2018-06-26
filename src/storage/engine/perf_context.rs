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

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
///
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
        self.internal_key_skipped_count = self
            .internal_key_skipped_count
            .saturating_add(other.internal_key_skipped_count);
        self.internal_delete_skipped_count = self
            .internal_delete_skipped_count
            .saturating_add(other.internal_delete_skipped_count);
        self.block_cache_hit_count = self
            .block_cache_hit_count
            .saturating_add(other.block_cache_hit_count);
        self.block_read_count = self.block_read_count.saturating_add(other.block_read_count);
        self.block_read_byte = self.block_read_byte.saturating_add(other.block_read_byte);
    }

    /// Calculates delta statistics.
    pub fn delta(&self) -> Self {
        assert!(self.absolute);
        let other = Self::new();
        assert!(other.absolute);
        Self {
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
    pub fn new() -> Self {
        let perf_context = PerfContext::get();
        Self {
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
