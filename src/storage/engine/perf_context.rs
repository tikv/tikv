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

use std::fmt;
use std::marker::PhantomData;

use rocksdb::PerfContext;

// TODO: Can be simplified by const generics once RFC #2000 is landed.

#[derive(Clone, Debug, Copy)]
pub struct TypeDelta;
#[derive(Clone, Debug, Copy)]
pub struct TypeInstant;

pub trait Type: fmt::Debug {}
impl Type for TypeDelta {}
impl Type for TypeInstant {}

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
///
/// Depends on different contexts, it may either store instant values (i.e. absolute values
/// at a specific time) or delta values (i.e. changed values during some time).
#[derive(Clone, Debug, Copy)]
pub struct PerfStatistics<T: Type> {
    _phantom: PhantomData<T>,
    pub internal_key_skipped_count: usize,
    pub internal_delete_skipped_count: usize,
    pub block_cache_hit_count: usize,
    pub block_read_count: usize,
    pub block_read_byte: usize,
}

/// Indicate that this statistics store delta values between two instant statistics.
pub type PerfStatisticsDelta = PerfStatistics<TypeDelta>;

/// Indicate that this statistics store instant values.
pub type PerfStatisticsInstant = PerfStatistics<TypeInstant>;

impl PerfStatisticsInstant {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new() -> Self {
        let perf_context = PerfContext::get();
        Self {
            _phantom: Default::default(),
            internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            block_read_count: perf_context.block_read_count() as usize,
            block_read_byte: perf_context.block_read_byte() as usize,
        }
    }

    /// Calculates delta values.
    pub fn delta_between(&self, other: &Self) -> PerfStatisticsDelta {
        PerfStatisticsDelta {
            _phantom: Default::default(),
            internal_key_skipped_count: other.internal_key_skipped_count
                - self.internal_key_skipped_count,
            internal_delete_skipped_count: other.internal_delete_skipped_count
                - self.internal_delete_skipped_count,
            block_cache_hit_count: other.block_cache_hit_count - self.block_cache_hit_count,
            block_read_count: other.block_read_count - self.block_read_count,
            block_read_byte: other.block_read_byte - self.block_read_byte,
        }
    }

    /// Calculate delta values until now.
    pub fn delta(&self) -> PerfStatisticsDelta {
        let now = Self::new();
        self.delta_between(&now)
    }
}

impl Default for PerfStatisticsDelta {
    fn default() -> Self {
        Self {
            _phantom: Default::default(),
            internal_key_skipped_count: 0,
            internal_delete_skipped_count: 0,
            block_cache_hit_count: 0,
            block_read_count: 0,
            block_read_byte: 0,
        }
    }
}
