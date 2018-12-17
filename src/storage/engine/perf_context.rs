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

use std::ops::{Deref, DerefMut};

use rocksdb::{set_perf_level, IOStatsContext, PerfContext, PerfLevel};
use util::time::Instant;

#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign)]
pub struct PerfStatisticsFields {
    pub internal_key_skipped_count: usize,
    pub internal_delete_skipped_count: usize,
    pub block_cache_hit_count: usize,
    pub block_read_count: usize,
    pub block_read_byte: usize,
}

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This statistics store instant values.
#[derive(Debug, Clone, Copy)]
pub struct PerfStatisticsInstant(pub PerfStatisticsFields);

impl PerfStatisticsInstant {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new() -> Self {
        let perf_context = PerfContext::get();
        PerfStatisticsInstant(PerfStatisticsFields {
            internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            block_read_count: perf_context.block_read_count() as usize,
            block_read_byte: perf_context.block_read_byte() as usize,
        })
    }

    /// Calculate delta values until now.
    pub fn delta(&self) -> PerfStatisticsDelta {
        let now = Self::new();
        PerfStatisticsDelta(now.0 - self.0)
    }
}

impl Deref for PerfStatisticsInstant {
    type Target = PerfStatisticsFields;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PerfStatisticsInstant {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This this statistics store delta values between two instant statistics.
#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign)]
pub struct PerfStatisticsDelta(pub PerfStatisticsFields);

impl Deref for PerfStatisticsDelta {
    type Target = PerfStatisticsFields;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PerfStatisticsDelta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct WriteContextTracker {
    tag: &'static str,
    start: Instant,
    write_wal_time: u64,
    write_memtable_time: u64,
    write_delay_time: u64,
    write_scheduling_flushes_compactions_time: u64,
    write_pre_and_post_process_time: u64,
    write_thread_wait_nanos: u64,
    db_mutex_lock_nanos: u64,
    db_condition_wait_nanos: u64,
    bytes_written: u64,
    open_nanos: u64,
    allocate_nanos: u64,
    write_nanos: u64,
    range_sync_nanos: u64,
    fsync_nanos: u64,
    prepare_write_nanos: u64,
    logger_nanos: u64,
}

impl WriteContextTracker {
    pub fn new(tag: &'static str) -> Self {
        set_perf_level(PerfLevel::EnableTime);
        let perf = PerfContext::get();
        let iostats = IOStatsContext::get();
        Self {
            tag,
            start: Instant::now(),
            write_wal_time: perf.write_wal_time(),
            write_memtable_time: perf.write_memtable_time(),
            write_delay_time: perf.write_delay_time(),
            write_scheduling_flushes_compactions_time: perf
                .write_scheduling_flushes_compactions_time(),
            write_pre_and_post_process_time: perf.write_pre_and_post_process_time(),
            write_thread_wait_nanos: perf.write_thread_wait_nanos(),
            db_mutex_lock_nanos: perf.db_mutex_lock_nanos(),
            db_condition_wait_nanos: perf.db_condition_wait_nanos(),
            bytes_written: iostats.bytes_written(),
            open_nanos: iostats.open_nanos(),
            allocate_nanos: iostats.allocate_nanos(),
            write_nanos: iostats.write_nanos(),
            range_sync_nanos: iostats.range_sync_nanos(),
            fsync_nanos: iostats.fsync_nanos(),
            prepare_write_nanos: iostats.prepare_write_nanos(),
            logger_nanos: iostats.logger_nanos(),
        }
    }
}

impl Drop for WriteContextTracker {
    fn drop(&mut self) {
        let perf = PerfContext::get();
        let iostats = IOStatsContext::get();
        let elapsed = self.start.elapsed_secs();
        if elapsed >= 0.01 {
            warn!(
                "[WriteContext] {} elapsed {} write_wal {} write_memtable {} \
                 write_delay {} write_scheduling_flushes_compactions {} \
                 write_pre_and_post_process {} write_thread_wait_nanos {} \
                 db_mutex_lock {} db_condition_wait {} bytes_written {} \
                 open {} allocate {} write {} range_sync {} fsync {} \
                 prepare_write {} logger {}",
                self.tag,
                elapsed * 1000.0,
                (perf.write_wal_time() - self.write_wal_time) / 1000,
                (perf.write_memtable_time() - self.write_memtable_time) / 1000,
                (perf.write_delay_time() - self.write_delay_time) / 1000,
                (perf.write_scheduling_flushes_compactions_time()
                    - self.write_scheduling_flushes_compactions_time) / 1000,
                (perf.write_pre_and_post_process_time() - self.write_pre_and_post_process_time)
                    / 1000,
                (perf.write_thread_wait_nanos() - self.write_thread_wait_nanos) / 1000,
                (perf.db_mutex_lock_nanos() - self.db_mutex_lock_nanos) / 1000,
                (perf.db_condition_wait_nanos() - self.db_condition_wait_nanos) / 1000,
                (iostats.bytes_written() - self.bytes_written),
                (iostats.open_nanos() - self.open_nanos) / 1000,
                (iostats.allocate_nanos() - self.allocate_nanos) / 1000,
                (iostats.write_nanos() - self.write_nanos) / 1000,
                (iostats.range_sync_nanos() - self.range_sync_nanos) / 1000,
                (iostats.fsync_nanos() - self.fsync_nanos) / 1000,
                (iostats.prepare_write_nanos() - self.prepare_write_nanos) / 1000,
                (iostats.logger_nanos() - self.logger_nanos) / 1000
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_operations() {
        let f1 = PerfStatisticsFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
        };
        let f2 = PerfStatisticsFields {
            internal_key_skipped_count: 2,
            internal_delete_skipped_count: 3,
            block_cache_hit_count: 5,
            block_read_count: 7,
            block_read_byte: 11,
        };
        let f3 = f1 + f2;
        assert_eq!(f3.internal_key_skipped_count, 3);
        assert_eq!(f3.block_cache_hit_count, 8);
        assert_eq!(f3.block_read_byte, 16);

        let mut f3 = f1;
        f3 += f2;
        assert_eq!(f3.internal_key_skipped_count, 3);
        assert_eq!(f3.block_cache_hit_count, 8);
        assert_eq!(f3.block_read_byte, 16);

        let f3 = f2 - f1;
        assert_eq!(f3.internal_key_skipped_count, 1);
        assert_eq!(f3.block_cache_hit_count, 2);
        assert_eq!(f3.block_read_byte, 6);

        let mut f3 = f2;
        f3 -= f1;
        assert_eq!(f3.internal_key_skipped_count, 1);
        assert_eq!(f3.block_cache_hit_count, 2);
        assert_eq!(f3.block_read_byte, 6);
    }

    #[test]
    fn test_deref() {
        let mut stats = PerfStatisticsDelta(PerfStatisticsFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
        });
        assert_eq!(stats.block_cache_hit_count, 3);
        stats.block_cache_hit_count = 6;
        assert_eq!(stats.block_cache_hit_count, 6);
    }
}
