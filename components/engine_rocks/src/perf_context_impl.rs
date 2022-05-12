// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, marker::PhantomData, ops::Sub};

use derive_more::{Add, AddAssign, Sub, SubAssign};
use engine_traits::{PerfContextKind, PerfLevel};
use kvproto::kvrpcpb::ScanDetailV2;
use lazy_static::lazy_static;
use slog_derive::KV;

use crate::{
    perf_context_metrics::{
        APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC, STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC,
    },
    raw_util, set_perf_flags, set_perf_level, PerfContext as RawPerfContext, PerfFlag, PerfFlags,
};

macro_rules! report_write_perf_context {
    ($ctx: expr, $metric: ident) => {
        if $ctx.perf_level != PerfLevel::Disable {
            $ctx.write = WritePerfContext::capture();
            observe_perf_context_type!($ctx, $metric, write_wal_time);
            observe_perf_context_type!($ctx, $metric, write_memtable_time);
            observe_perf_context_type!($ctx, $metric, db_mutex_lock_nanos);
            observe_perf_context_type!($ctx, $metric, pre_and_post_process);
            observe_perf_context_type!($ctx, $metric, write_thread_wait);
            observe_perf_context_type!($ctx, $metric, write_scheduling_flushes_compactions_time);
            observe_perf_context_type!($ctx, $metric, db_condition_wait_nanos);
            observe_perf_context_type!($ctx, $metric, write_delay_time);
        }
    };
}

macro_rules! observe_perf_context_type {
    ($s:expr, $metric: expr, $v:ident) => {
        $metric.$v.observe(($s.write.$v) as f64 / 1e9);
    };
}

lazy_static! {
    /// Default perf flags for a write operation.
    static ref DEFAULT_WRITE_PERF_FLAGS: PerfFlags = PerfFlag::WriteWalTime
        | PerfFlag::WritePreAndPostProcessTime
        | PerfFlag::WriteMemtableTime
        | PerfFlag::WriteThreadWaitNanos
        | PerfFlag::DbMutexLockNanos
        | PerfFlag::WriteSchedulingFlushesCompactionsTime
        | PerfFlag::DbConditionWaitNanos
        | PerfFlag::WriteDelayTime;

    /// Default perf flags for read operations.
    static ref DEFAULT_READ_PERF_FLAGS: PerfFlags = PerfFlag::UserKeyComparisonCount
        | PerfFlag::BlockCacheHitCount
        | PerfFlag::BlockReadCount
        | PerfFlag::BlockReadByte
        | PerfFlag::BlockReadTime
        | PerfFlag::BlockCacheIndexHitCount
        | PerfFlag::IndexBlockReadCount
        | PerfFlag::BlockCacheFilterHitCount
        | PerfFlag::FilterBlockReadCount
        | PerfFlag::CompressionDictBlockReadCount
        | PerfFlag::GetReadBytes
        | PerfFlag::InternalKeySkippedCount
        | PerfFlag::InternalDeleteSkippedCount
        | PerfFlag::InternalRecentSkippedCount
        | PerfFlag::GetSnapshotTime
        | PerfFlag::GetFromMemtableCount
        | PerfFlag::SeekOnMemtableCount
        | PerfFlag::NextOnMemtableCount
        | PerfFlag::PrevOnMemtableCount
        | PerfFlag::SeekChildSeekCount
        | PerfFlag::DbMutexLockNanos
        | PerfFlag::DbConditionWaitNanos
        | PerfFlag::BloomMemtableHitCount
        | PerfFlag::BloomMemtableMissCount
        | PerfFlag::BloomSstHitCount
        | PerfFlag::BloomSstMissCount
        | PerfFlag::UserKeyReturnCount
        | PerfFlag::BlockCacheMissCount
        | PerfFlag::BloomFilterFullPositive
        | PerfFlag::BloomFilterUseful
        | PerfFlag::BloomFilterFullTruePositive
        | PerfFlag::BytesRead;
}

#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign, KV)]
pub struct ReadPerfContext {
    pub user_key_comparison_count: u64,
    pub block_cache_hit_count: u64,
    pub block_read_count: u64,
    pub block_read_byte: u64,
    pub block_read_time: u64,
    pub block_cache_index_hit_count: u64,
    pub index_block_read_count: u64,
    pub block_cache_filter_hit_count: u64,
    pub filter_block_read_count: u64,
    pub block_checksum_time: u64,
    pub block_decompress_time: u64,
    pub get_read_bytes: u64,
    pub iter_read_bytes: u64,
    pub internal_key_skipped_count: u64,
    pub internal_delete_skipped_count: u64,
    pub internal_recent_skipped_count: u64,
    pub get_snapshot_time: u64,
    pub get_from_memtable_time: u64,
    pub get_from_memtable_count: u64,
    pub get_post_process_time: u64,
    pub get_from_output_files_time: u64,
    pub seek_on_memtable_time: u64,
    pub seek_on_memtable_count: u64,
    pub next_on_memtable_count: u64,
    pub prev_on_memtable_count: u64,
    pub seek_child_seek_time: u64,
    pub seek_child_seek_count: u64,
    pub seek_min_heap_time: u64,
    pub seek_max_heap_time: u64,
    pub seek_internal_seek_time: u64,
    pub db_mutex_lock_nanos: u64,
    pub db_condition_wait_nanos: u64,
    pub read_index_block_nanos: u64,
    pub read_filter_block_nanos: u64,
    pub new_table_block_iter_nanos: u64,
    pub new_table_iterator_nanos: u64,
    pub block_seek_nanos: u64,
    pub find_table_nanos: u64,
    pub bloom_memtable_hit_count: u64,
    pub bloom_memtable_miss_count: u64,
    pub bloom_sst_hit_count: u64,
    pub bloom_sst_miss_count: u64,
    pub get_cpu_nanos: u64,
    pub iter_next_cpu_nanos: u64,
    pub iter_prev_cpu_nanos: u64,
    pub iter_seek_cpu_nanos: u64,
    pub encrypt_data_nanos: u64,
    pub decrypt_data_nanos: u64,
}

impl ReadPerfContext {
    pub fn write_scan_detail(&self, detail_v2: &mut ScanDetailV2) {
        detail_v2.set_rocksdb_delete_skipped_count(self.internal_delete_skipped_count);
        detail_v2.set_rocksdb_key_skipped_count(self.internal_key_skipped_count);
        detail_v2.set_rocksdb_block_cache_hit_count(self.block_cache_hit_count);
        detail_v2.set_rocksdb_block_read_count(self.block_read_count);
        detail_v2.set_rocksdb_block_read_byte(self.block_read_byte);
    }
}

#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign, KV)]
pub struct WritePerfContext {
    pub write_wal_time: u64,
    pub pre_and_post_process: u64,
    pub write_memtable_time: u64,
    pub write_thread_wait: u64,
    pub db_mutex_lock_nanos: u64,
    pub write_scheduling_flushes_compactions_time: u64,
    pub db_condition_wait_nanos: u64,
    pub write_delay_time: u64,
}

#[derive(Debug)]
pub struct PerfContextStatistics {
    pub perf_level: PerfLevel,
    pub kind: PerfContextKind,
    pub read: ReadPerfContext,
    pub write: WritePerfContext,
}

impl PerfContextStatistics {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new(perf_level: PerfLevel, kind: PerfContextKind) -> Self {
        PerfContextStatistics {
            perf_level,
            kind,
            read: Default::default(),
            write: Default::default(),
        }
    }

    fn apply_perf_settings(&self) {
        if self.perf_level == PerfLevel::Uninitialized {
            match self.kind {
                PerfContextKind::GenericRead => set_perf_flags(&*DEFAULT_READ_PERF_FLAGS),
                PerfContextKind::RaftstoreStore | PerfContextKind::RaftstoreApply => {
                    set_perf_flags(&*DEFAULT_WRITE_PERF_FLAGS)
                }
            }
        } else {
            set_perf_level(raw_util::to_raw_perf_level(self.perf_level));
        }
    }

    pub fn start(&mut self) {
        if self.perf_level == PerfLevel::Disable {
            return;
        }
        let mut ctx = RawPerfContext::get();
        ctx.reset();
        self.apply_perf_settings();
    }

    pub fn report(&mut self) {
        match self.kind {
            PerfContextKind::RaftstoreApply => {
                report_write_perf_context!(self, APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
            }
            PerfContextKind::RaftstoreStore => {
                report_write_perf_context!(self, STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
            }
            PerfContextKind::GenericRead => {
                // TODO: Currently, metrics about reading is reported in other ways.
                // It is better to unify how to report the perf metrics.
                //
                // Here we only record the PerfContext data into the fields.
                self.read = ReadPerfContext::capture();
            }
        }
    }
}

pub trait PerfContextFields: Debug + Clone + Copy + Sub<Output = Self> + slog::KV {
    fn capture() -> Self;
}

// TODO: PerfStatisticsInstant are leaked details of the underlying engine.
// It's better to clean up direct usages of it in TiKV except in tests.
// Switch to use the perf context of the engine_trait.
//
/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This statistics store instant values.
#[derive(Debug, Clone)]
pub struct PerfStatisticsInstant<P: PerfContextFields> {
    inner: P,
    // The phantom is to make this type !Send and !Sync
    _phantom: PhantomData<*const ()>,
}

pub type ReadPerfInstant = PerfStatisticsInstant<ReadPerfContext>;
pub type WritePerfInstant = PerfStatisticsInstant<WritePerfContext>;

impl<P: PerfContextFields> PerfStatisticsInstant<P> {
    pub fn new() -> Self {
        Self {
            inner: P::capture(),
            _phantom: PhantomData,
        }
    }

    pub fn delta(&self) -> P {
        P::capture() - self.inner
    }
}

impl<P: PerfContextFields> Default for PerfStatisticsInstant<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: PerfContextFields> slog::KV for PerfStatisticsInstant<P> {
    fn serialize(
        &self,
        record: &::slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::KV::serialize(&self.inner, record, serializer)
    }
}

impl PerfContextFields for ReadPerfContext {
    fn capture() -> Self {
        let perf_context = RawPerfContext::get();
        ReadPerfContext {
            user_key_comparison_count: perf_context.user_key_comparison_count(),
            block_cache_hit_count: perf_context.block_cache_hit_count(),
            block_read_count: perf_context.block_read_count(),
            block_read_byte: perf_context.block_read_byte(),
            block_read_time: perf_context.block_read_time(),
            block_cache_index_hit_count: perf_context.block_cache_index_hit_count(),
            index_block_read_count: perf_context.index_block_read_count(),
            block_cache_filter_hit_count: perf_context.block_cache_filter_hit_count(),
            filter_block_read_count: perf_context.filter_block_read_count(),
            block_checksum_time: perf_context.block_checksum_time(),
            block_decompress_time: perf_context.block_decompress_time(),
            get_read_bytes: perf_context.get_read_bytes(),
            iter_read_bytes: perf_context.iter_read_bytes(),
            internal_key_skipped_count: perf_context.internal_key_skipped_count(),
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count(),
            internal_recent_skipped_count: perf_context.internal_recent_skipped_count(),
            get_snapshot_time: perf_context.get_snapshot_time(),
            get_from_memtable_time: perf_context.get_from_memtable_time(),
            get_from_memtable_count: perf_context.get_from_memtable_count(),
            get_post_process_time: perf_context.get_post_process_time(),
            get_from_output_files_time: perf_context.get_from_output_files_time(),
            seek_on_memtable_time: perf_context.seek_on_memtable_time(),
            seek_on_memtable_count: perf_context.seek_on_memtable_count(),
            next_on_memtable_count: perf_context.next_on_memtable_count(),
            prev_on_memtable_count: perf_context.prev_on_memtable_count(),
            seek_child_seek_time: perf_context.seek_child_seek_time(),
            seek_child_seek_count: perf_context.seek_child_seek_count(),
            seek_min_heap_time: perf_context.seek_min_heap_time(),
            seek_max_heap_time: perf_context.seek_max_heap_time(),
            seek_internal_seek_time: perf_context.seek_internal_seek_time(),
            db_mutex_lock_nanos: perf_context.db_mutex_lock_nanos(),
            db_condition_wait_nanos: perf_context.db_condition_wait_nanos(),
            read_index_block_nanos: perf_context.read_index_block_nanos(),
            read_filter_block_nanos: perf_context.read_filter_block_nanos(),
            new_table_block_iter_nanos: perf_context.new_table_block_iter_nanos(),
            new_table_iterator_nanos: perf_context.new_table_iterator_nanos(),
            block_seek_nanos: perf_context.block_seek_nanos(),
            find_table_nanos: perf_context.find_table_nanos(),
            bloom_memtable_hit_count: perf_context.bloom_memtable_hit_count(),
            bloom_memtable_miss_count: perf_context.bloom_memtable_miss_count(),
            bloom_sst_hit_count: perf_context.bloom_sst_hit_count(),
            bloom_sst_miss_count: perf_context.bloom_sst_miss_count(),
            get_cpu_nanos: perf_context.get_cpu_nanos(),
            iter_next_cpu_nanos: perf_context.iter_next_cpu_nanos(),
            iter_prev_cpu_nanos: perf_context.iter_prev_cpu_nanos(),
            iter_seek_cpu_nanos: perf_context.iter_seek_cpu_nanos(),
            encrypt_data_nanos: perf_context.encrypt_data_nanos(),
            decrypt_data_nanos: perf_context.decrypt_data_nanos(),
        }
    }
}

impl PerfContextFields for WritePerfContext {
    fn capture() -> Self {
        let perf_context = RawPerfContext::get();
        WritePerfContext {
            write_wal_time: perf_context.write_wal_time(),
            pre_and_post_process: perf_context.write_pre_and_post_process_time(),
            write_memtable_time: perf_context.write_memtable_time(),
            write_thread_wait: perf_context.write_thread_wait_nanos(),
            db_mutex_lock_nanos: perf_context.db_mutex_lock_nanos(),
            write_scheduling_flushes_compactions_time: perf_context
                .write_scheduling_flushes_compactions_time(),
            db_condition_wait_nanos: perf_context.db_condition_wait_nanos(),
            write_delay_time: perf_context.write_delay_time(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_operations() {
        let f1 = ReadPerfContext {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        };
        let f2 = ReadPerfContext {
            internal_key_skipped_count: 2,
            internal_delete_skipped_count: 3,
            block_cache_hit_count: 5,
            block_read_count: 7,
            block_read_byte: 11,
            ..Default::default()
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
        let mut stats = ReadPerfContext {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        };
        assert_eq!(stats.block_cache_hit_count, 3);
        stats.block_cache_hit_count = 6;
        assert_eq!(stats.block_cache_hit_count, 6);
    }
}
