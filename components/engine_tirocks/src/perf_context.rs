// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, marker::PhantomData, mem, ops::Sub, time::Duration};

use derive_more::{Add, AddAssign, Sub, SubAssign};
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;
use slog_derive::KV;
use tikv_util::time::Instant;
use tirocks::perf_context::{set_perf_flags, set_perf_level, PerfContext, PerfFlag, PerfFlags};
use tracker::{Tracker, TrackerToken, GLOBAL_TRACKERS};

use crate::{util, RocksEngine};

macro_rules! report_write_perf_context {
    ($ctx:expr, $metric:ident) => {
        if $ctx.perf_level != engine_traits::PerfLevel::Disable {
            $ctx.write = WritePerfContext::capture();
            observe_write_time!($ctx, $metric, write_wal_time);
            observe_write_time!($ctx, $metric, write_memtable_time);
            observe_write_time!($ctx, $metric, db_mutex_lock_nanos);
            observe_write_time!($ctx, $metric, pre_and_post_process);
            observe_write_time!($ctx, $metric, write_thread_wait);
            observe_write_time!($ctx, $metric, write_scheduling_flushes_compactions_time);
            observe_write_time!($ctx, $metric, db_condition_wait_nanos);
            observe_write_time!($ctx, $metric, write_delay_time);
        }
    };
}

macro_rules! observe_write_time {
    ($ctx:expr, $metric:expr, $v:ident) => {
        $metric.$v.observe(($ctx.write.$v) as f64 / 1e9);
    };
}

make_auto_flush_static_metric! {
    pub label_enum PerfContextType {
        write_wal_time,
        write_delay_time,
        write_scheduling_flushes_compactions_time,
        db_condition_wait_nanos,
        write_memtable_time,
        pre_and_post_process,
        write_thread_wait,
        db_mutex_lock_nanos,
    }

    pub struct PerfContextTimeDuration : LocalHistogram {
        "type" => PerfContextType
    }
}

lazy_static! {
    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_apply_perf_context_time_duration_secs",
        "Bucketed histogram of request wait time duration.",
        &["type"],
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_store_perf_context_time_duration_secs",
        "Bucketed histogram of request wait time duration.",
        &["type"],
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref STORAGE_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref COPR_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration =
        auto_flush_from!(APPLY_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);
    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration =
        auto_flush_from!(STORE_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);


    /// Default perf flags for a write operation.
    static ref DEFAULT_WRITE_PERF_FLAGS: PerfFlags = PerfFlags::default()
        | PerfFlag::write_wal_time
        | PerfFlag::write_pre_and_post_process_time
        | PerfFlag::write_memtable_time
        | PerfFlag::write_thread_wait_nanos
        | PerfFlag::db_mutex_lock_nanos
        | PerfFlag::write_scheduling_flushes_compactions_time
        | PerfFlag::db_condition_wait_nanos
        | PerfFlag::write_delay_time;

    /// Default perf flags for read operations.
    static ref DEFAULT_READ_PERF_FLAGS: PerfFlags = PerfFlags::default()
        | PerfFlag::user_key_comparison_count
        | PerfFlag::block_cache_hit_count
        | PerfFlag::block_read_count
        | PerfFlag::block_read_byte
        | PerfFlag::block_read_time
        | PerfFlag::block_cache_index_hit_count
        | PerfFlag::index_block_read_count
        | PerfFlag::block_cache_filter_hit_count
        | PerfFlag::filter_block_read_count
        | PerfFlag::compression_dict_block_read_count
        | PerfFlag::get_read_bytes
        | PerfFlag::internal_key_skipped_count
        | PerfFlag::internal_delete_skipped_count
        | PerfFlag::internal_recent_skipped_count
        | PerfFlag::get_snapshot_time
        | PerfFlag::get_from_memtable_count
        | PerfFlag::seek_on_memtable_count
        | PerfFlag::next_on_memtable_count
        | PerfFlag::prev_on_memtable_count
        | PerfFlag::seek_child_seek_count
        | PerfFlag::db_mutex_lock_nanos
        | PerfFlag::db_condition_wait_nanos
        | PerfFlag::bloom_memtable_hit_count
        | PerfFlag::bloom_memtable_miss_count
        | PerfFlag::bloom_sst_hit_count
        | PerfFlag::bloom_sst_miss_count
        | PerfFlag::user_key_return_count
        | PerfFlag::block_cache_miss_count
        | PerfFlag::bloom_filter_full_positive
        | PerfFlag::bloom_filter_useful
        | PerfFlag::bloom_filter_full_true_positive
        | PerfFlag::bytes_read;
}

impl engine_traits::PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context(
        &self,
        level: engine_traits::PerfLevel,
        kind: engine_traits::PerfContextKind,
    ) -> Self::PerfContext {
        RocksPerfContext::new(level, kind)
    }
}

#[derive(Debug)]
pub struct RocksPerfContext {
    pub stats: PerfContextStatistics,
}

impl RocksPerfContext {
    pub fn new(level: engine_traits::PerfLevel, kind: engine_traits::PerfContextKind) -> Self {
        RocksPerfContext {
            stats: PerfContextStatistics::new(level, kind),
        }
    }
}

impl engine_traits::PerfContext for RocksPerfContext {
    fn start_observe(&mut self) {
        self.stats.start()
    }

    fn report_metrics(&mut self, trackers: &[TrackerToken]) {
        self.stats.report(trackers)
    }
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
    fn report_to_tracker(&self, tracker: &mut Tracker) {
        tracker.metrics.block_cache_hit_count += self.block_cache_hit_count;
        tracker.metrics.block_read_byte += self.block_read_byte;
        tracker.metrics.block_read_count += self.block_read_count;
        tracker.metrics.block_read_nanos += self.block_read_time;
        tracker.metrics.deleted_key_skipped_count += self.internal_delete_skipped_count;
        tracker.metrics.internal_key_skipped_count += self.internal_key_skipped_count;
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
    perf_level: engine_traits::PerfLevel,
    kind: engine_traits::PerfContextKind,
    read: ReadPerfContext,
    write: WritePerfContext,
    last_flush_time: Instant,
}

const FLUSH_METRICS_INTERVAL: Duration = Duration::from_secs(2);

impl PerfContextStatistics {
    /// Create an instance which stores instant statistics values, retrieved at
    /// creation.
    pub fn new(perf_level: engine_traits::PerfLevel, kind: engine_traits::PerfContextKind) -> Self {
        PerfContextStatistics {
            perf_level,
            kind,
            read: Default::default(),
            write: Default::default(),
            last_flush_time: Instant::now_coarse(),
        }
    }

    fn apply_perf_settings(&self) {
        if self.perf_level == engine_traits::PerfLevel::Uninitialized {
            match self.kind {
                engine_traits::PerfContextKind::Storage(_)
                | engine_traits::PerfContextKind::Coprocessor(_) => {
                    set_perf_flags(&DEFAULT_READ_PERF_FLAGS)
                }
                engine_traits::PerfContextKind::RaftstoreStore
                | engine_traits::PerfContextKind::RaftstoreApply => {
                    set_perf_flags(&DEFAULT_WRITE_PERF_FLAGS)
                }
            }
        } else {
            set_perf_level(util::to_rocks_perf_level(self.perf_level));
        }
    }

    pub fn start(&mut self) {
        if self.perf_level == engine_traits::PerfLevel::Disable {
            return;
        }
        let mut ctx = PerfContext::get();
        ctx.reset();
        self.apply_perf_settings();
    }

    pub fn report(&mut self, trackers: &[TrackerToken]) {
        match self.kind {
            engine_traits::PerfContextKind::RaftstoreApply => {
                report_write_perf_context!(self, APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
                for token in trackers {
                    GLOBAL_TRACKERS.with_tracker(*token, |t| {
                        t.metrics.apply_mutex_lock_nanos = self.write.db_mutex_lock_nanos;
                        t.metrics.apply_thread_wait_nanos = self.write.write_thread_wait;
                        t.metrics.apply_write_wal_nanos = self.write.write_wal_time;
                        t.metrics.apply_write_memtable_nanos = self.write.write_memtable_time;
                    });
                }
            }
            engine_traits::PerfContextKind::RaftstoreStore => {
                report_write_perf_context!(self, STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
                for token in trackers {
                    GLOBAL_TRACKERS.with_tracker(*token, |t| {
                        t.metrics.store_mutex_lock_nanos = self.write.db_mutex_lock_nanos;
                        t.metrics.store_thread_wait_nanos = self.write.write_thread_wait;
                        t.metrics.store_write_wal_nanos = self.write.write_wal_time;
                        t.metrics.store_write_memtable_nanos = self.write.write_memtable_time;
                    });
                }
            }
            engine_traits::PerfContextKind::Storage(_)
            | engine_traits::PerfContextKind::Coprocessor(_) => {
                let perf_context = ReadPerfContext::capture();
                for token in trackers {
                    GLOBAL_TRACKERS.with_tracker(*token, |t| perf_context.report_to_tracker(t));
                }
                self.read += perf_context;
                self.maybe_flush_read_metrics();
            }
        }
    }

    fn maybe_flush_read_metrics(&mut self) {
        if self.last_flush_time.saturating_elapsed() < FLUSH_METRICS_INTERVAL {
            return;
        }
        self.last_flush_time = Instant::now_coarse();
        let ctx = mem::take(&mut self.read);
        let (v, tag) = match self.kind {
            engine_traits::PerfContextKind::Storage(tag) => (&*STORAGE_ROCKSDB_PERF_COUNTER, tag),
            engine_traits::PerfContextKind::Coprocessor(tag) => (&*COPR_ROCKSDB_PERF_COUNTER, tag),
            _ => unreachable!(),
        };
        v.get_metric_with_label_values(&[tag, "user_key_comparison_count"])
            .unwrap()
            .inc_by(ctx.user_key_comparison_count);
        v.get_metric_with_label_values(&[tag, "block_cache_hit_count"])
            .unwrap()
            .inc_by(ctx.block_cache_hit_count);
        v.get_metric_with_label_values(&[tag, "block_read_count"])
            .unwrap()
            .inc_by(ctx.block_read_count);
        v.get_metric_with_label_values(&[tag, "block_read_byte"])
            .unwrap()
            .inc_by(ctx.block_read_byte);
        v.get_metric_with_label_values(&[tag, "block_read_time"])
            .unwrap()
            .inc_by(ctx.block_read_time);
        v.get_metric_with_label_values(&[tag, "block_cache_index_hit_count"])
            .unwrap()
            .inc_by(ctx.block_cache_index_hit_count);
        v.get_metric_with_label_values(&[tag, "index_block_read_count"])
            .unwrap()
            .inc_by(ctx.index_block_read_count);
        v.get_metric_with_label_values(&[tag, "block_cache_filter_hit_count"])
            .unwrap()
            .inc_by(ctx.block_cache_filter_hit_count);
        v.get_metric_with_label_values(&[tag, "filter_block_read_count"])
            .unwrap()
            .inc_by(ctx.filter_block_read_count);
        v.get_metric_with_label_values(&[tag, "block_checksum_time"])
            .unwrap()
            .inc_by(ctx.block_checksum_time);
        v.get_metric_with_label_values(&[tag, "block_decompress_time"])
            .unwrap()
            .inc_by(ctx.block_decompress_time);
        v.get_metric_with_label_values(&[tag, "get_read_bytes"])
            .unwrap()
            .inc_by(ctx.get_read_bytes);
        v.get_metric_with_label_values(&[tag, "iter_read_bytes"])
            .unwrap()
            .inc_by(ctx.iter_read_bytes);
        v.get_metric_with_label_values(&[tag, "internal_key_skipped_count"])
            .unwrap()
            .inc_by(ctx.internal_key_skipped_count);
        v.get_metric_with_label_values(&[tag, "internal_delete_skipped_count"])
            .unwrap()
            .inc_by(ctx.internal_delete_skipped_count);
        v.get_metric_with_label_values(&[tag, "internal_recent_skipped_count"])
            .unwrap()
            .inc_by(ctx.internal_recent_skipped_count);
        v.get_metric_with_label_values(&[tag, "get_snapshot_time"])
            .unwrap()
            .inc_by(ctx.get_snapshot_time);
        v.get_metric_with_label_values(&[tag, "get_from_memtable_time"])
            .unwrap()
            .inc_by(ctx.get_from_memtable_time);
        v.get_metric_with_label_values(&[tag, "get_from_memtable_count"])
            .unwrap()
            .inc_by(ctx.get_from_memtable_count);
        v.get_metric_with_label_values(&[tag, "get_post_process_time"])
            .unwrap()
            .inc_by(ctx.get_post_process_time);
        v.get_metric_with_label_values(&[tag, "get_from_output_files_time"])
            .unwrap()
            .inc_by(ctx.get_from_output_files_time);
        v.get_metric_with_label_values(&[tag, "seek_on_memtable_time"])
            .unwrap()
            .inc_by(ctx.seek_on_memtable_time);
        v.get_metric_with_label_values(&[tag, "seek_on_memtable_count"])
            .unwrap()
            .inc_by(ctx.seek_on_memtable_count);
        v.get_metric_with_label_values(&[tag, "next_on_memtable_count"])
            .unwrap()
            .inc_by(ctx.next_on_memtable_count);
        v.get_metric_with_label_values(&[tag, "prev_on_memtable_count"])
            .unwrap()
            .inc_by(ctx.prev_on_memtable_count);
        v.get_metric_with_label_values(&[tag, "seek_child_seek_time"])
            .unwrap()
            .inc_by(ctx.seek_child_seek_time);
        v.get_metric_with_label_values(&[tag, "seek_child_seek_count"])
            .unwrap()
            .inc_by(ctx.seek_child_seek_count);
        v.get_metric_with_label_values(&[tag, "seek_min_heap_time"])
            .unwrap()
            .inc_by(ctx.seek_min_heap_time);
        v.get_metric_with_label_values(&[tag, "seek_max_heap_time"])
            .unwrap()
            .inc_by(ctx.seek_max_heap_time);
        v.get_metric_with_label_values(&[tag, "seek_internal_seek_time"])
            .unwrap()
            .inc_by(ctx.seek_internal_seek_time);
        v.get_metric_with_label_values(&[tag, "db_mutex_lock_nanos"])
            .unwrap()
            .inc_by(ctx.db_mutex_lock_nanos);
        v.get_metric_with_label_values(&[tag, "db_condition_wait_nanos"])
            .unwrap()
            .inc_by(ctx.db_condition_wait_nanos);
        v.get_metric_with_label_values(&[tag, "read_index_block_nanos"])
            .unwrap()
            .inc_by(ctx.read_index_block_nanos);
        v.get_metric_with_label_values(&[tag, "read_filter_block_nanos"])
            .unwrap()
            .inc_by(ctx.read_filter_block_nanos);
        v.get_metric_with_label_values(&[tag, "new_table_block_iter_nanos"])
            .unwrap()
            .inc_by(ctx.new_table_block_iter_nanos);
        v.get_metric_with_label_values(&[tag, "new_table_iterator_nanos"])
            .unwrap()
            .inc_by(ctx.new_table_iterator_nanos);
        v.get_metric_with_label_values(&[tag, "block_seek_nanos"])
            .unwrap()
            .inc_by(ctx.block_seek_nanos);
        v.get_metric_with_label_values(&[tag, "find_table_nanos"])
            .unwrap()
            .inc_by(ctx.find_table_nanos);
        v.get_metric_with_label_values(&[tag, "bloom_memtable_hit_count"])
            .unwrap()
            .inc_by(ctx.bloom_memtable_hit_count);
        v.get_metric_with_label_values(&[tag, "bloom_memtable_miss_count"])
            .unwrap()
            .inc_by(ctx.bloom_memtable_miss_count);
        v.get_metric_with_label_values(&[tag, "bloom_sst_hit_count"])
            .unwrap()
            .inc_by(ctx.bloom_sst_hit_count);
        v.get_metric_with_label_values(&[tag, "bloom_sst_miss_count"])
            .unwrap()
            .inc_by(ctx.bloom_sst_miss_count);
        v.get_metric_with_label_values(&[tag, "get_cpu_nanos"])
            .unwrap()
            .inc_by(ctx.get_cpu_nanos);
        v.get_metric_with_label_values(&[tag, "iter_next_cpu_nanos"])
            .unwrap()
            .inc_by(ctx.iter_next_cpu_nanos);
        v.get_metric_with_label_values(&[tag, "iter_prev_cpu_nanos"])
            .unwrap()
            .inc_by(ctx.iter_prev_cpu_nanos);
        v.get_metric_with_label_values(&[tag, "iter_seek_cpu_nanos"])
            .unwrap()
            .inc_by(ctx.iter_seek_cpu_nanos);
        v.get_metric_with_label_values(&[tag, "encrypt_data_nanos"])
            .unwrap()
            .inc_by(ctx.encrypt_data_nanos);
        v.get_metric_with_label_values(&[tag, "decrypt_data_nanos"])
            .unwrap()
            .inc_by(ctx.decrypt_data_nanos);
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
        let perf_context = PerfContext::get();
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
        let perf_context = PerfContext::get();
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
