// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Prometheus metrics for storage functionality.

use prometheus::*;
use prometheus_static_metric::*;

use std::cell::RefCell;
use std::mem;
use std::sync::Arc;

use crate::server::metrics::{GcKeysCF as ServerGcKeysCF, GcKeysDetail as ServerGcKeysDetail};
use crate::storage::kv::{FlowStatsReporter, PerfStatisticsDelta, Statistics};
use collections::HashMap;
use kvproto::kvrpcpb::KeyRange;
use kvproto::metapb;
use kvproto::pdpb::QueryKind;
use pd_client::BucketMeta;
use raftstore::store::util::build_key_range;
use raftstore::store::ReadStats;

struct StorageLocalMetrics {
    local_scan_details: HashMap<CommandKind, Statistics>,
    local_read_stats: ReadStats,
    local_perf_stats: HashMap<CommandKind, PerfStatisticsDelta>,
}

thread_local! {
    static TLS_STORAGE_METRICS: RefCell<StorageLocalMetrics> = RefCell::new(
        StorageLocalMetrics {
            local_scan_details: HashMap::default(),
            local_read_stats:ReadStats::default(),
            local_perf_stats: HashMap::default(),
        }
    );
}

macro_rules! tls_flush_perf_stats {
    ($tag:ident, $local_stats:ident, $stat:ident) => {
        STORAGE_ROCKSDB_PERF_COUNTER_STATIC
            .get($tag)
            .$stat
            .inc_by($local_stats.0.$stat as u64);
    };
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();

        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details_enum().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS_STATIC
                        .get(cmd)
                        .get((*cf).into())
                        .get((*tag).into())
                        .inc_by(*count as u64);
                }
            }
        }

        // Report PD metrics
        if !m.local_read_stats.is_empty() {
            let mut read_stats = ReadStats::default();
            mem::swap(&mut read_stats, &mut m.local_read_stats);
            reporter.report_read_stats(read_stats);
        }

        for (req_tag, perf_stats) in m.local_perf_stats.drain() {
            tls_flush_perf_stats!(req_tag, perf_stats, user_key_comparison_count);
            tls_flush_perf_stats!(req_tag, perf_stats, block_cache_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, block_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, block_read_byte);
            tls_flush_perf_stats!(req_tag, perf_stats, block_read_time);
            tls_flush_perf_stats!(req_tag, perf_stats, block_cache_index_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, index_block_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, block_cache_filter_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, filter_block_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, block_checksum_time);
            tls_flush_perf_stats!(req_tag, perf_stats, block_decompress_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_read_bytes);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_read_bytes);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_key_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_delete_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_recent_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_snapshot_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_memtable_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_post_process_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_output_files_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_on_memtable_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, next_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, prev_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_child_seek_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_child_seek_count);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_min_heap_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_max_heap_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_internal_seek_time);
            tls_flush_perf_stats!(req_tag, perf_stats, db_mutex_lock_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, db_condition_wait_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, read_index_block_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, read_filter_block_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, new_table_block_iter_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, new_table_iterator_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, block_seek_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, find_table_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_memtable_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_memtable_miss_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_sst_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_sst_miss_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_next_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_prev_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_seek_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, encrypt_data_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, decrypt_data_nanos);
        }
    });
}

pub fn tls_collect_scan_details(cmd: CommandKind, stats: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(
    region_id: u64,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    statistics: &Statistics,
    buckets: Option<&Arc<BucketMeta>>,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats.add_flow(
            region_id,
            buckets,
            start,
            end,
            &statistics.write.flow_stats,
            &statistics.data.flow_stats,
        );
    });
}

pub fn tls_collect_query(
    region_id: u64,
    peer: &metapb::Peer,
    start_key: &[u8],
    end_key: &[u8],
    reverse_scan: bool,
    kind: QueryKind,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_range = build_key_range(start_key, end_key, reverse_scan);
        m.local_read_stats
            .add_query_num(region_id, peer, key_range, kind);
    });
}

pub fn tls_collect_query_batch(
    region_id: u64,
    peer: &metapb::Peer,
    key_ranges: Vec<KeyRange>,
    kind: QueryKind,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats
            .add_query_num_batch(region_id, peer, key_ranges, kind);
    });
}

pub fn tls_collect_perf_stats(cmd: CommandKind, perf_stats: &PerfStatisticsDelta) {
    TLS_STORAGE_METRICS.with(|m| {
        *(m.borrow_mut()
            .local_perf_stats
            .entry(cmd)
            .or_insert_with(Default::default)) += *perf_stats;
    })
}

make_auto_flush_static_metric! {
    pub label_enum CommandKind {
        get,
        raw_batch_get_command,
        scan,
        batch_get,
        batch_get_command,
        prewrite,
        acquire_pessimistic_lock,
        commit,
        cleanup,
        rollback,
        pessimistic_rollback,
        txn_heart_beat,
        check_txn_status,
        check_secondary_locks,
        scan_lock,
        resolve_lock,
        resolve_lock_lite,
        delete_range,
        pause,
        key_mvcc,
        start_ts_mvcc,
        raw_get,
        raw_batch_get,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_range,
        raw_batch_delete,
        raw_get_key_ttl,
        raw_compare_and_swap,
        raw_atomic_store,
        raw_checksum,
    }

    pub label_enum CommandStageKind {
        new,
        snapshot,
        async_snapshot_err,
        snapshot_ok,
        snapshot_err,
        read_finish,
        next_cmd,
        lock_wait,
        process,
        prepare_write_err,
        write,
        write_finish,
        async_write_err,
        error,
        pipelined_write,
        pipelined_write_finish,
        async_apply_prewrite,
        async_apply_prewrite_finish,
    }

    pub label_enum CommandPriority {
        low,
        normal,
        high,
    }

    pub label_enum GcKeysCF {
        default,
        lock,
        write,
    }

    pub label_enum GcKeysDetail {
        processed_keys,
        get,
        next,
        prev,
        seek,
        seek_for_prev,
        over_seek_bound,
        next_tombstone,
        prev_tombstone,
        seek_tombstone,
        seek_for_prev_tombstone,
        raw_value_tombstone,
    }

    pub label_enum CheckMemLockResult {
        locked,
        unlocked,
    }

    pub label_enum PerfMetric {
        user_key_comparison_count,
        block_cache_hit_count,
        block_read_count,
        block_read_byte,
        block_read_time,
        block_cache_index_hit_count,
        index_block_read_count,
        block_cache_filter_hit_count,
        filter_block_read_count,
        block_checksum_time,
        block_decompress_time,
        get_read_bytes,
        iter_read_bytes,
        internal_key_skipped_count,
        internal_delete_skipped_count,
        internal_recent_skipped_count,
        get_snapshot_time,
        get_from_memtable_time,
        get_from_memtable_count,
        get_post_process_time,
        get_from_output_files_time,
        seek_on_memtable_time,
        seek_on_memtable_count,
        next_on_memtable_count,
        prev_on_memtable_count,
        seek_child_seek_time,
        seek_child_seek_count,
        seek_min_heap_time,
        seek_max_heap_time,
        seek_internal_seek_time,
        db_mutex_lock_nanos,
        db_condition_wait_nanos,
        read_index_block_nanos,
        read_filter_block_nanos,
        new_table_block_iter_nanos,
        new_table_iterator_nanos,
        block_seek_nanos,
        find_table_nanos,
        bloom_memtable_hit_count,
        bloom_memtable_miss_count,
        bloom_sst_hit_count,
        bloom_sst_miss_count,
        get_cpu_nanos,
        iter_next_cpu_nanos,
        iter_prev_cpu_nanos,
        iter_seek_cpu_nanos,
        encrypt_data_nanos,
        decrypt_data_nanos,
    }

    pub label_enum InMemoryPessimisticLockingResult {
        success,
        full,
    }

    pub struct CommandScanDetails: LocalIntCounter {
        "req" => CommandKind,
        "cf" => GcKeysCF,
        "tag" => GcKeysDetail,
    }

    pub struct SchedDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct ProcessingReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KvCommandCounterVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedStageCounterVec: LocalIntCounter {
        "type" => CommandKind,
        "stage" => CommandStageKind,
    }

    pub struct SchedLatchDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KvCommandKeysWrittenVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct SchedTooBusyVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedCommandPriCounterVec: LocalIntCounter {
        "priority" => CommandPriority,
    }

    pub struct CheckMemLockHistogramVec: LocalHistogram {
        "type" => CommandKind,
        "result" => CheckMemLockResult,
    }

    pub struct PerfCounter: LocalIntCounter {
        "req" => CommandKind,
        "metric" => PerfMetric,
    }

    pub struct TxnCommandThrottleTimeCounterVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct InMemoryPessimisticLockingCounter: LocalIntCounter {
        "result" => InMemoryPessimisticLockingResult,
    }
}

impl From<ServerGcKeysCF> for GcKeysCF {
    fn from(cf: ServerGcKeysCF) -> GcKeysCF {
        match cf {
            ServerGcKeysCF::default => GcKeysCF::default,
            ServerGcKeysCF::lock => GcKeysCF::lock,
            ServerGcKeysCF::write => GcKeysCF::write,
        }
    }
}

impl From<ServerGcKeysDetail> for GcKeysDetail {
    fn from(detail: ServerGcKeysDetail) -> GcKeysDetail {
        match detail {
            ServerGcKeysDetail::processed_keys => GcKeysDetail::processed_keys,
            ServerGcKeysDetail::get => GcKeysDetail::get,
            ServerGcKeysDetail::next => GcKeysDetail::next,
            ServerGcKeysDetail::prev => GcKeysDetail::prev,
            ServerGcKeysDetail::seek => GcKeysDetail::seek,
            ServerGcKeysDetail::seek_for_prev => GcKeysDetail::seek_for_prev,
            ServerGcKeysDetail::over_seek_bound => GcKeysDetail::over_seek_bound,
            ServerGcKeysDetail::next_tombstone => GcKeysDetail::next_tombstone,
            ServerGcKeysDetail::prev_tombstone => GcKeysDetail::prev_tombstone,
            ServerGcKeysDetail::seek_tombstone => GcKeysDetail::seek_tombstone,
            ServerGcKeysDetail::seek_for_prev_tombstone => GcKeysDetail::seek_for_prev_tombstone,
            ServerGcKeysDetail::raw_value_tombstone => GcKeysDetail::raw_value_tombstone,
        }
    }
}

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_command_total",
        "Total number of commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref KV_COMMAND_COUNTER_VEC_STATIC: KvCommandCounterVec =
        auto_flush_from!(KV_COMMAND_COUNTER_VEC, KvCommandCounterVec);
    pub static ref SCHED_STAGE_COUNTER: IntCounterVec = {
        register_int_counter_vec!(
            "tikv_scheduler_stage_total",
            "Total number of commands on each stage.",
            &["type", "stage"]
        )
        .unwrap()
    };
    pub static ref SCHED_STAGE_COUNTER_VEC: SchedStageCounterVec =
        auto_flush_from!(SCHED_STAGE_COUNTER, SchedStageCounterVec);
    pub static ref SCHED_WRITING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_writing_bytes",
        "Total number of writing kv."
    )
    .unwrap();
    pub static ref SCHED_CONTEX_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_contex_total",
        "Total number of pending commands."
    )
    .unwrap();
    pub static ref SCHED_WRITE_FLOW_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_write_flow",
        "The write flow passed through at scheduler level."
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_FLOW_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_throttle_flow",
        "The throttled write flow at scheduler level."
    )
    .unwrap();
       pub static ref SCHED_L0_TARGET_FLOW_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_l0_target_flow",
        "The target flow of L0."
    )
    .unwrap();

    pub static ref SCHED_MEMTABLE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_memtable",
        "The number of memtables.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_l0",
        "The number of l0 files.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_AVG_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_l0_avg",
        "The number of average l0 files.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_FLUSH_FLOW_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_flush_flow",
        "The speed of flush flow.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_FLOW_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_l0_flow",
        "The speed of l0 compaction flow.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_ACTION_COUNTER: IntCounterVec = {
        register_int_counter_vec!(
            "tikv_scheduler_throttle_action_total",
            "Total number of actions for flow control.",
            &["cf", "type"]
        )
        .unwrap()
    };
    pub static ref SCHED_DISCARD_RATIO_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_discard_ratio",
        "The discard ratio for flow control."
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_CF_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_throttle_cf",
        "The CF being throttled.",
        &["cf"]
    ).unwrap();
    pub static ref SCHED_PENDING_COMPACTION_BYTES_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_scheduler_pending_compaction_bytes",
        "The number of pending compaction bytes.",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_TIME: Histogram =
        register_histogram!(
            "tikv_scheduler_throttle_duration_seconds",
            "Bucketed histogram of peer commits logs duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_command_duration_seconds",
        "Bucketed histogram of command execution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC_STATIC: SchedDurationVec =
        auto_flush_from!(SCHED_HISTOGRAM_VEC, SchedDurationVec);
    pub static ref SCHED_LATCH_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_latch_wait_duration_seconds",
        "Bucketed histogram of latch wait",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_LATCH_HISTOGRAM_VEC: SchedLatchDurationVec =
        auto_flush_from!(SCHED_LATCH_HISTOGRAM, SchedLatchDurationVec);
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_read_duration_seconds",
        "Bucketed histogram of processing read duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_STATIC: ProcessingReadVec =
        auto_flush_from!(SCHED_PROCESSING_READ_HISTOGRAM_VEC, ProcessingReadVec);
    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_write_duration_seconds",
        "Bucketed histogram of processing write duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_too_busy_total",
        "Total count of scheduler too busy",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: SchedTooBusyVec =
        auto_flush_from!(SCHED_TOO_BUSY_COUNTER, SchedTooBusyVec);
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_commands_pri_total",
        "Total count of different priority commands",
        &["priority"]
    )
    .unwrap();
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC: SchedCommandPriCounterVec =
        auto_flush_from!(SCHED_COMMANDS_PRI_COUNTER_VEC, SchedCommandPriCounterVec);
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_kv_command_key_read",
        "Bucketed histogram of keys read of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_STATIC: KReadVec =
        auto_flush_from!(KV_COMMAND_KEYREAD_HISTOGRAM_VEC, KReadVec);
    pub static ref KV_COMMAND_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_kv_scan_details",
        "Bucketed counter of kv keys scan details for each cf",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref KV_COMMAND_SCAN_DETAILS_STATIC: CommandScanDetails =
        auto_flush_from!(KV_COMMAND_SCAN_DETAILS, CommandScanDetails);
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_kv_command_key_write",
        "Bucketed histogram of keys write of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: KvCommandKeysWrittenVec =
        auto_flush_from!(KV_COMMAND_KEYWRITE_HISTOGRAM, KvCommandKeysWrittenVec);
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "tikv_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
    pub static ref CHECK_MEM_LOCK_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_storage_check_mem_lock_duration_seconds",
        "Histogram of the duration of checking memory locks",
        &["type", "result"],
        exponential_buckets(1e-6f64, 4f64, 10).unwrap() // 1us ~ 262ms
    )
    .unwrap();
    pub static ref CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC: CheckMemLockHistogramVec =
        auto_flush_from!(CHECK_MEM_LOCK_DURATION_HISTOGRAM, CheckMemLockHistogramVec);

    pub static ref STORAGE_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();

    pub static ref STORAGE_ROCKSDB_PERF_COUNTER_STATIC: PerfCounter =
        auto_flush_from!(STORAGE_ROCKSDB_PERF_COUNTER, PerfCounter);

    pub static ref TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_txn_command_throttle_time_total",
        "Total throttle time (microsecond) of txn commands.",
        &["type"]
    )
    .unwrap();

    pub static ref TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC: TxnCommandThrottleTimeCounterVec =
        auto_flush_from!(TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC, TxnCommandThrottleTimeCounterVec);

    pub static ref IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_in_memory_pessimistic_locking",
        "Count of different types of in-memory pessimistic locking",
        &["result"]
    )
    .unwrap();
    pub static ref IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER_STATIC: InMemoryPessimisticLockingCounter =
        auto_flush_from!(IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER, InMemoryPessimisticLockingCounter);
}
