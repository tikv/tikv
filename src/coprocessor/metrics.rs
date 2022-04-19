// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::sync::Arc;

use crate::storage::{kv::PerfStatisticsDelta, FlowStatsReporter, Statistics};
use collections::HashMap;
use kvproto::metapb;
use kvproto::pdpb::QueryKind;
use pd_client::BucketMeta;
use raftstore::store::util::build_key_range;
use raftstore::store::ReadStats;

use crate::server::metrics::{GcKeysCF, GcKeysDetail};
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum ReqTag {
        select,
        index,
        // For AnalyzeType::{TypeColumn,TypeMixed}.
        analyze_table,
        // For AnalyzeType::{TypeIndex,TypeCommonHandle}.
        analyze_index,
        // For AnalyzeType::TypeFullSampling.
        analyze_full_sampling,
        checksum_table,
        checksum_index,
        test,
    }

    pub label_enum CF {
        default,
        lock,
        write,
    }

    pub label_enum ScanKeysKind {
        processed_keys,
        total,
    }

    pub label_enum ScanKind {
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

    pub label_enum WaitType {
        all,
        schedule,
        snapshot,
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

    pub label_enum MemLockCheckResult {
        unlocked,
        locked,
    }

    pub struct CoprReqHistogram: LocalHistogram {
        "req" => ReqTag,
    }

    pub struct ReqWaitHistogram: LocalHistogram {
        "req" => ReqTag,
        "type" => WaitType,
    }

    pub struct PerfCounter: LocalIntCounter {
        "req" => ReqTag,
        "metric" => PerfMetric,
    }

    pub struct CoprScanKeysHistogram: LocalHistogram {
        "req" => ReqTag,
        "kind" => ScanKeysKind,
    }

    pub struct CoprScanDetails : LocalIntCounter {
        "req" => ReqTag,
        "cf" => CF,
        "tag" => ScanKind,
    }

    pub struct MemLockCheckHistogramVec: LocalHistogram {
        "result" => MemLockCheckResult,
    }
}

lazy_static! {
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_duration_seconds",
        "Bucketed histogram of coprocessor request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HISTOGRAM_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HISTOGRAM_VEC, CoprReqHistogram);
    pub static ref COPR_REQ_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_handle_seconds",
        "Bucketed histogram of coprocessor handle request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HANDLE_TIME_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HANDLE_TIME, CoprReqHistogram);
    pub static ref COPR_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_wait_seconds",
        "Bucketed histogram of coprocessor request wait duration",
        &["req", "type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_WAIT_TIME_STATIC: ReqWaitHistogram =
        auto_flush_from!(COPR_REQ_WAIT_TIME, ReqWaitHistogram);
    pub static ref COPR_REQ_HANDLER_BUILD_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_handler_build_seconds",
        "Bucketed histogram of coprocessor request handler build duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_REQ_HANDLER_BUILD_TIME_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_REQ_HANDLER_BUILD_TIME, CoprReqHistogram);
    pub static ref COPR_REQ_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_request_error",
        "Total number of push down request error.",
        &["reason"]
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_scan_keys",
        "Bucketed histogram of coprocessor per request scan keys",
        &["req", "kind"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS_STATIC: CoprScanKeysHistogram =
        auto_flush_from!(COPR_SCAN_KEYS, CoprScanKeysHistogram);
    pub static ref COPR_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_scan_details",
        "Bucketed counter of coprocessor scan details for each CF",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref COPR_SCAN_DETAILS_STATIC: CoprScanDetails =
        auto_flush_from!(COPR_SCAN_DETAILS, CoprScanDetails);
    pub static ref COPR_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref COPR_ROCKSDB_PERF_COUNTER_STATIC: PerfCounter =
        auto_flush_from!(COPR_ROCKSDB_PERF_COUNTER, PerfCounter);
    pub static ref COPR_DAG_REQ_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_dag_request_count",
        "Total number of DAG requests",
        &["vec_type"]
    )
    .unwrap();
    pub static ref COPR_RESP_SIZE: IntCounter = register_int_counter!(
        "tikv_coprocessor_response_bytes",
        "Total bytes of response body"
    )
    .unwrap();
    pub static ref COPR_ACQUIRE_SEMAPHORE_TYPE: CoprAcquireSemaphoreTypeCounterVec =
        register_static_int_counter_vec!(
            CoprAcquireSemaphoreTypeCounterVec,
            "tikv_coprocessor_acquire_semaphore_type",
            "The acquire type of the coprocessor semaphore",
            &["type"],
        )
        .unwrap();
    pub static ref COPR_WAITING_FOR_SEMAPHORE: IntGauge = register_int_gauge!(
        "tikv_coprocessor_waiting_for_semaphore",
        "The number of tasks waiting for the semaphore"
    )
    .unwrap();
    pub static ref MEM_LOCK_CHECK_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_mem_lock_check_duration_seconds",
            "Duration of memory lock checking for coprocessor",
            &["result"],
            exponential_buckets(1e-6f64, 4f64, 10).unwrap() // 1us ~ 262ms
        )
        .unwrap();
    pub static ref MEM_LOCK_CHECK_HISTOGRAM_VEC_STATIC: MemLockCheckHistogramVec =
        auto_flush_from!(MEM_LOCK_CHECK_HISTOGRAM_VEC, MemLockCheckHistogramVec);
}

make_static_metric! {
    pub label_enum AcquireSemaphoreType {
        unacquired,
        acquired,
    }

    pub struct CoprAcquireSemaphoreTypeCounterVec: IntCounter {
        "type" => AcquireSemaphoreType,
    }
}

pub struct CopLocalMetrics {
    local_scan_details: HashMap<ReqTag, Statistics>,
    local_read_stats: ReadStats,
    local_perf_stats: HashMap<ReqTag, PerfStatisticsDelta>,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_scan_details: HashMap::default(),
            local_read_stats: ReadStats::default(),
            local_perf_stats: HashMap::default(),
        }
    );
}

macro_rules! tls_flush_perf_stats {
    ($tag:ident, $local_stats:ident, $stat:ident) => {
        COPR_ROCKSDB_PERF_COUNTER_STATIC
            .get($tag)
            .$stat
            .inc_by($local_stats.0.$stat as u64);
    };
}

impl From<GcKeysCF> for CF {
    fn from(cf: GcKeysCF) -> CF {
        match cf {
            GcKeysCF::default => CF::default,
            GcKeysCF::lock => CF::lock,
            GcKeysCF::write => CF::write,
        }
    }
}

impl From<GcKeysDetail> for ScanKind {
    fn from(detail: GcKeysDetail) -> ScanKind {
        match detail {
            GcKeysDetail::processed_keys => ScanKind::processed_keys,
            GcKeysDetail::get => ScanKind::get,
            GcKeysDetail::next => ScanKind::next,
            GcKeysDetail::prev => ScanKind::prev,
            GcKeysDetail::seek => ScanKind::seek,
            GcKeysDetail::seek_for_prev => ScanKind::seek_for_prev,
            GcKeysDetail::over_seek_bound => ScanKind::over_seek_bound,
            GcKeysDetail::next_tombstone => ScanKind::next_tombstone,
            GcKeysDetail::prev_tombstone => ScanKind::prev_tombstone,
            GcKeysDetail::seek_tombstone => ScanKind::seek_tombstone,
            GcKeysDetail::seek_for_prev_tombstone => ScanKind::seek_for_prev_tombstone,
            GcKeysDetail::raw_value_tombstone => ScanKind::raw_value_tombstone,
        }
    }
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut m = m.borrow_mut();

        for (req_tag, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details_enum().iter() {
                for (tag, count) in cf_details.iter() {
                    COPR_SCAN_DETAILS_STATIC
                        .get(req_tag)
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

pub fn tls_collect_scan_details(cmd: ReqTag, stats: &Statistics) {
    TLS_COP_METRICS.with(|m| {
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
    TLS_COP_METRICS.with(|m| {
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
) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_range = build_key_range(start_key, end_key, reverse_scan);
        m.local_read_stats
            .add_query_num(region_id, peer, key_range, QueryKind::Coprocessor);
    });
}

pub fn tls_collect_perf_stats(cmd: ReqTag, perf_stats: &PerfStatisticsDelta) {
    TLS_COP_METRICS.with(|m| {
        *(m.borrow_mut()
            .local_perf_stats
            .entry(cmd)
            .or_insert_with(Default::default)) += *perf_stats;
    });
}
