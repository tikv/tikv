// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;

use crate::storage::{FlowStatsReporter, Statistics};
use kvproto::metapb;
use raftstore::store::util::build_key_range;
use raftstore::store::ReadStats;
use tikv_util::collections::HashMap;

use crate::server::metrics::{GcKeysCF, GcKeysDetail};
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum ReqTag {
        select,
        index,
        analyze_table,
        analyze_index,
        checksum_table,
        checksum_index,
        test,
    }

    pub label_enum CF {
        default,
        lock,
        write,
    }

    pub label_enum StatDetail {
        total,
        processed,
        get,
        next,
        prev,
        seek,
        seek_for_prev,
        over_seek_bound,
    }

    pub label_enum WaitType {
        all,
        schedule,
        snapshot,
    }

    pub label_enum PerfMetric {
        internal_key_skipped_count,
        internal_delete_skipped_count,
        block_cache_hit_count,
        block_read_count,
        block_read_byte,
        encrypt_data_nanos,
        decrypt_data_nanos,
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

    pub struct CoprScanDetails : LocalIntCounter {
        "req" => ReqTag,
        "cf" => CF,
        "tag" => StatDetail,
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
        &["req"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COPR_SCAN_KEYS_STATIC: CoprReqHistogram =
        auto_flush_from!(COPR_SCAN_KEYS, CoprReqHistogram);
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
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_scan_details:
                HashMap::default(),
            local_read_stats:
                ReadStats::default(),
        }
    );
}

impl Into<CF> for GcKeysCF {
    fn into(self) -> CF {
        match self {
            GcKeysCF::default => CF::default,
            GcKeysCF::lock => CF::lock,
            GcKeysCF::write => CF::write,
        }
    }
}

impl Into<StatDetail> for GcKeysDetail {
    fn into(self) -> StatDetail {
        match self {
            GcKeysDetail::total => StatDetail::total,
            GcKeysDetail::processed => StatDetail::processed,
            GcKeysDetail::get => StatDetail::get,
            GcKeysDetail::next => StatDetail::next,
            GcKeysDetail::prev => StatDetail::prev,
            GcKeysDetail::seek => StatDetail::seek,
            GcKeysDetail::seek_for_prev => StatDetail::seek_for_prev,
            GcKeysDetail::over_seek_bound => StatDetail::over_seek_bound,
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
                        .inc_by(*count as i64);
                }
            }
        }

        // Report PD metrics
        if !m.local_read_stats.is_empty() {
            let mut read_stats = ReadStats::default();
            mem::swap(&mut read_stats, &mut m.local_read_stats);
            reporter.report_read_stats(read_stats);
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

pub fn tls_collect_read_flow(region_id: u64, statistics: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats.add_flow(
            region_id,
            &statistics.write.flow_stats,
            &statistics.data.flow_stats,
        );
    });
}

pub fn tls_collect_qps(
    region_id: u64,
    peer: &metapb::Peer,
    start_key: &[u8],
    end_key: &[u8],
    reverse_scan: bool,
) {
    TLS_COP_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_range = build_key_range(start_key, end_key, reverse_scan);
        m.local_read_stats.add_qps(region_id, peer, key_range);
    });
}
