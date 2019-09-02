// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;
use std::sync::{Arc, Mutex};

use hdrhistogram::Histogram as HdrHistogram;
use prometheus::exponential_buckets;

use crate::storage::ErrorHeaderKind;
use tikv_util::collections::HashMap;
use tikv_util::time::Instant;

make_static_metric! {
    pub label_enum GrpcTypeKind {
        invalid,
        kv_get,
        kv_scan,
        kv_prewrite,
        kv_pessimistic_lock,
        kv_pessimistic_rollback,
        kv_commit,
        kv_cleanup,
        kv_batch_get,
        kv_batch_rollback,
        kv_scan_lock,
        kv_resolve_lock,
        kv_gc,
        kv_delete_range,
        raw_get,
        raw_batch_get,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_range,
        raw_batch_delete,
        unsafe_destroy_range,
        coprocessor,
        coprocessor_stream,
        mvcc_get_by_key,
        mvcc_get_by_start_ts,
        split_region,
        read_index,
    }
    pub struct GrpcMsgHistogramVec: Histogram {
        "type" => GrpcTypeKind,
    }
    pub struct GrpcMsgFailCounterVec: IntCounter {
        "type" => GrpcTypeKind,
    }
}

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_server_send_snapshot_duration_seconds",
        "Bucketed histogram of server send snapshots duration",
        exponential_buckets(0.05, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SNAP_TASK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_snapshot_task_total",
        "Total number of snapshot task",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_MSG_HISTOGRAM_VEC: GrpcMsgHistogramVec = register_static_histogram_vec!(
        GrpcMsgHistogramVec,
        "tikv_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GRPC_MSG_FAIL_COUNTER: GrpcMsgFailCounterVec = register_static_int_counter_vec!(
        GrpcMsgFailCounterVec,
        "tikv_grpc_msg_fail_total",
        "Total number of handle grpc message failure",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_REQ_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "tikv_server_grpc_req_batch_size",
        "grpc batch size of gRPC requests",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref GRPC_RESP_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "tikv_server_grpc_resp_batch_size",
        "grpc batch size of gRPC responses",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_RECV_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_recv_total",
        "Total number of raft messages received"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_BATCH_SIZE: Histogram = register_histogram!(
        "tikv_server_raft_message_batch_size",
        "Raft messages batch size",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref RESOLVE_STORE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_resolve_store_total",
        "Total number of resolving store",
        &["type"]
    )
    .unwrap();
    pub static ref REPORT_FAILURE_MSG_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_report_failure_msg_total",
        "Total number of reporting failure messages",
        &["type", "store_id"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_flush_total",
        "Total number of raft messages flushed immediately"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_DELAY_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_delay_flush_total",
        "Total number of raft messages flushed delay"
    )
    .unwrap();
    pub static ref CONFIG_ROCKSDB_GAUGE: GaugeVec = register_gauge_vec!(
        "tikv_config_rocksdb",
        "Config information of rocksdb",
        &["cf", "name"]
    )
    .unwrap();
}

make_static_metric! {
    pub label_enum RequestStatusKind {
        all,
        success,
        err_timeout,
        err_empty_request,
        err_other,
        err_io,
        err_server,
        err_invalid_resp,
        err_invalid_req,
        err_not_leader,
        err_region_not_found,
        err_key_not_in_region,
        err_epoch_not_match,
        err_server_is_busy,
        err_stale_command,
        err_store_not_match,
        err_raft_entry_too_large,
    }

    pub label_enum RequestTypeKind {
        write,
        snapshot,
    }

    pub struct AsyncRequestsCounterVec: IntCounter {
        "type" => RequestTypeKind,
        "status" => RequestStatusKind,
    }

    pub struct AsyncRequestsDurationVec: Histogram {
        "type" => RequestTypeKind,
    }
}

impl From<ErrorHeaderKind> for RequestStatusKind {
    fn from(kind: ErrorHeaderKind) -> Self {
        match kind {
            ErrorHeaderKind::NotLeader => RequestStatusKind::err_not_leader,
            ErrorHeaderKind::RegionNotFound => RequestStatusKind::err_region_not_found,
            ErrorHeaderKind::KeyNotInRegion => RequestStatusKind::err_key_not_in_region,
            ErrorHeaderKind::EpochNotMatch => RequestStatusKind::err_epoch_not_match,
            ErrorHeaderKind::ServerIsBusy => RequestStatusKind::err_server_is_busy,
            ErrorHeaderKind::StaleCommand => RequestStatusKind::err_stale_command,
            ErrorHeaderKind::StoreNotMatch => RequestStatusKind::err_store_not_match,
            ErrorHeaderKind::RaftEntryTooLarge => RequestStatusKind::err_raft_entry_too_large,
            ErrorHeaderKind::Other => RequestStatusKind::err_other,
        }
    }
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER_VEC: AsyncRequestsCounterVec = {
        register_static_int_counter_vec!(
            AsyncRequestsCounterVec,
            "tikv_storage_engine_async_request_total",
            "Total number of engine asynchronous requests",
            &["type", "status"]
        )
        .unwrap()
    };
    pub static ref ASYNC_REQUESTS_DURATIONS_VEC: AsyncRequestsDurationVec = {
        register_static_histogram_vec!(
            AsyncRequestsDurationVec,
            "tikv_storage_engine_async_request_duration_seconds",
            "Bucketed histogram of processing successful asynchronous requests.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap()
    };
}

// Defines a HdrHistogram, its minimum resolution value and maximum trackable time interval are about 500us and 1000s, respectively.
macro_rules! define_hdrhistogram {
    ($NAME: ident) => {
        lazy_static! {
            pub static ref $NAME: Arc<Mutex<HdrHistogram<u64>>> = Arc::new(Mutex::new(
                HdrHistogram::<u64>::new_with_bounds(1 << 9, 1 << 30, 3).unwrap()
            ));
        }
    };
}

define_hdrhistogram!(INVALID_HDR);
define_hdrhistogram!(KV_GET_HDR);
define_hdrhistogram!(KV_SCAN_HDR);
define_hdrhistogram!(KV_PREWRITE_HDR);
define_hdrhistogram!(KV_PESSIMISTIC_LOCK_HDR);
define_hdrhistogram!(KV_PESSIMISTIC_ROLLBACK_HDR);
define_hdrhistogram!(KV_COMMIT_HDR);
define_hdrhistogram!(KV_CLEANUP_HDR);
define_hdrhistogram!(KV_BATCH_GET_HDR);
define_hdrhistogram!(KV_BATCH_ROLLBACK_HDR);
define_hdrhistogram!(KV_SCAN_LOCK_HDR);
define_hdrhistogram!(KV_RESOLVE_LOCK_HDR);
define_hdrhistogram!(KV_GC_HDR);
define_hdrhistogram!(KV_DELETE_RANGE_HDR);
define_hdrhistogram!(RAW_GET_HDR);
define_hdrhistogram!(RAW_BATCH_GET_HDR);
define_hdrhistogram!(RAW_SCAN_HDR);
define_hdrhistogram!(RAW_BATCH_SCAN_HDR);
define_hdrhistogram!(RAW_PUT_HDR);
define_hdrhistogram!(RAW_BATCH_PUT_HDR);
define_hdrhistogram!(RAW_DELETE_HDR);
define_hdrhistogram!(RAW_DELETE_RANGE_HDR);
define_hdrhistogram!(RAW_BATCH_DELETE_HDR);
define_hdrhistogram!(UNSAFE_DESTROY_RANGE_HDR);
define_hdrhistogram!(COPROCESSOR_HDR);
define_hdrhistogram!(COPROCESSOR_STREAM_HDR);
define_hdrhistogram!(MVCC_GET_BY_KEY_HDR);
define_hdrhistogram!(MVCC_GET_BY_START_TS_HDR);
define_hdrhistogram!(SPLIT_REGION_HDR);
define_hdrhistogram!(READ_INDEX_HDR);

pub struct HdrTimer {
    start: Instant,
    hdr: Arc<Mutex<HdrHistogram<u64>>>,
}

impl HdrTimer {
    pub fn new(hdr: &Arc<Mutex<HdrHistogram<u64>>>) -> Self {
        Self {
            hdr: hdr.clone(),
            start: Instant::now_coarse(),
        }
    }

    pub fn observe_duration(self) {
        drop(self);
    }

    fn observe(&mut self) {
        let v = self.start.elapsed().as_micros() as u64;
        self.hdr.lock().unwrap().record(v).unwrap();
    }
}

impl Drop for HdrTimer {
    fn drop(&mut self) {
        self.observe();
    }
}

/// Use to collect each operation's latency with specified quantile, e.g., P95 or P99 lantency.
pub struct OpLatencyStatistics {
    quantile: f64,
    // Measured in the unit of microsecond (us)
    latencies: HashMap<String, u64>,
}

impl OpLatencyStatistics {
    pub fn new(quantile: f64) -> Self {
        Self {
            quantile,
            latencies: HashMap::default(),
        }
    }

    pub fn record(&mut self) {
        macro_rules! update_latency {
            ($NAME: ident) => {
                let latency = self
                    .latencies
                    .entry(stringify!($NAME).to_owned())
                    .or_insert(0);
                *latency = $NAME.lock().unwrap().value_at_quantile(self.quantile);
            };
        }

        update_latency!(INVALID_HDR);
        update_latency!(KV_GET_HDR);
        update_latency!(KV_SCAN_HDR);
        update_latency!(KV_PREWRITE_HDR);
        update_latency!(KV_PESSIMISTIC_LOCK_HDR);
        update_latency!(KV_PESSIMISTIC_ROLLBACK_HDR);
        update_latency!(KV_COMMIT_HDR);
        update_latency!(KV_CLEANUP_HDR);
        update_latency!(KV_BATCH_GET_HDR);
        update_latency!(KV_BATCH_ROLLBACK_HDR);
        update_latency!(KV_SCAN_LOCK_HDR);
        update_latency!(KV_RESOLVE_LOCK_HDR);
        update_latency!(KV_GC_HDR);
        update_latency!(KV_DELETE_RANGE_HDR);
        update_latency!(RAW_GET_HDR);
        update_latency!(RAW_BATCH_GET_HDR);
        update_latency!(RAW_SCAN_HDR);
        update_latency!(RAW_BATCH_SCAN_HDR);
        update_latency!(RAW_PUT_HDR);
        update_latency!(RAW_BATCH_PUT_HDR);
        update_latency!(RAW_DELETE_HDR);
        update_latency!(RAW_DELETE_RANGE_HDR);
        update_latency!(RAW_BATCH_DELETE_HDR);
        update_latency!(UNSAFE_DESTROY_RANGE_HDR);
        update_latency!(COPROCESSOR_HDR);
        update_latency!(COPROCESSOR_STREAM_HDR);
        update_latency!(MVCC_GET_BY_KEY_HDR);
        update_latency!(MVCC_GET_BY_START_TS_HDR);
        update_latency!(SPLIT_REGION_HDR);
        update_latency!(READ_INDEX_HDR);
    }

    pub fn get_op_latencies(&self) -> HashMap<String, u64> {
        self.latencies.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_hdr_histogram() {
        let h = Arc::new(Mutex::new(
            HdrHistogram::<u64>::new_with_bounds(1 << 9, 1 << 30, 3).unwrap(),
        ));

        let hdr_timer = HdrTimer::new(&h);
        thread::sleep(Duration::from_millis(10));
        hdr_timer.observe_duration();

        let mut hdr_histogram = h.lock().unwrap();
        hdr_histogram.record(20000).unwrap();

        assert_eq!(hdr_histogram.len(), 2);

        let count = hdr_histogram.count_between(1000, 100000);
        assert_eq!(count, 2);
    }
}
