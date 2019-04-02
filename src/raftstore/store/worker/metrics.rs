// Copyright 2016 TiKV Project Authors.
use prometheus::{exponential_buckets, Gauge, Histogram, HistogramVec, IntCounterVec};

lazy_static! {
    pub static ref SNAP_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_snapshot_total",
        "Total number of raftstore snapshot processed.",
        &["type", "status"]
    )
    .unwrap();
    pub static ref CHECK_SPILT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_check_split_total",
        "Total number of raftstore split check.",
        &["type"]
    )
    .unwrap();
    pub static ref SNAP_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_snapshot_duration_seconds",
        "Bucketed histogram of raftstore snapshot process duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CHECK_SPILT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_check_split_duration_seconds",
        "Bucketed histogram of raftstore split check duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COMPACT_RANGE_CF: HistogramVec = register_histogram_vec!(
        "tikv_compact_range_cf_duration_seconds",
        "Bucketed histogram of compact range for cf execution",
        &["cf"]
    )
    .unwrap();
    pub static ref REGION_HASH_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_hash_duration_seconds",
        "Bucketed histogram of raftstore hash compution duration"
    )
    .unwrap();
    pub static ref STALE_PEER_PENDING_DELETE_RANGE_GAUGE: Gauge = register_gauge!(
        "tikv_pending_delete_ranges_of_stale_peer",
        "Total number of tikv pending delete range of stale peer"
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_local_read_reject_total",
        "Total number of rejections from the local read thread.",
        &["reason"]
    )
    .unwrap();
    pub static ref LOCAL_READ_WAIT_DURATION: Histogram = register_histogram!(
        "tikv_raftstore_local_read_requests_wait_duration",
        "Bucketed histogram of local read requests wait duration.",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref LOCAL_READ_BATCH_REQUESTS: Histogram = register_histogram!(
        "tikv_raftstore_local_read_batch_requests_total",
        "Bucketed histogram of local read batch requests size.",
        exponential_buckets(1.0, 2.0, 15).unwrap()
    )
    .unwrap();
}
