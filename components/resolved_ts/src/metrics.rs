// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref RTS_CHANNEL_PENDING_CMD_BYTES: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_channel_pending_cmd_bytes_total",
        "Total bytes of pending commands in the channel"
    )
    .unwrap();
    pub static ref CHECK_LEADER_REQ_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_check_leader_request_size_bytes",
        "Bucketed histogram of the check leader request size",
        exponential_buckets(512.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CHECK_LEADER_REQ_ITEM_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_check_leader_request_item_count",
        "The number of region info in a check leader request",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref PENDING_CHECK_LEADER_REQ_COUNT: IntGauge = register_int_gauge!(
        "tikv_check_leader_request_pending_count",
        "Total number of pending check leader requests"
    )
    .unwrap();
    pub static ref PENDING_CHECK_LEADER_REQ_SENT_COUNT: IntGauge = register_int_gauge!(
        "tikv_check_leader_request_sent_pending_count",
        "Total number of pending sent check leader requests"
    )
    .unwrap();
    pub static ref PENDING_RTS_COUNT: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_pending_count",
        "Total number of pending rts"
    )
    .unwrap();
    pub static ref RTS_MIN_RESOLVED_TS_GAP: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_resolved_ts_gap_millis",
        "The gap between now() and the minimal (non-zero) resolved ts"
    )
    .unwrap();
    pub static ref RTS_RESOLVED_FAIL_ADVANCE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_resolved_ts_fail_advance_count",
        "The count of fail to advance resolved-ts",
        &["reason"]
    )
    .unwrap();
    pub static ref RTS_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_scan_duration_seconds",
        "Bucketed histogram of resolved-ts async scan duration",
        exponential_buckets(0.005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RTS_SCAN_TASKS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resolved_ts_scan_tasks",
        "Total number of resolved-ts scan tasks",
        &["type"]
    )
    .unwrap();
    pub static ref RTS_MIN_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_resolved_ts_region",
        "The region which has minimal resolved ts"
    )
    .unwrap();
    pub static ref RTS_MIN_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_resolved_ts",
        "The minimal (non-zero) resolved ts for observed regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_safe_ts_region",
        "The region id of the follower that has minimal safe ts"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_safe_ts",
        "The minimal (non-zero) safe ts for followers"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_GAP: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_safe_ts_gap_millis",
        "The gap between now() and the minimal (non-zero) safe ts for followers"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_DUATION_TO_LAST_UPDATE_SAFE_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_leader_min_resolved_ts_duration_to_last_update_safe_ts",
        "The duration since last update_safe_ts() called by resolved-ts routine in the leader with min resolved ts. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_DURATION_TO_LAST_CONSUME_LEADER: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_safe_ts_duration_to_last_consume_leader",
        "The duration since last check_leader() in the follower region with min safe ts. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_ZERO_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_zero_resolved_ts",
        "The number of zero resolved ts for observed regions"
    )
    .unwrap();
    pub static ref RTS_LOCK_HEAP_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_lock_heap_bytes",
        "Total bytes in memory of resolved-ts observed regions's lock heap"
    )
    .unwrap();
    pub static ref RTS_LOCK_QUOTA_IN_USE_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_memory_quota_in_use_bytes",
        "Total bytes in memory of resolved-ts observed regions's lock heap"
    )
    .unwrap();
    pub static ref RTS_REGION_RESOLVE_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resolved_ts_region_resolve_status",
        "The status of resolved-ts observed regions",
        &["type"]
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_GAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_min_follower_safe_ts_gap_millis_histogram",
        "Bucketed histogram of the gap between now() and the minimal (non-zero) safe ts for followers",
        exponential_buckets(50.0, 2.0, 20).unwrap(),
    )
    .unwrap();
    pub static ref RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_resolved_ts_check_leader_duration_seconds",
        "Bucketed histogram of resolved-ts check leader duration",
        &["type"],
        exponential_buckets(0.005, 2.0, 20).unwrap(),
    )
    .unwrap();
    pub static ref RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_tikv_client_init_duration_seconds",
        "Bucketed histogram of resolved-ts tikv client initializing duration",
        exponential_buckets(0.005, 2.0, 20).unwrap(),
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_leader_resolved_ts_region",
        "The region whose leader peer has minimal resolved ts"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_RESOLVED_TS_REGION_MIN_LOCK_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_leader_resolved_ts_region_min_lock_ts",
        "The minimal lock ts for the region whose leader peer has minimal resolved ts. 0 means no lock. -1 means no region found."
    )
    .unwrap();
    pub static ref CONCURRENCY_MANAGER_MIN_LOCK_TS: IntGauge = register_int_gauge!(
        "tikv_concurrency_manager_min_lock_ts",
        "The minimal lock ts in concurrency manager. 0 means no lock."
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_leader_resolved_ts",
        "The minimal (non-zero) resolved ts for observe leader peers"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_RESOLVED_TS_GAP: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_leader_resolved_ts_gap_millis",
        "The gap between now() and the minimal (non-zero) resolved ts for leader peers"
    )
    .unwrap();

    // for min_follower_resolved_ts
    pub static ref RTS_MIN_FOLLOWER_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_resolved_ts_region",
        "The region id of the follower has minimal resolved ts"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_resolved_ts",
        "The minimal (non-zero) resolved ts for follower regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_RESOLVED_TS_GAP: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_resolved_ts_gap_millis",
        "The max gap of now() and the minimal (non-zero) resolved ts for follower regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_RESOLVED_TS_DURATION_TO_LAST_CONSUME_LEADER: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_min_follower_resolved_ts_duration_to_last_consume_leader",
        "The duration since last check_leader() in the follower region with min resolved ts. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_INITIAL_SCAN_BACKOFF_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_initial_scan_backoff_duration_seconds",
        "Bucketed histogram of resolved-ts initial scan backoff duration",
        exponential_buckets(0.1, 2.0, 16).unwrap(),
    )
    .unwrap();
}
