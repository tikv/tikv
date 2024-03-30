// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref RTS_CHANNEL_PENDING_CMD_BYTES: IntGauge = register_int_gauge!(
        "tikv_watermark_channel_pending_cmd_bytes_total",
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
        "tikv_watermark_pending_count",
        "Total number of pending rts"
    )
    .unwrap();
    pub static ref RTS_MIN_WATERMARK_GAP: IntGauge = register_int_gauge!(
        "tikv_watermark_min_watermark_gap_millis",
        "The gap between now() and the minimal (non-zero) watermark"
    )
    .unwrap();
    pub static ref RTS_RESOLVED_FAIL_ADVANCE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_watermark_fail_advance_count",
        "The count of fail to advance watermark",
        &["reason"]
    )
    .unwrap();
    pub static ref RTS_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_watermark_scan_duration_seconds",
        "Bucketed histogram of watermark async scan duration",
        exponential_buckets(0.005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RTS_SCAN_TASKS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_watermark_scan_tasks",
        "Total number of watermark scan tasks",
        &["type"]
    )
    .unwrap();
    pub static ref RTS_MIN_WATERMARK_REGION: IntGauge = register_int_gauge!(
        "tikv_watermark_min_watermark_region",
        "The region which has minimal watermark"
    )
    .unwrap();
    pub static ref RTS_MIN_WATERMARK: IntGauge = register_int_gauge!(
        "tikv_watermark_min_watermark",
        "The minimal (non-zero) watermark for observed regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_safe_ts_region",
        "The region id of the follower that has minimal safe ts"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_safe_ts",
        "The minimal (non-zero) safe ts for followers"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_GAP: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_safe_ts_gap_millis",
        "The gap between now() and the minimal (non-zero) safe ts for followers"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_DUATION_TO_LAST_UPDATE_SAFE_TS: IntGauge = register_int_gauge!(
        "tikv_watermark_leader_min_watermark_duration_to_last_update_safe_ts",
        "The duration since last update_safe_ts() called by watermark routine in the leader with min watermark. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_SAFE_TS_DURATION_TO_LAST_CONSUME_LEADER: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_safe_ts_duration_to_last_consume_leader",
        "The duration since last check_leader() in the follower region with min safe ts. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_ZERO_WATERMARK: IntGauge = register_int_gauge!(
        "tikv_watermark_zero_watermark",
        "The number of zero watermark for observed regions"
    )
    .unwrap();
    pub static ref RTS_LOCK_HEAP_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_watermark_lock_heap_bytes",
        "Total bytes in memory of watermark observed regions's lock heap"
    )
    .unwrap();
    pub static ref RTS_LOCK_QUOTA_IN_USE_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_watermark_memory_quota_in_use_bytes",
        "Total bytes in memory of watermark observed regions's lock heap"
    )
    .unwrap();
    pub static ref RTS_REGION_RESOLVE_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_watermark_region_resolve_status",
        "The status of watermark observed regions",
        &["type"]
    )
    .unwrap();
    pub static ref RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_watermark_check_leader_duration_seconds",
        "Bucketed histogram of watermark check leader duration",
        &["type"],
        exponential_buckets(0.005, 2.0, 20).unwrap(),
    )
    .unwrap();
    pub static ref RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_watermark_tikv_client_init_duration_seconds",
        "Bucketed histogram of watermark tikv client initializing duration",
        exponential_buckets(0.005, 2.0, 20).unwrap(),
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_WATERMARK_REGION: IntGauge = register_int_gauge!(
        "tikv_watermark_min_leader_watermark_region",
        "The region whose leader peer has minimal watermark"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_WATERMARK_REGION_MIN_LOCK_TS: IntGauge = register_int_gauge!(
        "tikv_watermark_min_leader_watermark_region_min_lock_ts",
        "The minimal lock ts for the region whose leader peer has minimal watermark. 0 means no lock. -1 means no region found."
    )
    .unwrap();
    pub static ref CONCURRENCY_MANAGER_MIN_LOCK_TS: IntGauge = register_int_gauge!(
        "tikv_concurrency_manager_min_lock_ts",
        "The minimal lock ts in concurrency manager. 0 means no lock."
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_WATERMARK: IntGauge = register_int_gauge!(
        "tikv_watermark_min_leader_watermark",
        "The minimal (non-zero) watermark for observe leader peers"
    )
    .unwrap();
    pub static ref RTS_MIN_LEADER_WATERMARK_GAP: IntGauge = register_int_gauge!(
        "tikv_watermark_min_leader_watermark_gap_millis",
        "The gap between now() and the minimal (non-zero) watermark for leader peers"
    )
    .unwrap();

    // for min_follower_watermark
    pub static ref RTS_MIN_FOLLOWER_WATERMARK_REGION: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_watermark_region",
        "The region id of the follower has minimal watermark"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_WATERMARK: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_watermark",
        "The minimal (non-zero) watermark for follower regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_WATERMARK_GAP: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_watermark_gap_millis",
        "The max gap of now() and the minimal (non-zero) watermark for follower regions"
    )
    .unwrap();
    pub static ref RTS_MIN_FOLLOWER_WATERMARK_DURATION_TO_LAST_CONSUME_LEADER: IntGauge = register_int_gauge!(
        "tikv_watermark_min_follower_watermark_duration_to_last_consume_leader",
        "The duration since last check_leader() in the follower region with min watermark. -1 denotes None."
    )
    .unwrap();
    pub static ref RTS_INITIAL_SCAN_BACKOFF_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_watermark_initial_scan_backoff_duration_seconds",
        "Bucketed histogram of watermark initial scan backoff duration",
        exponential_buckets(0.1, 2.0, 16).unwrap(),
    )
    .unwrap();
}
