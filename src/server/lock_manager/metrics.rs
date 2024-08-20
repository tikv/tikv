// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub struct LocalTaskCounter: LocalIntCounter {
        "type" => {
            wait_for,
            wake_up,
            dump,
            detect,
            clean_up_wait_for,
            clean_up,
            update_wait_for,
        },
    }

    pub struct LocalErrorCounter: LocalIntCounter {
        "type" => {
            dropped,
            not_leader,
            reconnect_leader,
            leader_not_found,
            deadlock,
        },
    }
}

make_static_metric! {
    pub struct WaitTableStatusGauge: IntGauge {
        "type" => {
            locks,
            txns,
        },
    }
}

lazy_static! {
    pub static ref TASK_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_lock_manager_task_counter",
        "Total number of tasks received",
        &["type"]
    )
    .unwrap();
    pub static ref ERROR_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_lock_manager_error_counter",
        "Total number of errors",
        &["type"]
    )
    .unwrap();
    pub static ref WAITER_LIFETIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_lock_manager_waiter_lifetime_duration",
        "Duration of waiters' lifetime in seconds",
        exponential_buckets(0.00001, 2.0, 26).unwrap() // 0.5ms ~ 524s
    )
    .unwrap();
    pub static ref DETECT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_lock_manager_detect_duration",
        "Duration of handling detect requests",
        exponential_buckets(0.0001, 2.0, 20).unwrap() // 0.1ms ~ 104s
    )
    .unwrap();
    pub static ref DETECTOR_LEADER_GAUGE: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_leader_heartbeat",
        "Heartbeat of the leader of the deadlock detector"
    )
    .unwrap();
    pub static ref DETECTOR_SEND_CHANNEL_SIZE_GAUGE: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_send_buffer_size",
        "Size of the send buffer of the deadlock detector"
    )
    .unwrap();
    pub static ref WAITER_WAIT_TABLE_ESTIMATED_MEM: IntGauge = register_int_gauge!(
        "tikv_lock_manager_waiter_wait_table_estimated_mem",
        "Estimated memory usage of the wait table"
    )
    .unwrap();
    pub static ref DETECTOR_WAIT_FOR_MAP_SIZE: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_wait_for_map_size",
        "The number of entries in the wait_for_map"
    )
    .unwrap();
    pub static ref DETECTOR_TOTAL_BLOCKERS: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_total_blockers",
        "The number of blockers in wait_for_map; 1 txn can be counted more than once"
    )
    .unwrap();
    pub static ref DETECTOR_TOTAL_KEYS: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_total_keys",
        "The number of keys in wait_for_map"
    )
    .unwrap();

    pub static ref TASK_COUNTER_METRICS: LocalTaskCounter =
        auto_flush_from!(TASK_COUNTER_VEC, LocalTaskCounter);
    pub static ref ERROR_COUNTER_METRICS: LocalErrorCounter =
        auto_flush_from!(ERROR_COUNTER_VEC,LocalErrorCounter);
}
