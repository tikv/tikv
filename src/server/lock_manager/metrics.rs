// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub struct TaskCounterVec: IntCounter {
        "type" => {
            wait_for,
            wake_up,
            dump,
            detect,
            clean_up_wait_for,
            clean_up,
        },
    }

    pub struct ErrorCounterVec: IntCounter {
        "type" => {
            dropped,
            not_leader,
            reconnect_leader,
            leader_not_found,
            deadlock,
        },
    }

    pub struct WaitTableStatusGauge: IntGauge {
        "type" => {
            locks,
            txns,
        },
    }
}

lazy_static! {
    pub static ref TASK_COUNTER_VEC: TaskCounterVec = register_static_int_counter_vec!(
        TaskCounterVec,
        "tikv_lock_manager_task_counter",
        "Total number of tasks received",
        &["type"]
    )
    .unwrap();
    pub static ref ERROR_COUNTER_VEC: ErrorCounterVec = register_static_int_counter_vec!(
        ErrorCounterVec,
        "tikv_lock_manager_error_counter",
        "Total number of errors",
        &["type"]
    )
    .unwrap();
    pub static ref WAITER_LIFETIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_lock_manager_waiter_lifetime_duration",
        "Duration of waiters' lifetime in seconds",
        exponential_buckets(0.0005, 2.0, 20).unwrap() // 0.5ms ~ 524s
    )
    .unwrap();
    pub static ref DETECT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_lock_manager_detect_duration",
        "Duration of handling detect requests",
        exponential_buckets(0.0001, 2.0, 20).unwrap() // 0.1ms ~ 104s
    )
    .unwrap();
    pub static ref WAIT_TABLE_STATUS_GAUGE: WaitTableStatusGauge = register_static_int_gauge_vec!(
        WaitTableStatusGauge,
        "tikv_lock_manager_wait_table_status",
        "Status of the wait table",
        &["type"]
    )
    .unwrap();
    pub static ref DETECTOR_LEADER_GAUGE: IntGauge = register_int_gauge!(
        "tikv_lock_manager_detector_leader_heartbeat",
        "Heartbeat of the leader of the deadlock detector"
    )
    .unwrap();
}
