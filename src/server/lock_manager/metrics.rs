// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;
use tikv_util::metrics::TLSMetricGroup;

make_static_metric! {
    pub struct LocalTaskCounter: LocalIntCounter {
        "type" => {
            wait_for,
            wake_up,
            dump,
            detect,
            clean_up_wait_for,
            clean_up,
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

    pub struct DetectorHistogramVec: LocalHistogram {
        "type" => {
            monitor_membership_change,
            detect,
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
        exponential_buckets(0.0005, 2.0, 20).unwrap() // 0.5ms ~ 524s
    )
    .unwrap();
    pub static ref DETECTOR_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_lock_manager_detector_histogram",
        "Bucketed histogram of deadlock detector",
        &["type"],
        exponential_buckets(0.0001, 2.0, 20).unwrap() // 0.1ms ~ 104s
    )
    .unwrap();
    pub static ref WAIT_TABLE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
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

thread_local! {
    pub static TASK_COUNTER_METRICS: TLSMetricGroup<LocalTaskCounter> =
        TLSMetricGroup::new(LocalTaskCounter::from(&TASK_COUNTER_VEC));
    pub static ERROR_COUNTER_METRICS: TLSMetricGroup<LocalErrorCounter> =
        TLSMetricGroup::new(LocalErrorCounter::from(&ERROR_COUNTER_VEC));
    pub static DETECTOR_HISTOGRAM_METRICS: TLSMetricGroup<DetectorHistogramVec> =
        TLSMetricGroup::new(DetectorHistogramVec::from(&DETECTOR_HISTOGRAM_VEC));
}
