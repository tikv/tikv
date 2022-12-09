// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref FUTUREPOOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_futurepool_pending_task_total",
        "Current future_pool pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref FUTUREPOOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_futurepool_handled_task_total",
        "Total number of future_pool handled tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref YATP_POOL_SCHEDULE_WAIT_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "tikv_yatp_pool_schedule_wait_duration",
        "Histogram of yatp pool schedule wait duration.",
        &["name"],
        exponential_buckets(1e-5, 4.0, 12).unwrap() // 10us ~ 41s
    )
    .unwrap();
}
