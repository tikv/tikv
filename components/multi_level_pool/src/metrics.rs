// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use once_cell::sync::Lazy;
use prometheus::*;

pub static FUTUREPOOL_RUNNING_TASK_VEC: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "tikv_futurepool_pending_task_total",
        "Current future_pool pending + running tasks.",
        &["name"]
    )
    .unwrap()
});
pub static FUTUREPOOL_HANDLED_TASK_VEC: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "tikv_futurepool_handled_task_total",
        "Total number of future_pool handled tasks.",
        &["name"]
    )
    .unwrap()
});
