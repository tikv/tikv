// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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
}
