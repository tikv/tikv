// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref WORKER_PENDING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_worker_pending_task_total",
        "Current worker pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref WORKER_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_worker_handled_task_total",
        "Total number of worker handled tasks.",
        &["name"]
    )
    .unwrap();
}
