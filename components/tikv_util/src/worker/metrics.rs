// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::HIGH_PRIORITY_REGISTRY;
use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref WORKER_PENDING_TASK_VEC: IntGaugeVec = register_int_gauge_vec_with_registry!(
        "tikv_worker_pending_task_total",
        "Current worker pending + running tasks.",
        &["name"],
        HIGH_PRIORITY_REGISTRY
    )
    .unwrap();
    pub static ref WORKER_HANDLED_TASK_VEC: IntCounterVec =
        register_int_counter_vec_with_registry!(
            "tikv_worker_handled_task_total",
            "Total number of worker handled tasks.",
            &["name"],
            HIGH_PRIORITY_REGISTRY
        )
        .unwrap();
}
