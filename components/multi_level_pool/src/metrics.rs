// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

#[derive(Clone)]
pub struct TaskSourceCount {
    injectors: [IntCounter; 3],
    local: IntCounter,
    steal: IntCounter,
}

impl TaskSourceCount {
    pub fn new(name: &str) -> Self {
        TaskSourceCount {
            injectors: [
                MULTI_LEVEL_POOL_TASK_SOURCE.with_label_values(&[name, "L0"]),
                MULTI_LEVEL_POOL_TASK_SOURCE.with_label_values(&[name, "L1"]),
                MULTI_LEVEL_POOL_TASK_SOURCE.with_label_values(&[name, "L2"]),
            ],
            local: MULTI_LEVEL_POOL_TASK_SOURCE.with_label_values(&[name, "local"]),
            steal: MULTI_LEVEL_POOL_TASK_SOURCE.with_label_values(&[name, "steal"]),
        }
    }

    pub fn inc_injector(&self, level: usize) {
        self.injectors[level].inc();
    }

    pub fn inc_local(&self) {
        self.local.inc();
    }

    pub fn inc_steal(&self) {
        self.steal.inc();
    }
}

lazy_static! {
    pub static ref MULTI_LEVEL_POOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_multi_level_pool_pending_task_total",
        "Current multi-level pool pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_handled_task_total",
        "Total number of multi-level pool handled tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_POLL_TIMES: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_poll_times",
        "Poll times of tasks in each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_ELAPSED: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_elapsed",
        "Running time of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_PROPORTIONS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_multi_level_pool_proportions",
        "Sum proportions above the specific level (with 2^32 as denominator)",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_TASK_SOURCE: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_task_source",
        "Count of task source of each polling",
        &["name", "source"]
    )
    .unwrap();
}
