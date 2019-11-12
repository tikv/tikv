// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

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
    pub static ref MULTI_LEVEL_POOL_LEVEL_ELAPSED: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_elapsed",
        "Running time of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_PROPORTIONS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_multi_level_pool_proportions",
        "Proportions of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_RUN: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_run",
        "Run count of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_STOLEN: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_stolen",
        "Stolen count of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_POLL_TIME: HistogramVec = register_histogram_vec!(
        "tikv_multi_level_pool_level_poll_time",
        "Poll time of each level",
        &["name", "level"]
    )
    .unwrap();
}
