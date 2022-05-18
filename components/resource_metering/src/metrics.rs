// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref STAT_TASK_COUNT: IntCounter = register_int_counter!(
        "tikv_resource_metering_stat_task_count",
        "Counter of times to read the stat of tasks from procfs"
    )
    .unwrap();
    pub static ref REPORT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resource_metering_report_duration_seconds",
        "Bucketed histogram of reporting time (s) to the resource metering clients"
    )
    .unwrap();
    pub static ref REPORT_DATA_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_report_data_count",
        "Total number of reporting data",
        &["type"]
    )
    .unwrap();
    pub static ref IGNORED_DATA_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_ignored_data",
        "Total number of ignored data",
        &["type"]
    )
    .unwrap();
}
