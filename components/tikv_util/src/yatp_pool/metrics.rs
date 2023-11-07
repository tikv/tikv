// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum PoolName {
        unified_read_pool,
        yatp_pool,
        unknown,
    }

    pub label_enum ResourceGroupPriority {
        high,
        medium,
        low,
        unknown,
    }

    pub struct YATPPoolScheduleWaitDurationVec: LocalHistogram {
        "name" => PoolName,
        "priority" => ResourceGroupPriority,
    }
}

lazy_static! {
    pub static ref YATP_POOL_SCHEDULE_WAIT_DURATION_STATIC: YATPPoolScheduleWaitDurationVec =
    auto_flush_from!(YATP_POOL_SCHEDULE_WAIT_DURATION_VEC, YATPPoolScheduleWaitDurationVec);


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
        &["name","priority"],
        exponential_buckets(1e-5, 4.0, 12).unwrap() // 10us ~ 41s
    )
    .unwrap();
}

impl From<u32> for ResourceGroupPriority {
    fn from(priority: u32) -> Self {
        // the mapping definition of priority in TIDB repo,
        // see: https://github.com/tidb/blob/8b151114546d6a02d8250787a2a3213620e30524/parser/parser.y#L1740-L1752
        match priority {
            1 => ResourceGroupPriority::low,
            8 => ResourceGroupPriority::medium,
            16 => ResourceGroupPriority::high,
            _ => ResourceGroupPriority::unknown,
        }
    }
}

impl From<String> for PoolName {
    fn from(name: String) -> Self {
        match name.as_str() {
            "unified_read_pool" => PoolName::unified_read_pool,
            "yatp_pool" => PoolName::yatp_pool,
            _ => PoolName::unknown,
        }
    }
}
