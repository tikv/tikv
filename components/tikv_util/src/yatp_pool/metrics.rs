// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref FUTUREPOOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_futurepool_pending_task_total",
        "Current future_pool pending + running tasks.",
        &["name", "priority"]
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
        &["name", "priority"],
        exponential_buckets(1e-5, 2.0, 22).unwrap() // 10us ~ 42s
    )
    .unwrap();
    pub static ref YATP_POOL_SCHEDULE_EXEC_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "tikv_yatp_pool_schedule_exec_duration",
        "Histogram of yatp pool schedule exec duration.",
        &["name", "priority"],
        exponential_buckets(1e-5, 2.0, 22).unwrap() // 10us ~ 42s
    )
    .unwrap();
    static ref FAIR_POOL_WALL_NANOS: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_wall_nanoseconds_total",
        "Actual synchronous poll wall time in the fair read pool, excluding queue and async waits."
    )
    .unwrap();
    static ref FAIR_POOL_CPU_NANOS: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_cpu_nanoseconds_total",
        "Actual thread CPU time in the fair read pool, including checkpoint charges exactly once."
    )
    .unwrap();
    static ref FAIR_POOL_GATE_PENDING: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_gate_pending_total",
        "Dispatch attempts paused by the fair read pool CPU gate."
    )
    .unwrap();
    static ref FAIR_POOL_REJECTED_GLOBAL: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_rejected_global_total",
        "Read tasks rejected because the global live-task limit could not admit them."
    )
    .unwrap();
    static ref FAIR_POOL_REJECTED_GROUP: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_rejected_group_total",
        "Read tasks rejected because their resource group live-task limit could not admit them."
    )
    .unwrap();
    static ref FAIR_POOL_EVICTED_TASKS: IntCounter = register_int_counter!(
        "tikv_yatp_fair_pool_evicted_tasks_total",
        "Queued read tasks evicted to admit a strictly higher business priority."
    )
    .unwrap();
    static ref FAIR_POOL_POLL_DURATION: Histogram = register_histogram!(
        "tikv_yatp_fair_pool_poll_duration_seconds",
        "Actual synchronous poll wall duration in the fair read pool.",
        // Includes an exact 1ms bucket for the default cooperative slice.
        exponential_buckets(0.00003125, 2.0, 18).unwrap()
    )
    .unwrap();
}

/// Cached, unlabeled handles for the process's fair read pool. Construct once
/// when building the queue; hot paths neither register metrics nor look up
/// resource-group labels. Precharge/reservation must never enter these
/// counters.
#[derive(Clone)]
pub struct FairQueueMetrics {
    pub wall_nanos: IntCounter,
    pub cpu_nanos: IntCounter,
    pub gate_pending: IntCounter,
    pub rejected_global: IntCounter,
    pub rejected_group: IntCounter,
    pub evicted_tasks: IntCounter,
    pub poll_duration: Histogram,
}

impl Default for FairQueueMetrics {
    fn default() -> Self {
        Self {
            wall_nanos: FAIR_POOL_WALL_NANOS.clone(),
            cpu_nanos: FAIR_POOL_CPU_NANOS.clone(),
            gate_pending: FAIR_POOL_GATE_PENDING.clone(),
            rejected_global: FAIR_POOL_REJECTED_GLOBAL.clone(),
            rejected_group: FAIR_POOL_REJECTED_GROUP.clone(),
            evicted_tasks: FAIR_POOL_EVICTED_TASKS.clone(),
            poll_duration: FAIR_POOL_POLL_DURATION.clone(),
        }
    }
}

impl FairQueueMetrics {
    /// Record each completed poll once, including both checkpoint CPU charges
    /// and its final CPU tail. The inputs are actual durations, never VT.
    #[inline]
    pub fn record_poll(&self, wall: Duration, cpu: Duration) {
        self.wall_nanos
            .inc_by(wall.as_nanos().min(u64::MAX as u128) as u64);
        self.cpu_nanos
            .inc_by(cpu.as_nanos().min(u64::MAX as u128) as u64);
        self.poll_duration.observe(wall.as_secs_f64());
    }
}
