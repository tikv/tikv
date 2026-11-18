// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::OnceCell, time::Duration};

use lazy_static::lazy_static;
use prometheus::{
    local::{LocalHistogram, LocalIntCounter},
    *,
};

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

// All fair queues report to the same process-wide metrics. Workers from other
// YATP pools can tick without allocating a histogram or registering metrics.
thread_local! {
    static POLL_METRICS: OnceCell<LocalPollMetrics> = const { OnceCell::new() };
}

struct LocalPollMetrics {
    wall_nanos: LocalIntCounter,
    cpu_nanos: LocalIntCounter,
    poll_duration: LocalHistogram,
}

impl LocalPollMetrics {
    fn flush(&self) {
        self.wall_nanos.flush();
        self.cpu_nanos.flush();
        self.poll_duration.flush();
    }
}

impl Drop for LocalPollMetrics {
    fn drop(&mut self) {
        // A worker can exit before its next tick. LocalIntCounter has no
        // drop-time flush of its own.
        self.flush();
    }
}

impl FairQueueMetrics {
    /// Record directly to these handles outside the worker-local reporting
    /// path.
    #[inline]
    pub fn record_poll(&self, wall: Duration, cpu: Duration) {
        self.wall_nanos
            .inc_by(wall.as_nanos().min(u64::MAX as u128) as u64);
        self.cpu_nanos
            .inc_by(cpu.as_nanos().min(u64::MAX as u128) as u64);
        self.poll_duration.observe(wall.as_secs_f64());
    }

    /// Fair-queue workers all write to the same three process-wide collectors.
    pub(super) fn record_worker_poll(&self, wall: Duration, cpu: Duration) {
        let recorded = POLL_METRICS.try_with(|local| {
            let local = local.get_or_init(|| LocalPollMetrics {
                wall_nanos: self.wall_nanos.local(),
                cpu_nanos: self.cpu_nanos.local(),
                poll_duration: self.poll_duration.local(),
            });
            local
                .wall_nanos
                .inc_by(wall.as_nanos().min(u64::MAX as u128) as u64);
            local
                .cpu_nanos
                .inc_by(cpu.as_nanos().min(u64::MAX as u128) as u64);
            local.poll_duration.observe(wall.as_secs_f64());
        });
        if recorded.is_err() {
            // A prepared dispatch can settle during TLS destruction, after
            // this worker has already flushed and dropped its local metrics.
            self.record_poll(wall, cpu);
        }
    }
}

pub(super) fn flush_poll_metrics() {
    POLL_METRICS.with(|local| {
        if let Some(metrics) = local.get() {
            metrics.flush();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_poll_metrics_flush_on_tick_and_exit() {
        struct FinalPoll(FairQueueMetrics);
        impl Drop for FinalPoll {
            fn drop(&mut self) {
                self.0
                    .record_worker_poll(Duration::from_millis(1), Duration::from_millis(1));
            }
        }
        thread_local! { static FINAL_POLL: OnceCell<FinalPoll> = const { OnceCell::new() }; }

        // Use private collectors so other pools running in parallel cannot
        // contribute to the totals checked here.
        let wall = IntCounter::new("wall", "wall").unwrap();
        let cpu = IntCounter::new("cpu", "cpu").unwrap();
        let histogram = Histogram::with_opts(HistogramOpts::new("poll", "poll")).unwrap();
        let metrics = FairQueueMetrics {
            wall_nanos: wall.clone(),
            cpu_nanos: cpu.clone(),
            poll_duration: histogram.clone(),
            ..FairQueueMetrics::default()
        };
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                // A dispatch prepared before metrics initialization may be
                // destroyed after the worker's local metrics.
                FINAL_POLL.with(|pending| {
                    pending.get_or_init(|| FinalPoll(metrics.clone()));
                });
                metrics.record_worker_poll(Duration::from_millis(4), Duration::from_millis(2));
                assert_eq!(
                    (wall.get(), cpu.get(), histogram.get_sample_count()),
                    (0, 0, 0)
                );
                flush_poll_metrics();
                flush_poll_metrics();
                assert_eq!(
                    (wall.get(), cpu.get(), histogram.get_sample_count()),
                    (4_000_000, 2_000_000, 1)
                );
                // Leave a partial batch when the worker exits before its next tick.
                metrics.record_worker_poll(Duration::from_millis(8), Duration::from_millis(3));
            });
            // The implicit scoped join can return before TLS destructors run.
            worker.join().unwrap();
        });
        assert_eq!(
            (wall.get(), cpu.get(), histogram.get_sample_count()),
            (13_000_000, 6_000_000, 3)
        );
        assert!((histogram.get_sample_sum() - 0.013).abs() < f64::EPSILON);
    }
}
