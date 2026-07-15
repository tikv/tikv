// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Common resource-pressure scoring shared by background quota adjustment
//! (`worker.rs`) and foreground/read-pool throttling (`resource_group.rs`).
//!
//! `compute_resource_scores` produces three independent 0-100 scores —
//! `cpu_score`, `io_score`, `compaction_score` — each simply the resource's
//! utilization percentage, clamped to `[0, 100]`. The CPU score is the max of
//! whole-process and grpc normalized utilization, so pressure is detected on
//! whichever signal is hottest.
//!
//! Background quota adjustment turns its score into a `[0, 1]` pressure
//! fraction via [`pressure_fraction`], over its own
//! `(bg_cpu_throttle_threshold, fg_cpu_throttle_threshold)` range.
//! Foreground/read-pool throttling instead just tightens whatever's currently
//! in effect by a fixed percentage per tick once engaged — see
//! `resource_group.rs::adjust_group_throttling` and
//! `compute_read_pool_target_cpu`.

use std::time::Duration;

use tikv_util::{
    sys::thread::{full_thread_stat, process_id, thread_ids, ticks_per_second},
    thread_name_prefix::matches_thread_name_prefix,
    time::Instant,
};

/// Test-only: a cpu_score high enough to guarantee maximum pressure
/// regardless of configured thresholds, used to drive foreground/read-pool
/// throttling tests unconditionally into their "fully engaged" branch.
#[cfg(test)]
pub(crate) const TARGET_CPU: f64 = 90.0;

/// Maps `score` onto a `[0, 1]` pressure fraction: `0` at `start`, `1` at
/// `end`, linear in between, clamped.
pub(crate) fn pressure_fraction(score: f64, start: f64, end: f64) -> f64 {
    let range = (end - start).max(1.0);
    ((score - start) / range).clamp(0.0, 1.0)
}

/// Raw measurements feeding [`compute_resource_scores`].
pub struct ResourceScoreInputs {
    /// Whole-process CPU utilization percentage
    /// (`cpu_stats.current_used / cpu_stats.total_quota * 100`).
    pub process_cpu_util: f64,
    /// Measured grpc-server CPU usage, in cores.
    pub grpc_cpu_cores: f64,
    /// IO utilization percentage (`io_stats.current_used /
    /// io_stats.total_quota * 100`); 0 when IO quota is unlimited.
    pub io_util: f64,
    /// Raw compaction pending-bytes ratio, range `[0, 100+]`.
    pub compaction_pending_ratio: f64,
}

/// Static capacities used to normalize per-pool CPU measurements into
/// utilization percentages, comparable to whole-process CPU utilization.
pub struct ResourceCapacities {
    /// `SysQuota::cpu_cores_quota()`.
    pub total_cpu_cores: f64,
    /// `config.server.grpc_concurrency`.
    pub grpc_concurrency: f64,
}

/// Output of [`compute_resource_scores`]: three independent 0-100 scores,
/// each a plain utilization percentage clamped to `[0, 100]`.
pub struct ResourceScores {
    /// CPU utilization score: the max of process and grpc normalized
    /// utilization.
    pub cpu_score: f64,
    /// IO utilization score (background-only).
    pub io_score: f64,
    /// Compaction pending-bytes score (background-only); the input ratio
    /// clamped to `[0, 100]`.
    pub compaction_score: f64,
}

/// Computes the three resource-utilization scores from raw measurements.
/// Each score is a plain percentage in `[0, 100]` — 100% CPU utilization
/// yields `cpu_score == 100.0`, 10% yields `10.0`.
pub fn compute_resource_scores(
    inputs: &ResourceScoreInputs,
    caps: &ResourceCapacities,
) -> ResourceScores {
    let grpc_util = if caps.grpc_concurrency > 0.0 {
        inputs.grpc_cpu_cores / caps.grpc_concurrency * 100.0
    } else {
        0.0
    };
    let max_cpu_util = inputs.process_cpu_util.max(grpc_util);

    ResourceScores {
        cpu_score: max_cpu_util.clamp(0.0, 100.0),
        io_score: inputs.io_util.clamp(0.0, 100.0),
        compaction_score: inputs.compaction_pending_ratio.clamp(0.0, 100.0),
    }
}

/// Tracks CPU usage (in cores) of all threads whose name matches a given
/// prefix, e.g. `UNIFIED_READ_POOL_THREAD` or `GRPC_SERVER_THREAD`. Generalizes
/// the `/proc`-based scan originally written for the unified read pool alone.
pub struct ThreadGroupCpuTracker {
    name_prefix: &'static str,
    prev_total_cpu_ticks: i64,
    prev_check_time: Instant,
    prev_cpu_cores: f64,
    // Whether a baseline has been established yet. `utime`/`stime` are
    // cumulative since thread start, so the first scan can't compute a
    // meaningful delta — it only records the baseline.
    initialized: bool,
}

impl ThreadGroupCpuTracker {
    pub fn new(name_prefix: &'static str) -> Self {
        Self {
            name_prefix,
            prev_total_cpu_ticks: 0,
            prev_check_time: Instant::now_coarse(),
            prev_cpu_cores: 0.0,
            initialized: false,
        }
    }

    /// Returns the average CPU usage (in cores) of threads matching
    /// `name_prefix` since the previous call.
    pub fn measure_cpu_cores(&mut self) -> f64 {
        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_check_time);
        // Minimum duration check to avoid noise - if too soon, return the
        // cached value (mirrors ReadPoolCpuTimeTracker::get_unified_read_pool_cpu).
        if duration < Duration::from_millis(500) {
            return self.prev_cpu_cores;
        }

        let pid = process_id();
        let tids = match thread_ids::<Vec<_>>(pid) {
            Ok(tids) => tids,
            Err(_) => {
                // Transient /proc read failure: keep the previous cached
                // value instead of treating it as zero CPU usage. Otherwise
                // this tick would spuriously report 0 cores, and the next
                // successful read would compute tick_diff against a reset
                // baseline of 0, producing an artificial spike.
                return self.prev_cpu_cores;
            }
        };

        let mut current_total_cpu_ticks = 0i64;
        for tid in tids {
            if let Ok(stat) = full_thread_stat(pid, tid)
                && matches_thread_name_prefix(&stat.command, self.name_prefix)
            {
                current_total_cpu_ticks += stat.utime + stat.stime;
            }
        }

        if !self.initialized {
            // First successful scan: establish the baseline rather than
            // treating the thread's entire historical CPU-tick count as this
            // tick's delta, which would report a false spike.
            self.initialized = true;
            self.prev_total_cpu_ticks = current_total_cpu_ticks;
            self.prev_check_time = check_time;
            self.prev_cpu_cores = 0.0;
            return 0.0;
        }

        let tick_diff = current_total_cpu_ticks.saturating_sub(self.prev_total_cpu_ticks);
        let cpu_cores = if duration.as_secs_f64() > 0.0 && tick_diff > 0 {
            let cpu_seconds = tick_diff as f64 / ticks_per_second() as f64;
            cpu_seconds / duration.as_secs_f64()
        } else {
            0.0
        };

        self.prev_total_cpu_ticks = current_total_cpu_ticks;
        self.prev_check_time = check_time;
        self.prev_cpu_cores = cpu_cores;
        cpu_cores
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_fraction_boundaries_and_clamp() {
        assert_eq!(pressure_fraction(60.0, 60.0, 90.0), 0.0);
        assert_eq!(pressure_fraction(90.0, 60.0, 90.0), 1.0);
        assert_eq!(pressure_fraction(75.0, 60.0, 90.0), 0.5);
        assert_eq!(pressure_fraction(200.0, 60.0, 90.0), 1.0);
        assert_eq!(pressure_fraction(0.0, 60.0, 90.0), 0.0);
    }

    fn base_inputs() -> ResourceScoreInputs {
        ResourceScoreInputs {
            process_cpu_util: 0.0,
            grpc_cpu_cores: 0.0,
            io_util: 0.0,
            compaction_pending_ratio: 0.0,
        }
    }

    fn base_caps() -> ResourceCapacities {
        ResourceCapacities {
            total_cpu_cores: 8.0,
            grpc_concurrency: 8.0,
        }
    }

    #[test]
    fn test_cpu_score_is_plain_utilization() {
        let mut inputs = base_inputs();
        inputs.process_cpu_util = 100.0;
        assert_eq!(
            compute_resource_scores(&inputs, &base_caps()).cpu_score,
            100.0
        );
        inputs.process_cpu_util = 10.0;
        assert_eq!(
            compute_resource_scores(&inputs, &base_caps()).cpu_score,
            10.0
        );
    }

    #[test]
    fn test_cpu_score_process_dominant() {
        let mut inputs = base_inputs();
        inputs.process_cpu_util = 75.0;
        let scores = compute_resource_scores(&inputs, &base_caps());
        assert_eq!(scores.cpu_score, 75.0);
    }

    #[test]
    fn test_cpu_score_grpc_dominant() {
        let mut inputs = base_inputs();
        inputs.process_cpu_util = 30.0;
        inputs.grpc_cpu_cores = 7.2; // 7.2/8*100 = 90% util
        let scores = compute_resource_scores(&inputs, &base_caps());
        assert_eq!(scores.cpu_score, 90.0);
    }

    #[test]
    fn test_io_score_independent_of_cpu() {
        let mut inputs = base_inputs();
        inputs.process_cpu_util = 100.0;
        inputs.grpc_cpu_cores = 8.0;
        inputs.io_util = 65.0;
        let scores = compute_resource_scores(&inputs, &base_caps());
        assert_eq!(scores.io_score, 65.0);

        // Unlimited IO quota is represented as io_util = 0 by the caller.
        inputs.io_util = 0.0;
        let scores = compute_resource_scores(&inputs, &base_caps());
        assert_eq!(scores.io_score, 0.0);
    }

    #[test]
    fn test_compaction_score_clamp() {
        let mut inputs = base_inputs();
        inputs.compaction_pending_ratio = 150.0;
        assert_eq!(
            compute_resource_scores(&inputs, &base_caps()).compaction_score,
            100.0
        );
        inputs.compaction_pending_ratio = 55.0;
        assert_eq!(
            compute_resource_scores(&inputs, &base_caps()).compaction_score,
            55.0
        );
        inputs.compaction_pending_ratio = 0.0;
        assert_eq!(
            compute_resource_scores(&inputs, &base_caps()).compaction_score,
            0.0
        );
    }

    #[test]
    fn test_zero_grpc_capacity_guard() {
        let mut inputs = base_inputs();
        inputs.process_cpu_util = 50.0;
        inputs.grpc_cpu_cores = 100.0; // would dominate if normalized
        let caps = ResourceCapacities {
            total_cpu_cores: 8.0,
            grpc_concurrency: 0.0,
        };
        // Zero grpc capacity => grpc signal contributes 0 util, no div-by-zero.
        let scores = compute_resource_scores(&inputs, &caps);
        assert_eq!(scores.cpu_score, 50.0);
    }
}
