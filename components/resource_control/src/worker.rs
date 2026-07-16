// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    array,
    io::Result as IoResult,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use file_system::{IoBytes, IoType, fetch_io_bytes};
use prometheus::Histogram;
use strum::EnumCount;
use tikv_util::{
    resource_control::{DEFAULT_RESOURCE_GROUP_NAME, TaskPriority},
    sys::{SysQuota, cpu_time::ProcessStat},
    thread_name_prefix::{
        GRPC_SERVER_THREAD, SCHEDULE_WORKER_PRIORITY_THREAD, UNIFIED_READ_POOL_THREAD,
    },
    time::Instant,
    warn,
    yatp_pool::metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC,
};

use crate::{
    metrics::*,
    resource_group::ResourceGroupManager,
    resource_limiter::{GroupStatistics, ResourceLimiter, ResourceType},
    score::{
        ResourceCapacities, ResourceScoreInputs, ResourceScores, ThreadGroupCpuTracker,
        compute_resource_scores, pressure_fraction,
    },
};

pub const QUOTA_ADJUST_DURATION: Duration = Duration::from_secs(10);

const MICROS_PER_SEC: f64 = 1_000_000.0;
// the minimal schedule wait duration due to the overhead of queue.
// We should exclude this cause when calculate the estimated total wait
// duration.
const MINIMAL_SCHEDULE_WAIT_SECS: f64 = 0.000_005; //5us

/// Bundles the two scale-range thresholds so they can be passed as a single
/// argument to `background_adjust_resource_quota`.
#[derive(Clone, Copy)]
struct ScaleThresholds {
    bg_scale_start: f64,
    fg_cpu_throttle_threshold: f64,
}

pub struct ResourceUsageStats {
    total_quota: f64,
    current_used: f64,
}

pub trait ResourceStatsProvider {
    fn get_current_stats(&mut self, _t: ResourceType) -> IoResult<ResourceUsageStats>;
}

pub struct SysQuotaGetter {
    process_stat: ProcessStat,
    prev_io_stats: [IoBytes; IoType::COUNT],
    prev_io_ts: Instant,
    io_bandwidth: f64,
}

impl ResourceStatsProvider for SysQuotaGetter {
    fn get_current_stats(&mut self, ty: ResourceType) -> IoResult<ResourceUsageStats> {
        match ty {
            ResourceType::Cpu => {
                let total_quota = SysQuota::cpu_cores_quota();
                self.process_stat.cpu_usage().map(|u| ResourceUsageStats {
                    // cpu is measured in us.
                    total_quota: total_quota * MICROS_PER_SEC,
                    current_used: u * MICROS_PER_SEC,
                })
            }
            ResourceType::Io => {
                let mut stats = ResourceUsageStats {
                    total_quota: self.io_bandwidth,
                    current_used: 0.0,
                };
                let now = Instant::now_coarse();
                let dur = now.saturating_duration_since(self.prev_io_ts).as_secs_f64();
                if dur < 0.1 {
                    return Ok(stats);
                }
                let new_io_stats = fetch_io_bytes();
                let total_io_used = self
                    .prev_io_stats
                    .iter()
                    .zip(new_io_stats.iter())
                    .map(|(s, new_s)| {
                        let delta = *new_s - *s;
                        delta.read + delta.write
                    })
                    .sum::<u64>();
                self.prev_io_stats = new_io_stats;
                self.prev_io_ts = now;

                stats.current_used = total_io_used as f64 / dur;
                Ok(stats)
            }
        }
    }
}

pub struct GroupQuotaAdjustWorker<R> {
    last_adjust_time: Instant,
    resource_ctl: Arc<ResourceGroupManager>,
    resource_quota_getter: R,
    // Shared compaction pressure (0-100+) written by EnginesResourceInfo::update().
    compaction_pending_bytes_ratio: Arc<AtomicU32>,
    // Single shared limiter for all background tasks.
    bg_limiter: Arc<ResourceLimiter>,
    // Previous statistics snapshot per resource type for delta computation.
    prev_stats: [GroupStatistics; ResourceType::COUNT],
    // Whether the previous tick had background groups. Used to detect the
    // empty->non-empty transition and discard stale accumulated consumption.
    prev_had_background: bool,
    // Measures grpc-server thread CPU usage, feeding the common
    // resource-pressure score alongside whole-process CPU.
    grpc_cpu_tracker: ThreadGroupCpuTracker,
    caps: ResourceCapacities,
    // IO quota (bytes/s) available to `resource_quota_getter` for
    // ResourceType::Io. 0 means IO rate limiting isn't configured at all, in
    // which case there's nothing to fetch stats for or throttle.
    io_bandwidth: f64,
}

impl GroupQuotaAdjustWorker<SysQuotaGetter> {
    pub fn new(
        resource_ctl: Arc<ResourceGroupManager>,
        io_bandwidth: u64,
        compaction_pending_bytes_ratio: Arc<AtomicU32>,
        grpc_concurrency: usize,
    ) -> Self {
        let resource_quota_getter = SysQuotaGetter {
            process_stat: ProcessStat::cur_proc_stat().unwrap(),
            prev_io_stats: [IoBytes::default(); IoType::COUNT],
            prev_io_ts: Instant::now_coarse(),
            io_bandwidth: io_bandwidth as f64,
        };
        Self::with_quota_getter(
            resource_ctl,
            resource_quota_getter,
            compaction_pending_bytes_ratio,
            grpc_concurrency,
            io_bandwidth as f64,
        )
    }
}

impl<R: ResourceStatsProvider> GroupQuotaAdjustWorker<R> {
    pub fn with_quota_getter(
        resource_ctl: Arc<ResourceGroupManager>,
        resource_quota_getter: R,
        compaction_pending_bytes_ratio: Arc<AtomicU32>,
        grpc_concurrency: usize,
        io_bandwidth: f64,
    ) -> Self {
        let bg_limiter = resource_ctl.get_background_limiter();
        Self {
            last_adjust_time: Instant::now_coarse(),
            resource_ctl,
            resource_quota_getter,
            compaction_pending_bytes_ratio,
            bg_limiter,
            prev_stats: array::from_fn(|_| GroupStatistics::default()),
            prev_had_background: false,
            grpc_cpu_tracker: ThreadGroupCpuTracker::new(GRPC_SERVER_THREAD),
            caps: ResourceCapacities {
                total_cpu_cores: SysQuota::cpu_cores_quota(),
                grpc_concurrency: grpc_concurrency as f64,
            },
            io_bandwidth,
        }
    }

    pub fn adjust_quota(&mut self) {
        let now = Instant::now_coarse();
        let dur_secs = now
            .saturating_duration_since(self.last_adjust_time)
            .as_secs_f64();
        // a conservative check, skip adjustment if the duration is too short.
        if dur_secs < 1.0 {
            return;
        }
        self.last_adjust_time = now;

        // Fetch CPU stats once so background and foreground use a consistent
        // snapshot and we avoid double-reading /proc/stat.
        let cpu_stats = match self
            .resource_quota_getter
            .get_current_stats(ResourceType::Cpu)
        {
            Ok(s) => s,
            Err(e) => {
                warn!("get process total cpu failed; skip adjustment."; "err" => ?e);
                return;
            }
        };
        let process_cpu_util = if cpu_stats.total_quota > f64::EPSILON {
            cpu_stats.current_used / cpu_stats.total_quota * 100.0
        } else {
            0.0
        };

        let io_util = self.fetch_io_util();

        let grpc_cpu_cores = self.grpc_cpu_tracker.measure_cpu_cores();

        let compaction_pending_ratio =
            self.compaction_pending_bytes_ratio.load(Ordering::Relaxed) as f64;

        let inputs = ResourceScoreInputs {
            process_cpu_util,
            grpc_cpu_cores,
            io_util,
            compaction_pending_ratio,
        };
        let scores = compute_resource_scores(&inputs, &self.caps);
        RESOURCE_SCORE_VEC
            .with_label_values(&["cpu"])
            .set(scores.cpu_score);
        RESOURCE_SCORE_VEC
            .with_label_values(&["io"])
            .set(scores.io_score);
        RESOURCE_SCORE_VEC
            .with_label_values(&["compaction"])
            .set(scores.compaction_score);

        self.background_adjust_quota(dur_secs, cpu_stats.total_quota, &scores);
        self.foreground_adjust_quota(scores.cpu_score);
    }

    fn foreground_adjust_quota(&mut self, cpu_score: f64) {
        self.resource_ctl.online_adjust_resource_quota(cpu_score);
    }

    // io_score is only consumed by `background_adjust_quota`, so mirror its
    // gates here and skip the IO fetch entirely when there's nothing
    // background to throttle, or when IO rate limiting isn't even configured
    // (io_bandwidth == 0, so background_adjust_resource_quota would bail out
    // anyway) — the old code never tracked IO stats unconditionally either.
    fn fetch_io_util(&mut self) -> f64 {
        if !self.resource_ctl.has_background_groups() || self.io_bandwidth <= 0.0 {
            return 0.0;
        }
        // A fetch failure only disables IO scoring/adjustment for this tick,
        // not the whole tick.
        match self
            .resource_quota_getter
            .get_current_stats(ResourceType::Io)
        {
            Ok(s) if s.total_quota > f64::EPSILON => s.current_used / s.total_quota * 100.0,
            Ok(_) => 0.0,
            Err(e) => {
                warn!("get io stats failed; skip io adjustment."; "err" => ?e);
                0.0
            }
        }
    }

    fn background_adjust_quota(
        &mut self,
        dur_secs: f64,
        cpu_total_quota: f64,
        scores: &ResourceScores,
    ) {
        let mut bg_util_limit = self
            .resource_ctl
            .get_resource_group(DEFAULT_RESOURCE_GROUP_NAME)
            .map_or(0, |r| {
                r.group.get_background_settings().get_utilization_limit()
            });
        if bg_util_limit == 0 {
            bg_util_limit = 100;
        }
        let (bg_scale_start, fg_cpu_throttle_threshold) = {
            let config = self.resource_ctl.get_config().value();
            let start = config.bg_cpu_throttle_threshold.clamp(1.0, 99.0);
            let end = config.fg_cpu_throttle_threshold.clamp(start, 99.0);
            (start, end)
        };
        // Cap utilization limit to fg_cpu_throttle_threshold. Background tasks should
        // never consume more than this fraction of total resources. Scale-down
        // begins at bg_scale_start and reaches min_floor at fg_cpu_throttle_threshold.
        bg_util_limit = bg_util_limit.min(fg_cpu_throttle_threshold as u64);

        if !self.resource_ctl.has_background_groups() {
            self.resource_ctl.set_bg_cpu_at_floor(true);
            self.prev_had_background = false;
            return;
        }
        if !self.prev_had_background {
            // Flush consumption that accumulated while the limiter ran unlimited
            // during the empty period, and drain the CPU delta so the first real
            // tick sees a clean baseline.
            self.prev_stats = [
                self.bg_limiter.get_limit_statistics(ResourceType::Cpu),
                self.bg_limiter.get_limit_statistics(ResourceType::Io),
            ];
            let _ = self
                .resource_quota_getter
                .get_current_stats(ResourceType::Cpu);
            self.prev_had_background = true;
        }

        let thresholds = ScaleThresholds {
            bg_scale_start,
            fg_cpu_throttle_threshold,
        };
        self.background_adjust_resource_quota(
            ResourceType::Cpu,
            dur_secs,
            bg_util_limit,
            thresholds,
            cpu_total_quota,
            scores.cpu_score,
        );
        self.background_adjust_resource_quota(
            ResourceType::Io,
            dur_secs,
            bg_util_limit,
            thresholds,
            self.io_bandwidth,
            scores.io_score,
        );
        self.adjust_write_io_by_compaction_pressure(scores.compaction_score);
    }

    fn background_adjust_resource_quota(
        &mut self,
        resource_type: ResourceType,
        dur_secs: f64,
        // Configured background share, 0-100 — a score in the same units as
        // `resource_score`.
        utilization_limit: u64,
        thresholds: ScaleThresholds,
        total_quota: f64,
        // Common 0-100 resource-pressure score for this resource type
        // (cpu_score or io_score from `compute_resource_scores`).
        resource_score: f64,
    ) {
        if total_quota <= f64::EPSILON {
            self.bg_limiter
                .get_limiter(resource_type)
                .set_rate_limit(f64::INFINITY);
            return;
        }

        // Collect statistics for metrics from the single global limiter.
        let total_stats = self.bg_limiter.get_limit_statistics(resource_type);
        let last_stats =
            std::mem::replace(&mut self.prev_stats[resource_type as usize], total_stats);
        let stats_delta = total_stats - last_stats;
        BACKGROUND_RESOURCE_CONSUMPTION
            .with_label_values(&[resource_type.as_str()])
            .inc_by(stats_delta.total_consumed);
        let background_consumed = (stats_delta / dur_secs).total_consumed as f64;

        // Centi-cores (cores * 100) for CPU (background_consumed is core-us/s), bytes/s
        // for IO.
        let background_resource = match resource_type {
            ResourceType::Cpu => background_consumed / MICROS_PER_SEC * 100.0,
            ResourceType::Io => background_consumed,
        };
        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(background_resource as i64);

        let utilization_limit_score = (utilization_limit as f64).min(100.0);
        let target = total_quota * utilization_limit_score / 100.0;

        // Treat infinity as target (initial state before first adjustment).
        let current_limit = {
            let l = self.bg_limiter.get_limiter(resource_type).get_rate_limit();
            if l.is_infinite() { target } else { l }
        };
        let current_limit_score = current_limit / total_quota * 100.0;

        // Minimum: 1 CPU core for CPU, 10% of total for IO.
        let min_floor = match resource_type {
            ResourceType::Cpu => MICROS_PER_SEC,
            ResourceType::Io => total_quota * 0.1,
        }
        .min(target);
        let min_floor_score = min_floor / total_quota * 100.0;

        let new_budget_score = Self::compute_budget_score(
            current_limit_score,
            utilization_limit_score,
            min_floor_score,
            resource_score,
            thresholds,
        );
        let new_budget = (new_budget_score / 100.0 * total_quota).clamp(min_floor, target);
        self.bg_limiter
            .get_limiter(resource_type)
            .set_rate_limit(new_budget);
        let new_budget_resource = match resource_type {
            ResourceType::Cpu => new_budget / MICROS_PER_SEC * 100.0,
            ResourceType::Io => new_budget,
        };
        BACKGROUND_QUOTA_LIMIT_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(new_budget_resource as i64);

        // Update the "background at floor" flag so foreground throttling
        // only kicks in after background has been fully squeezed.
        if resource_type == ResourceType::Cpu {
            let at_floor = new_budget <= min_floor && background_consumed <= new_budget;
            self.resource_ctl.set_bg_cpu_at_floor(at_floor);
        }
    }

    /// Pure budget decision in score space (0-100 throughout, no absolute
    /// units or per-resource-type knowledge): tightens `current_limit_score`
    /// toward `min_floor_score` as `resource_score` rises from
    /// `bg_scale_start` to `fg_cpu_throttle_threshold`, resets to
    /// `target_score` if the current limit exceeds it, ramps up
    /// incrementally when idle, or holds otherwise. Always clamped to
    /// `[min_floor_score, target_score]` by the caller.
    fn compute_budget_score(
        current_limit_score: f64,
        target_score: f64,
        min_floor_score: f64,
        resource_score: f64,
        thresholds: ScaleThresholds,
    ) -> f64 {
        let ScaleThresholds {
            bg_scale_start,
            fg_cpu_throttle_threshold,
        } = thresholds;
        if resource_score > bg_scale_start {
            // Linearly scale budget from target down to min_floor as the
            // resource-utilization score goes from bg_scale_start to
            // fg_cpu_throttle_threshold.
            let pressure =
                pressure_fraction(resource_score, bg_scale_start, fg_cpu_throttle_threshold);
            let new_budget_score = target_score * (1.0 - pressure) + min_floor_score * pressure;
            // Only tighten: never increase the limit in the throttle branch.
            new_budget_score.min(current_limit_score)
        } else if current_limit_score > target_score {
            // Background limit exceeds its allowed share; reset to target.
            target_score
        } else if current_limit_score < 0.9 * target_score && resource_score < 0.9 * bg_scale_start
        {
            // System is idle; increase limit incrementally from current limit.
            current_limit_score * 1.1
        } else {
            target_score
        }
    }

    /// Adjust the write-only IO limiter based on compaction pressure.
    /// When pressure >= threshold, linearly scale write IO from ceiling to
    /// floor. Below threshold, write IO ramps up 10% per tick capped at
    /// ceiling.
    fn adjust_write_io_by_compaction_pressure(&self, pressure: f64) {
        let config = self.resource_ctl.get_config().value().clone();
        let threshold = config.bg_compaction_pressure_threshold.clamp(1.0, 99.0);
        let ceiling = config.bg_write_io_ceiling.0 as f64; // bytes/s
        let floor = config.bg_write_io_floor.0 as f64; // bytes/s

        let new_limit = if pressure < threshold {
            // Below threshold: ramp up current limit by 10%, capped at ceiling.
            let current_limit = self.bg_limiter.get_write_io_limiter().get_rate_limit();
            if current_limit.is_infinite() || current_limit >= ceiling {
                ceiling
            } else {
                (current_limit * 1.1).min(ceiling)
            }
        } else {
            // Linear interpolation from ceiling to floor as pressure goes from
            // threshold to 100%.
            let pressure_ratio = ((pressure - threshold) / (100.0 - threshold)).clamp(0.0, 1.0);
            (ceiling * (1.0 - pressure_ratio) + floor * pressure_ratio).max(floor)
        };

        self.bg_limiter
            .get_write_io_limiter()
            .set_rate_limit(new_limit);
    }
}

/// PriorityLimiterAdjustWorker automically adjust the quota of each priority
/// limiter based on the statistics data during a certain period of time.
/// In general, caller should call this function in a fixed interval.
pub struct PriorityLimiterAdjustWorker<R> {
    resource_ctl: Arc<ResourceGroupManager>,
    trackers: [PriorityLimiterStatsTracker; TaskPriority::PRIORITY_COUNT],
    resource_quota_getter: R,
    last_adjust_time: Instant,
    is_last_low_cpu: bool,
    is_last_single_group: bool,
}

impl PriorityLimiterAdjustWorker<SysQuotaGetter> {
    pub fn new(resource_ctl: Arc<ResourceGroupManager>) -> Self {
        let resource_quota_getter = SysQuotaGetter {
            process_stat: ProcessStat::cur_proc_stat().unwrap(),
            prev_io_stats: [IoBytes::default(); IoType::COUNT],
            prev_io_ts: Instant::now_coarse(),
            io_bandwidth: f64::INFINITY,
        };
        Self::with_quota_getter(resource_ctl, resource_quota_getter)
    }
}

impl<R: ResourceStatsProvider> PriorityLimiterAdjustWorker<R> {
    fn with_quota_getter(
        resource_ctl: Arc<ResourceGroupManager>,
        resource_quota_getter: R,
    ) -> Self {
        let limiters = resource_ctl.get_priority_resource_limiters();
        let priorities = TaskPriority::priorities();
        let trackers = std::array::from_fn(|i| {
            PriorityLimiterStatsTracker::new(limiters[i].clone(), priorities[i].as_str())
        });
        Self {
            resource_ctl,
            trackers,
            resource_quota_getter,
            last_adjust_time: Instant::now_coarse(),
            is_last_low_cpu: true,
            is_last_single_group: true,
        }
    }
    pub fn adjust(&mut self) {
        let now = Instant::now_coarse();
        let dur = now.saturating_duration_since(self.last_adjust_time);
        if dur < Duration::from_secs(1) {
            warn!("adjust duration too small, skip adjustment."; "dur" => ?dur);
            return;
        }
        self.last_adjust_time = now;

        // fast path for only the default resource group which means resource
        // control is not used at all.
        let group_count = self.resource_ctl.get_group_count();
        if group_count == 1 {
            if self.is_last_single_group {
                return;
            }
            self.is_last_single_group = true;
            self.trackers.iter().skip(1).for_each(|t| {
                t.limiter
                    .get_limiter(ResourceType::Cpu)
                    .set_rate_limit(f64::INFINITY)
            });
            return;
        }
        self.is_last_single_group = false;

        let stats: [_; TaskPriority::PRIORITY_COUNT] =
            array::from_fn(|i| self.trackers[i].get_and_update_last_stats(dur.as_secs_f64()));

        let process_cpu_stats = match self
            .resource_quota_getter
            .get_current_stats(ResourceType::Cpu)
        {
            Ok(s) => s,
            Err(e) => {
                warn!("get process total cpu failed; skip adjusment."; "err" => ?e);
                return;
            }
        };

        if process_cpu_stats.current_used < process_cpu_stats.total_quota * 0.3 {
            if self.is_last_low_cpu {
                return;
            }
            self.is_last_low_cpu = true;
            self.trackers.iter().skip(1).for_each(|t| {
                t.limiter
                    .get_limiter(ResourceType::Cpu)
                    .set_rate_limit(f64::INFINITY);
                // 0 represent infinity
                PRIORITY_QUOTA_LIMIT_VEC
                    .get_metric_with_label_values(&[t.priority])
                    .unwrap()
                    .set(0);
            });
            return;
        }
        self.is_last_low_cpu = false;

        let total_cpus: f64 = stats.iter().map(|s| s.cpu_secs).sum();
        let max_cpus = stats.iter().map(|s| s.cpu_secs).fold(0.0, f64::max);
        // there is only 1 active priority, do not restrict.
        if total_cpus * 0.99 <= max_cpus {
            self.trackers
                .iter()
                .skip(1)
                .for_each(|t: &PriorityLimiterStatsTracker| {
                    t.limiter
                        .get_limiter(ResourceType::Cpu)
                        .set_rate_limit(f64::INFINITY)
                });
            return;
        }

        let cpu_duration: [_; TaskPriority::PRIORITY_COUNT] = array::from_fn(|i| stats[i].cpu_secs);
        let real_cpu_total: f64 = cpu_duration.iter().sum();

        let available_quota_percentage = self
            .resource_ctl
            .get_config()
            .value()
            .priority_ctl_strategy
            .to_resource_util_percentage();
        let expect_pool_cpu_total = real_cpu_total
            * (process_cpu_stats.total_quota * available_quota_percentage)
            / process_cpu_stats.current_used;
        let mut limits = [0.0; 2];
        let level_expected: [_; TaskPriority::PRIORITY_COUNT] =
            array::from_fn(|i| stats[i].cpu_secs + stats[i].wait_secs);
        // substract the cpu time usage for priority high.
        let mut expect_cpu_time_total = expect_pool_cpu_total - level_expected[0];

        // still reserve a minimal cpu quota
        let minimal_quota = process_cpu_stats.total_quota / MICROS_PER_SEC * 0.1;
        for i in 1..self.trackers.len() {
            if expect_cpu_time_total < minimal_quota {
                expect_cpu_time_total = minimal_quota;
            }
            let limit = expect_cpu_time_total * MICROS_PER_SEC;
            self.trackers[i]
                .limiter
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(limit);
            PRIORITY_QUOTA_LIMIT_VEC
                .get_metric_with_label_values(&[self.trackers[i].priority])
                .unwrap()
                .set(limit as i64);
            limits[i - 1] = limit;
            expect_cpu_time_total -= level_expected[i];
        }
    }
}

#[derive(Debug)]
struct LimiterStats {
    // QuotaLimiter consumed cpu secs in total
    cpu_secs: f64,
    // QuotaLimiter waited secs in total.
    wait_secs: f64,
}

struct HistogramTracker {
    metrics: Histogram,
    last_sum: f64,
    last_count: u64,
}

impl HistogramTracker {
    fn new(metrics: Histogram) -> Self {
        let last_sum = metrics.get_sample_sum();
        let last_count = metrics.get_sample_count();
        Self {
            metrics,
            last_sum,
            last_count,
        }
    }

    fn get_and_upate_statistics(&mut self) -> (f64, u64) {
        let cur_sum = self.metrics.get_sample_sum();
        let cur_count = self.metrics.get_sample_count();
        let res = (cur_sum - self.last_sum, cur_count - self.last_count);
        self.last_sum = cur_sum;
        self.last_count = cur_count;
        res
    }
}

struct PriorityLimiterStatsTracker {
    priority: &'static str,
    limiter: Arc<ResourceLimiter>,
    last_stats: GroupStatistics,
    // unified-read-pool and schedule-worker-pool wait duration metrics.
    task_wait_dur_trakcers: [HistogramTracker; 2],
}

impl PriorityLimiterStatsTracker {
    fn new(limiter: Arc<ResourceLimiter>, priority: &'static str) -> Self {
        let task_wait_dur_trakcers = [UNIFIED_READ_POOL_THREAD, SCHEDULE_WORKER_PRIORITY_THREAD]
            .map(|pool_name| {
                HistogramTracker::new(
                    YATP_POOL_SCHEDULE_WAIT_DURATION_VEC
                        .get_metric_with_label_values(&[pool_name, priority])
                        .unwrap(),
                )
            });
        let last_stats = limiter.get_limit_statistics(ResourceType::Cpu);
        Self {
            priority,
            limiter,
            last_stats,
            task_wait_dur_trakcers,
        }
    }

    fn get_and_update_last_stats(&mut self, dur_secs: f64) -> LimiterStats {
        let cur_stats = self.limiter.get_limit_statistics(ResourceType::Cpu);
        let stats_delta = cur_stats - self.last_stats;
        self.last_stats = cur_stats;
        PRIORITY_CPU_TIME_VEC
            .with_label_values(&[self.priority])
            .inc_by(stats_delta.total_consumed);
        let stats_per_sec = stats_delta / dur_secs;

        let wait_stats: [_; 2] =
            array::from_fn(|i| self.task_wait_dur_trakcers[i].get_and_upate_statistics());
        let schedule_wait_dur_secs = wait_stats.iter().map(|s| s.0).sum::<f64>() / dur_secs;
        let expected_wait_dur_secs =
            stats_per_sec.request_count as f64 * MINIMAL_SCHEDULE_WAIT_SECS;
        let normed_schedule_wait_dur_secs =
            (schedule_wait_dur_secs - expected_wait_dur_secs).max(0.0);
        LimiterStats {
            cpu_secs: stats_per_sec.total_consumed as f64 / MICROS_PER_SEC,
            wait_secs: stats_per_sec.total_wait_dur_us as f64 / MICROS_PER_SEC
                + normed_schedule_wait_dur_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tikv_util::thread_name_prefix::BACKGROUND_WORKER_THREAD;

    use super::*;
    use crate::resource_group::tests::*;

    struct TestResourceStatsProvider {
        cpu_total: f64,
        cpu_used: f64,
        io_total: f64,
        io_used: f64,
    }

    impl TestResourceStatsProvider {
        fn new(cpu_total: f64, io_total: f64) -> Self {
            Self {
                cpu_total,
                cpu_used: 0.0,
                io_total,
                io_used: 0.0,
            }
        }
    }

    impl ResourceStatsProvider for TestResourceStatsProvider {
        fn get_current_stats(&mut self, t: ResourceType) -> IoResult<ResourceUsageStats> {
            match t {
                ResourceType::Cpu => Ok(ResourceUsageStats {
                    total_quota: self.cpu_total * MICROS_PER_SEC,
                    current_used: self.cpu_used * MICROS_PER_SEC,
                }),
                ResourceType::Io => Ok(ResourceUsageStats {
                    total_quota: self.io_total,
                    current_used: self.io_used,
                }),
            }
        }
    }

    #[test]
    fn test_adjust_resource_limiter() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // Non-background group should not get a limiter.
        let rg1 = new_resource_group_ru("test".into(), 1000, 14);
        resource_ctl.add_resource_group(rg1);
        assert!(
            resource_ctl
                .get_background_resource_limiter("test", "br")
                .is_none()
        );

        // 8 CPU cores, 10000 bytes/s IO bandwidth.
        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pending_bytes_ratio,
            8,
            10000.0,
        );

        // Create default background group with 80% utilization limit.
        let mut default_bg =
            new_background_resource_group_ru("default".into(), 2000, 8, vec!["br".into()]);
        default_bg
            .mut_background_settings()
            .set_utilization_limit(80);
        resource_ctl.add_resource_group(default_bg);

        assert!(
            resource_ctl
                .get_background_resource_limiter("default", "lightning")
                .is_none()
        );
        let limiter = resource_ctl
            .get_background_resource_limiter("default", "br")
            .unwrap();
        assert!(
            limiter
                .get_limiter(ResourceType::Cpu)
                .get_rate_limit()
                .is_infinite()
        );
        assert!(
            limiter
                .get_limiter(ResourceType::Io)
                .get_rate_limit()
                .is_infinite()
        );

        let reset_quota = |worker: &mut GroupQuotaAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64,
                           io: f64,
                           dur: Duration| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.resource_quota_getter.io_used = io;
            let now = Instant::now_coarse();
            worker.last_adjust_time = now - dur;
        };

        #[track_caller]
        fn check(val: f64, expected: f64) {
            assert!(
                expected * 0.99 < val && val < expected * 1.01,
                "actual: {}, expected: {}",
                val,
                expected
            );
        }

        #[track_caller]
        fn check_limiter_rates(limiter: &Arc<ResourceLimiter>, cpu: f64, io: f64) {
            check(
                limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
                cpu * MICROS_PER_SEC,
            );
            check(limiter.get_limiter(ResourceType::Io).get_rate_limit(), io);
        }

        // util_limit configured at 80% but capped to fg_cpu_throttle_threshold=70%.
        // CPU target = 8 * 0.7 = 5.6 cores, IO target = 10000 * 0.7 = 7000
        // CPU min_floor = 1 core, IO min_floor = 1000
        // bg_scale_start=60%, fg_cpu_throttle_threshold=70% (scale range: 60→70).

        // No load: initial infinity → treated as target → budget = target.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Short duration (< 1s): adjustment skipped, limits unchanged.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_millis(500));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Under target (50% CPU): resource_score < 60%, current == target,
        // so none of the branches fire → budget = target.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // 80% CPU, 80% IO: resource_score > 60% (bg_scale_start).
        // pressure = (80-60)/(70-60) = 2.0 clamped to 1.0 → min_floor.
        // CPU: budget = min_floor = 1.0. IO: budget = min_floor = 1000.
        reset_quota(&mut worker, 6.4, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);

        // 100% CPU, 95% IO: pressure still clamped 1.0 → min_floor.
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);

        // Still at 100% CPU, 95% IO: same result (deterministic, no decay).
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);

        // 65% CPU, 65% IO: in the scale range (60-70%).
        // CPU: pressure = 0.5, computed = 3.3. But current_limit=1.0 (min_floor)
        // and 3.3 > 1.0 → only-tighten clamps to 1.0.
        // IO: same logic → stays 1000.
        reset_quota(&mut worker, 5.2, 6500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);

        // Load drops (50% CPU, 20% IO): resource_score < 54% (0.9*60%), current <
        // 0.9*target → incremental increase: budget = current * 1.1
        // CPU: 1.0 * 1.1 = 1.1. IO: 1000 * 1.1 = 1100
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.1, 1100.0);

        // Adding a second background group does not change the limiter —
        // all background groups share the single global bg_limiter.
        let bg = new_background_resource_group_ru(
            BACKGROUND_WORKER_THREAD.into(),
            1000,
            15,
            vec!["br".into()],
        );
        resource_ctl.add_resource_group(bg);
        let bg_limiter2 = resource_ctl
            .get_background_resource_limiter(BACKGROUND_WORKER_THREAD, "br")
            .unwrap();
        // Both groups return the same shared limiter.
        assert!(Arc::ptr_eq(&limiter, &bg_limiter2));

        // No load: current at 1.1M < 0.9*target (5.04M) and util < 63% → incremental.
        // budget = 1.1 * 1.1 = 1.21
        // IO: 1100 * 1.1 = 1210
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.21, 1210.0);

        // Over 70% (100% CPU, 95% IO): budget scales down to min_floor.
        // CPU: pressure = 1.0. budget = min_floor = 1.0
        // IO: pressure = (95-60)/(70-60) = 3.5 clamped 1.0. budget = min_floor = 1000.
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);
    }

    #[test]
    fn test_bg_limiter_with_infinite_ru_groups() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // 3 foreground groups with large RU quota (no background limiter).
        for i in 0..3 {
            let rg = new_resource_group_ru(format!("fg_{i}"), i32::MAX as u64, 8);
            resource_ctl.add_resource_group(rg);
        }

        // 1 background group with no explicit utilization limit.
        // bg_util_limit defaults to 100, capped to 70 by fg_cpu_throttle_threshold.
        // bg_scale_start=60%, fg_cpu_throttle_threshold=70%.
        let bg = new_background_resource_group_ru("bg_worker".into(), 5000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let limiter = resource_ctl
            .get_background_resource_limiter("bg_worker", "br")
            .unwrap();

        // 8 CPU cores, 10000 bytes/s IO.
        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pending_bytes_ratio,
            8,
            10000.0,
        );

        let reset_quota = |worker: &mut GroupQuotaAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64,
                           io: f64,
                           dur: Duration| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.resource_quota_getter.io_used = io;
            let now = Instant::now_coarse();
            worker.last_adjust_time = now - dur;
        };

        #[track_caller]
        fn check(val: f64, expected: f64) {
            assert!(
                expected * 0.99 < val && val < expected * 1.01,
                "actual: {}, expected: {}",
                val,
                expected
            );
        }

        // CPU target = 8 * 0.7 = 5.6 cores, IO target = 10000 * 0.7 = 7000
        // fg groups have no background settings; only bg_worker triggers adjustment.

        // --- Initial: no load → budget = target ---
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            7000.0,
        );

        // --- Saturate CPU (100%, 80% IO) → budget scales to min_floor ---
        // CPU: pressure = (100-60)/(70-60) = 4.0 clamped 1.0 → min_floor = 1.0
        // IO: pressure = (80-60)/(70-60) = 2.0 clamped 1.0 → min_floor = 1000
        reset_quota(&mut worker, 8.0, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.0 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            1000.0,
        );

        // --- Unsaturate CPU (25%) → budget recovers incrementally ---
        // CPU: resource_score = 25% < 54% (0.9*60%), current 1.0M < 5.04M → 1.0 * 1.1 =
        // 1.1 IO: resource_score = 20% < 54%, current 1000 < 6300 → 1000 * 1.1 =
        // 1100
        reset_quota(&mut worker, 2.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.1 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            1100.0,
        );

        // --- 65% CPU, 65% IO: in scale range (60-70%) ---
        // CPU: pressure = 0.5, computed = 3.3M. current_limit=1.1M < 3.3M → only
        // tighten → stays 1.1M. IO: computed = 4000, current=1100 < 4000 →
        // stays 1100.
        reset_quota(&mut worker, 5.2, 6500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.1 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            1100.0,
        );
    }

    #[test]
    fn test_compute_budget_score() {
        let thresholds = ScaleThresholds {
            bg_scale_start: 60.0,
            fg_cpu_throttle_threshold: 70.0,
        };

        // Under target, not idle enough to ramp: holds at target.
        assert_eq!(
            GroupQuotaAdjustWorker::<SysQuotaGetter>::compute_budget_score(
                70.0, 70.0, 10.0, 50.0, thresholds,
            ),
            70.0,
        );

        // Over bg_scale_start: tighten toward min_floor proportional to
        // pressure, never exceeding current_limit_score.
        let score = GroupQuotaAdjustWorker::<SysQuotaGetter>::compute_budget_score(
            70.0, 70.0, 10.0, 80.0, thresholds,
        );
        // pressure = (80-60)/(70-60) = 2.0 clamped to 1.0 -> min_floor.
        assert_eq!(score, 10.0);

        // Current limit exceeds its allowed share: reset to target.
        assert_eq!(
            GroupQuotaAdjustWorker::<SysQuotaGetter>::compute_budget_score(
                90.0, 70.0, 10.0, 50.0, thresholds,
            ),
            70.0,
        );

        // Idle (resource_score comfortably below bg_scale_start) and current
        // limit well under target: ramp up incrementally.
        assert_eq!(
            GroupQuotaAdjustWorker::<SysQuotaGetter>::compute_budget_score(
                10.0, 70.0, 10.0, 20.0, thresholds,
            ),
            11.0,
        );
    }

    #[test]
    fn test_background_throttled_by_grpc_driven_cpu_score() {
        // cpu_score is max(process_cpu_util, grpc_util) — a grpc-driven
        // spike must throttle background CPU exactly like a process-CPU
        // spike would, even though the *measured* process CPU here is low
        // and total_quota is untouched (a healthy, always-positive 8 cores).
        let resource_ctl = Arc::new(ResourceGroupManager::default());
        let bg = new_background_resource_group_ru("bg".into(), 1000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let bg_limiter = resource_ctl
            .get_background_resource_limiter("bg", "br")
            .unwrap();

        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            TestResourceStatsProvider::new(8.0, 10000.0),
            compaction_pending_bytes_ratio,
            8,
            10000.0,
        );

        // Process CPU is only 20% (comfortably idle on its own), but
        // grpc_util is pinned to 90% — cpu_score = max(20, 90) = 90.
        let scores = ResourceScores {
            cpu_score: 90.0,
            io_score: 0.0,
            compaction_score: 0.0,
        };
        // 8 cores * MICROS_PER_SEC, matching TestResourceStatsProvider::new(8.0, ..).
        let cpu_total_quota = 8.0 * MICROS_PER_SEC;
        worker.background_adjust_quota(1.0, cpu_total_quota, &scores);

        // bg_scale_start=60%, fg_cpu_throttle_threshold=70%: pressure =
        // (90-60)/(70-60) = 3.0 clamped to 1.0 -> min_floor (1 core).
        let rate = bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        assert!(
            (rate - MICROS_PER_SEC).abs() < MICROS_PER_SEC * 0.01,
            "grpc-driven cpu_score should throttle background CPU to the floor, got {rate}"
        );
    }

    #[test]
    fn test_zero_io_bandwidth_skips_io_fetch_and_uncaps_io_limiter() {
        // io_bandwidth == 0 means IO rate limiting isn't configured at all:
        // adjust_quota should skip fetching IO stats entirely (unobservable
        // here directly, but exercised so a panic would catch a regression)
        // and the background IO limiter should stay/become unlimited, same
        // as background_adjust_resource_quota's own total_quota <= EPSILON
        // bailout would produce.
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        let bg = new_background_resource_group_ru("bg_worker".into(), 5000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let limiter = resource_ctl
            .get_background_resource_limiter("bg_worker", "br")
            .unwrap();

        // 8 CPU cores, but io_total == 0 (IO rate limiting disabled).
        let test_provider = TestResourceStatsProvider::new(8.0, 0.0);
        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pending_bytes_ratio,
            8,
            0.0, // io_bandwidth
        );

        worker.resource_quota_getter.cpu_used = 8.0; // saturate CPU
        worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(1);
        worker.adjust_quota();

        assert!(
            limiter
                .get_limiter(ResourceType::Io)
                .get_rate_limit()
                .is_infinite(),
            "IO limiter should stay unlimited when io_bandwidth == 0"
        );
    }

    #[test]
    fn test_adjust_priority_resource_limiter() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());
        let priority_limiters = resource_ctl.get_priority_resource_limiters();
        let test_provider = TestResourceStatsProvider::new(8.0, f64::INFINITY);
        let mut worker =
            PriorityLimiterAdjustWorker::with_quota_getter(resource_ctl.clone(), test_provider);

        let reset_quota = |worker: &mut PriorityLimiterAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(10);
            priority_limiters[1]
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(f64::INFINITY);
            priority_limiters[2]
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(f64::INFINITY);
        };

        #[track_caller]
        fn check(val: f64, expected: f64) {
            assert!(
                (val.is_infinite() && expected.is_infinite())
                    || (expected * 0.99 < val && val < expected * 1.01),
                "actual: {}, expected: {}",
                val,
                expected
            );
        }

        let check_limiter = |high: f64, medium: f64, low: f64| {
            check(
                priority_limiters[0]
                    .get_limiter(ResourceType::Cpu)
                    .get_rate_limit(),
                high * MICROS_PER_SEC,
            );
            check(
                priority_limiters[1]
                    .get_limiter(ResourceType::Cpu)
                    .get_rate_limit(),
                medium * MICROS_PER_SEC,
            );
            check(
                priority_limiters[2]
                    .get_limiter(ResourceType::Cpu)
                    .get_rate_limit(),
                low * MICROS_PER_SEC,
            );
        };

        // only default group, always return infinity.
        reset_quota(&mut worker, 6.4);
        priority_limiters[1].consume(Duration::from_secs(50), IoBytes::default(), true, false);
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        let rg1 = new_resource_group_ru("test_high".into(), 1000, 16);
        resource_ctl.add_resource_group(rg1);
        let rg2 = new_resource_group_ru("test_low".into(), 2000, 1);
        resource_ctl.add_resource_group(rg2);

        reset_quota(&mut worker, 6.4);
        priority_limiters[1].consume(Duration::from_secs(64), IoBytes::default(), true, false);
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[0].consume(
                Duration::from_millis(240),
                IoBytes::default(),
                true,
                false,
            );
            priority_limiters[1].consume(
                Duration::from_millis(400),
                IoBytes::default(),
                true,
                false,
            );
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 0.8);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[0].consume(
                Duration::from_millis(120),
                IoBytes::default(),
                true,
                false,
            );
            priority_limiters[1].consume(
                Duration::from_millis(200),
                IoBytes::default(),
                true,
                false,
            );
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 1.6, 0.8);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[2].consume(
                Duration::from_millis(200),
                IoBytes::default(),
                true,
                false,
            );
        }
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        reset_quota(&mut worker, 8.0);
        for _i in 0..100 {
            priority_limiters[0].consume(
                Duration::from_millis(240),
                IoBytes::default(),
                true,
                false,
            );
            priority_limiters[1].consume(
                Duration::from_millis(240),
                IoBytes::default(),
                true,
                false,
            );
            priority_limiters[2].consume(
                Duration::from_millis(320),
                IoBytes::default(),
                true,
                false,
            );
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 0.8);

        reset_quota(&mut worker, 6.0);
        for _i in 0..100 {
            priority_limiters[0].consume(
                Duration::from_millis(240),
                IoBytes::default(),
                true,
                false,
            );
            priority_limiters[2].consume(
                Duration::from_millis(360),
                IoBytes::default(),
                true,
                false,
            );
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 3.2);

        // duration too small, unchanged.
        worker.resource_quota_getter.cpu_used = 6.0;
        worker.last_adjust_time = Instant::now_coarse() - Duration::from_millis(500);
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 3.2);
    }

    /// Verifies background load-shedding:
    ///
    /// - 0–60% CPU   : background full
    /// - 60–70% CPU  : background linearly throttled to min_floor
    /// - >70% CPU    : background at min_floor
    #[test]
    fn test_tiered_load_shedding() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        let bg = new_background_resource_group_ru("bg".into(), 1000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let bg_limiter = resource_ctl
            .get_background_resource_limiter("bg", "br")
            .unwrap();

        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut bg_worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            TestResourceStatsProvider::new(8.0, 10000.0),
            compaction_pending_bytes_ratio,
            8,
            10000.0,
        );

        const BG_TARGET: f64 = 5.6;
        const BG_FLOOR: f64 = 1.0;

        #[track_caller]
        fn approx(val: f64, expected: f64) {
            assert!(
                (val.is_infinite() && expected.is_infinite())
                    || (expected * 0.99 < val && val < expected * 1.01),
                "actual: {val}, expected: {expected}"
            );
        }

        macro_rules! tick {
            ($cpu:expr) => {{
                bg_worker.resource_quota_getter.cpu_used = $cpu;
                bg_worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(10);
                bg_worker.adjust_quota();
            }};
        }

        // 50% CPU — below bg_scale_start (60%): bg=full.
        tick!(4.0);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_TARGET * MICROS_PER_SEC,
        );

        // 65% CPU — mid bg range: pressure = 0.5.
        tick!(5.2);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            3.3 * MICROS_PER_SEC,
        );

        // 70% CPU — bg fully throttled.
        tick!(5.6);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );

        // 80% CPU — still at min_floor.
        tick!(6.4);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );

        // Recovery: CPU drops to 50%.
        tick!(4.0);
        let bg_rate = bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        assert!(
            bg_rate > BG_FLOOR * MICROS_PER_SEC && bg_rate < BG_TARGET * MICROS_PER_SEC,
            "bg should be recovering, got {bg_rate}"
        );
    }

    #[test]
    fn test_compaction_pending_bytes_ratio_write_io_throttle() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // 8 CPU cores, 10000 bytes/s IO bandwidth.
        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pending_bytes_ratio.clone(),
            8,
            10000.0,
        );

        // Create default background group with 80% utilization limit.
        let mut default_bg =
            new_background_resource_group_ru("default".into(), 2000, 8, vec!["br".into()]);
        default_bg
            .mut_background_settings()
            .set_utilization_limit(80);
        resource_ctl.add_resource_group(default_bg);

        let limiter = resource_ctl
            .get_background_resource_limiter("default", "br")
            .unwrap();

        let reset_quota = |worker: &mut GroupQuotaAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64,
                           io: f64,
                           dur: Duration| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.resource_quota_getter.io_used = io;
            let now = Instant::now_coarse();
            worker.last_adjust_time = now - dur;
        };

        #[track_caller]
        fn check(val: f64, expected: f64) {
            assert!(
                (val.is_infinite() && expected.is_infinite())
                    || (expected * 0.99 < val && val < expected * 1.01),
                "actual: {}, expected: {}",
                val,
                expected
            );
        }

        // Constants matching config defaults (bg_write_io_ceiling=100GB/s,
        // bg_write_io_floor=10MB/s).
        let ceiling = 100.0 * 1024.0 * 1024.0 * 1024.0; // 100 GB/s in bytes/s
        let floor = 10.0 * 1024.0 * 1024.0; // 10 MB/s in bytes/s

        // First adjustment: establish baseline limits.
        // util_limit 80% capped to 70%. CPU target = 5.6 cores, IO target = 7000
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            7000.0,
        );

        // Pressure = 0: below threshold → first call, current is infinite so
        // set to ceiling.
        check(limiter.get_write_io_limiter().get_rate_limit(), ceiling);

        // Pressure = 50 (< 70): already at ceiling, stays at ceiling.
        compaction_pending_bytes_ratio.store(50, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(limiter.get_write_io_limiter().get_rate_limit(), ceiling);

        // CPU and combined IO limits unchanged at target.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            7000.0,
        );

        // Pressure = 70 (at threshold): pressure_ratio = 0 → budget = ceiling.
        compaction_pending_bytes_ratio.store(70, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(limiter.get_write_io_limiter().get_rate_limit(), ceiling);

        // CPU and combined IO limits should not be affected by compaction pressure.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );

        // Pressure = 85: pressure_ratio = (85-70)/(100-70) = 0.5
        // budget = ceiling * 0.5 + floor * 0.5
        compaction_pending_bytes_ratio.store(85, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let expected_85 = ceiling * 0.5 + floor * 0.5;
        check(limiter.get_write_io_limiter().get_rate_limit(), expected_85);

        // CPU limit unaffected.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );

        // Pressure = 100: pressure_ratio = 1.0 → budget = floor (10 MB/s).
        compaction_pending_bytes_ratio.store(100, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(limiter.get_write_io_limiter().get_rate_limit(), floor);

        // CPU limit unaffected.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );

        // Pressure drops to 0 (< threshold): ramp up by 10% from floor.
        // floor * 1.1 = 10 MB/s * 1.1 = 11534336
        compaction_pending_bytes_ratio.store(0, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let ramp1 = floor * 1.1;
        check(limiter.get_write_io_limiter().get_rate_limit(), ramp1);

        // Pressure stays at 0: another 10% ramp up.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let ramp2 = ramp1 * 1.1;
        check(limiter.get_write_io_limiter().get_rate_limit(), ramp2);

        // Keep ramping until we hit ceiling.
        let mut current = ramp2;
        while current * 1.1 < ceiling {
            reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
            worker.adjust_quota();
            current = (current * 1.1).min(ceiling);
            check(limiter.get_write_io_limiter().get_rate_limit(), current);
        }
        // One more adjustment should cap at ceiling.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(limiter.get_write_io_limiter().get_rate_limit(), ceiling);
    }

    // Verify that with multiple background groups the budget is set globally on
    // a single shared limiter, not split proportionally across groups.
    //
    // Old behaviour: 2 groups with RU 2000 and 1000 would receive 2/3 and 1/3
    // of the total budget.
    // New behaviour: both groups return the same Arc<ResourceLimiter> and the
    // full budget is applied to it once.
    #[test]
    fn test_global_budget_not_split_across_groups() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // 8 CPU cores, 10000 bytes/s IO bandwidth.
        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pending_bytes_ratio,
            8,
            10000.0,
        );

        // Add two background groups with different RU quotas.
        let mut grp_a =
            new_background_resource_group_ru("group_a".into(), 2000, 8, vec!["br".into()]);
        grp_a.mut_background_settings().set_utilization_limit(80);
        resource_ctl.add_resource_group(grp_a);

        let grp_b = new_background_resource_group_ru("group_b".into(), 1000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(grp_b);

        let limiter_a = resource_ctl
            .get_background_resource_limiter("group_a", "br")
            .unwrap();
        let limiter_b = resource_ctl
            .get_background_resource_limiter("group_b", "br")
            .unwrap();

        // Both groups must point to the same global limiter.
        assert!(Arc::ptr_eq(&limiter_a, &limiter_b));

        // util_limit = 80% capped to 70% by bg_resource_threshold.
        // CPU target = 8 * 0.7 = 5.6 cores, IO target = 10000 * 0.7 = 7000.
        // Under no load the full target is applied to the single limiter.
        worker.resource_quota_getter.cpu_used = 0.0;
        worker.resource_quota_getter.io_used = 0.0;
        worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(1);
        worker.adjust_quota();

        #[track_caller]
        fn check(val: f64, expected: f64) {
            assert!(
                expected * 0.99 < val && val < expected * 1.01,
                "actual: {val}, expected: {expected}"
            );
        }

        // Full budget, not 2/3 of it.
        check(
            limiter_a.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );
        check(
            limiter_a.get_limiter(ResourceType::Io).get_rate_limit(),
            7000.0,
        );

        // limiter_b is the same Arc so it reflects the same rates.
        check(
            limiter_b.get_limiter(ResourceType::Cpu).get_rate_limit(),
            5.6 * MICROS_PER_SEC,
        );
        check(
            limiter_b.get_limiter(ResourceType::Io).get_rate_limit(),
            7000.0,
        );

        // Under heavy load (100% CPU) the budget scales down to min_floor for
        // the single global limiter — not independently per group.
        worker.resource_quota_getter.cpu_used = 8.0;
        worker.resource_quota_getter.io_used = 9500.0;
        worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(1);
        worker.adjust_quota();

        // CPU: resource_score=100%, bg_scale_start=60%, fg_cpu_throttle_threshold=70%.
        // pressure = (100-60)/(70-60) = 4.0 clamped to 1.0 → min_floor = 1.0 core.
        // IO: resource_score=95%, pressure = (95-60)/(70-60) = 3.5 clamped to 1.0 →
        // min_floor = 1000.
        check(
            limiter_a.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.0 * MICROS_PER_SEC,
        );
        check(
            limiter_a.get_limiter(ResourceType::Io).get_rate_limit(),
            1000.0,
        );
    }
}
