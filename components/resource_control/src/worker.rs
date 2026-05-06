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
    debug,
    resource_control::{DEFAULT_RESOURCE_GROUP_NAME, TaskPriority},
    sys::{SysQuota, cpu_time::ProcessStat},
    thread_name_prefix::{SCHEDULE_WORKER_PRIORITY_THREAD, UNIFIED_READ_POOL_THREAD},
    time::Instant,
    warn,
    yatp_pool::metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC,
};

use crate::{
    metrics::*,
    resource_group::ResourceGroupManager,
    resource_limiter::{GroupStatistics, ResourceLimiter, ResourceType},
};

pub const BACKGROUND_LIMIT_ADJUST_DURATION: Duration = Duration::from_secs(10);

const MICROS_PER_SEC: f64 = 1_000_000.0;
// the minimal schedule wait duration due to the overhead of queue.
// We should exclude this cause when calculate the estimated total wait
// duration.
const MINIMAL_SCHEDULE_WAIT_SECS: f64 = 0.000_005; //5us

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
}

impl GroupQuotaAdjustWorker<SysQuotaGetter> {
    pub fn new(
        resource_ctl: Arc<ResourceGroupManager>,
        io_bandwidth: u64,
        compaction_pending_bytes_ratio: Arc<AtomicU32>,
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
        )
    }
}

impl<R: ResourceStatsProvider> GroupQuotaAdjustWorker<R> {
    pub fn with_quota_getter(
        resource_ctl: Arc<ResourceGroupManager>,
        resource_quota_getter: R,
        compaction_pending_bytes_ratio: Arc<AtomicU32>,
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

        let mut bg_util_limit = self
            .resource_ctl
            .get_resource_group(DEFAULT_RESOURCE_GROUP_NAME)
            .map_or(0, |r| {
                r.group.get_background_settings().get_utilization_limit()
            });
        if bg_util_limit == 0 {
            bg_util_limit = 100;
        }
        let bg_resource_threshold = self
            .resource_ctl
            .get_config()
            .value()
            .bg_resource_threshold
            .clamp(1.0, 99.0);
        // Cap utilization limit to bg_resource_threshold. Background
        // tasks should never consume more than this fraction of total resources.
        // When CPU utilization exceeds the threshold, the headroom goes negative
        // and the background rate limit is reduced.
        bg_util_limit = bg_util_limit.min(bg_resource_threshold as u64);

        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&["limit"])
            .set(bg_util_limit as i64);

        if !self.resource_ctl.has_background_groups() {
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

        self.do_adjust(
            ResourceType::Cpu,
            dur_secs,
            bg_util_limit,
            bg_resource_threshold,
        );
        self.do_adjust(
            ResourceType::Io,
            dur_secs,
            bg_util_limit,
            bg_resource_threshold,
        );
        self.adjust_write_io_by_compaction_pressure();
    }

    fn do_adjust(
        &mut self,
        resource_type: ResourceType,
        dur_secs: f64,
        utilization_limit: u64,
        bg_resource_threshold: f64,
    ) {
        let resource_stats = match self.resource_quota_getter.get_current_stats(resource_type) {
            Ok(r) => r,
            Err(e) => {
                warn!("get resource statistics info failed, skip adjust"; "type" => ?resource_type, "err" => ?e);
                return;
            }
        };
        // if total resource quota is unlimited, set background limit to unlimited.
        if resource_stats.total_quota <= f64::EPSILON {
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
        if resource_type == ResourceType::Cpu {
            BACKGROUND_TASKS_WAIT_DURATION.inc_by(stats_delta.total_wait_dur_us);
        }
        let background_consumed = (stats_delta / dur_secs).total_consumed as f64;

        let background_util = (background_consumed / resource_stats.total_quota * 100.0) as u64;
        let resource_util = resource_stats.current_used / resource_stats.total_quota * 100.0;
        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(background_util as i64);

        let util_limit_percent = (utilization_limit as f64 / 100.0).min(1.0);
        let target = resource_stats.total_quota * util_limit_percent;

        // Treat infinity as target (initial state before first adjustment).
        let current_limit = {
            let l = self.bg_limiter.get_limiter(resource_type).get_rate_limit();
            if l.is_infinite() { target } else { l }
        };

        // Minimum: 1 CPU core for CPU, 10% of total for IO.
        let min_floor = match resource_type {
            ResourceType::Cpu => MICROS_PER_SEC,
            ResourceType::Io => resource_stats.total_quota * 0.1,
        }
        .min(target);

        let mut new_budget = target;
        if resource_util > bg_resource_threshold {
            // System is overloaded. Linearly scale budget from target down to
            // min_floor as utilization goes from threshold (70%) to 100%.
            let pressure =
                (resource_util - bg_resource_threshold) / (100.0 - bg_resource_threshold);
            new_budget = target * (1.0 - pressure) + min_floor * pressure;
        } else if current_limit > target {
            // Background limit exceeds its allowed share; reset to target.
            new_budget = target;
        } else if current_limit < 0.9 * target && resource_util < 0.9 * bg_resource_threshold {
            // System is idle; increase limit incrementally from current limit.
            new_budget = current_limit * 1.1;
        }

        let new_budget = new_budget.clamp(min_floor, target);
        self.bg_limiter
            .get_limiter(resource_type)
            .set_rate_limit(new_budget);
        BACKGROUND_QUOTA_LIMIT_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(new_budget as i64);
    }

    /// Adjust the write-only IO limiter based on compaction pressure.
    /// When pressure >= threshold, linearly scale write IO from ceiling to
    /// floor. Below threshold, write IO ramps up 10% per tick capped at
    /// ceiling.
    fn adjust_write_io_by_compaction_pressure(&self) {
        let pressure = self.compaction_pending_bytes_ratio.load(Ordering::Relaxed) as f64;
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
        debug!("adjsut cpu limiter by priority"; "cpu_quota" => process_cpu_stats.total_quota,
            "process_cpu" => process_cpu_stats.current_used, "expected_cpu" => ?level_expected,
            "cpu_costs" => ?cpu_duration, "limits" => ?limits,
            "limit_cpu_total" => expect_pool_cpu_total, "pool_cpu_cost" => real_cpu_total);
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

        // util_limit configured at 80% but capped to 70%.
        // CPU target = 8 * 0.7 = 5.6 cores, IO target = 10000 * 0.7 = 7000
        // CPU min_floor = 1 core, IO min_floor = 1000

        // No load: initial infinity → treated as target → budget = target.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Short duration (< 1s): adjustment skipped, limits unchanged.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_millis(500));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Under target (50% CPU): resource_util < 70%, current == target,
        // so none of the branches fire → budget = target.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Above 70% (80% CPU, 80% IO): resource_util > 70 branch fires.
        // Budget linearly interpolates from target to min_floor.
        // CPU: pressure = (80-70)/(100-70) = 1/3. budget = 5.6*(1-1/3) + 1.0*(1/3) ≈
        // 4.067 IO: pressure = (80-70)/(100-70) = 1/3. budget = 7000*(2/3) +
        // 1000*(1/3) ≈ 5000
        reset_quota(&mut worker, 6.4, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 4.067, 5000.0);

        // 100% CPU, 95% IO: heavier pressure.
        // CPU: pressure = (100-70)/30 = 1.0. budget = 5.6*0 + 1.0*1.0 = 1.0 (min_floor)
        // IO: pressure = (95-70)/30 = 25/30 = 5/6. budget = 7000*(1/6) + 1000*(5/6) ≈
        // 2000
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 2000.0);

        // Still at 100% CPU, 95% IO: same result (deterministic, no decay).
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 2000.0);

        // 85% CPU, 80% IO: moderate pressure.
        // CPU: pressure = (85-70)/30 = 0.5. budget = 5.6*0.5 + 1.0*0.5 = 3.3
        // IO: pressure = (80-70)/30 = 1/3. budget = 7000*(2/3) + 1000*(1/3) = 5000
        reset_quota(&mut worker, 6.8, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.3, 5000.0);

        // Load drops (50% CPU, 20% IO): resource_util < 63%, current < 0.9*target
        // → incremental increase: budget = current * 1.1
        // CPU: 3.3 * 1.1 = 3.63
        // IO: 5000 * 1.1 = 5500
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.63, 5500.0);

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

        // No load: current at 3.63M < 0.9*target (5.04M) and util < 63% → incremental.
        // budget = 3.63 * 1.1 = 3.993
        // IO: 5500 * 1.1 = 6050
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.993, 6050.0);

        // Over 70% (100% CPU, 95% IO): budget scales down.
        // CPU: pressure = 1.0. budget = min_floor = 1.0
        // IO: pressure = 25/30 = 5/6. budget = 7000*(1/6) + 1000*(5/6) = 2000
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 2000.0);
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
        // bg_util_limit defaults to 100, capped to 70 by bg_resource_threshold.
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

        // --- Saturate CPU (100%, 80% IO) → budget scales down from target ---
        // CPU: pressure = (100-70)/30 = 1.0. budget = min_floor = 1.0
        // IO: pressure = (80-70)/30 = 1/3. budget = 7000*(2/3) + 1000*(1/3) = 5000
        reset_quota(&mut worker, 8.0, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.0 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            5000.0,
        );

        // --- Unsaturate CPU (25%) → budget recovers incrementally ---
        // CPU: resource_util = 25% < 63, current 1.0M < 5.04M → budget = 1.0 * 1.1 =
        // 1.1 IO: resource_util = 20% < 63, current 5000 < 6300 → budget = 5000
        // * 1.1 = 5500
        reset_quota(&mut worker, 2.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.1 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            5500.0,
        );

        // --- 85% CPU, 80% IO: moderate pressure ---
        // CPU: pressure = (85-70)/30 = 0.5. budget = 5.6*0.5 + 1.0*0.5 = 3.3
        // IO: pressure = (80-70)/30 = 1/3. budget = 7000*(2/3) + 1000*(1/3) = 5000
        reset_quota(&mut worker, 6.8, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            3.3 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            5000.0,
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

        // CPU: pressure = 1.0 → budget = min_floor = 1.0 core.
        // IO: pressure = (95-70)/30 = 5/6 → budget = 7000/6 + 1000*5/6 ≈ 2000.
        check(
            limiter_a.get_limiter(ResourceType::Cpu).get_rate_limit(),
            1.0 * MICROS_PER_SEC,
        );
        check(
            limiter_a.get_limiter(ResourceType::Io).get_rate_limit(),
            2000.0,
        );
    }
}
