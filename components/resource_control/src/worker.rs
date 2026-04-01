// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    array,
    collections::{HashMap, HashSet},
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
    prev_stats_by_group: [HashMap<String, GroupStatistics>; ResourceType::COUNT],
    last_adjust_time: Instant,
    resource_ctl: Arc<ResourceGroupManager>,
    resource_quota_getter: R,
    // Shared compaction pressure (0-100+) written by EnginesResourceInfo::update().
    compaction_pending_bytes_ratio: Arc<AtomicU32>,
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
        let prev_stats_by_group = array::from_fn(|_| HashMap::default());
        Self {
            prev_stats_by_group,
            last_adjust_time: Instant::now_coarse(),
            resource_ctl,
            resource_quota_getter,
            compaction_pending_bytes_ratio,
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
        let (bg_scale_start, bg_resource_threshold) = {
            let config = self.resource_ctl.get_config().value();
            let start = config.bg_scale_start_threshold.clamp(1.0, 99.0);
            let end = config.bg_resource_threshold.clamp(start, 99.0);
            (start, end)
        };
        // Cap utilization limit to bg_resource_threshold. Background tasks should
        // never consume more than this fraction of total resources. Scale-down
        // begins at bg_scale_start and reaches min_floor at bg_resource_threshold.
        bg_util_limit = bg_util_limit.min(bg_resource_threshold as u64);

        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&["limit"])
            .set(bg_util_limit as i64);

        let mut background_groups: Vec<_> = self
            .resource_ctl
            .resource_groups
            .iter()
            .filter_map(|kv| {
                let g = kv.value();
                g.limiter.as_ref().map(|limiter| GroupStats {
                    name: g.group.name.clone(),
                    ru_quota: g.get_ru_quota() as f64,
                    limiter: limiter.clone(),
                })
            })
            .collect();
        if background_groups.is_empty() {
            return;
        }

        self.do_adjust(
            ResourceType::Cpu,
            dur_secs,
            bg_util_limit,
            bg_scale_start,
            bg_resource_threshold,
            &mut background_groups,
        );
        self.do_adjust(
            ResourceType::Io,
            dur_secs,
            bg_util_limit,
            bg_scale_start,
            bg_resource_threshold,
            &mut background_groups,
        );

        // Adjust write IO limiter based on compaction pressure.
        self.adjust_write_io_by_compaction_pressure(&background_groups, dur_secs);

        // clean up deleted group stats
        if self.prev_stats_by_group[0].len() != background_groups.len() {
            let name_set: HashSet<_> =
                HashSet::from_iter(background_groups.iter().map(|g| &g.name));
            for stat_map in &mut self.prev_stats_by_group {
                stat_map.retain(|k, _v| !name_set.contains(k));
            }
        }
    }

    fn do_adjust(
        &mut self,
        resource_type: ResourceType,
        dur_secs: f64,
        utilization_limit: u64,
        bg_scale_start: f64,
        bg_resource_threshold: f64,
        bg_group_stats: &mut [GroupStats],
    ) {
        let resource_stats = match self.resource_quota_getter.get_current_stats(resource_type) {
            Ok(r) => r,
            Err(e) => {
                warn!("get resource statistics info failed, skip adjust"; "type" => ?resource_type, "err" => ?e);
                return;
            }
        };
        // if total resource quota is unlimited, set all groups' limit to unlimited.
        if resource_stats.total_quota <= f64::EPSILON {
            for g in bg_group_stats {
                g.limiter
                    .get_limiter(resource_type)
                    .set_rate_limit(f64::INFINITY);
            }
            return;
        }

        // Collect statistics for metrics.
        let mut total_ru_quota = 0.0;
        let mut background_consumed_total = 0.0;
        for g in bg_group_stats.iter_mut() {
            total_ru_quota += g.ru_quota;
            let total_stats = g.limiter.get_limit_statistics(resource_type);
            let last_stats = self.prev_stats_by_group[resource_type as usize]
                .insert(g.name.clone(), total_stats)
                .unwrap_or_default();
            let stats_delta = if total_stats.version == last_stats.version {
                total_stats - last_stats
            } else {
                total_stats
            };
            BACKGROUND_RESOURCE_CONSUMPTION
                .with_label_values(&[&g.name, resource_type.as_str()])
                .inc_by(stats_delta.total_consumed);
            if resource_type == ResourceType::Cpu {
                BACKGROUND_TASKS_WAIT_DURATION
                    .with_label_values(&[&g.name])
                    .inc_by(stats_delta.total_wait_dur_us);
            }
            let stats_per_sec = stats_delta / dur_secs;
            background_consumed_total += stats_per_sec.total_consumed as f64;
        }

        let background_util =
            (background_consumed_total / resource_stats.total_quota * 100.0) as u64;
        let resource_util = resource_stats.current_used / resource_stats.total_quota * 100.0;
        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(background_util as i64);

        if total_ru_quota <= f64::EPSILON {
            return;
        }

        let util_limit_percent = (utilization_limit as f64 / 100.0).min(1.0);
        let target = resource_stats.total_quota * util_limit_percent;

        // Sum current background limits; treat infinity as an equal share of
        // the target (initial state before first adjustment).
        let current_total_bg_limit: f64 = bg_group_stats
            .iter()
            .map(|g| {
                let limit = g.limiter.get_limiter(resource_type).get_rate_limit();
                if limit.is_infinite() {
                    target / bg_group_stats.len() as f64
                } else {
                    limit
                }
            })
            .sum();

        // Minimum: 1 CPU core for CPU, 10% of total for IO.
        let min_floor = match resource_type {
            ResourceType::Cpu => MICROS_PER_SEC,
            ResourceType::Io => resource_stats.total_quota * 0.1,
        }
        .min(target);

        let mut new_total_bg_budget = target;
        if resource_util > bg_scale_start {
            // Linearly scale budget from target down to min_floor as utilization
            // goes from bg_scale_start (60%) to bg_resource_threshold (70%).
            let range = (bg_resource_threshold - bg_scale_start).max(1.0);
            let pressure = ((resource_util - bg_scale_start) / range).clamp(0.0, 1.0);
            new_total_bg_budget = target * (1.0 - pressure) + min_floor * pressure;
        } else if current_total_bg_limit > target {
            // Background limit exceeds its allowed share; reset to target.
            new_total_bg_budget = target;
        } else if current_total_bg_limit < 0.9 * target
            && resource_util < 0.9 * bg_scale_start
        {
            // System is idle; increase limit incrementally from current limit.
            new_total_bg_budget = current_total_bg_limit + 0.1 * current_total_bg_limit;
        }

        let new_total_bg_budget = new_total_bg_budget.clamp(min_floor, target);

        // Distribute proportionally by RU quota.
        for g in bg_group_stats.iter() {
            let limit = new_total_bg_budget * (g.ru_quota / total_ru_quota);
            g.limiter.get_limiter(resource_type).set_rate_limit(limit);
            BACKGROUND_QUOTA_LIMIT_VEC
                .with_label_values(&[&g.name, resource_type.as_str()])
                .set(limit as i64);
        }
    }

    /// Adjust the write-only IO limiter based on compaction pressure.
    /// When pressure >= threshold, linearly scale write IO from ceiling to
    /// floor. Below threshold, write IO is unlimited (infinity).
    ///
    /// Ceiling and floor are read from config (bg_write_io_ceiling,
    /// bg_write_io_floor) in MB/s.
    fn adjust_write_io_by_compaction_pressure(
        &self,
        bg_group_stats: &[GroupStats],
        _dur_secs: f64,
    ) {
        let pressure = self.compaction_pending_bytes_ratio.load(Ordering::Relaxed) as f64;
        let config = self.resource_ctl.get_config().value().clone();
        let threshold = config.bg_compaction_pressure_threshold.clamp(1.0, 99.0);
        let ceiling = config.bg_write_io_ceiling.0 as f64; // bytes/s
        let floor = config.bg_write_io_floor.0 as f64; // bytes/s

        let total_budget = if pressure < threshold {
            // Below threshold: ramp up current limit by 10%, capped at ceiling.
            let current_limit = bg_group_stats
                .first()
                .map(|g| g.limiter.get_write_io_limiter().get_rate_limit())
                .unwrap_or(ceiling);
            if current_limit.is_infinite() || current_limit >= ceiling {
                ceiling
            } else {
                (current_limit * 1.1).min(ceiling)
            }
        } else {
            // Linear interpolation from ceiling to floor as pressure goes from
            // threshold to 100%.
            let pressure_ratio = ((pressure - threshold) / (100.0 - threshold)).clamp(0.0, 1.0);
            let total_budget = ceiling * (1.0 - pressure_ratio) + floor * pressure_ratio;
            total_budget.max(floor)
        };

        // Distribute proportionally by RU quota.
        let total_ru_quota: f64 = bg_group_stats.iter().map(|g| g.ru_quota).sum();
        if total_ru_quota <= f64::EPSILON {
            return;
        }
        for g in bg_group_stats {
            let limit = total_budget * (g.ru_quota / total_ru_quota);
            g.limiter.get_write_io_limiter().set_rate_limit(limit);
        }
    }
}

struct GroupStats {
    name: String,
    limiter: Arc<ResourceLimiter>,
    ru_quota: f64,
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

        // Fast path: only the default resource group means resource control is not used.
        let group_count = self.resource_ctl.get_group_count();
        if group_count == 1 {
            if self.is_last_single_group {
                return;
            }
            self.is_last_single_group = true;
            let low_limiter = &self.trackers[TaskPriority::Low as usize].limiter;
            low_limiter
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(f64::INFINITY);
            PRIORITY_QUOTA_LIMIT_VEC
                .get_metric_with_label_values(&[TaskPriority::Low.as_str()])
                .unwrap()
                .set(0);
            return;
        }
        self.is_last_single_group = false;

        // Update per-priority CPU stats for metrics only.
        let dur_secs = dur.as_secs_f64();
        for tracker in &mut self.trackers {
            tracker.get_and_update_last_stats(dur_secs);
        }

        let process_cpu_stats = match self
            .resource_quota_getter
            .get_current_stats(ResourceType::Cpu)
        {
            Ok(s) => s,
            Err(e) => {
                warn!("get process total cpu failed; skip adjustment."; "err" => ?e);
                return;
            }
        };

        let config = self.resource_ctl.get_config().value();
        // Low-priority scale-down: linearly throttle from bg_resource_threshold (70%)
        // to low_pri_cpu_end_threshold (80%), mirroring how background traffic is
        // handled in the bg_scale_start→bg_resource_threshold range.
        let low_pri_start = config.bg_resource_threshold.clamp(1.0, 99.0);
        let low_pri_end = config
            .low_pri_cpu_end_threshold
            .clamp(low_pri_start + 1.0, 100.0);

        let cpu_util = if process_cpu_stats.total_quota > f64::EPSILON {
            process_cpu_stats.current_used / process_cpu_stats.total_quota * 100.0
        } else {
            0.0
        };

        let low_limiter = &self.trackers[TaskPriority::Low as usize].limiter;
        if cpu_util <= low_pri_start {
            if !self.is_last_low_cpu {
                self.is_last_low_cpu = true;
                low_limiter
                    .get_limiter(ResourceType::Cpu)
                    .set_rate_limit(f64::INFINITY);
                PRIORITY_QUOTA_LIMIT_VEC
                    .get_metric_with_label_values(&[TaskPriority::Low.as_str()])
                    .unwrap()
                    .set(0);
            }
            return;
        }
        self.is_last_low_cpu = false;

        // 1% of total CPU as minimum floor for low-priority tasks.
        let min_floor = process_cpu_stats.total_quota * 0.01;
        let range = (low_pri_end - low_pri_start).max(1.0);
        let pressure = ((cpu_util - low_pri_start) / range).clamp(0.0, 1.0);
        let low_pri_limit =
            (process_cpu_stats.total_quota * (1.0 - pressure) + min_floor * pressure)
                .max(min_floor);

        low_limiter
            .get_limiter(ResourceType::Cpu)
            .set_rate_limit(low_pri_limit);
        PRIORITY_QUOTA_LIMIT_VEC
            .get_metric_with_label_values(&[TaskPriority::Low.as_str()])
            .unwrap()
            .set(low_pri_limit as i64);
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

        // util_limit configured at 80% but capped to bg_resource_threshold=70%.
        // CPU target = 8 * 0.7 = 5.6 cores, IO target = 10000 * 0.7 = 7000
        // CPU min_floor = 1 core, IO min_floor = 1000
        // bg_scale_start=60%, bg_resource_threshold=70% (scale range: 60→70).

        // No load: initial infinity → treated as target → budget = target.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Short duration (< 1s): adjustment skipped, limits unchanged.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_millis(500));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // Under target (50% CPU): resource_util < 60%, current == target,
        // so none of the branches fire → budget = target.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 5.6, 7000.0);

        // 80% CPU, 80% IO: resource_util > 60% (bg_scale_start).
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
        // CPU: pressure = (65-60)/(70-60) = 0.5. budget = 5.6*0.5 + 1.0*0.5 = 3.3
        // IO: pressure = 0.5. budget = 7000*0.5 + 1000*0.5 = 4000
        reset_quota(&mut worker, 5.2, 6500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.3, 4000.0);

        // Load drops (50% CPU, 20% IO): resource_util < 54% (0.9*60%), current < 0.9*target
        // → incremental increase: budget = current * 1.1
        // CPU: 3.3 * 1.1 = 3.63. IO: 4000 * 1.1 = 4400
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.63, 4400.0);

        // --- Multi-group: proportional distribution by RU quota ---
        let bg = new_background_resource_group_ru(
            BACKGROUND_WORKER_THREAD.into(),
            1000,
            15,
            vec!["br".into()],
        );
        resource_ctl.add_resource_group(bg);
        let bg_limiter = resource_ctl
            .get_background_resource_limiter(BACKGROUND_WORKER_THREAD, "br")
            .unwrap();

        // default ru=2000, bg ru=1000 → shares: 2/3, 1/3
        // No load: default at 3.63M, bg at infinity → target/2 = 2.8M
        // current_total = 3.63M + 2.8M = 6.43M > target (5.6M) → budget = target = 5.6M
        // default: 5.6 * 2/3 ≈ 3.733, bg: 5.6/3 ≈ 1.867
        // IO: current_total = 4400 + 3500 = 7900 > 7000 → budget = 7000
        // default: 7000 * 2/3 ≈ 4666.7, bg: 7000/3 ≈ 2333.3
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.733, 4666.7);
        check_limiter_rates(&bg_limiter, 1.867, 2333.3);

        // Over 60% (100% CPU, 95% IO): budget scales down from target.
        // CPU: pressure = (100-60)/(70-60) = 4.0 clamped 1.0 → min_floor = 1.0M.
        //   default: 1.0M * 2/3 ≈ 0.667, bg: 1.0M/3 ≈ 0.333
        // IO: pressure clamped 1.0 → min_floor = 1000.
        //   default: 1000 * 2/3 ≈ 666.7, bg: 1000/3 ≈ 333.3
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 0.667, 666.7);
        check_limiter_rates(&bg_limiter, 0.333, 333.3);

        // --- Limiter version change ---
        let bg = new_resource_group_ru(BACKGROUND_WORKER_THREAD.into(), 1000, 15);
        resource_ctl.add_resource_group(bg);

        let new_bg = new_background_resource_group_ru(
            BACKGROUND_WORKER_THREAD.into(),
            1000,
            15,
            vec!["br".into()],
        );
        resource_ctl.add_resource_group(new_bg);
        let new_bg_limiter = resource_ctl
            .get_background_resource_limiter(BACKGROUND_WORKER_THREAD, "br")
            .unwrap();
        assert_ne!(&*bg_limiter as *const _, &*new_bg_limiter as *const _);
        assert!(
            new_bg_limiter
                .get_limit_statistics(ResourceType::Cpu)
                .version
                > bg_limiter.get_limit_statistics(ResourceType::Cpu).version
        );
        let cpu_stats = new_bg_limiter.get_limit_statistics(ResourceType::Cpu);
        assert_eq!(cpu_stats.total_consumed, 0);
        assert_eq!(cpu_stats.total_wait_dur_us, 0);
        let io_stats = new_bg_limiter.get_limit_statistics(ResourceType::Io);
        assert_eq!(io_stats.total_consumed, 0);
        assert_eq!(io_stats.total_wait_dur_us, 0);

        // New bg limiter at infinity → target/2 = 2.8M.
        // default at 0.667M. current_total = 0.667M + 2.8M = 3.467M
        // 3.467M < 5.04M and util=0% < 54% → incremental: budget = 3.467 * 1.1 = 3.8137M
        // default: 3.8137 * 2/3 ≈ 2.542, new_bg: 3.8137/3 ≈ 1.271
        // IO: default 666.7, new_bg inf→3500, total=4166.7 < 6300 → 4166.7*1.1=4583.4
        // default: 4583.4 * 2/3 ≈ 3055.6, new_bg: 4583.4/3 ≈ 1527.8
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 2.542, 3055.6);
        check_limiter_rates(&new_bg_limiter, 1.271, 1527.8);
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
        // bg_scale_start=60%, bg_resource_threshold=70%.
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
        // Only bg_worker has a limiter; fg groups are not in bg_group_stats.

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
        // CPU: resource_util = 25% < 54% (0.9*60%), current 1.0M < 5.04M → 1.0 * 1.1 = 1.1
        // IO: resource_util = 20% < 54%, current 1000 < 6300 → 1000 * 1.1 = 1100
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
        // CPU: pressure = (65-60)/(70-60) = 0.5. budget = 5.6*0.5 + 1.0*0.5 = 3.3
        // IO: pressure = 0.5. budget = 7000*0.5 + 1000*0.5 = 4000
        reset_quota(&mut worker, 5.2, 6500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            3.3 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            4000.0,
        );
    }

    #[test]
    fn test_adjust_priority_resource_limiter() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());
        let priority_limiters = resource_ctl.get_priority_resource_limiters();
        // 8 CPU cores. low_pri_start=70% (bg_resource_threshold default),
        // low_pri_end=80% (low_pri_cpu_end_threshold default).
        // min_floor = 8M * 0.01 = 0.08M µs = 0.08 cores.
        let test_provider = TestResourceStatsProvider::new(8.0, f64::INFINITY);
        let mut worker =
            PriorityLimiterAdjustWorker::with_quota_getter(resource_ctl.clone(), test_provider);

        let reset_quota = |worker: &mut PriorityLimiterAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(10);
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

        let check_limiters = |high: f64, medium: f64, low: f64| {
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

        // Only default group: always infinity for all priorities.
        reset_quota(&mut worker, 6.4);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        let rg1 = new_resource_group_ru("test_high".into(), 1000, 16);
        resource_ctl.add_resource_group(rg1);
        let rg2 = new_resource_group_ru("test_low".into(), 2000, 1);
        resource_ctl.add_resource_group(rg2);

        // CPU=60% (4.8 cores) ≤ 70% (low_pri_start) → low priority unlimited.
        reset_quota(&mut worker, 4.8);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        // CPU=70% (5.6 cores): exactly at threshold → low priority unlimited.
        reset_quota(&mut worker, 5.6);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        // CPU=75% (6.0 cores): pressure = (75-70)/(80-70) = 0.5.
        // limit = 8M*(1-0.5) + 0.08M*0.5 = 4M + 0.04M = 4.04M µs = 4.04 cores.
        // High and medium stay infinity.
        reset_quota(&mut worker, 6.0);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, 4.04);

        // CPU=80% (6.4 cores): pressure = 1.0 → min_floor = 0.08 cores.
        reset_quota(&mut worker, 6.4);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, 0.08);

        // CPU=100% (8.0 cores): pressure clamped 1.0 → min_floor = 0.08 cores.
        reset_quota(&mut worker, 8.0);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, 0.08);

        // CPU drops back to 65% ≤ 70% → low priority restored to infinity.
        reset_quota(&mut worker, 5.2);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        // Duration too small: limits unchanged.
        worker.resource_quota_getter.cpu_used = 8.0;
        worker.last_adjust_time = Instant::now_coarse() - Duration::from_millis(500);
        worker.adjust();
        check_limiters(f64::INFINITY, f64::INFINITY, f64::INFINITY);
    }

    /// Verifies that the three load-shedding tiers are contiguous and do not
    /// bleed into each other:
    ///
    /// - 0–60% CPU   : background full, low-priority unlimited
    /// - 60–70% CPU  : background linearly throttled, low-priority unlimited
    /// - 70–80% CPU  : background at min_floor, low-priority linearly throttled
    /// - >80% CPU    : both at min_floor
    #[test]
    fn test_tiered_load_shedding() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // One background group (bg_util_limit from default = 100% capped to 70%).
        let bg = new_background_resource_group_ru("bg".into(), 1000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let bg_limiter = resource_ctl
            .get_background_resource_limiter("bg", "br")
            .unwrap();

        // Two foreground groups so group_count > 1 and low-priority limiter fires.
        resource_ctl.add_resource_group(new_resource_group_ru("fg_high".into(), 1000, 16));
        resource_ctl.add_resource_group(new_resource_group_ru("fg_low".into(), 1000, 1));

        let priority_limiters = resource_ctl.get_priority_resource_limiters();
        let low_limiter = &priority_limiters[TaskPriority::Low as usize];

        let compaction_pending_bytes_ratio = Arc::new(AtomicU32::new(0));
        let mut bg_worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            TestResourceStatsProvider::new(8.0, 10000.0),
            compaction_pending_bytes_ratio,
        );
        let mut pri_worker = PriorityLimiterAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            TestResourceStatsProvider::new(8.0, f64::INFINITY),
        );

        // bg: target = 8 * 0.7 = 5.6 cores, min_floor = 1.0 core.
        // low: total = 8M µs, min_floor = 8M * 0.01 = 0.08M µs = 0.08 cores.
        const BG_TARGET: f64 = 5.6;
        const BG_FLOOR: f64 = 1.0;
        const LOW_FLOOR: f64 = 0.08;

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
                pri_worker.resource_quota_getter.cpu_used = $cpu;
                pri_worker.last_adjust_time = Instant::now_coarse() - Duration::from_secs(10);
                pri_worker.adjust();
            }};
        }

        // 50% CPU — below bg_scale_start (60%): bg=full, low=unlimited.
        tick!(4.0);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_TARGET * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            f64::INFINITY,
        );

        // 60% CPU — exactly at bg_scale_start: condition is `> 60%`, so no
        // scale-down yet. bg=full, low=unlimited.
        tick!(4.8);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_TARGET * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            f64::INFINITY,
        );

        // 65% CPU — mid bg range (60–70%): pressure = (65-60)/(70-60) = 0.5.
        // bg = 5.6*0.5 + 1.0*0.5 = 3.3 cores. low = unlimited.
        tick!(5.2);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            3.3 * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            f64::INFINITY,
        );

        // 70% CPU — bg fully throttled (pressure=1.0 → min_floor).
        // low_pri_start = bg_resource_threshold = 70%: condition is `> 70%`, so
        // low-priority is still unlimited at exactly 70%.
        tick!(5.6);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            f64::INFINITY,
        );

        // 75% CPU — bg stays at min_floor; low enters scale range (70–80%).
        // low pressure = (75-70)/(80-70) = 0.5.
        // low limit = 8M*(1-0.5) + 0.08M*0.5 = 4.04 cores.
        tick!(6.0);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            4.04 * MICROS_PER_SEC,
        );

        // 80% CPU — both at min_floor.
        tick!(6.4);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            LOW_FLOOR * MICROS_PER_SEC,
        );

        // 90% CPU — still both at min_floor (pressure clamped to 1.0).
        tick!(7.2);
        approx(
            bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            BG_FLOOR * MICROS_PER_SEC,
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            LOW_FLOOR * MICROS_PER_SEC,
        );

        // Recovery: CPU drops to 50% — bg ramps up incrementally, low=unlimited.
        tick!(4.0);
        let bg_rate = bg_limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        assert!(
            bg_rate > BG_FLOOR * MICROS_PER_SEC && bg_rate < BG_TARGET * MICROS_PER_SEC,
            "bg should be recovering, got {bg_rate}"
        );
        approx(
            low_limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            f64::INFINITY,
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
}
