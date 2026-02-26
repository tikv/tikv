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
        Self::with_quota_getter(resource_ctl, resource_quota_getter, compaction_pending_bytes_ratio)
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

        let mut background_util_limit = self
            .resource_ctl
            .get_resource_group(DEFAULT_RESOURCE_GROUP_NAME)
            .map_or(0, |r| {
                r.group.get_background_settings().get_utilization_limit()
            });
        if background_util_limit == 0 {
            background_util_limit = 100;
        }

        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&["limit"])
            .set(background_util_limit as i64);

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
            background_util_limit,
            &mut background_groups,
        );
        self.do_adjust(
            ResourceType::Io,
            dur_secs,
            background_util_limit,
            &mut background_groups,
        );

        // Adjust write IO limiter based on compaction pressure.
        self.adjust_write_io_by_compaction_pressure(&background_groups);

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
        BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC
            .with_label_values(&[resource_type.as_str()])
            .set(background_util as i64);

        if total_ru_quota <= f64::EPSILON {
            return;
        }

        let util_limit_percent = (utilization_limit as f64 / 100.0).min(1.0);
        let target = resource_stats.total_quota * util_limit_percent;
        let headroom = target - resource_stats.current_used;

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
        };

        // If resources are available (positive headroom), increase the limit.
        // If over target (negative headroom), reduce proportionally.
        let new_total_bg_budget = (current_total_bg_limit + headroom).clamp(min_floor, target);

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
    /// When compaction pressure >= 70%, progressively throttle write IO down to
    /// 10% of the current combined IO limit at 100% pressure.
    /// When pressure < 70%, set write IO to unlimited.
    fn adjust_write_io_by_compaction_pressure(&self, bg_group_stats: &[GroupStats]) {
        let pressure = self.compaction_pending_bytes_ratio.load(Ordering::Relaxed) as f64;

        if pressure < 70.0 {
            for g in bg_group_stats {
                g.limiter.get_write_io_limiter().set_rate_limit(f64::INFINITY);
            }
            return;
        }

        // pressure_ratio: 0.0 at 70%, 1.0 at 100%+
        let pressure_ratio = ((pressure - 70.0) / 30.0).clamp(0.0, 1.0);
        // throttle_factor: 1.0 at 70%, 0.1 at 100%
        let throttle_factor = 1.0 - 0.9 * pressure_ratio;

        for g in bg_group_stats {
            // Use the current combined IO limit as the base for the write budget.
            let io_limit = g.limiter.get_limiter(ResourceType::Io).get_rate_limit();
            let base = if io_limit.is_infinite() {
                // If combined IO is unlimited, use proportional share of a
                // reasonable default; the write limiter stays unlimited too.
                g.limiter.get_write_io_limiter().set_rate_limit(f64::INFINITY);
                continue;
            } else {
                io_limit
            };
            let write_io_budget = base * throttle_factor;
            g.limiter
                .get_write_io_limiter()
                .set_rate_limit(write_io_budget);
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
        let compaction_pressure = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pressure,
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

        // CPU target = 8 * 0.8 = 6.4 cores, IO target = 10000 * 0.8 = 8000
        // CPU min_floor = 1 core, IO min_floor = 1000

        // No load: initial infinity → budget = target.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 6.4, 8000.0);

        // Short duration (< 1s): adjustment skipped, limits unchanged.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_millis(500));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 6.4, 8000.0);

        // Under target (50% CPU): headroom positive, budget stays at target.
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 6.4, 8000.0);

        // At target (80% CPU, 80% IO): headroom = 0, budget unchanged.
        reset_quota(&mut worker, 6.4, 8000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 6.4, 8000.0);

        // Over target (100% CPU): throttle.
        // CPU: headroom = 6.4 - 8.0 = -1.6, budget = 6.4 - 1.6 = 4.8
        // IO: headroom = 8000 - 9500 = -1500, budget = 8000 - 1500 = 6500
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 4.8, 6500.0);

        // Still over target: further reduction.
        // CPU: headroom = 6.4 - 7.5 = -1.1, budget = 4.8 - 1.1 = 3.7
        // IO: headroom = 8000 - 9500 = -1500, budget = 6500 - 1500 = 5000
        reset_quota(&mut worker, 7.5, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 3.7, 5000.0);

        // Load drops: budget recovers.
        // CPU: headroom = 6.4 - 4.0 = 2.4, budget = clamp(3.7 + 2.4, 1.0, 6.4) = 6.1
        // IO: headroom = 8000 - 2000 = 6000, budget = clamp(5000 + 6000, 1000, 8000) = 8000
        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 6.1, 8000.0);

        // Sustained overload drives budget to min_floor.
        // Each cycle: CPU headroom = -1.6, IO headroom = -1500
        // CPU: 6.1 → 4.5 → 2.9 → 1.3 → 1.0 → 1.0
        // IO: 8000 → 6500 → 5000 → 3500 → 2000 → 1000
        for _ in 0..5 {
            reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
            worker.adjust_quota();
        }
        check_limiter_rates(&limiter, 1.0, 1000.0);

        // At floor, stays at floor under continued overload.
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 1.0, 1000.0);

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
        // Under target: budget = target = 6.4 CPU, 8000 IO
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        // default: 6.4*2/3 ≈ 4.27, bg: 6.4/3 ≈ 2.13
        check_limiter_rates(&limiter, 4.27, 5333.0);
        check_limiter_rates(&bg_limiter, 2.13, 2667.0);

        // Over target: budget shrinks, still distributed proportionally.
        // CPU: budget = clamp(6.4 - 1.6, 1.0, 6.4) = 4.8
        // IO: budget = clamp(8000 - 1500, 1000, 8000) = 6500
        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        // default: 4.8*2/3 = 3.2, bg: 4.8/3 = 1.6
        check_limiter_rates(&limiter, 3.2, 4333.0);
        check_limiter_rates(&bg_limiter, 1.6, 2167.0);

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

        // New bg limiter starts at infinity → treated as target/2.
        // Under no load: budget = target, distributed by RU quota.
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter_rates(&limiter, 4.27, 5333.0);
        check_limiter_rates(&new_bg_limiter, 2.13, 2667.0);
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
        priority_limiters[1].consume(Duration::from_secs(50), IoBytes::default(), true);
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        let rg1 = new_resource_group_ru("test_high".into(), 1000, 16);
        resource_ctl.add_resource_group(rg1);
        let rg2 = new_resource_group_ru("test_low".into(), 2000, 1);
        resource_ctl.add_resource_group(rg2);

        reset_quota(&mut worker, 6.4);
        priority_limiters[1].consume(Duration::from_secs(64), IoBytes::default(), true);
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[0].consume(Duration::from_millis(240), IoBytes::default(), true);
            priority_limiters[1].consume(Duration::from_millis(400), IoBytes::default(), true);
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 0.8);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[0].consume(Duration::from_millis(120), IoBytes::default(), true);
            priority_limiters[1].consume(Duration::from_millis(200), IoBytes::default(), true);
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 1.6, 0.8);

        reset_quota(&mut worker, 6.4);
        for _i in 0..100 {
            priority_limiters[2].consume(Duration::from_millis(200), IoBytes::default(), true);
        }
        worker.adjust();
        check_limiter(f64::INFINITY, f64::INFINITY, f64::INFINITY);

        reset_quota(&mut worker, 8.0);
        for _i in 0..100 {
            priority_limiters[0].consume(Duration::from_millis(240), IoBytes::default(), true);
            priority_limiters[1].consume(Duration::from_millis(240), IoBytes::default(), true);
            priority_limiters[2].consume(Duration::from_millis(320), IoBytes::default(), true);
        }
        worker.adjust();
        check_limiter(f64::INFINITY, 3.2, 0.8);

        reset_quota(&mut worker, 6.0);
        for _i in 0..100 {
            priority_limiters[0].consume(Duration::from_millis(240), IoBytes::default(), true);
            priority_limiters[2].consume(Duration::from_millis(360), IoBytes::default(), true);
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
    fn test_compaction_pressure_write_io_throttle() {
        let resource_ctl = Arc::new(ResourceGroupManager::default());

        // 8 CPU cores, 10000 bytes/s IO bandwidth.
        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let compaction_pressure = Arc::new(AtomicU32::new(0));
        let mut worker = GroupQuotaAdjustWorker::with_quota_getter(
            resource_ctl.clone(),
            test_provider,
            compaction_pressure.clone(),
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

        // First adjustment: establish baseline limits.
        // CPU target = 6.4 cores, IO target = 8000
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let cpu_limit = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        let io_limit = limiter.get_limiter(ResourceType::Io).get_rate_limit();
        check(cpu_limit, 6.4 * MICROS_PER_SEC);
        check(io_limit, 8000.0);

        // Pressure = 0: write IO limiter should be unlimited.
        assert!(limiter.get_write_io_limiter().get_rate_limit().is_infinite());

        // Pressure = 50 (< 70): write IO should still be unlimited.
        compaction_pressure.store(50, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        assert!(limiter.get_write_io_limiter().get_rate_limit().is_infinite());

        // Verify CPU and IO limits are unchanged at target.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            6.4 * MICROS_PER_SEC,
        );
        check(
            limiter.get_limiter(ResourceType::Io).get_rate_limit(),
            8000.0,
        );

        // Pressure = 70: throttle_factor = 1.0 - 0.9 * 0.0 = 1.0
        // write IO budget = io_limit * 1.0 = 8000
        compaction_pressure.store(70, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let io_limit_now = limiter.get_limiter(ResourceType::Io).get_rate_limit();
        check(
            limiter.get_write_io_limiter().get_rate_limit(),
            io_limit_now * 1.0,
        );

        // CPU and combined IO limits should not be affected by compaction pressure.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            6.4 * MICROS_PER_SEC,
        );

        // Pressure = 85: pressure_ratio = (85-70)/30 = 0.5
        // throttle_factor = 1.0 - 0.9 * 0.5 = 0.55
        compaction_pressure.store(85, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let io_limit_now = limiter.get_limiter(ResourceType::Io).get_rate_limit();
        check(
            limiter.get_write_io_limiter().get_rate_limit(),
            io_limit_now * 0.55,
        );

        // CPU limit unaffected.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            6.4 * MICROS_PER_SEC,
        );

        // Pressure = 100: pressure_ratio = 1.0
        // throttle_factor = 1.0 - 0.9 * 1.0 = 0.1
        compaction_pressure.store(100, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        let io_limit_now = limiter.get_limiter(ResourceType::Io).get_rate_limit();
        check(
            limiter.get_write_io_limiter().get_rate_limit(),
            io_limit_now * 0.1,
        );

        // CPU limit unaffected.
        check(
            limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
            6.4 * MICROS_PER_SEC,
        );

        // Pressure drops back to 0: write IO should become unlimited again.
        compaction_pressure.store(0, Ordering::Relaxed);
        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        assert!(limiter.get_write_io_limiter().get_rate_limit().is_infinite());
    }
}
