// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{array, collections::HashMap, io::Result as IoResult, sync::Arc, time::Duration};

use file_system::{fetch_io_bytes, IoBytes, IoType};
use strum::EnumCount;
use tikv_util::{
    sys::{cpu_time::ProcessStat, SysQuota},
    time::Instant,
    warn,
};

use crate::{
    resource_group::ResourceGroupManager,
    resource_limiter::{GroupStatistics, ResourceLimiter, ResourceType},
};

pub const BACKGROUND_LIMIT_ADJUST_DURATION: Duration = Duration::from_secs(10);

const MICROS_PER_SEC: f64 = 1_000_000.0;

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
    io_bandwidth: u64,
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
                    total_quota: self.io_bandwidth as f64,
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
    is_last_time_low_load: [bool; ResourceType::COUNT],
    resource_quota_getter: R,
}

impl GroupQuotaAdjustWorker<SysQuotaGetter> {
    pub fn new(resource_ctl: Arc<ResourceGroupManager>, io_bandwidth: u64) -> Self {
        let resource_quota_getter = SysQuotaGetter {
            process_stat: ProcessStat::cur_proc_stat().unwrap(),
            prev_io_stats: [IoBytes::default(); IoType::COUNT],
            prev_io_ts: Instant::now_coarse(),
            io_bandwidth,
        };
        Self::with_quota_getter(resource_ctl, resource_quota_getter)
    }
}

impl<R: ResourceStatsProvider> GroupQuotaAdjustWorker<R> {
    pub fn with_quota_getter(
        resource_ctl: Arc<ResourceGroupManager>,
        resource_quota_getter: R,
    ) -> Self {
        let prev_stats_by_group = array::from_fn(|_| HashMap::default());
        Self {
            prev_stats_by_group,
            last_adjust_time: Instant::now_coarse(),
            resource_ctl,
            resource_quota_getter,
            is_last_time_low_load: array::from_fn(|_| false),
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
                    stats_per_sec: GroupStatistics::default(),
                    expect_cost_rate: 0.0,
                })
            })
            .collect();
        if background_groups.is_empty() {
            return;
        }

        self.do_adjust(ResourceType::Cpu, dur_secs, &mut background_groups);
        self.do_adjust(ResourceType::Io, dur_secs, &mut background_groups);
    }

    fn do_adjust(
        &mut self,
        resource_type: ResourceType,
        dur_secs: f64,
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

        let mut total_ru_quota = 0.0;
        let mut background_consumed_total = 0.0;
        let mut has_wait = false;
        for g in bg_group_stats.iter_mut() {
            total_ru_quota += g.ru_quota;
            let total_stats = g.limiter.get_limiter(resource_type).get_statistics();
            let stats_per_sec = (total_stats
                - self.prev_stats_by_group[resource_type as usize]
                    .insert(g.name.clone(), total_stats)
                    .unwrap_or_default())
                / dur_secs;
            background_consumed_total += stats_per_sec.total_consumed as f64;
            g.stats_per_sec = stats_per_sec;
            if stats_per_sec.total_wait_dur_us > 0 {
                has_wait = true;
            }
        }

        // fast path if process cpu is low
        let is_low_load = resource_stats.current_used <= (resource_stats.total_quota * 0.1);
        if is_low_load && !has_wait && self.is_last_time_low_load[resource_type as usize] {
            return;
        }
        self.is_last_time_low_load[resource_type as usize] = is_low_load;

        // the available resource for background tasks is defined as:
        // (total_resource_quota - foreground_task_used). foreground_task_used
        // resource is calculated by: (resource_current_total_used -
        // background_consumed_total). We reserve 10% of the free resources for
        // foreground tasks in case the fore ground traffics increases.
        let mut available_resource_rate = ((resource_stats.total_quota
            - resource_stats.current_used
            + background_consumed_total)
            * 0.9)
            .max(resource_stats.total_quota * 0.1);
        let mut total_expected_cost = 0.0;
        for g in bg_group_stats.iter_mut() {
            let mut rate_limit = g.limiter.get_limiter(resource_type).get_rate_limit();
            if rate_limit.is_infinite() {
                rate_limit = 0.0;
            }
            let group_expected_cost = g.stats_per_sec.total_consumed as f64
                + g.stats_per_sec.total_wait_dur_us as f64 / MICROS_PER_SEC * rate_limit;
            g.expect_cost_rate = group_expected_cost;
            total_expected_cost += group_expected_cost;
        }
        // sort groups by the expect_cost_rate per ru
        bg_group_stats.sort_by(|g1, g2| {
            (g1.expect_cost_rate / g1.ru_quota)
                .partial_cmp(&(g2.expect_cost_rate / g2.ru_quota))
                .unwrap()
        });

        // quota is enough, group is allowed to got more resource then its share by ru.
        // e.g. Given a totol resource of 10000, and ("name", ru_quota, expected_rate)
        // of:  (rg1, 2000, 3000), (rg2, 3000, 1000), (rg3, 5000, 5000)
        // then after the previous sort, the order is rg2, rg3, rg1 and the handle order
        // is rg1, rg3, rg2 so the final rate limit assigned is: (rg1, 3000),
        // (rg3, 5833(7000/6*5)), (rg2, 1166(7000/6*1))
        if total_expected_cost <= available_resource_rate {
            for g in bg_group_stats.iter().rev() {
                let limit = g
                    .expect_cost_rate
                    .max(available_resource_rate / total_ru_quota * g.ru_quota);
                g.limiter.get_limiter(resource_type).set_rate_limit(limit);
                available_resource_rate -= limit;
                total_ru_quota -= g.ru_quota;
            }
            return;
        }

        // quota is not enough, assign by share
        // e.g. Given a totol resource of 10000, and ("name", ru_quota, expected_rate)
        // of:  (rg1, 2000, 1000), (rg2, 3000, 5000), (rg3, 5000, 7000)
        // then after the previous sort, the order is rg1, rg3, rg2, and handle order is
        // rg1, rg3, rg2 so the final rate limit assigned is: (rg1, 1000), (rg3,
        // 5250(9000/12*7)), (rg2, 3750(9000/12*5))
        for g in bg_group_stats {
            let limit = g
                .expect_cost_rate
                .min(available_resource_rate / total_ru_quota * g.ru_quota);
            g.limiter.get_limiter(resource_type).set_rate_limit(limit);
            available_resource_rate -= limit;
            total_ru_quota -= g.ru_quota;
        }
    }
}

struct GroupStats {
    name: String,
    limiter: Arc<ResourceLimiter>,
    ru_quota: f64,
    stats_per_sec: GroupStatistics,
    expect_cost_rate: f64,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{resource_group::tests::*, resource_limiter::QuotaLimiter};

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
                    total_quota: self.cpu_total * 1_000_000.0,
                    current_used: self.cpu_used * 1_000_000.0,
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
        let rg1 = new_resource_group_ru("test".into(), 1000, 14);
        resource_ctl.add_resource_group(rg1);
        assert!(resource_ctl.get_resource_limiter("test", "br").is_none());

        let test_provider = TestResourceStatsProvider::new(8.0, 10000.0);
        let mut worker =
            GroupQuotaAdjustWorker::with_quota_getter(resource_ctl.clone(), test_provider);

        let default_bg =
            new_background_resource_group_ru("default".into(), 100000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(default_bg);
        assert!(
            resource_ctl
                .get_resource_limiter("default", "lightning")
                .is_none()
        );
        let limiter = resource_ctl.get_resource_limiter("default", "br").unwrap();
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

        fn reset_quota_limiter(limiter: &QuotaLimiter) {
            let limit = limiter.get_rate_limit();
            if limit.is_finite() {
                limiter.set_rate_limit(f64::INFINITY);
                limiter.set_rate_limit(limit);
            }
        }

        fn reset_limiter(limiter: &Arc<ResourceLimiter>) {
            reset_quota_limiter(limiter.get_limiter(ResourceType::Cpu));
            reset_quota_limiter(limiter.get_limiter(ResourceType::Io));
        }

        let reset_quota = |worker: &mut GroupQuotaAdjustWorker<TestResourceStatsProvider>,
                           cpu: f64,
                           io: f64,
                           dur: Duration| {
            worker.resource_quota_getter.cpu_used = cpu;
            worker.resource_quota_getter.io_used = io;
            let now = Instant::now_coarse();
            worker.last_adjust_time = now - dur;
        };

        fn check(val: f64, expected: f64) {
            assert!(
                expected * 0.99 < val && val < expected * 1.01,
                "actual: {}, expected: {}",
                val,
                expected
            );
        }

        fn check_limiter(limiter: &Arc<ResourceLimiter>, cpu: f64, io: f64) {
            check(
                limiter.get_limiter(ResourceType::Cpu).get_rate_limit(),
                cpu * 1_000_000.0,
            );
            check(limiter.get_limiter(ResourceType::Io).get_rate_limit(), io);
            reset_limiter(limiter);
        }

        reset_quota(&mut worker, 0.0, 0.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter(&limiter, 7.2, 9000.0);

        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_millis(500));
        worker.adjust_quota();
        check_limiter(&limiter, 7.2, 9000.0);

        reset_quota(&mut worker, 4.0, 2000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter(&limiter, 3.6, 7200.0);

        reset_quota(&mut worker, 6.0, 4000.0, Duration::from_secs(1));
        limiter.consume(Duration::from_secs(2), 2000);
        worker.adjust_quota();
        check_limiter(&limiter, 3.6, 7200.0);

        reset_quota(&mut worker, 8.0, 9500.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter(&limiter, 0.8, 1000.0);

        reset_quota(&mut worker, 7.5, 9500.0, Duration::from_secs(1));
        limiter.consume(Duration::from_secs(2), 2000);
        worker.adjust_quota();
        check_limiter(&limiter, 2.25, 2250.0);

        reset_quota(&mut worker, 7.5, 9500.0, Duration::from_secs(5));
        limiter.consume(Duration::from_secs(10), 10000);
        worker.adjust_quota();
        check_limiter(&limiter, 2.25, 2250.0);

        let default =
            new_background_resource_group_ru("default".into(), 2000, 8, vec!["br".into()]);
        resource_ctl.add_resource_group(default);
        let new_limiter = resource_ctl.get_resource_limiter("default", "br").unwrap();
        assert_eq!(&*new_limiter as *const _, &*limiter as *const _);

        let bg = new_background_resource_group_ru("background".into(), 1000, 15, vec!["br".into()]);
        resource_ctl.add_resource_group(bg);
        let bg_limiter = resource_ctl
            .get_resource_limiter("background", "br")
            .unwrap();

        reset_quota(&mut worker, 5.0, 7000.0, Duration::from_secs(1));
        worker.adjust_quota();
        check_limiter(&limiter, 1.8, 1800.0);
        check_limiter(&bg_limiter, 0.9, 900.0);

        reset_quota(&mut worker, 6.0, 5000.0, Duration::from_secs(1));
        limiter.consume(Duration::from_millis(1200), 1200);
        bg_limiter.consume(Duration::from_millis(1800), 1800);
        worker.adjust_quota();
        check_limiter(&limiter, 2.4, 3600.0);
        check_limiter(&bg_limiter, 2.1, 3600.0);
    }
}
