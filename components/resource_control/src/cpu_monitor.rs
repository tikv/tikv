// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::compat::Future01CompatExt;
use tikv_util::{
    info,
    sys::{
        thread::{self, Pid, THREAD_NAME_HASHMAP},
        SysQuota,
    },
    timer::GLOBAL_TIMER_HANDLE,
    worker::Worker,
};

use crate::cpu_throttle::CpuThrottleManager;

struct CpuThrottleMonitorGuard {
    manager: Arc<CpuThrottleManager>,
}

impl Drop for CpuThrottleMonitorGuard {
    fn drop(&mut self) {
        self.manager.on_cpu_monitor_stopped();
    }
}

struct ThreadCollector {
    process_id: Pid,
    thread_prefix: String,
    last_per_thread_cpu: HashMap<Pid, f64>,
}

impl ThreadCollector {
    fn new(thread_prefix: impl Into<String>) -> Self {
        Self {
            process_id: thread::process_id(),
            thread_prefix: thread_prefix.into(),
            last_per_thread_cpu: HashMap::new(),
        }
    }

    fn collect_delta_cpu_time(&mut self) -> f64 {
        self.collect_delta_from_snapshot(self.collect_current_per_thread_cpu())
    }

    fn collect_current_per_thread_cpu(&self) -> HashMap<Pid, f64> {
        let thread_name_map = THREAD_NAME_HASHMAP.lock().unwrap();
        let current_tids: Vec<Pid> = thread_name_map
            .iter()
            .filter(|(_, name)| name.starts_with(&self.thread_prefix))
            .map(|(tid, _)| *tid)
            .collect();
        drop(thread_name_map);

        let mut current_per_thread_cpu = HashMap::with_capacity(current_tids.len());
        for tid in current_tids {
            if let Ok(stat) = thread::thread_stat(self.process_id, tid) {
                current_per_thread_cpu.insert(tid, stat.total_cpu_time());
            }
        }
        current_per_thread_cpu
    }

    fn collect_delta_from_snapshot(&mut self, current_per_thread_cpu: HashMap<Pid, f64>) -> f64 {
        let mut delta_cpu_sec = 0.0;
        for (tid, current_cpu_sec) in &current_per_thread_cpu {
            if let Some(previous_cpu_sec) = self.last_per_thread_cpu.get(tid) {
                delta_cpu_sec += (current_cpu_sec - previous_cpu_sec).max(0.0);
            }
        }

        self.last_per_thread_cpu = current_per_thread_cpu;
        delta_cpu_sec
    }
}

#[derive(Debug)]
struct CpuSample {
    instant: Instant,
    global_delta_cpu_sec: f64,
    per_resource_group_dag_delta_cpu_us: HashMap<String, u64>,
}

#[derive(Debug)]
struct CpuSampleWindow {
    window_size: Duration,
    samples: VecDeque<CpuSample>,
    global_sum_cpu_sec: f64,
    per_resource_group_dag_sum_cpu_us: HashMap<String, u64>,
}

impl CpuSampleWindow {
    fn new(window_size: Duration) -> Self {
        Self {
            window_size,
            samples: VecDeque::new(),
            global_sum_cpu_sec: 0.0,
            per_resource_group_dag_sum_cpu_us: HashMap::new(),
        }
    }

    fn append(&mut self, sample: CpuSample) {
        self.global_sum_cpu_sec += sample.global_delta_cpu_sec;
        for (resource_group, delta_cpu_us) in &sample.per_resource_group_dag_delta_cpu_us {
            let total_cpu_us = self
                .per_resource_group_dag_sum_cpu_us
                .entry(resource_group.clone())
                .or_insert(0);
            *total_cpu_us = total_cpu_us.saturating_add(*delta_cpu_us);
        }
        let now = sample.instant;
        self.samples.push_back(sample);
        self.evict_old(now);
    }

    fn evict_old(&mut self, now: Instant) {
        while let Some(sample) = self.samples.front() {
            if now.duration_since(sample.instant) <= self.window_size {
                break;
            }
            let sample = self.samples.pop_front().unwrap();
            self.global_sum_cpu_sec =
                (self.global_sum_cpu_sec - sample.global_delta_cpu_sec).max(0.0);
            for (resource_group, delta_cpu_us) in sample.per_resource_group_dag_delta_cpu_us {
                let mut should_remove = false;
                if let Some(total_cpu_us) = self
                    .per_resource_group_dag_sum_cpu_us
                    .get_mut(&resource_group)
                {
                    *total_cpu_us = total_cpu_us.saturating_sub(delta_cpu_us);
                    if *total_cpu_us == 0 {
                        should_remove = true;
                    }
                }
                if should_remove {
                    self.per_resource_group_dag_sum_cpu_us
                        .remove(&resource_group);
                }
            }
        }
    }

    fn update_window_size(&mut self, window_size: Duration, now: Instant) {
        if self.window_size == window_size {
            return;
        }
        self.window_size = window_size;
        self.evict_old(now);
    }
}

struct CpuUsageMonitor {
    thread_collector: ThreadCollector,
    window: CpuSampleWindow,
    manager: Arc<CpuThrottleManager>,
}

impl CpuUsageMonitor {
    fn new(manager: Arc<CpuThrottleManager>) -> Self {
        let window_size = manager.window_size();
        Self {
            thread_collector: ThreadCollector::new("unified-read-pool"),
            window: CpuSampleWindow::new(window_size),
            manager,
        }
    }

    fn tick(&mut self) {
        let start = Instant::now();
        self.window
            .update_window_size(self.manager.window_size(), start);
        let global_delta_cpu_sec = self.thread_collector.collect_delta_cpu_time();
        let per_resource_group_dag_delta_cpu_us =
            self.manager.take_per_resource_group_dag_cpu_deltas();

        self.window.append(CpuSample {
            instant: start,
            global_delta_cpu_sec,
            per_resource_group_dag_delta_cpu_us,
        });
        self.manager.log_monitor_tick_summary(
            global_delta_cpu_sec,
            self.window.global_sum_cpu_sec,
            self.window.window_size,
            &self
                .window
                .samples
                .back()
                .expect("cpu sample window must contain the latest sample")
                .per_resource_group_dag_delta_cpu_us,
        );

        let max_cpu_time_window_sec = (SysQuota::cpu_cores_quota().max(1.0)
            * self.manager.max_read_cpu_ratio()
            * self.window.window_size.as_secs_f64())
        .max(f64::EPSILON);
        let global_ratio = self.window.global_sum_cpu_sec / max_cpu_time_window_sec;
        let per_resource_group_dag_ratios = self
            .window
            .per_resource_group_dag_sum_cpu_us
            .iter()
            .map(|(resource_group, cpu_us)| {
                (
                    resource_group.clone(),
                    *cpu_us as f64 / (max_cpu_time_window_sec * 1_000_000.0),
                )
            })
            .collect();

        self.manager
            .update_usage(global_ratio, per_resource_group_dag_ratios);
        self.manager.adjust_refill_rates();
        self.manager.log_resource_group_quota_snapshots_if_needed();
        self.manager
            .observe_cpu_monitor_collect_duration(start.elapsed());
    }
}

async fn sleep_async(duration: Duration) {
    let _ = GLOBAL_TIMER_HANDLE
        .delay(std::time::Instant::now() + duration)
        .compat()
        .await;
}

pub fn start_cpu_throttle_monitor(bg_worker: &Worker, manager: Arc<CpuThrottleManager>) {
    if !manager.try_start_cpu_monitor() {
        return;
    }

    let mut monitor = CpuUsageMonitor::new(manager.clone());
    info!(
        "[CPU throttle] start cpu throttle monitor";
        "stats_interval" => ?manager.stats_interval(),
        "window_size" => ?manager.window_size(),
        "thread_prefix" => "unified-read-pool",
    );
    bg_worker.spawn_async_task(async move {
        let _monitor_guard = CpuThrottleMonitorGuard {
            manager: manager.clone(),
        };
        loop {
            if !manager.is_enabled() {
                manager.reset_cpu_monitor_state();
                info!(
                    "[CPU throttle] stop cpu throttle monitor";
                    "thread_prefix" => "unified-read-pool",
                );
                return;
            }
            monitor.tick();
            sleep_async(manager.stats_interval()).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_collector_returns_zero_before_threads_exist() {
        let mut collector = ThreadCollector::new("unified-read-pool");

        assert_eq!(collector.collect_delta_from_snapshot(HashMap::new()), 0.0);
        assert!(collector.last_per_thread_cpu.is_empty());
    }

    #[test]
    fn test_thread_collector_picks_up_scale_out_threads_on_next_tick() {
        let mut collector = ThreadCollector::new("unified-read-pool");

        assert_eq!(
            collector.collect_delta_from_snapshot(HashMap::from([(1 as Pid, 1.0)])),
            0.0
        );
        assert_eq!(
            collector
                .collect_delta_from_snapshot(HashMap::from([(1 as Pid, 2.0), (2 as Pid, 5.0),])),
            1.0
        );
        assert_eq!(
            collector
                .collect_delta_from_snapshot(HashMap::from([(1 as Pid, 3.0), (2 as Pid, 7.0),])),
            3.0
        );
    }

    #[test]
    fn test_thread_collector_clamps_scale_in_to_non_negative_delta() {
        let mut collector = ThreadCollector::new("unified-read-pool");

        assert_eq!(
            collector
                .collect_delta_from_snapshot(HashMap::from([(1 as Pid, 2.0), (2 as Pid, 4.0),])),
            0.0
        );
        assert_eq!(
            collector.collect_delta_from_snapshot(HashMap::from([(1 as Pid, 3.5)])),
            1.5
        );
    }

    #[test]
    fn test_cpu_sample_window_evicts_old_samples() {
        let mut window = CpuSampleWindow::new(Duration::from_secs(10));
        let start = Instant::now();

        window.append(CpuSample {
            instant: start,
            global_delta_cpu_sec: 1.0,
            per_resource_group_dag_delta_cpu_us: HashMap::from([(String::from("rg1"), 100)]),
        });
        window.append(CpuSample {
            instant: start + Duration::from_secs(5),
            global_delta_cpu_sec: 2.0,
            per_resource_group_dag_delta_cpu_us: HashMap::from([(String::from("rg1"), 200)]),
        });
        window.append(CpuSample {
            instant: start + Duration::from_secs(11),
            global_delta_cpu_sec: 3.0,
            per_resource_group_dag_delta_cpu_us: HashMap::from([(String::from("rg2"), 300)]),
        });

        assert_eq!(window.samples.len(), 2);
        assert_eq!(window.global_sum_cpu_sec, 5.0);
        assert_eq!(
            window.per_resource_group_dag_sum_cpu_us.get("rg1").copied(),
            Some(200)
        );
        assert_eq!(
            window.per_resource_group_dag_sum_cpu_us.get("rg2").copied(),
            Some(300)
        );
    }
}
