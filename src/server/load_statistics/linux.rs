// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use collections::HashMap;
use tikv_util::sys::thread::{self, Pid};

use crate::server::load_statistics::ThreadLoadPool;

/// A Linux-specific `ThreadLoadStatistics`. It collects threads load metrics.
pub struct ThreadLoadStatistics {
    pid: Pid,
    tids: Vec<Pid>,
    slots: usize,
    cur_pos: usize,
    cpu_usages: Vec<HashMap<Pid, f64>>,
    instants: Vec<Instant>,
    thread_loads: Arc<ThreadLoadPool>,
}

impl ThreadLoadStatistics {
    /// Create a thread load statistics for all threads with `prefix`. `ThreadLoad` is stored into
    /// `thread_loads` for each thread. At most `slots` old records will be kept, to make the curve
    /// more smooth.
    ///
    /// Note: call this after the target threads are initialized, otherwise it can't catch them.
    pub fn new(slots: usize, prefix: &str, thread_loads: Arc<ThreadLoadPool>) -> Self {
        let pid = thread::process_id();
        let mut tids = vec![];
        let mut cpu_total_count = HashMap::default();
        let all_tids: Vec<_> = thread::thread_ids(pid).unwrap();
        let mut loads = thread_loads.stats.lock();
        for tid in all_tids {
            if let Ok(stat) = thread::full_thread_stat(pid, tid) {
                if !stat.command.starts_with(prefix) {
                    continue;
                }
                loads.entry(tid).or_default();
                cpu_total_count.insert(tid, thread::linux::cpu_total(&stat));
                tids.push(tid);
            }
        }
        drop(loads);
        ThreadLoadStatistics {
            pid,
            tids,
            slots,
            cur_pos: 0,
            cpu_usages: vec![cpu_total_count; slots],
            instants: vec![Instant::now(); slots],
            thread_loads,
        }
    }

    /// For every threads with the name prefix given in `ThreadLoadStatistics::new`,
    /// gather cpu usage from `/proc/<pid>/task/<tid>` and store it in `thread_load`
    /// passed in `ThreadLoadStatistics::new`.
    ///
    /// Some old usages and instants (at most `slots`) will be kept internal to make
    /// the usage curve more smooth.
    pub fn record(&mut self, instant: Instant) {
        self.instants[self.cur_pos] = instant;
        self.cpu_usages[self.cur_pos].clear();
        for tid in &self.tids {
            // TODO: if monitored threads exited and restarted then, we should update `self.tids`.
            if let Ok(stat) = thread::full_thread_stat(self.pid, *tid) {
                let total = thread::linux::cpu_total(&stat);
                self.cpu_usages[self.cur_pos].insert(*tid, total);
            }
        }
        let current_instant = self.instants[self.cur_pos];
        let current_cpu_usages = &self.cpu_usages[self.cur_pos];

        let next_pos = (self.cur_pos + 1) % self.slots;
        let earlist_instant = self.instants[next_pos];
        let earlist_cpu_usages = &self.cpu_usages[next_pos];
        self.cur_pos = next_pos;

        let millis = (current_instant - earlist_instant).as_millis() as usize;
        let mut total_usage = 0;
        if millis > 0 {
            let mut loads = self.thread_loads.stats.lock();
            for (tid, load) in loads.iter_mut() {
                let cpu_usage = if let Some(current_cpu_usage) = current_cpu_usages.get(tid) {
                    let earlist_cpu_usage = earlist_cpu_usages.get(tid).cloned().unwrap_or(0.);
                    cmp::min(
                        100,
                        calc_cpu_load(millis, earlist_cpu_usage, *current_cpu_usage),
                    )
                } else {
                    0
                };
                total_usage += cpu_usage;
                load.store(cpu_usage, Ordering::Relaxed);
            }
            self.thread_loads
                .total_load
                .store(total_usage, Ordering::Relaxed);
        }
    }
}

#[inline]
fn calc_cpu_load(elapsed_millis: usize, start_usage: f64, end_usage: f64) -> usize {
    // Multiply by 1000 for millis, and multiply 100 for percentage.
    let cpu_usage = (end_usage - start_usage) * 1000f64 * 100f64;
    cpu_usage as usize / elapsed_millis
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;

    #[test]
    fn test_thread_load_statistic() {
        const THREAD_NAME: &str = "linux_thd_ld";
        let loads = Arc::new(ThreadLoadPool::with_threshold(10));
        let l = loads.clone();
        thread::Builder::new()
            .name(THREAD_NAME.to_string())
            .spawn_wrapper(move || {
                let mut stats = ThreadLoadStatistics::new(2, THREAD_NAME, Arc::clone(&l));
                let start = Instant::now();
                loop {
                    if (Instant::now() - start).as_millis() > 200 {
                        break;
                    }
                }
                stats.record(Instant::now());
                let cpu_usage = l.total_load();
                assert!(cpu_usage <= 100);
                // Busy loop should be recorded as heavy load.
                assert!(l.current_thread_in_heavy_load(), "{}", cpu_usage);

                thread::sleep(Duration::from_millis(200));
                stats.record(Instant::now());
                let cpu_usage = l.total_load();
                assert!(cpu_usage <= 10);
                // Idle thread should be not be marked in heavy load.
                assert!(!l.current_thread_in_heavy_load());
            })
            .unwrap()
            .join()
            .unwrap();

        let stats = loads.stats.lock();
        // There is only 1 thread.
        assert_eq!(stats.len(), 1);
    }
}
