// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use libc::{getpid, pid_t};

use crate::server::load_statistics::ThreadLoad;
use tikv_util::metrics::{cpu_total, get_thread_ids};

use procinfo::pid;

/// A Linux-specific `ThreadLoadStatistics`. It collects threads load metrics.
pub struct ThreadLoadStatistics {
    pid: pid_t,
    tids: Vec<pid_t>,
    prefix: String,
    slots: usize,
    cur_pos: usize,
    cpu_usages: Vec<f64>,
    instants: Vec<Instant>,
    thread_load: Arc<ThreadLoad>,
    target: usize,
}

impl ThreadLoadStatistics {
    /// Create a thread load statistics for all threads with `prefix`. `ThreadLoad` is stored into
    /// `thread_load`. At most `slots` old records will be kept, to make the curve more smooth.
    ///
    /// Note: call this after the target threads are initialized, otherwise it can't catch them.
    pub fn new(slots: usize, prefix: &str, thread_load: Arc<ThreadLoad>) -> Self {
        let pid: pid_t = unsafe { getpid() };
        let (tids, cpu_total_count) = poll_task(pid, prefix);
        ThreadLoadStatistics {
            pid,
            tids,
            prefix: prefix.to_string(),
            slots,
            cur_pos: 0,
            cpu_usages: vec![cpu_total_count; slots],
            instants: vec![Instant::now(); slots],
            thread_load,
            target: 0,
        }
    }

    /// Designate target thread count of this collector.
    pub fn set_thread_target(&mut self, target: usize) {
        self.target = target;
    }

    /// For every threads with the name prefix given in `ThreadLoadStatistics::new`,
    /// gather cpu usage from `/proc/<pid>/task/<tid>` and store it in `thread_load`
    /// passed in `ThreadLoadStatistics::new`.
    ///
    /// Some old usages and instants (at most `slots`) will be kept internal to make
    /// the usage curve more smooth.
    pub fn record(&mut self, instant: Instant) {
        self.instants[self.cur_pos] = instant;
        self.cpu_usages[self.cur_pos] = 0f64;
        // workaround for tokio's threadpool with unstable worker.
        let need_poll_task = self.tids.len() < self.target;
        if !need_poll_task {
            for tid in &self.tids {
                if let Ok(stat) = pid::stat_task(self.pid, *tid) {
                    self.cpu_usages[self.cur_pos] += cpu_total(&stat);
                    // } else {
                    // TODO: if monitored threads exited and restarted then, we should update `self.tids`.
                    // pending this for it will arouse unstable integration tests.
                    // need_poll_task = true;
                    // break;
                }
            }
        }
        if need_poll_task {
            let (tids, cpu_usage) = poll_task(self.pid, self.prefix.as_str());
            std::mem::replace(&mut self.tids, tids);
            self.cpu_usages[self.cur_pos] = cpu_usage;
        }
        let current_instant = self.instants[self.cur_pos];
        let current_cpu_usage = self.cpu_usages[self.cur_pos];

        let next_pos = (self.cur_pos + 1) % self.slots;
        let earlist_instant = self.instants[next_pos];
        let earlist_cpu_usage = self.cpu_usages[next_pos];
        self.cur_pos = next_pos;

        let millis = (current_instant - earlist_instant).as_millis() as usize;
        if millis > 0 {
            let mut cpu_usage = calc_cpu_load(millis, earlist_cpu_usage, current_cpu_usage);
            if cpu_usage > self.tids.len() * 100 {
                cpu_usage = self.tids.len() * 100;
            }
            self.thread_load.load.store(cpu_usage, Ordering::Relaxed);
            self.thread_load.term.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[inline]
fn calc_cpu_load(elapsed_millis: usize, start_usage: f64, end_usage: f64) -> usize {
    // Multiply by 1000 for millis, and multiply 100 for percentage.
    let cpu_usage = (end_usage - start_usage) * 1000f64 * 100f64;
    cpu_usage as usize / elapsed_millis
}

#[inline]
fn poll_task(pid: pid_t, prefix: &str) -> (Vec<pid_t>, f64) {
    let tids = get_thread_ids(pid).unwrap();
    let mut cpu_total_count = 0f64;
    for tid in &tids {
        if let Ok(stat) = pid::stat_task(pid, *tid) {
            if !stat.command.starts_with(prefix) {
                continue;
            }
            cpu_total_count += cpu_total(&stat);
        }
    }
    (tids, cpu_total_count)
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    // FIXME(#4364) Flaky test - on CI gets 0 cpu usages, but passes locally.
    #[ignore]
    fn test_thread_load_statistic() {
        // OS thread name is truncated to 16 bytes, including the last '\0'.
        let t = thread::current();
        let thread_name = t.name().unwrap();
        let end = ::std::cmp::min(thread_name.len(), 15);
        let thread_name = thread_name[..end].to_owned();

        let load = Arc::new(ThreadLoad::with_threshold(80));
        let mut stats = ThreadLoadStatistics::new(2, &thread_name, Arc::clone(&load));
        let start = Instant::now();
        loop {
            if (Instant::now() - start).as_millis() > 200 {
                break;
            }
        }
        stats.record(Instant::now());
        let cpu_usage = load.load();
        assert!(cpu_usage < 100); // There is only 1 thread.
        if cpu_usage < 80 {
            panic!("the load must be heavy than 80, but got {}", cpu_usage);
        }
    }
}
