// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use libc::{getpid, pid_t};

use server::load_statistics::ThreadLoad;
use util::metrics::{get_thread_ids, Stat};

pub struct ThreadLoadStatistics {
    pid: pid_t,
    tids: Vec<pid_t>,
    slots: usize,
    cur_pos: usize,
    cpu_usages: Vec<f64>,
    instants: Vec<Instant>,
    thread_load: Arc<ThreadLoad>,
}

impl ThreadLoadStatistics {
    /// Create a thread load statistics for all threads with `prefix`. `ThreadLoad` is stored into
    /// `thread_load`. At most `slots` old records will be kept, to make the curve more smooth.
    ///
    /// Note: call this after the target threads are initialized, otherwise it can't catch them.
    pub fn new(slots: usize, prefix: &str, thread_load: Arc<ThreadLoad>) -> Self {
        let pid: pid_t = unsafe { getpid() };
        let mut tids = vec![];
        let mut cpu_total = 0f64;
        for tid in get_thread_ids(pid).unwrap() {
            if let Ok(stat) = Stat::collect(pid, tid) {
                if !stat.name().starts_with(prefix) {
                    continue;
                }
                cpu_total += stat.cpu_total();
                tids.push(tid);
            }
        }
        ThreadLoadStatistics {
            pid,
            tids,
            slots,
            cur_pos: 0,
            cpu_usages: vec![cpu_total; slots],
            instants: vec![Instant::now(); slots],
            thread_load,
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
        self.cpu_usages[self.cur_pos] = 0f64;
        for tid in &self.tids {
            // TODO: if monitored threads exited and restarted then, we should update `self.tids`.
            if let Ok(stat) = Stat::collect(self.pid, *tid) {
                self.cpu_usages[self.cur_pos] += stat.cpu_total();
            }
        }
        let current_instant = self.instants[self.cur_pos];
        let current_cpu_usage = self.cpu_usages[self.cur_pos];

        let next_pos = (self.cur_pos + 1) % self.slots;
        let earlist_instant = self.instants[next_pos];
        let earlist_cpu_usage = self.cpu_usages[next_pos];
        self.cur_pos = next_pos;

        let millis = (current_instant - earlist_instant).as_millis() as usize;
        if millis > 0 {
            let cpu_usage = calc_cpu_load(millis, earlist_cpu_usage, current_cpu_usage);
            self.thread_load.load.store(cpu_usage, Ordering::Release);
            self.thread_load.term.fetch_add(1, Ordering::Release);
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
    use std::thread;

    use super::*;

    #[test]
    fn test_thread_load_statistic() {
        // OS thread name is truncated to 16 bytes, including the last '\0'.
        let thread_name = thread::current().name().unwrap()[0..15].to_owned();

        let load = Arc::new(ThreadLoad::with_threshold(80));
        let mut stats = ThreadLoadStatistics::new(2, &thread_name, Arc::clone(&load));
        let start = Instant::now();
        loop {
            if (Instant::now() - start).as_millis() > 100 {
                break;
            }
        }
        stats.record(Instant::now());
        match load.load() {
            80...100 => {}
            e => panic!("the load must be heavy than 80, but got {}", e),
        }
    }
}
