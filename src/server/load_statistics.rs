use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use libc::{getpid, pid_t};

use util::metrics::{get_thread_ids, Stat};

pub struct ThreadLoad {
    term: AtomicUsize,
    load: AtomicUsize,
    threshold: usize,
}

impl ThreadLoad {
    pub fn with_threshold(threshold: usize) -> Self {
        ThreadLoad {
            term: AtomicUsize::new(0),
            load: AtomicUsize::new(0),
            threshold,
        }
    }

    #[allow(dead_code)]
    pub fn in_heavy_load(&self) -> bool {
        self.load.load(Ordering::Acquire) > self.threshold
    }

    /// Incease when every time updating `load`.
    #[allow(dead_code)]
    pub fn term(&self) -> usize {
        self.term.load(Ordering::Acquire)
    }

    /// For example, 200 means the threads eat 200% CPU.
    #[allow(dead_code)]
    pub fn load(&self) -> usize {
        self.load.load(Ordering::Acquire)
    }
}

#[cfg(target_os = "linux")]
pub struct ThreadLoadStatistics {
    pid: pid_t,
    tids: Vec<pid_t>,
    slots: usize,
    cur_pos: usize,
    cpu_usages: Vec<f64>,
    instants: Vec<Instant>,
    thread_load: Arc<ThreadLoad>,
}

#[cfg(target_os = "linux")]
impl ThreadLoadStatistics {
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

    pub fn record(&mut self, instant: Instant) {
        self.instants[self.cur_pos] = instant;
        self.cpu_usages[self.cur_pos] = 0f64;
        for tid in &self.tids {
            let stat = Stat::collect(self.pid, *tid).unwrap();
            self.cpu_usages[self.cur_pos] += stat.cpu_total();
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
fn calc_cpu_load(millis: usize, start_usage: f64, end_usage: f64) -> usize {
    let cpu_usage = (end_usage - start_usage) * 1000f64 * 100f64;
    cpu_usage as usize / millis
}

#[cfg(test)]
mod tests {
    use libc::{syscall, SYS_gettid};

    use super::*;

    impl ThreadLoadStatistics {
        fn for_test(slots: usize, thread_load: Arc<ThreadLoad>) -> Self {
            let pid: pid_t = unsafe { getpid() };
            let tid: pid_t = unsafe { syscall(SYS_gettid) as pid_t };
            let stat = Stat::collect(pid, tid).unwrap();

            ThreadLoadStatistics {
                pid,
                tids: vec![tid],
                slots,
                cur_pos: 0,
                cpu_usages: vec![stat.cpu_total(); slots],
                instants: vec![Instant::now(); slots],
                thread_load,
            }
        }
    }

    #[test]
    fn test_thread_load_statistic() {
        let load = Arc::new(ThreadLoad::with_threshold(80));
        let mut stats = ThreadLoadStatistics::for_test(2, Arc::clone(&load));
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
