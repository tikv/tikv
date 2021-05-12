// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{ReqCpuConfig, RequestCpuRecords, RequestCpuReporter, RequestTags};

use std::cell::Cell;
use std::fs::read_dir;
use std::marker::PhantomData;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use collections::{HashMap, HashSet};
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::pid_t;
use procinfo::pid;

thread_local! {
    static CURRENT_REQ: LocalReqTags = {
        let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };

        let shared_ptr = SharedReqTagsPtr::default();
        CHANNEL.0.send(RegisterMsg {
            thread_id,
            thread_name: std::thread::current().name().unwrap_or("<unknown>").to_owned(),
            shared_ptr: shared_ptr.clone(),
        }).ok();

        LocalReqTags {
            is_set: Cell::new(false),
            shared_ptr,
        }
    };
}

#[derive(Default, Clone)]
struct SharedReqTagsPtr {
    req_tags: Arc<AtomicPtr<RequestTags>>,
}

struct LocalReqTags {
    is_set: Cell<bool>,
    shared_ptr: SharedReqTagsPtr,
}

impl SharedReqTagsPtr {
    fn take(&self) -> Option<Arc<RequestTags>> {
        let prev_ptr = self.req_tags.swap(std::ptr::null_mut(), Acquire);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }

    fn swap(&self, value: Arc<RequestTags>) -> Option<Arc<RequestTags>> {
        let tags_arc_ptr = Arc::into_raw(value);
        let prev_ptr = self.req_tags.swap(tags_arc_ptr as _, AcqRel);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }
}

impl RequestTags {
    pub fn attach(self: &Arc<Self>) -> Guard {
        CURRENT_REQ.with(|s| {
            if s.is_set.get() {
                panic!("Nested attachment is not allowed.")
            }

            let prev = s.shared_ptr.swap(self.clone());
            assert!(prev.is_none());
            s.is_set.set(true);
        });

        Guard::default()
    }
}

#[derive(Default)]
pub struct Guard {
    // A trick to impl !Send, !Sync
    _p: PhantomData<*const ()>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        CURRENT_REQ.with(|s| {
            while s.shared_ptr.take().is_none() {}
            s.is_set.set(false);
        });
    }
}

struct RegisterMsg {
    thread_id: pid_t,
    thread_name: String,
    shared_ptr: SharedReqTagsPtr,
}

lazy_static! {
    static ref PID: pid_t = unsafe { libc::getpid() };
    static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    static ref CHANNEL: (Sender<RegisterMsg>, Receiver<RegisterMsg>) = unbounded();
}

struct ReqCpuCollector {
    config: ReqCpuConfig,

    thread_stats: HashMap<pid_t, ThreadStat>,
    current_window_records: RequestCpuRecords,

    last_collect_instant: Instant,
    last_gc_instant: Instant,

    reporter: Arc<Mutex<RequestCpuReporter>>,
}

struct ThreadStat {
    prev_stat: pid::Stat,
    thread_name: String,
    shared_ptr: SharedReqTagsPtr,
}

impl ReqCpuCollector {
    pub fn new(reporter: Arc<Mutex<RequestCpuReporter>>, config: ReqCpuConfig) -> Self {
        let now = Instant::now();

        Self {
            config,
            reporter,

            last_collect_instant: now,
            last_gc_instant: now,

            thread_stats: HashMap::default(),
            current_window_records: RequestCpuRecords::default(),
        }
    }

    pub fn handle_registration(&mut self) {
        while let Ok(RegisterMsg {
            thread_id,
            thread_name,
            shared_ptr,
        }) = CHANNEL.1.try_recv()
        {
            if let Ok(stat) = procinfo::pid::stat_task(*PID, thread_id) {
                self.thread_stats.insert(
                    thread_id,
                    ThreadStat {
                        prev_stat: stat,
                        thread_name,
                        shared_ptr,
                    },
                );
            }
        }
    }

    pub fn record(&mut self) {
        for (tid, thread_stat) in &mut self.thread_stats {
            if let Some(req_tags) = thread_stat.shared_ptr.take() {
                if !self.current_window_records.records.contains_key(&req_tags) {
                    self.current_window_records
                        .records
                        .insert(req_tags.clone(), 0);
                }
                let ms = self
                    .current_window_records
                    .records
                    .get_mut(&req_tags)
                    .unwrap();
                let prev = thread_stat.shared_ptr.swap(req_tags);
                assert!(prev.is_none());

                if let Ok(stat) = procinfo::pid::stat_task(*PID, *tid) {
                    let prev_cpu_clock = (thread_stat.prev_stat.utime as u64)
                        .wrapping_add(thread_stat.prev_stat.stime as u64);
                    let current_cpu_clock = (stat.utime as u64).wrapping_add(stat.stime as u64);
                    thread_stat.prev_stat = stat;

                    let delta_ms =
                        current_cpu_clock.wrapping_sub(prev_cpu_clock) * 1_000 / (*CLK_TCK as u64);
                    *ms += delta_ms;
                }
            }
        }
    }

    pub fn may_gc(&mut self) -> bool {
        const THREAD_STAT_LEN_THRESHOLD: usize = 1 << 10;
        const RECORD_LEN_THRESHOLD: usize = 1 << 20;

        let duration = self.last_gc_instant.elapsed().as_millis();
        let need_gc = duration > self.config.gc_interval_ms as _;

        if need_gc {
            if let Some(thread_ids) = Self::get_thread_ids() {
                self.thread_stats.retain(|k, v| {
                    let retain = thread_ids.contains(k);

                    if !retain {
                        assert!(v.shared_ptr.take().is_none());
                    }

                    retain
                });
            }

            if self.thread_stats.capacity() > THREAD_STAT_LEN_THRESHOLD
                && self.thread_stats.len() < THREAD_STAT_LEN_THRESHOLD / 2
            {
                self.thread_stats.shrink_to(THREAD_STAT_LEN_THRESHOLD);
            }

            if self.current_window_records.records.capacity() > RECORD_LEN_THRESHOLD
                && self.current_window_records.records.len() < RECORD_LEN_THRESHOLD / 2
            {
                self.current_window_records
                    .records
                    .shrink_to(RECORD_LEN_THRESHOLD);
            }
        }

        need_gc
    }

    pub fn may_advance_window(&mut self) -> bool {
        let duration = self.last_collect_instant.elapsed().as_millis();
        let need_advance = duration >= self.config.window_size_ms as _;

        if need_advance {
            let mut records = std::mem::take(&mut self.current_window_records);
            records.duration_ms = duration as _;

            if !records.records.is_empty() {
                let mut r = self.reporter.lock().unwrap();
                r.push(records);
            }

            self.last_collect_instant = Instant::now();
        }

        need_advance
    }

    fn get_thread_ids() -> Option<HashSet<pid_t>> {
        read_dir(format!("/proc/{}/task", *PID)).ok().map(|dir| {
            dir.filter_map(|task| {
                let file_name = task.ok().map(|t| t.file_name());
                file_name.and_then(|f| f.to_str().and_then(|tid| tid.parse().ok()))
            })
            .collect::<HashSet<pid_t>>()
        })
    }
}

pub fn build(config: ReqCpuConfig) -> Arc<Mutex<RequestCpuReporter>> {
    let reporter = Arc::new(Mutex::new(RequestCpuReporter::with_capacity(
        config.buffer_size,
    )));

    let r = reporter.clone();
    std::thread::Builder::new()
        .name("req-cpu-collector".to_owned())
        .spawn(move || {
            let mut c = ReqCpuCollector::new(r, config);

            loop {
                c.record();
                c.may_advance_window();
                c.may_gc();
                c.handle_registration();

                std::thread::sleep(Duration::from_millis(c.config.record_interval_ms));
            }
        })
        .expect("Create req-cpu-collector thread failed.");

    reporter
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn one_context_per_thread() {
        let r = build(ReqCpuConfig::default());

        let num = Arc::new(AtomicU64::new(0));

        [100000, 500000, 2000000, 1000000]
            .iter()
            .map(|i| {
                let num = num.clone();
                std::thread::spawn(move || {
                    let tid = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t } as u64;
                    let req_tags = Arc::new(RequestTags {
                        store_id: tid,
                        region_id: tid,
                        peer_id: tid,
                        request_id: tid,
                        extra_attachment: vec![],
                    });

                    let _g = req_tags.attach();

                    for _ in 0..*i {
                        let m: u64 = rand::random();
                        let n: u64 = rand::random();
                        let m = m ^ n;
                        let n = m.wrapping_mul(n);
                        let m = m.wrapping_add(n);
                        let n = m & n;
                        let m = m | n;
                        let n = m.wrapping_sub(n);
                        num.store(n, Relaxed);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|handle| handle.join().unwrap());

        let r = r.lock().unwrap();
        assert!(r.records.iter().next().is_some());
    }
}
