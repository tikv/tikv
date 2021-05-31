// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{Collector, CollectorId};
use crate::{RecorderConfig, RequestCpuRecords, RequestTag};

use std::cell::Cell;
use std::fs::read_dir;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicPtr, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use collections::{HashMap, HashSet};
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::pid_t;
use procinfo::pid;
use procinfo::pid::Stat;

thread_local! {
    static CURRENT_REQ: LocalReqTag = {
        let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };

        let shared_ptr = SharedReqTagPtr::default();
        THREAD_REGISTRATION_CHANNEL.0.send(ThreadRegistrationMsg {
            thread_id,
            shared_ptr: shared_ptr.clone(),
        }).ok();

        LocalReqTag {
            is_set: Cell::new(false),
            shared_ptr,
        }
    };
}

#[derive(Default, Clone)]
struct SharedReqTagPtr {
    req_tag: Arc<AtomicPtr<RequestTag>>,
}

struct LocalReqTag {
    is_set: Cell<bool>,
    shared_ptr: SharedReqTagPtr,
}

impl SharedReqTagPtr {
    fn take(&self) -> Option<Arc<RequestTag>> {
        let prev_ptr = self.req_tag.swap(std::ptr::null_mut(), Acquire);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }

    fn swap(&self, value: Arc<RequestTag>) -> Option<Arc<RequestTag>> {
        let tag_arc_ptr = Arc::into_raw(value);
        let prev_ptr = self.req_tag.swap(tag_arc_ptr as _, AcqRel);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }
}

impl RequestTag {
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

struct ThreadRegistrationMsg {
    thread_id: pid_t,
    shared_ptr: SharedReqTagPtr,
}

enum CollectorRegistrationMsg {
    Register {
        collector: Box<dyn Collector>,
        id: CollectorId,
    },
    Unregister {
        id: CollectorId,
    },
}

lazy_static! {
    static ref PID: pid_t = unsafe { libc::getpid() };
    static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    static ref THREAD_REGISTRATION_CHANNEL: (
        Sender<ThreadRegistrationMsg>,
        Receiver<ThreadRegistrationMsg>
    ) = unbounded();
    static ref COLLECTOR_REGISTRATION_CHANNEL: (
        Sender<CollectorRegistrationMsg>,
        Receiver<CollectorRegistrationMsg>
    ) = unbounded();
}

struct ReqCpuRecorder {
    config: RecorderConfig,

    thread_stats: HashMap<pid_t, ThreadStat>,
    current_window_records: RequestCpuRecords,

    last_collect_instant: Instant,
    last_gc_instant: Instant,

    collectors: HashMap<CollectorId, Box<dyn Collector>>,
}

struct ThreadStat {
    shared_ptr: SharedReqTagPtr,
    prev_tag: Option<Arc<RequestTag>>,
    prev_stat: pid::Stat,
}

impl ReqCpuRecorder {
    pub fn new(config: RecorderConfig) -> Self {
        let now = Instant::now();

        Self {
            config,

            last_collect_instant: now,
            last_gc_instant: now,

            thread_stats: HashMap::default(),
            current_window_records: RequestCpuRecords::default(),

            collectors: HashMap::default(),
        }
    }

    pub fn handle_collector_registration(&mut self) {
        let mut should_reset = false;
        loop {
            while let Ok(msg) = COLLECTOR_REGISTRATION_CHANNEL.1.try_recv() {
                self.handle_collector_registration_msg(msg);
            }

            if self.collectors.is_empty() {
                // Block the record thread until a new collector coming.
                if let Ok(msg) = COLLECTOR_REGISTRATION_CHANNEL.1.recv() {
                    self.handle_collector_registration_msg(msg);
                }

                // May wait a long time. So drop out-dated state by resetting.
                should_reset = true;
            } else {
                break;
            }
        }

        if should_reset {
            self.reset();
        }
    }

    pub fn handle_thread_registration(&mut self) {
        while let Ok(ThreadRegistrationMsg {
            thread_id,
            shared_ptr,
        }) = THREAD_REGISTRATION_CHANNEL.1.try_recv()
        {
            self.thread_stats.insert(
                thread_id,
                ThreadStat {
                    prev_stat: Stat::default(),
                    shared_ptr,
                    prev_tag: None,
                },
            );
        }
    }

    pub fn record(&mut self) {
        for (tid, thread_stat) in &mut self.thread_stats {
            let cur_tag = thread_stat.shared_ptr.take().map(|req_tag| {
                let tag = req_tag.clone();
                // Put it back as quickly as possible.
                assert!(thread_stat.shared_ptr.swap(req_tag).is_none());
                tag
            });

            let prev_tag = thread_stat.prev_tag.take();

            if cur_tag.is_some() || prev_tag.is_some() {
                STAT_TASK_COUNT.inc();

                // If existing current tag, we need to store the beginning stat.
                // If existing previous tag, we need to get the end stat to calculate delta.
                if let Ok(stat) = procinfo::pid::stat_task(*PID, *tid) {
                    // Accumulate the cpu time of the previous tag.
                    if let Some(prev_tag) = prev_tag {
                        let prev_cpu_ticks = (thread_stat.prev_stat.utime as u64)
                            .wrapping_add(thread_stat.prev_stat.stime as u64);
                        let current_cpu_ticks = (stat.utime as u64).wrapping_add(stat.stime as u64);
                        let delta_ms = current_cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000
                            / (*CLK_TCK as u64);

                        *self
                            .current_window_records
                            .records
                            .entry(prev_tag)
                            .or_insert(0) += delta_ms;
                    }

                    // Store the beginning stat for the current tag.
                    if cur_tag.is_some() {
                        thread_stat.prev_tag = cur_tag;
                        thread_stat.prev_stat = stat;
                    }
                }
            }
        }
    }

    pub fn may_gc(&mut self) -> bool {
        const THREAD_STAT_LEN_THRESHOLD: usize = 500;
        const RECORD_LEN_THRESHOLD: usize = 20_000;

        let duration = self.last_gc_instant.elapsed().as_millis();
        let need_gc = duration > self.config.gc_interval_ms as _;

        if need_gc {
            if let Some(thread_ids) = Self::get_thread_ids() {
                self.thread_stats.retain(|k, v| {
                    let retain = thread_ids.contains(k);
                    assert!(retain || v.shared_ptr.take().is_none());
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
        let need_advance = duration >= self.config.collect_interval_ms as _;

        if need_advance {
            let mut records = std::mem::take(&mut self.current_window_records);
            records.duration_ms = duration as _;

            if !records.records.is_empty() {
                let records = Arc::new(records);
                for collector in self.collectors.values() {
                    collector.collect(records.clone());
                }
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

    fn handle_collector_registration_msg(&mut self, msg: CollectorRegistrationMsg) {
        match msg {
            CollectorRegistrationMsg::Register { id, collector } => {
                self.collectors.insert(id, collector);
            }
            CollectorRegistrationMsg::Unregister { id } => {
                self.collectors.remove(&id);
            }
        }
    }

    fn reset(&mut self) {
        let now = Instant::now();
        self.current_window_records = RequestCpuRecords::default();
        for v in self.thread_stats.values_mut() {
            v.prev_tag = None;
        }
        self.last_collect_instant = now;
        self.last_gc_instant = now;
    }
}

pub fn init_recorder() {
    lazy_static! {
        static ref PHANTOM: () = {
            std::thread::Builder::new()
                .name("req-cpu-recorder".to_owned())
                .spawn(move || {
                    let mut recorder = ReqCpuRecorder::new(RecorderConfig::default());

                    loop {
                        recorder.handle_collector_registration();
                        recorder.handle_thread_registration();
                        recorder.record();
                        recorder.may_advance_window();
                        recorder.may_gc();

                        std::thread::sleep(Duration::from_micros(
                            (recorder.config.record_interval_ms * 1_000.0) as _,
                        ));
                    }
                })
                .expect("Failed to create recorder thread");
        };
    }
    *PHANTOM
}

pub struct CollectorHandle {
    id: CollectorId,
}
impl Drop for CollectorHandle {
    fn drop(&mut self) {
        COLLECTOR_REGISTRATION_CHANNEL
            .0
            .send(CollectorRegistrationMsg::Unregister { id: self.id })
            .ok();
    }
}

pub fn register_collector(collector: Box<dyn Collector>) -> CollectorHandle {
    lazy_static! {
        static ref NEXT_COLLECTOR_ID: Arc<AtomicU64> = Arc::new(AtomicU64::new(1));
    }

    let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    COLLECTOR_REGISTRATION_CHANNEL
        .0
        .send(CollectorRegistrationMsg::Register { collector, id })
        .ok();
    CollectorHandle { id }
}

lazy_static! {
    static ref STAT_TASK_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
        "tikv_req_cpu_stat_task_count",
        "Counter of stat_task call"
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Mutex;

    #[derive(Default, Clone)]
    struct DummyCollector {
        records: Arc<Mutex<Vec<Arc<RequestCpuRecords>>>>,
    }

    impl Collector for DummyCollector {
        fn collect(&self, records: Arc<RequestCpuRecords>) {
            let mut r = self.records.lock().unwrap();
            r.push(records);
        }
    }

    fn heavy_job() -> u64 {
        let m: u64 = rand::random();
        let n: u64 = rand::random();
        let m = m ^ n;
        let n = m.wrapping_mul(n);
        let m = m.wrapping_add(n);
        let n = m & n;
        let m = m | n;
        m.wrapping_sub(n)
    }

    #[test]
    fn one_context_per_thread() {
        init_recorder();

        let collector = Box::new(DummyCollector::default());
        let _handle = register_collector(collector.clone());

        let num = Arc::new(AtomicU64::new(0));
        [100000, 500000, 2000000, 1000000]
            .iter()
            .map(|i| {
                let num = num.clone();
                std::thread::spawn(move || {
                    let tid = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t } as u64;
                    let req_tag = Arc::new(RequestTag {
                        store_id: tid,
                        region_id: tid,
                        peer_id: tid,
                        extra_attachment: vec![],
                    });

                    let _g = req_tag.attach();

                    for _ in 0..*i {
                        num.store(heavy_job(), Relaxed);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|handle| handle.join().unwrap());

        let r = collector.records.lock().unwrap();
        assert!(!r.is_empty());
    }
}
