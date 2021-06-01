// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{Collector, CollectorId};
use crate::{CpuRecorderConfig, RequestCpuRecords, ResourceMeteringTag};

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

pub fn init_recorder() {
    lazy_static! {
        static ref PHANTOM: () = {
            std::thread::Builder::new()
                .name("req-cpu-recorder".to_owned())
                .spawn(move || {
                    let mut recorder = ReqCpuRecorder::new(CpuRecorderConfig::default());

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

impl ResourceMeteringTag {
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

#[derive(Default, Clone)]
struct SharedReqTagPtr {
    req_tag: Arc<AtomicPtr<ResourceMeteringTag>>,
}
impl SharedReqTagPtr {
    fn take(&self) -> Option<Arc<ResourceMeteringTag>> {
        let prev_ptr = self.req_tag.swap(std::ptr::null_mut(), Acquire);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }

    fn swap(&self, value: Arc<ResourceMeteringTag>) -> Option<Arc<ResourceMeteringTag>> {
        let tag_arc_ptr = Arc::into_raw(value);
        let prev_ptr = self.req_tag.swap(tag_arc_ptr as _, AcqRel);
        (!prev_ptr.is_null()).then(|| unsafe { Arc::from_raw(prev_ptr as _) })
    }
}

struct LocalReqTag {
    is_set: Cell<bool>,
    shared_ptr: SharedReqTagPtr,
}
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

struct ReqCpuRecorder {
    config: CpuRecorderConfig,

    thread_stats: HashMap<pid_t, ThreadStat>,
    current_window_records: RequestCpuRecords,

    last_collect_instant: Instant,
    last_gc_instant: Instant,

    collectors: HashMap<CollectorId, Box<dyn Collector>>,
}

struct ThreadStat {
    shared_ptr: SharedReqTagPtr,
    prev_tag: Option<Arc<ResourceMeteringTag>>,
    prev_stat: pid::Stat,
}

impl ReqCpuRecorder {
    pub fn new(config: CpuRecorderConfig) -> Self {
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

                // If existing current tag, need to store the beginning stat.
                // If existing previous tag, need to get the end stat to calculate delta.
                if let Ok(stat) = procinfo::pid::stat_task(*PID, *tid) {
                    // Accumulate the cpu time for the previous tag.
                    if let Some(prev_tag) = prev_tag {
                        let prev_cpu_ticks = (thread_stat.prev_stat.utime as u64)
                            .wrapping_add(thread_stat.prev_stat.stime as u64);
                        let current_cpu_ticks = (stat.utime as u64).wrapping_add(stat.stime as u64);
                        let delta_ms = current_cpu_ticks.wrapping_sub(prev_cpu_ticks) * 1_000
                            / (*CLK_TCK as u64);

                        if delta_ms != 0 {
                            *self
                                .current_window_records
                                .records
                                .entry(prev_tag)
                                .or_insert(0) += delta_ms;
                        }
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

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Mutex;
    use std::thread::JoinHandle;

    enum Operation {
        SetContext(&'static str),
        ResetContext,
        CpuHeavy(u64),
        Sleep(u64),
    }

    struct Operations {
        ops: Vec<Operation>,
        current_ctx: Option<&'static str>,
        cpu_time: HashMap<String, u64>,
    }

    impl Operations {
        fn begin() -> Self {
            Self {
                ops: Vec::default(),
                current_ctx: None,
                cpu_time: HashMap::default(),
            }
        }

        fn then(mut self, op: Operation) -> Self {
            match op {
                Operation::SetContext(tag) => {
                    assert!(self.current_ctx.is_none(), "cannot set nested contexts");
                    self.current_ctx = Some(tag);
                    self.ops.push(op);
                    self
                }
                Operation::ResetContext => {
                    assert!(self.current_ctx.is_some(), "context is not set");
                    self.ops.push(op);
                    self.current_ctx = None;
                    self
                }
                Operation::CpuHeavy(ms) => {
                    if let Some(tag) = self.current_ctx {
                        *self.cpu_time.entry(tag.to_string()).or_insert(0) += ms;
                    }
                    self.ops.push(op);
                    self
                }
                Operation::Sleep(_) => {
                    self.ops.push(op);
                    self
                }
            }
        }

        fn spawn(self) -> (JoinHandle<()>, HashMap<String, u64>) {
            assert!(
                self.current_ctx.is_none(),
                "should keep context clean finally"
            );

            let Operations { ops, cpu_time, .. } = self;

            let handle = std::thread::spawn(|| {
                let mut guard = None;

                for op in ops {
                    match op {
                        Operation::SetContext(tag) => {
                            let tag = Arc::new(ResourceMeteringTag {
                                store_id: 0,
                                region_id: 0,
                                peer_id: 0,
                                extra_attachment: Vec::from(tag),
                            });

                            guard = Some(tag.attach());
                        }
                        Operation::ResetContext => {
                            guard.take();
                        }
                        Operation::CpuHeavy(ms) => {
                            let done = Arc::new(AtomicBool::new(false));
                            let done1 = done.clone();
                            std::thread::spawn(move || {
                                std::thread::sleep(Duration::from_millis(ms));
                                done.store(true, SeqCst);
                            });

                            while !done1.load(SeqCst) {
                                Self::heavy_job();
                            }
                        }
                        Operation::Sleep(ms) => {
                            std::thread::sleep(Duration::from_millis(ms));
                        }
                    }
                }
            });

            (handle, cpu_time)
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
    }

    #[derive(Default, Clone)]
    struct DummyCollector {
        records: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl Collector for DummyCollector {
        fn collect(&self, records: Arc<RequestCpuRecords>) {
            if let Ok(mut r) = self.records.lock() {
                for (tag, ms) in &records.records {
                    let str = String::from_utf8(tag.extra_attachment.clone()).unwrap();
                    *r.entry(str).or_insert(0) += *ms;
                }
            }
        }
    }

    use Operation::*;

    #[test]
    fn test_req_cpu() {
        init_recorder();

        // Heavy CPU only with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(2000))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Sleep only with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(Sleep(2000))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Hybrid workload with 1 thread
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle, expected) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(600))
                .then(Sleep(400))
                .then(ResetContext)
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(500))
                .then(Sleep(500))
                .then(ResetContext)
                .then(CpuHeavy(400))
                .then(SetContext("ctx-2"))
                .then(Sleep(600))
                .then(ResetContext)
                .spawn();
            handle.join().unwrap();

            collector.check(expected);
        }

        // Heavy CPU with 3 threads
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle0, expected0) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            let (handle1, expected1) = Operations::begin()
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            let (handle2, expected2) = Operations::begin()
                .then(SetContext("ctx-2"))
                .then(CpuHeavy(1500))
                .then(ResetContext)
                .spawn();
            handle0.join().unwrap();
            handle1.join().unwrap();
            handle2.join().unwrap();

            collector.check(merge(vec![expected0, expected1, expected2]));
        }

        // Hybrid workload with 3 threads
        {
            let collector = DummyCollector::default();
            let _handle = register_collector(Box::new(collector.clone()));

            let (handle0, expected0) = Operations::begin()
                .then(SetContext("ctx-0"))
                .then(CpuHeavy(200))
                .then(Sleep(300))
                .then(ResetContext)
                .then(SetContext("ctx-1"))
                .then(Sleep(200))
                .then(CpuHeavy(600))
                .then(ResetContext)
                .then(CpuHeavy(500))
                .spawn();
            let (handle1, expected1) = Operations::begin()
                .then(SetContext("ctx-1"))
                .then(CpuHeavy(500))
                .then(ResetContext)
                .then(CpuHeavy(200))
                .then(SetContext("ctx-2"))
                .then(Sleep(400))
                .then(ResetContext)
                .then(Sleep(300))
                .spawn();
            let (handle2, expected2) = Operations::begin()
                .then(SetContext("ctx-2"))
                .then(CpuHeavy(800))
                .then(ResetContext)
                .then(SetContext("ctx-1"))
                .then(Sleep(200))
                .then(ResetContext)
                .then(CpuHeavy(200))
                .spawn();
            handle0.join().unwrap();
            handle1.join().unwrap();
            handle2.join().unwrap();

            collector.check(merge(vec![expected0, expected1, expected2]));
        }
    }

    impl DummyCollector {
        fn check(&self, mut expected: HashMap<String, u64>) {
            // Wait a collect interval to avoid losing records.
            std::thread::sleep(Duration::from_millis(1200));

            const MAX_DRIFT: u64 = 50;
            let mut res = self.records.lock().unwrap();

            for k in expected.keys() {
                res.entry(k.clone()).or_insert(0);
            }
            for k in res.keys() {
                expected.entry(k.clone()).or_insert(0);
            }

            for (k, expected_value) in expected {
                let value = res.get(&k).unwrap();
                let l = value.saturating_sub(MAX_DRIFT);
                let r = value.saturating_add(MAX_DRIFT);
                if !(l <= expected_value && expected_value <= r) {
                    panic!(
                        "tag {} cpu time expected {} got {}",
                        k, expected_value, value
                    );
                }
            }
        }
    }

    fn merge(maps: impl IntoIterator<Item = HashMap<String, u64>>) -> HashMap<String, u64> {
        let mut map = HashMap::default();
        for m in maps {
            for (k, v) in m {
                *map.entry(k).or_insert(0) += v;
            }
        }
        map
    }
}
