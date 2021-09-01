// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{Collector, CollectorId};
use crate::cpu::collector::{CollectorRegistrationMsg, COLLECTOR_REGISTRATION_CHANNEL};
use crate::cpu::recorder::CpuRecords;
use crate::{Config, ResourceMeteringTag, TagInfos};

use std::cell::Cell;
use std::fs::read_dir;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use collections::{HashMap, HashSet};
use crossbeam::channel::{unbounded, Receiver, Sender};
use fail::fail_point;
use lazy_static::lazy_static;
use libc::pid_t;
use procinfo::pid;

use tikv_util::time::Instant;

use super::RecorderHandle;

const RECORD_FREQUENCY: f64 = 99.0;
const GC_INTERVAL_SECS: u64 = 15 * 60;

pub fn init_recorder(config: &Config) -> RecorderHandle {
    lazy_static! {
        static ref HANDLE: RecorderHandle = {
            let pause = Arc::new(AtomicBool::new(false));
            let pause0 = pause.clone();
            let precision_ms = Arc::new(AtomicU64::new(1000));
            let precision_ms0 = precision_ms.clone();

            let join_handle = std::thread::Builder::new()
                .name("cpu-recorder".to_owned())
                .spawn(move || {
                    let mut recorder = CpuRecorder::new(pause, precision_ms);

                    loop {
                        recorder.handle_pause();
                        recorder.handle_collector_registration();
                        recorder.handle_thread_registration();
                        recorder.record();
                        recorder.may_advance_window();
                        recorder.may_gc();

                        std::thread::sleep(Duration::from_micros(
                            (1_000.0 / RECORD_FREQUENCY * 1_000.0) as _,
                        ));
                    }
                })
                .expect("Failed to create recorder thread");
            RecorderHandle::new(join_handle, pause0, precision_ms0)
        };
    }

    let handle = HANDLE.clone();
    if config.enabled {
        handle.resume();
    } else {
        handle.pause();
    }
    handle.set_precision(config.precision.0);
    handle
}

impl ResourceMeteringTag {
    pub fn attach(&self) -> Guard {
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
struct SharedTagPtr {
    tag: Arc<AtomicPtr<TagInfos>>,
}
impl SharedTagPtr {
    fn take(&self) -> Option<ResourceMeteringTag> {
        let prev_ptr = self.tag.swap(std::ptr::null_mut(), SeqCst);
        (!prev_ptr.is_null())
            .then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev_ptr as _)) })
    }

    fn swap(&self, value: ResourceMeteringTag) -> Option<ResourceMeteringTag> {
        let tag_arc_ptr = Arc::into_raw(value.infos);
        let prev_ptr = self.tag.swap(tag_arc_ptr as _, SeqCst);
        (!prev_ptr.is_null())
            .then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev_ptr as _)) })
    }
}

struct LocalReqTag {
    is_set: Cell<bool>,
    shared_ptr: SharedTagPtr,
}
thread_local! {
    static CURRENT_REQ: LocalReqTag = {
        let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };

        let shared_ptr = SharedTagPtr::default();
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

lazy_static! {
    static ref PID: pid_t = unsafe { libc::getpid() };
    static ref CLK_TCK: libc::c_long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    static ref THREAD_REGISTRATION_CHANNEL: (
        Sender<ThreadRegistrationMsg>,
        Receiver<ThreadRegistrationMsg>
    ) = unbounded();
}
struct ThreadRegistrationMsg {
    thread_id: pid_t,
    shared_ptr: SharedTagPtr,
}

struct CpuRecorder {
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,

    thread_stats: HashMap<pid_t, ThreadStat>,
    current_window_records: CpuRecords,

    last_collect_instant: Instant,
    last_gc_instant: Instant,

    collectors: HashMap<CollectorId, Box<dyn Collector>>,
}

struct ThreadStat {
    shared_ptr: SharedTagPtr,
    stat: pid::Stat,
}

impl CpuRecorder {
    pub fn new(pause: Arc<AtomicBool>, precision_ms: Arc<AtomicU64>) -> Self {
        let now = Instant::now();

        Self {
            pause,
            precision_ms,

            last_collect_instant: now,
            last_gc_instant: now,

            thread_stats: HashMap::default(),
            current_window_records: CpuRecords::default(),

            collectors: HashMap::default(),
        }
    }

    pub fn handle_pause(&mut self) {
        let mut should_reset = false;
        while self.pause.load(SeqCst) {
            thread::park();
            should_reset = true;
        }

        if should_reset {
            self.reset();
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
                    stat: pid::stat_task(*PID, thread_id).unwrap_or_default(),
                    shared_ptr,
                },
            );
        }
    }

    pub fn record(&mut self) {
        let records = &mut self.current_window_records.records;
        self.thread_stats.iter_mut().for_each(|(tid, thread_stat)| {
            let cur_tag = thread_stat.shared_ptr.take().map(|req_tag| {
                let tag = req_tag.clone();
                // Put it back as quickly as possible.
                assert!(thread_stat.shared_ptr.swap(req_tag).is_none());
                tag
            });

            fail_point!(
                "cpu-record-test-filter",
                cur_tag.as_ref().map_or(false, |t| !t
                    .infos
                    .extra_attachment
                    .starts_with(super::TEST_TAG_PREFIX)),
                |_| {}
            );

            if let Some(cur_tag) = cur_tag {
                if let Ok(cur_stat) = pid::stat_task(*PID, *tid) {
                    STAT_TASK_COUNT.inc();

                    let last_stat = &thread_stat.stat;
                    let last_cpu_tick = last_stat.utime.wrapping_add(last_stat.stime);
                    let cur_cpu_tick = cur_stat.utime.wrapping_add(cur_stat.stime);
                    let delta_ticks = cur_cpu_tick.wrapping_sub(last_cpu_tick);

                    if delta_ticks > 0 {
                        let delta_ms = (delta_ticks * 1_000 / *CLK_TCK) as u64;
                        *records.entry(cur_tag).or_insert(0) += delta_ms;
                    }

                    thread_stat.stat = cur_stat;
                }
            }
        });
    }

    pub fn may_gc(&mut self) -> bool {
        const THREAD_STAT_LEN_THRESHOLD: usize = 500;
        const RECORD_LEN_THRESHOLD: usize = 20_000;

        let duration_secs = self.last_gc_instant.saturating_elapsed().as_secs();
        let need_gc = duration_secs >= GC_INTERVAL_SECS;

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
        let duration = self.last_collect_instant.saturating_elapsed();
        let need_advance = duration.as_millis() >= self.precision_ms.load(Relaxed) as _;

        if need_advance {
            let mut records = std::mem::take(&mut self.current_window_records);
            records.duration = duration;

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
        self.current_window_records = CpuRecords::default();
        for (thread_id, stat) in &mut self.thread_stats {
            stat.stat = pid::stat_task(*PID, *thread_id).unwrap_or_default();
        }
        self.last_collect_instant = now;
        self.last_gc_instant = now;
    }
}

lazy_static! {
    static ref STAT_TASK_COUNT: prometheus::IntCounter = prometheus::register_int_counter!(
        "tikv_resource_metering_stat_task_count",
        "Counter of stat_task call"
    )
    .unwrap();
}
