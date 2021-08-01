// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::recorder::RecorderHandle;
use crate::summary::collector::{Collector, CollectorId};
use crate::summary::collector::{CollectorRegistrationMsg, COLLECTOR_REGISTRATION_CHANNEL};

use crate::summary::recorder::{
    ReqSummary, ReqSummaryRecords, TagInfo, ThreadRegistrationMsg, ThreadStat,
    THREAD_REGISTRATION_CHANNEL,
};

use std::fs::read_dir;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use collections::{HashMap, HashSet};
use lazy_static::lazy_static;
use libc::pid_t;
use std::borrow::Borrow;
use tikv_util::time::Instant;

const GC_INTERVAL_SECS: u64 = 1 * 60;

pub fn init_recorder() -> RecorderHandle {
    lazy_static! {
        static ref HANDLE: RecorderHandle = {
            let config = crate::Config::default();

            let pause = Arc::new(AtomicBool::new(config.enabled));
            let pause0 = pause.clone();
            let precision_ms = Arc::new(AtomicU64::new(config.precision.0.as_millis() as _));
            let precision_ms0 = precision_ms.clone();

            let join_handle = std::thread::Builder::new()
                .name("summary-recorder".to_owned())
                .spawn(move || {
                    let mut recorder = ReqSummaryRecorder::new(pause, precision_ms);

                    loop {
                        recorder.handle_pause();
                        recorder.handle_collector_registration();
                        recorder.handle_thread_registration();
                        recorder.record();
                        recorder.may_advance_window();
                        recorder.may_gc();

                        std::thread::sleep(Duration::from_millis(
                            recorder.precision_ms.load(Relaxed),
                        ));
                    }
                })
                .expect("Failed to create recorder thread");
            RecorderHandle::new(join_handle, pause0, precision_ms0)
        };
    }
    HANDLE.clone()
}

lazy_static! {
    static ref PID: pid_t = unsafe { libc::getpid() };
}

struct ReqSummaryRecorder {
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,
    thread_stats: HashMap<pid_t, ThreadStat>,
    current_window_records: ReqSummaryRecords,
    last_collect_instant: Instant,
    last_gc_instant: Instant,
    collectors: HashMap<CollectorId, Box<dyn Collector>>,
}

impl ReqSummaryRecorder {
    pub fn new(pause: Arc<AtomicBool>, precision_ms: Arc<AtomicU64>) -> Self {
        let now = Instant::now();

        Self {
            pause,
            precision_ms,
            thread_stats: HashMap::default(),
            current_window_records: ReqSummaryRecords::default(),
            last_collect_instant: now,
            last_gc_instant: now,
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
            thread_stat,
        }) = THREAD_REGISTRATION_CHANNEL.1.try_recv()
        {
            self.thread_stats.insert(thread_id, thread_stat);
        }
    }
    pub fn record(&mut self) {
        let records = &mut self.current_window_records.records;
        self.thread_stats
            .iter_mut()
            .for_each(|(_tid, thread_stat)| {
                let mut map_guard = thread_stat.records_by_tag.lock().unwrap();
                for (k, v) in map_guard.drain() {
                    records.entry(k).or_insert(ReqSummary::default()).merge(&v);
                }
                let cur_tag = thread_stat.shared_ptr.take().map(|req_tag| {
                    let tag = req_tag.clone();
                    // Put it back as quickly as possible.
                    assert!(thread_stat.shared_ptr.swap(req_tag).is_none());
                    tag
                });
                if let Some(tag) = cur_tag {
                    let cur_req_summary: &ReqSummary = thread_stat.req_summary.borrow();
                    let cur_req_summary = cur_req_summary.take_and_reset();
                    let tag = &tag.infos.extra_attachment;
                    let tag = TagInfo::new(tag.clone());
                    records
                        .entry(tag)
                        .or_insert(ReqSummary::default())
                        .merge(&cur_req_summary);
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
                self.thread_stats.retain(|k, _v| thread_ids.contains(k));
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
        self.current_window_records = ReqSummaryRecords::default();
        self.last_collect_instant = now;
        self.last_gc_instant = now;
    }
}
