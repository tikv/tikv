// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod recorder;

use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::pid_t;
use std::borrow::Borrow;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::SharedTagPtr;
pub use recorder::init_recorder;

pub fn add_thread_read_key(count: u64) {
    CURRENT_REQ_SUMMARY.with(|r| {
        r.read_key_count.fetch_add(count, Relaxed);
    })
}

pub fn add_thread_write_key(count: u64) {
    CURRENT_REQ_SUMMARY.with(|r| {
        r.write_key_count.fetch_add(count, Relaxed);
    })
}

pub fn on_poll_begin() {
    CURRENT_REQ_SUMMARY.with(|r| {
        r.reset();
    })
}

pub fn on_poll_finish(tag: Vec<u8>) {
    if tag.is_empty() {
        return;
    }
    CURRENT_REQ_SUMMARY.with(|current_summary| {
        REQ_SUMMARY_MAP.with(|map| {
            let mut map_guard = map.lock().unwrap();
            let tag = TagInfo::new(tag);
            match map_guard.get(&tag) {
                Some(summary) => summary.merge(current_summary),
                None => {
                    let current_summary: &ReqSummary = current_summary.borrow();
                    map_guard.insert(tag, current_summary.clone());
                }
            }
        })
    })
}

thread_local! {
    pub static CURRENT_REQ_SUMMARY: Arc<ReqSummary> = Arc::new(ReqSummary::default());
    pub static REQ_SUMMARY_MAP: Arc<Mutex<HashMap<TagInfo, ReqSummary>>> = {
        let map = Arc::new(Mutex::new(HashMap::default()));
        CURRENT_REQ_SUMMARY.with(|r|{
            let cur_req_summary = r.clone();
            crate::cpu::recorder::CURRENT_REQ.with(|s| {
                // let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };
                let thread_id = thread_id::get() as libc::pid_t;
                let shared_ptr = s.shared_ptr.clone();
                THREAD_REGISTRATION_CHANNEL.0.send(ThreadRegistrationMsg {
                    thread_id,
                    thread_stat: ThreadStat{
                        shared_ptr: shared_ptr,
                        req_summary: cur_req_summary,
                        records_by_tag: map.clone(),
                    }
                }).ok();
            });
        });
        map
    };
}

pub(crate) struct ThreadRegistrationMsg {
    pub(crate) thread_id: pid_t,
    pub(crate) thread_stat: ThreadStat,
}

pub(crate) struct ThreadStat {
    pub(crate) shared_ptr: SharedTagPtr,
    pub(crate) req_summary: Arc<ReqSummary>,
    // tag -> Req
    pub(crate) records_by_tag: Arc<Mutex<HashMap<TagInfo, ReqSummary>>>,
}

lazy_static! {
    pub(crate) static ref THREAD_REGISTRATION_CHANNEL: (
        Sender<ThreadRegistrationMsg>,
        Receiver<ThreadRegistrationMsg>
    ) = unbounded();
}

#[derive(Debug, Default)]
pub struct ReqSummary {
    pub read_key_count: AtomicU64,
    pub write_key_count: AtomicU64,
}

impl ReqSummary {
    pub fn reset(&self) {
        self.read_key_count.store(0, Relaxed);
        self.write_key_count.store(0, Relaxed);
    }

    pub fn get_read_key_count(&self) -> u64 {
        self.read_key_count.load(Relaxed)
    }

    pub fn get_write_key_count(&self) -> u64 {
        self.write_key_count.load(Relaxed)
    }

    pub fn merge(&self, other: &Self) {
        self.read_key_count
            .fetch_add(other.read_key_count.load(Relaxed), Relaxed);
        self.write_key_count
            .fetch_add(other.write_key_count.load(Relaxed), Relaxed);
    }

    pub fn clone(&self) -> Self {
        Self {
            read_key_count: AtomicU64::new(self.read_key_count.load(Relaxed)),
            write_key_count: AtomicU64::new(self.write_key_count.load(Relaxed)),
        }
    }

    pub fn take_and_reset(&self) -> Self {
        Self {
            read_key_count: AtomicU64::new(self.read_key_count.swap(0, Relaxed)),
            write_key_count: AtomicU64::new(self.write_key_count.swap(0, Relaxed)),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfo {
    pub(crate) tag: Vec<u8>,
}

impl TagInfo {
    fn new(tag: Vec<u8>) -> Self {
        Self { tag }
    }
}

#[derive(Debug)]
pub struct ReqSummaryRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> ReqSummary
    pub records: HashMap<TagInfo, ReqSummary>,
}

impl Default for ReqSummaryRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_secs: now_unix_time.as_secs(),
            duration: Duration::default(),
            records: HashMap::default(),
        }
    }
}
