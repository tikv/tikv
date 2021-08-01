// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{ResourceMeteringTag, SharedTagPtr};

use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::pid_t;
use std::cell::Cell;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const TEST_TAG_PREFIX: &[u8] = b"__resource_metering::tests::";

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{init_recorder, Guard};

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::{init_recorder, Guard};

#[derive(Clone, Default)]
pub struct RecorderHandle {
    inner: Option<Arc<RecorderHandleInner>>,
}

pub struct RecorderHandleInner {
    join_handle: JoinHandle<()>,
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,
}

impl RecorderHandle {
    pub fn new(
        join_handle: JoinHandle<()>,
        pause: Arc<AtomicBool>,
        precision_ms: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Some(Arc::new(RecorderHandleInner {
                join_handle,
                pause,
                precision_ms,
            })),
        }
    }

    pub fn pause(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(true, SeqCst);
        }
    }

    pub fn resume(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(false, SeqCst);
            inner.join_handle.thread().unpark();
        }
    }

    pub fn set_precision(&self, value: Duration) {
        if let Some(inner) = self.inner.as_ref() {
            inner.precision_ms.store(value.as_millis() as _, SeqCst);
        }
    }
}

#[derive(Debug)]
pub struct CpuRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> ms
    pub records: HashMap<ResourceMeteringTag, u64>,
}

impl Default for CpuRecords {
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

pub struct LocalReqTag {
    pub(crate) is_set: Cell<bool>,
    pub shared_ptr: SharedTagPtr,
}

thread_local! {
    pub static CURRENT_REQ: LocalReqTag = {
        let thread_id = get_thread_id();
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

#[cfg(not(target_os = "linux"))]
pub fn get_thread_id() -> libc::pid_t {
    thread_id::get() as libc::pid_t
}

#[cfg(target_os = "linux")]
pub fn get_thread_id() -> libc::pid_t {
    let thread_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };
    thread_id
}

lazy_static! {
    pub(crate) static ref THREAD_REGISTRATION_CHANNEL: (
        Sender<ThreadRegistrationMsg>,
        Receiver<ThreadRegistrationMsg>
    ) = unbounded();
}

pub(crate) struct ThreadRegistrationMsg {
    pub(crate) thread_id: pid_t,
    pub(crate) shared_ptr: SharedTagPtr,
}
