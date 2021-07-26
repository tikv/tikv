// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;

use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use collections::HashMap;

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
