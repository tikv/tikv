// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(shrink_to)]

pub(crate) mod util;

mod future_ext;
pub use future_ext::FutureExt;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::build;
#[cfg(target_os = "linux")]
pub use linux::Guard;

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::build;
#[cfg(not(target_os = "linux"))]
pub use dummy::Guard;

use crate::util::wrapping_deque::WrappingDeque;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use collections::HashMap;

pub struct ReqCpuConfig {
    record_interval_ms: f64,
    window_size_ms: u64,
    gc_interval_ms: u64,
    buffer_size: usize,
}

impl Default for ReqCpuConfig {
    fn default() -> Self {
        Self {
            record_interval_ms: 10.1,
            window_size_ms: 1_000,
            gc_interval_ms: 10 * 60 * 1_000,
            buffer_size: 60,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct RequestTags {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    pub extra_attachment: Vec<u8>,
}

impl RequestTags {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
}

#[derive(Debug)]
struct RequestCpuRecords {
    begin_unix_time_ms: u64,
    duration_ms: u64,
    records: HashMap<Arc<RequestTags>, u64>,
}

impl Default for RequestCpuRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_ms: now_unix_time.as_millis() as _,
            duration_ms: 0,
            records: HashMap::default(),
        }
    }
}

#[derive(Debug)]
pub struct RequestCpuReporter {
    records: WrappingDeque<RequestCpuRecords>,
}

impl RequestCpuReporter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            records: WrappingDeque::with_capacity(capacity),
        }
    }

    fn push(&mut self, record: RequestCpuRecords) {
        self.records.push(record);
    }
}
