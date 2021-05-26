// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(shrink_to)]

pub mod collector;

mod future_ext;
pub use future_ext::FutureExt;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::Guard;

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::Guard;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::collector::Collector;
use collections::HashMap;

pub struct ReqCpuConfig {
    record_interval_ms: f64,
    collect_interval_ms: u64,
    gc_interval_ms: u64,
}

impl Default for ReqCpuConfig {
    fn default() -> Self {
        Self {
            record_interval_ms: 10.1,
            collect_interval_ms: 1_000,
            gc_interval_ms: 10 * 60 * 1_000,
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

#[derive(Debug, Clone)]
pub struct RequestCpuRecords {
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

pub struct Builder {
    config: ReqCpuConfig,
    collectors: Vec<Box<dyn Collector>>,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            config: ReqCpuConfig::default(),
            collectors: Vec::default(),
        }
    }

    pub fn record_interval_ms(mut self, value: f64) -> Self {
        self.config.record_interval_ms = value;
        self
    }

    pub fn collect_interval_ms(mut self, value: u64) -> Self {
        self.config.collect_interval_ms = value;
        self
    }

    pub fn gc_interval_ms(mut self, value: u64) -> Self {
        self.config.gc_interval_ms = value;
        self
    }

    pub fn register_collector<T: Collector + 'static>(mut self, c: T) -> Self {
        self.collectors.push(Box::new(c));
        self
    }
}
