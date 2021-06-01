// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(shrink_to)]

pub mod collector;

mod future_ext;
pub use future_ext::FutureExt;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{init_recorder, register_collector, CollectorHandle, Guard};

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::{init_recorder, register_collector, CollectorHandle, Guard};

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use collections::HashMap;

pub struct CpuRecorderConfig {
    record_interval_ms: f64,
    collect_interval_ms: u64,
    gc_interval_ms: u64,
}

impl Default for CpuRecorderConfig {
    fn default() -> Self {
        Self {
            record_interval_ms: 10.1,
            collect_interval_ms: 1_000,
            gc_interval_ms: 10 * 60 * 1_000,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    pub infos: Arc<TagInfos>,
}

impl ResourceMeteringTag {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
    }
}

impl From<Arc<TagInfos>> for ResourceMeteringTag {
    fn from(infos: Arc<TagInfos>) -> Self {
        Self { infos }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfos {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    pub extra_attachment: Vec<u8>,
}

impl TagInfos {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        TagInfos {
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
    records: HashMap<ResourceMeteringTag, u64>,
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
