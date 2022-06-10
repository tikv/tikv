// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(derive_default_enum)]
#![feature(array_from_fn)]

mod metrics;
mod slab;
mod tls;

use kvproto::kvrpcpb as pb;

pub use self::{
    slab::{TrackerToken, GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN},
    tls::*,
};

#[derive(Debug)]
pub struct Tracker {
    pub req_info: RequestInfo,
    pub metrics: RequestMetrics,
    // TODO: Add request stage info
    // pub current_stage: RequestStage,
}

impl Tracker {
    pub fn new(req_info: RequestInfo) -> Self {
        Self {
            req_info,
            metrics: Default::default(),
        }
    }

    pub fn write_scan_detail(&self, detail_v2: &mut pb::ScanDetailV2) {
        detail_v2.set_rocksdb_block_read_byte(self.metrics.block_read_byte);
        detail_v2.set_rocksdb_block_read_count(self.metrics.block_read_count);
        detail_v2.set_rocksdb_block_cache_hit_count(self.metrics.block_cache_hit_count);
        detail_v2.set_rocksdb_key_skipped_count(self.metrics.internal_key_skipped_count);
        detail_v2.set_rocksdb_delete_skipped_count(self.metrics.deleted_key_skipped_count);
    }
}

#[derive(Debug, Default)]
pub struct RequestInfo {
    pub region_id: u64,
    pub start_ts: u64,
    pub task_id: u64,
    pub resource_group_tag: Vec<u8>,
    pub request_type: RequestType,
}

impl RequestInfo {
    pub fn new(ctx: &pb::Context, request_type: RequestType, start_ts: u64) -> RequestInfo {
        RequestInfo {
            region_id: ctx.get_region_id(),
            start_ts,
            task_id: ctx.get_task_id(),
            resource_group_tag: ctx.get_resource_group_tag().to_vec(),
            request_type,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RequestType {
    #[default]
    Unknown,
    KvGet,
    KvBatchGet,
    KvBatchGetCommand,
    KvScan,
    KvScanLock,
    CoprocessorDag,
    CoprocessorAnalyze,
    CoprocessorChecksum,
}

#[derive(Debug, Default, Clone)]
pub struct RequestMetrics {
    pub get_snapshot_nanos: u64,
    pub block_cache_hit_count: u64,
    pub block_read_count: u64,
    pub block_read_byte: u64,
    pub block_read_nanos: u64,
    pub internal_key_skipped_count: u64,
    pub deleted_key_skipped_count: u64,
}
