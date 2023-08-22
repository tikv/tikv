// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod metrics;
mod slab;
mod tls;

use std::time::Instant;

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
        detail_v2.set_rocksdb_block_read_nanos(self.metrics.block_read_nanos);
        detail_v2.set_rocksdb_block_cache_hit_count(self.metrics.block_cache_hit_count);
        detail_v2.set_rocksdb_key_skipped_count(self.metrics.internal_key_skipped_count);
        detail_v2.set_rocksdb_delete_skipped_count(self.metrics.deleted_key_skipped_count);
        detail_v2.set_get_snapshot_nanos(self.metrics.get_snapshot_nanos);
        detail_v2.set_read_index_propose_wait_nanos(self.metrics.read_index_propose_wait_nanos);
        detail_v2.set_read_index_confirm_wait_nanos(self.metrics.read_index_confirm_wait_nanos);
        detail_v2.set_read_pool_schedule_wait_nanos(self.metrics.read_pool_schedule_wait_nanos);
    }

    pub fn write_write_detail(&self, detail: &mut pb::WriteDetail) {
        detail.set_latch_wait_nanos(self.metrics.latch_wait_nanos);
        detail.set_process_nanos(self.metrics.scheduler_process_nanos);
        detail.set_throttle_nanos(self.metrics.scheduler_throttle_nanos);
        detail.set_pessimistic_lock_wait_nanos(self.metrics.pessimistic_lock_wait_nanos);
        detail.set_store_batch_wait_nanos(self.metrics.wf_batch_wait_nanos);
        detail.set_propose_send_wait_nanos(
            self.metrics
                .wf_send_proposal_nanos
                .saturating_sub(self.metrics.wf_send_to_queue_nanos),
        );
        detail.set_persist_log_nanos(
            self.metrics.wf_persist_log_nanos - self.metrics.wf_send_to_queue_nanos,
        );
        detail.set_raft_db_write_leader_wait_nanos(
            self.metrics.store_mutex_lock_nanos + self.metrics.store_thread_wait_nanos,
        );
        detail.set_raft_db_sync_log_nanos(self.metrics.store_write_wal_nanos);
        detail.set_raft_db_write_memtable_nanos(self.metrics.store_write_memtable_nanos);
        // It's an approximation considering generating proposal is fast CPU operation.
        // And note that the time before flushing the raft message to the RPC channel is
        // also counted in this value (to be improved in the future).
        detail.set_commit_log_nanos(
            self.metrics.wf_commit_log_nanos - self.metrics.wf_batch_wait_nanos,
        );
        detail.set_apply_batch_wait_nanos(self.metrics.apply_wait_nanos);
        // When async_prewrite_apply is set, the `apply_time_nanos` could be less than
        // apply_wait_nanos.
        if self.metrics.apply_time_nanos > self.metrics.apply_wait_nanos {
            detail
                .set_apply_log_nanos(self.metrics.apply_time_nanos - self.metrics.apply_wait_nanos);
        }
        detail.set_apply_mutex_lock_nanos(self.metrics.apply_mutex_lock_nanos);
        detail.set_apply_write_leader_wait_nanos(self.metrics.apply_thread_wait_nanos);
        detail.set_apply_write_wal_nanos(self.metrics.apply_write_wal_nanos);
        detail.set_apply_write_memtable_nanos(self.metrics.apply_write_memtable_nanos);
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

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum RequestType {
    #[default]
    Unknown,
    KvGet,
    KvBatchGet,
    KvBatchGetCommand,
    KvScan,
    KvScanLock,
    KvPrewrite,
    KvCommit,
    KvPessimisticLock,
    KvCheckTxnStatus,
    KvCheckSecondaryLocks,
    KvCleanup,
    KvResolveLock,
    KvTxnHeartBeat,
    KvRollback,
    KvPessimisticRollback,
    KvFlashbackToVersion,
    CoprocessorDag,
    CoprocessorAnalyze,
    CoprocessorChecksum,
}

#[derive(Debug, Default, Clone)]
pub struct RequestMetrics {
    pub get_snapshot_nanos: u64,
    pub read_index_propose_wait_nanos: u64,
    pub read_index_confirm_wait_nanos: u64,
    pub read_pool_schedule_wait_nanos: u64,
    pub local_read: bool,
    pub block_cache_hit_count: u64,
    pub block_read_count: u64,
    pub block_read_byte: u64,
    pub block_read_nanos: u64,
    pub internal_key_skipped_count: u64,
    pub deleted_key_skipped_count: u64,
    pub pessimistic_lock_wait_nanos: u64,
    pub latch_wait_nanos: u64,
    pub scheduler_process_nanos: u64,
    pub scheduler_throttle_nanos: u64,
    // temp instant used in raftstore metrics, first be the instant when creating the write
    // callback, then reset when it is ready to apply
    pub write_instant: Option<Instant>,
    pub wf_batch_wait_nanos: u64,
    pub wf_send_to_queue_nanos: u64,
    pub wf_send_proposal_nanos: u64,
    pub wf_persist_log_nanos: u64,
    pub wf_before_write_nanos: u64,
    pub wf_write_end_nanos: u64,
    pub wf_kvdb_end_nanos: u64,
    pub wf_commit_log_nanos: u64,
    pub commit_not_persisted: bool,
    pub store_mutex_lock_nanos: u64, // should be 0 if using raft-engine
    pub store_thread_wait_nanos: u64,
    pub store_write_wal_nanos: u64,
    pub store_write_memtable_nanos: u64,
    pub store_time_nanos: u64,
    pub apply_wait_nanos: u64,
    pub apply_time_nanos: u64,
    pub apply_mutex_lock_nanos: u64,
    pub apply_thread_wait_nanos: u64,
    pub apply_write_wal_nanos: u64,
    pub apply_write_memtable_nanos: u64,
}
