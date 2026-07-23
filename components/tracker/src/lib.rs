// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod future;
mod metrics;
mod slab;
mod tls;

use std::time::Instant;

use kvproto::kvrpcpb as pb;

pub use self::{
    future::{FutureTrack, track},
    slab::{GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN, TrackerToken, TrackerTokenArray},
    tls::*,
};

#[derive(Debug)]
pub struct Tracker {
    pub req_info: RequestInfo,
    pub metrics: RequestMetrics,
}

impl Tracker {
    pub fn new(req_info: RequestInfo) -> Self {
        Self {
            req_info,
            metrics: Default::default(),
        }
    }

    /// This function is used to merge the time detail of the request into the
    /// `TimeDetailV2` of the request.
    // Note: This function uses merge because the
    // [`tikv::coprocessor::tracker::Tracker`] sets the `TimeDetailV2`, and we
    // don't want to overwrite the its result.
    pub fn merge_time_detail(&self, detail_v2: &mut pb::TimeDetailV2) {
        detail_v2.set_kv_grpc_process_time_ns(
            detail_v2.kv_grpc_process_time_ns + self.metrics.grpc_process_nanos,
        );
        detail_v2.set_process_wall_time_ns(
            detail_v2.process_wall_time_ns + self.metrics.future_process_nanos,
        );
        detail_v2.set_process_suspend_wall_time_ns(
            detail_v2.process_suspend_wall_time_ns + self.metrics.future_suspend_nanos,
        );
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

        // NOTE: These stats are mainly for write operations. Read operations populate
        // MVCC scan stats by `Statistics::write_scan_detail` before calling this
        // function. So only fill them when unset to avoid clobbering existing values.
        if detail_v2.get_total_versions() == 0 {
            detail_v2.set_total_versions(self.metrics.mvcc_total_versions);
        }
        if detail_v2.get_processed_versions() == 0 {
            detail_v2.set_processed_versions(self.metrics.mvcc_processed_versions);
        }
        if detail_v2.get_processed_versions_size() == 0 {
            detail_v2.set_processed_versions_size(self.metrics.mvcc_processed_versions_size);
        }
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

    pub fn write_ru_v2(&self, detail: &mut pb::Ruv2) {
        detail.set_coprocessor_executor_iterations(self.metrics.coprocessor_executor_iterations);
        detail.set_coprocessor_response_bytes(self.metrics.coprocessor_response_bytes);
        detail.set_raftstore_store_write_trigger_wb_bytes(
            self.metrics.raftstore_store_write_trigger_wb_bytes,
        );
        detail.set_storage_processed_keys_batch_get(self.metrics.storage_processed_keys_batch_get);
        detail.set_storage_processed_keys_get(self.metrics.storage_processed_keys_get);

        let executor_inputs = detail.mut_executor_inputs();
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_index_scan(
            self.metrics.executor_work_batch_index_scan,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_table_scan(
            self.metrics.executor_work_batch_table_scan,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_selection(
            self.metrics.executor_work_batch_selection,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_top_n(
            self.metrics.executor_work_batch_top_n,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_limit(
            self.metrics.executor_work_batch_limit,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_simple_aggr(
            self.metrics.executor_work_batch_simple_aggr,
        );
        executor_inputs.set_tikv_coprocessor_executor_work_total_batch_fast_hash_aggr(
            self.metrics.executor_work_batch_fast_hash_aggr,
        );
    }
}

/// Attributes a tracked future's poll time to the slab tracker addressed by
/// a token: busy poll time accumulates into
/// [`RequestMetrics::future_process_nanos`] and the gaps between polls —
/// including the wait before the first poll — into
/// [`RequestMetrics::future_suspend_nanos`], which
/// [`Tracker::merge_time_detail`] folds into the request's `TimeDetailV2`.
/// Unlike `TlsFutureTracker` in the storage layer it keeps no thread local
/// state, so it can track a future handed to a foreign executor; an invalid
/// or removed token makes it a no-op.
#[derive(Debug)]
pub struct TokenFutureTracker {
    token: TrackerToken,
    poll_began: Option<Instant>,
    last_finished: Instant,
    suspend_nanos: u64,
}

impl TokenFutureTracker {
    pub fn new(token: TrackerToken) -> Self {
        Self {
            token,
            poll_began: None,
            last_finished: Instant::now(),
            suspend_nanos: 0,
        }
    }
}

impl FutureTrack for TokenFutureTracker {
    fn on_poll_begin(&mut self) {
        let now = Instant::now();
        self.suspend_nanos += now.saturating_duration_since(self.last_finished).as_nanos() as u64;
        self.poll_began = Some(now);
    }

    fn on_poll_finish(&mut self) {
        let now = Instant::now();
        let process_nanos = self
            .poll_began
            .take()
            .map_or(0, |at| now.saturating_duration_since(at).as_nanos() as u64);
        let suspend_nanos = std::mem::take(&mut self.suspend_nanos);
        GLOBAL_TRACKERS.with_tracker(self.token, |tracker| {
            tracker.metrics.future_process_nanos += process_nanos;
            tracker.metrics.future_suspend_nanos += suspend_nanos;
        });
        self.last_finished = now;
    }
}

#[derive(Debug, Clone)]
pub struct RequestInfo {
    pub region_id: u64,
    pub start_ts: u64,
    pub task_id: u64,
    pub resource_group_tag: Vec<u8>,
    pub begin: Instant,

    // Information recorded after the task is scheduled.
    pub request_type: RequestType,
    pub cid: u64,
    pub is_external_req: bool,
}

impl RequestInfo {
    pub fn new(ctx: &pb::Context, request_type: RequestType, start_ts: u64) -> RequestInfo {
        RequestInfo {
            region_id: ctx.get_region_id(),
            start_ts,
            task_id: ctx.get_task_id(),
            resource_group_tag: ctx.get_resource_group_tag().to_vec(),
            begin: Instant::now(),
            request_type,
            cid: 0,
            is_external_req: ctx.get_request_source().contains("external"),
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
    KvFlush,
    KvBufferBatchGet,
}

#[derive(Debug, Default, Clone)]
pub struct RequestMetrics {
    pub grpc_process_nanos: u64,
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

    // MVCC scan stats for txn commands, to be written into `ScanDetailV2`.
    pub mvcc_total_versions: u64,
    pub mvcc_processed_versions: u64,
    pub mvcc_processed_versions_size: u64,

    pub future_process_nanos: u64,
    pub future_suspend_nanos: u64,

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

    // RU v2 metrics.
    pub executor_work_batch_index_scan: u64,
    pub executor_work_batch_table_scan: u64,
    pub executor_work_batch_selection: u64,
    pub executor_work_batch_top_n: u64,
    pub executor_work_batch_limit: u64,
    pub executor_work_batch_simple_aggr: u64,
    pub executor_work_batch_fast_hash_aggr: u64,
    pub coprocessor_executor_iterations: u64,
    pub coprocessor_response_bytes: u64,
    pub raftstore_store_write_trigger_wb_bytes: u64,
    pub storage_processed_keys_batch_get: u64,
    pub storage_processed_keys_get: u64,

    // recorded outside the read_pool thread, accessed inside the read_pool thread for topsql usage
    pub grpc_req_size: u64,
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    fn new_req_info() -> RequestInfo {
        RequestInfo {
            region_id: 0,
            start_ts: 0,
            task_id: 0,
            resource_group_tag: vec![],
            begin: Instant::now(),
            request_type: RequestType::default(),
            cid: 0,
            is_external_req: false,
        }
    }

    #[test]
    fn test_token_future_tracker_attributes_poll_time() {
        use std::{
            future::{Future, poll_fn},
            pin::pin,
            task::{Context, Poll, Waker},
            time::Duration,
        };

        // Spinning self-measures, so assertions hold under arbitrary
        // scheduling load, unlike sleeps.
        fn spin(d: Duration) {
            let begin = Instant::now();
            while begin.elapsed() < d {}
        }

        let token = GLOBAL_TRACKERS.insert(Tracker::new(new_req_info()));
        let mut polled = false;
        let fut = track(
            poll_fn(move |_cx| {
                spin(Duration::from_millis(2));
                if polled {
                    Poll::Ready(())
                } else {
                    polled = true;
                    Poll::Pending
                }
            }),
            TokenFutureTracker::new(token),
        );
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        spin(Duration::from_millis(2));
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        spin(Duration::from_millis(2));
        assert!(fut.as_mut().poll(&mut cx).is_ready());

        let mut detail = pb::TimeDetailV2::default();
        GLOBAL_TRACKERS.with_tracker(token, |tracker| tracker.merge_time_detail(&mut detail));
        GLOBAL_TRACKERS.remove(token);

        // Two polls spun >= 2ms each; a >= 2ms gap preceded each poll.
        let expected = Duration::from_millis(4).as_nanos() as u64;
        assert!(detail.get_process_wall_time_ns() >= expected);
        assert!(detail.get_process_suspend_wall_time_ns() >= expected);
    }

    #[test]
    fn test_write_scan_detail_sets_mvcc_scan_stats_when_unset_and_is_idempotent() {
        let mut tracker = Tracker::new(new_req_info());
        tracker.metrics.mvcc_processed_versions = 3;
        tracker.metrics.mvcc_total_versions = 5;
        tracker.metrics.mvcc_processed_versions_size = 7;

        let mut detail_v2 = pb::ScanDetailV2::default();

        tracker.write_scan_detail(&mut detail_v2);
        tracker.write_scan_detail(&mut detail_v2);

        assert_eq!(detail_v2.get_processed_versions(), 3);
        assert_eq!(detail_v2.get_total_versions(), 5);
        assert_eq!(detail_v2.get_processed_versions_size(), 7);
    }

    #[test]
    fn test_write_scan_detail_does_not_override_existing_mvcc_scan_stats() {
        let mut tracker = Tracker::new(new_req_info());
        tracker.metrics.mvcc_processed_versions = 3;
        tracker.metrics.mvcc_total_versions = 5;
        tracker.metrics.mvcc_processed_versions_size = 7;

        let mut detail_v2 = pb::ScanDetailV2::default();
        detail_v2.set_processed_versions(11);
        detail_v2.set_total_versions(13);
        detail_v2.set_processed_versions_size(17);

        tracker.write_scan_detail(&mut detail_v2);

        assert_eq!(detail_v2.get_processed_versions(), 11);
        assert_eq!(detail_v2.get_total_versions(), 13);
        assert_eq!(detail_v2.get_processed_versions_size(), 17);
    }
}
