// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod future;
mod metrics;
mod slab;
mod tls;

use std::time::Instant;

use kvproto::kvrpcpb as pb;

#[cfg(feature = "linearizability-track")]
pub use self::linearizability_track::*;
pub use self::{
    future::{FutureTrack, track},
    slab::{GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN, TrackerToken, TrackerTokenArray},
    tls::*,
};

#[derive(Debug)]
pub struct Tracker {
    pub req_info: RequestInfo,
    pub metrics: RequestMetrics,
    #[cfg(feature = "linearizability-track")]
    pub linearizability_track: Option<LinearizabilityDebug>,
}

impl Tracker {
    pub fn new(req_info: RequestInfo) -> Self {
        #[cfg(not(feature = "linearizability-track"))]
        {
            Self {
                req_info,
                metrics: Default::default(),
            }
        }
        #[cfg(feature = "linearizability-track")]
        {
            let debug = if req_info.is_external_req {
                Some(Default::default())
            } else {
                None
            };
            Self {
                req_info,
                metrics: Default::default(),
                linearizability_track: debug,
            }
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
}

#[cfg(feature = "linearizability-track")]
mod linearizability_track {
    use std::{default::Default, fmt::Debug, mem};

    use chrono::{DateTime, Local};
    use uuid::Uuid;

    use super::*;
    use crate::tls::get_tls_peer_state;

    pub const INVALID_PEER_STATE: PeerStateTracker = PeerStateTracker {
        region_id: 0,
        peer_id: 0,
        term: 0,
        committed_index: 0,
        applied_index: 0,
    };

    #[derive(Debug, Default, PartialEq, Clone, Copy)]
    pub struct PeerStateTracker {
        pub region_id: u64,
        pub peer_id: u64,
        pub term: u64,
        pub committed_index: u64,
        pub applied_index: u64,
    }

    impl Tracker {
        pub fn track_propose_skip(&mut self, reason: &'static str) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.propose_state =
                        ProposeState::Skip(chrono::offset::Local::now(), reason.to_owned());
                }
                _ => {}
            }
        }

        pub fn track_propose_amend(&mut self, amend_to: Uuid) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.propose_state = ProposeState::Amend(
                        chrono::offset::Local::now(),
                        amend_to,
                        get_tls_peer_state(),
                    );
                }
                _ => {}
            }
        }

        pub fn track_propose_read_index(&mut self, uuid: Uuid) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.propose_state = ProposeState::ReadIndex(
                        chrono::offset::Local::now(),
                        uuid,
                        get_tls_peer_state(),
                    );
                }
                _ => {}
            }
        }

        pub fn track_propose_normal(&mut self) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.propose_state =
                        ProposeState::Normal(chrono::offset::Local::now(), get_tls_peer_state());
                }
                _ => {}
            }
        }

        pub fn propose_must_checked(&self) {
            match self.linearizability_track.as_ref() {
                Some(debug) => {
                    assert_ne!(debug.propose_state, ProposeState::None);
                }
                _ => {}
            }
        }

        pub fn track_ready_committed(&mut self) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.ready_state =
                        ReadyState::Committed(chrono::offset::Local::now(), get_tls_peer_state());
                }
                _ => {}
            }
        }

        pub fn track_ready_replica_read(&mut self, read_index: Option<u64>) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    assert_eq!(debug.ready_state, ReadyState::None);
                    debug.ready_state = ReadyState::ReplicaRead(
                        chrono::offset::Local::now(),
                        read_index,
                        get_tls_peer_state(),
                    );
                }
                _ => {}
            }
        }

        pub fn track_apply_applied(&mut self) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    assert_eq!(debug.apply_state, ApplyState::None);
                    debug.apply_state =
                        ApplyState::Applied(chrono::offset::Local::now(), get_tls_peer_state());
                }
                _ => {}
            }
        }

        pub fn track_flush_scheduler_snapshot(&mut self) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.scheduler_snapshot_states = Some((
                        mem::replace(&mut debug.propose_state, ProposeState::None),
                        mem::replace(&mut debug.ready_state, ReadyState::None),
                    ));
                }
                _ => {}
            }
        }

        pub fn track_snapshot_seq_no(&mut self, seq_no: u64) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.snapshot_seq_no = Some(seq_no);
                }
                _ => {}
            }
        }

        pub fn track_success(&mut self) {
            match self.linearizability_track.as_mut() {
                Some(debug) => {
                    debug.success = true;
                }
                _ => {}
            }
        }
    }

    impl PeerStateTracker {
        pub fn new(
            region_id: u64,
            peer_id: u64,
            term: u64,
            committed_index: u64,
            applied_index: u64,
        ) -> Self {
            Self {
                region_id,
                peer_id,
                term,
                committed_index,
                applied_index,
            }
        }
    }

    #[derive(Debug, Default)]
    pub struct LinearizabilityDebug {
        // general states
        pub success: bool,

        // gRPC pool states
        pub grpc_process_at: Option<DateTime<Local>>,

        // Scheduler or Read Pool states
        pub scheduler_snapshot_states: Option<(ProposeState, ReadyState)>,
        pub snapshot_seq_no: Option<u64>,

        // RaftPool states
        pub propose_state: ProposeState,
        pub ready_state: ReadyState,

        // ApplyPool states
        pub apply_state: ApplyState,
        // RocksDB states
    }

    #[derive(PartialEq)]
    pub enum ProposeState {
        None,
        Skip(DateTime<Local>, String),
        Amend(DateTime<Local>, Uuid, PeerStateTracker),
        ReadIndex(DateTime<Local>, Uuid, PeerStateTracker),
        Normal(DateTime<Local>, PeerStateTracker),
    }

    impl Default for ProposeState {
        fn default() -> Self {
            ProposeState::None
        }
    }

    impl Debug for ProposeState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ProposeState::None => write!(f, "None"),
                ProposeState::Skip(time, reason) => {
                    write!(f, "skip at {:?}, reason {}", time, reason)
                }
                ProposeState::Amend(time, uuid, state) => write!(
                    f,
                    "amend at {:?}, to {:?}, peer_state {:?}",
                    time, uuid, state
                ),
                ProposeState::ReadIndex(time, uuid, state) => write!(
                    f,
                    "propose read-index at {:?}, uuid: {:?}, peer_state {:?}",
                    time, uuid, state
                ),
                ProposeState::Normal(time, state) => {
                    write!(f, "propose normal at {:?}, peer_state {:?}", time, state)
                }
            }
        }
    }

    #[derive(PartialEq)]
    pub enum ReadyState {
        None,
        Committed(DateTime<Local>, PeerStateTracker),
        ReplicaRead(DateTime<Local>, Option<u64>, PeerStateTracker),
    }

    impl Default for ReadyState {
        fn default() -> Self {
            ReadyState::None
        }
    }

    impl Debug for ReadyState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ReadyState::None => write!(f, "None"),
                ReadyState::Committed(time, state) => {
                    write!(f, "committed at {:?}, peer_state {:?}", time, state)
                }
                ReadyState::ReplicaRead(time, read_index, state) => {
                    write!(
                        f,
                        "replica-read at {:?}, read_index: {:?}, peer_state {:?}",
                        time, read_index, state
                    )
                }
            }
        }
    }

    #[derive(PartialEq)]
    pub enum ApplyState {
        None,
        Applied(DateTime<Local>, PeerStateTracker),
    }

    impl Default for ApplyState {
        fn default() -> Self {
            ApplyState::None
        }
    }

    impl Debug for ApplyState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ApplyState::None => write!(f, "None"),
                ApplyState::Applied(time, state) => {
                    write!(f, "applied at {:?}, peer_state {:?}", time, state)
                }
            }
        }
    }
}
