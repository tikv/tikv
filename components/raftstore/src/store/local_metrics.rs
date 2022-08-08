// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::{Arc, Mutex};

use collections::HashSet;
use prometheus::local::LocalHistogram;
use raft::eraftpb::MessageType;

use super::metrics::*;

/// The buffered metrics counters for raft ready handling.
#[derive(Debug, Default, Clone)]
pub struct RaftReadyMetrics {
    pub message: u64,
    pub commit: u64,
    pub append: u64,
    pub snapshot: u64,
    pub pending_region: u64,
    pub has_ready_region: u64,
}

impl RaftReadyMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.message > 0 {
            STORE_RAFT_READY_COUNTER.message.inc_by(self.message);
            self.message = 0;
        }
        if self.commit > 0 {
            STORE_RAFT_READY_COUNTER.commit.inc_by(self.commit);
            self.commit = 0;
        }
        if self.append > 0 {
            STORE_RAFT_READY_COUNTER.append.inc_by(self.append);
            self.append = 0;
        }
        if self.snapshot > 0 {
            STORE_RAFT_READY_COUNTER.snapshot.inc_by(self.snapshot);
            self.snapshot = 0;
        }
        if self.pending_region > 0 {
            STORE_RAFT_READY_COUNTER
                .pending_region
                .inc_by(self.pending_region);
            self.pending_region = 0;
        }
        if self.has_ready_region > 0 {
            STORE_RAFT_READY_COUNTER
                .has_ready_region
                .inc_by(self.has_ready_region);
            self.has_ready_region = 0;
        }
    }
}

pub type SendStatus = [u64; 2];

macro_rules! flush_send_status {
    ($metrics:ident, $self:ident) => {{
        if $self.$metrics[0] > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER
                .$metrics
                .drop
                .inc_by($self.$metrics[0]);
            $self.$metrics[0] = 0;
        }
        if $self.$metrics[1] > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER
                .$metrics
                .accept
                .inc_by($self.$metrics[1]);
            $self.$metrics[1] = 0;
        }
    }};
}

/// The buffered metrics counters for raft message.
#[derive(Debug, Default, Clone)]
pub struct RaftSendMessageMetrics {
    pub append: SendStatus,
    pub append_resp: SendStatus,
    pub prevote: SendStatus,
    pub prevote_resp: SendStatus,
    pub vote: SendStatus,
    pub vote_resp: SendStatus,
    pub snapshot: SendStatus,
    pub heartbeat: SendStatus,
    pub heartbeat_resp: SendStatus,
    pub transfer_leader: SendStatus,
    pub timeout_now: SendStatus,
    pub read_index: SendStatus,
    pub read_index_resp: SendStatus,
}

impl RaftSendMessageMetrics {
    pub fn add(&mut self, msg_type: MessageType, success: bool) {
        let i = success as usize;
        match msg_type {
            MessageType::MsgAppend => self.append[i] += 1,
            MessageType::MsgAppendResponse => self.append_resp[i] += 1,
            MessageType::MsgRequestPreVote => self.prevote[i] += 1,
            MessageType::MsgRequestPreVoteResponse => self.prevote_resp[i] += 1,
            MessageType::MsgRequestVote => self.vote[i] += 1,
            MessageType::MsgRequestVoteResponse => self.vote_resp[i] += 1,
            MessageType::MsgSnapshot => self.snapshot[i] += 1,
            MessageType::MsgHeartbeat => self.heartbeat[i] += 1,
            MessageType::MsgHeartbeatResponse => self.heartbeat_resp[i] += 1,
            MessageType::MsgTransferLeader => self.transfer_leader[i] += 1,
            MessageType::MsgReadIndex => self.read_index[i] += 1,
            MessageType::MsgReadIndexResp => self.read_index_resp[i] += 1,
            MessageType::MsgTimeoutNow => self.timeout_now[i] += 1,
            // We do not care about these message types for metrics.
            // Explicitly declare them so when we add new message types we are forced to
            // decide.
            MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgPropose
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum => {}
        }
    }
    /// Flushes all metrics
    pub fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        flush_send_status!(append, self);
        flush_send_status!(append_resp, self);
        flush_send_status!(prevote, self);
        flush_send_status!(prevote_resp, self);
        flush_send_status!(vote, self);
        flush_send_status!(vote_resp, self);
        flush_send_status!(snapshot, self);
        flush_send_status!(heartbeat, self);
        flush_send_status!(heartbeat_resp, self);
        flush_send_status!(transfer_leader, self);
        flush_send_status!(timeout_now, self);
        flush_send_status!(read_index, self);
        flush_send_status!(read_index_resp, self);
    }
}

#[derive(Debug, Default, Clone)]
pub struct RaftMessageDropMetrics {
    pub mismatch_store_id: u64,
    pub mismatch_region_epoch: u64,
    pub stale_msg: u64,
    pub region_overlap: u64,
    pub region_no_peer: u64,
    pub region_tombstone_peer: u64,
    pub region_nonexistent: u64,
    pub applying_snap: u64,
    pub disk_full: u64,
}

impl RaftMessageDropMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .mismatch_store_id
                .inc_by(self.mismatch_store_id);
            self.mismatch_store_id = 0;
        }
        if self.mismatch_region_epoch > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .mismatch_region_epoch
                .inc_by(self.mismatch_region_epoch);
            self.mismatch_region_epoch = 0;
        }
        if self.stale_msg > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .stale_msg
                .inc_by(self.stale_msg);
            self.stale_msg = 0;
        }
        if self.region_overlap > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .region_overlap
                .inc_by(self.region_overlap);
            self.region_overlap = 0;
        }
        if self.region_no_peer > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .region_no_peer
                .inc_by(self.region_no_peer);
            self.region_no_peer = 0;
        }
        if self.region_tombstone_peer > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .region_tombstone_peer
                .inc_by(self.region_tombstone_peer);
            self.region_tombstone_peer = 0;
        }
        if self.region_nonexistent > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .region_nonexistent
                .inc_by(self.region_nonexistent);
            self.region_nonexistent = 0;
        }
        if self.applying_snap > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .applying_snap
                .inc_by(self.applying_snap);
            self.applying_snap = 0;
        }
        if self.disk_full > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER
                .disk_full
                .inc_by(self.disk_full);
            self.disk_full = 0;
        }
    }
}

/// The buffered metrics counters for raft propose.
#[derive(Clone)]
pub struct RaftProposeMetrics {
    pub all: u64,
    pub local_read: u64,
    pub read_index: u64,
    pub unsafe_read_index: u64,
    pub dropped_read_index: u64,
    pub normal: u64,
    pub batch: usize,
    pub transfer_leader: u64,
    pub conf_change: u64,
    pub request_wait_time: LocalHistogram,
}

impl Default for RaftProposeMetrics {
    fn default() -> RaftProposeMetrics {
        RaftProposeMetrics {
            all: 0,
            local_read: 0,
            read_index: 0,
            unsafe_read_index: 0,
            normal: 0,
            transfer_leader: 0,
            conf_change: 0,
            batch: 0,
            dropped_read_index: 0,
            request_wait_time: REQUEST_WAIT_TIME_HISTOGRAM.local(),
        }
    }
}

impl RaftProposeMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.all > 0 {
            PEER_PROPOSAL_COUNTER.all.inc_by(self.all);
            self.all = 0;
        }
        if self.local_read > 0 {
            PEER_PROPOSAL_COUNTER.local_read.inc_by(self.local_read);
            self.local_read = 0;
        }
        if self.read_index > 0 {
            PEER_PROPOSAL_COUNTER.read_index.inc_by(self.read_index);
            self.read_index = 0;
        }
        if self.unsafe_read_index > 0 {
            PEER_PROPOSAL_COUNTER
                .unsafe_read_index
                .inc_by(self.unsafe_read_index);
            self.unsafe_read_index = 0;
        }
        if self.dropped_read_index > 0 {
            PEER_PROPOSAL_COUNTER
                .dropped_read_index
                .inc_by(self.dropped_read_index);
            self.dropped_read_index = 0;
        }
        if self.normal > 0 {
            PEER_PROPOSAL_COUNTER.normal.inc_by(self.normal);
            self.normal = 0;
        }
        if self.transfer_leader > 0 {
            PEER_PROPOSAL_COUNTER
                .transfer_leader
                .inc_by(self.transfer_leader);
            self.transfer_leader = 0;
        }
        if self.conf_change > 0 {
            PEER_PROPOSAL_COUNTER.conf_change.inc_by(self.conf_change);
            self.conf_change = 0;
        }
        if self.batch > 0 {
            PEER_PROPOSAL_COUNTER.batch.inc_by(self.batch as u64);
            self.batch = 0;
        }
        self.request_wait_time.flush();
    }
}

/// The buffered metrics counter for invalid propose
#[derive(Clone, Default)]
pub struct RaftInvalidProposeMetrics {
    pub mismatch_store_id: u64,
    pub region_not_found: u64,
    pub not_leader: u64,
    pub mismatch_peer_id: u64,
    pub stale_command: u64,
    pub epoch_not_match: u64,
    pub read_index_no_leader: u64,
    pub region_not_initialized: u64,
    pub is_applying_snapshot: u64,
}

impl RaftInvalidProposeMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .mismatch_store_id
                .inc_by(self.mismatch_store_id);
            self.mismatch_store_id = 0;
        }
        if self.region_not_found > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .region_not_found
                .inc_by(self.region_not_found);
            self.region_not_found = 0;
        }
        if self.not_leader > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .not_leader
                .inc_by(self.not_leader);
            self.not_leader = 0;
        }
        if self.mismatch_peer_id > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .mismatch_peer_id
                .inc_by(self.mismatch_peer_id);
            self.mismatch_peer_id = 0;
        }
        if self.stale_command > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .stale_command
                .inc_by(self.stale_command);
            self.stale_command = 0;
        }
        if self.epoch_not_match > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .epoch_not_match
                .inc_by(self.epoch_not_match);
            self.epoch_not_match = 0;
        }
        if self.read_index_no_leader > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .read_index_no_leader
                .inc_by(self.read_index_no_leader);
            self.read_index_no_leader = 0;
        }
        if self.region_not_initialized > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .region_not_initialized
                .inc_by(self.region_not_initialized);
            self.region_not_initialized = 0;
        }
        if self.is_applying_snapshot > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER
                .is_applying_snapshot
                .inc_by(self.is_applying_snapshot);
            self.is_applying_snapshot = 0;
        }
    }
}

#[derive(Clone, Default)]
pub struct RaftLogGcSkippedMetrics {
    pub reserve_log: u64,
    pub threshold_limit: u64,
    pub compact_idx_too_small: u64,
}

impl RaftLogGcSkippedMetrics {
    fn flush(&mut self) {
        if self.reserve_log > 0 {
            RAFT_LOG_GC_SKIPPED.reserve_log.inc_by(self.reserve_log);
            self.reserve_log = 0;
        }
        if self.threshold_limit > 0 {
            RAFT_LOG_GC_SKIPPED
                .threshold_limit
                .inc_by(self.threshold_limit);
            self.threshold_limit = 0;
        }
        if self.compact_idx_too_small > 0 {
            RAFT_LOG_GC_SKIPPED
                .compact_idx_too_small
                .inc_by(self.compact_idx_too_small);
            self.compact_idx_too_small = 0;
        }
    }
}

/// The buffered metrics counters for raft.
#[derive(Clone)]
pub struct RaftMetrics {
    pub store_time: LocalHistogram,
    pub ready: RaftReadyMetrics,
    pub send_message: RaftSendMessageMetrics,
    pub message_dropped: RaftMessageDropMetrics,
    pub propose: RaftProposeMetrics,
    pub process_ready: LocalHistogram,
    pub commit_log: LocalHistogram,
    pub leader_missing: Arc<Mutex<HashSet<u64>>>,
    pub invalid_proposal: RaftInvalidProposeMetrics,
    pub write_block_wait: LocalHistogram,
    pub waterfall_metrics: bool,
    pub wf_batch_wait: LocalHistogram,
    pub wf_send_to_queue: LocalHistogram,
    pub wf_persist_log: LocalHistogram,
    pub wf_commit_log: LocalHistogram,
    pub wf_commit_not_persist_log: LocalHistogram,
    pub raft_log_gc_skipped: RaftLogGcSkippedMetrics,
}

impl RaftMetrics {
    pub fn new(waterfall_metrics: bool) -> Self {
        Self {
            store_time: STORE_TIME_HISTOGRAM.local(),
            ready: Default::default(),
            send_message: Default::default(),
            message_dropped: Default::default(),
            propose: Default::default(),
            process_ready: PEER_RAFT_PROCESS_DURATION
                .with_label_values(&["ready"])
                .local(),
            commit_log: PEER_COMMIT_LOG_HISTOGRAM.local(),
            leader_missing: Arc::default(),
            invalid_proposal: Default::default(),
            write_block_wait: STORE_WRITE_MSG_BLOCK_WAIT_DURATION_HISTOGRAM.local(),
            waterfall_metrics,
            wf_batch_wait: STORE_WF_BATCH_WAIT_DURATION_HISTOGRAM.local(),
            wf_send_to_queue: STORE_WF_SEND_TO_QUEUE_DURATION_HISTOGRAM.local(),
            wf_persist_log: STORE_WF_PERSIST_LOG_DURATION_HISTOGRAM.local(),
            wf_commit_log: STORE_WF_COMMIT_LOG_DURATION_HISTOGRAM.local(),
            wf_commit_not_persist_log: STORE_WF_COMMIT_NOT_PERSIST_LOG_DURATION_HISTOGRAM.local(),
            raft_log_gc_skipped: RaftLogGcSkippedMetrics::default(),
        }
    }

    /// Flushs all metrics
    pub fn flush(&mut self) {
        self.store_time.flush();
        self.ready.flush();
        self.send_message.flush();
        self.propose.flush();
        self.process_ready.flush();
        self.commit_log.flush();
        self.message_dropped.flush();
        self.invalid_proposal.flush();
        self.write_block_wait.flush();
        self.raft_log_gc_skipped.flush();
        if self.waterfall_metrics {
            self.wf_batch_wait.flush();
            self.wf_send_to_queue.flush();
            self.wf_persist_log.flush();
            self.wf_commit_log.flush();
            self.wf_commit_not_persist_log.flush();
        }
        let mut missing = self.leader_missing.lock().unwrap();
        LEADER_MISSING.set(missing.len() as i64);
        missing.clear();
    }
}

pub struct StoreWriteMetrics {
    pub task_wait: LocalHistogram,
    pub waterfall_metrics: bool,
    pub wf_before_write: LocalHistogram,
    pub wf_kvdb_end: LocalHistogram,
    pub wf_write_end: LocalHistogram,
}

impl StoreWriteMetrics {
    pub fn new(waterfall_metrics: bool) -> Self {
        Self {
            task_wait: STORE_WRITE_TASK_WAIT_DURATION_HISTOGRAM.local(),
            waterfall_metrics,
            wf_before_write: STORE_WF_BEFORE_WRITE_DURATION_HISTOGRAM.local(),
            wf_kvdb_end: STORE_WF_WRITE_KVDB_END_DURATION_HISTOGRAM.local(),
            wf_write_end: STORE_WF_WRITE_END_DURATION_HISTOGRAM.local(),
        }
    }

    pub fn flush(&mut self) {
        self.task_wait.flush();
        if self.waterfall_metrics {
            self.wf_before_write.flush();
            self.wf_kvdb_end.flush();
            self.wf_write_end.flush();
        }
    }
}
