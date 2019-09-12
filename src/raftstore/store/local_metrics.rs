// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::local::LocalHistogram;
use std::sync::{Arc, Mutex};

use tikv_util::collections::HashSet;

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
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["message"])
                .inc_by(self.message as i64);
            self.message = 0;
        }
        if self.commit > 0 {
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["commit"])
                .inc_by(self.commit as i64);
            self.commit = 0;
        }
        if self.append > 0 {
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["append"])
                .inc_by(self.append as i64);
            self.append = 0;
        }
        if self.snapshot > 0 {
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["snapshot"])
                .inc_by(self.snapshot as i64);
            self.snapshot = 0;
        }
        if self.pending_region > 0 {
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["pending_region"])
                .inc_by(self.pending_region as i64);
            self.pending_region = 0;
        }
        if self.has_ready_region > 0 {
            STORE_RAFT_READY_COUNTER_VEC
                .with_label_values(&["has_ready_region"])
                .inc_by(self.has_ready_region as i64);
            self.has_ready_region = 0;
        }
    }
}

/// The buffered metrics counters for raft message.
#[derive(Debug, Default, Clone)]
pub struct RaftMessageMetrics {
    pub append: u64,
    pub append_resp: u64,
    pub prevote: u64,
    pub prevote_resp: u64,
    pub vote: u64,
    pub vote_resp: u64,
    pub snapshot: u64,
    pub request_snapshot: u64,
    pub heartbeat: u64,
    pub heartbeat_resp: u64,
    pub transfer_leader: u64,
    pub timeout_now: u64,
}

impl RaftMessageMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.append > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["append"])
                .inc_by(self.append as i64);
            self.append = 0;
        }
        if self.append_resp > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["append_resp"])
                .inc_by(self.append_resp as i64);
            self.append_resp = 0;
        }
        if self.prevote > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["prevote"])
                .inc_by(self.prevote as i64);
            self.prevote = 0;
        }
        if self.prevote_resp > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["prevote_resp"])
                .inc_by(self.prevote_resp as i64);
            self.prevote_resp = 0;
        }
        if self.vote > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["vote"])
                .inc_by(self.vote as i64);
            self.vote = 0;
        }
        if self.vote_resp > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["vote_resp"])
                .inc_by(self.vote_resp as i64);
            self.vote_resp = 0;
        }
        if self.snapshot > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["snapshot"])
                .inc_by(self.snapshot as i64);
            self.snapshot = 0;
        }
        if self.request_snapshot > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["request_snapshot"])
                .inc_by(self.request_snapshot as i64);
            self.request_snapshot = 0;
        }
        if self.heartbeat > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["heartbeat"])
                .inc_by(self.heartbeat as i64);
            self.heartbeat = 0;
        }
        if self.heartbeat_resp > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["heartbeat_resp"])
                .inc_by(self.heartbeat_resp as i64);
            self.heartbeat_resp = 0;
        }
        if self.transfer_leader > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["transfer_leader"])
                .inc_by(self.transfer_leader as i64);
            self.transfer_leader = 0;
        }
        if self.timeout_now > 0 {
            STORE_RAFT_SENT_MESSAGE_COUNTER_VEC
                .with_label_values(&["timeout_now"])
                .inc_by(self.timeout_now as i64);
            self.timeout_now = 0;
        }
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
}

impl RaftMessageDropMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["mismatch_store_id"])
                .inc_by(self.mismatch_store_id as i64);
            self.mismatch_store_id = 0;
        }
        if self.mismatch_region_epoch > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["mismatch_region_epoch"])
                .inc_by(self.mismatch_region_epoch as i64);
            self.mismatch_region_epoch = 0;
        }
        if self.stale_msg > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["stale_msg"])
                .inc_by(self.stale_msg as i64);
            self.stale_msg = 0;
        }
        if self.region_overlap > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["region_overlap"])
                .inc_by(self.region_overlap as i64);
            self.region_overlap = 0;
        }
        if self.region_no_peer > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["region_no_peer"])
                .inc_by(self.region_no_peer as i64);
            self.region_no_peer = 0;
        }
        if self.region_tombstone_peer > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["region_tombstone_peer"])
                .inc_by(self.region_tombstone_peer as i64);
            self.region_tombstone_peer = 0;
        }
        if self.region_nonexistent > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["region_nonexistent"])
                .inc_by(self.region_nonexistent as i64);
            self.region_nonexistent = 0;
        }
        if self.applying_snap > 0 {
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["applying_snap"])
                .inc_by(self.applying_snap as i64);
            self.applying_snap = 0;
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
    pub normal: u64,
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
            request_wait_time: REQUEST_WAIT_TIME_HISTOGRAM.local(),
        }
    }
}

impl RaftProposeMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        // reset all buffered metrics once they have been added
        if self.all > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["all"])
                .inc_by(self.all as i64);
            self.all = 0;
        }
        if self.local_read > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["local_read"])
                .inc_by(self.local_read as i64);
            self.local_read = 0;
        }
        if self.read_index > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["read_index"])
                .inc_by(self.read_index as i64);
            self.read_index = 0;
        }
        if self.unsafe_read_index > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["unsafe_read_index"])
                .inc_by(self.unsafe_read_index as i64);
            self.unsafe_read_index = 0;
        }
        if self.normal > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["normal"])
                .inc_by(self.normal as i64);
            self.normal = 0;
        }
        if self.transfer_leader > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["transfer_leader"])
                .inc_by(self.transfer_leader as i64);
            self.transfer_leader = 0;
        }
        if self.conf_change > 0 {
            PEER_PROPOSAL_COUNTER_VEC
                .with_label_values(&["conf_change"])
                .inc_by(self.conf_change as i64);
            self.conf_change = 0;
        }
        self.request_wait_time.flush();
    }
}

/// The buffered metrics counter for invalid propose
#[derive(Clone)]
pub struct RaftInvalidProposeMetrics {
    pub mismatch_store_id: u64,
    pub region_not_found: u64,
    pub not_leader: u64,
    pub mismatch_peer_id: u64,
    pub stale_command: u64,
    pub epoch_not_match: u64,
    pub read_index_no_leader: u64,
}

impl Default for RaftInvalidProposeMetrics {
    fn default() -> RaftInvalidProposeMetrics {
        RaftInvalidProposeMetrics {
            mismatch_store_id: 0,
            region_not_found: 0,
            not_leader: 0,
            mismatch_peer_id: 0,
            stale_command: 0,
            epoch_not_match: 0,
            read_index_no_leader: 0,
        }
    }
}

impl RaftInvalidProposeMetrics {
    fn flush(&mut self) {
        if self.mismatch_store_id > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["mismatch_store_id"])
                .inc_by(self.mismatch_store_id as i64);
            self.mismatch_store_id = 0;
        }
        if self.region_not_found > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["region_not_found"])
                .inc_by(self.region_not_found as i64);
            self.region_not_found = 0;
        }
        if self.not_leader > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["not_leader"])
                .inc_by(self.not_leader as i64);
            self.not_leader = 0;
        }
        if self.mismatch_peer_id > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["mismatch_peer_id"])
                .inc_by(self.mismatch_peer_id as i64);
            self.mismatch_peer_id = 0;
        }
        if self.stale_command > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["stale_command"])
                .inc_by(self.stale_command as i64);
            self.stale_command = 0;
        }
        if self.epoch_not_match > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["epoch_not_match"])
                .inc_by(self.epoch_not_match as i64);
            self.epoch_not_match = 0;
        }
        if self.read_index_no_leader > 0 {
            RAFT_INVALID_PROPOSAL_COUNTER_VEC
                .with_label_values(&["read_index_no_leader"])
                .inc_by(self.read_index_no_leader as i64);
            self.read_index_no_leader = 0;
        }
    }
}
/// The buffered metrics counters for raft.
#[derive(Clone)]
pub struct RaftMetrics {
    pub ready: RaftReadyMetrics,
    pub message: RaftMessageMetrics,
    pub message_dropped: RaftMessageDropMetrics,
    pub propose: RaftProposeMetrics,
    pub process_ready: LocalHistogram,
    pub append_log: LocalHistogram,
    pub commit_log: LocalHistogram,
    pub leader_missing: Arc<Mutex<HashSet<u64>>>,
    pub invalid_proposal: RaftInvalidProposeMetrics,
}

impl Default for RaftMetrics {
    fn default() -> RaftMetrics {
        RaftMetrics {
            ready: Default::default(),
            message: Default::default(),
            message_dropped: Default::default(),
            propose: Default::default(),
            process_ready: PEER_RAFT_PROCESS_DURATION
                .with_label_values(&["ready"])
                .local(),
            append_log: PEER_APPEND_LOG_HISTOGRAM.local(),
            commit_log: PEER_COMMIT_LOG_HISTOGRAM.local(),
            leader_missing: Arc::default(),
            invalid_proposal: Default::default(),
        }
    }
}

impl RaftMetrics {
    /// Flushs all metrics
    pub fn flush(&mut self) {
        self.ready.flush();
        self.message.flush();
        self.propose.flush();
        self.process_ready.flush();
        self.append_log.flush();
        self.commit_log.flush();
        self.message_dropped.flush();
        self.invalid_proposal.flush();
        let mut missing = self.leader_missing.lock().unwrap();
        LEADER_MISSING.set(missing.len() as i64);
        missing.clear();
    }
}
