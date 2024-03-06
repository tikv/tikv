// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::{Arc, Mutex};

use collections::HashSet;
use prometheus::local::LocalHistogram;
use raft::eraftpb::MessageType;
use tikv_util::time::{Duration, Instant};
use tracker::{Tracker, TrackerToken, GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN};

use super::metrics::*;

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

macro_rules! set_send_status {
    ($metrics:expr, $success:ident) => {{
        if $success {
            $metrics.accept.inc();
        } else {
            $metrics.drop.inc();
        }
    }};
}

pub struct RaftSendMessageMetrics(RaftSentMessageCounterVec);

impl Default for RaftSendMessageMetrics {
    fn default() -> Self {
        Self(RaftSentMessageCounterVec::from(
            &STORE_RAFT_SENT_MESSAGE_COUNTER_VEC,
        ))
    }
}

impl RaftSendMessageMetrics {
    pub fn add(&mut self, msg_type: MessageType, success: bool) {
        match msg_type {
            MessageType::MsgAppend => set_send_status!(self.0.append, success),
            MessageType::MsgAppendResponse => set_send_status!(self.0.append_resp, success),
            MessageType::MsgRequestPreVote => set_send_status!(self.0.prevote, success),
            MessageType::MsgRequestPreVoteResponse => {
                set_send_status!(self.0.prevote_resp, success)
            }
            MessageType::MsgRequestVote => set_send_status!(self.0.vote, success),
            MessageType::MsgRequestVoteResponse => set_send_status!(self.0.vote_resp, success),
            MessageType::MsgSnapshot => set_send_status!(self.0.snapshot, success),
            MessageType::MsgHeartbeat => set_send_status!(self.0.heartbeat, success),
            MessageType::MsgHeartbeatResponse => set_send_status!(self.0.heartbeat_resp, success),
            MessageType::MsgTransferLeader => set_send_status!(self.0.transfer_leader, success),
            MessageType::MsgReadIndex => set_send_status!(self.0.read_index, success),
            MessageType::MsgReadIndexResp => set_send_status!(self.0.read_index_resp, success),
            MessageType::MsgTimeoutNow => set_send_status!(self.0.timeout_now, success),
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

    pub fn flush(&mut self) {
        self.0.flush();
    }
}

/// The buffered metrics counters for raft.
pub struct RaftMetrics {
    // local counter
    pub ready: RaftReadyCounterVec,
    pub send_message: RaftSendMessageMetrics,
    pub message_dropped: RaftDroppedMessageCounterVec,
    pub propose: RaftProposalCounterVec,
    pub invalid_proposal: RaftInvalidProposalCounterVec,
    pub raft_log_gc_skipped: RaftLogGcSkippedCounterVec,

    // local histogram
    pub store_time: LocalHistogram,
    pub propose_wait_time: LocalHistogram,
    pub process_ready: LocalHistogram,
    pub event_time: RaftEventDurationVec,
    pub peer_msg_len: LocalHistogram,
    pub commit_log: LocalHistogram,
    pub write_block_wait: LocalHistogram,
    pub propose_log_size: LocalHistogram,

    // waterfall metrics
    pub waterfall_metrics: bool,
    pub wf_batch_wait: LocalHistogram,
    pub wf_send_to_queue: LocalHistogram,
    pub wf_send_proposal: LocalHistogram,
    pub wf_persist_log: LocalHistogram,
    pub wf_commit_log: LocalHistogram,
    pub wf_commit_not_persist_log: LocalHistogram,

    pub leader_missing: Arc<Mutex<HashSet<u64>>>,

    last_flush_time: Instant,
}

impl RaftMetrics {
    pub fn new(waterfall_metrics: bool) -> Self {
        Self {
            ready: RaftReadyCounterVec::from(&STORE_RAFT_READY_COUNTER_VEC),
            send_message: RaftSendMessageMetrics::default(),
            message_dropped: RaftDroppedMessageCounterVec::from(
                &STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC,
            ),
            propose: RaftProposalCounterVec::from(&PEER_PROPOSAL_COUNTER_VEC),
            invalid_proposal: RaftInvalidProposalCounterVec::from(
                &RAFT_INVALID_PROPOSAL_COUNTER_VEC,
            ),
            raft_log_gc_skipped: RaftLogGcSkippedCounterVec::from(&RAFT_LOG_GC_SKIPPED_VEC),
            store_time: STORE_TIME_HISTOGRAM.local(),
            propose_wait_time: REQUEST_WAIT_TIME_HISTOGRAM.local(),
            process_ready: PEER_RAFT_PROCESS_DURATION
                .with_label_values(&["ready"])
                .local(),
            event_time: RaftEventDurationVec::from(&RAFT_EVENT_DURATION_VEC),
            peer_msg_len: PEER_MSG_LEN.local(),
            commit_log: PEER_COMMIT_LOG_HISTOGRAM.local(),
            write_block_wait: STORE_WRITE_MSG_BLOCK_WAIT_DURATION_HISTOGRAM.local(),
            propose_log_size: PEER_PROPOSE_LOG_SIZE_HISTOGRAM.local(),
            waterfall_metrics,
            wf_batch_wait: STORE_WF_BATCH_WAIT_DURATION_HISTOGRAM.local(),
            wf_send_to_queue: STORE_WF_SEND_TO_QUEUE_DURATION_HISTOGRAM.local(),
            wf_send_proposal: STORE_WF_SEND_PROPOSAL_DURATION_HISTOGRAM.local(),
            wf_persist_log: STORE_WF_PERSIST_LOG_DURATION_HISTOGRAM.local(),
            wf_commit_log: STORE_WF_COMMIT_LOG_DURATION_HISTOGRAM.local(),
            wf_commit_not_persist_log: STORE_WF_COMMIT_NOT_PERSIST_LOG_DURATION_HISTOGRAM.local(),
            leader_missing: Arc::default(),
            last_flush_time: Instant::now_coarse(),
        }
    }

    /// Flushes all metrics
    pub fn maybe_flush(&mut self) {
        if self.last_flush_time.saturating_elapsed() < Duration::from_millis(METRICS_FLUSH_INTERVAL)
        {
            return;
        }
        self.last_flush_time = Instant::now_coarse();

        self.ready.flush();
        self.send_message.flush();
        self.message_dropped.flush();
        self.propose.flush();
        self.invalid_proposal.flush();
        self.raft_log_gc_skipped.flush();

        self.store_time.flush();
        self.propose_wait_time.flush();
        self.process_ready.flush();
        self.event_time.flush();
        self.peer_msg_len.flush();
        self.commit_log.flush();
        self.write_block_wait.flush();
        self.propose_log_size.flush();

        if self.waterfall_metrics {
            self.wf_batch_wait.flush();
            self.wf_send_to_queue.flush();
            self.wf_send_proposal.flush();
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

/// Tracker for the durations of a raftstore request.
/// If a global tracker is not available, it will fallback to an Instant.
#[derive(Debug, Clone, Copy)]
pub struct TimeTracker {
    token: TrackerToken,
    start: std::time::Instant,
}

impl Default for TimeTracker {
    #[inline]
    fn default() -> Self {
        let token = tracker::get_tls_tracker_token();
        let start = std::time::Instant::now();
        let tracker = TimeTracker { token, start };
        if token == INVALID_TRACKER_TOKEN {
            return tracker;
        }

        GLOBAL_TRACKERS.with_tracker(token, |tracker| {
            tracker.metrics.write_instant = Some(start);
        });
        tracker
    }
}

impl TimeTracker {
    #[inline]
    pub fn as_tracker_token(&self) -> Option<TrackerToken> {
        if self.token == INVALID_TRACKER_TOKEN {
            None
        } else {
            Some(self.token)
        }
    }

    #[inline]
    pub fn observe(
        &self,
        now: std::time::Instant,
        local_metric: &LocalHistogram,
        tracker_metric: impl FnOnce(&mut Tracker) -> &mut u64,
    ) {
        let dur = now.saturating_duration_since(self.start);
        local_metric.observe(dur.as_secs_f64());
        if self.token == INVALID_TRACKER_TOKEN {
            return;
        }
        GLOBAL_TRACKERS.with_tracker(self.token, |tracker| {
            let metric = tracker_metric(tracker);
            if *metric == 0 {
                *metric = dur.as_nanos() as u64;
            }
        });
    }

    #[inline]
    pub fn reset(&mut self, start: std::time::Instant) {
        self.start = start;
    }
}
