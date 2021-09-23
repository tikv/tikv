// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub type SendStatus = [u64; 2];

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
    pub request_snapshot: SendStatus,
    pub heartbeat: SendStatus,
    pub heartbeat_resp: SendStatus,
    pub transfer_leader: SendStatus,
    pub timeout_now: SendStatus,
    pub read_index: SendStatus,
    pub read_index_resp: SendStatus,
}

impl RaftSendMessageMetrics {
    /// Flushes all metrics
    fn flush(&mut self) {
        todo!()
    }
}
