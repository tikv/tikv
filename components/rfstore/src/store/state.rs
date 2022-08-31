// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use raft_proto::eraftpb;

use super::peer_storage::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use crate::store::TERM_KEY;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftApplyState {
    pub(crate) applied_index: u64,
    pub(crate) applied_index_term: u64,
}

impl Default for RaftApplyState {
    fn default() -> Self {
        Self {
            applied_index: RAFT_INIT_LOG_INDEX,
            applied_index_term: RAFT_INIT_LOG_TERM,
        }
    }
}

impl RaftApplyState {
    pub(crate) fn new(applied_index: u64, applied_index_term: u64) -> Self {
        Self {
            applied_index,
            applied_index_term,
        }
    }

    pub(crate) fn from_snapshot(snap: &kvenginepb::Snapshot) -> Self {
        let index = snap.get_data_sequence();
        let term_val = kvengine::get_shard_property(TERM_KEY, snap.get_properties()).unwrap();
        let term = term_val.as_slice().get_u64_le();
        Self::new(index, term)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct RaftState {
    pub(crate) term: u64,
    pub(crate) vote: u64,
    pub(crate) commit: u64,
    pub(crate) last_index: u64,
    /// `ShardMeta` is changed by preprocessed committed entries. When recovering, we can't replay
    /// entries from applied_index to committed_index directly, because some committed entries may
    /// not be preprocessed, so we record `last_preprocessed_index` to replay to it.
    pub(crate) last_preprocessed_index: u64,
}

impl RaftState {
    pub(crate) fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u64_le(self.term);
        buf.put_u64_le(self.vote);
        buf.put_u64_le(self.commit);
        buf.put_u64_le(self.last_index);
        buf.put_u64_le(self.last_preprocessed_index);
        buf.freeze()
    }

    pub(crate) fn unmarshal(&mut self, mut data: &[u8]) {
        self.term = data.get_u64_le();
        self.vote = data.get_u64_le();
        self.commit = data.get_u64_le();
        self.last_index = data.get_u64_le();
        self.last_preprocessed_index = data.get_u64_le();
    }

    pub(crate) fn get_hard_state(&self) -> eraftpb::HardState {
        let mut hs = eraftpb::HardState::default();
        hs.set_term(self.term);
        hs.set_vote(self.vote);
        hs.set_commit(self.commit);
        hs
    }

    pub(crate) fn set_hard_state(&mut self, hs: &eraftpb::HardState) {
        self.term = hs.get_term();
        self.vote = hs.get_vote();
        self.commit = hs.get_commit();
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftTruncatedState {
    pub(crate) truncated_index: u64,
    pub(crate) truncated_index_term: u64,
}

impl Default for RaftTruncatedState {
    fn default() -> Self {
        Self {
            truncated_index: RAFT_INIT_LOG_INDEX,
            truncated_index_term: RAFT_INIT_LOG_TERM,
        }
    }
}

impl RaftTruncatedState {
    pub(crate) fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.truncated_index_term);
        buf.put_u64_le(self.truncated_index);
        buf.freeze()
    }

    pub(crate) fn unmarshal(&mut self, mut data: &[u8]) {
        self.truncated_index_term = data.get_u64_le();
        self.truncated_index = data.get_u64_le();
    }
}
