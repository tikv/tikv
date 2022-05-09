// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
use raft_proto::eraftpb;

use super::peer_storage::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};

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
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct RaftState {
    pub(crate) term: u64,
    pub(crate) vote: u64,
    pub(crate) commit: u64,
    pub(crate) last_index: u64,
}

impl RaftState {
    pub(crate) fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u64_le(self.term);
        buf.put_u64_le(self.vote);
        buf.put_u64_le(self.commit);
        buf.put_u64_le(self.last_index);
        buf.freeze()
    }

    pub(crate) fn unmarshal(&mut self, data: &[u8]) {
        self.term = LittleEndian::read_u64(data);
        self.vote = LittleEndian::read_u64(&data[8..]);
        self.commit = LittleEndian::read_u64(&data[16..]);
        self.last_index = LittleEndian::read_u64(&data[24..]);
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
