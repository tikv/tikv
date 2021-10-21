// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::peer_storage::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use super::*;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftApplyState {
    pub(crate) applied_index: u64,
    pub(crate) applied_index_term: u64,
    pub(crate) truncated_index: u64,
    pub(crate) truncated_index_term: u64,
}

impl Default for RaftApplyState {
    fn default() -> Self {
        Self {
            applied_index: RAFT_INIT_LOG_INDEX,
            applied_index_term: RAFT_INIT_LOG_TERM,
            truncated_index: RAFT_INIT_LOG_INDEX,
            truncated_index_term: RAFT_INIT_LOG_TERM,
        }
    }
}

impl RaftApplyState {
    pub(crate) fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u64_le(self.applied_index);
        buf.put_u64_le(self.applied_index_term);
        buf.put_u64_le(self.truncated_index);
        buf.put_u64_le(self.truncated_index_term);
        buf.freeze()
    }

    pub(crate) fn unmarshal(&mut self, data: &[u8]) {
        self.applied_index = LittleEndian::read_u64(data);
        self.applied_index_term = LittleEndian::read_u64(&data[8..]);
        self.truncated_index = LittleEndian::read_u64(&data[16..]);
        self.truncated_index_term = LittleEndian::read_u64(&data[24..]);
    }
}

#[derive(Debug, Default, Clone, Copy)]
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
}
