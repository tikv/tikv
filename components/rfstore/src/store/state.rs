// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::peer_storage::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use super::*;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RaftApplyState {
    pub(crate) applied_index: u64,
    pub(crate) truncated_index: u64,
}

impl Default for RaftApplyState {
    fn default() -> Self {
        Self {
            applied_index: RAFT_INIT_LOG_INDEX,
            truncated_index: RAFT_INIT_LOG_INDEX,
        }
    }
}

impl RaftApplyState {
    pub(crate) fn new(applied_index: u64, truncated_index: u64) -> Self {
        Self {
            applied_index,
            truncated_index,
        }
    }

    pub(crate) fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.applied_index);
        buf.put_u64_le(self.truncated_index);
        buf.freeze()
    }

    pub(crate) fn unmarshal(&mut self, data: &[u8]) {
        self.applied_index = LittleEndian::read_u64(data);
        self.truncated_index = LittleEndian::read_u64(&data[8..]);
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
