// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Bytes, BytesMut};
use kvproto::raft_cmdpb;

use super::*;

type CustomRaftlogType = u8;

pub const CUSTOM_FLAG: u8 = 64;
pub const TYPE_PREWRITE: CustomRaftlogType = 1;
pub const TYPE_COMMIT: CustomRaftlogType = 2;
pub const TYPE_ROLLBACK: CustomRaftlogType = 3;
pub const TYPE_PESSIMISTIC_LOCK: CustomRaftlogType = 4;
pub const TYPE_PESSIMISTIC_ROLLBACK: CustomRaftlogType = 5;
pub const TYPE_PRE_SPLIT: CustomRaftlogType = 6;
pub const TYPE_FLUSH: CustomRaftlogType = 7;
pub const TYPE_COMPACTION: CustomRaftlogType = 8;
pub const TYPE_SPLIT_FILES: CustomRaftlogType = 9;
pub const TYPE_NEX_MEM_TABLE_SIZE: CustomRaftlogType = 10;

// CustomRaftLog is the raft log format for unistore to store Prewrite/Commit/PessimisticLock.
//  | flag(1) | type(1) | version(2) | header(40) | entries
//
// It reduces the cost of marshal/unmarshal and avoid DB lookup during apply.
pub(crate) struct CustomRaftLog {
    pub(crate) header: CustomHeader,
    pub(crate) data: Bytes,
}

pub(crate) struct CustomHeader {
    pub(crate) region_id: u64,
    pub(crate) epoch: Epoch,
    pub(crate) peer_id: u64,
    pub(crate) store_id: u64,
}

impl CustomHeader {
    pub(crate) fn marshal(&self) -> Bytes {
        todo!()
    }
}

impl CustomRaftLog {
    // F: (key, val)
    pub(crate) fn iterate_lock<F>(f: F)
    where
        F: FnMut(&[u8], &[u8]),
    {
        todo!()
    }

    // F: (key, val, commit_ts)
    pub(crate) fn iterate_commit<F>(f: F)
    where
        F: FnMut(&[u8], &[u8], u64),
    {
        todo!()
    }

    // F: (key, start_ts, delete_lock)
    pub(crate) fn iterate_rollback<F>(f: F)
    where
        F: FnMut(&[u8], u64, bool),
    {
        todo!()
    }

    pub(crate) fn iterate_keys_only<F>(f: F)
    where
        F: FnMut(&[u8]),
    {
        todo!()
    }

    pub(crate) fn get_change_set() -> Option<kvenginepb::ChangeSet> {
        todo!()
    }
}

impl RaftLog for CustomRaftLog {
    fn region_id(&self) -> u64 {
        todo!()
    }

    fn epoch(&self) -> Epoch {
        todo!()
    }

    fn peer_id(&self) -> u64 {
        todo!()
    }

    fn store_id(&self) -> u64 {
        todo!()
    }

    fn term(&self) -> u64 {
        todo!()
    }

    fn marshal(&self) -> Bytes {
        todo!()
    }

    fn get_raft_cmd_request(&self) -> Option<raft_cmdpb::RaftCmdRequest> {
        todo!()
    }
}

pub struct CustomBuilder {
    buf: BytesMut,
    cnt: i32,
}

impl CustomBuilder {
    pub(crate) fn new() -> Self {
        todo!()
    }

    pub(crate) fn append_lock(&mut self, key: &[u8], val: &[u8]) {
        todo!()
    }

    pub(crate) fn append_commit(&mut self, key: &[u8], val: &[u8], commit_ts: u64) {
        todo!()
    }

    pub(crate) fn append_rollback(&mut self, key: &[u8], start_ts: u64, delete_lock: bool) {
        todo!()
    }

    pub(crate) fn append_key_only(&mut self, key: &[u8]) {
        todo!()
    }

    pub(crate) fn set_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        todo!()
    }

    pub(crate) fn set_type(&mut self, tp: CustomRaftlogType) {
        todo!()
    }

    pub(crate) fn get_type(&self) -> CustomRaftlogType {
        todo!()
    }

    pub(crate) fn build(&self) -> CustomRaftLog {
        todo!()
    }

    pub(crate) fn len(&self) -> usize {
        self.cnt as usize
    }
}

pub fn is_engine_meta_log(data: &[u8]) -> bool {
    todo!()
}

pub fn is_background_change_set(data: &[u8]) -> bool {
    todo!()
}

pub fn is_pre_split_log(data: &[u8]) -> bool {
    todo!()
}

pub fn try_get_split(data: &[u8]) -> Option<raft_cmdpb::BatchSplitRequest> {
    todo!()
}
