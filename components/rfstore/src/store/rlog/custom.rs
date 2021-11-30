// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use byteorder::LittleEndian;
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
pub const TYPE_DELETE_RANGE: CustomRaftlogType = 11;

// CustomRaftLog is the raft log format for unistore to store Prewrite/Commit/PessimisticLock.
//  | flag(1) | type(1) | version(2) | header(40) | entries
//
// It reduces the cost of marshal/unmarshal and avoid DB lookup during apply.
#[derive(Debug)]
pub(crate) struct CustomRaftLog {
    pub(crate) header: CustomHeader,
    pub(crate) data: Bytes,
}

impl Deref for CustomRaftLog {
    type Target = CustomHeader;

    fn deref(&self) -> &Self::Target {
        return &self.header
    }
}


const CUSTOM_HEADER_SIZE: usize = std::mem::size_of::<CustomHeader>();

pub(crate) struct CustomHeader {
    pub(crate) region_id: u64,
    pub(crate) epoch: Epoch,
    pub(crate) peer_id: u64,
    pub(crate) store_id: u64,
}

impl CustomHeader {
    pub(crate) fn new_from_data(data: &[u8]) -> Self {
        let region_id = LittleEndian::read_u64(data);
        let data = &data[8..];
        let ver = LittleEndian::read_u32(data);
        let data = &data[4..];
        let conf_ver = LittleEndian::read_u32(data);
        let data = &data[4..];
        let peer_id = LittleEndian::read_u64(data);
        let data = &data[8..];
        let store_id = LittleEndian::read_u64(data);
        Self {
            region_id,
            epoch: Epoch {
                ver,
                conf_ver,
            },
            peer_id,
            store_id
        }
    }

    pub(crate) fn marshal(&self) -> Bytes {
        todo!()
    }
}

impl CustomRaftLog {
    pub(crate) fn new_from_data(data: Bytes) -> Self {
        let header = CustomHeader::new_from_data(&data[4..]);
        Self {
            header,
            data,
        }
    }

    pub(crate) fn get_type(&self) -> CustomRaftlogType {
        self.data[1] as CustomRaftlogType
    }

    // F: (key, val)
    pub(crate) fn iterate_lock<F>(&self, f: F)
    where
        F: FnMut(Bytes, Bytes),
    {
        let mut i = 4 + CUSTOM_HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i+key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i+val_len];
            i += val_len;
            f(key, val)
        }
    }

    // F: (key, val, commit_ts)
    pub(crate) fn iterate_commit<F>(&self, f: F)
    where
        F: FnMut(&[u8], &[u8], u64),
    {
        let mut i = 4 + CUSTOM_HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i+key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i+val_len];
            i += val_len;
            let commit_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            f(key, val, commit_ts)
        }
    }

    // F: (key, start_ts, delete_lock)
    pub(crate) fn iterate_rollback<F>(&self, f: F)
    where
        F: FnMut(&[u8], u64, bool),
    {
        let mut i = 4 + CUSTOM_HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i+key_len];
            i += key_len;
            let start_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            let del = self.data[i];
            i += 1;
            f(key, start_ts, del > 0)
        }
    }

    pub(crate) fn iterate_keys_only<F>(&self, f: F)
    where
        F: FnMut(&[u8]),
    {
        let mut i = 4 + CUSTOM_HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i+key_len];
            i += key_len;
            f(key)
        }
    }

    pub(crate) fn get_change_set(&self) -> crate::Result<kvenginepb::ChangeSet> {
        let mut cs = kvenginepb::ChangeSet::new();
        cs.merge_from_bytes(&self.data[4+CUSTOM_HEADER_SIZE..])?;
        Ok(cs)
    }

    pub(crate) fn get_delete_range(&self) -> (Bytes, Bytes) {
        let mut i = 4 + CUSTOM_HEADER_SIZE;
        let start_key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
        i += 2;
        let start_key = self.data.slice(i..i+start_key_len);
        let end_key = self.data.slice(i+start_key_len..);
        (start_key, end_key)
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
