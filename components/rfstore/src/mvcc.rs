// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
use std::ops::Deref;

pub const WRITE_CF: usize = 0;
pub const LOCK_CF: usize = 1;
pub const EXTRA_CF: usize = 2;

pub const LOCK_HEADER_SIZE: usize = std::mem::size_of::<LockHeader>();

#[derive(Clone, Copy)]
pub struct LockHeader {
    pub start_ts: u64,
    pub for_update_ts: u64,
    pub min_commit_ts: u64,
    pub ttl: u32,
    pub op: u8,
    pub has_old_ver: bool,
    pub primary_len: u16,
    pub use_async_commit: u8,
    pub secondary_num: u32,
}

pub struct Lock {
    pub header: LockHeader,
    pub primary: Bytes,
    pub value: Bytes,
    pub secondaries: Vec<Bytes>,
}

impl Deref for Lock {
    type Target = LockHeader;

    fn deref(&self) -> &Self::Target {
        return &self.header;
    }
}

impl Lock {
    pub fn decode(mut data: &[u8]) -> Self {
        let mut lock = Self {
            header: unsafe { *(data.as_ptr() as *const LockHeader) },
            primary: Default::default(),
            value: Default::default(),
            secondaries: vec![],
        };
        let primary_len = lock.primary_len as usize;
        let buf = Bytes::copy_from_slice(&data[LOCK_HEADER_SIZE..]);
        lock.primary = buf.slice(..primary_len);
        let mut cursor = primary_len;
        if lock.secondary_num > 0 {
            lock.secondaries = Vec::with_capacity(lock.secondary_num as usize);
            for _ in 0..lock.secondary_num {
                let key_len = LittleEndian::read_u16(&buf[cursor..]) as usize;
                cursor += 2;
                let secondary = buf.slice(cursor..cursor + key_len);
                lock.secondaries.push(secondary);
            }
        }
        lock
    }
}

const USER_META_SIZE: usize = std::mem::size_of::<UserMeta>();

#[derive(Clone, Copy)]
pub struct UserMeta {
    pub start_ts: u64,
    pub commit_ts: u64,
}

impl UserMeta {
    pub fn from_slice(buf: &[u8]) -> Self {
        Self {
            start_ts: LittleEndian::read_u64(buf),
            commit_ts: LittleEndian::read_u64(&buf[8..]),
        }
    }

    pub fn new(start_ts: u64, commit_ts: u64) -> Self {
        Self {
            start_ts,
            commit_ts,
        }
    }

    pub fn to_array(&self) -> [u8; USER_META_SIZE] {
        unsafe { std::mem::transmute(*self) }
    }
}

pub fn encode_extra_txn_status_key(key: &[u8], start_ts: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(key.len() + 8);
    buf.extend_from_slice(key);
    buf.put_u64(start_ts.reverse_bits());
    buf.freeze()
}
