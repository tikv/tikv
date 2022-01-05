// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
use std::ops::Deref;

pub const WRITE_CF: usize = 0;
pub const LOCK_CF: usize = 1;
pub const EXTRA_CF: usize = 2;

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
