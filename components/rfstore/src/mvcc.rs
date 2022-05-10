// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};

pub const WRITE_CF: usize = 0;
pub const LOCK_CF: usize = 1;
pub const EXTRA_CF: usize = 2;

pub const USER_META_FORMAT_V1: u8 = 1;

// format(1) + start_ts(8) + commit_ts(8)
const USER_META_SIZE: usize = 1 + std::mem::size_of::<UserMeta>();

#[derive(Clone, Copy)]
pub struct UserMeta {
    pub start_ts: u64,
    pub commit_ts: u64,
}

impl UserMeta {
    pub fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf[0], USER_META_FORMAT_V1);
        Self {
            start_ts: LittleEndian::read_u64(&buf[1..]),
            commit_ts: LittleEndian::read_u64(&buf[9..]),
        }
    }

    pub fn new(start_ts: u64, commit_ts: u64) -> Self {
        Self {
            start_ts,
            commit_ts,
        }
    }

    pub fn to_array(&self) -> [u8; USER_META_SIZE] {
        let mut array = [0u8; USER_META_SIZE];
        array[0] = USER_META_FORMAT_V1;
        LittleEndian::write_u64(&mut array[1..], self.start_ts);
        LittleEndian::write_u64(&mut array[9..], self.commit_ts);
        array
    }
}

pub fn encode_extra_txn_status_key(key: &[u8], start_ts: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(key.len() + 8);
    buf.extend_from_slice(key);
    buf.put_u64(start_ts.reverse_bits());
    buf.freeze()
}
