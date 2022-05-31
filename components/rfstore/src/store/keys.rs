// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use kvproto::metapb;
use tikv_util::codec::bytes::decode_bytes;

pub(crate) const RAFT_STATE_KEY_BYTE: u8 = 1;
pub(crate) const REGION_META_KEY_BYTE: u8 = 2;
pub(crate) const REGION_META_KEY_PREFIX: &[u8] = &[REGION_META_KEY_BYTE];
pub(crate) const STORE_IDENT_KEY: &[u8] = &[3];
pub(crate) const PREPARE_BOOTSTRAP_KEY: &[u8] = &[4];
pub(crate) const KV_ENGINE_META_KEY: &[u8] = &[5];
pub(crate) const EMPTY_KEY: &[u8] = &[];
pub(crate) const RAW_INITIAL_START_KEY: &[u8] = &[2];
pub(crate) const RAW_INITIAL_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

pub(crate) fn raft_state_key(version: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(5);
    key.put_u8(RAFT_STATE_KEY_BYTE);
    key.put_u32(version as u32);
    key.freeze()
}

pub(crate) fn region_state_key(version: u64, conf_ver: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(9);
    key.put_u8(REGION_META_KEY_BYTE);
    key.put_u32(version as u32);
    key.put_u32(conf_ver as u32);
    key.freeze()
}

pub(crate) fn parse_region_state_key(key: &[u8]) -> (u64, u64) {
    let ver = BigEndian::read_u32(&key[1..]);
    let conf_ver = BigEndian::read_u32(&key[5..]);
    (ver as u64, conf_ver as u64)
}

// Get the `start_key` of current region in raw form.
pub(crate) fn raw_start_key(region: &metapb::Region) -> Bytes {
    // only initialized region's start_key can be encoded, otherwise there must be bugs
    // somewhere.
    if region.start_key.is_empty() {
        // Data starts with 0x01 is used as local key.
        return Bytes::from_static(RAW_INITIAL_START_KEY);
    }
    let mut slice = region.start_key.as_slice();
    let start_key = decode_bytes(&mut slice, false).unwrap();
    Bytes::from(start_key)
}

// Get the `end_key` of current region in raw form.
pub(crate) fn raw_end_key(region: &metapb::Region) -> Bytes {
    // only initialized region's end_key can be encoded, otherwise there must be bugs
    // somewhere.
    if region.end_key.is_empty() {
        return Bytes::from_static(RAW_INITIAL_END_KEY);
    }
    let mut slice = region.end_key.as_slice();
    let end_key = decode_bytes(&mut slice, false).unwrap();
    Bytes::from(end_key)
}
