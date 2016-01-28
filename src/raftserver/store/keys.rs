use std::vec::Vec;

use byteorder::{BigEndian, WriteBytesExt};

use util::codec::bytes;

pub const MIN_KEY: &'static [u8] = &[];
pub const MAX_KEY: &'static [u8] = &[0xFF];

// local is in (0x01, 0x02);
pub const LOCAL_PREFIX: &'static [u8] = &[0x01];
pub const LOCAL_MIN: &'static [u8] = &[0x01];
pub const LOCAL_MAX: &'static [u8] = &[0x02];
pub const META1_PREFIX: &'static [u8] = &[0x02];
pub const META2_PREFIX: &'static [u8] = &[0x03];
pub const META_MIN: &'static [u8] = &[0x02];
pub const META_MAX: &'static [u8] = &[0x04];

pub const DATA_PREFIX: &'static [u8] = &[b'z'];

// Following keys are all local keys, so the first byte must be 0x01.
const STORE_IDENT_KEY: &'static [u8] = &[0x01, 0x01];
const REGION_ID_KEY_PREFIX: &'static [u8] = &[0x01, 0x02];
const REGION_META_KEY_PREFIX: &'static [u8] = &[0x01, 0x03];

// Following are the suffix after the local prefix.
// For region id
const RAFT_LOG_SUFFIX: u8 = 0x01;
const RAFT_HARD_STATE_SUFFIX: u8 = 0x02;
const RAFT_APPLIED_INDEX_SUFFIX: u8 = 0x03;
const RAFT_LAST_INDEX_SUFFIX: u8 = 0x04;
const RAFT_TRUNCATED_STATE_SUFFIX: u8 = 0x05;

// For region meta
const REGION_INFO_SUFFIX: u8 = 0x01;

pub fn store_ident_key() -> Vec<u8> {
    let mut key = Vec::with_capacity(STORE_IDENT_KEY.len());
    key.extend_from_slice(STORE_IDENT_KEY);
    key
}

fn make_region_id_key(region_id: u64, suffix: u8, extra_cap: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(REGION_ID_KEY_PREFIX.len() + 8 + 1 + extra_cap);
    key.extend_from_slice(REGION_ID_KEY_PREFIX);
    // no need check error here, can't panic;
    key.write_u64::<BigEndian>(region_id).unwrap();
    key.push(suffix);
    key
}

pub fn region_id_prefix(region_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(REGION_ID_KEY_PREFIX.len() + 8);
    key.extend_from_slice(REGION_ID_KEY_PREFIX);
    // no need check error here, can't panic;
    key.write_u64::<BigEndian>(region_id).unwrap();
    key
}

pub fn raft_log_key(region_id: u64, log_index: u64) -> Vec<u8> {
    let mut key = make_region_id_key(region_id, RAFT_LOG_SUFFIX, 8);
    // no need check error here, can't panic;
    key.write_u64::<BigEndian>(log_index).unwrap();
    key
}

pub fn raft_log_prefix(region_id: u64) -> Vec<u8> {
    make_region_id_key(region_id, RAFT_LOG_SUFFIX, 0)
}

pub fn raft_hard_state_key(region_id: u64) -> Vec<u8> {
    make_region_id_key(region_id, RAFT_HARD_STATE_SUFFIX, 0)
}

pub fn raft_applied_index_key(region_id: u64) -> Vec<u8> {
    make_region_id_key(region_id, RAFT_APPLIED_INDEX_SUFFIX, 0)
}

pub fn raft_last_index_key(region_id: u64) -> Vec<u8> {
    make_region_id_key(region_id, RAFT_LAST_INDEX_SUFFIX, 0)
}

pub fn raft_truncated_state_key(region_id: u64) -> Vec<u8> {
    make_region_id_key(region_id, RAFT_TRUNCATED_STATE_SUFFIX, 0)
}



fn make_region_meta_key(region_key: &[u8], suffix: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(REGION_META_KEY_PREFIX.len() + 1 + (region_key.len() / 8) * 9);
    key.extend_from_slice(REGION_META_KEY_PREFIX);
    key.extend(bytes::encode_bytes(region_key));
    key.push(suffix);
    key
}

pub fn region_meta_prefix(region_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(REGION_META_KEY_PREFIX.len() + (region_key.len() / 8) * 9);
    key.extend_from_slice(REGION_META_KEY_PREFIX);
    key.extend(bytes::encode_bytes(region_key));
    key
}

pub fn region_info_key(region_key: &[u8]) -> Vec<u8> {
    make_region_meta_key(region_key, REGION_INFO_SUFFIX)
}
