// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use tikv_util::codec;
use tikv_util::codec::number::{self, NumberEncoder};

pub const TEST_CURRENT_TS: u64 = 100;

/// Check if key in range [`start_key`, `end_key`).
#[allow(dead_code)]
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange {
            key: key.to_vec(),
            region_id,
            start: start_key.to_vec(),
            end: end_key.to_vec(),
        })
    }
}

pub fn append_expire_ts(value: &mut Vec<u8>, expire_ts: u64) {
    value.encode_u64(expire_ts).unwrap();
}

pub fn get_expire_ts(value_with_ttl: &[u8]) -> Result<u64> {
    let len = value_with_ttl.len();
    if len < number::U64_SIZE {
        return Err(Error::Codec(codec::Error::ValueLength));
    }
    let mut ts = &value_with_ttl[len - number::U64_SIZE..];
    Ok(number::decode_u64(&mut ts)?)
}

pub fn strip_expire_ts(value_with_ttl: &[u8]) -> &[u8] {
    let len = value_with_ttl.len();
    &value_with_ttl[..len - number::U64_SIZE]
}

pub fn truncate_expire_ts(value_with_ttl: &mut Vec<u8>) -> Result<()> {
    let len = value_with_ttl.len();
    if len < number::U64_SIZE {
        return Err(Error::Codec(codec::Error::ValueLength));
    }
    value_with_ttl.truncate(len - number::U64_SIZE);
    Ok(())
}
