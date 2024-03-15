// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;

use bytes::{BufMut, Bytes, BytesMut};
use engine_traits::CacheRange;
use skiplist_rs::KeyComparator;
use tikv_util::codec::number::NumberEncoder;
use txn_types::{Key, TimeStamp};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    Deletion = 0,
    Value = 1,
}

// See `compare` of InternalKeyComparator, for the same user key and same
// sequence number, ValueType::Value is less than ValueType::Deletion
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;
pub const VALUE_TYPE_FOR_SEEK_FOR_PREV: ValueType = ValueType::Deletion;

impl TryFrom<u8> for ValueType {
    type Error = String;
    fn try_from(value: u8) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::Deletion),
            1 => Ok(ValueType::Value),
            _ => Err(format!("invalid value: {}", value)),
        }
    }
}

pub struct InternalKey<'a> {
    // key with mvcc version
    pub user_key: &'a [u8],
    pub v_type: ValueType,
    pub sequence: u64,
}

pub const ENC_KEY_SEQ_LENGTH: usize = std::mem::size_of::<u64>();

impl<'a> From<&'a [u8]> for InternalKey<'a> {
    fn from(encoded_key: &'a [u8]) -> Self {
        decode_key(encoded_key)
    }
}

#[inline]
pub fn decode_key(encoded_key: &[u8]) -> InternalKey<'_> {
    assert!(encoded_key.len() >= ENC_KEY_SEQ_LENGTH);
    let seq_offset = encoded_key.len() - ENC_KEY_SEQ_LENGTH;
    let num = u64::from_be_bytes(
        encoded_key[seq_offset..seq_offset + ENC_KEY_SEQ_LENGTH]
            .try_into()
            .unwrap(),
    );
    let sequence = num >> 8;
    let v_type = ((num & 0xff) as u8).try_into().unwrap();
    InternalKey {
        user_key: &encoded_key[..seq_offset],
        v_type,
        sequence,
    }
}

#[inline]
pub fn extract_user_key_and_suffix_u64(encoded_key: &[u8]) -> (&[u8], u64) {
    assert!(encoded_key.len() >= ENC_KEY_SEQ_LENGTH);
    let seq_offset = encoded_key.len() - ENC_KEY_SEQ_LENGTH;
    let num = u64::from_be_bytes(
        encoded_key[seq_offset..seq_offset + ENC_KEY_SEQ_LENGTH]
            .try_into()
            .unwrap(),
    );

    (&encoded_key[..seq_offset], num)
}

/// Format for an internal key (used by the skip list.)
/// ```
/// contents:      key of size n     | value type | sequence number shifted by 8 bits
/// byte position:         0 ..  n-1 | n          |  n + 1 .. n + 7
/// ```
/// value type 0 encodes deletion, value type 1 encodes value.
///
/// It follows the pattern of RocksDB, where the most 8 significant bits of u64
/// will not used by sequence number.
#[inline]
pub fn encode_key_internal<T: BufMut>(
    key: &[u8],
    seq: u64,
    v_type: ValueType,
    f: impl FnOnce(usize) -> T,
) -> T {
    assert!(seq == u64::MAX || seq >> ((ENC_KEY_SEQ_LENGTH - 1) * 8) == 0);
    let mut e = f(key.len() + ENC_KEY_SEQ_LENGTH);
    e.put(key);
    e.put_u64((seq << 8) | v_type as u64);
    e
}

#[inline]
pub fn encode_key(key: &[u8], seq: u64, v_type: ValueType) -> Bytes {
    let e = encode_key_internal::<BytesMut>(key, seq, v_type, BytesMut::with_capacity);
    e.freeze()
}

#[inline]
pub fn encode_seek_key(key: &[u8], seq: u64, v_type: ValueType) -> Vec<u8> {
    encode_key_internal::<Vec<_>>(key, seq, v_type, Vec::with_capacity)
}

// range keys deos not contain mvcc version and sequence number
#[inline]
pub fn encode_key_for_eviction(range: &CacheRange) -> (Vec<u8>, Vec<u8>) {
    // Both encoded_start and encoded_end should be the smallest key in the
    // respective of user key, so that the eviction covers all versions of the range
    // start and covers nothing of range end.
    let mut encoded_start = Vec::with_capacity(range.start.len() + 16);
    encoded_start.extend_from_slice(&range.start);
    encoded_start.encode_u64_desc(u64::MAX).unwrap();
    encoded_start.put_u64((u64::MAX << 8) | VALUE_TYPE_FOR_SEEK as u64);

    let mut encoded_end = Vec::with_capacity(range.end.len() + 16);
    encoded_end.extend_from_slice(&range.end);
    encoded_end.encode_u64_desc(u64::MAX).unwrap();
    encoded_end.put_u64((u64::MAX << 8) | VALUE_TYPE_FOR_SEEK as u64);

    (encoded_start, encoded_end)
}

#[inline]
pub fn encoding_for_filter(mvcc_prefix: &[u8], start_ts: TimeStamp) -> Vec<u8> {
    let mut default_key = Vec::with_capacity(mvcc_prefix.len() + 2 * ENC_KEY_SEQ_LENGTH);
    default_key.extend_from_slice(mvcc_prefix);
    let mut default_key = Key::from_encoded(default_key)
        .append_ts(start_ts)
        .into_encoded();
    default_key.put_u64((u64::MAX << 8) | VALUE_TYPE_FOR_SEEK as u64);
    default_key
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InternalKeyComparator {}

impl InternalKeyComparator {
    fn same_key(lhs: &[u8], rhs: &[u8]) -> bool {
        let k_1 = decode_key(lhs);
        let k_2 = decode_key(rhs);
        k_1.user_key == k_2.user_key
    }
}

impl KeyComparator for InternalKeyComparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (k_1, s_1) = extract_user_key_and_suffix_u64(lhs);
        let (k_2, s_2) = extract_user_key_and_suffix_u64(rhs);
        let r = k_1.cmp(k_2);
        if r.is_eq() {
            match s_1.cmp(&s_2) {
                cmp::Ordering::Greater => {
                    return cmp::Ordering::Less;
                }
                cmp::Ordering::Less => {
                    return cmp::Ordering::Greater;
                }
                cmp::Ordering::Equal => {
                    return cmp::Ordering::Equal;
                }
            }
        }
        r
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        InternalKeyComparator::same_key(lhs, rhs)
    }
}

#[cfg(test)]
pub fn construct_user_key(i: u64) -> Vec<u8> {
    let k = format!("k{:08}", i);
    k.as_bytes().to_owned()
}

#[cfg(test)]
pub fn construct_key(i: u64, mvcc: u64) -> Vec<u8> {
    let k = format!("k{:08}", i);
    let mut key = k.as_bytes().to_vec();
    // mvcc version should be make bit-wise reverse so that k-100 is less than k-99
    key.put_u64(!mvcc);
    key
}

#[cfg(test)]
pub fn construct_value(i: u64, j: u64) -> String {
    format!("value-{:04}-{:04}", i, j)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use skiplist_rs::KeyComparator;

    use super::{InternalKeyComparator, ValueType};
    use crate::keys::encode_key;

    fn construct_key(i: u64, mvcc: u64) -> Vec<u8> {
        let k = format!("k{:08}", i);
        let mut key = k.as_bytes().to_vec();
        // mvcc version should be make bit-wise reverse so that k-100 is less than k-99
        key.put_u64(!mvcc);
        key
    }

    #[test]
    fn test_compare_key() {
        let c = InternalKeyComparator::default();
        let k = construct_key(1, 10);
        // key1: k1_10_10_val
        let key1 = encode_key(&k, 10, ValueType::Value);
        // key2: k1_10_10_del
        let key2 = encode_key(&k, 10, ValueType::Deletion);
        assert!(c.compare_key(&key1, &key2).is_le());

        // key2: k1_10_0_val
        let key2 = encode_key(&k, 0, ValueType::Value);
        assert!(c.compare_key(&key1, &key2).is_le());

        // key1: k1_10_MAX_val
        let key1 = encode_key(&k, u64::MAX, ValueType::Value);
        assert!(c.compare_key(&key1, &key2).is_le());

        let k = construct_key(1, 0);
        // key2: k1_0_10_val
        let key2 = encode_key(&k, 10, ValueType::Value);
        assert!(c.compare_key(&key1, &key2).is_le());

        // key1: k1_MAX_0_val
        let k = construct_key(1, u64::MAX);
        let key1 = encode_key(&k, 0, ValueType::Value);
        assert!(c.compare_key(&key1, &key2).is_le());

        let k = construct_key(2, u64::MAX);
        // key2: k2_MAX_MAX_val
        let key2 = encode_key(&k, u64::MAX, ValueType::Value);
        assert!(c.compare_key(&key1, &key2).is_le());
    }
}
