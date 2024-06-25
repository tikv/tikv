// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    cmp::{self, Ordering},
    sync::Arc,
};

use bytes::{BufMut, Bytes};
use engine_traits::CacheRange;
use txn_types::{Key, TimeStamp};

use crate::{memory_controller::MemoryController, write_batch::MEM_CONTROLLER_OVERHEAD};

/// The internal bytes used in the skiplist. See comments on
/// `encode_internal_bytes`.
#[derive(Debug)]
pub struct InternalBytes {
    bytes: Bytes,
    // memory_limiter **must** be set when used as key/values being inserted
    // into skiplist as keys/values.
    memory_controller: Option<Arc<MemoryController>>,
}

impl Drop for InternalBytes {
    fn drop(&mut self) {
        let size = InternalBytes::memory_size_required(self.bytes.len());
        let controller = self.memory_controller.take();
        if let Some(controller) = controller {
            // Reclaim the memory though the bytes have not been drop. This time
            // gap should not matter.
            controller.release(size);
        }
    }
}

impl InternalBytes {
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self {
            bytes,
            memory_controller: None,
        }
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self {
            bytes: Bytes::from(vec),
            memory_controller: None,
        }
    }

    pub fn memory_controller_set(&self) -> bool {
        self.memory_controller.is_some()
    }

    pub fn set_memory_controller(&mut self, controller: Arc<MemoryController>) {
        self.memory_controller = Some(controller);
    }

    pub fn clone_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn same_user_key_with(&self, other: &InternalBytes) -> bool {
        let InternalKey { user_key, .. } = decode_key(self.as_slice());
        let InternalKey {
            user_key: other_user_key,
            ..
        } = decode_key(other.as_slice());
        user_key == other_user_key
    }

    // The IntervalBytes that has been written in the in-memory engine have set the
    // `memory_controller`, so the memory usage of it is the memory usage of bytes +
    // 8 bytes (the size of Arc).
    #[inline]
    pub fn memory_size_required(bytes_size: usize) -> usize {
        bytes_size + MEM_CONTROLLER_OVERHEAD
    }
}

impl PartialEq for InternalBytes {
    fn eq(&self, other: &Self) -> bool {
        self.bytes.eq(&other.bytes)
    }
}

impl Eq for InternalBytes {}

impl Ord for InternalBytes {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let k1 = &self.bytes[..self.bytes.len() - ENC_KEY_SEQ_LENGTH];
        let k2 = &other.bytes[..other.bytes.len() - ENC_KEY_SEQ_LENGTH];
        let c = k1.cmp(k2);
        if c != Ordering::Equal {
            return c;
        }

        let n1 = u64::from_le_bytes(
            self.bytes[(self.bytes.len() - ENC_KEY_SEQ_LENGTH)..]
                .try_into()
                .unwrap(),
        );
        let n2 = u64::from_le_bytes(
            other.bytes[(other.bytes.len() - ENC_KEY_SEQ_LENGTH)..]
                .try_into()
                .unwrap(),
        );

        #[allow(clippy::comparison_chain)]
        if n1 < n2 {
            Ordering::Greater
        } else if n1 > n2 {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for InternalBytes {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

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
    // key with mvcc version in memory comparable format
    pub user_key: &'a [u8],
    pub v_type: ValueType,
    pub sequence: u64,
}

// The size of sequence number suffix
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
    let num = u64::from_le_bytes(
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

/// Format for an internal key (used by the skip list.)
///
/// ```text
/// Format: | user key (n bytes) | value type (1 bytes) | sequence number (7 bytes) |
/// ```
/// value type 0 encodes deletion, value type 1 encodes value.
///
/// It follows the pattern of RocksDB, where the most 8 significant bits of u64
/// will not used by sequence number.
#[inline]
pub fn encode_internal_bytes(key: &[u8], seq: u64, v_type: ValueType) -> InternalBytes {
    assert!(seq == u64::MAX || seq >> ((ENC_KEY_SEQ_LENGTH - 1) * 8) == 0);
    let mut e = Vec::with_capacity(key.len() + ENC_KEY_SEQ_LENGTH);
    e.put(key);
    // RocksDB encodes u64 in little endian.
    e.put_u64_le((seq << 8) | v_type as u64);
    InternalBytes::from_vec(e)
}

/// encode mvcc user key with sequence number and value type
#[inline]
pub fn encode_key(key: &[u8], seq: u64, v_type: ValueType) -> InternalBytes {
    encode_internal_bytes(key, seq, v_type)
}

#[inline]
pub fn encode_seek_key(key: &[u8], seq: u64) -> InternalBytes {
    encode_internal_bytes(key, seq, VALUE_TYPE_FOR_SEEK)
}

#[inline]
pub fn encode_seek_for_prev_key(key: &[u8], seq: u64) -> InternalBytes {
    encode_internal_bytes(key, seq, VALUE_TYPE_FOR_SEEK_FOR_PREV)
}

// range keys deos not contain mvcc version and sequence number
#[inline]
pub fn encode_key_for_boundary_with_mvcc(range: &CacheRange) -> (InternalBytes, InternalBytes) {
    // Both encoded_start and encoded_end should be the smallest key in the
    // respective of user key (with mvcc version), so that the iterations cover all
    // versions of the range start and covers nothing of range end.

    // TODO: can we avoid one clone
    let start_mvcc_key = Key::from_encoded(range.start.to_vec())
        .append_ts(TimeStamp::max())
        .into_encoded();
    let encoded_start = encode_key(&start_mvcc_key, u64::MAX, VALUE_TYPE_FOR_SEEK);

    let end_mvcc_key = Key::from_encoded(range.end.to_vec())
        .append_ts(TimeStamp::max())
        .into_encoded();
    let encoded_end = encode_key(&end_mvcc_key, u64::MAX, VALUE_TYPE_FOR_SEEK);

    (encoded_start, encoded_end)
}

#[inline]
pub fn encode_key_for_boundary_without_mvcc(range: &CacheRange) -> (InternalBytes, InternalBytes) {
    // Both encoded_start and encoded_end should be the smallest key in the
    // respective of user key (without mvcc version), so that the iterations cover
    // all versions of the range start and covers nothing of range end.

    // TODO: can we avoid one clone
    let encoded_start = encode_key(&range.start, u64::MAX, VALUE_TYPE_FOR_SEEK);
    let encoded_end = encode_key(&range.end, u64::MAX, VALUE_TYPE_FOR_SEEK);

    (encoded_start, encoded_end)
}

// mvcc_prefix is already mem-comparison encoded.
#[inline]
pub fn encoding_for_filter(mvcc_prefix: &[u8], start_ts: TimeStamp) -> InternalBytes {
    let default_key = Key::from_encoded_slice(mvcc_prefix)
        .append_ts(start_ts)
        .into_encoded();
    encode_key(&default_key, u64::MAX, VALUE_TYPE_FOR_SEEK)
}

#[cfg(test)]
pub fn construct_user_key(i: u64) -> Vec<u8> {
    let k = format!("k{:08}", i);
    k.as_bytes().to_owned()
}

#[cfg(test)]
pub fn construct_key(i: u64, ts: u64) -> Vec<u8> {
    let k = format!("k{:08}", i);
    Key::from_encoded(k.as_bytes().to_vec())
        .append_ts(TimeStamp::new(ts))
        .into_encoded()
}

#[cfg(test)]
pub fn construct_value(i: u64, j: u64) -> String {
    format!("value-{:04}-{:04}", i, j)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;
    use crate::keys::{encode_key, ValueType};

    fn construct_key(i: u64, mvcc: u64) -> Vec<u8> {
        let k = format!("zk{:08}", i);
        let mut key = k.as_bytes().to_vec();
        // mvcc version should be make bit-wise reverse so that k-100 is less than k-99
        key.put_u64(!mvcc);
        key
    }

    #[test]
    fn test_compare_key() {
        let k = construct_key(1, 10);
        // key1: k1_10_10_val
        let key1 = encode_key(&k, 10, ValueType::Value);
        // key2: k1_10_10_del
        let key2 = encode_key(&k, 10, ValueType::Deletion);
        assert!(key1.cmp(&key2).is_le());

        // key2: k1_10_0_val
        let key2 = encode_key(&k, 0, ValueType::Value);
        assert!(key1.cmp(&key2).is_le());

        // key1: k1_10_MAX_val
        let key1 = encode_key(&k, u64::MAX, ValueType::Value);
        assert!(key1.cmp(&key2).is_le());

        let k = construct_key(1, 0);
        // key2: k1_0_10_val
        let key2 = encode_key(&k, 10, ValueType::Value);
        assert!(key1.cmp(&key2).is_le());

        // key1: k1_MAX_0_val
        let k = construct_key(1, u64::MAX);
        let key1 = encode_key(&k, 0, ValueType::Value);
        assert!(key1.cmp(&key2).is_le());

        let k = construct_key(2, u64::MAX);
        // key2: k2_MAX_MAX_val
        let key2 = encode_key(&k, u64::MAX, ValueType::Value);
        assert!(key1.cmp(&key2).is_le());
    }

    #[test]
    fn test_encode_decode() {
        let encoded_bytes = encode_internal_bytes(b"foo", 7, ValueType::Value);
        let key = decode_key(encoded_bytes.as_slice());
        assert_eq!(key.sequence, 7);
        assert_eq!(key.v_type, ValueType::Value as _);

        let value_type_byte = encoded_bytes.as_slice()[encoded_bytes.as_slice().len() - 8];
        assert_eq!(value_type_byte, ValueType::Value as u8);
        let mut seq_bytes = vec![0u8; 7];
        seq_bytes.copy_from_slice(&encoded_bytes.as_slice()[encoded_bytes.as_slice().len() - 7..]);
        seq_bytes.push(0);
        assert_eq!(u64::from_le_bytes(seq_bytes.try_into().unwrap()), 7);
    }
}
