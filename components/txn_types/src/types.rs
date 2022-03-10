// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::timestamp::TimeStamp;
use bitflags::bitflags;
use byteorder::{ByteOrder, NativeEndian};
use collections::HashMap;
use kvproto::kvrpcpb::{self, Assertion};
use std::fmt::{self, Debug, Display, Formatter};
use tikv_util::codec;
use tikv_util::codec::bytes;
use tikv_util::codec::bytes::BytesEncoder;
use tikv_util::codec::number::{self, NumberEncoder};

// Short value max len must <= 255.
pub const SHORT_VALUE_MAX_LEN: usize = 255;
pub const SHORT_VALUE_PREFIX: u8 = b'v';

pub fn is_short_value(value: &[u8]) -> bool {
    value.len() <= SHORT_VALUE_MAX_LEN
}

/// Value type which is essentially raw bytes.
pub type Value = Vec<u8>;

/// Key-value pair type.
///
/// The value is simply raw bytes; the key is a little bit tricky, which is
/// encoded bytes.
pub type KvPair = (Vec<u8>, Value);

/// Key type.
///
/// Keys have 2 types of binary representation - raw and encoded. The raw
/// representation is for public interface, the encoded representation is for
/// internal storage. We can get both representations from an instance of this
/// type.
///
/// Orthogonal to binary representation, keys may or may not embed a timestamp,
/// but this information is transparent to this type, the caller must use it
/// consistently.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Key(Vec<u8>);

/// Core functions for `Key`.
impl Key {
    /// Creates a key from raw bytes.
    #[inline]
    pub fn from_raw(key: &[u8]) -> Key {
        // adding extra length for appending timestamp
        let len = codec::bytes::max_encoded_bytes_size(key.len()) + codec::number::U64_SIZE;
        let mut encoded = Vec::with_capacity(len);
        encoded.encode_bytes(key, false).unwrap();
        Key(encoded)
    }

    /// Creates a key from raw bytes but returns None if the key is an empty slice.
    #[inline]
    pub fn from_raw_maybe_unbounded(key: &[u8]) -> Option<Key> {
        if key.is_empty() {
            None
        } else {
            Some(Key::from_raw(key))
        }
    }

    /// Gets and moves the raw representation of this key.
    #[inline]
    pub fn into_raw(self) -> Result<Vec<u8>, codec::Error> {
        let mut k = self.0;
        bytes::decode_bytes_in_place(&mut k, false)?;
        Ok(k)
    }

    /// Gets the raw representation of this key.
    #[inline]
    pub fn to_raw(&self) -> Result<Vec<u8>, codec::Error> {
        bytes::decode_bytes(&mut self.0.as_slice(), false)
    }

    /// Creates a key from encoded bytes vector.
    #[inline]
    pub fn from_encoded(encoded_key: Vec<u8>) -> Key {
        Key(encoded_key)
    }

    /// Creates a key with reserved capacity for timestamp from encoded bytes slice.
    #[inline]
    pub fn from_encoded_slice(encoded_key: &[u8]) -> Key {
        let mut k = Vec::with_capacity(encoded_key.len() + number::U64_SIZE);
        k.extend_from_slice(encoded_key);
        Key(k)
    }

    /// Gets the encoded representation of this key.
    #[inline]
    pub fn as_encoded(&self) -> &Vec<u8> {
        &self.0
    }

    /// Gets and moves the encoded representation of this key.
    #[inline]
    pub fn into_encoded(self) -> Vec<u8> {
        self.0
    }

    /// Creates a new key by appending a `u64` timestamp to this key.
    #[inline]
    #[must_use]
    pub fn append_ts(mut self, ts: TimeStamp) -> Key {
        self.0.encode_u64_desc(ts.into_inner()).unwrap();
        self
    }

    /// Gets the timestamp contained in this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped
    /// key.
    #[inline]
    pub fn decode_ts(&self) -> Result<TimeStamp, codec::Error> {
        Self::decode_ts_from(&self.0)
    }

    /// Creates a new key by truncating the timestamp from this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped key.
    #[inline]
    pub fn truncate_ts(mut self) -> Result<Key, codec::Error> {
        let len = self.0.len();
        if len < number::U64_SIZE {
            // TODO: IMHO, this should be an assertion failure instead of
            // returning an error. If this happens, it indicates a bug in
            // the caller module, have to make code change to fix it.
            //
            // Even if it passed the length check, it still could be buggy,
            // a better way is to introduce a type `TimestampedKey`, and
            // functions to convert between `TimestampedKey` and `Key`.
            // `TimestampedKey` is in a higher (MVCC) layer, while `Key` is
            // in the core storage engine layer.
            Err(codec::Error::KeyLength)
        } else {
            self.0.truncate(len - number::U64_SIZE);
            Ok(self)
        }
    }

    /// Split a ts encoded key, return the user key and timestamp.
    #[inline]
    pub fn split_on_ts_for(key: &[u8]) -> Result<(&[u8], TimeStamp), codec::Error> {
        if key.len() < number::U64_SIZE {
            Err(codec::Error::KeyLength)
        } else {
            let pos = key.len() - number::U64_SIZE;
            let k = &key[..pos];
            let mut ts = &key[pos..];
            Ok((k, number::decode_u64_desc(&mut ts)?.into()))
        }
    }

    /// Extract the user key from a ts encoded key.
    #[inline]
    pub fn truncate_ts_for(key: &[u8]) -> Result<&[u8], codec::Error> {
        let len = key.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        Ok(&key[..key.len() - number::U64_SIZE])
    }

    /// Decode the timestamp from a ts encoded key.
    #[inline]
    pub fn decode_ts_from(key: &[u8]) -> Result<TimeStamp, codec::Error> {
        let len = key.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        let mut ts = &key[len - number::U64_SIZE..];
        Ok(number::decode_u64_desc(&mut ts)?.into())
    }

    /// Whether the user key part of a ts encoded key `ts_encoded_key` equals to the encoded
    /// user key `user_key`.
    ///
    /// There is an optimization in this function, which is to compare the last 8 encoded bytes
    /// first before comparing the rest. It is because in TiDB many records are ended with an 8
    /// byte row id and in many situations only this part is different when calling this function.
    //
    // TODO: If the last 8 byte is memory aligned, it would be better.
    #[inline]
    pub fn is_user_key_eq(ts_encoded_key: &[u8], user_key: &[u8]) -> bool {
        let user_key_len = user_key.len();
        if ts_encoded_key.len() != user_key_len + number::U64_SIZE {
            return false;
        }
        if user_key_len >= number::U64_SIZE {
            // We compare last 8 bytes as u64 first, then compare the rest.
            // TODO: Can we just use == to check the left part and right part? `memcmp` might
            //       be smart enough.
            let left = NativeEndian::read_u64(&ts_encoded_key[user_key_len - 8..]);
            let right = NativeEndian::read_u64(&user_key[user_key_len - 8..]);
            if left != right {
                return false;
            }
            ts_encoded_key[..user_key_len - 8] == user_key[..user_key_len - 8]
        } else {
            ts_encoded_key[..user_key_len] == user_key[..]
        }
    }

    /// Returns whether the encoded key is encoded from `raw_key`.
    pub fn is_encoded_from(&self, raw_key: &[u8]) -> bool {
        bytes::is_encoded_from(&self.0, raw_key, false)
    }

    /// TiDB uses the same hash algorithm.
    pub fn gen_hash(&self) -> u64 {
        farmhash::fingerprint64(&self.to_raw().unwrap())
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Clone for Key {
    fn clone(&self) -> Self {
        // default clone implementation use self.len() to reserve capacity
        // for the sake of appending ts, we need to reserve more
        let mut key = Vec::with_capacity(self.0.capacity());
        key.extend_from_slice(&self.0);
        Key(key)
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &log_wrappers::Value::key(&self.0))
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &log_wrappers::Value::key(&self.0))
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MutationType {
    Put,
    Delete,
    Lock,
    Insert,
    Other,
}

/// A row mutation.
///
/// It may also carry an `Assertion` field, which means it has such an *assertion* to the data
/// (the key already exist or not exist). The assertion should pass if the mutation (in a prewrite
/// request) is going to be finished successfully, otherwise it indicates there should be some bug
/// causing the attempt to write wrong data.
#[derive(Debug, Clone)]
pub enum Mutation {
    /// Put `Value` into `Key`, overwriting any existing value.
    Put((Key, Value), Assertion),
    /// Delete `Key`.
    Delete(Key, Assertion),
    /// Set a lock on `Key`.
    Lock(Key, Assertion),
    /// Put `Value` into `Key` if `Key` does not yet exist.
    ///
    /// Returns `kvrpcpb::KeyError::AlreadyExists` if the key already exists.
    Insert((Key, Value), Assertion),
    /// Check `key` must be not exist.
    ///
    /// Returns `kvrpcpb::KeyError::AlreadyExists` if the key already exists.
    CheckNotExists(Key, Assertion),
}

impl Mutation {
    pub fn key(&self) -> &Key {
        match self {
            Mutation::Put((ref key, _), _) => key,
            Mutation::Delete(ref key, _) => key,
            Mutation::Lock(ref key, _) => key,
            Mutation::Insert((ref key, _), _) => key,
            Mutation::CheckNotExists(ref key, _) => key,
        }
    }

    pub fn mutation_type(&self) -> MutationType {
        match self {
            Mutation::Put(..) => MutationType::Put,
            Mutation::Delete(..) => MutationType::Delete,
            Mutation::Lock(..) => MutationType::Lock,
            Mutation::Insert(..) => MutationType::Insert,
            _ => MutationType::Other,
        }
    }

    pub fn into_key_value(self) -> (Key, Option<Value>) {
        match self {
            Mutation::Put((key, value), _) => (key, Some(value)),
            Mutation::Delete(key, _) => (key, None),
            Mutation::Lock(key, _) => (key, None),
            Mutation::Insert((key, value), _) => (key, Some(value)),
            Mutation::CheckNotExists(key, _) => (key, None),
        }
    }

    pub fn should_not_exists(&self) -> bool {
        matches!(
            self,
            Mutation::Insert(_, _) | Mutation::CheckNotExists(_, _)
        )
    }

    pub fn should_not_write(&self) -> bool {
        matches!(self, Mutation::CheckNotExists(_, _))
    }

    pub fn get_assertion(&self) -> Assertion {
        *match self {
            Mutation::Put(_, assertion) => assertion,
            Mutation::Delete(_, assertion) => assertion,
            Mutation::Lock(_, assertion) => assertion,
            Mutation::Insert(_, assertion) => assertion,
            Mutation::CheckNotExists(_, assertion) => assertion,
        }
    }

    pub fn set_assertion(&mut self, assertion: Assertion) {
        *match self {
            Mutation::Put(_, ref mut assertion) => assertion,
            Mutation::Delete(_, ref mut assertion) => assertion,
            Mutation::Lock(_, ref mut assertion) => assertion,
            Mutation::Insert(_, ref mut assertion) => assertion,
            Mutation::CheckNotExists(_, ref mut assertion) => assertion,
        } = assertion;
    }

    /// Creates a Put mutation with none assertion.
    pub fn make_put(key: Key, value: Value) -> Self {
        Mutation::Put((key, value), Assertion::None)
    }

    /// Creates a Delete mutation with none assertion.
    pub fn make_delete(key: Key) -> Self {
        Mutation::Delete(key, Assertion::None)
    }

    /// Creates a Lock mutation with none assertion.
    pub fn make_lock(key: Key) -> Self {
        Mutation::Lock(key, Assertion::None)
    }

    /// Creates a Insert mutation with none assertion.
    pub fn make_insert(key: Key, value: Value) -> Self {
        Mutation::Insert((key, value), Assertion::None)
    }

    /// Creates a CheckNotExists mutation with none assertion.
    pub fn make_check_not_exists(key: Key) -> Self {
        Mutation::CheckNotExists(key, Assertion::None)
    }
}

impl From<kvrpcpb::Mutation> for Mutation {
    fn from(mut m: kvrpcpb::Mutation) -> Mutation {
        match m.get_op() {
            kvrpcpb::Op::Put => Mutation::Put(
                (Key::from_raw(m.get_key()), m.take_value()),
                m.get_assertion(),
            ),
            kvrpcpb::Op::Del => Mutation::Delete(Key::from_raw(m.get_key()), m.get_assertion()),
            kvrpcpb::Op::Lock => Mutation::Lock(Key::from_raw(m.get_key()), m.get_assertion()),
            kvrpcpb::Op::Insert => Mutation::Insert(
                (Key::from_raw(m.get_key()), m.take_value()),
                m.get_assertion(),
            ),
            kvrpcpb::Op::CheckNotExists => {
                Mutation::CheckNotExists(Key::from_raw(m.get_key()), m.get_assertion())
            }
            _ => panic!("mismatch Op in prewrite mutations"),
        }
    }
}

/// `OldValue` is used by cdc to read the previous value associated with some key during the
/// prewrite process.
#[derive(Debug, Clone, PartialEq)]
pub enum OldValue {
    /// A real `OldValue`.
    Value { value: Value },
    /// A timestamp of an old value in case a value is not inlined in Write.
    ValueTimeStamp { start_ts: TimeStamp },
    /// `None` means we don't found a previous value.
    None,
    /// The user doesn't care about the previous value.
    Unspecified,
    /// Not sure whether the old value exists or not. users can seek CF_WRITE to the give position
    /// to take a look.
    SeekWrite(Key),
}

impl Default for OldValue {
    fn default() -> Self {
        OldValue::Unspecified
    }
}

impl OldValue {
    pub fn value(value: Value) -> Self {
        OldValue::Value { value }
    }

    pub fn seek_write(raw_user_key: &[u8], ts: u64) -> Self {
        let key = Key::from_raw(raw_user_key).append_ts(ts.into());
        OldValue::SeekWrite(key)
    }

    /// `resolved` means it's either something or `None`.
    pub fn resolved(&self) -> bool {
        match self {
            Self::Value { .. } | Self::ValueTimeStamp { .. } | Self::None => true,
            Self::Unspecified | Self::SeekWrite(_) => false,
        }
    }

    /// The finalized `OldValue::Value` content, or `None` for `OldValue::Unspecified`.
    ///
    /// # Panics
    ///
    /// Panic if it's `OldValue::ValueTimeStamp` or `OldValue::SeekWrite`.
    pub fn finalized(self) -> Option<Value> {
        match self {
            Self::Value { value } => Some(value),
            Self::None | Self::Unspecified => None,
            _ => panic!("OldValue must be finalized"),
        }
    }

    pub fn size(&self) -> usize {
        self.value_size() + std::mem::size_of::<OldValue>()
    }

    pub fn value_size(&self) -> usize {
        match self {
            OldValue::Value { value } => value.len(),
            _ => 0,
        }
    }
}

// Returned by MvccTxn when extra_op is set to kvrpcpb::ExtraOp::ReadOldValue.
// key with current ts -> (short value of the prev txn, start ts of the prev txn).
// The value of the map will be None when the mutation is `Insert`.
// MutationType is the type of mutation of the current write.
pub type OldValues = HashMap<Key, (OldValue, Option<MutationType>)>;

// Extra data fields filled by kvrpcpb::ExtraOp.
#[derive(Default, Debug, Clone)]
pub struct TxnExtra {
    pub old_values: OldValues,
    // Marks that this transaction is a 1PC transaction. RaftKv should set this flag
    // in the raft command request.
    pub one_pc: bool,
}

impl TxnExtra {
    pub fn is_empty(&self) -> bool {
        self.old_values.is_empty()
    }
}

pub trait TxnExtraScheduler: Send + Sync {
    fn schedule(&self, txn_extra: TxnExtra);
}

bitflags! {
    /// Additional flags for a write batch.
    /// They should be set in the `flags` field in `RaftRequestHeader`.
    pub struct WriteBatchFlags: u64 {
        /// Indicates this request is from a 1PC transaction.
        /// It helps CDC recognize 1PC transactions and handle them correctly.
        const ONE_PC = 0b00000001;
        /// Indicates this request is from a stale read-only transaction.
        const STALE_READ = 0b00000010;
        /// Indicates this request is a transfer leader command that needs to be proposed
        /// like a normal command.
        const TRANSFER_LEADER_PROPOSAL = 0b00000100;
    }
}

impl WriteBatchFlags {
    /// Convert from underlying bit representation
    /// panic if it contains bits that do not correspond to a flag
    pub fn from_bits_check(bits: u64) -> WriteBatchFlags {
        match WriteBatchFlags::from_bits(bits) {
            None => panic!("unrecognized flags: {:b}", bits),
            // zero or more flags
            Some(f) => f,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flags() {
        assert!(WriteBatchFlags::from_bits_check(0).is_empty());
        assert_eq!(
            WriteBatchFlags::from_bits_check(WriteBatchFlags::ONE_PC.bits()),
            WriteBatchFlags::ONE_PC
        );
        assert_eq!(
            WriteBatchFlags::from_bits_check(WriteBatchFlags::STALE_READ.bits()),
            WriteBatchFlags::STALE_READ
        );
    }

    #[test]
    fn test_flags_panic() {
        for _ in 0..100 {
            assert!(
                panic_hook::recover_safe(|| {
                    // r must be an invalid flags if it is not zero
                    let r = rand::random::<u64>() & !WriteBatchFlags::all().bits();
                    WriteBatchFlags::from_bits_check(r);
                    if r == 0 {
                        panic!("panic for zero");
                    }
                })
                .is_err()
            );
        }
    }

    #[test]
    fn test_is_user_key_eq() {
        // make a short name to keep format for the test.
        fn eq(a: &[u8], b: &[u8]) -> bool {
            Key::is_user_key_eq(a, b)
        }
        assert_eq!(false, eq(b"", b""));
        assert_eq!(false, eq(b"12345", b""));
        assert_eq!(true, eq(b"12345678", b""));
        assert_eq!(true, eq(b"x12345678", b"x"));
        assert_eq!(false, eq(b"x12345", b"x"));
        // user key len == 3
        assert_eq!(true, eq(b"xyz12345678", b"xyz"));
        assert_eq!(true, eq(b"xyz________", b"xyz"));
        assert_eq!(false, eq(b"xyy12345678", b"xyz"));
        assert_eq!(false, eq(b"yyz12345678", b"xyz"));
        assert_eq!(false, eq(b"xyz12345678", b"xy"));
        assert_eq!(false, eq(b"xyy12345678", b"xy"));
        assert_eq!(false, eq(b"yyz12345678", b"xy"));
        // user key len == 7
        assert_eq!(true, eq(b"abcdefg12345678", b"abcdefg"));
        assert_eq!(true, eq(b"abcdefgzzzzzzzz", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefg12345678", b"abcdef"));
        assert_eq!(false, eq(b"abcdefg12345678", b"bcdefg"));
        assert_eq!(false, eq(b"abcdefv12345678", b"abcdefg"));
        assert_eq!(false, eq(b"vbcdefg12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abccefg12345678", b"abcdefg"));
        // user key len == 8
        assert_eq!(true, eq(b"abcdefgh12345678", b"abcdefgh"));
        assert_eq!(true, eq(b"abcdefghyyyyyyyy", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"bcdefgh"));
        assert_eq!(false, eq(b"abcdefgz12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"zbcdefgh12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcddfgh12345678", b"abcdefgh"));
        // user key len == 9
        assert_eq!(true, eq(b"abcdefghi12345678", b"abcdefghi"));
        assert_eq!(true, eq(b"abcdefghixxxxxxxx", b"abcdefghi"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"bcdefghi"));
        assert_eq!(false, eq(b"abcdefghy12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"ybcdefghi12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"abcddfghi12345678", b"abcdefghi"));
        // user key len == 11
        assert_eq!(true, eq(b"abcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(true, eq(b"abcdefghijkabcdefgh", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"abcdefghij"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"bcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijx87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"xbcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abxdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"axcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdeffhijk87654321", b"abcdefghijk"));
    }

    #[test]
    fn test_is_encoded_from() {
        for raw_len in 0..=24 {
            let raw: Vec<u8> = (0..raw_len).collect();
            let encoded = Key::from_raw(&raw);
            assert!(encoded.is_encoded_from(&raw));

            let encoded_len = encoded.as_encoded().len();

            // Should return false if we modify one byte in raw
            for i in 0..raw.len() {
                let mut invalid_raw = raw.clone();
                invalid_raw[i] = raw[i].wrapping_add(1);
                assert!(!encoded.is_encoded_from(&invalid_raw));
            }

            // Should return false if we modify one byte in encoded
            for i in 0..encoded_len {
                let mut invalid_encoded = encoded.clone();
                invalid_encoded.0[i] = encoded.0[i].wrapping_add(1);
                assert!(!invalid_encoded.is_encoded_from(&raw));
            }

            // Should return false if encoded length is not a multiple of 9
            let mut invalid_encoded = encoded.clone();
            invalid_encoded.0.pop();
            assert!(!invalid_encoded.is_encoded_from(&raw));

            // Should return false if encoded has less or more chunks
            let shorter_encoded = Key::from_encoded_slice(&encoded.0[..encoded_len - 9]);
            assert!(!shorter_encoded.is_encoded_from(&raw));
            let mut longer_encoded = encoded.as_encoded().clone();
            longer_encoded.extend(&[0, 0, 0, 0, 0, 0, 0, 0, 0xFF]);
            let longer_encoded = Key::from_encoded(longer_encoded);
            assert!(!longer_encoded.is_encoded_from(&raw));

            // Should return false if raw is longer or shorter
            if !raw.is_empty() {
                let shorter_raw = &raw[..raw.len() - 1];
                assert!(!encoded.is_encoded_from(shorter_raw));
            }
            let mut longer_raw = raw.to_vec();
            longer_raw.push(0);
            assert!(!encoded.is_encoded_from(&longer_raw));
        }
    }

    #[test]
    fn test_old_value_resolved() {
        let cases = vec![
            (OldValue::Unspecified, false),
            (OldValue::None, true),
            (OldValue::Value { value: vec![] }, true),
            (OldValue::ValueTimeStamp { start_ts: 0.into() }, true),
        ];
        for (old_value, v) in cases {
            assert_eq!(old_value.resolved(), v);
        }
    }
}
