// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use std::fmt::{self, Display, Formatter};
use std::u64;

use byteorder::{ByteOrder, NativeEndian};
use hex::ToHex;

use tikv_util::codec;
use tikv_util::codec::bytes;
use tikv_util::codec::bytes::BytesEncoder;
use tikv_util::codec::number::{self, NumberEncoder};

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
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    pub fn append_ts(self, ts: u64) -> Key {
        let mut encoded = self.0;
        encoded.encode_u64_desc(ts).unwrap();
        Key(encoded)
    }

    /// Gets the timestamp contained in this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped
    /// key.
    #[inline]
    pub fn decode_ts(&self) -> Result<u64, codec::Error> {
        Ok(Self::decode_ts_from(&self.0)?)
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
    pub fn split_on_ts_for(key: &[u8]) -> Result<(&[u8], u64), codec::Error> {
        if key.len() < number::U64_SIZE {
            Err(codec::Error::KeyLength)
        } else {
            let pos = key.len() - number::U64_SIZE;
            let k = &key[..pos];
            let mut ts = &key[pos..];
            Ok((k, number::decode_u64_desc(&mut ts)?))
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
    pub fn decode_ts_from(key: &[u8]) -> Result<u64, codec::Error> {
        let len = key.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        let mut ts = &key[len - number::U64_SIZE..];
        number::decode_u64_desc(&mut ts)
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
}