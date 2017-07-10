// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core data types.

use std::hash::{Hash, Hasher};
use std::fmt::{self, Formatter, Display};
use std::u64;

use util::{escape, codec};
use util::codec::number::{self, NumberEncoder, NumberDecoder};
use util::codec::bytes::BytesDecoder;

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
#[derive(Debug, Clone)]
pub struct Key(Vec<u8>);

/// Core functions for `Key`.
impl Key {
    /// Creates a key from raw bytes.
    pub fn from_raw(key: &[u8]) -> Key {
        Key(codec::bytes::encode_bytes(key))
    }

    /// Gets the raw representation of this key.
    pub fn raw(&self) -> Result<Vec<u8>, codec::Error> {
        self.0.as_slice().decode_bytes(false)
    }

    /// Creates a key from encoded bytes.
    pub fn from_encoded(encoded_key: Vec<u8>) -> Key {
        Key(encoded_key)
    }

    /// Gets the encoded representation of this key.
    pub fn encoded(&self) -> &Vec<u8> {
        &self.0
    }

    /// Creates a new key by appending a `u64` timestamp to this key.
    pub fn append_ts(&self, ts: u64) -> Key {
        let mut encoded = self.0.clone();
        encoded.encode_u64_desc(ts).unwrap();
        Key(encoded)
    }

    /// Gets the timestamp contained in this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped
    /// key.
    pub fn decode_ts(&self) -> Result<u64, codec::Error> {
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
            let mut ts = &self.0[len - number::U64_SIZE..];
            Ok(try!(ts.decode_u64_desc()))
        }
    }

    /// Creates a new key by truncating the timestamp from this key.
    ///
    /// Preconditions: the caller must ensure this is actually a timestamped key.
    pub fn truncate_ts(&self) -> Result<Key, codec::Error> {
        let len = self.0.len();
        if len < number::U64_SIZE {
            // TODO: (the same as above)
            return Err(codec::Error::KeyLength);
        }
        Ok(Key::from_encoded(self.0[..len - number::U64_SIZE].to_vec()))
    }
}

/// Hash for `Key`.
impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encoded().hash(state)
    }
}

/// Display for `Key`.
impl Display for Key {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", escape(&self.0))
    }
}

/// Partial equality for `Key`.
impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.0 == other.0
    }
}

/// Creates a new key from raw bytes.
pub fn make_key(k: &[u8]) -> Key {
    Key::from_raw(k)
}

/// Splits encoded key on timestamp.
/// Returns the split key and timestamp.
pub fn split_encoded_key_on_ts(key: &[u8]) -> Result<(&[u8], u64), codec::Error> {
    if key.len() < number::U64_SIZE {
        Err(codec::Error::KeyLength)
    } else {
        let pos = key.len() - number::U64_SIZE;
        let k = &key[..pos];
        let mut ts = &key[pos..];
        Ok((k, try!(ts.decode_u64_desc())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ts() {
        let k = b"k";
        let ts = 123;
        assert!(split_encoded_key_on_ts(k).is_err());
        let enc = Key::from_encoded(k.to_vec()).append_ts(ts);
        let res = split_encoded_key_on_ts(enc.encoded()).unwrap();
        assert_eq!(res, (k.as_ref(), ts));
    }
}
