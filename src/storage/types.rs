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

use std::hash::{Hash, Hasher};
use byteorder::{BigEndian, WriteBytesExt};

pub type Value = Vec<u8>;
pub type KvPair = (Vec<u8>, Value);

#[derive(Debug, Clone)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn from_raw(key: Vec<u8>) -> Key {
        Key(key)
    }

    pub fn raw(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn encode_ts(&self, ts: u64) -> Key {
        let mut encoded = self.0.clone();
        encoded.write_u64::<BigEndian>(ts).unwrap();
        Key(encoded)
    }

    /// Returns a key that can be used for `seek` next key.
    pub fn seek_key(&self) -> Key {
        self.encode_ts(u64::max_value())
    }

    /// Returns a key that can be used for `reverse_seek` next key.
    pub fn reverse_seek_key(&self) -> Key {
        let mut encoded = self.0.clone();
        let len = encoded.len();
        // It won't overflow since for a encoded key, last char will be the the PADDING_MARK, which
        // will be not less than '0xFF - 8'.
        // TODO: This is not elegant without doubt. We may re-arrange key layout to make it easier
        // to do reverse seek.
        encoded[len - 1] -= 1u8;
        Key(encoded)
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw().hash(state)
    }
}

#[cfg(test)]
pub fn make_key(k: &[u8]) -> Key {
    use util::codec::bytes;
    Key::from_raw(bytes::encode_bytes(k))
}
