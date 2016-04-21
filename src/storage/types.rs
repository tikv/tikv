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
use std::mem;
use byteorder::{BigEndian, WriteBytesExt};
use util::codec;

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

    pub fn truncate_ts(&self) -> Result<Key, codec::Error> {
        let len = self.0.len();
        if len < mem::size_of::<u64>() {
            return Err(codec::Error::KeyLength);
        }
        Ok(Key::from_raw(self.0[..len - mem::size_of::<u64>()].to_owned()))
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
