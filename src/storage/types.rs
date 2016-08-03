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
use std::fmt::{self, Formatter, Display};
use std::u64;

use util::{escape, codec};
use util::codec::number::{self, NumberEncoder};
use util::codec::bytes::BytesDecoder;
use super::mvcc;

pub type Value = Vec<u8>;
pub type KvPair = (Vec<u8>, Value);

#[derive(Debug, Clone)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn from_raw(key: &[u8]) -> Key {
        Key(codec::bytes::encode_bytes(key))
    }

    pub fn raw(&self) -> Result<Vec<u8>, codec::Error> {
        self.0.as_slice().decode_bytes(false)
    }

    pub fn from_encoded(encoded_key: Vec<u8>) -> Key {
        Key(encoded_key)
    }

    pub fn encoded(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn append_ts(&self, ts: u64) -> Key {
        let mut encoded = self.0.clone();
        if ts == mvcc::FIRST_META_INDEX || ts == u64::MAX {
            encoded.encode_u64(ts).unwrap();
        } else {
            encoded.encode_u64_desc(ts).unwrap();
        }
        Key(encoded)
    }

    pub fn truncate_ts(&self) -> Result<Key, codec::Error> {
        let len = self.0.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        Ok(Key::from_encoded(self.0[..len - number::U64_SIZE].to_vec()))
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encoded().hash(state)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", escape(&self.0))
    }
}

pub fn make_key(k: &[u8]) -> Key {
    Key::from_raw(k)
}
