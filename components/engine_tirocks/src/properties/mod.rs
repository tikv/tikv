// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod mvcc;
mod range;
mod table;
mod ttl;

use std::{
    cmp,
    collections::BTreeMap,
    io::Read,
    ops::{Deref, DerefMut},
};

use codec::{
    number::NumberCodec,
    prelude::{NumberDecoder, NumberEncoder},
};
use collections::HashMap;
use tirocks::properties::table::user::UserCollectedProperties;

pub use self::{
    mvcc::MvccPropertiesCollectorFactory,
    range::{RangeProperties, RangePropertiesCollectorFactory},
    table::{RocksTablePropertiesCollection, RocksUserCollectedProperties},
    ttl::TtlPropertiesCollectorFactory,
};

/// A struct to help collect properties.
///
/// The properties of a file can be collected by ranges. Every range will be
/// referenced by a `PropIndex`.
#[derive(Clone, Debug, Default)]
pub struct PropIndex {
    /// The properties calculated from the range. The range starts from
    /// `offset` of previous `PropIndex` to this `offset`. How large the range
    /// is depends on the implementation.
    pub prop: u64,
    /// The offset in the file. Offsets are not necessary the size of file. It
    /// only makes sense to the implementations.
    pub offset: u64,
}

#[derive(Debug, Default)]
pub struct PropIndexes(BTreeMap<Vec<u8>, PropIndex>);

impl Deref for PropIndexes {
    type Target = BTreeMap<Vec<u8>, PropIndex>;
    fn deref(&self) -> &BTreeMap<Vec<u8>, PropIndex> {
        &self.0
    }
}

impl DerefMut for PropIndexes {
    fn deref_mut(&mut self) -> &mut BTreeMap<Vec<u8>, PropIndex> {
        &mut self.0
    }
}

impl PropIndexes {
    pub fn new() -> PropIndexes {
        PropIndexes(BTreeMap::new())
    }

    pub fn into_map(self) -> BTreeMap<Vec<u8>, PropIndex> {
        self.0
    }

    pub fn add(&mut self, key: Vec<u8>, index: PropIndex) {
        self.0.insert(key, index);
    }

    // Format: | klen | k | v.size | v.offset |
    pub fn encode(&self) -> Vec<u8> {
        let cap = cmp::min((8 * 3 + 24) * self.0.len(), 1024);
        let mut buf = Vec::with_capacity(cap);
        for (k, v) in &self.0 {
            buf.write_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.write_u64(v.prop).unwrap();
            buf.write_u64(v.offset).unwrap();
        }
        buf
    }

    pub fn decode(mut buf: &[u8]) -> codec::Result<PropIndexes> {
        let mut res = BTreeMap::new();
        while !buf.is_empty() {
            let klen = buf.read_u64()?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let v = PropIndex {
                prop: buf.read_u64()?,
                offset: buf.read_u64()?,
            };
            res.insert(k, v);
        }
        Ok(PropIndexes(res))
    }
}

trait EncodeProperties {
    fn encode(&mut self, name: &str, value: &[u8]);

    #[inline]
    fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = [0; 8];
        NumberCodec::encode_u64(&mut buf, value);
        self.encode(name, &buf);
    }

    #[inline]
    fn encode_indexes(&mut self, name: &str, indexes: &PropIndexes) {
        self.encode(name, &indexes.encode());
    }
}

impl EncodeProperties for UserCollectedProperties {
    #[inline]
    fn encode(&mut self, name: &str, value: &[u8]) {
        self.add(name.as_bytes(), value);
    }
}

impl EncodeProperties for HashMap<Vec<u8>, Vec<u8>> {
    #[inline]
    fn encode(&mut self, name: &str, value: &[u8]) {
        self.insert(name.as_bytes().to_owned(), value.to_owned());
    }
}

trait DecodeProperties {
    fn decode(&self, k: &str) -> codec::Result<&[u8]>;

    #[inline]
    fn decode_u64(&self, k: &str) -> codec::Result<u64> {
        let mut buf = self.decode(k)?;
        buf.read_u64()
    }

    #[inline]
    fn decode_indexes(&self, k: &str) -> codec::Result<PropIndexes> {
        let buf = self.decode(k)?;
        PropIndexes::decode(buf)
    }
}

impl DecodeProperties for UserCollectedProperties {
    #[inline]
    fn decode(&self, k: &str) -> codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or_else(|| codec::ErrorInner::KeyNotFound.into())
    }
}

impl DecodeProperties for HashMap<Vec<u8>, Vec<u8>> {
    #[inline]
    fn decode(&self, k: &str) -> codec::Result<&[u8]> {
        match self.get(k.as_bytes()) {
            Some(v) => Ok(v.as_slice()),
            None => Err(codec::ErrorInner::KeyNotFound.into()),
        }
    }
}
