// Copyright 2017 PingCAP, Inc.
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

use std::cmp;
use std::collections::{HashMap, BTreeMap};
use std::collections::Bound::{Included, Unbounded};
use std::u64;
use std::io::Read;

use storage::mvcc::{Write, WriteType};
use storage::types;
use raftstore::store::keys;
use rocksdb::{DBEntryType, UserCollectedProperties, TablePropertiesCollector,
              TablePropertiesCollectorFactory};
use util::codec::{Error, Result};
use util::codec::number::{NumberEncoder, NumberDecoder};

const PROP_MIN_TS: &'static str = "tikv.min_ts";
const PROP_MAX_TS: &'static str = "tikv.max_ts";
const PROP_NUM_ROWS: &'static str = "tikv.num_rows";
const PROP_NUM_PUTS: &'static str = "tikv.num_puts";
const PROP_NUM_VERSIONS: &'static str = "tikv.num_versions";
const PROP_MAX_ROW_VERSIONS: &'static str = "tikv.max_row_versions";
const PROP_NUM_ERRORS: &'static str = "tikv.num_errors";
const PROP_TOTAL_SIZE: &'static str = "tikv.total_size";
const PROP_SIZE_INDEX: &'static str = "tikv.size_index";
const PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct MvccProperties {
    pub min_ts: u64, // The minimal timestamp.
    pub max_ts: u64, // The maximal timestamp.
    pub num_rows: u64, // The number of rows.
    pub num_puts: u64, // The number of MVCC puts of all rows.
    pub num_versions: u64, // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single row.
    pub num_errors: u64,
}

impl MvccProperties {
    pub fn new() -> MvccProperties {
        MvccProperties {
            min_ts: u64::MAX,
            max_ts: u64::MIN,
            num_rows: 0,
            num_puts: 0,
            num_versions: 0,
            max_row_versions: 0,
            num_errors: 0,
        }
    }

    pub fn add(&mut self, other: &MvccProperties) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_versions += other.num_versions;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
        self.num_errors += other.num_errors;
    }

    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MIN_TS, self.min_ts);
        props.encode_u64(PROP_MAX_TS, self.max_ts);
        props.encode_u64(PROP_NUM_ROWS, self.num_rows);
        props.encode_u64(PROP_NUM_PUTS, self.num_puts);
        props.encode_u64(PROP_NUM_VERSIONS, self.num_versions);
        props.encode_u64(PROP_MAX_ROW_VERSIONS, self.max_row_versions);
        props.encode_u64(PROP_NUM_ERRORS, self.num_errors);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<MvccProperties> {
        let mut res = MvccProperties::new();
        res.min_ts = try!(props.decode_u64(PROP_MIN_TS));
        res.max_ts = try!(props.decode_u64(PROP_MAX_TS));
        res.num_rows = try!(props.decode_u64(PROP_NUM_ROWS));
        res.num_puts = try!(props.decode_u64(PROP_NUM_PUTS));
        res.num_versions = try!(props.decode_u64(PROP_NUM_VERSIONS));
        res.max_row_versions = try!(props.decode_u64(PROP_MAX_ROW_VERSIONS));
        res.num_errors = try!(props.decode_u64(PROP_NUM_ERRORS));
        Ok(res)
    }
}

pub struct MvccPropertiesCollector {
    props: MvccProperties,
    last_row: Vec<u8>,
    row_versions: u64,
}

impl MvccPropertiesCollector {
    fn new() -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            props: MvccProperties::new(),
            last_row: Vec::new(),
            row_versions: 0,
        }
    }
}

impl TablePropertiesCollector for MvccPropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        if !keys::validate_data_key(key) {
            self.props.num_errors += 1;
            return;
        }

        let (k, ts) = match types::split_encoded_key_on_ts(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.props.num_errors += 1;
                return;
            }
        };

        self.props.min_ts = cmp::min(self.props.min_ts, ts);
        self.props.max_ts = cmp::max(self.props.max_ts, ts);
        match entry_type {
            DBEntryType::Put => self.props.num_versions += 1,
            _ => return,
        }

        if k != self.last_row.as_slice() {
            self.props.num_rows += 1;
            self.row_versions = 1;
            self.last_row.clear();
            self.last_row.extend(k);
        } else {
            self.row_versions += 1;
        }
        if self.row_versions > self.props.max_row_versions {
            self.props.max_row_versions = self.row_versions;
        }

        let v = match Write::parse(value) {
            Ok(v) => v,
            Err(_) => {
                self.props.num_errors += 1;
                return;
            }
        };

        if v.write_type == WriteType::Put {
            self.props.num_puts += 1;
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.props.encode().0
    }
}

#[derive(Default)]
pub struct MvccPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for MvccPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<TablePropertiesCollector> {
        Box::new(MvccPropertiesCollector::new())
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexHandle {
    pub size: u64, // The size of the stored block
    pub offset: u64, // The offset of the block in the file
}

#[derive(Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub index_handles: BTreeMap<Vec<u8>, IndexHandle>,
}

impl SizeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_TOTAL_SIZE, self.total_size);
        props.encode_handles(PROP_SIZE_INDEX, &self.index_handles);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<SizeProperties> {
        let mut res = SizeProperties::default();
        res.total_size = try!(props.decode_u64(PROP_TOTAL_SIZE));
        res.index_handles = try!(props.decode_handles(PROP_SIZE_INDEX));
        Ok(res)
    }

    pub fn get_approximate_size_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        let mut range = self.index_handles.range::<[u8], _>((Included(start), Unbounded));
        let start_offset = match range.next() {
            Some((_, v)) => v.offset,
            None => return 0,
        };
        let mut range = self.index_handles.range::<[u8], _>((Included(end), Unbounded));
        let end_offset = match range.next() {
            Some((_, v)) => v.offset,
            None => {
                // Last handle must exists if we have start offset.
                let (_, v) = self.index_handles.iter().last().unwrap();
                v.offset
            }
        };
        assert!(end_offset >= start_offset);
        end_offset - start_offset
    }
}

pub struct SizePropertiesCollector {
    props: SizeProperties,
    last_key: Vec<u8>,
    index_handle: IndexHandle,
}

impl SizePropertiesCollector {
    fn new() -> SizePropertiesCollector {
        SizePropertiesCollector {
            props: SizeProperties::default(),
            last_key: Vec::new(),
            index_handle: IndexHandle::default(),
        }
    }
}

impl TablePropertiesCollector for SizePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], _: DBEntryType, _: u64, _: u64) {
        let size = key.len() + value.len();
        self.index_handle.size += size as u64;
        self.index_handle.offset += size as u64;
        // Add the start key for convenience.
        if self.last_key.is_empty() || self.index_handle.size >= PROP_SIZE_INDEX_DISTANCE {
            self.props.index_handles.insert(key.to_owned(), self.index_handle.clone());
            self.index_handle.size = 0;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.props.total_size = self.index_handle.offset;
        if self.index_handle.size > 0 {
            self.props.index_handles.insert(self.last_key.clone(), self.index_handle.clone());
        }
        self.props.encode().0
    }
}

#[derive(Default)]
pub struct SizePropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for SizePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<TablePropertiesCollector> {
        Box::new(SizePropertiesCollector::new())
    }
}

pub struct UserProperties(HashMap<Vec<u8>, Vec<u8>>);

impl UserProperties {
    fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    fn encode(&mut self, name: &str, value: Vec<u8>) {
        self.0.insert(name.as_bytes().to_owned(), value);
    }

    pub fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = Vec::with_capacity(8);
        buf.encode_u64(value).unwrap();
        self.encode(name, buf);
    }

    // Format: | klen | k | v.size | v.offset |
    pub fn encode_handles(&mut self, name: &str, handles: &BTreeMap<Vec<u8>, IndexHandle>) {
        let mut buf = Vec::with_capacity(1024);
        for (k, v) in handles {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(v.size).unwrap();
            buf.encode_u64(v.offset).unwrap();
        }
        self.encode(name, buf);
    }
}

pub trait DecodeProperties {
    fn decode(&self, k: &str) -> Result<&[u8]>;

    fn decode_u64(&self, k: &str) -> Result<u64> {
        let mut buf = try!(self.decode(k));
        buf.decode_u64()
    }

    fn decode_handles(&self, k: &str) -> Result<BTreeMap<Vec<u8>, IndexHandle>> {
        let mut res = BTreeMap::new();
        let mut buf = try!(self.decode(k));
        while !buf.is_empty() {
            let klen = try!(buf.decode_u64());
            let mut k = vec![0; klen as usize];
            try!(buf.read_exact(&mut k));
            let mut v = IndexHandle::default();
            v.size = try!(buf.decode_u64());
            v.offset = try!(buf.decode_u64());
            res.insert(k, v);
        }
        Ok(res)
    }
}

impl DecodeProperties for UserProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v.as_slice()),
            None => Err(Error::KeyNotFound),
        }
    }
}

impl DecodeProperties for UserCollectedProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.get(k.as_bytes()) {
            Some(v) => Ok(v),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{DBEntryType, TablePropertiesCollector};
    use storage::Key;
    use storage::mvcc::{Write, WriteType};
    use raftstore::store::keys;

    #[test]
    fn test_mvcc_properties_collector() {
        let cases = [("ab", 2, WriteType::Put, DBEntryType::Put),
                     ("ab", 1, WriteType::Delete, DBEntryType::Put),
                     ("ab", 1, WriteType::Delete, DBEntryType::Delete),
                     ("cd", 5, WriteType::Delete, DBEntryType::Put),
                     ("cd", 4, WriteType::Put, DBEntryType::Put),
                     ("cd", 3, WriteType::Put, DBEntryType::Put),
                     ("ef", 6, WriteType::Put, DBEntryType::Put),
                     ("ef", 6, WriteType::Put, DBEntryType::Delete),
                     ("gh", 7, WriteType::Delete, DBEntryType::Put)];
        let mut collector = MvccPropertiesCollector::new();
        for &(key, ts, write_type, entry_type) in &cases {
            let k = Key::from_raw(key.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.encoded());
            let v = Write::new(write_type, ts, None).to_bytes();
            collector.add(&k, &v, entry_type, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = MvccProperties::decode(&result).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 7);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
    }

    #[test]
    fn test_size_properties_collector() {
        let cases = [("a", 0),
                     // handle "a": size = 1, offset = 1,
                     ("b", PROP_SIZE_INDEX_DISTANCE / 8),
                     ("c", PROP_SIZE_INDEX_DISTANCE / 4),
                     ("d", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("e", PROP_SIZE_INDEX_DISTANCE / 8),
                     // handle "e": size = DISTANCE + 4, offset = DISTANCE + 5
                     ("f", PROP_SIZE_INDEX_DISTANCE / 4),
                     ("g", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("h", PROP_SIZE_INDEX_DISTANCE / 8),
                     ("i", PROP_SIZE_INDEX_DISTANCE / 4),
                     // handle "i": size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9
                     ("j", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("k", PROP_SIZE_INDEX_DISTANCE)];
        // handle "k": size = DISTANCE / 8 * 12 + 2, offset = DISTANCE / 8 * 29 + 11

        let mut collector = SizePropertiesCollector::new();
        for &(k, vlen) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = SizeProperties::decode(&result).unwrap();
        assert_eq!(props.total_size, PROP_SIZE_INDEX_DISTANCE / 8 * 29 + 11);
        let handles = &props.index_handles;
        assert_eq!(handles.len(), 4);
        let a = &handles[b"a".as_ref()];
        assert_eq!(a.size, 1);
        assert_eq!(a.offset, 1);
        let e = &handles[b"e".as_ref()];
        assert_eq!(e.size, PROP_SIZE_INDEX_DISTANCE + 4);
        assert_eq!(e.offset, PROP_SIZE_INDEX_DISTANCE + 5);
        let i = &handles[b"i".as_ref()];
        assert_eq!(i.size, PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4);
        assert_eq!(i.offset, PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = &handles[b"k".as_ref()];
        assert_eq!(k.size, PROP_SIZE_INDEX_DISTANCE / 8 * 12 + 2);
        assert_eq!(k.offset, PROP_SIZE_INDEX_DISTANCE / 8 * 29 + 11);

        let cases = [(" ", "z", k.offset - a.offset),
                     (" ", " ", 0),
                     ("z", "z", 0),
                     ("a", "k", k.offset - a.offset),
                     ("a", "i", i.offset - a.offset),
                     ("e", "h", i.offset - e.offset),
                     ("g", "h", 0),
                     ("g", "g", 0)];
        for &(start, end, size) in &cases {
            assert_eq!(props.get_approximate_size_in_range(start.as_bytes(), end.as_bytes()),
                       size);
        }
    }
}
