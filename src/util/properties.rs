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
const PROP_SIZE_INDEX: &'static str = "tikv.size_index";

const PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;

#[derive(Default)]
pub struct UserProperties {
    pub num_errors: u64,
    pub mvcc: Option<MvccProperties>,
    pub size_index: Option<SizeIndexProperties>,
}

#[derive(Clone, Debug, Default)]
pub struct MvccProperties {
    pub min_ts: u64, // The minimal timestamp.
    pub max_ts: u64, // The maximal timestamp.
    pub num_rows: u64, // The number of rows.
    pub num_puts: u64, // The number of MVCC puts of all rows.
    pub num_versions: u64, // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single row.
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
        }
    }

    pub fn add(&mut self, other: &MvccProperties) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_versions += other.num_versions;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
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
        Ok(res)
    }
}

pub struct MvccPropertiesCollector {
    props: MvccProperties,
    last_row: Vec<u8>,
    row_versions: u64,
    size_handle: IndexHandle,
}

impl MvccPropertiesCollector {
    fn new() -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            props: MvccProperties::new(),
            last_row: Vec::new(),
            row_versions: 0,
            size_handle: IndexHandle::default(),
        }
    }

    fn mvcc_add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType) {
        if !keys::validate_data_key(key) {
            self.num_errors += 1;
            return;
        }

        let (k, ts) = match types::split_encoded_key_on_ts(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        self.mvcc.min_ts = cmp::min(self.mvcc.min_ts, ts);
        self.mvcc.max_ts = cmp::max(self.mvcc.max_ts, ts);
        match entry_type {
            DBEntryType::Put => self.mvcc.num_versions += 1,
            _ => return,
        }

        if !self.last_key.as_slice().starts_with(k) {
            self.mvcc.num_rows += 1;
            self.row_versions = 1;
            self.last_row.clear();
            self.last_row.extend(k);
        } else {
            self.row_versions += 1;
        }
        if self.row_versions > self.mvcc.max_row_versions {
            self.mvcc.max_row_versions = self.row_versions;
        }

        let v = match Write::parse(value) {
            Ok(v) => v,
            Err(_) => {
                self.num_errors += 1;
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
}

pub trait DecodeProperties {
    fn decode(&self, k: &str) -> Result<&[u8]>;

    fn decode_u64(&self, k: &str) -> Result<u64> {
        let mut buf = try!(self.decode(k));
        buf.decode_u64()
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
    fn test_mvcc_properties() {
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
    fn test_size_index_properties() {
        // handle "a": size = 0, offset = 0
        let cases = [("a", PROP_SIZE_INDEX_DISTANCE / 8),
                     ("b", PROP_SIZE_INDEX_DISTANCE / 4),
                     ("c", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("d", PROP_SIZE_INDEX_DISTANCE / 8),
                     // handle "d": size = DISTANCE + 4, offset = DISTANCE + 4
                     ("e", PROP_SIZE_INDEX_DISTANCE / 4),
                     ("f", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("g", PROP_SIZE_INDEX_DISTANCE / 8),
                     ("h", PROP_SIZE_INDEX_DISTANCE / 4),
                     // handle "h": size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 8
                     ("i", PROP_SIZE_INDEX_DISTANCE / 2),
                     ("j", PROP_SIZE_INDEX_DISTANCE)];
        // handle "j": size = DISTANCE / 8 * 12 + 2, offset = DISTANCE / 8 * 29 + 10

        let mut collector = UserPropertiesCollector::new(SIZE_INDEX_PROPERTIES);
        for &(k, vlen) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
        }

        let props = SizeIndexProperties::decode(&collector.finish()).unwrap();
        assert_eq!(props.handles.len(), 4);
        let a = &props.handles[b"a".as_ref()];
        assert_eq!(a.size, 0);
        assert_eq!(a.offset, 0);
        let d = &props.handles[b"d".as_ref()];
        assert_eq!(d.size, PROP_SIZE_INDEX_DISTANCE + 4);
        assert_eq!(d.offset, PROP_SIZE_INDEX_DISTANCE + 4);
        let h = &props.handles[b"h".as_ref()];
        assert_eq!(h.size, PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4);
        assert_eq!(h.offset, PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 8);
        let j = &props.handles[b"j".as_ref()];
        assert_eq!(j.size, PROP_SIZE_INDEX_DISTANCE / 8 * 12 + 2);
        assert_eq!(j.offset, PROP_SIZE_INDEX_DISTANCE / 8 * 29 + 10);
    }
}
