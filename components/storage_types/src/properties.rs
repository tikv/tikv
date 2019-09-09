// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::collections::Bound::{Included, Unbounded};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::u64;

use crate::mvcc::write::{Write, WriteType};
use keys::Key;
use engine::rocks::{
    CFHandle, DBEntryType, Range, TablePropertiesCollector, TablePropertiesCollectorFactory,
    TitanBlobIndex, UserCollectedProperties, DB,
};
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::{Error, Result};

const PROP_NUM_ERRORS: &str = "tikv.num_errors";
const PROP_MIN_TS: &str = "tikv.min_ts";
const PROP_MAX_TS: &str = "tikv.max_ts";
const PROP_NUM_ROWS: &str = "tikv.num_rows";
const PROP_NUM_PUTS: &str = "tikv.num_puts";
const PROP_NUM_VERSIONS: &str = "tikv.num_versions";
const PROP_MAX_ROW_VERSIONS: &str = "tikv.max_row_versions";
const PROP_ROWS_INDEX: &str = "tikv.rows_index";
const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;
const PROP_TOTAL_SIZE: &str = "tikv.total_size";
const PROP_SIZE_INDEX: &str = "tikv.size_index";
const PROP_RANGE_INDEX: &str = "tikv.range_index";
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

fn get_entry_size(value: &[u8], entry_type: DBEntryType) -> std::result::Result<u64, ()> {
    match entry_type {
        DBEntryType::Put => Ok(value.len() as u64),
        DBEntryType::BlobIndex => match TitanBlobIndex::decode(value) {
            Ok(index) => Ok(index.blob_size + value.len() as u64),
            Err(_) => Err(()),
        },
        _ => Err(()),
    }
}

#[derive(Clone, Debug, Default)]
pub struct MvccProperties {
    pub min_ts: u64,           // The minimal timestamp.
    pub max_ts: u64,           // The maximal timestamp.
    pub num_rows: u64,         // The number of rows.
    pub num_puts: u64,         // The number of MVCC puts of all rows.
    pub num_versions: u64,     // The number of MVCC versions of all rows.
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
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<MvccProperties> {
        let mut res = MvccProperties::new();
        res.min_ts = props.decode_u64(PROP_MIN_TS)?;
        res.max_ts = props.decode_u64(PROP_MAX_TS)?;
        res.num_rows = props.decode_u64(PROP_NUM_ROWS)?;
        res.num_puts = props.decode_u64(PROP_NUM_PUTS)?;
        res.num_versions = props.decode_u64(PROP_NUM_VERSIONS)?;
        res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
        Ok(res)
    }
}

pub struct MvccPropertiesCollector {
    props: MvccProperties,
    last_row: Vec<u8>,
    num_errors: u64,
    row_versions: u64,
    cur_index_handle: IndexHandle,
    row_index_handles: IndexHandles,
}

impl MvccPropertiesCollector {
    fn new() -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            props: MvccProperties::new(),
            last_row: Vec::new(),
            num_errors: 0,
            row_versions: 0,
            cur_index_handle: IndexHandle::default(),
            row_index_handles: IndexHandles::new(),
        }
    }
}

impl TablePropertiesCollector for MvccPropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        if entry_type != DBEntryType::Put {
            return;
        }

        if !keys::validate_data_key(key) {
            self.num_errors += 1;
            return;
        }

        let (k, ts) = match Key::split_on_ts_for(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        self.props.min_ts = cmp::min(self.props.min_ts, ts);
        self.props.max_ts = cmp::max(self.props.max_ts, ts);
        self.props.num_versions += 1;

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

        let write_type = match Write::parse_type(value) {
            Ok(v) => v,
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        if write_type == WriteType::Put {
            self.props.num_puts += 1;
        }

        // Add new row.
        if self.row_versions == 1 {
            self.cur_index_handle.size += 1;
            self.cur_index_handle.offset += 1;
            if self.cur_index_handle.offset == 1
                || self.cur_index_handle.size >= PROP_ROWS_INDEX_DISTANCE
            {
                self.row_index_handles
                    .insert(self.last_row.clone(), self.cur_index_handle.clone());
                self.cur_index_handle.size = 0;
            }
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        // Insert last handle.
        if self.cur_index_handle.size > 0 {
            self.row_index_handles
                .insert(self.last_row.clone(), self.cur_index_handle.clone());
        }
        let mut res = self.props.encode();
        res.encode_u64(PROP_NUM_ERRORS, self.num_errors);
        res.encode_handles(PROP_ROWS_INDEX, &self.row_index_handles);
        res.0
    }
}

#[derive(Default)]
pub struct MvccPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for MvccPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(MvccPropertiesCollector::new())
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexHandle {
    pub size: u64,   // The size of the stored block
    pub offset: u64, // The offset of the block in the file
}

#[derive(Debug, Default)]
pub struct IndexHandles(BTreeMap<Vec<u8>, IndexHandle>);

impl Deref for IndexHandles {
    type Target = BTreeMap<Vec<u8>, IndexHandle>;
    fn deref(&self) -> &BTreeMap<Vec<u8>, IndexHandle> {
        &self.0
    }
}

impl DerefMut for IndexHandles {
    fn deref_mut(&mut self) -> &mut BTreeMap<Vec<u8>, IndexHandle> {
        &mut self.0
    }
}

impl IndexHandles {
    pub fn new() -> IndexHandles {
        IndexHandles(BTreeMap::new())
    }

    pub fn add(&mut self, key: Vec<u8>, index_handle: IndexHandle) {
        self.0.insert(key, index_handle);
    }

    // Format: | klen | k | v.size | v.offset |
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        for (k, v) in &self.0 {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(v.size).unwrap();
            buf.encode_u64(v.offset).unwrap();
        }
        buf
    }

    fn decode(mut buf: &[u8]) -> Result<IndexHandles> {
        let mut res = BTreeMap::new();
        while !buf.is_empty() {
            let klen = number::decode_u64(&mut buf)?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let mut v = IndexHandle::default();
            v.size = number::decode_u64(&mut buf)?;
            v.offset = number::decode_u64(&mut buf)?;
            res.insert(k, v);
        }
        Ok(IndexHandles(res))
    }

    fn get_approximate_distance_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        assert!(start <= end);
        if start == end {
            return 0;
        }
        let range = self.range::<[u8], _>((Unbounded, Included(start)));
        let start_offset = match range.last() {
            Some((_, v)) => v.offset,
            None => 0,
        };
        let mut range = self.range::<[u8], _>((Included(end), Unbounded));
        let end_offset = match range.next() {
            Some((_, v)) => v.offset,
            None => self.iter().last().map_or(0, |(_, v)| v.offset),
        };
        if end_offset < start_offset {
            panic!(
                "start {:?} end {:?} start_offset {} end_offset {}",
                start, end, start_offset, end_offset
            );
        }
        end_offset - start_offset
    }
}

#[derive(Default)]
pub struct RowsProperties {
    pub total_rows: u64,
    pub index_handles: IndexHandles,
}

impl RowsProperties {
    pub fn decode<T: DecodeProperties>(props: &T) -> Result<RowsProperties> {
        let mut res = RowsProperties::default();
        res.total_rows = props.decode_u64(PROP_NUM_ROWS)?;
        res.index_handles = props.decode_handles(PROP_ROWS_INDEX)?;
        Ok(res)
    }

    pub fn get_approximate_rows_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.index_handles
            .get_approximate_distance_in_range(start, end)
    }
}

#[derive(Debug, Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub index_handles: IndexHandles,
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
        res.total_size = props.decode_u64(PROP_TOTAL_SIZE)?;
        res.index_handles = props.decode_handles(PROP_SIZE_INDEX)?;
        Ok(res)
    }

    pub fn get_approximate_size_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.index_handles
            .get_approximate_distance_in_range(start, end)
    }

    pub fn smallest_key(&self) -> Option<Vec<u8>> {
        self.index_handles
            .0
            .iter()
            .next()
            .map(|(key, _)| key.clone())
    }

    pub fn largest_key(&self) -> Option<Vec<u8>> {
        self.index_handles
            .0
            .iter()
            .last()
            .map(|(key, _)| key.clone())
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
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.index_handle.size += size;
        self.index_handle.offset += size as u64;
        // Add the start key for convenience.
        if self.last_key.is_empty() || self.index_handle.size >= DEFAULT_PROP_SIZE_INDEX_DISTANCE {
            self.props
                .index_handles
                .insert(key.to_owned(), self.index_handle.clone());
            self.index_handle.size = 0;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.props.total_size = self.index_handle.offset;
        if self.index_handle.size > 0 {
            self.props
                .index_handles
                .insert(self.last_key.clone(), self.index_handle.clone());
        }
        self.props.encode().0
    }
}

#[derive(Default)]
pub struct SizePropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for SizePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(SizePropertiesCollector::new())
    }
}

pub struct UserProperties(HashMap<Vec<u8>, Vec<u8>>);

impl Deref for UserProperties {
    type Target = HashMap<Vec<u8>, Vec<u8>>;
    fn deref(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.0
    }
}

impl DerefMut for UserProperties {
    fn deref_mut(&mut self) -> &mut HashMap<Vec<u8>, Vec<u8>> {
        &mut self.0
    }
}

impl UserProperties {
    fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    fn encode(&mut self, name: &str, value: Vec<u8>) {
        self.insert(name.as_bytes().to_owned(), value);
    }

    pub fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = Vec::with_capacity(8);
        buf.encode_u64(value).unwrap();
        self.encode(name, buf);
    }

    pub fn encode_handles(&mut self, name: &str, handles: &IndexHandles) {
        self.encode(name, handles.encode())
    }
}

pub trait DecodeProperties {
    fn decode(&self, k: &str) -> Result<&[u8]>;

    fn decode_u64(&self, k: &str) -> Result<u64> {
        let mut buf = self.decode(k)?;
        number::decode_u64(&mut buf)
    }

    fn decode_handles(&self, k: &str) -> Result<IndexHandles> {
        let buf = self.decode(k)?;
        IndexHandles::decode(buf)
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

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum RangeOffsetKind {
    Size,
    Keys,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOffsets {
    size: u64,
    keys: u64,
}

impl RangeOffsets {
    fn get(&self, kind: RangeOffsetKind) -> u64 {
        match kind {
            RangeOffsetKind::Keys => self.keys,
            RangeOffsetKind::Size => self.size,
        }
    }
}

#[derive(Debug, Default)]
pub struct RangeProperties {
    pub offsets: BTreeMap<Vec<u8>, RangeOffsets>,
}

impl RangeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut buf = Vec::with_capacity(1024);
        for (k, offsets) in &self.offsets {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(offsets.size).unwrap();
            buf.encode_u64(offsets.keys).unwrap();
        }
        let mut props = UserProperties::new();
        props.encode(PROP_RANGE_INDEX, buf);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<RangeProperties> {
        match RangeProperties::decode_from_range_properties(props) {
            Ok(res) => return Ok(res),
            Err(e) => debug!("old_version:decode to RangeProperties failed with err: {:?}, try to decode to SizeProperties", e),
        }
        SizeProperties::decode(props).map(|res| res.into())
    }

    fn decode_from_range_properties<T: DecodeProperties>(props: &T) -> Result<RangeProperties> {
        let mut res = RangeProperties::default();
        let mut buf = props.decode(PROP_RANGE_INDEX)?;
        while !buf.is_empty() {
            let klen = number::decode_u64(&mut buf)?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let mut offsets = RangeOffsets::default();
            offsets.size = number::decode_u64(&mut buf)?;
            offsets.keys = number::decode_u64(&mut buf)?;
            res.offsets.insert(k, offsets);
        }
        Ok(res)
    }

    pub fn get_approximate_size_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(RangeOffsetKind::Size, start, end)
    }

    pub fn get_approximate_keys_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(RangeOffsetKind::Keys, start, end)
    }

    fn get_approximate_distance_in_range(
        &self,
        kind: RangeOffsetKind,
        start: &[u8],
        end: &[u8],
    ) -> u64 {
        assert!(start <= end);
        if start == end {
            return 0;
        }

        let range = self.offsets.range::<[u8], _>((Unbounded, Included(start)));
        let start_offset = match range.last() {
            Some((_, v)) => v.get(kind),
            None => 0,
        };
        let range = self.offsets.range::<[u8], _>((Unbounded, Included(end)));
        let end_offset = match range.last() {
            Some((_, v)) => v.get(kind),
            None => 0,
        };
        if end_offset < start_offset {
            panic!(
                "start {:?} end {:?} start_offset {} end_offset {}",
                start, end, start_offset, end_offset
            );
        }
        end_offset - start_offset
    }

    pub fn smallest_key(&self) -> Option<Vec<u8>> {
        self.offsets.iter().next().map(|(key, _)| key.clone())
    }

    pub fn largest_key(&self) -> Option<Vec<u8>> {
        self.offsets.iter().last().map(|(key, _)| key.clone())
    }
}

impl From<SizeProperties> for RangeProperties {
    fn from(p: SizeProperties) -> RangeProperties {
        let mut res = RangeProperties::default();
        for (key, size_handle) in p.index_handles.0 {
            let mut range = RangeOffsets::default();
            range.size = size_handle.offset;
            res.offsets.insert(key, range);
        }
        res
    }
}

pub struct RangePropertiesCollector {
    props: RangeProperties,
    last_offsets: RangeOffsets,
    last_key: Vec<u8>,
    cur_offsets: RangeOffsets,
    prop_size_index_distance: u64,
    prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollector {
    fn default() -> Self {
        RangePropertiesCollector {
            props: RangeProperties::default(),
            last_offsets: RangeOffsets::default(),
            last_key: vec![],
            cur_offsets: RangeOffsets::default(),
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl RangePropertiesCollector {
    pub fn new(prop_size_index_distance: u64, prop_keys_index_distance: u64) -> Self {
        RangePropertiesCollector {
            prop_size_index_distance,
            prop_keys_index_distance,
            ..Default::default()
        }
    }

    fn size_in_last_range(&self) -> u64 {
        self.cur_offsets.size - self.last_offsets.size
    }

    fn keys_in_last_range(&self) -> u64 {
        self.cur_offsets.keys - self.last_offsets.keys
    }

    fn insert_new_point(&mut self, key: Vec<u8>) {
        self.last_offsets = self.cur_offsets.clone();
        self.props.offsets.insert(key, self.cur_offsets.clone());
    }
}

impl TablePropertiesCollector for RangePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // size
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.cur_offsets.size += size;
        // keys
        self.cur_offsets.keys += 1;
        // Add the start key for convenience.
        if self.last_key.is_empty()
            || self.size_in_last_range() >= self.prop_size_index_distance
            || self.keys_in_last_range() >= self.prop_keys_index_distance
        {
            self.insert_new_point(key.to_owned());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.size_in_last_range() > 0 || self.keys_in_last_range() > 0 {
            let key = self.last_key.clone();
            self.insert_new_point(key);
        }
        self.props.encode().0
    }
}

pub struct RangePropertiesCollectorFactory {
    pub prop_size_index_distance: u64,
    pub prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollectorFactory {
    fn default() -> Self {
        RangePropertiesCollectorFactory {
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl TablePropertiesCollectorFactory for RangePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(RangePropertiesCollector::new(
            self.prop_size_index_distance,
            self.prop_keys_index_distance,
        ))
    }
}

pub fn get_range_entries_and_versions(
    engine: &DB,
    cf: &CFHandle,
    start: &[u8],
    end: &[u8],
) -> Option<(u64, u64)> {
    let range = Range::new(start, end);
    let collection = match engine.get_properties_of_tables_in_range(cf, &[range]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    // Aggregate total MVCC properties and total number entries.
    let mut props = MvccProperties::new();
    let mut num_entries = 0;
    for (_, v) in &*collection {
        let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&mvcc);
    }

    Some((num_entries, props.num_versions))
}

#[cfg(test)]
mod tests {
    use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
    use engine::rocks::{DBEntryType, TablePropertiesCollector};
    use rand::Rng;
    use tempfile::Builder;
    use test::Bencher;

    use super::MvccPropertiesCollectorFactory;
    use crate::mvcc::write::{Write, WriteType};
    use keys::Key;
    use engine::rocks;
    use engine::rocks::util::CFOptions;
    use engine::{CF_WRITE, LARGE_CFS};

    use super::*;

    #[test]
    fn test_get_range_entries_and_versions() {
        let path = Builder::new()
            .prefix("_test_get_range_entries_and_versions")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(2).as_encoded());
            let write_cf = db.cf_handle(CF_WRITE).unwrap();
            db.put_cf(write_cf, &k1, b"v1").unwrap();
            db.delete_cf(write_cf, &k1).unwrap();
            let key = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(3).as_encoded());
            db.put_cf(write_cf, &key, b"v2").unwrap();
            db.flush_cf(write_cf, true).unwrap();
        }

        let start_keys = keys::data_key(&[]);
        let end_keys = keys::data_end_key(&[]);
        let cf = rocks::util::get_cf_handle(&db, CF_WRITE).unwrap();
        let (entries, versions) =
            get_range_entries_and_versions(&db, cf, &start_keys, &end_keys).unwrap();
        assert_eq!(entries, (cases.len() * 2) as u64);
        assert_eq!(versions, cases.len() as u64);
    }

    #[test]
    fn test_mvcc_properties() {
        let cases = [
            ("ab", 2, WriteType::Put, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Delete),
            ("cd", 5, WriteType::Delete, DBEntryType::Put),
            ("cd", 4, WriteType::Put, DBEntryType::Put),
            ("cd", 3, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Delete),
            ("gh", 7, WriteType::Delete, DBEntryType::Put),
        ];
        let mut collector = MvccPropertiesCollector::new();
        for &(key, ts, write_type, entry_type) in &cases {
            let k = Key::from_raw(key.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
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

    #[bench]
    fn bench_mvcc_properties(b: &mut Bencher) {
        let ts = 1;
        let num_entries = 100;
        let mut entries = Vec::new();
        for i in 0..num_entries {
            let s = format!("{:032}", i);
            let k = Key::from_raw(s.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let w = Write::new(WriteType::Put, ts, Some(s.as_bytes().to_owned()));
            entries.push((k, w.to_bytes()));
        }

        let mut collector = MvccPropertiesCollector::new();
        b.iter(|| {
            for &(ref k, ref v) in &entries {
                collector.add(k, v, DBEntryType::Put, 0, 0);
            }
        });
    }

    #[test]
    fn test_rows_properties() {
        let mut collector = MvccPropertiesCollector::new();
        let num_rows = PROP_ROWS_INDEX_DISTANCE * 3 + 2;
        for i in 0..num_rows {
            let key = format!("k-{}", i);
            let k1 = Key::from_raw(key.as_bytes()).append_ts(2);
            let k1 = keys::data_key(k1.as_encoded());
            let k2 = Key::from_raw(key.as_bytes()).append_ts(1);
            let k2 = keys::data_key(k2.as_encoded());
            let v = Write::new(WriteType::Put, 0, None).to_bytes();
            collector.add(&k1, &v, DBEntryType::Put, 0, 0);
            collector.add(&k2, &v, DBEntryType::Put, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = RowsProperties::decode(&result).unwrap();
        assert_eq!(props.total_rows, num_rows);
        assert_eq!(props.index_handles.len(), 5);
        let cases = [
            ("k-0", 1, 1),
            (
                "k-10000",
                PROP_ROWS_INDEX_DISTANCE,
                PROP_ROWS_INDEX_DISTANCE + 1,
            ),
            (
                "k-20000",
                PROP_ROWS_INDEX_DISTANCE,
                PROP_ROWS_INDEX_DISTANCE * 2 + 1,
            ),
            (
                "k-30000",
                PROP_ROWS_INDEX_DISTANCE,
                PROP_ROWS_INDEX_DISTANCE * 3 + 1,
            ),
            ("k-30001", 1, PROP_ROWS_INDEX_DISTANCE * 3 + 2),
        ];
        for &(key, size, offset) in &cases {
            let k = Key::from_raw(key.as_bytes());
            let k = keys::data_key(k.as_encoded());
            let h = &props.index_handles[&k];
            assert_eq!(h.size, size);
            assert_eq!(h.offset, offset);
        }

        let h: Vec<_> = props.index_handles.values().collect();
        let cases = [
            ("a", "z", h[4].offset),
            ("a", "a", 0),
            ("z", "z", 0),
            ("k-1", "k-10000", h[1].offset - h[0].offset),
            ("k-1", "k-20000", h[2].offset - h[0].offset),
            ("k-16666", "k-18888", h[2].offset - h[1].offset),
            ("k-16666", "k-26666", h[3].offset - h[1].offset),
        ];
        for &(start, end, rows) in &cases {
            let start = keys::data_key(start.as_bytes());
            let end = keys::data_key(end.as_bytes());
            assert_eq!(props.get_approximate_rows_in_range(&start, &end), rows);
        }
    }

    #[test]
    fn test_size_properties() {
        let cases = [
            ("a", 0),
            // handle "a": size = 1, offset = 1,
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            // handle "e": size = DISTANCE + 4, offset = DISTANCE + 5
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            // handle "i": size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE),
        ];
        // handle "k": size = DISTANCE / 8 * 12 + 2, offset = DISTANCE / 8 * 29 + 11

        let mut collector = SizePropertiesCollector::new();
        for &(k, vlen) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
        }
        for &(k, vlen) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = SizeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.total_size,
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 29 + 11
        );
        let handles = &props.index_handles;
        assert_eq!(handles.len(), 4);
        let a = &handles[b"a".as_ref()];
        assert_eq!(a.size, 1);
        assert_eq!(a.offset, 1);
        let e = &handles[b"e".as_ref()];
        assert_eq!(e.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 4);
        assert_eq!(e.offset, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 5);
        let i = &handles[b"i".as_ref()];
        assert_eq!(i.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4);
        assert_eq!(i.offset, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = &handles[b"k".as_ref()];
        assert_eq!(k.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 12 + 2);
        assert_eq!(k.offset, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 29 + 11);

        let cases = [
            (" ", "z", k.offset),
            (" ", " ", 0),
            ("z", "z", 0),
            ("a", "k", k.offset - a.offset),
            ("a", "i", i.offset - a.offset),
            ("e", "h", i.offset - e.offset),
            ("g", "h", i.offset - e.offset),
            ("g", "g", 0),
        ];
        for &(start, end, size) in &cases {
            assert_eq!(
                props.get_approximate_size_in_range(start.as_bytes(), end.as_bytes()),
                size
            );
        }
    }

    #[test]
    fn test_range_properties() {
        let cases = [
            ("a", 0, 1),
            // handle "a": size(size = 1, offset = 1),keys(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
            ("l", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            ("m", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            //handle "m": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset = 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("n", 1, DEFAULT_PROP_KEYS_INDEX_DISTANCE),
            //handle "n": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset = 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("o", 1, 1),
            // handle　"o": keys = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
        ];

        let mut collector = RangePropertiesCollector::default();
        for &(k, vlen, count) in &cases {
            let v = vec![0; vlen as usize];
            for _ in 0..count {
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            }
        }
        for &(k, vlen, _) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = RangeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
        assert_eq!(props.get_approximate_keys_in_range(b"", b"k"), 11 as u64);

        let handles = &props.offsets;
        assert_eq!(handles.len(), 7);
        let a = &handles[b"a".as_ref()];
        assert_eq!(a.size, 1);
        let e = &handles[b"e".as_ref()];
        assert_eq!(e.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 5);
        let i = &handles[b"i".as_ref()];
        assert_eq!(i.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = &handles[b"k".as_ref()];
        assert_eq!(k.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11);
        let m = &handles[b"m".as_ref()];
        assert_eq!(m.keys, 11 + DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let n = &handles[b"n".as_ref()];
        assert_eq!(n.keys, 11 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let o = &handles[b"o".as_ref()];
        assert_eq!(o.keys, 12 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let empty = RangeOffsets::default();
        let cases = [
            (" ", "k", k, &empty),
            (" ", " ", &empty, &empty),
            ("k", "k", k, k),
            ("a", "k", k, a),
            ("a", "i", i, a),
            ("e", "h", e, e),
            ("b", "h", e, a),
            ("g", "g", i, i),
        ];
        for &(start, end, end_idx, start_idx) in &cases {
            let size = end_idx.size - start_idx.size;
            assert_eq!(
                props.get_approximate_size_in_range(start.as_bytes(), end.as_bytes()),
                size
            );
            let keys = end_idx.keys - start_idx.keys;
            assert_eq!(
                props.get_approximate_keys_in_range(start.as_bytes(), end.as_bytes()),
                keys
            );
        }
    }

    #[test]
    fn test_range_properties_with_blob_index() {
        let cases = [
            ("a", 0),
            // handle "a": size(size = 1, offset = 1),keys(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
        ];

        let handles = ["a", "e", "i", "k"];

        let mut rng = rand::thread_rng();
        let mut collector = RangePropertiesCollector::default();
        let mut extra_value_size: u64 = 0;
        for &(k, vlen) in &cases {
            if handles.contains(&k) || rng.gen_range(0, 2) == 0 {
                let v = vec![0; vlen as usize - extra_value_size as usize];
                extra_value_size = 0;
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            } else {
                let mut blob_index = TitanBlobIndex::default();
                blob_index.blob_size = vlen - extra_value_size;
                let v = blob_index.encode();
                extra_value_size = v.len() as u64;
                collector.add(k.as_bytes(), &v, DBEntryType::BlobIndex, 0, 0);
            }
        }
        let result = UserProperties(collector.finish());

        let props = RangeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"e", b"i"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
    }
}
