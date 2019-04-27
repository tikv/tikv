use engine::rocks::{
    DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
    TitanBlobIndex, UserCollectedProperties,
};
use std::collections::Bound::{Included, Unbounded};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::ops::{Deref, DerefMut};
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::{Error, Result};

pub const PROP_NUM_ERRORS: &str = "tikv.num_errors";
pub const PROP_MIN_TS: &str = "tikv.min_ts";
pub const PROP_MAX_TS: &str = "tikv.max_ts";
pub const PROP_NUM_ROWS: &str = "tikv.num_rows";
pub const PROP_NUM_PUTS: &str = "tikv.num_puts";
pub const PROP_NUM_VERSIONS: &str = "tikv.num_versions";
pub const PROP_MAX_ROW_VERSIONS: &str = "tikv.max_row_versions";
pub const PROP_ROWS_INDEX: &str = "tikv.rows_index";
pub const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;
pub const PROP_TOTAL_SIZE: &str = "tikv.total_size";
pub const PROP_SIZE_INDEX: &str = "tikv.size_index";
pub const PROP_RANGE_INDEX: &str = "tikv.range_index";

pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

pub fn get_entry_size(value: &[u8], entry_type: DBEntryType) -> std::result::Result<u64, ()> {
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
pub struct IndexHandle {
    pub size: u64,   // The size of the stored block
    pub offset: u64, // The offset of the block in the file
}

#[derive(Debug, Default)]
pub struct IndexHandles(pub BTreeMap<Vec<u8>, IndexHandle>);

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
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        for (k, v) in &self.0 {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(v.size).unwrap();
            buf.encode_u64(v.offset).unwrap();
        }
        buf
    }

    pub fn decode(mut buf: &[u8]) -> Result<IndexHandles> {
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

    pub fn get_approximate_distance_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
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

pub struct UserProperties(pub HashMap<Vec<u8>, Vec<u8>>);

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
    pub fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    pub fn encode(&mut self, name: &str, value: Vec<u8>) {
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
    pub size: u64,
    pub keys: u64,
}

impl RangeOffsets {
    pub fn get(&self, kind: RangeOffsetKind) -> u64 {
        match kind {
            RangeOffsetKind::Keys => self.keys,
            RangeOffsetKind::Size => self.size,
        }
    }
}

