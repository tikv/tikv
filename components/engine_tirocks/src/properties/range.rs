// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ffi::CStr, io::Read, path::Path};

use codec::prelude::{NumberDecoder, NumberEncoder};
use engine_traits::{MvccProperties, Range, Result, CF_DEFAULT, CF_LOCK, CF_WRITE, LARGE_CFS};
use tikv_util::{box_err, box_try, debug, info};
use tirocks::{
    properties::table::user::{
        Context, EntryType, SequenceNumber, TablePropertiesCollector,
        TablePropertiesCollectorFactory, UserCollectedProperties,
    },
    titan::TitanBlobIndex,
};

use super::{mvcc::decode_mvcc, DecodeProperties, EncodeProperties, PropIndexes};
use crate::RocksEngine;

const PROP_TOTAL_SIZE: &str = "tikv.total_size";
const PROP_SIZE_INDEX: &str = "tikv.size_index";
const PROP_RANGE_INDEX: &str = "tikv.range_index";
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

// Deprecated. Only for compatible issue from v2.0 or older version.
#[derive(Debug, Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub prop_indexes: PropIndexes,
}

impl SizeProperties {
    fn decode(props: &impl DecodeProperties) -> codec::Result<SizeProperties> {
        Ok(SizeProperties {
            total_size: props.decode_u64(PROP_TOTAL_SIZE)?,
            prop_indexes: props.decode_indexes(PROP_SIZE_INDEX)?,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RangeOffsets {
    pub size: u64,
    pub keys: u64,
}

#[derive(Debug, Default)]
pub struct RangeProperties {
    pub offsets: Vec<(Vec<u8>, RangeOffsets)>,
}

impl RangeProperties {
    pub fn get(&self, key: &[u8]) -> &RangeOffsets {
        let idx = self
            .offsets
            .binary_search_by_key(&key, |&(ref k, _)| k)
            .unwrap();
        &self.offsets[idx].1
    }

    fn encode(&self, props: &mut impl EncodeProperties) {
        let mut buf = Vec::with_capacity(1024);
        for (k, offsets) in &self.offsets {
            buf.write_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.write_u64(offsets.size).unwrap();
            buf.write_u64(offsets.keys).unwrap();
        }
        props.encode(PROP_RANGE_INDEX, &buf);
    }

    pub(super) fn decode(props: &impl DecodeProperties) -> codec::Result<RangeProperties> {
        match RangeProperties::decode_from_range_properties(props) {
            Ok(res) => return Ok(res),
            Err(e) => info!(
                "decode to RangeProperties failed with err: {:?}, try to decode to SizeProperties, maybe upgrade from v2.0 or older version?",
                e
            ),
        }
        SizeProperties::decode(props).map(|res| res.into())
    }

    fn decode_from_range_properties(
        props: &impl DecodeProperties,
    ) -> codec::Result<RangeProperties> {
        let mut res = RangeProperties::default();
        let mut buf = props.decode(PROP_RANGE_INDEX)?;
        while !buf.is_empty() {
            let klen = buf.read_u64()?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let offsets = RangeOffsets {
                size: buf.read_u64()?,
                keys: buf.read_u64()?,
            };
            res.offsets.push((k, offsets));
        }
        Ok(res)
    }

    pub fn get_approximate_size_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(start, end).0
    }

    pub fn get_approximate_keys_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(start, end).1
    }

    /// Returns `size` and `keys`.
    pub fn get_approximate_distance_in_range(&self, start: &[u8], end: &[u8]) -> (u64, u64) {
        assert!(start <= end);
        if start == end {
            return (0, 0);
        }
        let start_offset = match self.offsets.binary_search_by_key(&start, |&(ref k, _)| k) {
            Ok(idx) => Some(idx),
            Err(next_idx) => next_idx.checked_sub(1),
        };
        let end_offset = match self.offsets.binary_search_by_key(&end, |&(ref k, _)| k) {
            Ok(idx) => Some(idx),
            Err(next_idx) => next_idx.checked_sub(1),
        };
        let start = start_offset.map_or_else(|| Default::default(), |x| self.offsets[x].1);
        let end = end_offset.map_or_else(|| Default::default(), |x| self.offsets[x].1);
        assert!(end.size >= start.size && end.keys >= start.keys);
        (end.size - start.size, end.keys - start.keys)
    }

    // equivalent to range(Excluded(start_key), Excluded(end_key))
    pub fn take_excluded_range(
        mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(Vec<u8>, RangeOffsets)> {
        let start_offset = match self
            .offsets
            .binary_search_by_key(&start_key, |&(ref k, _)| k)
        {
            Ok(idx) => {
                if idx == self.offsets.len() - 1 {
                    return vec![];
                } else {
                    idx + 1
                }
            }
            Err(next_idx) => next_idx,
        };

        let end_offset = match self.offsets.binary_search_by_key(&end_key, |&(ref k, _)| k) {
            Ok(idx) => {
                if idx == 0 {
                    return vec![];
                } else {
                    idx - 1
                }
            }
            Err(next_idx) => {
                if next_idx == 0 {
                    return vec![];
                } else {
                    next_idx - 1
                }
            }
        };

        if start_offset > end_offset {
            return vec![];
        }

        self.offsets.drain(start_offset..=end_offset).collect()
    }

    pub fn smallest_key(&self) -> Option<Vec<u8>> {
        self.offsets.first().map(|(k, _)| k.to_owned())
    }

    pub fn largest_key(&self) -> Option<Vec<u8>> {
        self.offsets.last().map(|(k, _)| k.to_owned())
    }
}

impl From<SizeProperties> for RangeProperties {
    fn from(p: SizeProperties) -> RangeProperties {
        let mut res = RangeProperties::default();
        for (key, size_index) in p.prop_indexes.into_map() {
            let range = RangeOffsets {
                // For SizeProperties, the offset is accumulation of the size.
                size: size_index.offset,
                ..Default::default()
            };
            res.offsets.push((key, range));
        }
        res
    }
}

fn range_properties_collector_name() -> &'static CStr {
    CStr::from_bytes_with_nul(b"tikv.range-properties-collector\0").unwrap()
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

    #[inline]
    fn size_in_last_range(&self) -> u64 {
        self.cur_offsets.size - self.last_offsets.size
    }

    #[inline]
    fn keys_in_last_range(&self) -> u64 {
        self.cur_offsets.keys - self.last_offsets.keys
    }

    #[inline]
    fn insert_new_point(&mut self, key: Vec<u8>) {
        self.last_offsets = self.cur_offsets;
        self.props.offsets.push((key, self.cur_offsets));
    }

    #[inline]
    fn finish(&mut self, props: &mut impl EncodeProperties) {
        if self.size_in_last_range() > 0 || self.keys_in_last_range() > 0 {
            let key = self.last_key.clone();
            self.insert_new_point(key);
        }
        self.props.encode(props);
    }
}

impl TablePropertiesCollector for RangePropertiesCollector {
    #[inline]
    fn name(&self) -> &CStr {
        range_properties_collector_name()
    }

    #[inline]
    fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _: SequenceNumber,
        _: u64,
    ) -> tirocks::Result<()> {
        // size
        let entry_size = match entry_type {
            EntryType::kEntryPut => value.len() as u64,
            EntryType::kEntryBlobIndex => match TitanBlobIndex::decode(value) {
                Ok(index) => index.blob_size + value.len() as u64,
                // Perhaps should panic?
                Err(_) => return Ok(()),
            },
            _ => return Ok(()),
        };
        self.cur_offsets.size += entry_size + key.len() as u64;
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
        Ok(())
    }

    #[inline]
    fn finish(&mut self, prop: &mut UserCollectedProperties) -> tirocks::Result<()> {
        self.finish(prop);
        Ok(())
    }
}

pub struct RangePropertiesCollectorFactory {
    pub prop_size_index_distance: u64,
    pub prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollectorFactory {
    #[inline]
    fn default() -> Self {
        RangePropertiesCollectorFactory {
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl TablePropertiesCollectorFactory for RangePropertiesCollectorFactory {
    type Collector = RangePropertiesCollector;

    #[inline]
    fn name(&self) -> &CStr {
        range_properties_collector_name()
    }

    #[inline]
    fn create_table_properties_collector(&self, _: Context) -> RangePropertiesCollector {
        RangePropertiesCollector::new(self.prop_size_index_distance, self.prop_keys_index_distance)
    }
}

fn get_range_entries_and_versions(
    engine: &crate::RocksEngine,
    cf: &str,
    start: &[u8],
    end: &[u8],
) -> Option<(u64, u64)> {
    let collection = match engine.properties_of_tables_in_range(cf, &[(start, end)]) {
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
        let mvcc = match decode_mvcc(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&mvcc);
    }

    Some((num_entries, props.num_versions))
}

impl engine_traits::RangePropertiesExt for RocksEngine {
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        // try to get from RangeProperties first.
        match self.get_range_approximate_keys_cf(CF_WRITE, range, large_threshold) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => debug!(
                "failed to get keys from RangeProperties";
                "err" => ?e,
            ),
        }

        let start = &range.start_key;
        let end = &range.end_key;
        let (_, keys) =
            get_range_entries_and_versions(self, CF_WRITE, start, end).unwrap_or_default();
        Ok(keys)
    }

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_keys = 0;
        let (mem_keys, _) =
            self.approximate_memtable_stats(cfname, range.start_key, range.end_key)?;
        total_keys += mem_keys;

        let collection = box_try!(self.range_properties(cfname, start_key, end_key));
        for (_, v) in &*collection {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_keys += props.get_approximate_keys_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_keys > large_threshold {
            let ssts = collection
                .into_iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
                    let keys = props.get_approximate_keys_in_range(start_key, end_key);
                    let p = std::str::from_utf8(k).unwrap();
                    format!(
                        "{}:{}",
                        Path::new(p)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(p),
                        keys
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "range contains too many keys";
                "start" => log_wrappers::Value::key(range.start_key),
                "end" => log_wrappers::Value::key(range.end_key),
                "total_keys" => total_keys,
                "memtable" => mem_keys,
                "ssts_keys" => ssts,
                "cf" => cfname,
            )
        }
        Ok(total_keys)
    }

    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        let mut size = 0;
        for cf in LARGE_CFS {
            size += self
                .get_range_approximate_size_cf(cf, range, large_threshold)
                // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
                // backward compatibility.
                .or_else(|e| if cf == &CF_LOCK { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_range_approximate_size_cf(
        &self,
        cf: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_size = 0;
        let (_, mem_size) = self.approximate_memtable_stats(cf, range.start_key, range.end_key)?;
        total_size += mem_size;

        let collection = box_try!(self.range_properties(cf, start_key, end_key));
        for (_, v) in &*collection {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_size += props.get_approximate_size_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_size > large_threshold {
            let ssts = collection
                .into_iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
                    let size = props.get_approximate_size_in_range(start_key, end_key);
                    let p = std::str::from_utf8(k).unwrap();
                    format!(
                        "{}:{}",
                        Path::new(p)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(p),
                        size
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "range size is too large";
                "start" => log_wrappers::Value::key(range.start_key),
                "end" => log_wrappers::Value::key(range.end_key),
                "total_size" => total_size,
                "memtable" => mem_size,
                "ssts_size" => ssts,
                "cf" => cf,
            )
        }
        Ok(total_size)
    }

    fn get_range_approximate_split_keys(
        &self,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let get_cf_size = |cf: &str| self.get_range_approximate_size_cf(cf, range, 0);
        let cfs = [
            (CF_DEFAULT, box_try!(get_cf_size(CF_DEFAULT))),
            (CF_WRITE, box_try!(get_cf_size(CF_WRITE))),
            // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
            // backward compatibility.
            (CF_LOCK, get_cf_size(CF_LOCK).unwrap_or(0)),
        ];

        let total_size: u64 = cfs.iter().map(|(_, s)| s).sum();
        if total_size == 0 {
            return Err(box_err!("all CFs are empty"));
        }

        let (cf, _) = cfs.iter().max_by_key(|(_, s)| s).unwrap();

        self.get_range_approximate_split_keys_cf(cf, range, key_count)
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let collection = box_try!(self.range_properties(cfname, start_key, end_key));

        let mut keys = vec![];
        for (_, v) in &*collection {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            keys.extend(
                props
                    .take_excluded_range(start_key, end_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }

        if keys.is_empty() {
            return Ok(vec![]);
        }

        const SAMPLING_THRESHOLD: usize = 20000;
        const SAMPLE_RATIO: usize = 1000;
        // If there are too many keys, reduce its amount before sorting, or it may take
        // too much time to sort the keys.
        if keys.len() > SAMPLING_THRESHOLD {
            let len = keys.len();
            keys = keys.into_iter().step_by(len / SAMPLE_RATIO).collect();
        }
        keys.sort();

        // If the keys are too few, return them directly.
        if keys.len() <= key_count {
            return Ok(keys);
        }

        // Find `key_count` keys which divides the whole range into `parts` parts
        // evenly.
        let mut res = Vec::with_capacity(key_count);
        let section_len = (keys.len() as f64) / ((key_count + 1) as f64);
        for i in 1..=key_count {
            res.push(keys[(section_len * (i as f64)) as usize].clone())
        }
        res.dedup();
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use collections::HashMap;
    use engine_traits::{SyncMutable, CF_WRITE, LARGE_CFS};
    use rand::Rng;
    use tempfile::Builder;
    use tirocks::properties::table::user::SysTablePropertiesCollectorFactory;
    use txn_types::Key;

    use super::*;
    use crate::{
        cf_options::RocksCfOptions, db_options::RocksDbOptions,
        properties::mvcc::MvccPropertiesCollectorFactory,
    };

    #[allow(clippy::many_single_char_names)]
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
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 +
            // 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
            ("l", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            ("m", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            // handle "m": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset =
            // 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("n", 1, DEFAULT_PROP_KEYS_INDEX_DISTANCE),
            // handle "n": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset =
            // 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("o", 1, 1),
            // handle　"o": keys = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
        ];

        let mut collector = RangePropertiesCollector::default();
        for &(k, vlen, count) in &cases {
            let v = vec![0; vlen as usize];
            for _ in 0..count {
                collector
                    .add(k.as_bytes(), &v, EntryType::kEntryPut, 0, 0)
                    .unwrap();
            }
        }
        for &(k, vlen, _) in &cases {
            let v = vec![0; vlen as usize];
            collector
                .add(k.as_bytes(), &v, EntryType::kEntryOther, 0, 0)
                .unwrap();
        }
        let mut result = HashMap::default();
        collector.finish(&mut result);

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
        assert_eq!(props.get_approximate_keys_in_range(b"", b"k"), 11_u64);

        assert_eq!(props.offsets.len(), 7);
        let a = props.get(b"a");
        assert_eq!(a.size, 1);
        let e = props.get(b"e");
        assert_eq!(e.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 5);
        let i = props.get(b"i");
        assert_eq!(i.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = props.get(b"k");
        assert_eq!(k.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11);
        let m = props.get(b"m");
        assert_eq!(m.keys, 11 + DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let n = props.get(b"n");
        assert_eq!(n.keys, 11 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let o = props.get(b"o");
        assert_eq!(o.keys, 12 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let empty = RangeOffsets::default();
        let cases = [
            (" ", "k", k, &empty, 3),
            (" ", " ", &empty, &empty, 0),
            ("k", "k", k, k, 0),
            ("a", "k", k, a, 2),
            ("a", "i", i, a, 1),
            ("e", "h", e, e, 0),
            ("b", "h", e, a, 1),
            ("g", "g", i, i, 0),
        ];
        for &(start, end, end_idx, start_idx, count) in &cases {
            let props = RangeProperties::decode(&result).unwrap();
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
            assert_eq!(
                props
                    .take_excluded_range(start.as_bytes(), end.as_bytes())
                    .len(),
                count
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
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 +
            // 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
        ];

        let handles = ["a", "e", "i", "k"];

        let mut rng = rand::thread_rng();
        let mut collector = RangePropertiesCollector::default();
        let mut extra_value_size: u64 = 0;
        for &(k, vlen) in &cases {
            if handles.contains(&k) || rng.gen_range(0..2) == 0 {
                let v = vec![0; vlen as usize - extra_value_size as usize];
                extra_value_size = 0;
                collector
                    .add(k.as_bytes(), &v, EntryType::kEntryPut, 0, 0)
                    .unwrap();
            } else {
                let blob_index = TitanBlobIndex::new(0, vlen - extra_value_size, 0);
                let v = blob_index.encode();
                extra_value_size = v.len() as u64;
                collector
                    .add(k.as_bytes(), &v, EntryType::kEntryBlobIndex, 0, 0)
                    .unwrap();
            }
        }
        let mut result = HashMap::default();
        collector.finish(&mut result);

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

    #[test]
    fn test_get_range_entries_and_versions() {
        let path = Builder::new()
            .prefix("_test_get_range_entries_and_versions")
            .tempdir()
            .unwrap();
        let db_opts = RocksDbOptions::default();
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = RocksCfOptions::default();
                cf_opts
                    .set_level0_file_num_compaction_trigger(10)
                    .add_table_properties_collector_factory(
                        &SysTablePropertiesCollectorFactory::new(
                            MvccPropertiesCollectorFactory::default(),
                        ),
                    );
                (*cf, cf_opts)
            })
            .collect();
        let db = crate::util::new_engine_opt(path.path(), db_opts, cfs_opts).unwrap();

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(2.into())
                    .as_encoded(),
            );
            db.put_cf(CF_WRITE, &k1, b"v1").unwrap();
            db.delete_cf(CF_WRITE, &k1).unwrap();
            let key = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(3.into())
                    .as_encoded(),
            );
            db.put_cf(CF_WRITE, &key, b"v2").unwrap();
            db.flush(CF_WRITE, true).unwrap();
        }

        let start_keys = keys::data_key(&[]);
        let end_keys = keys::data_end_key(&[]);
        let (entries, versions) =
            get_range_entries_and_versions(&db, CF_WRITE, &start_keys, &end_keys).unwrap();
        assert_eq!(entries, (cases.len() * 2) as u64);
        assert_eq!(versions, cases.len() as u64);
    }
}
