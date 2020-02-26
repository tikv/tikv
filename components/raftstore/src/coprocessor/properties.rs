// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::collections::HashMap;
use std::u64;

use engine::rocks::{DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory};
pub use engine_rocks::{
    RangeOffsets, RangeProperties, RangePropertiesCollector, RangePropertiesCollectorFactory,
    UserProperties,
};
use engine_traits::KvEngine;
use engine_traits::Range;
use engine_traits::{
    DecodeProperties, IndexHandle, IndexHandles, TableProperties, TablePropertiesCollection,
};
use tikv_util::codec::Result;
use txn_types::{Key, TimeStamp, Write, WriteType};

const PROP_NUM_ERRORS: &str = "tikv.num_errors";
const PROP_MIN_TS: &str = "tikv.min_ts";
const PROP_MAX_TS: &str = "tikv.max_ts";
const PROP_NUM_ROWS: &str = "tikv.num_rows";
const PROP_NUM_PUTS: &str = "tikv.num_puts";
const PROP_NUM_VERSIONS: &str = "tikv.num_versions";
const PROP_MAX_ROW_VERSIONS: &str = "tikv.max_row_versions";
const PROP_ROWS_INDEX: &str = "tikv.rows_index";
const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;

#[derive(Clone, Debug, Default)]
pub struct MvccProperties {
    pub min_ts: TimeStamp,     // The minimal timestamp.
    pub max_ts: TimeStamp,     // The maximal timestamp.
    pub num_rows: u64,         // The number of rows.
    pub num_puts: u64,         // The number of MVCC puts of all rows.
    pub num_versions: u64,     // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single row.
}

impl MvccProperties {
    pub fn new() -> MvccProperties {
        MvccProperties {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
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
        props.encode_u64(PROP_MIN_TS, self.min_ts.into_inner());
        props.encode_u64(PROP_MAX_TS, self.max_ts.into_inner());
        props.encode_u64(PROP_NUM_ROWS, self.num_rows);
        props.encode_u64(PROP_NUM_PUTS, self.num_puts);
        props.encode_u64(PROP_NUM_VERSIONS, self.num_versions);
        props.encode_u64(PROP_MAX_ROW_VERSIONS, self.max_row_versions);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<MvccProperties> {
        let mut res = MvccProperties::new();
        res.min_ts = props.decode_u64(PROP_MIN_TS)?.into();
        res.max_ts = props.decode_u64(PROP_MAX_TS)?.into();
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

pub fn get_range_entries_and_versions<E>(
    engine: &E,
    cf: &E::CFHandle,
    start: &[u8],
    end: &[u8],
) -> Option<(u64, u64)>
where
    E: KvEngine,
{
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
    for (_, v) in collection.iter() {
        let mvcc = match MvccProperties::decode(&v.user_collected_properties()) {
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
    use std::sync::Arc;

    use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
    use engine::rocks::{DBEntryType, TablePropertiesCollector};
    use tempfile::Builder;
    use test::Bencher;

    use crate::coprocessor::properties::MvccPropertiesCollectorFactory;
    use engine::rocks;
    use engine::rocks::util::CFOptions;
    use engine_rocks::Compat;
    use engine_traits::CFHandleExt;
    use engine_traits::{CF_WRITE, LARGE_CFS};
    use txn_types::{Key, Write, WriteType};

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
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(2.into())
                    .as_encoded(),
            );
            let write_cf = db.cf_handle(CF_WRITE).unwrap();
            db.put_cf(write_cf, &k1, b"v1").unwrap();
            db.delete_cf(write_cf, &k1).unwrap();
            let key = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(3.into())
                    .as_encoded(),
            );
            db.put_cf(write_cf, &key, b"v2").unwrap();
            db.flush_cf(write_cf, true).unwrap();
        }

        let start_keys = keys::data_key(&[]);
        let end_keys = keys::data_end_key(&[]);
        let cf = db.c().cf_handle(CF_WRITE).unwrap();
        let (entries, versions) =
            get_range_entries_and_versions(db.c(), cf, &start_keys, &end_keys).unwrap();
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
            let ts = ts.into();
            let k = Key::from_raw(key.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let v = Write::new(write_type, ts, None).as_ref().to_bytes();
            collector.add(&k, &v, entry_type, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = MvccProperties::decode(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
    }

    #[bench]
    fn bench_mvcc_properties(b: &mut Bencher) {
        let ts = 1.into();
        let num_entries = 100;
        let mut entries = Vec::new();
        for i in 0..num_entries {
            let s = format!("{:032}", i);
            let k = Key::from_raw(s.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let w = Write::new(WriteType::Put, ts, Some(s.as_bytes().to_owned()));
            entries.push((k, w.as_ref().to_bytes()));
        }

        let mut collector = MvccPropertiesCollector::new();
        b.iter(|| {
            for &(ref k, ref v) in &entries {
                collector.add(k, v, DBEntryType::Put, 0, 0);
            }
        });
    }
}
