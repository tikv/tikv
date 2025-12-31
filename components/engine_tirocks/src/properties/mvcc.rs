// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, ffi::CStr};

use api_version::{ApiV2, KeyMode, KvFormat};
use engine_traits::{raw_ttl::ttl_current_ts, MvccProperties};
use tirocks::properties::table::user::{
    Context, EntryType, SequenceNumber, TablePropertiesCollector, TablePropertiesCollectorFactory,
    UserCollectedProperties,
};
use txn_types::{Key, TimeStamp, Write, WriteType};

use super::{DecodeProperties, EncodeProperties, PropIndex, PropIndexes};
use crate::RocksEngine;

pub const PROP_NUM_ERRORS: &str = "tikv.num_errors";
pub const PROP_MIN_TS: &str = "tikv.min_ts";
pub const PROP_MAX_TS: &str = "tikv.max_ts";
pub const PROP_NUM_ROWS: &str = "tikv.num_rows";
pub const PROP_NUM_PUTS: &str = "tikv.num_puts";
pub const PROP_NUM_DELETES: &str = "tikv.num_deletes";
pub const PROP_NUM_VERSIONS: &str = "tikv.num_versions";
pub const PROP_MAX_ROW_VERSIONS: &str = "tikv.max_row_versions";
pub const PROP_ROWS_INDEX: &str = "tikv.rows_index";
pub const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;

/// Can be used for write CF in TiDB & TxnKV scenario, or be used for default CF
/// in RawKV scenario.
pub struct MvccPropertiesCollector {
    name: &'static CStr,
    props: MvccProperties,
    last_row: Vec<u8>,
    num_errors: u64,
    row_versions: u64,
    cur_prop_index: PropIndex,
    row_prop_indexes: PropIndexes,
    key_mode: KeyMode, // Use KeyMode::Txn for both TiDB & TxnKV, KeyMode::Raw for RawKV.
    current_ts: u64,
}

impl MvccPropertiesCollector {
    fn new(name: &'static CStr, key_mode: KeyMode) -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            name,
            props: MvccProperties::new(),
            last_row: Vec::new(),
            num_errors: 0,
            row_versions: 0,
            cur_prop_index: PropIndex::default(),
            row_prop_indexes: PropIndexes::new(),
            key_mode,
            current_ts: ttl_current_ts(),
        }
    }

    fn finish(&mut self, properties: &mut impl EncodeProperties) {
        // Insert last handle.
        if self.cur_prop_index.prop > 0 {
            self.row_prop_indexes
                .insert(self.last_row.clone(), self.cur_prop_index.clone());
        }
        encode_mvcc(&self.props, properties);
        properties.encode_u64(PROP_NUM_ERRORS, self.num_errors);
        properties.encode_indexes(PROP_ROWS_INDEX, &self.row_prop_indexes);
    }
}

impl TablePropertiesCollector for MvccPropertiesCollector {
    fn name(&self) -> &CStr {
        self.name
    }

    fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _: SequenceNumber,
        _: u64,
    ) -> tirocks::Result<()> {
        // TsFilter filters sst based on max_ts and min_ts during iterating.
        // To prevent seeing outdated (GC) records, we should consider
        // RocksDB delete entry type.
        if entry_type != EntryType::kEntryPut && entry_type != EntryType::kEntryDelete {
            return Ok(());
        }

        if !keys::validate_data_key(key) {
            self.num_errors += 1;
            return Ok(());
        }

        let (k, ts) = match Key::split_on_ts_for(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.num_errors += 1;
                return Ok(());
            }
        };

        self.props.min_ts = cmp::min(self.props.min_ts, ts);
        self.props.max_ts = cmp::max(self.props.max_ts, ts);
        if entry_type == EntryType::kEntryDelete {
            // Empty value for delete entry type, skip following properties.
            return Ok(());
        }

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

        if self.key_mode == KeyMode::Raw {
            let decode_raw_value = ApiV2::decode_raw_value(value);
            match decode_raw_value {
                Ok(raw_value) => {
                    if raw_value.is_valid(self.current_ts) {
                        self.props.num_puts += 1;
                    } else {
                        self.props.num_deletes += 1;
                    }
                }
                Err(_) => {
                    self.num_errors += 1;
                }
            }
        } else {
            let write_type = match Write::parse_type(value) {
                Ok(v) => v,
                Err(_) => {
                    self.num_errors += 1;
                    return Ok(());
                }
            };

            match write_type {
                WriteType::Put => self.props.num_puts += 1,
                WriteType::Delete => self.props.num_deletes += 1,
                _ => {}
            }
        }

        // Add new row.
        if self.row_versions == 1 {
            self.cur_prop_index.prop += 1;
            self.cur_prop_index.offset += 1;
            if self.cur_prop_index.offset == 1
                || self.cur_prop_index.prop >= PROP_ROWS_INDEX_DISTANCE
            {
                self.row_prop_indexes
                    .insert(self.last_row.clone(), self.cur_prop_index.clone());
                self.cur_prop_index.prop = 0;
            }
        }
        Ok(())
    }

    fn finish(&mut self, properties: &mut UserCollectedProperties) -> tirocks::Result<()> {
        self.finish(properties);
        Ok(())
    }
}

/// Can be used for write CF of TiDB/TxnKV, default CF of RawKV.
pub struct MvccPropertiesCollectorFactory {
    name: &'static CStr,
    key_mode: KeyMode,
}

impl Default for MvccPropertiesCollectorFactory {
    fn default() -> Self {
        Self {
            name: CStr::from_bytes_with_nul(b"tikv.mvcc-properties-collector\0").unwrap(),
            key_mode: KeyMode::Txn,
        }
    }
}

impl MvccPropertiesCollectorFactory {
    pub fn rawkv() -> Self {
        Self {
            name: CStr::from_bytes_with_nul(b"tikv.rawkv-mvcc-properties-collector\0").unwrap(),
            key_mode: KeyMode::Raw,
        }
    }
}

impl TablePropertiesCollectorFactory for MvccPropertiesCollectorFactory {
    type Collector = MvccPropertiesCollector;

    fn name(&self) -> &CStr {
        self.name
    }

    fn create_table_properties_collector(&self, _: Context) -> Self::Collector {
        MvccPropertiesCollector::new(self.name, self.key_mode)
    }
}

fn encode_mvcc(mvcc_props: &MvccProperties, props: &mut impl EncodeProperties) {
    props.encode_u64(PROP_MIN_TS, mvcc_props.min_ts.into_inner());
    props.encode_u64(PROP_MAX_TS, mvcc_props.max_ts.into_inner());
    props.encode_u64(PROP_NUM_ROWS, mvcc_props.num_rows);
    props.encode_u64(PROP_NUM_PUTS, mvcc_props.num_puts);
    props.encode_u64(PROP_NUM_DELETES, mvcc_props.num_deletes);
    props.encode_u64(PROP_NUM_VERSIONS, mvcc_props.num_versions);
    props.encode_u64(PROP_MAX_ROW_VERSIONS, mvcc_props.max_row_versions);
}

pub(super) fn decode_mvcc(props: &impl DecodeProperties) -> codec::Result<MvccProperties> {
    let mut res = MvccProperties::new();
    res.min_ts = props.decode_u64(PROP_MIN_TS)?.into();
    res.max_ts = props.decode_u64(PROP_MAX_TS)?.into();
    res.num_rows = props.decode_u64(PROP_NUM_ROWS)?;
    res.num_puts = props.decode_u64(PROP_NUM_PUTS)?;
    res.num_versions = props.decode_u64(PROP_NUM_VERSIONS)?;
    // To be compatible with old versions.
    res.num_deletes = props
        .decode_u64(PROP_NUM_DELETES)
        .unwrap_or(res.num_versions - res.num_puts);
    res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
    Ok(res)
}

impl engine_traits::MvccPropertiesExt for RocksEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        let collection = match self.range_properties(cf, start_key, end_key) {
            Ok(c) if !c.is_empty() => c,
            _ => return None,
        };
        let mut props = MvccProperties::new();
        for (_, v) in &*collection {
            let mvcc = match decode_mvcc(v.user_collected_properties()) {
                Ok(m) => m,
                Err(_) => return None,
            };
            // Filter out properties after safe_point.
            if mvcc.min_ts > safe_point {
                continue;
            }
            props.add(&mvcc);
        }
        Some(props)
    }
}

#[cfg(test)]
mod tests {
    use api_version::RawValue;
    use collections::HashMap;
    use test::Bencher;
    use txn_types::{Key, Write, WriteType};

    use super::*;

    #[test]
    fn test_mvcc_properties() {
        let cases = [
            ("ab", 2, WriteType::Put, EntryType::kEntryPut),
            ("ab", 1, WriteType::Delete, EntryType::kEntryPut),
            ("ab", 1, WriteType::Delete, EntryType::kEntryDelete),
            ("cd", 5, WriteType::Delete, EntryType::kEntryPut),
            ("cd", 4, WriteType::Put, EntryType::kEntryPut),
            ("cd", 3, WriteType::Put, EntryType::kEntryPut),
            ("ef", 6, WriteType::Put, EntryType::kEntryPut),
            ("ef", 6, WriteType::Put, EntryType::kEntryDelete),
            ("gh", 7, WriteType::Delete, EntryType::kEntryPut),
        ];
        let mut collector =
            MvccPropertiesCollector::new(CStr::from_bytes_with_nul(b"\0").unwrap(), KeyMode::Txn);
        for &(key, ts, write_type, entry_type) in &cases {
            let ts = ts.into();
            let k = Key::from_raw(key.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let v = Write::new(write_type, ts, None).as_ref().to_bytes();
            collector.add(&k, &v, entry_type, 0, 0).unwrap();
        }
        let mut result = HashMap::default();
        collector.finish(&mut result);

        let props = decode_mvcc(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
    }

    #[test]
    fn test_mvcc_properties_rawkv_mode() {
        let test_raws = vec![
            (b"r\0a", 1, false, u64::MAX),
            (b"r\0a", 5, false, u64::MAX),
            (b"r\0a", 7, false, u64::MAX),
            (b"r\0b", 1, false, u64::MAX),
            (b"r\0b", 1, true, u64::MAX),
            (b"r\0c", 1, true, 10),
            (b"r\0d", 1, true, 10),
        ];

        let mut collector =
            MvccPropertiesCollector::new(CStr::from_bytes_with_nul(b"\0").unwrap(), KeyMode::Raw);
        for &(key, ts, is_delete, expire_ts) in &test_raws {
            let encode_key = ApiV2::encode_raw_key(key, Some(ts.into()));
            let k = keys::data_key(encode_key.as_encoded());
            let v = ApiV2::encode_raw_value(RawValue {
                user_value: &[0; 10][..],
                expire_ts: Some(expire_ts),
                is_delete,
            });
            collector.add(&k, &v, EntryType::kEntryPut, 0, 0).unwrap();
        }

        let mut result = HashMap::default();
        collector.finish(&mut result);

        let props = decode_mvcc(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_deletes, 3);
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

        let mut collector =
            MvccPropertiesCollector::new(CStr::from_bytes_with_nul(b"\0").unwrap(), KeyMode::Txn);
        b.iter(|| {
            for &(ref k, ref v) in &entries {
                collector.add(k, v, EntryType::kEntryPut, 0, 0).unwrap();
            }
        });
    }
}
