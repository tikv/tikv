// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::util::get_cf_handle;
use crate::{RocksEngine, UserProperties};
use engine_traits::{
    DecodeProperties, MvccProperties, MvccPropertiesExt, Result, TableProperties,
    TablePropertiesCollection, TablePropertiesExt,
};
use std::collections::HashSet;
use txn_types::TimeStamp;

pub(crate) const PROP_NUM_ERRORS: &str = "tikv.num_errors";
pub(crate) const PROP_MIN_TS: &str = "tikv.min_ts";
pub(crate) const PROP_MAX_TS: &str = "tikv.max_ts";
pub(crate) const PROP_NUM_ROWS: &str = "tikv.num_rows";
pub(crate) const PROP_NUM_PUTS: &str = "tikv.num_puts";
pub(crate) const PROP_NUM_DELETES: &str = "tikv.num_deletes";
pub(crate) const PROP_NUM_VERSIONS: &str = "tikv.num_versions";
pub(crate) const PROP_MAX_ROW_VERSIONS: &str = "tikv.max_row_versions";
pub(crate) const PROP_ROWS_INDEX: &str = "tikv.rows_index";
pub(crate) const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;

pub struct RocksMvccProperties;

impl RocksMvccProperties {
    pub fn encode(mvcc_props: &MvccProperties) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MIN_TS, mvcc_props.min_ts.into_inner());
        props.encode_u64(PROP_MAX_TS, mvcc_props.max_ts.into_inner());
        props.encode_u64(PROP_NUM_ROWS, mvcc_props.num_rows);
        props.encode_u64(PROP_NUM_PUTS, mvcc_props.num_puts);
        props.encode_u64(PROP_NUM_DELETES, mvcc_props.num_deletes);
        props.encode_u64(PROP_NUM_VERSIONS, mvcc_props.num_versions);
        props.encode_u64(PROP_MAX_ROW_VERSIONS, mvcc_props.max_row_versions);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<MvccProperties> {
        let mut res = MvccProperties::new();
        res.min_ts = props.decode_u64(PROP_MIN_TS)?.into();
        res.max_ts = props.decode_u64(PROP_MAX_TS)?.into();
        res.num_rows = props.decode_u64(PROP_NUM_ROWS)?;
        res.num_puts = props.decode_u64(PROP_NUM_PUTS)?;
        res.num_versions = props.decode_u64(PROP_NUM_VERSIONS)?;
        // To be compatible with old versions.
        res.num_deletes = props
            .decode_u64(PROP_NUM_DELETES)
            .unwrap_or_else(|_| res.num_versions - res.num_puts);
        res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
        Ok(res)
    }
}

impl MvccPropertiesExt for RocksEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
        ignore_level_0: bool,
    ) -> Option<MvccProperties> {
        let collection = match self.get_range_properties_cf(cf, &start_key, &end_key) {
            Ok(c) if !c.is_empty() => c,
            _ => return None,
        };

        let mut level_0_set = HashSet::new();
        if ignore_level_0 {
            let raw = self.as_inner();
            let cf_handle = get_cf_handle(raw, cf).unwrap();
            let metadata = raw.get_column_family_meta_data(cf_handle);
            let level_0_meta = metadata.get_level(0);
            let level_0_vec = level_0_meta.get_files();
            for file_meta in level_0_vec {
                let file_path = file_meta.get_name();
                let file_name = file_path.split('/').last().unwrap();
                level_0_set.insert(file_name.to_owned());
            }
        }
        info!("L0 files: {:?}", level_0_set);

        let mut props = MvccProperties::new();
        for (k, v) in collection.iter() {
            if ignore_level_0 {
                let file_name = k.split('/').last().unwrap();
                if level_0_set.contains(file_name) {
                    info!("ignored L0 file: {:?}", file_name);
                    // Skip files at level 0.
                    continue;
                }
            }

            let mvcc = match RocksMvccProperties::decode(&v.user_collected_properties()) {
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
