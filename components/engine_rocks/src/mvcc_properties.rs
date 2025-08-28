// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MvccProperties, MvccPropertiesExt, Result};
use kll_rs::KllDoubleSketch;
use txn_types::TimeStamp;

use crate::{RocksEngine, RocksTtlProperties, UserProperties, decode_properties::DecodeProperties};

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

// KLL sketch properties for timestamp distribution analysis
pub const PROP_STALE_VERSION_KLL_SKETCH: &str = "tikv.stale_version_kll_sketch";
pub const PROP_DELETE_KLL_SKETCH: &str = "tikv.delete_kll_sketch";

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
        RocksTtlProperties::encode_to(&mvcc_props.ttl, &mut props);
        RocksMvccProperties::encode_kll_sketch(
            &mvcc_props.stale_version_kll_sketch,
            &mut props,
            PROP_STALE_VERSION_KLL_SKETCH,
        );
        RocksMvccProperties::encode_kll_sketch(
            &mvcc_props.delete_kll_sketch,
            &mut props,
            PROP_DELETE_KLL_SKETCH,
        );
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
            .unwrap_or(res.num_versions - res.num_puts);
        res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
        RocksTtlProperties::decode_from(&mut res.ttl, props);
        res.stale_version_kll_sketch =
            RocksMvccProperties::decode_kll_sketch(props, PROP_STALE_VERSION_KLL_SKETCH)
                .unwrap_or_default();
        res.delete_kll_sketch =
            RocksMvccProperties::decode_kll_sketch(props, PROP_DELETE_KLL_SKETCH)
                .unwrap_or_default();
        Ok(res)
    }

    pub fn encode_kll_sketch(
        sketch: &KllDoubleSketch,
        props: &mut UserProperties,
        prop_name: &str,
    ) {
        if let Ok(serialized) = sketch.serialize() {
            props.insert(prop_name.as_bytes().to_vec(), serialized.to_vec());
        }
    }

    pub fn decode_kll_sketch<T: DecodeProperties>(
        props: &T,
        prop_name: &str,
    ) -> Option<KllDoubleSketch> {
        match props.decode(prop_name) {
            Ok(data) => KllDoubleSketch::deserialize(data).ok(),
            Err(_) => None,
        }
    }
}

impl MvccPropertiesExt for RocksEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        let collection = match self.get_range_properties_cf(cf, start_key, end_key) {
            Ok(c) if !c.is_empty() => c,
            _ => return None,
        };
        let mut props = MvccProperties::new();
        for (_, v) in collection.iter() {
            let mvcc = match RocksMvccProperties::decode(v.user_collected_properties()) {
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
