// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use engine_traits::{RangePropertiesExt, Result, Range, CF_WRITE, CFHandleExt, MiscExt, TablePropertiesExt, TablePropertiesCollection, TableProperties};
use crate::engine::RocksEngine;
use crate::properties::{get_range_entries_and_versions, RangeProperties};

impl RangePropertiesExt for RocksEngine {
    fn get_range_approximate_keys(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        // try to get from RangeProperties first.
        match self.get_range_approximate_keys_cf(CF_WRITE, range, region_id, large_threshold) {
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
        let cf = box_try!(self.cf_handle(CF_WRITE));
        let (_, keys) = get_range_entries_and_versions(self, cf, &start, &end).unwrap_or_default();
        Ok(keys)
    }

    fn get_range_approximate_keys_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_keys = 0;
        let (mem_keys, _) = box_try!(self.get_approximate_memtable_stats_cf(cfname, &range));
        total_keys += mem_keys;

        let collection = box_try!(self.get_range_properties_cf(cfname, start_key, end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
            total_keys += props.get_approximate_keys_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_keys > large_threshold {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(&v.user_collected_properties()).unwrap();
                    let keys = props.get_approximate_keys_in_range(start_key, end_key);
                    format!(
                        "{}:{}",
                        Path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
                        keys
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "region contains too many keys";
                "region_id" => region_id,
                "total_keys" => total_keys,
                "memtable" => mem_keys,
                "ssts_keys" => ssts,
                "cf" => cfname,
            )
        }
        Ok(total_keys)
    }

    fn get_range_approximate_size(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }

}
