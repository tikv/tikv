// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine_traits::{
    MiscExt, Range, RangePropertiesExt, Result, CF_DEFAULT, CF_LOCK, CF_WRITE, LARGE_CFS,
};
use tikv_util::{box_err, box_try, debug, info};

use crate::{
    engine::RocksEngine,
    properties::{get_range_entries_and_versions, RangeProperties},
};

impl RangePropertiesExt for RocksEngine {
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
        let (mem_keys, _) = box_try!(self.get_approximate_memtable_stats_cf(cfname, &range));
        total_keys += mem_keys;

        let collection = box_try!(self.get_range_properties_cf(cfname, start_key, end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_keys += props.get_approximate_keys_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_keys > large_threshold {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
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
        for cfname in LARGE_CFS {
            size += self
                .get_range_approximate_size_cf(cfname, range, large_threshold)
                // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
                // backward compatibility.
                .or_else(|e| if cfname == &CF_LOCK { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_size = 0;
        let (_, mem_size) = box_try!(self.get_approximate_memtable_stats_cf(cfname, &range));
        total_size += mem_size;

        let collection = box_try!(self.get_range_properties_cf(cfname, start_key, end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_size += props.get_approximate_size_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_size > large_threshold {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
                    let size = props.get_approximate_size_in_range(start_key, end_key);
                    format!(
                        "{}:{}",
                        Path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
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
                "cf" => cfname,
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
        let collection = box_try!(self.get_range_properties_cf(cfname, start_key, end_key));

        let mut keys = vec![];
        for (_, v) in collection.iter() {
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
        // If there are too many keys, reduce its amount before sorting, or it may take too much
        // time to sort the keys.
        if keys.len() > SAMPLING_THRESHOLD {
            let len = keys.len();
            keys = keys.into_iter().step_by(len / SAMPLE_RATIO).collect();
        }
        keys.sort();

        // If the keys are too few, return them directly.
        if keys.len() <= key_count {
            return Ok(keys);
        }

        // Find `key_count` keys which divides the whole range into `parts` parts evenly.
        let mut res = Vec::with_capacity(key_count);
        let section_len = (keys.len() as f64) / ((key_count + 1) as f64);
        for i in 1..=key_count {
            res.push(keys[(section_len * (i as f64)) as usize].clone())
        }
        res.dedup();
        Ok(res)
    }
}
