// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::properties::{get_range_entries_and_versions, RangeProperties};
use engine_traits::{
    MiscExt, Range, RangePropertiesExt, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesExt, CF_DEFAULT, CF_LOCK, CF_WRITE, LARGE_CFS,
};
use std::path::Path;

impl RangePropertiesExt for RocksEngine {
    fn get_range_approximate_keys(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
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
        let (_, keys) =
            get_range_entries_and_versions(self, CF_WRITE, &start, &end).unwrap_or_default();
        Ok(keys)
    }

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
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

    fn get_range_approximate_size(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
        let mut size = 0;
        for cfname in LARGE_CFS {
            size += self
                .get_range_approximate_size_cf(cfname, range, region_id, large_threshold)
                // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
                // backward compatibility.
                .or_else(|e| if cfname == &CF_LOCK { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_size = 0;
        let (_, mem_size) = box_try!(self.get_approximate_memtable_stats_cf(cfname, &range));
        total_size += mem_size;

        let collection = box_try!(self.get_range_properties_cf(cfname, &start_key, &end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
            total_size += props.get_approximate_size_in_range(&start_key, &end_key);
        }

        if large_threshold != 0 && total_size > large_threshold {
            let ssts = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(&v.user_collected_properties()).unwrap();
                    let size = props.get_approximate_size_in_range(&start_key, &end_key);
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
                "region size is too large";
                "region_id" => region_id,
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
        range: Range,
        region_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        let get_cf_size = |cf: &str| self.get_range_approximate_size_cf(cf, range, region_id, 0);
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

        let (cf, cf_size) = cfs.iter().max_by_key(|(_, s)| s).unwrap();
        // assume the size of keys is uniform distribution in both cfs.
        let cf_split_size = split_size * cf_size / total_size;

        self.get_range_approximate_split_keys_cf(
            cf,
            range,
            region_id,
            cf_split_size,
            max_size,
            batch_split_limit,
        )
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        _region_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let collection = box_try!(self.get_range_properties_cf(cfname, &start_key, &end_key));

        let mut keys = vec![];
        let mut total_size = 0;
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
            total_size += props.get_approximate_size_in_range(&start_key, &end_key);

            keys.extend(
                props
                    .take_excluded_range(start_key, end_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }
        if keys.len() == 1 {
            return Ok(vec![]);
        }
        if keys.is_empty() || total_size == 0 || split_size == 0 {
            return Err(box_err!(
                "unexpected key len {} or total_size {} or split size {}, len of collection {}, cf {}, start {}, end {}",
                keys.len(),
                total_size,
                split_size,
                collection.len(),
                cfname,
                log_wrappers::Value::key(&start_key),
                log_wrappers::Value::key(&end_key)
            ));
        }
        keys.sort();

        // use total size of this range and the number of keys in this range to
        // calculate the average distance between two keys, and we produce a
        // split_key every `split_size / distance` keys.
        let len = keys.len();
        let distance = total_size as f64 / len as f64;
        let n = (split_size as f64 / distance).ceil() as usize;
        if n == 0 {
            return Err(box_err!(
                "unexpected n == 0, total_size: {}, split_size: {}, len: {}, distance: {}",
                total_size,
                split_size,
                keys.len(),
                distance
            ));
        }

        // cause first element of the iterator will always be returned by step_by(),
        // so the first key returned may not the desired split key. Note that, the
        // start key of region is not included, so we we drop first n - 1 keys.
        //
        // For example, the split size is `3 * distance`. And the numbers stand for the
        // key in `RangeProperties`, `^` stands for produced split key.
        //
        // skip:
        // start___1___2___3___4___5___6___7....
        //                 ^           ^
        //
        // not skip:
        // start___1___2___3___4___5___6___7....
        //         ^           ^           ^
        let mut split_keys = keys
            .into_iter()
            .skip(n - 1)
            .step_by(n)
            .collect::<Vec<Vec<u8>>>();

        if split_keys.len() as u64 > batch_split_limit {
            split_keys.truncate(batch_split_limit as usize);
        } else {
            // make sure not to split when less than max_size for last part
            let rest = (len % n) as u64;
            if rest * distance as u64 + split_size < max_size {
                split_keys.pop();
            }
        }
        Ok(split_keys)
    }

    fn get_range_approximate_middle(
        &self,
        range: Range,
        region_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let get_cf_size = |cf: &str| self.get_range_approximate_size_cf(cf, range, region_id, 0);

        let default_cf_size = box_try!(get_cf_size(CF_DEFAULT));
        let write_cf_size = box_try!(get_cf_size(CF_WRITE));

        let middle_by_cf = if default_cf_size >= write_cf_size {
            CF_DEFAULT
        } else {
            CF_WRITE
        };

        self.get_range_approximate_middle_cf(middle_by_cf, range, region_id)
    }

    fn get_range_approximate_middle_cf(
        &self,
        cfname: &str,
        range: Range,
        _region_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let collection = box_try!(self.get_range_properties_cf(cfname, &start_key, &end_key));

        let mut keys = Vec::new();
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
            keys.extend(
                props
                    .take_excluded_range(start_key, end_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }
        if keys.is_empty() {
            return Ok(None);
        }
        keys.sort();
        // Calculate the position by (len-1)/2. So it's the left one
        // of two middle positions if the number of keys is even.
        let middle = (keys.len() - 1) / 2;
        Ok(Some(keys.swap_remove(middle)))
    }

    fn divide_range(&self, range: Range, region_id: u64, parts: usize) -> Result<Vec<Vec<u8>>> {
        let default_cf_size =
            self.get_range_approximate_keys_cf(CF_DEFAULT, range, region_id, 0)?;
        let write_cf_size = self.get_range_approximate_keys_cf(CF_WRITE, range, region_id, 0)?;

        let cf = if default_cf_size >= write_cf_size {
            CF_DEFAULT
        } else {
            CF_WRITE
        };

        self.divide_range_cf(cf, range, region_id, parts)
    }

    fn divide_range_cf(
        &self,
        cf: &str,
        range: Range,
        _region_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let start = &range.start_key;
        let end = &range.end_key;
        let collection = self.get_range_properties_cf(cf, start, end)?;

        let mut keys = Vec::new();
        let mut found_keys_count = 0;
        for (_, v) in collection.iter() {
            let props = RangeProperties::decode(&v.user_collected_properties())?;
            keys.extend(
                props
                    .take_excluded_range(start, end)
                    .into_iter()
                    .filter(|_| {
                        found_keys_count += 1;
                        found_keys_count % 100 == 0
                    })
                    .map(|(k, _)| k),
            );
        }

        debug!(
            "({} points found, {} points selected for dividing)",
            found_keys_count,
            keys.len()
        );

        if keys.is_empty() {
            return Ok(vec![]);
        }

        // If there are too many keys, reduce its amount before sorting, or it may take too much
        // time to sort the keys.
        if keys.len() > 20000 {
            let len = keys.len();
            keys = keys.into_iter().step_by(len / 10000).collect();
        }

        keys.sort();
        keys.dedup();

        // If the keys are too few, return them directly.
        if keys.len() < parts {
            return Ok(keys);
        }

        // Find `parts - 1` keys which divides the whole range into `parts` parts evenly.
        let mut res = Vec::with_capacity(parts - 1);
        let section_len = (keys.len() as f64) / (parts as f64);
        for i in 1..parts {
            res.push(keys[(section_len * (i as f64)) as usize].clone())
        }
        Ok(res)
    }
}
