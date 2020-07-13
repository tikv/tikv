// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use engine_traits::{RangePropertiesExt, Result, Range, CF_WRITE, CFHandleExt, MiscExt, TablePropertiesExt, TablePropertiesCollection, TableProperties, LARGE_CFS, CF_LOCK, CF_DEFAULT};
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
        let mut size = 0;
        for cfname in LARGE_CFS {
            size += self.get_range_approximate_size_cf(cfname, range, region_id, large_threshold)
            // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
            // backward compatibility.
                .or_else(|e| if cfname == &CF_LOCK { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_range_approximate_size_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
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

    fn get_range_approximate_split_keys(&self, range: Range, region_id: u64, split_size: u64, max_size: u64, batch_split_limit: u64) -> Result<Vec<Vec<u8>>> {
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

        self.get_range_approximate_split_keys_cf(cf, range, region_id, cf_split_size, max_size, batch_split_limit)
    }

    fn get_range_approximate_split_keys_cf(&self, cfname: &str, range: Range, _region_id: u64, split_size: u64, max_size: u64, batch_split_limit: u64) -> Result<Vec<Vec<u8>>> {
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
                hex::encode_upper(&start_key),
                hex::encode_upper(&end_key)
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

    fn get_range_approximate_middle(&self, range: Range, region_id: u64) -> Result<Option<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_middle_cf(&self, cfname: &str, range: Range, region_id: u64) -> Result<Option<Vec<u8>>> {
        panic!()
    }

}
