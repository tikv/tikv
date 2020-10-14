// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{
    CFHandleExt, Error, Range, RangePropertiesExt, Result, CF_DEFAULT, CF_LOCK, CF_WRITE, LARGE_CFS,
};
use std::ops::Range as StdRange;

impl RangePropertiesExt for SkiplistEngine {
    fn get_range_approximate_keys(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
        self.get_range_approximate_keys_cf(CF_WRITE, range, region_id, large_threshold)
    }

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
        let engine = self.get_cf_engine(cfname)?;
        let mut count = 0;
        engine
            .range(StdRange {
                start: (range.start_key.to_vec(), 0),
                end: (range.end_key.to_vec(), 0),
            })
            .for_each(|_| count += 1);
        Ok(count)
    }

    fn get_range_approximate_size(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64> {
        let mut size = 0;
        for cf in LARGE_CFS {
            size += self.get_range_approximate_keys_cf(cf, range, region_id, large_threshold)?;
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
        let engine = self.get_cf_engine(cfname)?;
        let mut count = 0;
        engine
            .range(StdRange {
                start: (range.start_key.to_vec(), 0),
                end: (range.end_key.to_vec(), 0),
            })
            .for_each(|e| count += e.key().0.len() + e.value().len());
        Ok(count as u64)
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
            (CF_DEFAULT, get_cf_size(CF_DEFAULT)?),
            (CF_WRITE, get_cf_size(CF_WRITE)?),
            // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
            // backward compatibility.
            (CF_LOCK, get_cf_size(CF_LOCK)?),
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
        region_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>> {
        let engine = self.get_cf_engine(cfname)?;
        let mut split_keys = vec![];
        let mut cur_size = 0;
        engine
            .range(StdRange {
                start: (range.start_key.to_vec(), 0),
                end: (range.end_key.to_vec(), 0),
            })
            .for_each(|e| {
                cur_size += e.key().0.len() + e.value().len();
                if cur_size as u64 >= split_size {
                    split_keys.push(e.key().0.clone());
                    cur_size = 0;
                }
            });
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
        region_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let keys = self.get_range_approximate_keys_cf(cfname, range, region_id, std::u64::MAX)?;
        if keys == 0 {
            return Ok(None);
        }
        let engine = self.get_cf_engine(cfname)?;
        Ok(engine
            .range(StdRange {
                start: (range.start_key.to_vec(), 0),
                end: (range.end_key.to_vec(), 0),
            })
            .nth((keys / 2) as usize)
            .map(|e| e.key().0.clone()))
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
        region_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let engine = self.get_cf_engine(cf)?;
        let mut keys = vec![];
        let mut count = 0;
        engine
            .range(StdRange {
                start: (range.start_key.to_vec(), 0),
                end: (range.end_key.to_vec(), 0),
            })
            .for_each(|e| {
                if count >= parts {
                    count = 0;
                    keys.push(e.key().0.clone());
                }
                count += 1;
            });
        Ok(keys)
    }
}
