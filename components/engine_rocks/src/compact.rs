// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CompactExt, Result, CFNamesExt};
use crate::engine::RocksEngine;
use crate::util;
use rocksdb::{CompactOptions, CompactionOptions, DBCompressionType};
use std::cmp;

impl CompactExt for RocksEngine {
    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        for cf_name in self.cf_names() {
            let cf = util::get_cf_handle(self.as_inner(), cf_name)?;
            if self.as_inner().get_options_cf(cf).get_disable_auto_compactions() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        let db = self.as_inner();
        let handle = util::get_cf_handle(db, cf)?;
        let mut compact_opts = CompactOptions::new();
        // `exclusive_manual == false` means manual compaction can
        // concurrently run with other background compactions.
        compact_opts.set_exclusive_manual_compaction(exclusive_manual);
        compact_opts.set_max_subcompactions(max_subcompactions as i32);
        db.compact_range_cf_opt(handle, &compact_opts, start_key, end_key);
        Ok(())
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        for cf_name in self.cf_names() {
            self.compact_files_in_range_cf(cf_name, start, end, output_level)?;
        }
        Ok(())
    }        

    fn compact_files_in_range_cf(
        &self,
        cf_name: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        let db = self.as_inner();
        let cf = util::get_cf_handle(db, cf_name)?;
        let cf_opts = db.get_options_cf(cf);
        let output_level = output_level.unwrap_or(cf_opts.get_num_levels() as i32 - 1);
        let output_compression = cf_opts
            .get_compression_per_level()
            .get(output_level as usize)
            .cloned()
            .unwrap_or(DBCompressionType::No);
        let output_file_size_limit = cf_opts.get_target_file_size_base() as usize;

        let mut input_files = Vec::new();
        let cf_meta = db.get_column_family_meta_data(cf);
        for (i, level) in cf_meta.get_levels().iter().enumerate() {
            if i as i32 >= output_level {
                break;
            }
            for f in level.get_files() {
                if end.is_some() && end.unwrap() <= f.get_smallestkey() {
                    continue;
                }
                if start.is_some() && start.unwrap() > f.get_largestkey() {
                    continue;
                }
                input_files.push(f.get_name());
            }
        }
        if input_files.is_empty() {
            return Ok(());
        }

        let mut opts = CompactionOptions::new();
        opts.set_compression(output_compression);
        let max_subcompactions = sysinfo::get_logical_cores();
        let max_subcompactions = cmp::min(max_subcompactions, 32);
        opts.set_max_subcompactions(max_subcompactions as i32);
        opts.set_output_file_size_limit(output_file_size_limit);
        db.compact_files_cf(cf, &opts, &input_files, output_level)?;

        Ok(())
    }
}
