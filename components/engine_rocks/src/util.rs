// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksColumnFamilyOptions;
use crate::db_options::RocksDBOptions;
use crate::engine::RocksEngine;
use crate::rocks_metrics_defs::*;
use engine::rocks::util::new_engine as new_engine_raw;
use engine::rocks::util::new_engine_opt as new_engine_opt_raw;
use engine::rocks::util::CFOptions;
use engine_traits::Range;
use engine_traits::CF_DEFAULT;
use engine_traits::{Error, Result};
use rocksdb::Range as RocksRange;
use rocksdb::{CFHandle, DB};
use std::str::FromStr;
use std::sync::Arc;

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    let engine =
        new_engine_raw(path, None, &[CF_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub fn get_engine_cf_used_size(engine: &DB, handle: &CFHandle) -> u64 {
    let mut cf_used_size = engine
        .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");
    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        cf_used_size += mem_table;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        cf_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
        engine.get_property_int_cf(handle, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        cf_used_size += obsolete_blob;
    }

    cf_used_size
}

/// Gets engine's compression ratio at given level.
pub fn get_engine_compression_ratio_at_level(
    engine: &DB,
    handle: &CFHandle,
    level: usize,
) -> Option<f64> {
    let prop = format!("{}{}", ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, level);
    if let Some(v) = engine.get_property_value_cf(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // RocksDB returns -1.0 if the level is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of files at given level of given column family.
pub fn get_cf_num_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_NUM_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_num_immutable_mem_table(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE)
}

pub struct RocksCFOptions<'a> {
    cf: &'a str,
    options: RocksColumnFamilyOptions,
}

impl<'a> RocksCFOptions<'a> {
    pub fn new(cf: &'a str, options: RocksColumnFamilyOptions) -> RocksCFOptions<'a> {
        RocksCFOptions { cf, options }
    }

    pub fn into_raw(self) -> CFOptions<'a> {
        CFOptions::new(self.cf, self.options.into_raw())
    }
}

pub fn new_engine(
    path: &str,
    db_opts: Option<RocksDBOptions>,
    cfs: &[&str],
    opts: Option<Vec<RocksCFOptions<'_>>>,
) -> Result<RocksEngine> {
    let db_opts = db_opts.map(RocksDBOptions::into_raw);
    let opts = opts.map(|o| o.into_iter().map(RocksCFOptions::into_raw).collect());
    let engine = new_engine_raw(path, db_opts, cfs, opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: RocksDBOptions,
    cfs_opts: Vec<RocksCFOptions<'_>>,
) -> Result<RocksEngine> {
    let db_opt = db_opt.into_raw();
    let cfs_opts = cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect();
    let engine =
        new_engine_opt_raw(path, db_opt, cfs_opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> RocksRange<'a> {
    RocksRange::new(range.start_key, range.end_key)
}
