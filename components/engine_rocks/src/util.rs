// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{str::FromStr, sync::Arc};

use engine_traits::{Engines, Error, Range, Result, CF_DEFAULT};
use rocksdb::{CFHandle, Range as RocksRange, SliceTransform, DB};
use tikv_util::box_err;

use crate::{
    cf_options::RocksColumnFamilyOptions,
    db_options::RocksDBOptions,
    engine::RocksEngine,
    raw_util::{new_engine as new_engine_raw, new_engine_opt as new_engine_opt_raw, CFOptions},
    rocks_metrics_defs::*,
};

pub fn new_temp_engine(path: &tempfile::TempDir) -> Engines<RocksEngine, RocksEngine> {
    let raft_path = path.path().join(std::path::Path::new("raft"));
    Engines::new(
        new_engine(
            path.path().to_str().unwrap(),
            None,
            engine_traits::ALL_CFS,
            None,
        )
        .unwrap(),
        new_engine(
            raft_path.to_str().unwrap(),
            None,
            &[engine_traits::CF_DEFAULT],
            None,
        )
        .unwrap(),
    )
}

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    let engine =
        new_engine_raw(path, None, &[CF_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = RocksEngine::from_db(engine);
    Ok(engine)
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

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> RocksRange<'a> {
    RocksRange::new(range.start_key, range.end_key)
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

/// Gets the number of blob files at given level of given column family.
pub fn get_cf_num_blob_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_cf_num_immutable_mem_table(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE)
}

/// Gets the amount of pending compaction bytes of given column family.
pub fn get_cf_pending_compaction_bytes(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_PENDING_COMPACTION_BYTES)
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}
