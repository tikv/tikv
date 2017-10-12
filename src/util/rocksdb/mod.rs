// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod properties;
pub mod event_listener;
pub mod engine_metrics;
pub mod metrics_flusher;

pub use self::event_listener::EventListener;
pub use self::metrics_flusher::MetricsFlusher;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::str::FromStr;

use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK};
use rocksdb::{ColumnFamilyOptions, CompactOptions, DBCompressionType, DBOptions, ReadOptions,
              SliceTransform, Writable, WriteBatch, DB};
use rocksdb::rocksdb::supported_compression;
use util::rocksdb::engine_metrics::{ROCKSDB_COMPRESSION_RATIO_AT_LEVEL,
                                    ROCKSDB_CUR_SIZE_ALL_MEM_TABLES, ROCKSDB_TOTAL_SST_FILES_SIZE};
use util::rocksdb;

pub use rocksdb::CFHandle;

use super::cfs_diff;

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

pub fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .into_iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle, String> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found.", cf))
}

pub fn open(path: &str, cfs: &[&str]) -> Result<DB, String> {
    let mut opts = DBOptions::new();
    opts.create_if_missing(false);
    let mut cfs_opts = vec![];
    for _ in 0..cfs.len() {
        cfs_opts.push(ColumnFamilyOptions::new());
    }
    open_opt(opts, path, cfs.to_vec(), cfs_opts)
}

pub fn open_opt(
    opts: DBOptions,
    path: &str,
    cfs: Vec<&str>,
    cfs_opts: Vec<ColumnFamilyOptions>,
) -> Result<DB, String> {
    let cfds = cfs.into_iter().zip(cfs_opts).collect();
    DB::open_cf(opts, path, cfds)
}

pub struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
        CFOptions {
            cf: cf,
            options: options,
        }
    }
}

pub fn new_engine(path: &str, cfs: &[&str]) -> Result<DB, String> {
    let mut db_opts = DBOptions::new();
    db_opts.enable_statistics();
    let mut cfs_opts = Vec::with_capacity(cfs.len());
    for cf in cfs {
        cfs_opts.push(CFOptions::new(*cf, ColumnFamilyOptions::new()));
    }
    new_engine_opt(path, db_opts, cfs_opts)
}

fn check_and_open(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions>,
) -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs_v = vec![];
        let mut cf_opts_v = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs_v.push(x.cf);
            cf_opts_v.push(x.options.clone());
        }
        let cfds = cfs_v.into_iter().zip(cf_opts_v).collect();
        let mut db = DB::open_cf(db_opt, path, cfds)?;
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            db.create_cf((x.cf, x.options))?;
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();

    // If all column families are exist, just open db.
    if existed == needed {
        let mut cfs_v = vec![];
        let mut cfs_opts_v = vec![];
        for x in cfs_opts {
            cfs_v.push(x.cf);
            cfs_opts_v.push(x.options);
        }

        let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
        return DB::open_cf(db_opt, path, cfds);
    }

    // Open db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                cfs_opts_v.push(x.options.clone());
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    // Drop discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            db.drop_cf(cf)?;
        }
    }

    // Create needed column families not existed yet.
    for cf in cfs_diff(&needed, &existed) {
        db.create_cf((
            cf,
            cfs_opts
                .iter()
                .find(|x| x.cf == cf)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(db)
}

pub fn new_engine_opt(path: &str, opts: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
    check_and_open(path, opts, cfs_opts)
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

pub fn get_engine_used_size(engine: Arc<DB>) -> u64 {
    let mut used_size: u64 = 0;
    for cf in ALL_CFS {
        let handle = rocksdb::get_cf_handle(&engine, cf).unwrap();
        let cf_used_size = engine
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .expect("rocksdb is too old, missing total-sst-files-size property");

        used_size += cf_used_size;

        // For memtable
        if let Some(mem_table) =
            engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)
        {
            used_size += mem_table;
        }
    }
    used_size
}

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

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform {
            suffix_len: suffix_len,
        }
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
        FixedPrefixSliceTransform {
            prefix_len: prefix_len,
        }
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

pub fn roughly_cleanup_range(db: &DB, start_key: &[u8], end_key: &[u8]) -> Result<(), String> {
    if start_key > end_key {
        return Err(format!(
            "[roughly_cleanup_in_range] start_key({:?}) should't larger than end_key({:?}).",
            start_key,
            end_key
        ));
    }

    if start_key == end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        let handle = get_cf_handle(db, cf)?;
        if cf == CF_LOCK {
            // Todo: use delete_files_in_range after rocksdb support [start, end) semantics.
            let mut iter_opt = ReadOptions::new();
            iter_opt.fill_cache(false);
            iter_opt.set_iterate_upper_bound(end_key);
            let mut iter = db.iter_cf_opt(handle, iter_opt);
            iter.seek(start_key.into());
            let wb = WriteBatch::new();
            while iter.valid() {
                wb.delete_cf(handle, iter.key())?;
                iter.next();
            }
            if wb.count() > 0 {
                db.write(wb)?;
            }
        } else {
            db.delete_file_in_range_cf(handle, start_key, end_key)?;
        }
    }

    Ok(())
}

/// Compact the cf in the specified range by manual or not.
pub fn compact_range(
    db: &DB,
    handle: &CFHandle,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    exclusive_manual: bool,
) {
    let mut compact_opts = CompactOptions::new();
    // `exclusive_manual == false` means manual compaction can
    // concurrently run with other backgroud compactions.
    compact_opts.set_exclusive_manual_compaction(exclusive_manual);
    db.compact_range_cf_opt(handle, &compact_opts, start_key, end_key);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable, DB};
    use tempdir::TempDir;
    use storage::CF_DEFAULT;

    #[test]
    fn test_check_and_open() {
        let path = TempDir::new("_util_rocksdb_test_check_column_families").expect("");
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let cfs_opts = vec![CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new())];
        check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);

        // add cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new("cf1", ColumnFamilyOptions::new()),
        ];
        check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT, "cf1"]);

        // drop cf1.
        let cfs_opts = vec![CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new())];
        check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);

        // never drop default cf
        let cfs_opts = vec![];
        check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);
    }

    fn column_families_must_eq(path: &str, excepted: Vec<&str>) {
        let opts = DBOptions::new();
        let cfs_list = DB::list_column_families(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.iter().map(|v| *v).collect();
        cfs_existed.sort();
        cfs_excepted.sort();
        assert_eq!(cfs_existed, cfs_excepted);
    }

    #[test]
    fn test_compression_ratio() {
        let path = TempDir::new("_util_rocksdb_test_compression_ratio").expect("");
        let path_str = path.path().to_str().unwrap();

        let opts = DBOptions::new();
        let cf_opts = CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new());
        let db = check_and_open(path_str, opts, vec![cf_opts]).unwrap();
        let cf = db.cf_handle(CF_DEFAULT).unwrap();

        assert!(get_engine_compression_ratio_at_level(&db, cf, 0).is_none());
        db.put_cf(cf, b"a", b"a").unwrap();
        db.flush_cf(cf, true).unwrap();
        assert!(get_engine_compression_ratio_at_level(&db, cf, 0).is_some());
    }
}
