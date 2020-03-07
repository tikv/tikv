// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksColumnFamilyOptions;
use crate::db_options::RocksDBOptions;
use crate::engine::RocksEngine;
use crate::rocks_metrics_defs::*;
use engine_traits::CF_DEFAULT;
use engine_traits::{Error, Result};
use engine_traits::{Range, ALL_CFS};
use rocksdb::rocksdb::supported_compression;
use rocksdb::{CColumnFamilyDescriptor, CFHandle, ColumnFamilyOptions, DBOptions, DB};
use rocksdb::{
    CompactOptions, CompactionOptions, DBCompressionType, Range as RocksRange, SliceTransform,
};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::{cmp, fs};

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

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

pub fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

pub fn ingest_maybe_slowdown_writes(db: &DB, cf: &str) -> bool {
    let handle = get_cf_handle(db, cf).unwrap();
    if let Some(n) = get_cf_num_files_at_level(db, handle, 0) {
        let options = db.get_options_cf(handle);
        let slowdown_trigger = options.get_level_zero_slowdown_writes_trigger();
        // Leave enough buffer to tolerate heavy write workload,
        // which may flush some memtables in a short time.
        if n > u64::from(slowdown_trigger) / 2 {
            return true;
        }
    }
    false
}

pub fn open_opt(
    opts: DBOptions,
    path: &str,
    cfs: Vec<&str>,
    cfs_opts: Vec<ColumnFamilyOptions>,
) -> Result<DB> {
    let db = DB::open_cf(opts, path, cfs.into_iter().zip(cfs_opts).collect())?;
    Ok(db)
}

pub struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
        CFOptions { cf, options }
    }
}

/// Turns "dynamic level size" off for the existing column family which was off before.
/// Column families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    cf_descs: &[CColumnFamilyDescriptor],
    cf_options: &mut CFOptions<'_>,
) {
    if let Some(ref cf_desc) = cf_descs
        .iter()
        .find(|cf_desc| cf_desc.name() == cf_options.cf)
    {
        let existed_dynamic_level_bytes =
            cf_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes
            != cf_options
                .options
                .get_level_compaction_dynamic_level_bytes()
        {
            warn!(
                "change dynamic_level_bytes for existing column family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => cf_options.options.get_level_compaction_dynamic_level_bytes(),
            );
        }
        cf_options
            .options
            .set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, `DB::list_column_families` fails and we can clean up
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

/// Gets total used size of rocksdb engine, including:
/// *  total size (bytes) of all SST files.
/// *  total size (bytes) of active and unflushed immutable memtables.
/// *  total size (bytes) of all blob files.
///
pub fn get_engine_used_size(engine: Arc<DB>) -> u64 {
    let mut used_size: u64 = 0;
    for cf in ALL_CFS {
        let handle = get_cf_handle(&engine, cf).unwrap();
        used_size += get_engine_cf_used_size(&engine, handle);
    }
    used_size
}

/// Checks whether any column family sets `disable_auto_compactions` to `True` or not.
pub fn auto_compactions_is_disabled(engine: &DB) -> bool {
    for cf_name in engine.cf_names() {
        let cf = engine.cf_handle(cf_name).unwrap();
        if engine.get_options_cf(cf).get_disable_auto_compactions() {
            return true;
        }
    }
    false
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

/// Roughly deletes files in multiple ranges.
///
/// Note:
///    - After this operation, some keys in the range might still exist in the database.
///    - After this operation, some keys in the range might be removed from existing snapshot,
///      so you shouldn't expect to be able to read data from the range using existing snapshots
///      any more.
///
/// Ref: https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
pub fn roughly_cleanup_ranges(db: &DB, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    let mut delete_ranges = Vec::new();
    for &(ref start, ref end) in ranges {
        if start == end {
            continue;
        }
        assert!(start < end);
        delete_ranges.push(Range::new(start, end));
    }
    if delete_ranges.is_empty() {
        return Ok(());
    }

    for cf in db.cf_names() {
        let handle = get_cf_handle(db, cf)?;
        db.delete_files_in_ranges_cf(handle, &delete_ranges, /* include_end */ false)?;
    }

    Ok(())
}

/// Compacts the column families in the specified range by manual or not.
pub fn compact_range(
    db: &DB,
    handle: &CFHandle,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    exclusive_manual: bool,
    max_subcompactions: u32,
) {
    let mut compact_opts = CompactOptions::new();
    // `exclusive_manual == false` means manual compaction can
    // concurrently run with other background compactions.
    compact_opts.set_exclusive_manual_compaction(exclusive_manual);
    compact_opts.set_max_subcompactions(max_subcompactions as i32);
    db.compact_range_cf_opt(handle, &compact_opts, start_key, end_key);
}

/// Compacts files in the range and above the output level.
/// Compacts all files if the range is not specified.
/// Compacts all files to the bottommost level if the output level is not specified.
pub fn compact_files_in_range(
    db: &DB,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    output_level: Option<i32>,
) -> Result<()> {
    for cf_name in db.cf_names() {
        compact_files_in_range_cf(db, cf_name, start, end, output_level)?;
    }
    Ok(())
}

/// Compacts files in the range and above the output level of the given column family.
/// Compacts all files to the bottommost level if the output level is not specified.
pub fn compact_files_in_range_cf(
    db: &DB,
    cf_name: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    output_level: Option<i32>,
) -> Result<()> {
    let cf = db.cf_handle(cf_name).unwrap();
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

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::{ColumnFamilyOptions, DBOptions, Writable, DB};
    use engine_traits::{CFHandleExt, KvEngine, Mutable, CF_DEFAULT};
    use rocksdb::{ColumnFamilyOptions, DBOptions, DB};
    use tempfile::Builder;

    #[test]
    fn test_cfs_diff() {
        let a = vec!["1", "2", "3"];
        let a_diff_a = cfs_diff(&a, &a);
        assert!(a_diff_a.is_empty());
        let b = vec!["4"];
        assert_eq!(a, cfs_diff(&a, &b));
        let c = vec!["4", "5", "3", "6"];
        assert_eq!(vec!["1", "2"], cfs_diff(&a, &c));
        assert_eq!(vec!["4", "5", "6"], cfs_diff(&c, &a));
        let d = vec!["1", "2", "3", "4"];
        let a_diff_d = cfs_diff(&a, &d);
        assert!(a_diff_d.is_empty());
        assert_eq!(vec!["4"], cfs_diff(&d, &a));
    }

    #[test]
    fn test_new_engine_opt() {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_check_column_families")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let mut cfs_opts = vec![CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new())];
        let mut opts = ColumnFamilyOptions::new();
        opts.set_level_compaction_dynamic_level_bytes(true);
        cfs_opts.push(CFOptions::new("cf_dynamic_level_bytes", opts.clone()));
        {
            let mut db = new_engine_opt(
                path_str,
                RocksDBOptions::from_raw(DBOptions::new()),
                cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect(),
            )
            .unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db.as_inner());
        }

        // add cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, opts.clone()),
            CFOptions::new("cf_dynamic_level_bytes", opts.clone()),
            CFOptions::new("cf1", opts),
        ];
        {
            let mut db = new_engine_opt(
                path_str,
                RocksDBOptions::from_raw(DBOptions::new()),
                cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect(),
            )
            .unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
            check_dynamic_level_bytes(&mut db.as_inner());
        }

        // drop cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new("cf_dynamic_level_bytes", ColumnFamilyOptions::new()),
        ];
        {
            let mut db = new_engine_opt(
                path_str,
                RocksDBOptions::from_raw(DBOptions::new()),
                cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect(),
            )
            .unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db.as_inner());
        }

        // never drop default cf
        let cfs_opts = vec![];
        new_engine_opt(
            path_str,
            RocksDBOptions::from_raw(DBOptions::new()),
            cfs_opts,
        )
        .unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);
    }

    fn column_families_must_eq(path: &str, excepted: Vec<&str>) {
        let opts = DBOptions::new();
        let cfs_list = DB::list_column_families(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.clone();
        cfs_existed.sort();
        cfs_excepted.sort();
        assert_eq!(cfs_existed, cfs_excepted);
    }

    fn check_dynamic_level_bytes(db: &mut DB) {
        let cf_default = db.cf_handle(CF_DEFAULT).unwrap();
        let tmp_cf_opts = db.get_options_cf(cf_default);
        assert!(!tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
        let cf_test = db.cf_handle("cf_dynamic_level_bytes").unwrap();
        let tmp_cf_opts = db.get_options_cf(cf_test);
        assert!(tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
    }

    #[test]
    fn test_compression_ratio() {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_compression_ratio")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let opts = DBOptions::new();
        let cf_opts = CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new());
        let db = new_engine_opt(
            path_str,
            RocksDBOptions::from_raw(opts),
            vec![cf_opts]
                .into_iter()
                .map(RocksCFOptions::into_raw)
                .collect(),
        )
        .unwrap();
        let cf = db.cf_handle(CF_DEFAULT).unwrap();

        assert!(get_engine_compression_ratio_at_level(&db.as_inner(), cf.as_inner(), 0).is_none());
        db.put_cf(CF_DEFAULT, b"a", b"a").unwrap();
        db.flush_cf(cf, true).unwrap();
        assert!(get_engine_compression_ratio_at_level(&db.as_inner(), cf.as_inner(), 0).is_some());
    }

    #[test]
    fn test_compact_files_in_range() {
        let temp_dir = Builder::new()
            .prefix("test_compact_files_in_range")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("test", cf_opts),
        ];
        let cfs_opts = cfs_opts.into_iter().map(RocksCFOptions::into_raw).collect();
        let db = new_engine(
            temp_dir.path().to_str().unwrap(),
            None,
            &["default", "test"],
            Some(cfs_opts),
        )
        .unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            for i in 0..5 {
                db.put_cf(cf_name, &[i], &[i]).unwrap();
                db.put_cf(cf_name, &[i + 1], &[i + 1]).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            assert_eq!(cf_levels.first().unwrap().get_files().len(), 5);
        }

        // # Before
        // Level-0: [4-5], [3-4], [2-3], [1-2], [0-1]
        // # After
        // Level-0: [4-5]
        // Level-1: [0-4]
        compact_files_in_range(&db.as_inner(), None, Some(&[4]), Some(1)).unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            let level_0 = cf_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_1 = cf_levels[1].get_files();
            assert_eq!(level_1.len(), 1);
            assert_eq!(level_1[0].get_smallestkey(), &[0]);
            assert_eq!(level_1[0].get_largestkey(), &[4]);
        }

        // # Before
        // Level-0: [4-5]
        // Level-1: [0-4]
        // # After
        // Level-0: [4-5]
        // Level-N: [0-4]
        compact_files_in_range(&db.as_inner(), Some(&[2]), Some(&[4]), None).unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_opts = db.get_options_cf(cf);
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            let level_0 = cf_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_n = cf_levels[cf_opts.get_num_levels() - 1].get_files();
            assert_eq!(level_n.len(), 1);
            assert_eq!(level_n[0].get_smallestkey(), &[0]);
            assert_eq!(level_n[0].get_largestkey(), &[4]);
        }
    }
}
