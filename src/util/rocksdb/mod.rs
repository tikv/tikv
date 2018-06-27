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

pub mod engine_metrics;
pub mod event_listener;
pub mod metrics_flusher;
pub mod properties;
pub mod stats;

pub use self::event_listener::{CompactedEvent, CompactionListener, EventListener};
pub use self::metrics_flusher::MetricsFlusher;

use std::cmp;
use std::fs::{self, File};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use rocksdb::rocksdb::supported_compression;
use rocksdb::set_external_sst_file_global_seq_no;
use rocksdb::{
    ColumnFamilyOptions, CompactOptions, CompactionOptions, DBCompressionType, DBOptions, Range,
    SliceTransform, DB,
};
use storage::{ALL_CFS, CF_DEFAULT};
use sys_info;
use util::file::{calc_crc32, copy_and_sync};
use util::rocksdb;
use util::rocksdb::engine_metrics::{
    ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES,
    ROCKSDB_TOTAL_SST_FILES_SIZE,
};

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

pub fn open_opt(
    opts: DBOptions,
    path: &str,
    cfs: Vec<&str>,
    cfs_opts: Vec<ColumnFamilyOptions>,
) -> Result<DB, String> {
    DB::open_cf(opts, path, cfs.into_iter().zip(cfs_opts).collect())
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

pub fn new_engine(path: &str, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<DB, String> {
    let mut db_opts = DBOptions::new();
    db_opts.enable_statistics(true);
    let cf_opts = match opts {
        Some(opts_vec) => opts_vec,
        None => {
            let mut default_cfs_opts = Vec::with_capacity(cfs.len());
            for cf in cfs {
                default_cfs_opts.push(CFOptions::new(*cf, ColumnFamilyOptions::new()));
            }
            default_cfs_opts
        }
    };
    new_engine_opt(path, db_opts, cf_opts)
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
        let mut db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cf_opts_v).collect())?;
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

        return DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cfs_opts_v).collect());
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
        if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)
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

pub fn roughly_cleanup_ranges(db: &DB, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
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

/// Compact the cf in the specified range by manual or not.
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

/// Compact files in the range and above the output level.
/// Compact all files if the range is not specified.
/// Compact all files to the bottommost level if the output level is not specified.
pub fn compact_files_in_range(
    db: &DB,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    output_level: Option<i32>,
) -> Result<(), String> {
    for cf_name in db.cf_names() {
        compact_files_in_range_cf(db, cf_name, start, end, output_level)?;
    }
    Ok(())
}

pub fn compact_files_in_range_cf(
    db: &DB,
    cf_name: &str,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    output_level: Option<i32>,
) -> Result<(), String> {
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
    let max_subcompactions = sys_info::cpu_num().unwrap();
    let max_subcompactions = cmp::min(max_subcompactions, 32);
    opts.set_max_subcompactions(max_subcompactions as i32);
    opts.set_output_file_size_limit(output_file_size_limit);
    db.compact_files_cf(cf, &opts, &input_files, output_level)?;

    Ok(())
}

/// Prepare the SST file for ingestion.
/// The purpose is to make the ingestion retryable when using the `move_files` option.
/// Things we need to consider here:
/// 1. We need to access the original file on retry, so we should make a clone
///    before ingestion.
/// 2. `RocksDB` will modified the global seqno of the ingested file, so we need
///    to modified the global seqno back to 0 so that we can pass the checksum
///    validation.
/// 3. If the file has been ingested to `RocksDB`, we should not modified the
///    global seqno directly, because that may corrupt RocksDB's data.
#[cfg(target_os = "linux")]
pub fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
) -> Result<(), String> {
    use std::os::linux::fs::MetadataExt;

    let path = path.as_ref().to_str().unwrap();
    let clone = clone.as_ref().to_str().unwrap();

    if Path::new(clone).exists() {
        fs::remove_file(clone).map_err(|e| format!("remove {}: {:?}", clone, e))?;
    }

    let meta = fs::metadata(path).map_err(|e| format!("read metadata from {}: {:?}", path, e))?;

    if meta.st_nlink() == 1 {
        // RocksDB must not have this file, we can make a hard link.
        fs::hard_link(path, clone)
            .map_err(|e| format!("link from {} to {}: {:?}", path, clone, e))?;
    } else {
        // RocksDB may have this file, we should make a copy.
        copy_and_sync(path, clone)
            .map_err(|e| format!("copy from {} to {}: {:?}", path, clone, e))?;
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
) -> Result<(), String> {
    let path = path.as_ref().to_str().unwrap();
    let clone = clone.as_ref().to_str().unwrap();
    if !Path::new(clone).exists() {
        copy_and_sync(path, clone)
            .map_err(|e| format!("copy from {} to {}: {:?}", path, clone, e))?;
    }
    Ok(())
}

pub fn validate_sst_for_ingestion<P: AsRef<Path>>(
    db: &DB,
    cf: &str,
    path: P,
    expected_size: u64,
    expected_checksum: u32,
) -> Result<(), String> {
    let path = path.as_ref().to_str().unwrap();
    let f = File::open(path).map_err(|e| format!("open {}: {:?}", path, e))?;

    let meta = f
        .metadata()
        .map_err(|e| format!("read metadata from {}: {:?}", path, e))?;
    if meta.len() != expected_size {
        return Err(format!(
            "invalid size {} for {}, expected {}",
            meta.len(),
            path,
            expected_size
        ));
    }

    let checksum = calc_crc32(path).map_err(|e| format!("calc crc32 for {}: {:?}", path, e))?;
    if checksum == expected_checksum {
        return Ok(());
    }

    // RocksDB may have modified the global seqno.
    let cf_handle = get_cf_handle(db, cf)?;
    set_external_sst_file_global_seq_no(db, cf_handle, path, 0)?;
    f.sync_all().map_err(|e| format!("sync {}: {:?}", path, e))?;

    let checksum = calc_crc32(path).map_err(|e| format!("calc crc32 for {}: {:?}", path, e))?;
    if checksum != expected_checksum {
        return Err(format!(
            "invalid checksum {} for {}, expected {}",
            checksum, path, expected_checksum
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{
        ColumnFamilyOptions, DBOptions, EnvOptions, IngestExternalFileOptions, SstFileWriter,
        Writable, DB,
    };
    use storage::CF_DEFAULT;
    use tempdir::TempDir;

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

    #[cfg(target_os = "linux")]
    fn check_hard_link<P: AsRef<Path>>(path: P, nlink: u64) {
        use std::os::linux::fs::MetadataExt;
        assert_eq!(fs::metadata(path).unwrap().st_nlink(), nlink);
    }

    #[cfg(not(target_os = "linux"))]
    fn check_hard_link<P: AsRef<Path>>(_: P, _: u64) {
        // Just do nothing
    }

    fn gen_sst_with_kvs(db: &DB, cf: &CFHandle, path: &str, kvs: &[(&str, &str)]) {
        let opts = db.get_options_cf(cf).clone();
        let mut writer = SstFileWriter::new(EnvOptions::new(), opts);
        writer.open(path).unwrap();
        for &(k, v) in kvs {
            writer.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    fn check_db_with_kvs(db: &DB, cf: &CFHandle, kvs: &[(&str, &str)]) {
        for &(k, v) in kvs {
            assert_eq!(db.get_cf(cf, k.as_bytes()).unwrap().unwrap(), v.as_bytes());
        }
    }

    #[test]
    fn test_prepare_sst_for_ingestion() {
        let path = TempDir::new("_util_rocksdb_test_prepare_sst_for_ingestion").expect("");
        let path_str = path.path().to_str().unwrap();

        let sst_dir = TempDir::new("_util_rocksdb_test_prepare_sst_for_ingestion_sst").expect("");
        let sst_path = sst_dir.path().join("abc.sst");
        let sst_clone = sst_dir.path().join("abc.sst.clone");

        let kvs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")];

        let cf_name = "default";
        let db = new_engine(path_str, &[cf_name], None).unwrap();
        let cf = db.cf_handle(cf_name).unwrap();
        let mut ingest_opts = IngestExternalFileOptions::new();
        ingest_opts.move_files(true);

        gen_sst_with_kvs(&db, cf, sst_path.to_str().unwrap(), &kvs);
        let size = fs::metadata(&sst_path).unwrap().len();
        let checksum = calc_crc32(&sst_path).unwrap();

        // The first ingestion will hard link sst_path to sst_clone.
        check_hard_link(&sst_path, 1);
        prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        validate_sst_for_ingestion(&db, cf_name, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        // If we prepare again, it will use hard link too.
        prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        validate_sst_for_ingestion(&db, cf_name, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        db.ingest_external_file_cf(cf, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf, &kvs);
        assert!(!sst_clone.exists());

        // The second ingestion will copy sst_path to sst_clone.
        check_hard_link(&sst_path, 2);
        prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        validate_sst_for_ingestion(&db, cf_name, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 1);
        db.ingest_external_file_cf(cf, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf, &kvs);
        assert!(!sst_clone.exists());
    }

    #[test]
    fn test_compact_files_in_range() {
        let temp_dir = TempDir::new("test_compact_files_in_range").unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("test", cf_opts.clone()),
        ];
        let db = new_engine(
            temp_dir.path().to_str().unwrap(),
            &["default", "test"],
            Some(cfs_opts),
        ).unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            for i in 0..5 {
                db.put_cf(cf, &[i], &[i]).unwrap();
                db.put_cf(cf, &[i + 1], &[i + 1]).unwrap();
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
        compact_files_in_range(&db, None, Some(&[4]), Some(1)).unwrap();

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
        compact_files_in_range(&db, Some(&[2]), Some(&[4]), None).unwrap();

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
