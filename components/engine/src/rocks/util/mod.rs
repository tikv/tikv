// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub mod engine_metrics;
pub mod stats;

use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use self::engine_metrics::{
    ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES,
    ROCKSDB_NUM_FILES_AT_LEVEL, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE,
    ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE,
    ROCKSDB_TOTAL_SST_FILES_SIZE,
};
use crate::{Error, Result};
use rocksdb::load_latest_options;
use rocksdb::rocksdb::supported_compression;
use rocksdb::{
    CColumnFamilyDescriptor, ColumnFamilyOptions, DBCompressionType, DBOptions, Env,
    SliceTransform, DB,
};

pub use crate::rocks::CFHandle;
use engine_traits::CF_DEFAULT;

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

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
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

pub fn new_engine(
    path: &str,
    db_opts: Option<DBOptions>,
    cfs: &[&str],
    opts: Option<Vec<CFOptions<'_>>>,
) -> Result<DB> {
    let mut db_opts = match db_opts {
        Some(opt) => opt,
        None => DBOptions::new(),
    };
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

pub fn new_engine_opt(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions<'_>>,
) -> Result<DB> {
    // Creates a new db if it doesn't exist.
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

    // Lists all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();

    let cf_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    // If all column families exist, just open db.
    if existed == needed {
        let mut cfs_v = vec![];
        let mut cfs_opts_v = vec![];
        for mut x in cfs_opts {
            adjust_dynamic_level_bytes(&cf_descs, &mut x);
            cfs_v.push(x.cf);
            cfs_opts_v.push(x.options);
        }

        let db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cfs_opts_v).collect())?;
        return Ok(db);
    }

    // Opens db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                let mut tmp = CFOptions::new(x.cf, x.options.clone());
                adjust_dynamic_level_bytes(&cf_descs, &mut tmp);
                cfs_opts_v.push(tmp.options);
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    // Drops discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            db.drop_cf(cf)?;
        }
    }

    // Creates needed column families if they don't exist.
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

pub(crate) fn get_engine_cf_used_size(engine: &DB, handle: &CFHandle) -> u64 {
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
    use engine_traits::CF_DEFAULT;
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
            let mut db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db);
        }

        // add cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, opts.clone()),
            CFOptions::new("cf_dynamic_level_bytes", opts.clone()),
            CFOptions::new("cf1", opts),
        ];
        {
            let mut db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
            check_dynamic_level_bytes(&mut db);
        }

        // drop cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new("cf_dynamic_level_bytes", ColumnFamilyOptions::new()),
        ];
        {
            let mut db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
            check_dynamic_level_bytes(&mut db);
        }

        // never drop default cf
        let cfs_opts = vec![];
        new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
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
        let db = new_engine_opt(path_str, opts, vec![cf_opts]).unwrap();
        let cf = db.cf_handle(CF_DEFAULT).unwrap();

        assert!(get_engine_compression_ratio_at_level(&db, cf, 0).is_none());
        db.put_cf(cf, b"a", b"a").unwrap();
        db.flush_cf(cf, true).unwrap();
        assert!(get_engine_compression_ratio_at_level(&db, cf, 0).is_some());
    }
}
