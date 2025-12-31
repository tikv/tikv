// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ffi::CStr, path::Path, sync::Arc};

use engine_traits::{Result, CF_DEFAULT};
use slog_global::warn;
use tirocks::{
    db::{MultiCfBuilder, MultiCfTitanBuilder, RawCfHandle},
    env::Env,
    option::RawCfOptions,
    perf_context::PerfLevel,
    slice_transform::SliceTransform,
    CfOptions, Db, OpenOptions, Statistics,
};

use crate::{cf_options::RocksCfOptions, db_options::RocksDbOptions, r2e, RocksEngine};

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| !b.iter().any(|y| *x == y))
        .cloned()
        .collect()
}

/// Turns "dynamic level size" off for the existing column family which was off
/// before. Column families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    cf_descs: &[(String, CfOptions)],
    name: &str,
    opt: &mut RawCfOptions,
) {
    if let Some((_, exist_opt)) = cf_descs.iter().find(|(n, _)| n == name) {
        let existed_dynamic_level_bytes = exist_opt.level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes != opt.level_compaction_dynamic_level_bytes() {
            warn!(
                "change dynamic_level_bytes for existing column family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => opt.level_compaction_dynamic_level_bytes(),
            );
        }
        opt.set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

fn new_sanitized(
    path: &Path,
    db_opt: RocksDbOptions,
    cf_opts: Vec<(&str, RocksCfOptions)>,
) -> Result<Db> {
    if !db_opt.is_titan() {
        let mut builder = MultiCfBuilder::new(db_opt.into_rocks());
        for (name, opt) in cf_opts {
            builder.add_cf(name, opt.into_rocks());
        }
        builder.open(path.as_ref()).map_err(r2e)
    } else {
        let mut builder = MultiCfTitanBuilder::new(db_opt.into_titan());
        for (name, opt) in cf_opts {
            builder.add_cf(name, opt.into_titan());
        }
        builder.open(path.as_ref()).map_err(r2e)
    }
}

pub fn new_engine(path: &Path, cfs: &[&str]) -> Result<RocksEngine> {
    let mut db_opts = RocksDbOptions::default();
    db_opts.set_statistics(&Statistics::default());
    let cf_opts = cfs.iter().map(|name| (*name, Default::default())).collect();
    new_engine_opt(path, db_opts, cf_opts)
}

pub fn new_engine_opt(
    path: &Path,
    mut db_opt: RocksDbOptions,
    cf_opts: Vec<(&str, RocksCfOptions)>,
) -> Result<RocksEngine> {
    let is_titan = db_opt.is_titan();
    for (_, opt) in &cf_opts {
        // It's possible to convert non-titan to titan. But in our usage, they can't
        // be mixed used. So assert to detect bugs.
        assert_eq!(is_titan, opt.is_titan(), "Must pass the same option type");
    }
    if cf_opts.iter().all(|(name, _)| *name != CF_DEFAULT) {
        return Err(engine_traits::Error::Engine(
            engine_traits::Status::with_error(
                engine_traits::Code::InvalidArgument,
                "default cf must be specified",
            ),
        ));
    }
    if !RocksEngine::exists(path).unwrap_or(false) {
        db_opt.set_create_if_missing(true);
        db_opt.set_create_missing_column_families(true);
        let db = new_sanitized(path, db_opt, cf_opts)?;
        return Ok(RocksEngine::new(Arc::new(db)));
    }

    db_opt.set_create_if_missing(false);

    // Lists all column families in current db.
    let cfs_list = Db::list_cfs(&db_opt, path).map_err(r2e)?;
    let existed: Vec<_> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<_> = cf_opts.iter().map(|(name, _)| *name).collect();

    let cf_descs = if !existed.is_empty() {
        let res = if let Some(env) = db_opt.env() {
            Db::load_latest_options(path, env, true)
        } else {
            Db::load_latest_options(path, &Env::default(), true)
        };
        res.unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .1
    } else {
        vec![]
    };

    // Lifetime hack. We need to make `&str` have smaller scope. It will be
    // optimized away in release mode.
    let mut cf_opts: Vec<_> = cf_opts.into_iter().collect();
    for cf in &existed {
        if cf_opts.iter().all(|(name, _)| name != cf) {
            if !is_titan {
                cf_opts.push((cf, RocksCfOptions::default()));
            } else {
                cf_opts.push((cf, RocksCfOptions::default_titan()))
            }
        }
    }
    for (name, opt) in &mut cf_opts {
        adjust_dynamic_level_bytes(&cf_descs, name, opt);
    }

    // We have added all missing options by iterating `existed`. If two vecs still
    // have same length, then they must have same column families dispite their
    // orders. So just open db.
    if needed.len() == existed.len() && needed.len() == cf_opts.len() {
        let db = new_sanitized(path, db_opt, cf_opts)?;
        return Ok(RocksEngine::new(Arc::new(db)));
    }

    // Opens db.
    db_opt.set_create_missing_column_families(true);
    let mut db = new_sanitized(path, db_opt, cf_opts)?;

    // Drops discarded column families.
    for cf in cfs_diff(&existed, &needed) {
        // We have checked it at the very beginning, so it must be needed.
        assert_ne!(cf, CF_DEFAULT);
        db.destroy_cf(cf).map_err(r2e)?;
    }

    Ok(RocksEngine::new(Arc::new(db)))
}

/// A slice transform that removes fixed length suffix from key.
pub struct FixedSuffixSliceTransform {
    name: &'static CStr,
    suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(name: &'static CStr, suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { name, suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    #[inline]
    fn name(&self) -> &CStr {
        self.name
    }

    #[inline]
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        &key[..mid]
    }

    #[inline]
    fn in_domain(&self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }
}

/// A slice transform that keeps fixed length prefix from key.
pub struct FixedPrefixSliceTransform {
    name: &'static CStr,
    prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(name: &'static CStr, prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { name, prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    #[inline]
    fn name(&self) -> &CStr {
        self.name
    }

    #[inline]
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    #[inline]
    fn in_domain(&self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }
}

/// A slice tranform that always returns identical key.
pub struct NoopSliceTransform {
    name: &'static CStr,
}

impl Default for NoopSliceTransform {
    fn default() -> Self {
        Self {
            name: CStr::from_bytes_with_nul(b"NoopSliceTransform\0").unwrap(),
        }
    }
}

impl SliceTransform for NoopSliceTransform {
    #[inline]
    fn name(&self) -> &CStr {
        self.name
    }

    #[inline]
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    #[inline]
    fn in_domain(&self, _key: &[u8]) -> bool {
        true
    }
}

pub fn to_rocks_perf_level(level: engine_traits::PerfLevel) -> PerfLevel {
    match level {
        engine_traits::PerfLevel::Uninitialized => PerfLevel::kUninitialized,
        engine_traits::PerfLevel::Disable => PerfLevel::kDisable,
        engine_traits::PerfLevel::EnableCount => PerfLevel::kEnableCount,
        engine_traits::PerfLevel::EnableTimeExceptForMutex => PerfLevel::kEnableTimeExceptForMutex,
        engine_traits::PerfLevel::EnableTimeAndCpuTimeExceptForMutex => {
            PerfLevel::kEnableTimeAndCPUTimeExceptForMutex
        }
        engine_traits::PerfLevel::EnableTime => PerfLevel::kEnableTime,
        engine_traits::PerfLevel::OutOfBounds => PerfLevel::kOutOfBounds,
    }
}

pub fn to_engine_perf_level(level: PerfLevel) -> engine_traits::PerfLevel {
    match level {
        PerfLevel::kUninitialized => engine_traits::PerfLevel::Uninitialized,
        PerfLevel::kDisable => engine_traits::PerfLevel::Disable,
        PerfLevel::kEnableCount => engine_traits::PerfLevel::EnableCount,
        PerfLevel::kEnableTimeExceptForMutex => engine_traits::PerfLevel::EnableTimeExceptForMutex,
        PerfLevel::kEnableTimeAndCPUTimeExceptForMutex => {
            engine_traits::PerfLevel::EnableTimeAndCpuTimeExceptForMutex
        }
        PerfLevel::kEnableTime => engine_traits::PerfLevel::EnableTime,
        PerfLevel::kOutOfBounds => engine_traits::PerfLevel::OutOfBounds,
    }
}

pub fn cf_handle<'a>(db: &'a Db, cf: &str) -> Result<&'a RawCfHandle> {
    db.cf(cf).ok_or_else(|| {
        engine_traits::Error::Engine(engine_traits::Status::with_error(
            engine_traits::Code::InvalidArgument,
            format!("cf {} not found", cf),
        ))
    })
}

#[cfg(test)]
mod tests {
    use engine_traits::CF_DEFAULT;
    use tempfile::Builder;
    use tirocks::option::{ReadOptions, WriteOptions};

    use super::*;

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
        let temp = Builder::new()
            .prefix("_util_rocksdb_test_check_column_families")
            .tempdir()
            .unwrap();
        let path = temp.path();

        // create db when db not exist
        let mut cfs_opts = vec![(CF_DEFAULT, RocksCfOptions::default())];
        let build_cf_opt = || {
            let mut opts = RocksCfOptions::default();
            opts.set_level_compaction_dynamic_level_bytes(true);
            opts
        };
        cfs_opts.push(("cf_dynamic_level_bytes", build_cf_opt()));
        let db = new_engine_opt(path, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
        check_dynamic_level_bytes(&db);
        drop(db);

        // add cf1.
        let cfs_opts = vec![
            (CF_DEFAULT, build_cf_opt()),
            ("cf_dynamic_level_bytes", build_cf_opt()),
            ("cf1", build_cf_opt()),
        ];
        let db = new_engine_opt(path, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
        check_dynamic_level_bytes(&db);
        for name in &[CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"] {
            let handle = db.cf(name).unwrap();
            db.as_inner()
                .put(&WriteOptions::default(), handle, b"k", b"v")
                .unwrap();
        }
        drop(db);

        // change order should not cause data corruption.
        let cfs_opts = vec![
            ("cf_dynamic_level_bytes", build_cf_opt()),
            ("cf1", build_cf_opt()),
            (CF_DEFAULT, build_cf_opt()),
        ];
        let db = new_engine_opt(path, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
        check_dynamic_level_bytes(&db);
        let read_opt = ReadOptions::default();
        for name in &[CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"] {
            let handle = db.cf(name).unwrap();
            assert_eq!(
                db.as_inner().get(&read_opt, handle, b"k").unwrap().unwrap(),
                b"v"
            );
        }
        drop(db);

        // drop cf1.
        let cfs = vec![CF_DEFAULT, "cf_dynamic_level_bytes"];
        let db = new_engine(path, &cfs).unwrap();
        column_families_must_eq(path, cfs);
        check_dynamic_level_bytes(&db);
        drop(db);

        // drop all cfs.
        new_engine(path, &[CF_DEFAULT]).unwrap();
        column_families_must_eq(path, vec![CF_DEFAULT]);

        // not specifying default cf should error.
        new_engine(path, &[]).unwrap_err();
        column_families_must_eq(path, vec![CF_DEFAULT]);
    }

    fn column_families_must_eq(path: &Path, excepted: Vec<&str>) {
        let opts = RocksDbOptions::default();
        let cfs_list = Db::list_cfs(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.clone();
        cfs_existed.sort_unstable();
        cfs_excepted.sort_unstable();
        assert_eq!(cfs_existed, cfs_excepted);
    }

    fn check_dynamic_level_bytes(db: &RocksEngine) {
        let mut handle = db.cf(CF_DEFAULT).unwrap();
        let mut tmp_cf_opts = db.as_inner().cf_options(handle);
        assert!(
            !tmp_cf_opts
                .cf_options()
                .level_compaction_dynamic_level_bytes()
        );
        handle = db.cf("cf_dynamic_level_bytes").unwrap();
        tmp_cf_opts = db.as_inner().cf_options(handle);
        assert!(
            tmp_cf_opts
                .cf_options()
                .level_compaction_dynamic_level_bytes()
        );
    }
}
