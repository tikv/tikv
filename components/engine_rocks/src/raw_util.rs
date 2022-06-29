// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Functions for constructing the rocksdb crate's `DB` type
//!
//! These are an artifact of refactoring the engine traits and will go away
//! eventually. Prefer to use the versions in the `util` module.

use std::{fs, path::Path, sync::Arc};

use engine_traits::{Result, CF_DEFAULT};
use rocksdb::{
    load_latest_options, CColumnFamilyDescriptor, ColumnFamilyOptions, DBOptions, Env, DB,
};
use tikv_util::warn;

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
    if let Some(cf_desc) = cf_descs
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
    let mut db = DB::open_cf(db_opt, path, cfds)?;

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
    let current_file_path = path.join("CURRENT");
    if !current_file_path.exists() || !current_file_path.is_file() {
        return false;
    }

    // If path is not an empty directory, and current file exists, we say db exists. If path is not an empty directory
    // but db has not been created, `DB::list_column_families` fails and we can clean up
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| !b.iter().any(|y| *x == y))
        .cloned()
        .collect()
}

pub fn to_raw_perf_level(level: engine_traits::PerfLevel) -> rocksdb::PerfLevel {
    match level {
        engine_traits::PerfLevel::Uninitialized => rocksdb::PerfLevel::Uninitialized,
        engine_traits::PerfLevel::Disable => rocksdb::PerfLevel::Disable,
        engine_traits::PerfLevel::EnableCount => rocksdb::PerfLevel::EnableCount,
        engine_traits::PerfLevel::EnableTimeExceptForMutex => {
            rocksdb::PerfLevel::EnableTimeExceptForMutex
        }
        engine_traits::PerfLevel::EnableTimeAndCPUTimeExceptForMutex => {
            rocksdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        engine_traits::PerfLevel::EnableTime => rocksdb::PerfLevel::EnableTime,
        engine_traits::PerfLevel::OutOfBounds => rocksdb::PerfLevel::OutOfBounds,
    }
}

pub fn from_raw_perf_level(level: rocksdb::PerfLevel) -> engine_traits::PerfLevel {
    match level {
        rocksdb::PerfLevel::Uninitialized => engine_traits::PerfLevel::Uninitialized,
        rocksdb::PerfLevel::Disable => engine_traits::PerfLevel::Disable,
        rocksdb::PerfLevel::EnableCount => engine_traits::PerfLevel::EnableCount,
        rocksdb::PerfLevel::EnableTimeExceptForMutex => {
            engine_traits::PerfLevel::EnableTimeExceptForMutex
        }
        rocksdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex => {
            engine_traits::PerfLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        rocksdb::PerfLevel::EnableTime => engine_traits::PerfLevel::EnableTime,
        rocksdb::PerfLevel::OutOfBounds => engine_traits::PerfLevel::OutOfBounds,
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::CF_DEFAULT;
    use rocksdb::{ColumnFamilyOptions, DBOptions, DB};
    use tempfile::Builder;

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
        cfs_existed.sort_unstable();
        cfs_excepted.sort_unstable();
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
}
