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

use rocksdb::{DB, Options};
pub use rocksdb::CFHandle;
use std::collections::HashSet;
use std::path::Path;
use std::fs;
use storage::CF_DEFAULT;
use regex::Regex;

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle, String> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found.", cf))
}

pub fn open(path: &str, cfs: &[&str]) -> Result<DB, String> {
    let mut opts = Options::new();
    opts.create_if_missing(false);
    let mut cfs_opts = vec![];
    for _ in 0..cfs.len() {
        cfs_opts.push(Options::new());
    }
    open_opt(opts, path, cfs, cfs_opts)
}

pub fn open_opt(opts: Options,
                path: &str,
                cfs: &[&str],
                cfs_opts: Vec<Options>)
                -> Result<DB, String> {
    let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();
    DB::open_cf(opts, path, cfs, &cfs_ref_opts)
}

pub fn new_engine(path: &str, cfs: &[&str]) -> Result<DB, String> {
    let opts = Options::new();
    let mut cfs_opts = vec![];
    for _ in 0..cfs.len() {
        cfs_opts.push(Options::new());
    }
    new_engine_opt(opts, path, cfs, cfs_opts)
}

fn check_column_families(path: &str,
                         needed_cfs: &[&str],
                         cfs_opts: &[&Options])
                         -> Result<(), String> {
    // If db not exist, create it.
    if !db_exist(path) {
        // opts is used as Rocksdb's Options(include DBOptions and ColumnFamilyOptions)
        // when call DB::open.
        let mut opts = Options::new();
        opts.create_if_missing(true);
        let mut db = try!(DB::open(opts, path));
        for (&cf, &cf_opts) in needed_cfs.iter().zip(cfs_opts) {
            if cf == "default" {
                continue;
            }
            try!(db.create_cf(cf, cf_opts));
        }

        return Ok(());
    }

    // List all column families in current db.
    let mut opts = Options::new();
    opts.create_if_missing(false);
    let cfs_list = try!(DB::list_column_families(&opts, path));
    let existed: HashSet<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: HashSet<&str> = needed_cfs.iter().map(|v| *v).collect();
    if existed == needed {
        return Ok(());
    }

    // Open db.
    let mut cfs_opts: Vec<Options> = vec![];
    for _ in 0..cfs_list.len() {
        cfs_opts.push(Options::new());
    }
    let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();
    let cfs_list_str: Vec<&str> = cfs_list.iter().map(|cf| cf.as_str()).collect();
    let mut opts = Options::new();
    opts.create_if_missing(false);
    let mut db = DB::open_cf(opts, path, &cfs_list_str, &cfs_ref_opts).unwrap();

    // Drop discarded column families.
    for cf in existed.difference(&needed) {
        // Never drop default column families.
        if *cf != CF_DEFAULT {
            try!(db.drop_cf(cf));
        }
    }

    // Create needed column families not existed yet.
    let mut opts = Options::new();
    opts.create_if_missing(false);
    for cf in needed.difference(&existed) {
        try!(db.create_cf(cf, &opts));
    }

    Ok(())
}

pub fn new_engine_opt(mut opts: Options,
                      path: &str,
                      cfs: &[&str],
                      cfs_opts: Vec<Options>)
                      -> Result<DB, String> {
    // Drop discarded column families and create new needed column families not exist yet.
    // If db not exist check_column_families will create it.
    let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();
    try!(check_column_families(path, cfs, &cfs_ref_opts));

    // opts is used as Rocksdb's DBOptions when call DB::open_cf
    opts.create_if_missing(false);
    DB::open_cf(opts, path, cfs, &cfs_ref_opts)
}

fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

pub struct Statistics {
    pub block_cache_miss: u64,
    pub block_cache_hit: u64,

    pub stall_micros: u64,
    pub get_micros: u64,
    pub write_micros: u64,

    pub seek_micros_per_50: u64,
    pub seek_micros_per_95: u64,
    pub seek_micros_per_99: u64,

    pub compaction_micros: u64,
}

impl Statistics {
    pub fn new() {
        Statistics {
            block_cache_miss: 0,
            block_cache_hit: 0,
        }
    }
}

pub fn get_statistics(db: &DB) -> Option<Statistics> {
    let mut stat = Statistics::new();

    if let Some(info) = db.get_statistics() {
        let re = Regex::new("rocksdb\.block.cache\.miss[ ]+COUNT[ ]+:[ ]+([0-9]+)").unwrap();
        let cap = re.captrues(info).unwrap();
        stat.block_cache_miss = (cap[1].unwrap()).parse::<u64>();

        let re = Regex::new("rocksdb\.block\.cache\.hit[ ]+COUNT[ ]+:[ ]+([0-9]+)").unwrap();
        let cap = re.captrues(info).unwrap();
        stat.block_cache_hit = (cap[1].unwrap()).parse::<u64>();

        let re = Regex::new("rocksdb\.stall\.micros[ ]+COUNT[ ]+:[ ]+([0-9]+)").unwrap();
        let cap = re.captrues(info).unwrap();
        stat.stall_micros = (cap[1].unwrap()).parse::<u64>();

        let re =
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};
    use tempdir::TempDir;
    use super::check_column_families;

    #[test]
    fn test_check_column_families() {
        let path = TempDir::new("_util_rocksdb_test_check_column_families").expect("");
        let path_str = path.path().to_str().unwrap();
        let cfs_opts = vec![Options::new(), Options::new()];
        let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();

        // create db when db not exist
        check_column_families(path_str, &["default"], &cfs_ref_opts[..1]).unwrap();
        column_families_must_eq(path_str, &["default"]);

        // add cf1.
        check_column_families(path_str, &["default", "cf1"], &cfs_ref_opts).unwrap();
        column_families_must_eq(path_str, &["default", "cf1"]);

        // drop cf1.
        check_column_families(path_str, &["default"], &cfs_ref_opts[..1]).unwrap();
        column_families_must_eq(path_str, &["default"]);

        // never drop default cf
        check_column_families(path_str, &[], &[]).unwrap();
        column_families_must_eq(path_str, &["default"]);
    }

    fn column_families_must_eq(path: &str, excepted: &[&str]) {
        let opts = Options::new();
        let cfs_list = DB::list_column_families(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.iter().map(|v| *v).collect();
        cfs_existed.sort();
        cfs_excepted.sort();
        assert_eq!(cfs_existed, cfs_excepted);
    }
}
