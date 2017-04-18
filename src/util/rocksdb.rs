// Copyright 2017 PingCAP, Inc.
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

use std::fs;
use std::path::Path;

use storage::CF_DEFAULT;
use rocksdb::{DB, Options, SliceTransform};

use super::collections::{HashSet, HashMap};

pub use rocksdb::CFHandle;

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
    let mut db_opts = Options::new();
    db_opts.enable_statistics();
    let mut cfs_opts = HashMap::default();
    for cf in cfs {
        cfs_opts.insert(*cf, Options::new());
    }
    new_engine_opt(path, db_opts, cfs_opts)
}

fn check_and_open(path: &str,
                  mut db_opt: Options,
                  cfs_opts: HashMap<&str, Options>)
                  -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs = vec![];
        let mut cfs_opts_ref = vec![];
        if let Some(opt) = cfs_opts.get(CF_DEFAULT) {
            cfs.push(CF_DEFAULT);
            cfs_opts_ref.push(opt);
        }
        let mut db = try!(DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice()));
        for (cf, cf_opt) in &cfs_opts {
            if *cf == "default" {
                continue;
            }
            try!(db.create_cf(*cf, cf_opt));
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = try!(DB::list_column_families(&db_opt, path));
    let existed: HashSet<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: HashSet<&str> = cfs_opts.keys().map(|v| *v).collect();

    // If all column families are exist, just open db.
    if existed == needed {
        let mut cfs = vec![];
        let mut cfs_opts_ref = vec![];
        for (cf, cf_opt) in &cfs_opts {
            cfs.push(*cf);
            cfs_opts_ref.push(cf_opt);
        }

        return DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice());
    }

    // Open db.
    let common_opt = Options::new();
    let mut cfs = vec![];
    let mut cfs_opts_ref = vec![];
    for cf in &existed {
        cfs.push(*cf);
        match cfs_opts.get(cf) {
            Some(opt) => {
                cfs_opts_ref.push(opt);
            }
            None => {
                cfs_opts_ref.push(&common_opt);
            }
        }
    }
    let mut db = DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice()).unwrap();

    // Drop discarded column families.
    for cf in existed.difference(&needed) {
        // Never drop default column families.
        if *cf != CF_DEFAULT {
            try!(db.drop_cf(cf));
        }
    }

    // Create needed column families not existed yet.
    for cf in needed.difference(&existed) {
        try!(db.create_cf(cf, cfs_opts.get(cf).unwrap()));
    }

    Ok(db)
}

pub fn new_engine_opt(path: &str,
                      opts: Options,
                      cfs_opts: HashMap<&str, Options>)
                      -> Result<DB, String> {
    check_and_open(path, opts, cfs_opts)
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

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len: suffix_len }
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
        FixedPrefixSliceTransform { prefix_len: prefix_len }
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

#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};
    use tempdir::TempDir;
    use util::HashMap;
    use super::check_and_open;

    #[test]
    fn test_check_and_open() {
        let path = TempDir::new("_util_rocksdb_test_check_column_families").expect("");
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let cfs_opts = map!["default" => Options::new()];
        check_and_open(path_str, Options::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, &["default"]);

        // add cf1.
        let cfs_opts = map!["default" => Options::new(), "cf1" => Options::new()];
        check_and_open(path_str, Options::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, &["default", "cf1"]);

        // drop cf1.
        let cfs_opts = map!["default" => Options::new()];
        check_and_open(path_str, Options::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, &["default"]);

        // never drop default cf
        let cfs_opts = HashMap::default();
        check_and_open(path_str, Options::new(), cfs_opts).unwrap();
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
