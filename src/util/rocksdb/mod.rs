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

use std::fs;
use std::path::Path;

use storage::CF_DEFAULT;
use rocksdb::{ColumnFamilyOptions, DBCompressionType, DBOptions, SliceTransform, DB};
use rocksdb::rocksdb::supported_compression;

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
    DB::open_cf(opts, path, cfs, cfs_opts)
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
        let mut db = try!(DB::open_cf(db_opt, path, cfs_v, cf_opts_v));
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            try!(db.create_cf(x.cf, x.options));
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = try!(DB::list_column_families(&db_opt, path));
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

        return DB::open_cf(db_opt, path, cfs_v, cfs_opts_v);
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
    let mut db = DB::open_cf(db_opt, path, cfs_v, cfs_opts_v).unwrap();

    // Drop discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            try!(db.drop_cf(cf));
        }
    }

    // Create needed column families not existed yet.
    for cf in cfs_diff(&needed, &existed) {
        try!(
            db.create_cf(
                cf,
                cfs_opts
                    .iter()
                    .find(|x| x.cf == cf)
                    .unwrap()
                    .options
                    .clone()
            )
        );
    }

    Ok(db)
}

pub fn new_engine_opt(path: &str, opts: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
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

#[cfg(test)]
mod tests {
    use rocksdb::{ColumnFamilyOptions, DBOptions, DB};
    use tempdir::TempDir;
    use storage::CF_DEFAULT;
    use super::{check_and_open, CFOptions};

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
}
