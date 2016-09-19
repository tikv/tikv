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
    open_opt(&opts, path, cfs, cfs_opts)
}

pub fn open_opt(opts: &Options,
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

fn drop_discared_cfs(path: &str, used_cfs: &[&str]) -> Result<(), String>{
    // List all column families in current db, if db not exist, it will return Err("IO error,
    // XXXX/CURRENT: No such file or directory").
    let mut opts = Options::new();
    opts.create_if_missing(false);
    let cfs_list = match DB::list_column_families(&opts, path) {
        Ok(cfs) => cfs,
        Err(e) => {
            if e.starts_with("IO error") && e.ends_with("No such file or directory") {
                return Ok(());
            }
            return Err(e);
        },
    };

    // Collect discarded column families.
    let mut all_cfs_set = HashSet::new();
    used_cfs.iter().map(|cf| all_cfs_set.insert(cf));
    let mut discarded_cfs = vec![];
    for ref cf in cfs_list {
        if !all_cfs_set.contains(&cf.as_str()) {
            discarded_cfs.push(cf.clone());
        }
    }
    if discarded_cfs.is_empty() {
        return Ok(());
    }

    // Drop column families discarded.
    let mut cfs_opts: Vec<Options> = vec![];
    for _ in 0..cfs_list.len() {
        cfs_opts.push(Options::new());
    }
    let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();
    let cfs_list_str: Vec<&str> = cfs_list.iter().map(|cf| cf.as_str()).collect();
    let mut db = DB::open_cf(&opts, path, &cfs_list_str, &cfs_ref_opts).unwrap();
    for cf in discarded_cfs {
        try!(db.drop_cf(cf));
    }

    Ok(())
}

pub fn new_engine_opt(mut opts: Options,
                      path: &str,
                      cfs: &[&str],
                      cfs_opts: Vec<Options>)
                      -> Result<DB, String> {
    // Currently we support 1) Create new db. 2) Open a db with CFs we want. 3) Open db with no
    // CF.
    // TODO: Support open db with incomplete CFs.
    try!(drop_discared_cfs(path, cfs));

    // opts is used as Rocksdb's DBOptions when call DB::open_cf
    opts.create_if_missing(false);
    let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();
    if let Ok(db) = DB::open_cf(&opts, path, cfs, &cfs_ref_opts) {
        return Ok(db);
    }

    // opts is used as Rocksdb's Options(include DBOptions and ColumnFamilyOptions)
    // when call DB::open
    opts.create_if_missing(true);
    let mut db = match DB::open(&opts, path) {
        Ok(db) => db,
        Err(e) => return Err(e),
    };
    for (&cf, &cf_opts) in cfs.iter().zip(&cfs_ref_opts) {
        if cf == "default" {
            continue;
        }
        if let Err(e) = db.create_cf(cf, cf_opts) {
            return Err(e);
        }
    }
    Ok(db)
}

#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};
    use tempdir::TempDir;
    use super::drop_discared_cfs;

    #[test]
    fn test_drop_discared_cfs() {
        // db not existed.
        let path = TempDir::new("_util_rocksdb_test_drop_discared_cfs").expect("");
        let path_str = path.path().to_str().unwrap();
        drop_discared_cfs(path_str, &["default"]).unwrap();

        // no discarded column families.
        {
            let mut opts = Options::new();
            opts.create_if_missing(true);
            let mut db = DB::open(&opts, path_str).unwrap();
            db.create_cf("cf1", &opts).unwrap();
        }
        drop_discared_cfs(path_str, &["default", "cf1"]).unwrap();

        // drop cf1.
        drop_discared_cfs(path_str, &["default"]).unwrap();
    }
}
