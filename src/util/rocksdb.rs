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
use rocksdb::rocksdb_ffi::DBCFHandle;

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a DBCFHandle, String> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found.", cf))
}

pub fn new_engine(path: &str, cfs: &[&str]) -> Result<DB, String> {
    let opts = Options::new();
    new_engine_opt(opts, path, cfs)
}

pub fn new_engine_opt(mut opts: Options, path: &str, cfs: &[&str]) -> Result<DB, String> {
    // TODO: configurable opts for each CF.
    // Currently we support 1) Create new db. 2) Open a db with CFs we want. 3) Open db with no
    // CF.
    // TODO: Support open db with incomplete CFs.
    opts.create_if_missing(false);
    let cf_opts: Vec<Options> = cfs.iter().map(|_| Options::new()).collect();
    let cf_ref_opts: Vec<&Options> = cf_opts.iter().collect();
    match DB::open_cf(&opts, path, cfs, &cf_ref_opts) {
        Ok(db) => return Ok(db),
        Err(e) => warn!("open rocksdb fail: {}", e),
    }

    opts.create_if_missing(true);
    let mut db = match DB::open(&opts, path) {
        Ok(db) => db,
        Err(e) => return Err(e),
    };
    for &cf in cfs {
        if cf == "default" {
            continue;
        }
        if let Err(e) = db.create_cf(cf, &opts) {
            return Err(e);
        }
    }
    Ok(db)
}