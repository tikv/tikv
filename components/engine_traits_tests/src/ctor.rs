// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Constructor tests

use super::tempdir;

use engine_traits::ALL_CFS;
use engine_test::kv::KvTestEngine;
use engine_test::ctor::{EngineConstructorExt, DBOptions, CFOptions, ColumnFamilyOptions};

#[test]
fn new_engine_basic() {
    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let _db = KvTestEngine::new_engine(path, None, ALL_CFS, None).unwrap();
}

#[test]
fn new_engine_opt_basic() {
    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let cf_opts = ALL_CFS.iter().map(|cf| {
        CFOptions::new(cf, ColumnFamilyOptions::new())
    }).collect();
    let _db = KvTestEngine::new_engine_opt(path, db_opts, cf_opts).unwrap();
}
