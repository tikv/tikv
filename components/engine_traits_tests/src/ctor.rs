// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Constructor tests

use std::fs;

use engine_test::{
    ctor::{CfOptions, DbOptions, KvEngineConstructorExt},
    kv::KvTestEngine,
};
use engine_traits::{KvEngine, SyncMutable, ALL_CFS};

use super::tempdir;

#[test]
fn new_engine_basic() {
    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let _db = KvTestEngine::new_kv_engine(path, ALL_CFS).unwrap();
}

#[test]
fn new_engine_opt_basic() {
    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let db_opts = DbOptions::default();
    let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
    let _db = KvTestEngine::new_kv_engine_opt(path, db_opts, cf_opts).unwrap();
}

// The database directory is created if it doesn't exist
#[test]
fn new_engine_missing_dir() {
    let dir = tempdir();
    let path = dir.path();
    let path = path.join("missing").to_str().unwrap().to_owned();
    let db = KvTestEngine::new_kv_engine(&path, ALL_CFS).unwrap();
    db.put(b"foo", b"bar").unwrap();
    db.sync().unwrap();
}

#[test]
fn new_engine_opt_missing_dir() {
    let dir = tempdir();
    let path = dir.path();
    let path = path.join("missing").to_str().unwrap().to_owned();
    let db_opts = DbOptions::default();
    let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
    let db = KvTestEngine::new_kv_engine_opt(&path, db_opts, cf_opts).unwrap();
    db.put(b"foo", b"bar").unwrap();
    db.sync().unwrap();
}

#[test]
fn new_engine_readonly_dir() {
    let dir = tempdir();
    let path = dir.path();
    let path = path.join("readonly");

    fs::create_dir(&path).unwrap();

    let meta = fs::metadata(&path).unwrap();
    let mut perms = meta.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&path, perms).unwrap();

    let path = path.to_str().unwrap();
    let err = KvTestEngine::new_kv_engine(path, ALL_CFS);

    err.unwrap_err();
}

#[test]
fn new_engine_opt_readonly_dir() {
    let dir = tempdir();
    let path = dir.path();
    let path = path.join("readonly");

    fs::create_dir(&path).unwrap();

    let meta = fs::metadata(&path).unwrap();
    let mut perms = meta.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&path, perms).unwrap();

    let path = path.to_str().unwrap();
    let db_opts = DbOptions::default();
    let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
    let err = KvTestEngine::new_kv_engine_opt(path, db_opts, cf_opts);

    err.unwrap_err();
}
