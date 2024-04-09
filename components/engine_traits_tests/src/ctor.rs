// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Constructor tests

use std::fs;

use encryption_export::data_key_manager_from_config;
use engine_test::{
    ctor::{CfOptions, DbOptions, KvEngineConstructorExt},
    kv::KvTestEngine,
};
use engine_traits::{KvEngine, Peekable, SyncMutable, ALL_CFS, CF_DEFAULT};

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

#[test]
fn new_engine_opt_renamed_dir() {
    use std::sync::Arc;
    let dir = tempdir();
    let root_path = dir.path();

    let encryption_cfg = test_util::new_file_security_config(root_path);
    let key_manager = Arc::new(
        data_key_manager_from_config(&encryption_cfg, root_path.to_str().unwrap())
            .unwrap()
            .unwrap(),
    );

    let mut db_opts = DbOptions::default();
    db_opts.set_key_manager(Some(key_manager.clone()));
    let cf_opts: Vec<_> = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();

    let path = root_path.join("missing").to_str().unwrap().to_owned();
    {
        let db = KvTestEngine::new_kv_engine_opt(&path, db_opts.clone(), cf_opts.clone()).unwrap();
        db.put(b"foo", b"bar").unwrap();
        db.sync().unwrap();
    }
    let new_path = root_path.join("new").to_str().unwrap().to_owned();
    key_manager.link_file(&path, &new_path).unwrap();
    fs::rename(&path, &new_path).unwrap();
    key_manager.delete_file(&path, Some(&new_path)).unwrap();
    {
        let db =
            KvTestEngine::new_kv_engine_opt(&new_path, db_opts.clone(), cf_opts.clone()).unwrap();
        assert_eq!(
            db.get_value_cf(CF_DEFAULT, b"foo").unwrap().unwrap(),
            b"bar"
        );
    }
}
