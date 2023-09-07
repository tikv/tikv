// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! Checkpoint tests

use std::sync::Arc;

use encryption_export::{data_key_manager_from_config, trash_dir_all};
use engine_test::{
    ctor::{CfOptions, DbOptions, KvEngineConstructorExt},
    kv::KvTestEngine,
};
use engine_traits::{
    Checkpointable, Checkpointer, KvEngine, Peekable, SyncMutable, ALL_CFS, CF_DEFAULT,
};

use super::tempdir;

#[test]
fn test_encrypted_checkpoint() {
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

    let path1 = root_path.join("1").to_str().unwrap().to_owned();
    let db1 = KvTestEngine::new_kv_engine_opt(&path1, db_opts.clone(), cf_opts.clone()).unwrap();
    db1.put(b"foo", b"bar").unwrap();
    db1.sync().unwrap();

    let path2 = root_path.join("2");
    let mut checkpointer = db1.new_checkpointer().unwrap();
    checkpointer.create_at(&path2, None, 0).unwrap();
    let db2 =
        KvTestEngine::new_kv_engine_opt(path2.to_str().unwrap(), db_opts.clone(), cf_opts.clone())
            .unwrap();
    assert_eq!(
        db2.get_value_cf(CF_DEFAULT, b"foo").unwrap().unwrap(),
        b"bar"
    );
    drop(db1);
    drop(db2);
    // Match KvEngineFactory::destroy_tablet.
    trash_dir_all(path1, Some(&key_manager)).unwrap();
    trash_dir_all(path2, Some(&key_manager)).unwrap();
    assert_eq!(key_manager.file_count(), 0);
}
