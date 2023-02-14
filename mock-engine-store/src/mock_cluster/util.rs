// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{sync::Arc, thread, time::Duration};

use encryption::DataKeyManager;
use engine_traits::{Engines, Peekable};
use file_system::IoRateLimiter;
use raftstore::store::RaftRouter;
use tempfile::TempDir;
use tikv_util::{debug, escape};

use crate::{Config, TiFlashEngine};

pub fn create_tiflash_test_engine(
    // ref init_tiflash_engines and create_test_engine
    // TODO: pass it in for all cases.
    _router: Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    Engines<TiFlashEngine, engine_rocks::RocksEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let key_manager = encryption_export::data_key_manager_from_config(
        &cfg.security.encryption,
        dir.path().to_str().unwrap(),
    )
    .unwrap()
    .map(Arc::new);

    let env = engine_rocks::get_env(key_manager.clone(), limiter).unwrap();

    let kv_path = dir.path().join(tikv::config::DEFAULT_ROCKSDB_SUB_DIR);
    let kv_path_str = kv_path.to_str().unwrap();

    let kv_db_opt = cfg.rocksdb.build_opt(
        &cfg.rocksdb.build_resources(env.clone()),
        cfg.storage.engine,
    );

    let cache = cfg.storage.block_cache.build_shared_cache();
    let raft_cfs_opt = cfg.raftdb.build_cf_opts(&cache);

    let kv_cfs_opt = cfg.rocksdb.build_cf_opts(
        &cfg.rocksdb.build_cf_resources(cache),
        None,
        cfg.storage.api_version(),
        cfg.storage.engine,
    );

    let engine = engine_rocks::util::new_engine_opt(kv_path_str, kv_db_opt, kv_cfs_opt).unwrap();
    let engine = TiFlashEngine::from_rocks(engine);

    let raft_path = dir.path().join("raft");
    let raft_path_str = raft_path.to_str().unwrap();

    let raft_db_opt = cfg.raftdb.build_opt(env.clone(), None);

    let raft_engine =
        engine_rocks::util::new_engine_opt(raft_path_str, raft_db_opt, raft_cfs_opt).unwrap();

    // FFI is not usable for now.
    let engines = Engines::new(engine, raft_engine);

    (engines, key_manager, dir)
}

pub fn must_get(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
        if let (Some(value), Some(res)) = (value, res.as_ref()) {
            assert_eq!(value, &res[..]);
            return;
        }
        if value.is_none() && res.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    debug!("last try to get {}", log_wrappers::hex_encode_upper(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {}",
        value.map(escape),
        log_wrappers::hex_encode_upper(key)
    )
}

pub fn must_get_equal(engine: &engine_rocks::RocksEngine, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none(engine: &engine_rocks::RocksEngine, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_cf_equal(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8], value: &[u8]) {
    must_get(engine, cf, key, Some(value));
}

pub fn must_get_cf_none(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8]) {
    must_get(engine, cf, key, None);
}
