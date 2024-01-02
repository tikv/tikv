// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use encryption::DataKeyManager;
use engine_store_ffi::TiFlashEngine;
use engine_traits::Engines;
use file_system::IoRateLimiter;
use raftstore::store::RaftRouter;
use tempfile::TempDir;

use super::{common::*, Cluster, Simulator};

pub fn create_tiflash_test_engine_with_cluster_ctx<T: Simulator<TiFlashEngine>>(
    cluster: &mut Cluster<T>,
    router: Option<RaftRouter<TiFlashEngine, ProxyRaftEngine>>,
) -> (
    Engines<TiFlashEngine, ProxyRaftEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let (engines, key_manager, dir) = create_tiflash_test_engine(
        router.clone(),
        cluster.io_rate_limiter.clone(),
        &cluster.cfg,
    );

    // Set up FFI.
    let cluster_ptr = cluster as *const _ as isize;
    let cluster_ext_ptr = &cluster.cluster_ext as *const _ as isize;
    ClusterExt::create_ffi_helper_set(
        &mut cluster.cluster_ext,
        cluster_ptr,
        cluster_ext_ptr,
        &cluster.cfg.proxy_cfg,
        &cluster.cfg.tikv,
        cluster.cfg.mock_cfg.clone(),
        engines,
        &key_manager,
        &router,
        Some(cluster.pd_client.clone()),
    );
    let ffi_helper_set = cluster.cluster_ext.ffi_helper_lst.last_mut().unwrap();
    let engines = ffi_helper_set
        .engine_store_server
        .engines
        .as_ref()
        .unwrap()
        .clone();

    (engines, key_manager, dir)
}

pub fn create_tiflash_test_engine(
    // ref init_tiflash_engines and create_test_engine
    // TODO: pass it in for all cases.
    _router: Option<RaftRouter<TiFlashEngine, ProxyRaftEngine>>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &MixedClusterConfig,
) -> (
    Engines<TiFlashEngine, ProxyRaftEngine>,
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
        &cfg.rocksdb.build_resources(env.clone(), cfg.storage.engine),
        cfg.storage.engine,
    );

    let cache = cfg.storage.block_cache.build_shared_cache();

    #[allow(unused_variables)]
    let raft_cfs_opt = cfg.raftdb.build_cf_opts(&cache);
    let kv_cfs_opt = cfg.rocksdb.build_cf_opts(
        &cfg.rocksdb.build_cf_resources(cache),
        None,
        cfg.storage.api_version(),
        None,
        cfg.storage.engine,
    );

    let engine = engine_rocks::util::new_engine_opt(kv_path_str, kv_db_opt, kv_cfs_opt).unwrap();
    let engine = TiFlashEngine::from_rocks(engine);

    let raft_path = dir.path().join("raft");
    let raft_path_str = raft_path.to_str().unwrap();

    #[cfg(not(feature = "test-engine-raft-raft-engine"))]
    {
        tikv_util::debug!("Using Rocksdb for RaftEngine");
        let raft_db_opt = cfg.raftdb.build_opt(env.clone(), None);
        let raft_engine =
            engine_rocks::util::new_engine_opt(raft_path_str, raft_db_opt, raft_cfs_opt).unwrap();
        // FFI is not usable for now.
        let engines = Engines::new(engine, raft_engine);
        (engines, key_manager, dir)
    }
    #[cfg(feature = "test-engine-raft-raft-engine")]
    {
        tikv_util::debug!("Using RaftLogEngine for RaftEngine");
        let mut raft_log_engine_cfg = cfg.raft_engine.config();
        raft_log_engine_cfg.dir = String::from(raft_path_str);
        let raft_engine =
            raft_log_engine::RaftLogEngine::new(raft_log_engine_cfg, key_manager.clone(), None)
                .unwrap();
        // FFI is not usable for now.
        let engines = Engines::new(engine, raft_engine);
        (engines, key_manager, dir)
    }
}
