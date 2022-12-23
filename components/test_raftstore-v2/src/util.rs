// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::{TabletFactory, TabletRegistry, CF_DEFAULT};
use file_system::IoRateLimiter;
use raftstore_v2::StoreRouter;
use rand::RngCore;
use server::server2::ConfiguredRaftEngine;
use tempfile::TempDir;
use test_raftstore::{new_put_cf_cmd, Config};
use tikv::server::KvEngineFactoryBuilder;
use tikv_util::config::ReadableDuration;

use crate::{cluster::Cluster, Simulator};

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    TabletRegistry<RocksEngine>,
    RaftTestEngine,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let mut cfg = cfg.clone();
    cfg.storage.data_dir = dir.path().to_str().unwrap().to_string();
    cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
    cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
    let key_manager =
        data_key_manager_from_config(&cfg.security.encryption, dir.path().to_str().unwrap())
            .unwrap()
            .map(Arc::new);

    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), limiter)
        .unwrap();
    let cache = cfg.storage.block_cache.build_shared_cache();
    let (raft_engine, _) = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    let mut builder = KvEngineFactoryBuilder::new(env, &cfg.tikv, cache);

    // todo(SpadeA): checkout if compaction event sender is not needed

    // let sst_worker = LazyWorker::new("sst-recovery");
    // let scheduler = sst_worker.scheduler();
    let factory = Box::new(builder.build());
    let reg = TabletRegistry::new(factory, dir.path().join("tablet")).unwrap();

    (reg, raft_engine, key_manager, dir)
}
