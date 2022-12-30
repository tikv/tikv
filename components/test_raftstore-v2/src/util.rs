// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{RocksEngine, RocksStatistics};
use engine_test::raft::RaftTestEngine;
use engine_traits::{TabletFactory, TabletRegistry, CF_DEFAULT};
use file_system::IoRateLimiter;
use raftstore_v2::{StateStorage, StoreRouter};
use rand::RngCore;
use server::{fatal, server2::ConfiguredRaftEngine};
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{new_put_cf_cmd, Config};
use tikv::server::{KvEngineFactoryBuilder, NodeV2};
use tikv_util::{config::ReadableDuration, worker::LazyWorker};

use crate::{cluster::Cluster, Simulator};

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
    pd_client: &Arc<TestPdClient>,
) -> (
    TabletRegistry<RocksEngine>,
    RaftTestEngine,
    Option<Arc<DataKeyManager>>,
    TempDir,
    LazyWorker<String>,
    Arc<RocksStatistics>,
    Option<Arc<RocksStatistics>>,
    NodeV2<TestPdClient, RocksEngine, RaftTestEngine>,
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
    let cache = cfg.storage.block_cache.build_shared_cache();
    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), limiter)
        .unwrap();

    let sst_worker = LazyWorker::new("sst-recovery");
    let scheduler = sst_worker.scheduler();

    let (raft_engine, raft_statistics) = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    let builder =
        KvEngineFactoryBuilder::new(env, &cfg.tikv, cache).sst_recovery_sender(Some(scheduler));

    cfg.server.addr = format!("127.0.0.1:{}", test_util::alloc_port());
    println!("init addr {:?}", cfg.server.addr);
    let mut node = NodeV2::new(&cfg.server, pd_client.clone(), None);
    node.try_bootstrap_store(&cfg.raft_store, &raft_engine)
        .unwrap();
    assert_ne!(node.id(), 0);

    let router = node.router().clone();
    let builder = builder.state_storage(Arc::new(StateStorage::new(
        raft_engine.clone(),
        router.clone(),
    )));

    let factory = Box::new(builder.build());
    let rocks_statistics = factory.rocks_statistics();
    let reg = TabletRegistry::new(factory, dir.path().join("tablet")).unwrap();

    (
        reg,
        raft_engine,
        key_manager,
        dir,
        sst_worker,
        rocks_statistics,
        raft_statistics,
        node,
    )
}

/// Keep putting random kvs until specified size limit is reached.
pub fn put_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    put_cf_till_size(cluster, CF_DEFAULT, limit, range)
}

pub fn put_cf_till_size<T: Simulator>(
    cluster: &mut Cluster<T>,
    cf: &'static str,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    assert!(limit > 0);
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut key = String::new();
    let mut value = vec![0; 64];
    while len < limit {
        let batch_size = std::cmp::min(1024, limit - len);
        let mut reqs = vec![];
        for _ in 0..batch_size / 74 + 1 {
            key.clear();
            let key_id = range.next().unwrap();
            write!(key, "{:09}", key_id).unwrap();
            rng.fill_bytes(&mut value);
            // plus 1 for the extra encoding prefix
            len += key.len() as u64 + 1;
            len += value.len() as u64;
            reqs.push(new_put_cf_cmd(cf, key.as_bytes(), &value));
        }
        cluster.batch_put(key.as_bytes(), reqs).unwrap();
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        cluster.must_flush_cf(cf, true);
    }
    key.into_bytes()
}
