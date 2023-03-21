// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Write, sync::Arc, thread, time::Duration};

use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{RocksEngine, RocksStatistics};
use engine_test::raft::RaftTestEngine;
use engine_traits::{TabletRegistry, CF_DEFAULT};
use file_system::IoRateLimiter;
use futures::Future;
use kvproto::{kvrpcpb::Context, metapb, raft_cmdpb::RaftCmdResponse};
use raftstore::Result;
use rand::RngCore;
use server::server2::ConfiguredRaftEngine;
use tempfile::TempDir;
use test_raftstore::{new_get_cmd, new_put_cf_cmd, new_request, Config};
use tikv::{
    server::KvEngineFactoryBuilder,
    storage::{
        config::EngineType,
        kv::{SnapContext, SnapshotExt},
        Engine, Snapshot,
    },
};
use tikv_util::{config::ReadableDuration, worker::LazyWorker, HandyRwLock};

use crate::{bootstrap_store, cluster::Cluster, ServerCluster, Simulator};

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    id: Option<(u64, u64)>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    TabletRegistry<RocksEngine>,
    RaftTestEngine,
    Option<Arc<DataKeyManager>>,
    TempDir,
    LazyWorker<String>,
    Arc<RocksStatistics>,
    Option<Arc<RocksStatistics>>,
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
    let cache = cfg
        .storage
        .block_cache
        .build_shared_cache(EngineType::RaftKv2);
    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), limiter)
        .unwrap();

    let sst_worker = LazyWorker::new("sst-recovery");
    let scheduler = sst_worker.scheduler();

    let (raft_engine, raft_statistics) = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    if let Some((cluster_id, store_id)) = id {
        assert_ne!(store_id, 0);
        bootstrap_store(&raft_engine, cluster_id, store_id).unwrap();
    }

    let builder =
        KvEngineFactoryBuilder::new(env, &cfg.tikv, cache).sst_recovery_sender(Some(scheduler));

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

pub fn configure_for_snapshot(config: &mut Config) {
    // Truncate the log quickly so that we can force sending snapshot.
    config.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    config.raft_store.raft_log_gc_count_limit = Some(2);
    config.raft_store.merge_max_log_gap = 1;
    config.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(50);
}

pub fn configure_for_lease_read_v2<T: Simulator>(
    cluster: &mut Cluster<T>,
    base_tick_ms: Option<u64>,
    election_ticks: Option<usize>,
) -> Duration {
    if let Some(base_tick_ms) = base_tick_ms {
        cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    }
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    if let Some(election_ticks) = election_ticks {
        cluster.cfg.raft_store.raft_election_timeout_ticks = election_ticks;
    }
    let election_ticks = cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let election_timeout = base_tick_interval * election_ticks;
    // Adjust max leader lease.
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to
    // make a valid config, that is election timeout x 2 < peer stale state
    // check < abnormal < max leader missing duration.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.cfg.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 5);

    election_timeout
}

pub fn wait_for_synced(cluster: &mut Cluster<ServerCluster>, node_id: u64, region_id: u64) {
    let mut storage = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&node_id)
        .unwrap()
        .clone();
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let snapshot = storage.snapshot(snap_ctx).unwrap();
    let txn_ext = snapshot.txn_ext.clone().unwrap();
    for retry in 0..10 {
        if txn_ext.is_max_ts_synced() {
            break;
        }
        thread::sleep(Duration::from_millis(1 << retry));
    }
    assert!(snapshot.ext().is_max_ts_synced());
}

// Issue a read request on the specified peer.
pub fn read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    timeout: Duration,
) -> Result<RaftCmdResponse> {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    cluster.read(None, request, timeout)
}

pub fn async_read_on_peer<T: Simulator>(
    cluster: &mut Cluster<T>,
    peer: metapb::Peer,
    region: metapb::Region,
    key: &[u8],
    read_quorum: bool,
    replica_read: bool,
) -> impl Future<Output = Result<RaftCmdResponse>> {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(key)],
        read_quorum,
    );
    request.mut_header().set_peer(peer);
    request.mut_header().set_replica_read(replica_read);
    cluster.sim.wl().async_read(request)
}
