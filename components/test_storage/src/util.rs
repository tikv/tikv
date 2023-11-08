// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::KvFormat;
use kvproto::kvrpcpb::Context;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster, SimulateEngine};
use tikv_util::HandyRwLock;

use super::*;

#[macro_export]
macro_rules! prepare_raft_engine {
    ($cluster:expr, $key:expr) => {{
        $cluster.run();
        leader_raft_engine!($cluster, $key)
    }};
}

#[macro_export]
macro_rules! leader_raft_engine {
    ($cluster:expr, $key:expr) => {{
        // make sure leader has been elected.
        assert_eq!($cluster.must_get(b""), None);
        let region = $cluster.get_region($key.as_bytes());
        let leader = $cluster.leader_of_region(region.get_id()).unwrap();
        let engine = $cluster.sim.rl().storages[&leader.get_id()].clone();
        let mut ctx = Context::default();
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(leader);
        (engine, ctx)
    }};
}

#[macro_export]
macro_rules! follower_raft_engine {
    ($cluster:expr, $key:expr) => {{
        let mut ret = vec![];
        let region = $cluster.get_region($key.as_bytes());
        let leader = $cluster.leader_of_region(region.get_id()).unwrap();
        for peer in &region.peers {
            if peer.get_id() != leader.get_id() {
                let mut ctx = Context::default();
                ctx.set_stale_read(true);
                ctx.set_region_id(region.get_id());
                ctx.set_region_epoch(region.get_region_epoch().clone());
                ctx.set_peer(peer.clone());
                let engine = $cluster.sim.rl().storages[&peer.get_id()].clone();
                ret.push((engine, ctx));
            }
        }
        ret
    }};
}

pub fn new_raft_engine(
    count: usize,
    key: &str,
) -> (Cluster<ServerCluster>, SimulateEngine, Context) {
    let mut cluster = new_server_cluster(0, count);
    let (engine, ctx) = prepare_raft_engine!(cluster, key);
    (cluster, engine, ctx)
}

pub fn new_raft_storage_with_store_count<F: KvFormat>(
    count: usize,
    key: &str,
) -> (
    Cluster<ServerCluster>,
    SyncTestStorage<SimulateEngine, F>,
    Context,
) {
    let (cluster, engine, ctx) = new_raft_engine(count, key);
    (
        cluster,
        SyncTestStorageBuilder::from_engine(engine)
            .build(ctx.peer.as_ref().unwrap().store_id)
            .unwrap(),
        ctx,
    )
}
