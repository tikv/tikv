// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use tikv::util::HandyRwLock;
use tikv::storage::{Mutation, make_key, ALL_CFS};
use kvproto::kvrpcpb::Context;
use raftstore::server::new_server_cluster_with_cfs;
use raftstore::cluster::Cluster;
use raftstore::server::ServerCluster;
use super::sync_storage::SyncStorage;

fn new_raft_storage() -> (Cluster<ServerCluster>, SyncStorage, Context) {
    let mut cluster = new_server_cluster_with_cfs(0, 1, ALL_CFS);
    cluster.run();
    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b""), None);

    let region = cluster.get_region(b"");
    let leader_id = cluster.leader_of_region(region.get_id()).unwrap();
    let engine = cluster.sim.rl().storages[&leader_id.get_id()].clone();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());

    (cluster, SyncStorage::from_engine(engine, &Default::default()), ctx)
}

#[test]
fn test_raft_storage() {
    let (_cluster, storage, mut ctx) = new_raft_storage();
    let key = make_key(b"key");
    assert_eq!(storage.get(ctx.clone(), &key, 5).unwrap(), None);
    storage.prewrite(ctx.clone(),
                  vec![Mutation::Put((key.clone(), b"value".to_vec()))],
                  b"key".to_vec(),
                  10)
        .unwrap();
    storage.commit(ctx.clone(), vec![key.clone()], 10, 15).unwrap();
    assert_eq!(storage.get(ctx.clone(), &key, 20).unwrap().unwrap(),
               b"value".to_vec());

    // Test wronng region id.
    let region_id = ctx.get_region_id();
    ctx.set_region_id(region_id + 1);
    assert!(storage.get(ctx.clone(), &key, 20).is_err());
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(storage.scan(ctx.clone(), key.clone(), 1, false, 20).is_err());
    assert!(storage.scan_lock(ctx.clone(), 20).is_err());
}

#[test]
fn test_write_leader_change_twice() {
    let mut cluster = new_server_cluster_with_cfs(0, 3, ALL_CFS);
    cluster.run();

    let region = cluster.get_region(b"");
    let peers = region.get_peers();

    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let engine = cluster.sim.rl().storages[&peers[0].get_id()].clone();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peers[0].clone());

    let snapshot = engine.snapshot(&ctx).unwrap();
    let ctx = snapshot.context();
    // Not leader.
    cluster.must_transfer_leader(region.get_id(), peers[1].clone());
    assert!(engine.write(&ctx, vec![]).is_err());
    // Term not match.
    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    assert!(engine.write(&ctx, vec![]).is_err());
}
