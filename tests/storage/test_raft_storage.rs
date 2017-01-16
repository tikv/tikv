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

use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread;
use tikv::util::HandyRwLock;
use tikv::storage::{self, Storage, Mutation, make_key, ALL_CFS, Options, Engine};
use tikv::storage::{txn, engine};
use kvproto::kvrpcpb::Context;
use raftstore::server::new_server_cluster_with_cfs;
use raftstore::cluster::Cluster;
use raftstore::server::ServerCluster;
use raftstore::util::*;
use storage::util;
use super::sync_storage::SyncStorage;
use super::util::new_raft_storage_with_store_count;

fn new_raft_storage() -> (Cluster<ServerCluster>, SyncStorage, Context) {
    new_raft_storage_with_store_count(1, "")
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

    // Test wrong region id.
    let region_id = ctx.get_region_id();
    ctx.set_region_id(region_id + 1);
    assert!(storage.get(ctx.clone(), &key, 20).is_err());
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(storage.scan(ctx.clone(), key.clone(), 1, false, 20).is_err());
    assert!(storage.scan_lock(ctx.clone(), 20).is_err());
}

#[test]
fn test_raft_storage_store_not_match() {
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

    // Test store not match.
    let mut peer = ctx.get_peer().clone();
    let store_id = peer.get_store_id();

    peer.set_store_id(store_id + 1);
    ctx.set_peer(peer);
    assert!(storage.get(ctx.clone(), &key, 20).is_err());
    let res = storage.get(ctx.clone(), &key, 20);
    if let storage::Error::Txn(txn::Error::Engine(engine::Error::Request(ref e))) = *res.as_ref()
        .err()
        .unwrap() {
        assert!(e.has_store_not_match());
    } else {
        panic!("expect store_not_match, but got {:?}", res);
    }
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(storage.scan(ctx.clone(), key.clone(), 1, false, 20).is_err());
    assert!(storage.scan_lock(ctx.clone(), 20).is_err());
}

#[test]
fn test_engine_leader_change_twice() {
    let mut cluster = new_server_cluster_with_cfs(0, 3, ALL_CFS);
    cluster.run();

    let region = cluster.get_region(b"");
    let peers = region.get_peers();

    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let engine = cluster.sim.rl().storages[&peers[0].get_id()].clone();

    let term = cluster.request(b"", vec![new_get_cmd(b"")], true, Duration::from_secs(5))
        .get_header()
        .get_current_term();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peers[0].clone());
    ctx.set_term(term);

    // Not leader.
    cluster.must_transfer_leader(region.get_id(), peers[1].clone());
    assert!(engine.write(&ctx, vec![]).is_err());
    // Term not match.
    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let res = engine.write(&ctx, vec![]);
    if let engine::Error::Request(ref e) = *res.as_ref().err().unwrap() {
        assert!(e.has_stale_command());
    } else {
        panic!("expect stale command, but got {:?}", res);
    }
}

#[test]
fn test_scheduler_leader_change_twice() {
    let mut cluster = new_server_cluster_with_cfs(0, 2, ALL_CFS);
    cluster.run();

    let region = cluster.get_region(b"");
    let peers = region.get_peers();

    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let engine = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let engine = util::BlockEngine::new(engine);
    let config = Default::default();
    let mut storage = Storage::from_engine(engine.clone(), &config).unwrap();
    storage.start(&config).unwrap();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peers[0].clone());

    let (tx, rx) = channel();
    engine.block_snapshot();
    storage.async_prewrite(ctx.clone(),
                        vec![Mutation::Put((make_key(b"k"), b"v".to_vec()))],
                        b"k".to_vec(),
                        10,
                        Options::default(),
                        box move |res: storage::Result<_>| {
            if let storage::Error::Engine(engine::Error::Request(ref e)) = *res.as_ref()
                .err()
                .unwrap() {
                assert!(e.has_stale_command());
            } else {
                panic!("expect stale command, but got {:?}", res);
            }
            tx.send(1).unwrap();
        })
        .unwrap();
    // Sleep a while, the prewrite should be blocked at snapshot stage.
    thread::sleep(Duration::from_millis(200));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    engine.unblock_snapshot();

    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}
