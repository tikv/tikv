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

use std::thread;
use std::sync::mpsc::channel;
use std::time::Duration;

use tikv::util::HandyRwLock;
use tikv::storage::{self, make_key, Engine, Mutation, Options, Storage};
use tikv::storage::{engine, mvcc, txn};
use tikv::storage::config::Config;
use kvproto::kvrpcpb::Context;
use raftstore::server::new_server_cluster;
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
    storage
        .prewrite(
            ctx.clone(),
            vec![Mutation::Put((key.clone(), b"value".to_vec()))],
            b"key".to_vec(),
            10,
        )
        .unwrap();
    storage
        .commit(ctx.clone(), vec![key.clone()], 10, 15)
        .unwrap();
    assert_eq!(
        storage.get(ctx.clone(), &key, 20).unwrap().unwrap(),
        b"value".to_vec()
    );

    // Test wrong region id.
    let region_id = ctx.get_region_id();
    ctx.set_region_id(region_id + 1);
    assert!(storage.get(ctx.clone(), &key, 20).is_err());
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(
        storage
            .scan(ctx.clone(), key.clone(), 1, false, 20)
            .is_err()
    );
    assert!(storage.scan_lock(ctx.clone(), 20).is_err());
}

#[test]
fn test_raft_storage_get_after_lease() {
    let (_cluster, storage, ctx) = new_raft_storage();
    let key = b"key";
    let value = b"value";
    assert_eq!(storage.raw_get(ctx.clone(), key.to_vec()).unwrap(), None);
    storage
        .raw_put(ctx.clone(), key.to_vec(), value.to_vec())
        .unwrap();
    assert_eq!(
        storage.raw_get(ctx.clone(), key.to_vec()).unwrap().unwrap(),
        value.to_vec()
    );

    // Sleep until the leader lease is expired.
    thread::sleep(Duration::from_millis(MAX_LEADER_LEASE));
    assert_eq!(
        storage.raw_get(ctx.clone(), key.to_vec()).unwrap().unwrap(),
        value.to_vec()
    );
}

#[test]
fn test_raft_storage_rollback_before_prewrite() {
    let (_cluster, storage, ctx) = new_raft_storage();
    let ret = storage.rollback(ctx.clone(), vec![make_key(b"key")], 10);
    assert!(ret.is_ok());
    let ret = storage.prewrite(
        ctx.clone(),
        vec![Mutation::Put((make_key(b"key"), b"value".to_vec()))],
        b"key".to_vec(),
        10,
    );
    assert!(ret.is_err());
    let err = ret.unwrap_err();
    match err {
        storage::Error::Txn(txn::Error::Mvcc(mvcc::Error::WriteConflict { .. })) => {}
        _ => {
            panic!("expect WriteConflict error, but got {:?}", err);
        }
    }
}

#[test]
fn test_raft_storage_store_not_match() {
    let (_cluster, storage, mut ctx) = new_raft_storage();

    let key = make_key(b"key");
    assert_eq!(storage.get(ctx.clone(), &key, 5).unwrap(), None);
    storage
        .prewrite(
            ctx.clone(),
            vec![Mutation::Put((key.clone(), b"value".to_vec()))],
            b"key".to_vec(),
            10,
        )
        .unwrap();
    storage
        .commit(ctx.clone(), vec![key.clone()], 10, 15)
        .unwrap();
    assert_eq!(
        storage.get(ctx.clone(), &key, 20).unwrap().unwrap(),
        b"value".to_vec()
    );

    // Test store not match.
    let mut peer = ctx.get_peer().clone();
    let store_id = peer.get_store_id();

    peer.set_store_id(store_id + 1);
    ctx.set_peer(peer);
    assert!(storage.get(ctx.clone(), &key, 20).is_err());
    let res = storage.get(ctx.clone(), &key, 20);
    if let storage::Error::Txn(txn::Error::Engine(engine::Error::Request(ref e))) =
        *res.as_ref().err().unwrap()
    {
        assert!(e.has_store_not_match());
    } else {
        panic!("expect store_not_match, but got {:?}", res);
    }
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(
        storage
            .scan(ctx.clone(), key.clone(), 1, false, 20)
            .is_err()
    );
    assert!(storage.scan_lock(ctx.clone(), 20).is_err());
}

#[test]
fn test_engine_leader_change_twice() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    let region = cluster.get_region(b"");
    let peers = region.get_peers();

    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let engine = cluster.sim.rl().storages[&peers[0].get_id()].clone();

    let term = cluster
        .request(b"", vec![new_get_cmd(b"")], true, Duration::from_secs(5))
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
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let config = Config::default();

    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let mut engine0 = util::BlockEngine::new(engine0);
    let mut storage0 = Storage::from_engine(engine0.clone(), &config).unwrap();
    storage0.start(&config).unwrap();

    let mut ctx0 = Context::new();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    let (stx, srx) = channel();
    engine0.block_snapshot(stx.clone());
    storage0.async_prewrite(ctx0,
                        vec![Mutation::Put((make_key(b"k"), b"v".to_vec()))],
                        b"k".to_vec(),
                        10,
                        Options::default(),
                        box move |res: storage::Result<_>| {
            match res {
                Err(storage::Error::Engine(engine::Error::Request(ref e))) => {
                    assert!(e.has_stale_command());
                    prewrite_tx.send(false).unwrap();
                }
                Ok(_) => {
                    prewrite_tx.send(true).unwrap();
                }
                _ => {
                    panic!("expect stale command, but got {:?}", res);
                }
            }
        })
        .unwrap();
    // wait for the message, the prewrite should be blocked at snapshot stage.
    srx.recv_timeout(Duration::from_secs(2)).unwrap();
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    engine0.unblock_snapshot();

    // the snapshot request may meet read index, scheduler will retry the request.
    let ok = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    if ok {
        let region1 = cluster.get_region(b"");
        cluster.must_transfer_leader(region1.get_id(), peers[1].clone());

        let engine1 = cluster.sim.rl().storages[&peers[1].get_id()].clone();
        let mut storage1 = Storage::from_engine(engine1, &config).unwrap();
        storage1.start(&config).unwrap();
        let mut ctx1 = Context::new();
        ctx1.set_region_id(region1.get_id());
        ctx1.set_region_epoch(region1.get_region_epoch().clone());
        ctx1.set_peer(peers[1].clone());

        let (commit_tx, commit_rx) = channel();
        storage1
            .async_commit(
                ctx1,
                vec![make_key(b"k")],
                10,
                11,
                box move |res: storage::Result<_>| { commit_tx.send(res).unwrap(); },
            )
            .unwrap();
        // wait for the commit result.
        let res = commit_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if res.as_ref().is_err() {
            panic!("expect Ok(_), but got {:?}", res);
        }
    }
}
