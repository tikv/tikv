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
use std::time::Duration;

use kvproto::kvrpcpb::Context;

use test_raftstore::*;
use test_storage::*;
use tikv::storage::{self, Mutation};
use tikv::storage::{engine, mvcc, txn, Engine, Key};
use tikv::util::HandyRwLock;

fn new_raft_storage() -> (
    Cluster<ServerCluster>,
    SyncTestStorage<SimulateEngine>,
    Context,
) {
    new_raft_storage_with_store_count(1, "")
}

#[test]
fn test_raft_storage() {
    let (_cluster, storage, mut ctx) = new_raft_storage();
    let key = Key::from_raw(b"key");
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
            .scan(ctx.clone(), key.clone(), None, 1, false, 20)
            .is_err()
    );
    assert!(
        storage
            .scan_locks(ctx.clone(), 20, b"".to_vec(), 100)
            .is_err()
    );
}

#[test]
fn test_raft_storage_get_after_lease() {
    let (cluster, storage, ctx) = new_raft_storage();
    let key = b"key";
    let value = b"value";
    assert_eq!(
        storage
            .raw_get(ctx.clone(), "".to_string(), key.to_vec())
            .unwrap(),
        None
    );
    storage
        .raw_put(ctx.clone(), "".to_string(), key.to_vec(), value.to_vec())
        .unwrap();
    assert_eq!(
        storage
            .raw_get(ctx.clone(), "".to_string(), key.to_vec())
            .unwrap()
            .unwrap(),
        value.to_vec()
    );

    // Sleep until the leader lease is expired.
    thread::sleep(cluster.cfg.raft_store.raft_store_max_leader_lease.0);
    assert_eq!(
        storage
            .raw_get(ctx.clone(), "".to_string(), key.to_vec())
            .unwrap()
            .unwrap(),
        value.to_vec()
    );
}

#[test]
fn test_raft_storage_rollback_before_prewrite() {
    let (_cluster, storage, ctx) = new_raft_storage();
    let ret = storage.rollback(ctx.clone(), vec![Key::from_raw(b"key")], 10);
    assert!(ret.is_ok());
    let ret = storage.prewrite(
        ctx.clone(),
        vec![Mutation::Put((Key::from_raw(b"key"), b"value".to_vec()))],
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

    let key = Key::from_raw(b"key");
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
            .scan(ctx.clone(), key.clone(), None, 1, false, 20)
            .is_err()
    );
    assert!(
        storage
            .scan_locks(ctx.clone(), 20, b"".to_vec(), 100)
            .is_err()
    );
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
    engine
        .put(&ctx, Key::from_raw(b"a"), b"a".to_vec())
        .unwrap_err();
    // Term not match.
    cluster.must_transfer_leader(region.get_id(), peers[0].clone());
    let res = engine.put(&ctx, Key::from_raw(b"a"), b"a".to_vec());
    if let engine::Error::Request(ref e) = *res.as_ref().err().unwrap() {
        assert!(e.has_stale_command());
    } else {
        panic!("expect stale command, but got {:?}", res);
    }
}
