// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::thread;
use std::time::Duration;

use kvproto::kvrpcpb::Context;
use std::sync::mpsc::channel;
use std::sync::Arc;
use test_raftstore::*;
use test_storage::*;
use tikv::server::gc_worker::{AutoGcConfig, GcConfig};
use tikv::storage::kv::{Engine, Error as KvError, ErrorInner as KvErrorInner};
use tikv::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use tikv::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use tikv::storage::{Error as StorageError, ErrorInner as StorageErrorInner};
use tikv_util::collections::HashMap;
use tikv_util::HandyRwLock;
use txn_types::{Key, Mutation, TimeStamp};

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
    assert!(storage
        .scan(ctx.clone(), key.clone(), None, 1, false, 20)
        .is_err());
    assert!(storage
        .scan_locks(ctx.clone(), 20, b"".to_vec(), 100)
        .is_err());
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
        StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::WriteConflict { .. },
        ))))) => {}
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
    if let StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Engine(KvError(
        box KvErrorInner::Request(ref e),
    ))))) = *res.as_ref().err().unwrap()
    {
        assert!(e.has_store_not_match());
    } else {
        panic!("expect store_not_match, but got {:?}", res);
    }
    assert!(storage.batch_get(ctx.clone(), &[key.clone()], 20).is_err());
    assert!(storage
        .scan(ctx.clone(), key.clone(), None, 1, false, 20)
        .is_err());
    assert!(storage
        .scan_locks(ctx.clone(), 20, b"".to_vec(), 100)
        .is_err());
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

    let mut ctx = Context::default();
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
    if let KvError(box KvErrorInner::Request(ref e)) = *res.as_ref().err().unwrap() {
        assert!(e.has_stale_command());
    } else {
        panic!("expect stale command, but got {:?}", res);
    }
}

fn write_test_data<E: Engine>(
    storage: &SyncTestStorage<E>,
    ctx: &Context,
    data: &[(Vec<u8>, Vec<u8>)],
    ts: impl Into<TimeStamp>,
) {
    let mut ts = ts.into();
    for (k, v) in data {
        storage
            .prewrite(
                ctx.clone(),
                vec![Mutation::Put((Key::from_raw(k), v.to_vec()))],
                k.to_vec(),
                ts,
            )
            .unwrap()
            .into_iter()
            .for_each(|res| res.unwrap());
        storage
            .commit(ctx.clone(), vec![Key::from_raw(k)], ts, ts.next())
            .unwrap();
        ts.incr().incr();
    }
}

fn check_data<E: Engine>(
    cluster: &mut Cluster<ServerCluster>,
    storages: &HashMap<u64, SyncTestStorage<E>>,
    test_data: &[(Vec<u8>, Vec<u8>)],
    ts: impl Into<TimeStamp>,
    expect_success: bool,
) {
    let ts = ts.into();
    for (k, v) in test_data {
        let mut region = cluster.get_region(k);
        let leader = cluster.leader_of_region(region.get_id()).unwrap();
        let leader_id = leader.get_store_id();
        let mut ctx = Context::default();
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.take_region_epoch());
        ctx.set_peer(leader);

        let value = storages[&leader_id]
            .get(ctx, &Key::from_raw(k), ts)
            .unwrap();
        if expect_success {
            assert_eq!(value.unwrap().as_slice(), v.as_slice());
        } else {
            assert!(value.is_none());
        }
    }
}

#[test]
fn test_auto_gc() {
    let count = 3;
    let (mut cluster, first_leader_storage, ctx) = new_raft_storage_with_store_count(count, "");
    let pd_client = Arc::clone(&cluster.pd_client);

    // Used to wait for all storage's GC to finish
    let (finish_signal_tx, finish_signal_rx) = channel();

    // Create storage object for each store in the cluster
    let mut storages: HashMap<_, _> = cluster
        .sim
        .rl()
        .storages
        .iter()
        .map(|(id, engine)| {
            let mut config = GcConfig::default();
            // Do not skip GC
            config.ratio_threshold = 0.9;
            let storage = SyncTestStorageBuilder::from_engine(engine.clone())
                .gc_config(config)
                .build()
                .unwrap();

            (*id, storage)
        })
        .collect();

    let mut region_info_accessors = cluster.sim.rl().region_info_accessors.clone();

    for (id, storage) in &mut storages {
        let tx = finish_signal_tx.clone();

        let mut cfg = AutoGcConfig::new_test_cfg(
            Arc::clone(&pd_client),
            region_info_accessors.remove(id).unwrap(),
            *id,
        );
        cfg.post_a_round_of_gc = Some(Box::new(move || tx.send(()).unwrap()));
        storage.start_auto_gc(cfg);
    }

    assert_eq!(storages.len(), count);

    // test_data will be wrote with ts < 50
    let test_data: Vec<_> = [
        (b"k1", b"v1"),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
        (b"k4", b"v4"),
        (b"k5", b"v5"),
        (b"k6", b"v6"),
        (b"k7", b"v7"),
        (b"k8", b"v8"),
        (b"k9", b"v9"),
    ]
    .iter()
    .map(|(k, v)| (k.to_vec(), v.to_vec()))
    .collect();

    let test_data2: Vec<_> = test_data
        .iter()
        .map(|(k, v)| {
            let mut v = v.to_vec();
            v.push(b'1');
            (k.to_vec(), v)
        })
        .collect();

    let test_data3: Vec<_> = test_data
        .iter()
        .map(|(k, v)| {
            let mut v = v.to_vec();
            v.push(b'2');
            (k.to_vec(), v)
        })
        .collect();

    write_test_data(&first_leader_storage, &ctx, &test_data, 10);
    write_test_data(&first_leader_storage, &ctx, &test_data2, 100);
    write_test_data(&first_leader_storage, &ctx, &test_data3, 200);

    let split_keys: &[&[u8]] = &[b"k2", b"k4", b"k6", b"k8"];

    for k in split_keys {
        let region = cluster.get_region(*k);
        cluster.must_split(&region, *k);
    }

    check_data(&mut cluster, &storages, &test_data, 50, true);
    check_data(&mut cluster, &storages, &test_data2, 150, true);
    check_data(&mut cluster, &storages, &test_data3, 250, true);

    pd_client.set_gc_safe_point(150);

    for _ in 0..count {
        finish_signal_rx.recv().unwrap();
    }

    check_data(&mut cluster, &storages, &test_data, 50, false);
    check_data(&mut cluster, &storages, &test_data2, 150, true);
    check_data(&mut cluster, &storages, &test_data3, 250, true);

    // No more signals.
    finish_signal_rx
        .recv_timeout(Duration::from_millis(300))
        .unwrap_err();
}
