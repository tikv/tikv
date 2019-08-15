// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc::channel, Arc};
use std::thread;
use std::time::Duration;

use fail;
use grpcio::*;
use kvproto::kvrpcpb::{self, Context, Op, PrewriteRequest, RawPutRequest};
use kvproto::tikvpb::TikvClient;

use test_raftstore::{must_get_equal, must_get_none, new_server_cluster};
use test_storage::new_raft_engine;
use tikv::storage;
use tikv::storage::gc_worker::GC_MAX_PENDING_TASKS;
use tikv::storage::*;
use tikv_util::HandyRwLock;

#[test]
fn test_storage_gcworker_busy() {
    let _guard = crate::setup();
    let snapshot_fp = "raftkv_async_snapshot";
    let (_cluster, engine, ctx) = new_raft_engine(3, "");
    let storage = TestStorageBuilder::from_engine(engine.clone())
        .build()
        .unwrap();
    fail::cfg(snapshot_fp, "pause").unwrap();
    let (tx1, rx1) = channel();
    // Schedule `GC_MAX_PENDING` GC requests.
    for _i in 0..GC_MAX_PENDING_TASKS {
        let tx1 = tx1.clone();
        storage
            .async_gc(
                ctx.clone(),
                1,
                Box::new(move |res: storage::Result<()>| {
                    assert!(res.is_ok());
                    tx1.send(1).unwrap();
                }),
            )
            .unwrap();
    }
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Schedule one more request. So that there is a request being processed and
    // `GC_MAX_PENDING` requests in queue.
    storage
        .async_gc(
            ctx.clone(),
            1,
            Box::new(move |res: storage::Result<()>| {
                assert!(res.is_ok());
                tx1.send(1).unwrap();
            }),
        )
        .unwrap();

    // Old GC commands are blocked, the new one will get GCWorkerTooBusy error.
    let (tx2, rx2) = channel();
    storage
        .async_gc(
            Context::default(),
            1,
            Box::new(move |res: storage::Result<()>| {
                match res {
                    Err(storage::Error::GCWorkerTooBusy) => {}
                    res => panic!("expect too busy, got {:?}", res),
                }
                tx2.send(1).unwrap();
            }),
        )
        .unwrap();

    rx2.recv().unwrap();
    fail::remove(snapshot_fp);
    for _ in 0..=GC_MAX_PENDING_TASKS {
        rx1.recv().unwrap();
    }
}

#[test]
fn test_scheduler_leader_change_twice() {
    let _guard = crate::setup();
    let snapshot_fp = "scheduler_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let storage0 = TestStorageBuilder::from_engine(engine0.clone())
        .build()
        .unwrap();

    let mut ctx0 = Context::default();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::cfg(snapshot_fp, "pause").unwrap();
    storage0
        .async_prewrite(
            ctx0,
            vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
            b"k".to_vec(),
            10,
            Options::default(),
            Box::new(move |res: storage::Result<_>| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    fail::remove(snapshot_fp);

    match prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap() {
        Err(storage::Error::Txn(txn::Error::Engine(kv::Error::Request(ref e))))
        | Err(storage::Error::Engine(kv::Error::Request(ref e))) => {
            assert!(e.has_stale_command(), "{:?}", e);
        }
        res => {
            panic!("expect stale command, but got {:?}", res);
        }
    }
}

#[test]
fn test_server_catching_api_error() {
    let _guard = crate::setup();
    let raftkv_fp = "raftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();

    fail::cfg(raftkv_fp, "return()").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());

    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx.clone());
    let mut mutation = kvrpcpb::Mutation::default();
    mutation.op = Op::Put;
    mutation.key = b"k3".to_vec();
    mutation.value = b"v3".to_vec();
    prewrite_req.set_mutations(vec![mutation].into_iter().collect());
    prewrite_req.primary_lock = b"k3".to_vec();
    prewrite_req.start_version = 1;
    prewrite_req.lock_ttl = prewrite_req.start_version + 1;
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(prewrite_resp.has_region_error(), "{:?}", prewrite_resp);
    assert!(
        prewrite_resp.get_region_error().has_region_not_found(),
        "{:?}",
        prewrite_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = b"k3".to_vec();
    put_req.value = b"v3".to_vec();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(put_resp.has_region_error(), "{:?}", put_resp);
    assert!(
        put_resp.get_region_error().has_region_not_found(),
        "{:?}",
        put_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    fail::remove(raftkv_fp);
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error(), "{:?}", put_resp);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}
