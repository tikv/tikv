// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc::channel, Arc};
use std::thread;
use std::time::Duration;

use fail;
use grpcio::*;
use kvproto::kvrpcpb::{self, Context, Op, PrewriteRequest, RawPutRequest};
use kvproto::tikvpb::TikvClient;

use test_raftstore::{must_get_equal, must_get_none, new_server_cluster};
use tikv::storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use tikv::storage::txn::{commands, Error as TxnError, ErrorInner as TxnErrorInner};
use tikv::storage::{self, test_util::*, *};
use tikv_util::{collections::HashMap, HandyRwLock};
use txn_types::Key;
use txn_types::{Mutation, TimeStamp};

#[test]
fn test_scheduler_leader_change_twice() {
    let snapshot_fp = "scheduler_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let storage0 = TestStorageBuilder::from_engine(engine0).build().unwrap();

    let mut ctx0 = Context::default();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::cfg(snapshot_fp, "pause").unwrap();
    storage0
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                0,
                TimeStamp::default(),
                ctx0,
            ),
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
        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(KvError(
            box KvErrorInner::Request(ref e),
        ))))))
        | Err(Error(box ErrorInner::Engine(KvError(box KvErrorInner::Request(ref e))))) => {
            assert!(e.has_stale_command(), "{:?}", e);
        }
        res => {
            panic!("expect stale command, but got {:?}", res);
        }
    }
}

#[test]
fn test_server_catching_api_error() {
    let raftkv_fp = "raftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();

    fail::cfg(raftkv_fp, "return").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx.clone());
    let mut mutation = kvrpcpb::Mutation::default();
    mutation.op = Op::Put.into();
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
    put_req.set_context(ctx);
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

#[test]
fn test_raftkv_early_error_report() {
    let raftkv_fp = "raftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    cluster.must_split(&cluster.get_region(b"k0"), b"k1");

    let env = Arc::new(Environment::new(1));
    let mut clients: HashMap<&[u8], (Context, TikvClient)> = HashMap::default();
    for &k in &[b"k0", b"k1"] {
        let region = cluster.get_region(k);
        let leader = region.get_peers()[0].clone();
        let mut ctx = Context::default();
        let channel = ChannelBuilder::new(env.clone())
            .connect(cluster.sim.rl().get_addr(leader.get_store_id()));
        let client = TikvClient::new(channel);
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(leader);
        clients.insert(k, (ctx, client));
    }

    // Inject error to all regions.
    fail::cfg(raftkv_fp, "return").unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        assert!(put_resp.has_region_error(), "{:?}", put_resp);
        assert!(
            put_resp.get_region_error().has_region_not_found(),
            "{:?}",
            put_resp
        );
        must_get_none(&cluster.get_engine(1), k);
    }
    fail::remove(raftkv_fp);

    // Inject only one region
    let injected_region_id = clients[b"k0".as_ref()].0.get_region_id();
    fail::cfg(raftkv_fp, &format!("return({})", injected_region_id)).unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        if ctx.get_region_id() == injected_region_id {
            assert!(put_resp.has_region_error(), "{:?}", put_resp);
            assert!(
                put_resp.get_region_error().has_region_not_found(),
                "{:?}",
                put_resp
            );
            must_get_none(&cluster.get_engine(1), k);
        } else {
            assert!(!put_resp.has_region_error(), "{:?}", put_resp);
            must_get_equal(&cluster.get_engine(1), k, b"v");
        }
    }
    fail::remove(raftkv_fp);
}

#[test]
fn test_pipelined_pessimistic_lock() {
    let rockskv_async_write_fp = "rockskv_async_write";
    let rockskv_write_modifies_fp = "rockskv_write_modifies";
    let scheduler_async_write_finish_fp = "scheduler_async_write_finish";
    let scheduler_pipelined_write_finish_fp = "scheduler_pipelined_write_finish";

    {
        let storage = TestStorageBuilder::new()
            .set_pipelined_pessimistic_lock(false)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        // If storage fails to write the lock to engine, client should
        // receive the error when pipelined locking is disabled.
        fail::cfg(rockskv_write_modifies_fp, "return()").unwrap();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"key"), false)],
                    10,
                    10,
                    true,
                ),
                Box::new(move |res| {
                    res.unwrap_err();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        fail::remove(rockskv_write_modifies_fp);
    }

    let storage = TestStorageBuilder::new()
        .set_pipelined_pessimistic_lock(true)
        .build()
        .unwrap();

    let (tx, rx) = channel();
    let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());

    // Even if storage fails to write the lock to engine, client should
    // receive the successful response.
    fail::cfg(rockskv_write_modifies_fp, "return()").unwrap();
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 10, 10, true),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockRes::Values(vec![None]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_write_modifies_fp);
    fail::remove(scheduler_async_write_finish_fp);
    storage
        .sched_txn_command(
            commands::PrewritePessimistic::new(
                vec![(Mutation::Put((key.clone(), val.clone())), true)],
                key.to_raw().unwrap(),
                10.into(),
                3000,
                10.into(),
                1,
                11.into(),
                Context::default(),
            ),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();
    storage
        .sched_txn_command(
            commands::Commit::new(vec![key.clone()], 10.into(), 20.into(), Context::default()),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();

    // Should report failure if storage fails to schedule write request to engine.
    fail::cfg(rockskv_async_write_fp, "return()").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 30, 30, true),
            expect_fail_callback(tx.clone(), 0, |_| ()),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_async_write_fp);

    // Shouldn't release latches until async write finished.
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    for blocked in &[false, true] {
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 40, 40, true),
                expect_pessimistic_lock_res_callback(
                    tx.clone(),
                    PessimisticLockRes::Values(vec![Some(val.clone())]),
                ),
            )
            .unwrap();

        if !*blocked {
            rx.recv().unwrap();
        } else {
            // Blocked by latches.
            rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
        }
    }
    fail::remove(scheduler_async_write_finish_fp);
    rx.recv().unwrap();
    delete_pessimistic_lock(&storage, key.clone(), 40, 40);

    // Pipelined write is finished before async write.
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 50, 50, true),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockRes::Values(vec![Some(val.clone())]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(scheduler_async_write_finish_fp);
    delete_pessimistic_lock(&storage, key.clone(), 50, 50);

    // Async write is finished before pipelined write due to thread scheduling.
    // Storage should handle it properly.
    fail::cfg(scheduler_pipelined_write_finish_fp, "pause").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(
                vec![(key.clone(), false), (Key::from_raw(b"nonexist"), false)],
                60,
                60,
                true,
            ),
            expect_pessimistic_lock_res_callback(
                tx,
                PessimisticLockRes::Values(vec![Some(val), None]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(scheduler_pipelined_write_finish_fp);
    delete_pessimistic_lock(&storage, key, 60, 60);
}
