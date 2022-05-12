// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::{must_kv_prewrite, must_new_cluster_and_kv_client, must_new_cluster_mul};

#[test]
fn test_batch_get_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = BatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![b"a".to_vec(), b"b".to_vec()].into());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_batch_get(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_kv_scan_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = ScanRequest::default();
    req.set_context(ctx);
    req.set_start_key(b"a".to_vec());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_scan(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_scan_lock_push_async_commit() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    for (use_green_gc, ts) in &[(false, 100), (true, 200)] {
        // We will perform a async commit transaction with start_ts == `ts`.
        // First, try pushing max_ts to `ts + 10`.
        if *use_green_gc {
            let mut req = RegisterLockObserverRequest::default();
            req.set_max_ts(ts + 10);
            let resp = client.register_lock_observer(&req).unwrap();
            assert_eq!(resp.error.len(), 0);
        } else {
            let mut req = ScanLockRequest::default();
            req.set_context(ctx.clone());
            req.set_max_version(ts + 10);
            let resp = client.kv_scan_lock(&req).unwrap();
            assert!(!resp.has_region_error());
            assert!(!resp.has_error());
        }

        let k1 = b"k1";
        let v1 = b"v1";

        // The following code simulates another case: prewrite is locking the memlock, and then
        // another scan lock operation request meets the memlock.

        fail::cfg("before-set-lock-in-memory", "pause").unwrap();
        let client1 = client.clone();
        let ctx1 = ctx.clone();
        let handle1 = std::thread::spawn(move || {
            let mut prewrite = PrewriteRequest::default();
            prewrite.set_context(ctx1);
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(k1.to_vec());
            mutation.set_value(v1.to_vec());
            prewrite.set_mutations(vec![mutation].into());
            prewrite.set_primary_lock(k1.to_vec());
            prewrite.set_start_version(*ts);
            prewrite.set_lock_ttl(1000);
            prewrite.set_use_async_commit(true);

            let resp = client1.kv_prewrite(&prewrite).unwrap();
            assert!(!resp.has_region_error());
            assert_eq!(resp.get_errors(), &[]);
            // min_commit_ts should be the last scan_lock ts + 1.
            assert_eq!(resp.min_commit_ts, ts + 11);
        });

        // Wait for the prewrite acquires the memlock
        std::thread::sleep(Duration::from_millis(200));

        let client1 = client.clone();
        let ctx1 = ctx.clone();
        let handle2 = std::thread::spawn(move || {
            if *use_green_gc {
                let mut req = RegisterLockObserverRequest::default();
                req.set_max_ts(ts + 20);
                let resp = client1.register_lock_observer(&req).unwrap();
                assert!(!resp.error.is_empty());
            } else {
                let mut req = ScanLockRequest::default();
                req.set_context(ctx1);
                req.set_max_version(ts + 20);
                let resp = client1.kv_scan_lock(&req).unwrap();
                assert!(!resp.has_region_error());
                assert!(resp.has_error());
            }
        });

        fail::remove("before-set-lock-in-memory");

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Commit the key so that next turn of test will work.
        let mut req = CommitRequest::default();
        req.set_context(ctx.clone());
        req.set_start_version(*ts);
        req.set_commit_version(ts + 11);
        req.set_keys(vec![k1.to_vec()].into());
        let resp = client.kv_commit(&req).unwrap();
        assert!(!resp.has_region_error());
        assert!(!resp.has_error());
        assert_eq!(resp.commit_version, ts + 11);
    }
}

#[test]
fn test_snapshot_not_block_grpc() {
    let (cluster, leader, ctx) = must_new_cluster_mul(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_millis(500))
        .keepalive_timeout(Duration::from_millis(500))
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k".to_vec());
    mutation.set_value(b"v".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation.clone()],
        b"k".to_vec(),
        10,
    );
    // Block getting snapshot. It shouldn't trigger keepalive watchdog timeout.
    fail::cfg("after-snapshot", "sleep(2000)").unwrap();
    must_kv_prewrite(&client, ctx, vec![mutation], b"k".to_vec(), 10);
    fail::remove("after-snapshot");
}
