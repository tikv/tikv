// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use std::thread;
use std::time::Duration;
use test_raftstore::must_new_cluster_and_kv_client;

#[test]
fn test_batch_get_return_errors() {
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
fn test_kv_scan_return_errors() {
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

        fail::cfg("scheduler_async_write_finish", "pause").unwrap();
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
        thread::sleep(Duration::from_millis(200));

        let client1 = client.clone();
        let ctx1 = ctx.clone();
        let handle2 = thread::spawn(move || {
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

        handle2.join().unwrap();

        fail::remove("scheduler_async_write_finish");
        handle1.join().unwrap();

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

fn test_memory_lock_backoff_succeed(f: impl FnOnce(TikvClient, Context) + Send + 'static) {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let k1 = b"k1";
    let k2 = b"k2";
    let v = b"v";
    let ts = 10;

    fail::cfg("scheduler_async_write_finish", "pause").unwrap();
    let client1 = client.clone();
    let ctx1 = ctx.clone();
    let handle1 = std::thread::spawn(move || {
        let mut prewrite = PrewriteRequest::default();
        prewrite.set_context(ctx1);
        let mut m1 = Mutation::default();
        m1.set_op(Op::Put);
        m1.set_key(k1.to_vec());
        m1.set_value(v.to_vec());
        let mut m2 = Mutation::default();
        m2.set_op(Op::Put);
        m2.set_key(k2.to_vec());
        m2.set_value(v.to_vec());
        prewrite.set_mutations(vec![m1, m2].into());
        prewrite.set_primary_lock(k1.to_vec());
        prewrite.set_start_version(ts);
        prewrite.set_lock_ttl(1000);
        prewrite.set_use_async_commit(true);
        prewrite.set_try_one_pc(true);

        let resp = client1.kv_prewrite(&prewrite).unwrap();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_errors(), &[]);
    });

    // Wait until the prewrite acquires the mem lock
    thread::sleep(Duration::from_millis(200));

    let handle2 = thread::spawn(move || f(client, ctx));
    fail::cfg("scheduler_async_write_finish", "sleep(20)").unwrap();

    handle1.join().unwrap();
    handle2.join().unwrap();

    fail::remove("scheduler_async_write_finish");
}

fn test_memory_lock_backoff_succeed_partial(f: impl FnOnce(TikvClient, Context) + Send + 'static) {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let k1 = b"k1";
    let k2 = b"k2";
    let v = b"v";

    fail::cfg("scheduler_async_write_finish", "pause").unwrap();
    let client1 = client.clone();
    let ctx1 = ctx.clone();
    let handle1 = std::thread::spawn(move || {
        let mut prewrite = PrewriteRequest::default();
        prewrite.set_context(ctx1);
        let mut m1 = Mutation::default();
        m1.set_op(Op::Put);
        m1.set_key(k1.to_vec());
        m1.set_value(v.to_vec());
        prewrite.set_mutations(vec![m1].into());
        prewrite.set_primary_lock(k1.to_vec());
        prewrite.set_start_version(10);
        prewrite.set_lock_ttl(1000);
        prewrite.set_use_async_commit(true);
        prewrite.set_try_one_pc(true);
        let resp = client1.kv_prewrite(&prewrite).unwrap();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_errors(), &[]);
    });

    let client2 = client.clone();
    let ctx2 = ctx.clone();
    let handle2 = std::thread::spawn(move || {
        let mut prewrite = PrewriteRequest::default();
        prewrite.set_context(ctx2);
        let mut m2 = Mutation::default();
        m2.set_op(Op::Put);
        m2.set_key(k2.to_vec());
        m2.set_value(v.to_vec());
        prewrite.set_mutations(vec![m2].into());
        prewrite.set_primary_lock(k2.to_vec());
        prewrite.set_start_version(11);
        prewrite.set_lock_ttl(1000);
        prewrite.set_use_async_commit(true);
        prewrite.set_try_one_pc(true);
        let resp = client2.kv_prewrite(&prewrite).unwrap();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_errors(), &[]);
    });

    // Wait until the prewrites acquire the mem lock
    thread::sleep(Duration::from_millis(200));

    let handle3 = thread::spawn(move || f(client, ctx));
    fail::cfg("scheduler_async_write_finish", "sleep(20)").unwrap();
    fail::cfg("scheduler_async_write_finish", "pause").unwrap();

    handle3.join().unwrap();

    fail::remove("scheduler_async_write_finish");
    handle1.join().unwrap();
    handle2.join().unwrap();
}

#[test]
fn test_kv_get_memory_lock_backoff() {
    test_memory_lock_backoff_succeed(|client, ctx| {
        let mut req = GetRequest::default();
        req.set_context(ctx);
        req.set_key(b"k2".to_vec());
        req.set_version(u64::MAX);
        let resp = client.kv_get(&req).unwrap();
        assert_eq!(resp.get_value(), b"v");
    });
}

#[test]
fn test_kv_batch_get_memory_lock_backoff() {
    // test_memory_lock_backoff_succeed(|client, ctx| {
    //     let mut req = BatchGetRequest::default();
    //     req.set_context(ctx);
    //     req.set_keys(vec![b"k1".to_vec(), b"k2".to_vec()].into());
    //     req.set_version(u64::MAX);
    //     let resp = client.kv_batch_get(&req).unwrap();
    //     assert_eq!(resp.get_pairs()[0].get_key(), b"k1");
    //     assert_eq!(resp.get_pairs()[0].get_value(), b"v");
    //     assert_eq!(resp.get_pairs()[1].get_key(), b"k2");
    //     assert_eq!(resp.get_pairs()[1].get_value(), b"v");
    // });

    test_memory_lock_backoff_succeed_partial(|client, ctx| {
        let mut req = BatchGetRequest::default();
        req.set_context(ctx);
        req.set_keys(vec![b"k1".to_vec(), b"k2".to_vec()].into());
        req.set_version(u64::MAX);
        let resp = client.kv_batch_get(&req).unwrap();
        assert!(resp.has_error());
    });
}

#[test]
fn test_kv_scan_memory_lock_backoff() {
    test_memory_lock_backoff_succeed(|client, ctx| {
        let mut req = ScanRequest::default();
        req.set_context(ctx);
        req.set_start_key(b"k1".to_vec());
        req.set_end_key(b"z".to_vec());
        req.set_version(u64::MAX);
        req.set_limit(2);
        let resp = client.kv_scan(&req).unwrap();
        assert_eq!(resp.get_pairs()[0].get_key(), b"k1");
        assert_eq!(resp.get_pairs()[0].get_value(), b"v");
        assert_eq!(resp.get_pairs()[1].get_key(), b"k2");
        assert_eq!(resp.get_pairs()[1].get_value(), b"v");
    });

    test_memory_lock_backoff_succeed_partial(|client, ctx| {
        let mut req = ScanRequest::default();
        req.set_context(ctx);
        req.set_start_key(b"k1".to_vec());
        req.set_end_key(b"z".to_vec());
        req.set_version(u64::MAX);
        req.set_limit(2);
        let resp = client.kv_scan(&req).unwrap();
        assert!(resp.has_error());
    });
}
