// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{Future, Sink, Stream};
use grpcio::*;
use kvproto::tikvpb::TikvClient;
use kvproto::tikvpb::*;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use test_raftstore::new_server_cluster;
use tikv::server::service::batch_commands_request;
use tikv_util::HandyRwLock;

#[test]
fn test_batch_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let leader = cluster.get_region(b"").get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            batch_req.mut_requests().push(Default::default());
            batch_req.mut_request_ids().push(i);
        }
        match sender.send((batch_req, WriteFlags::default())).wait() {
            Ok(s) => sender = s,
            Err(e) => panic!("tikv client send fail: {:?}", e),
        }
    }

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in receiver
            .wait()
            .map(move |b| b.unwrap().get_responses().len())
        {
            count += x;
            if count == 10000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_empty_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let leader = cluster.get_region(b"").get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            let mut req = batch_commands_request::Request::default();
            req.cmd = Some(batch_commands_request::request::Cmd::Empty(
                Default::default(),
            ));
            batch_req.mut_requests().push(req);
            batch_req.mut_request_ids().push(i);
        }
        match sender.send((batch_req, WriteFlags::default())).wait() {
            Ok(s) => sender = s,
            Err(e) => panic!("tikv client send fail: {:?}", e),
        }
    }

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in receiver
            .wait()
            .map(move |b| b.unwrap().get_responses().len())
        {
            count += x;
            if count == 10000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}
<<<<<<< HEAD
=======

#[test]
fn test_async_commit_check_txn_status() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let (client, ctx) = build_client(&cluster);

    let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"key".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"key".to_vec());
    mutation.set_value(b"value".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(start_ts.into_inner());
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    client.kv_prewrite(&req).unwrap();

    let mut req = CheckTxnStatusRequest::default();
    req.set_context(ctx);
    req.set_primary_key(b"key".to_vec());
    req.set_lock_ts(start_ts.into_inner());
    req.set_rollback_if_not_exist(true);
    let resp = client.kv_check_txn_status(&req).unwrap();
    assert_ne!(resp.get_action(), Action::MinCommitTsPushed);
}

#[test]
fn test_read_index_check_memory_locks() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.run();

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b"k"), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let leader_cm = cluster.sim.rl().get_concurrency_manager(leader.get_id());

    let keys: Vec<_> = vec![b"k", b"l"]
        .into_iter()
        .map(|k| Key::from_raw(k))
        .collect();
    let guards = block_on(leader_cm.lock_keys(keys.iter()));
    let lock = Lock::new(
        LockType::Put,
        b"k".to_vec(),
        1.into(),
        20000,
        None,
        1.into(),
        1,
        2.into(),
    );
    guards[0].with_lock(|l| *l = Some(lock.clone()));

    // read on follower
    let mut follower_peer = None;
    let peers = region.get_peers();
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_peer = Some(p.clone());
            break;
        }
    }
    let follower_peer = follower_peer.unwrap();
    let addr = cluster.sim.rl().get_addr(follower_peer.get_store_id());

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(follower_peer);

    let read_index = |ranges: &[(&[u8], &[u8])]| {
        let mut req = ReadIndexRequest::default();
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
        req.set_context(ctx.clone());
        req.set_start_ts(start_ts.into_inner());
        for &(start_key, end_key) in ranges {
            let mut range = kvrpcpb::KeyRange::default();
            range.set_start_key(start_key.to_vec());
            range.set_end_key(end_key.to_vec());
            req.mut_ranges().push(range);
        }
        let resp = client.read_index(&req).unwrap();
        (resp, start_ts)
    };

    // wait a while until the node updates its own max ts
    thread::sleep(Duration::from_millis(300));

    let (resp, start_ts) = read_index(&[(b"l", b"yz")]);
    assert!(!resp.has_locked());
    assert_eq!(leader_cm.max_ts(), start_ts);

    let (resp, start_ts) = read_index(&[(b"a", b"b"), (b"j", b"k0")]);
    assert_eq!(resp.get_locked(), &lock.into_lock_info(b"k".to_vec()));
    assert_eq!(leader_cm.max_ts(), start_ts);

    drop(guards);

    let (resp, start_ts) = read_index(&[(b"a", b"z")]);
    assert!(!resp.has_locked());
    assert_eq!(leader_cm.max_ts(), start_ts);
}

#[test]
fn test_prewrite_check_max_commit_ts() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    cm.update_max_ts(100.into());

    let (client, ctx) = build_client(&cluster);

    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"k1".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k1".to_vec());
    mutation.set_value(b"v1".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(10);
    req.set_max_commit_ts(200);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    let resp = client.kv_prewrite(&req).unwrap();
    assert_eq!(resp.get_min_commit_ts(), 101);

    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"k2".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k2".to_vec());
    mutation.set_value(b"v2".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_min_commit_ts(21);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    // Test the idempotency of prewrite when falling back to 2PC.
    for _ in 0..2 {
        let resp = client.kv_prewrite(&req).unwrap();
        assert_eq!(resp.get_min_commit_ts(), 0);
        assert_eq!(resp.get_one_pc_commit_ts(), 0);
    }

    // 1PC
    let mut req = PrewriteRequest::default();
    req.set_context(ctx);
    req.set_primary_lock(b"k3".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k3".to_vec());
    mutation.set_value(b"v3".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_min_commit_ts(21);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    req.set_try_one_pc(true);
    // Test the idempotency of prewrite when falling back to 2PC.
    for _ in 0..2 {
        let resp = client.kv_prewrite(&req).unwrap();
        assert_eq!(resp.get_min_commit_ts(), 0);
        assert_eq!(resp.get_one_pc_commit_ts(), 0);
    }

    // There shouldn't be locks remaining in the lock table.
    assert!(cm.read_range_check(None, None, |_, _| Err(())).is_ok());
}

#[test]
fn test_txn_heart_beat() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let mut req = TxnHeartBeatRequest::default();
    let k = b"k".to_vec();
    let start_ts = 10;
    req.set_context(ctx);
    req.set_primary_lock(k.clone());
    req.set_start_version(start_ts);
    req.set_advise_lock_ttl(1000);
    let resp = client.kv_txn_heart_beat(&req).unwrap();
    assert!(!resp.has_region_error());
    assert_eq!(
        resp.get_error().get_txn_not_found(),
        &TxnNotFound {
            start_ts,
            primary_key: k,
            ..Default::default()
        }
    );
}

fn test_with_memory_lock_cluster(f: impl FnOnce(TikvClient, Context, /* raw_key */ Vec<u8>, Lock)) {
    let (cluster, client, ctx) = must_new_cluster_and_kv_client();
    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    let raw_key = b"key".to_vec();
    let key = Key::from_raw(&raw_key);
    let guard = block_on(cm.lock_key(&key));
    let lock = Lock::new(
        LockType::Put,
        b"key".to_vec(),
        10.into(),
        20000,
        None,
        10.into(),
        1,
        20.into(),
    )
    .use_async_commit(vec![]);
    guard.with_lock(|l| {
        *l = Some(lock.clone());
    });
    f(client, ctx, raw_key, lock);
}

#[test]
fn test_batch_get_memory_lock() {
    test_with_memory_lock_cluster(|client, ctx, raw_key, lock| {
        let mut req = BatchGetRequest::default();
        req.set_context(ctx);
        req.set_keys(vec![b"unlocked".to_vec(), raw_key.clone()].into());
        req.version = 50;
        let resp = client.kv_batch_get(&req).unwrap();
        let lock_info = lock.into_lock_info(raw_key);
        assert_eq!(resp.pairs[0].get_error().get_locked(), &lock_info);
        assert_eq!(resp.get_error().get_locked(), &lock_info);
    });
}

#[test]
fn test_kv_scan_memory_lock() {
    test_with_memory_lock_cluster(|client, ctx, raw_key, lock| {
        let mut req = ScanRequest::default();
        req.set_context(ctx);
        req.set_start_key(b"a".to_vec());
        req.version = 50;
        let resp = client.kv_scan(&req).unwrap();
        let lock_info = lock.into_lock_info(raw_key);
        assert_eq!(resp.pairs[0].get_error().get_locked(), &lock_info);
        assert_eq!(resp.get_error().get_locked(), &lock_info);
    });
}

macro_rules! test_func {
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $init:expr) => {{
        let mut req = $init;
        req.set_context($ctx.clone());

        // Not setting forwarding should lead to store not match.
        let resp = paste::paste! {
            $client.[<$func _opt>](&req, CallOption::default().timeout(Duration::from_secs(3))).unwrap()
        };
        let err = resp.get_region_error();
        assert!(err.has_store_not_match() || err.has_not_leader(), "{:?}", resp);

        // Proxy should redirect the request to the correct store.
        let resp = paste::paste! {
            $client.[<$func _opt>](&req, $call_opt.clone()).unwrap()
        };
        let err = resp.get_region_error();
        assert!(!err.has_store_not_match() && !err.has_not_leader(), "{:?}", resp);
    }};
}

macro_rules! test_func_init {
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident) => {{ test_func!($client, $ctx, $call_opt, $func, $req::default()) }};
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident, batch) => {{
        test_func!($client, $ctx, $call_opt, $func, {
            let mut req = $req::default();
            req.set_keys(vec![b"key".to_vec()].into());
            req
        })
    }};
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident, $op:expr) => {{
        test_func!($client, $ctx, $call_opt, $func, {
            let mut req = $req::default();
            let mut m = Mutation::default();
            m.set_op($op);
            m.key = b"key".to_vec();
            req.mut_mutations().push(m);
            req
        })
    }};
}

fn setup_cluster() -> (Cluster<ServerCluster>, TikvClient, CallOption, Context) {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id());
    let region = cluster.get_region(b"k1");
    let follower = region
        .get_peers()
        .iter()
        .find(|p| **p != leader)
        .unwrap()
        .clone();
    let follower_addr = cluster.sim.rl().get_addr(follower.get_store_id());
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&follower_addr);
    let client = TikvClient::new(channel);

    // Verify not setting forwarding header will result in store not match.
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(
        put_resp.get_region_error().has_store_not_match(),
        "{:?}",
        put_resp
    );
    assert!(put_resp.error.is_empty(), "{:?}", put_resp);

    let call_opt = server::build_forward_option(&leader_addr).timeout(Duration::from_secs(3));
    (cluster, client, call_opt, ctx)
}

/// Check all supported requests can go through proxy correctly.
#[test]
fn test_tikv_forwarding() {
    let (_cluster, client, call_opt, ctx) = setup_cluster();

    // Verify not setting forwarding header will result in store not match.
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(
        put_resp.get_region_error().has_store_not_match(),
        "{:?}",
        put_resp
    );
    assert!(put_resp.error.is_empty(), "{:?}", put_resp);

    test_func_init!(client, ctx, call_opt, kv_get, GetRequest);
    test_func_init!(client, ctx, call_opt, kv_scan, ScanRequest);
    test_func_init!(client, ctx, call_opt, kv_prewrite, PrewriteRequest, Op::Put);
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_pessimistic_lock,
        PessimisticLockRequest,
        Op::PessimisticLock
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        batch
    );
    test_func_init!(client, ctx, call_opt, kv_commit, CommitRequest, batch);
    test_func_init!(client, ctx, call_opt, kv_cleanup, CleanupRequest);
    test_func_init!(client, ctx, call_opt, kv_batch_get, BatchGetRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_batch_rollback,
        BatchRollbackRequest,
        batch
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_txn_heart_beat,
        TxnHeartBeatRequest
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_check_txn_status,
        CheckTxnStatusRequest
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        batch
    );
    test_func_init!(client, ctx, call_opt, kv_scan_lock, ScanLockRequest);
    test_func_init!(client, ctx, call_opt, kv_resolve_lock, ResolveLockRequest);
    test_func_init!(client, ctx, call_opt, kv_delete_range, DeleteRangeRequest);
    test_func_init!(client, ctx, call_opt, mvcc_get_by_key, MvccGetByKeyRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        mvcc_get_by_start_ts,
        MvccGetByStartTsRequest
    );
    test_func_init!(client, ctx, call_opt, raw_get, RawGetRequest);
    test_func_init!(client, ctx, call_opt, raw_batch_get, RawBatchGetRequest);
    test_func_init!(client, ctx, call_opt, raw_scan, RawScanRequest);
    test_func_init!(client, ctx, call_opt, raw_batch_scan, RawBatchScanRequest);
    test_func_init!(client, ctx, call_opt, raw_put, RawPutRequest);
    test_func!(client, ctx, call_opt, raw_batch_put, {
        let mut req = RawBatchPutRequest::default();
        req.set_pairs(vec![KvPair::default()].into());
        req
    });
    test_func_init!(client, ctx, call_opt, raw_delete, RawDeleteRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        raw_batch_delete,
        RawBatchDeleteRequest,
        batch
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        raw_delete_range,
        RawDeleteRangeRequest
    );
    test_func!(client, ctx, call_opt, coprocessor, {
        let mut req = Request::default();
        req.set_tp(REQ_TYPE_DAG);
        req
    });
    test_func!(client, ctx, call_opt, split_region, {
        let mut req = SplitRegionRequest::default();
        req.set_split_key(b"k1".to_vec());
        req
    });
    test_func_init!(client, ctx, call_opt, read_index, ReadIndexRequest);

    // Test if duplex can be redirect correctly.
    let cases = vec![
        (CallOption::default().timeout(Duration::from_secs(3)), false),
        (call_opt, true),
    ];
    for (opt, success) in cases {
        let (mut sender, receiver) = client.batch_commands_opt(opt).unwrap();
        for _ in 0..100 {
            let mut batch_req = BatchCommandsRequest::default();
            for i in 0..10 {
                let mut get = GetRequest::default();
                get.set_context(ctx.clone());
                let mut req = batch_commands_request::Request::default();
                req.cmd = Some(batch_commands_request::request::Cmd::Get(get));
                batch_req.mut_requests().push(req);
                batch_req.mut_request_ids().push(i);
            }
            block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
        }
        block_on(sender.close()).unwrap();

        // We have send 1k requests to the server, so we should get 1k responses.
        let resps = block_on(
            receiver
                .map(move |b| futures::stream::iter(b.unwrap().take_responses().into_vec()))
                .flatten()
                .collect::<Vec<_>>(),
        );
        assert_eq!(resps.len(), 1000);
        for resp in resps {
            let resp = match resp.cmd {
                Some(batch_commands_response::response::Cmd::Get(g)) => g,
                _ => panic!("unexpected response {:?}", resp),
            };
            let error = resp.get_region_error();
            if success {
                assert!(!error.has_store_not_match(), "{:?}", resp);
            } else {
                assert!(error.has_store_not_match(), "{:?}", resp);
            }
        }
    }
}

/// Test if forwarding works correctly if the target node is shutdown and restarted.
#[test]
fn test_forwarding_reconnect() {
    let (mut cluster, client, call_opt, ctx) = setup_cluster();
    let leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(leader.get_store_id());

    let mut req = RawGetRequest::default();
    req.set_context(ctx);
    // Large timeout value to ensure the error is from proxy instead of client.
    let timer = tikv_util::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let res = client.raw_get_opt(&req, call_opt.clone().timeout(timeout));
    let elapsed = timer.saturating_elapsed();
    assert!(elapsed < timeout, "{:?}", elapsed);
    // Because leader server is shutdown, reconnecting has to be timeout.
    match res {
        Err(grpcio::Error::RpcFailure(s)) => assert_eq!(s.code(), RpcStatusCode::CANCELLED),
        _ => panic!("unexpected result {:?}", res),
    }

    cluster.run_node(leader.get_store_id()).unwrap();
    let resp = client.raw_get_opt(&req, call_opt).unwrap();
    assert!(!resp.get_region_error().has_store_not_match(), "{:?}", resp);
}

#[test]
fn test_health_check() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let addr = cluster.sim.rl().get_addr(1);

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = HealthClient::new(channel);
    let req = HealthCheckRequest {
        service: "".to_string(),
        ..Default::default()
    };
    let resp = client.check(&req).unwrap();
    assert_eq!(ServingStatus::Serving, resp.status);

    cluster.shutdown();
    client.check(&req).unwrap_err();
}

#[test]
fn test_get_lock_wait_info_api() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let client2 = client.clone();

    let mut ctx1 = ctx.clone();
    ctx1.set_resource_group_tag(b"resource_group_tag1".to_vec());
    kv_pessimistic_lock_with_ttl(&client, ctx1, vec![b"a".to_vec()], 20, 20, false, 5000);
    let mut ctx2 = ctx.clone();
    let handle = thread::spawn(move || {
        ctx2.set_resource_group_tag(b"resource_group_tag2".to_vec());
        kv_pessimistic_lock_with_ttl(&client2, ctx2, vec![b"a".to_vec()], 30, 30, false, 5000);
    });

    let mut entries = None;
    for _retry in 0..200 {
        thread::sleep(Duration::from_millis(25));
        // The lock should be in waiting state here.
        let req = GetLockWaitInfoRequest::default();
        let resp = client.get_lock_wait_info(&req).unwrap();
        if resp.entries.len() != 0 {
            entries = Some(resp.entries.to_vec());
            break;
        }
    }

    let entries = entries.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].txn, 30);
    assert_eq!(entries[0].wait_for_txn, 20);
    assert_eq!(entries[0].key, b"a".to_vec());
    assert_eq!(
        entries[0].resource_group_tag,
        b"resource_group_tag2".to_vec()
    );
    must_kv_pessimistic_rollback(&client, ctx, b"a".to_vec(), 20);
    handle.join().unwrap();
}
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
