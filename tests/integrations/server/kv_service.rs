// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use futures::{SinkExt, StreamExt};
use grpcio::*;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use kvproto::tikvpb::*;
use pd_client::PdClient;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster};
use tikv::server::service::batch_commands_request;
use tikv_util::HandyRwLock;
use txn_types::{Key, Lock, LockType};

fn build_client(cluster: &Cluster<ServerCluster>) -> (TikvClient, Context) {
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id());

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(leader.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    (client, ctx)
}

#[test]
fn test_batch_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let (client, _) = build_client(&cluster);
    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            batch_req.mut_requests().push(Default::default());
            batch_req.mut_request_ids().push(i);
        }
        block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    }
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in block_on(
            receiver
                .map(move |b| b.unwrap().get_responses().len())
                .collect::<Vec<usize>>(),
        ) {
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

    let (client, _) = build_client(&cluster);
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
        block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    }
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in block_on(
            receiver
                .map(move |b| b.unwrap().get_responses().len())
                .collect::<Vec<usize>>(),
        ) {
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
            let mut range = KeyRange::default();
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
    req.set_context(ctx);
    req.set_primary_lock(b"k2".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k2".to_vec());
    mutation.set_value(b"v2".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    let resp = client.kv_prewrite(&req).unwrap();
    assert_eq!(
        resp.get_errors()[0]
            .get_commit_ts_too_large()
            .get_commit_ts(),
        101
    );
    // There shouldn't be locks remaining in the lock table.
    assert!(cm.read_range_check(None, None, |_, _| Err(())).is_ok());
}
