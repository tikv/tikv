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

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(leader.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

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
    req.set_context(ctx.clone());
    req.set_primary_key(b"key".to_vec());
    req.set_lock_ts(start_ts.into_inner());
    req.set_rollback_if_not_exist(true);
    let resp = client.kv_check_txn_status(&req).unwrap();
    assert_ne!(resp.get_action(), Action::MinCommitTsPushed);
}
