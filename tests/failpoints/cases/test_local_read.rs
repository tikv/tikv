// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, GetRequest, Op},
    tikvpb_grpc::TikvClient,
};
use test_raftstore::{get_tso, new_mutation, new_peer, new_server_cluster, PeerClient};
use tikv_util::HandyRwLock;

// The test mocks the situation that just after passing the lease check, even
// when lease expires, we can read the correct value.
#[test]
fn test_consistency_after_lease_pass() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    let leader = new_peer(1, 1);
    cluster.must_transfer_leader(1, leader.clone());

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(1));
    let client = TikvClient::new(channel);
    let leader_client = PeerClient::new(&cluster, 1, leader);

    let _ = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Ensure the request is executed by the local reader
    fail::cfg("localreader_before_redirect", "panic").unwrap();

    // Lease read works correctly
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // we pause just after pass the lease check, and then remove the peer. We can
    // still read the relevant value as we should have already got the snapshot when
    // passing the lease check.
    fail::cfg("after_pass_lease_check", "pause").unwrap();

    let region = cluster.get_region(&b"key1"[..]);
    let region_id = region.id;
    let leader = cluster.leader_of_region(region_id).unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(region.get_region_epoch().clone());

    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.key = b"key1".to_vec();
    get_req.version = get_tso(&pd_client);
    let mut receiver = client.kv_get_async(&get_req).unwrap();

    thread::sleep(Duration::from_millis(200));

    cluster.must_transfer_leader(1, new_peer(2, 2));

    assert!(
        !cluster
            .async_remove_peer(region_id, leader)
            .unwrap()
            .recv()
            .unwrap()
            .get_header()
            .has_error()
    );

    fail::cfg("after_pass_lease_check", "off").unwrap();

    assert_eq!(b"value1", receiver.receive_sync().unwrap().1.get_value());
}
