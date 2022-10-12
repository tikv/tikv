// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, RawGetRequest},
    tikvpb_grpc::TikvClient,
};
use test_raftstore::{
    must_get_equal, must_get_none, must_raw_get, must_raw_put, new_peer, new_server_cluster,
};
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
    cluster.must_transfer_leader(1, leader);

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(1));
    let client = TikvClient::new(channel);

    let region = cluster.get_region(&b"key1"[..]);
    let region_id = region.id;
    let leader = cluster.leader_of_region(region_id).unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(region.get_region_epoch().clone());

    must_raw_put(&client, ctx.clone(), b"key1".to_vec(), b"value1".to_vec());
    must_get_equal(&cluster.get_engine(1), b"key1", b"value1");

    // Ensure the request is executed by the local reader
    fail::cfg("localreader_before_redirect", "panic").unwrap();

    // Lease read works correctly
    assert_eq!(
        must_raw_get(&client, ctx.clone(), b"key1".to_vec()).unwrap(),
        b"value1".to_vec()
    );

    // we pause just after pass the lease check, and then remove the peer. We can
    // still read the relevant value as we should have already got the snapshot when
    // passing the lease check.
    fail::cfg("after_pass_lease_check", "pause").unwrap();

    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx);
    get_req.key = b"key1".to_vec();
    let mut receiver = client.raw_get_async(&get_req).unwrap();

    thread::sleep(Duration::from_millis(200));

    let mut peer = leader.clone();
    cluster.must_transfer_leader(1, new_peer(2, 2));
    pd_client.must_remove_peer(region_id, leader);
    peer.id = 1000;
    // After we pass the lease check, we should have got the snapshot, so the data
    // that the region contains cannot be deleted.
    // So we need to add the new peer for this region and stop before applying the
    // snapshot so that the old data will be deleted and the snapshot data has not
    // been written.
    fail::cfg("apply_snap_cleanup_range", "pause").unwrap();
    pd_client.must_add_peer(region_id, peer);

    // Wait for data to be cleaned
    must_get_none(&cluster.get_engine(1), b"key1");
    fail::cfg("after_pass_lease_check", "off").unwrap();

    assert_eq!(b"value1", receiver.receive_sync().unwrap().1.get_value());
}
