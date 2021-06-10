// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_cmdpb::*;
use raft::eraftpb::MessageType;
use raftstore::store::msg::*;
use std::sync::mpsc;
use std::time::Duration;
use test_raftstore::*;

fn assert_disk_full(resp: &RaftCmdResponse) {
    let msg = resp.get_header().get_error().get_message();
    assert!(msg.contains("disk full"));
}

fn test_unallowed_leader_behaviors(cluster: &mut Cluster<ServerCluster>) {
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_full_peer_1", "return").unwrap();

    // Test new normal proposals won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    // Test split won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let region = cluster.get_region(b"k1");
    let (tx, rx) = mpsc::sync_channel(1);
    cluster.split_region(
        &region,
        b"k1",
        Callback::write(Box::new(move |resp| tx.send(resp.response).unwrap())),
    );
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    fail::remove("disk_full_peer_1");
}

fn test_allowed_leader_behaviors(cluster: &mut Cluster<ServerCluster>) {
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_full_peer_1", "return").unwrap();

    // Test transfer leader should be allowed.
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Transfer the leadership back to store 1.
    fail::remove("disk_full_peer_1");
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_full_peer_1", "return").unwrap();

    // Test remove peer should be allowed.
    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");

    // Test add peer should be allowed.
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    fail::remove("disk_full_peer_1");
}

fn test_follower_behaviors(cluster: &mut Cluster<ServerCluster>) {
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_full_peer_2", "return").unwrap();

    // Test followers will reject pre-transfer-leader command.
    let epoch = cluster.get_region_epoch(1);
    let transfer = new_admin_request(1, &epoch, new_transfer_leader_cmd(new_peer(2, 2)));
    cluster
        .call_command_on_leader(transfer, Duration::from_secs(3))
        .unwrap();
    assert_eq!(cluster.leader_of_region(1).unwrap(), new_peer(1, 1));
    cluster.must_put(b"k2", b"v2");

    // Test followers will drop entries when disk is full.
    let old_last_index = cluster.raft_local_state(1, 2).last_index;
    cluster.must_put(b"k3", b"v3");
    let new_last_index = cluster.raft_local_state(1, 2).last_index;
    assert_eq!(old_last_index, new_last_index);
    must_get_none(&cluster.get_engine(2), b"k3");

    // Test followers will response votes when disk is full.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgRequestVoteResponse),
    ));
    cluster.must_transfer_leader(1, new_peer(3, 3));

    fail::remove("disk_full_peer_2");
}

#[test]
fn test_disk_full() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    test_unallowed_leader_behaviors(&mut cluster);
    test_allowed_leader_behaviors(&mut cluster);
    test_follower_behaviors(&mut cluster);
}
