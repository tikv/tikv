// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{kvrpcpb::AllowedLevel, raft_cmdpb::*};
use raft::eraftpb::MessageType;
use raftstore::store::msg::*;
use std::sync::mpsc;
use std::time::Duration;
use test_raftstore::*;

fn assert_disk_full(resp: &RaftCmdResponse) {
    assert!(resp.get_header().get_error().has_disk_full());
}

#[test]
fn test_disk_almost_full_unallowed_leader_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_almost_full_peer_1", "return").unwrap();

    // Test new normal proposals won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    fail::remove("disk_almost_full_peer_1");
}

#[test]
fn test_disk_almost_full_allowed_leader_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_almost_full_peer_1", "return").unwrap();

    // Test transfer leader should be allowed.
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Transfer the leadership back to store 1.
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Test remove peer should be allowed.
    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");

    // Test add peer should be allowed.
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Test split should be allowed.
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k1");

    // Test merge should be allowed.
    let region_1 = cluster.get_region(b"k0");
    let region_2 = cluster.get_region(b"k2");
    cluster.must_try_merge(region_1.get_id(), region_2.get_id());

    // Test set operations allowed flag will be allowed to exec.
    cluster.set_allowed_level_on_disk_full(AllowedLevel::AllowedAlmostFull);
    cluster.must_put(b"k11", b"v11");

    fail::remove("disk_almost_full_peer_1");
}

#[test]
fn test_disk_already_full_unallowed_leader_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    fail::cfg("disk_already_full_peer_1", "return").unwrap();

    // Test set operations AllowedAlmostFull flag will not be allowed to exec.
    cluster.set_allowed_level_on_disk_full(AllowedLevel::AllowedAlmostFull);
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);
    cluster.clear_allowed_level_on_disk_full();

    // Test split won't be allowed.
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

    // Test new normal proposals won't be allowed when disk is full.
    let old_last_index = cluster.raft_local_state(1, 1).last_index;
    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_disk_full(&rx.recv_timeout(Duration::from_secs(2)).unwrap());
    let new_last_index = cluster.raft_local_state(1, 1).last_index;
    assert_eq!(old_last_index, new_last_index);

    fail::remove("disk_already_full_peer_1");
}

#[test]
fn test_disk_already_full_unallowed_follower_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    fail::cfg("disk_already_full_peer_2", "return").unwrap();
    fail::cfg("disk_already_full_peer_3", "return").unwrap();

    let rx = cluster.async_put(b"k2", b"v2").unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    let new_last_index_peer_1 = cluster.raft_local_state(1, 1).last_index;
    let new_last_index_peer_2 = cluster.raft_local_state(1, 2).last_index;
    let new_last_index_peer_3 = cluster.raft_local_state(1, 3).last_index;
    assert!(new_last_index_peer_1 != new_last_index_peer_2);
    assert!(new_last_index_peer_1 != new_last_index_peer_3);
    assert_eq!(new_last_index_peer_2, new_last_index_peer_3);

    fail::remove("disk_already_full_peer_2");
    fail::remove("disk_already_full_peer_3");
}

#[test]
fn test_disk_already_full_allowed_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    fail::cfg("disk_already_full_peer_1", "return").unwrap();
    fail::cfg("disk_already_full_peer_2", "return").unwrap();
    fail::cfg("disk_already_full_peer_3", "return").unwrap();

    // Test transfer leader should be allowed.
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Transfer the leadership back to store 1.
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Test remove peer should be allowed.
    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");

    // Test add peer should be allowed.
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    //TODO Fix Test operations with
    // cluster.set_allowed_level_on_disk_full(AllowedLevel::AllowedAlreadyFull);
    // cluster.must_put(b"k00", b"v00");
    // cluster.clear_allowed_level_on_disk_full();

    fail::remove("disk_already_full_peer_1");
    fail::remove("disk_already_full_peer_2");
    fail::remove("disk_already_full_peer_3");
}

#[test]
fn test_disk_full_follower_behaviors() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // To ensure all replicas are not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::cfg("disk_full_peer_2", "return").unwrap();

    // Test followers will response votes when disk is full.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgRequestVoteResponse),
    ));
    cluster.must_transfer_leader(1, new_peer(3, 3));

    fail::remove("disk_full_peer_2");
}
