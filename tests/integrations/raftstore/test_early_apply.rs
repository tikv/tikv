// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksSnapshot;
use kvproto::raft_serverpb::*;
use raft::eraftpb::MessageType;
use std::time::*;
use test_raftstore::*;

/// Test whether system can recover from mismatched raft state and apply state.
///
/// If TiKV is not shutdown gracefully, apply state may go ahead of raft
/// state. TiKV should be able to recognize the situation and start normally.
#[test]
fn test_leader_early_apply() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    ));
    let last_index = cluster.raft_local_state(1, 1).get_last_index();
    cluster.async_put(b"k2", b"v2").unwrap();
    cluster.wait_last_index(1, 1, last_index + 1, Duration::from_secs(3));
    let snap = RocksSnapshot::new(cluster.get_engine(1));
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    cluster.stop_node(1);
    // Simulate data lost in raft cf.
    cluster.restore_raft(1, 1, &snap);

    cluster.run_node(1).unwrap();
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}
