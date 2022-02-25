// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;

/// When encountering raft/batch_raft mismatch store id error, the service is expected
/// to drop connections in order to let raft_client re-resolve store address from PD
/// This will make the mismatch error be automatically corrected.
/// Ths test verified this case.
#[test]
fn test_mismatch_store_node() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    let node_ids = cluster.get_node_ids();
    let mut iter = node_ids.iter();
    let node1_id = *iter.next().unwrap();
    let node2_id = *iter.next().unwrap();
    let node3_id = *iter.next().unwrap();
    let pd_client = cluster.pd_client.clone();
    must_get_equal(&cluster.get_engine(node1_id), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(node2_id), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(node3_id), b"k1", b"v1");
    let node1_addr = pd_client
        .get_store(node1_id)
        .unwrap()
        .get_address()
        .to_string();
    let node2_addr = pd_client
        .get_store(node2_id)
        .unwrap()
        .get_address()
        .to_string();
    let node3_addr = cluster
        .pd_client
        .get_store(node3_id)
        .unwrap()
        .get_address()
        .to_string();
    cluster.stop_node(node2_id);
    cluster.stop_node(node3_id);
    // run node2
    cluster.cfg.server.addr = node3_addr.clone();
    cluster.run_node(node2_id).unwrap();
    let filter = RegionPacketFilter::new(1, node2_id)
        .direction(Direction::Send)
        .msg_type(MessageType::MsgRequestPreVote);
    cluster.add_send_filter(CloneFilterFactory(filter));
    // run node3
    cluster.cfg.server.addr = node2_addr.clone();
    cluster.run_node(node3_id).unwrap();
    let filter = RegionPacketFilter::new(1, node3_id)
        .direction(Direction::Send)
        .msg_type(MessageType::MsgRequestPreVote);
    cluster.add_send_filter(CloneFilterFactory(filter));
    sleep_ms(600);
    fail::cfg("mock_store_refresh_interval_secs", "return(0)").unwrap();
    cluster.must_put(b"k2", b"v2");
    assert_eq!(
        node1_addr,
        pd_client.get_store(node1_id).unwrap().get_address()
    );
    assert_eq!(
        node3_addr,
        pd_client.get_store(node2_id).unwrap().get_address()
    );
    assert_eq!(
        node2_addr,
        cluster.pd_client.get_store(node3_id).unwrap().get_address()
    );
    must_get_equal(&cluster.get_engine(node3_id), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(node2_id), b"k2", b"v2");
    fail::remove("mock_store_refresh_interval_secs");
}

#[test]
fn test_send_raft_channel_full() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    for id in 1..=3 {
        must_get_equal(&cluster.get_engine(id), b"k1", b"v1");
    }

    let send_raft_message_full_fp = "send_raft_message_full";
    let on_batch_raft_stream_drop_by_err_fp = "on_batch_raft_stream_drop_by_err";
    fail::cfg(send_raft_message_full_fp, "return").unwrap();
    fail::cfg(on_batch_raft_stream_drop_by_err_fp, "panic").unwrap();

    // send request while channel full should not cause the connection drop
    cluster.async_put(b"k2", b"v2").unwrap();

    fail::remove(send_raft_message_full_fp);
    cluster.must_put(b"k3", b"v3");
    for id in 1..=3 {
        must_get_equal(&cluster.get_engine(id), b"k3", b"v3");
    }
    fail::remove(on_batch_raft_stream_drop_by_err_fp);
}
