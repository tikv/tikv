// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::HandyRwLock;

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
    must_get_equal(&cluster.get_engine(node1_id), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(node2_id), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(node3_id), b"k1", b"v1");
    let node1_addr = cluster
        .pd_client
        .get_store(node1_id)
        .unwrap()
        .get_address()
        .to_string();
    let node2_addr = cluster
        .pd_client
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
    fail::cfg("invalidate_addr_cache", "return()").unwrap();
    // run node2
    cluster.cfg.server.addr = node3_addr.clone();
    cluster.run_node(node2_id).unwrap();
    let recv_filter = RegionPacketFilter::new(1, node2_id)
        .direction(Direction::Recv)
        .msg_type(MessageType::MsgRequestVote);
    cluster
        .sim
        .wl()
        .add_recv_filter(node2_id, Box::new(recv_filter));

    // run node3
    cluster.cfg.server.addr = node2_addr.clone();
    cluster.run_node(node3_id).unwrap();
    let recv_filter = RegionPacketFilter::new(1, node3_id)
        .direction(Direction::Recv)
        .msg_type(MessageType::MsgRequestVote);
    cluster
        .sim
        .wl()
        .add_send_filter(node3_id, Box::new(recv_filter));
    fail::cfg("mock_store_refresh_interval_secs", "return(0)").unwrap();
    sleep_ms(600);
    cluster.must_put(b"k2", b"v2");
    assert_eq!(
        node1_addr,
        cluster.pd_client.get_store(node1_id).unwrap().get_address()
    );
    assert_eq!(
        node3_addr,
        cluster.pd_client.get_store(node2_id).unwrap().get_address()
    );
    assert_eq!(
        node2_addr,
        cluster.pd_client.get_store(node3_id).unwrap().get_address()
    );
    must_get_equal(&cluster.get_engine(node3_id), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(node2_id), b"k2", b"v2");
    fail::remove("invalidate_addr_cache");
    fail::remove("mock_store_refresh_interval_secs");
}
