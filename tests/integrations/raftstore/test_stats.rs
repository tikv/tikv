// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::config::*;

fn check_available<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    let engine = cluster.get_engine(1);
    let raft_engine = cluster.get_raft_engine(1);

    let stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_region_count(), 2);

    let value = vec![0; 1024];
    for i in 0..1000 {
        let last_available = stats.get_available();
        cluster.must_put(format!("k{}", i).as_bytes(), &value);
        raft_engine.flush(true).unwrap();
        engine.flush(true).unwrap();
        sleep_ms(20);

        let stats = pd_client.get_store_stats(1).unwrap();
        // Because the available is for disk size, even we add data
        // other process may reduce data too. so here we try to
        // check available size changed.
        if stats.get_available() != last_available {
            return;
        }
    }

    panic!("available not changed")
}

fn test_simple_store_stats<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);

    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    // wait store reports stats.
    for _ in 0..100 {
        sleep_ms(20);

        if pd_client.get_store_stats(1).is_some() {
            break;
        }
    }

    let engine = cluster.get_engine(1);
    let raft_engine = cluster.get_raft_engine(1);
    raft_engine.flush(true).unwrap();
    engine.flush(true).unwrap();
    let last_stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(last_stats.get_region_count(), 1);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"").unwrap();
    cluster.must_split(&region, b"k2");
    raft_engine.flush(true).unwrap();
    engine.flush(true).unwrap();

    // wait report region count after split
    for _ in 0..100 {
        sleep_ms(20);

        let stats = pd_client.get_store_stats(1).unwrap();
        if stats.get_region_count() == 2 {
            break;
        }
    }

    let stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_region_count(), 2);

    check_available(cluster);
}

#[test]
fn test_node_simple_store_stats() {
    let mut cluster = new_node_cluster(0, 1);
    test_simple_store_stats(&mut cluster);
}

#[test]
fn test_store_heartbeat_report_hotspots() {
    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    let (mut cluster, client, _) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
    });
    let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
    let (k3, v3) = (b"k3".to_vec(), b"v3".to_vec());
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let left = cluster.get_region(b"k1");
    let mut left_ctx = Context::default();
    left_ctx.set_region_id(left.id);
    left_ctx.set_peer(left.get_peers()[0].clone());
    left_ctx.set_region_epoch(cluster.get_region_epoch(left.id));
    let right = cluster.get_region(b"k3");
    let mut right_ctx = Context::default();
    right_ctx.set_region_id(right.id);
    right_ctx.set_peer(right.get_peers()[0].clone());
    right_ctx.set_region_epoch(cluster.get_region_epoch(right.id));

    // raw get k1 100 times
    for _i in 0..100 {
        let mut get_req = RawGetRequest::default();
        get_req.set_context(left_ctx.clone());
        get_req.key = k1.clone();
        let get_resp = client.raw_get(&get_req).unwrap();
        assert_eq!(get_resp.value, v1);
    }
    // raw get k3 10 times
    for _i in 0..10 {
        let mut get_req = RawGetRequest::default();
        get_req.set_context(right_ctx.clone());
        get_req.key = k3.clone();
        let get_resp = client.raw_get(&get_req).unwrap();
        assert_eq!(get_resp.value, v3);
    }
    sleep_ms(50);
    let store_id = 1;
    let hot_peers = cluster.pd_client.get_store_hotspots(store_id).unwrap();
    let peer_stat = hot_peers.get(&left.id).unwrap();
    assert!(peer_stat.get_read_keys() > 0);
    assert!(peer_stat.get_read_bytes() > 0);
    fail::remove("mock_tick_interval");
    fail::remove("mock_hotspot_threshold");
}
