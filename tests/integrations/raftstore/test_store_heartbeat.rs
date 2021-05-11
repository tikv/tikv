// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::*;
use test_raftstore::*;
use tikv_util::config::*;

#[test]
fn test_store_heartbeat_report_hotspots() {
    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    let (cluster, client, ctx) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(100);
    });
    let (k, v) = (b"key".to_vec(), b"v2".to_vec());

    // Raw put
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v.clone();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());
    for _i in 0..100 {
        // Raw get
        let mut get_req = RawGetRequest::default();
        get_req.set_context(ctx.clone());
        get_req.key = k.clone();
        let get_resp = client.raw_get(&get_req).unwrap();
        assert_eq!(get_resp.value, v);
        sleep_ms(10);
    }
    let region_id = cluster.get_region_id(b"");
    let store_id = 1;
    let hot_peers = cluster.pd_client.get_store_hotspots(store_id).unwrap();
    let peer_stat = hot_peers.get(&region_id).unwrap();
    assert_eq!(peer_stat.get_region_id(), region_id);
    assert!(peer_stat.get_read_keys() > 0);
    assert!(peer_stat.get_read_bytes() > 0);
    fail::remove("mock_hotspot_threshold");
}
