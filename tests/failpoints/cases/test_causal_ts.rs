// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::*;
use tikv_util::HandyRwLock;

fn new_context(cluster: &mut Cluster<ServerCluster>) -> Context {
    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);

    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);
    ctx.set_api_version(ApiVersion::V2);
    ctx
}

fn new_client(cluster: &mut Cluster<ServerCluster>) -> TikvClient {
    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let addr = cluster.sim.rl().get_addr(leader.store_id);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    TikvClient::new(channel)
}

fn must_raw_put(cluster: &mut Cluster<ServerCluster>, key: &[u8], value: &[u8]) {
    let client = new_client(cluster);
    let ctx = new_context(cluster);

    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = key.to_vec();
    put_req.value = value.to_vec();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error(), "{:?}", put_resp.region_error);
    assert!(put_resp.error.is_empty(), "error: {}", put_resp.error);
}

fn must_raw_get(cluster: &mut Cluster<ServerCluster>, key: &[u8]) -> Option<Vec<u8>> {
    let client = new_client(cluster);
    let ctx = new_context(cluster);

    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx);
    get_req.key = key.to_vec();
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error(), "{:?}", get_resp.region_error);
    assert!(get_resp.error.is_empty(), "{}", get_resp.error);
    if get_resp.not_found {
        None
    } else {
        Some(get_resp.value)
    }
}

#[test]
fn test_causal_ts_issue_12498() {
    // https://github.com/tikv/tikv/issues/12498
    let fp_causal_ts_background_renew_interval = "causal_ts_background_renew_interval";
    let fp_causal_ts_batch_size = "causal_ts_batch_size";
    let fp_causal_ts_before_on_role_change = "causal_ts_before_on_role_change";
    let fp_causal_ts_region_is_leader = "causal_ts_region_is_leader";
    let fp_causal_ts_flush_in_pre_propose = "causal_ts_flush_in_pre_propose";

    // Disable background renew.
    fail::cfg(fp_causal_ts_background_renew_interval, "return(0)").unwrap();
    // Make TSO batch easy to be used up.
    fail::cfg(fp_causal_ts_batch_size, "return(3)").unwrap();
    // Skip "on_role_change" & set "region_is_leader = false" to simulate scenario in issue that the leader transfer is observed late.
    fail::cfg(fp_causal_ts_before_on_role_change, "return").unwrap();
    fail::cfg(fp_causal_ts_region_is_leader, "return(false)").unwrap();
    // Skip the codes for bug fix to reproduce issue.
    fail::cfg(fp_causal_ts_flush_in_pre_propose, "return").unwrap();

    let mut cluster = new_server_cluster_with_api_ver(0, 3, ApiVersion::V2);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let region_id = 1;
    let key1 = b"rk1";

    // Transfer leader to store 1.
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    let origin_leader = cluster.leader_of_region(region_id).unwrap();
    {
        must_raw_put(&mut cluster, key1, b"v1");
        must_raw_put(&mut cluster, key1, b"v2");
        must_raw_put(&mut cluster, key1, b"v3"); // TSO batch used up
        must_raw_put(&mut cluster, key1, b"v4"); // Renew and get a ts bigger than other stores.
        assert_eq!(must_raw_get(&mut cluster, key1), Some(b"v4".to_vec()));
    }

    // Transfer leader to store 2.
    cluster.must_transfer_leader(region_id, new_peer(2, 2));
    let new_leader = cluster.leader_of_region(region_id).unwrap();
    assert_ne!(origin_leader, new_leader);
    {
        // Store 2 has a TSO batch smaller than store 1.
        must_raw_put(&mut cluster, key1, b"v5");
        // Reproduce the issue. It's causality violation.
        assert_eq!(must_raw_get(&mut cluster, key1), Some(b"v4".to_vec()));
        must_raw_put(&mut cluster, key1, b"v6");
        assert_eq!(must_raw_get(&mut cluster, key1), Some(b"v4".to_vec()));
    }

    {
        // Enable the codes for bug fix.
        // A flush will be invoked to maintain causality.
        fail::cfg(fp_causal_ts_flush_in_pre_propose, "off").unwrap();
        must_raw_put(&mut cluster, key1, b"v7");
        assert_eq!(must_raw_get(&mut cluster, key1), Some(b"v7".to_vec()));
    }

    fail::remove(fp_causal_ts_background_renew_interval);
    fail::remove(fp_causal_ts_batch_size);
    fail::remove(fp_causal_ts_before_on_role_change);
    fail::remove(fp_causal_ts_region_is_leader);
    fail::remove(fp_causal_ts_flush_in_pre_propose);
}
