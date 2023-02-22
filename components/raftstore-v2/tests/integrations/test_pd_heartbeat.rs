// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{MiscExt, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use pd_client::PdClient;
use raftstore::coprocessor::Config as CopConfig;
use raftstore_v2::{
    router::{PeerMsg, PeerTick},
    SimpleWriteEncoder,
};
use tikv_util::{config::ReadableSize, store::new_peer};

use crate::cluster::Cluster;

#[test]
fn test_region_heartbeat() {
    let region_id = 2;
    let cluster = Cluster::with_node_count(1, None);
    let router = &cluster.routers[0];

    // When there is only one peer, it should campaign immediately.
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionLeader);
    let res = router.query(region_id, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    assert_eq!(
        *status_resp.get_region_leader().get_leader(),
        new_peer(1, 3)
    );

    for _ in 0..5 {
        let resp = block_on(
            cluster
                .node(0)
                .pd_client()
                .get_region_leader_by_id(region_id),
        )
        .unwrap();
        if let Some((region, peer)) = resp {
            assert_eq!(region.get_id(), region_id);
            assert_eq!(peer.get_id(), 3);
            assert_eq!(peer.get_store_id(), 1);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("failed to get region leader");
}

#[test]
fn test_store_heartbeat() {
    let cluster = Cluster::with_node_count(1, None);
    let store_id = cluster.node(0).id();
    for _ in 0..5 {
        let stats = block_on(cluster.node(0).pd_client().get_store_stats_async(store_id)).unwrap();
        if stats.get_start_time() > 0 {
            assert_ne!(stats.get_capacity(), 0);
            assert_ne!(stats.get_used_size(), 0);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("failed to get store stats");
}

#[test]
fn test_report_buckets() {
    let region_id = 2;
    let mut cop_cfg = CopConfig::default();
    cop_cfg.enable_region_bucket = Some(true);
    cop_cfg.region_bucket_size = ReadableSize::kb(1);
    let cluster = Cluster::with_cop_cfg(cop_cfg);
    let store_id = cluster.node(0).id();
    let router = &cluster.routers[0];

    // When there is only one peer, it should campaign immediately.
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(store_id, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionLeader);
    let res = router.query(region_id, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    assert_eq!(
        *status_resp.get_region_leader().get_leader(),
        new_peer(store_id, 3)
    );
    router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    // load data to split bucket.
    let header = Box::new(router.new_request_for(region_id).take_header());
    let mut suffix = String::from("");
    for _ in 0..200 {
        suffix.push_str("fake ");
    }
    for i in 0..10 {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        let mut key = format!("key-{}", i);
        key.push_str(&suffix);
        put.put(CF_DEFAULT, key.as_bytes(), b"value");
        let (msg, sub) = PeerMsg::simple_write(header.clone(), put.clone().encode());
        router.send(region_id, msg).unwrap();
        let _resp = block_on(sub.result()).unwrap();
    }
    // To find the split keys, it should flush memtable manually.
    let mut cached = cluster.node(0).tablet_registry().get(region_id).unwrap();
    cached.latest().unwrap().flush_cf(CF_DEFAULT, true).unwrap();
    // send split region check to split bucket.
    router
        .send(region_id, PeerMsg::Tick(PeerTick::SplitRegionCheck))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    // report buckets to pd.
    router
        .send(region_id, PeerMsg::Tick(PeerTick::ReportBuckets))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let resp = block_on(cluster.node(0).pd_client().get_buckets_by_id(region_id)).unwrap();
    let mut buckets_tmp = vec![];
    let mut bucket_ranges = vec![];
    if let Some(buckets) = resp {
        assert!(buckets.get_keys().len() > 2);
        assert_eq!(buckets.get_region_id(), region_id);
        for i in 0..buckets.keys.len() - 1 {
            buckets_tmp.push(raftstore::store::Bucket::default());
            let bucket_range =
                raftstore::store::BucketRange(buckets.keys[i].clone(), buckets.keys[i + 1].clone());
            bucket_ranges.push(bucket_range);
        }
    }

    // send the same region buckets to refresh which needs to merge the last.
    let resp = block_on(cluster.node(0).pd_client().get_region_by_id(region_id)).unwrap();
    if let Some(region) = resp {
        let region_epoch = region.get_region_epoch().clone();
        for _ in 0..2 {
            let msg = PeerMsg::RefreshRegionBuckets {
                region_epoch: region_epoch.clone(),
                buckets: buckets_tmp.clone(),
                bucket_ranges: Some(bucket_ranges.clone()),
            };
            router.send(region_id, msg).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}
