// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::*,
    metapb::{Peer, Region},
    tikvpb::TikvClient,
};
use test_raftstore::*;
use tikv_util::{time::Instant, HandyRwLock};

struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    api_version: ApiVersion,
}

impl TestSuite {
    pub fn new(count: usize, api_version: ApiVersion) -> Self {
        let mut cluster = new_server_cluster_with_api_ver(1, count, api_version);
        // Disable background renew by setting `renew_interval` to 0, to make timestamp allocation predictable.
        configure_for_causal_ts(&mut cluster, "0s", 100);
        configure_for_merge(&mut cluster);
        cluster.run();
        cluster.pd_client.disable_default_operator();

        Self {
            cluster,
            api_version,
        }
    }

    pub fn stop(mut self) {
        self.cluster.shutdown();
    }

    pub fn get_context(&mut self, region_id: u64) -> Context {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let epoch = self.cluster.get_region_epoch(region_id);

        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);
        ctx.set_api_version(self.api_version);
        ctx
    }

    pub fn get_client(&mut self, region_id: u64) -> TikvClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let addr = self.cluster.sim.rl().get_addr(leader.get_store_id());
        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env).connect(&addr);
        TikvClient::new(channel)
    }

    pub fn must_raw_put(&mut self, key: &[u8], value: &[u8]) {
        let region_id = self.cluster.get_region_id(key);
        let client = self.get_client(region_id);
        let ctx = self.get_context(region_id);
        must_raw_put(&client, ctx, key.to_vec(), value.to_vec())
    }

    pub fn must_raw_get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let region_id = self.cluster.get_region_id(key);
        let client = self.get_client(region_id);
        let ctx = self.get_context(region_id);
        must_raw_get(&client, ctx, key.to_vec())
    }

    pub fn flush_timestamp(&mut self, node_id: u64) {
        self.cluster
            .sim
            .rl()
            .get_causal_ts_provider(node_id)
            .unwrap()
            .flush()
            .unwrap();
    }

    pub fn must_merge_region_by_key(&mut self, source_key: &[u8], target_key: &[u8]) {
        let source = self.cluster.get_region(source_key);
        let target = self.cluster.get_region(target_key);
        assert_ne!(source.get_id(), target.get_id());

        self.cluster
            .must_try_merge(source.get_id(), target.get_id());

        let mut merged;
        let timer = Instant::now();
        loop {
            if timer.saturating_elapsed() > Duration::from_secs(5) {
                panic!("region merge failed");
            }
            merged = self.cluster.get_region(source_key);
            if merged.get_id() == target.get_id() {
                break;
            }
            sleep_ms(100);
        }

        assert_eq!(merged.get_start_key(), source.get_start_key());
        assert_eq!(merged.get_end_key(), target.get_end_key())
    }

    pub fn must_transfer_leader(&mut self, region: &Region, store_id: u64) {
        self.cluster
            .must_transfer_leader(region.get_id(), peer_on_store(region, store_id));
    }

    pub fn must_leader_on_store(&mut self, key: &[u8], store_id: u64) -> Peer {
        let region_id = self.cluster.get_region_id(key);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        assert_eq!(leader.get_store_id(), store_id);
        leader
    }
}

const FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP: &str = "causal_observer_flush_timestamp";

/// Verify correctness on leader transfer.
// TODO: simulate and test for the scenario of issue #12498.
#[test]
fn test_leader_transfer() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let key1 = b"rk1";
    let region = suite.cluster.get_region(key1);

    // Disable CausalObserver::flush_timestamp to produce causality issue.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "return").unwrap();

    // Transfer leader and write to store 1.
    {
        suite.must_transfer_leader(&region, 1);
        let leader1 = suite.must_leader_on_store(key1, 1);

        suite.must_raw_put(key1, b"v1");
        suite.must_raw_put(key1, b"v2");
        suite.must_raw_put(key1, b"v3");
        suite.flush_timestamp(leader1.get_store_id()); // Flush to make ts bigger than other stores.
        suite.must_raw_put(key1, b"v4");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
    }

    // Transfer leader and write to store 2.
    {
        suite.must_transfer_leader(&region, 2);
        suite.must_leader_on_store(key1, 2);

        // Store 2 has a TSO batch smaller than store 1.
        suite.must_raw_put(key1, b"v5");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
        suite.must_raw_put(key1, b"v6");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
    }

    // Transfer leader back.
    suite.must_transfer_leader(&region, 1);
    suite.must_leader_on_store(key1, 1);
    // Enable CausalObserver::flush_timestamp.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "off").unwrap();
    // Transfer leader and write to store 2 again.
    {
        suite.must_transfer_leader(&region, 2);
        suite.must_leader_on_store(key1, 2);

        suite.must_raw_put(key1, b"v7");
        assert_eq!(suite.must_raw_get(key1), Some(b"v7".to_vec()));
        suite.must_raw_put(key1, b"v8");
        assert_eq!(suite.must_raw_get(key1), Some(b"v8".to_vec()));
    }

    fail::remove(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP);
    suite.stop();
}

/// Verify correctness on region merge.
/// See issue #12680.
#[test]
fn test_region_merge() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let keys = vec![b"rk0", b"rk1", b"rk2", b"rk3", b"rk4", b"rk5"];

    suite.must_raw_put(keys[1], b"v1");
    suite.must_raw_put(keys[3], b"v3");
    suite.must_raw_put(keys[5], b"v5");

    // Split to: region1: (-, 2), region3: [2, 4), region5: [4, +)
    let region1 = suite.cluster.get_region(keys[1]);
    suite.cluster.must_split(&region1, keys[2]);
    let region1 = suite.cluster.get_region(keys[1]);
    let region3 = suite.cluster.get_region(keys[3]);
    suite.cluster.must_split(&region3, keys[4]);
    let region3 = suite.cluster.get_region(keys[3]);
    let region5 = suite.cluster.get_region(keys[5]);
    assert_eq!(region1.get_end_key(), region3.get_start_key());
    assert_eq!(region3.get_end_key(), region5.get_start_key());

    // Disable CausalObserver::flush_timestamp to produce causality issue.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "return").unwrap();

    // Transfer leaders: region 1 -> store 1, region 3 -> store 2, region 5 -> store 3.
    suite.must_transfer_leader(&region1, 1);
    suite.must_transfer_leader(&region3, 2);
    suite.must_transfer_leader(&region5, 3);

    // Write to region 1.
    {
        let leader1 = suite.must_leader_on_store(keys[1], 1);

        suite.must_raw_put(keys[1], b"v2");
        suite.must_raw_put(keys[1], b"v3");
        suite.flush_timestamp(leader1.get_store_id()); // Flush to make ts of store 1 larger than others.
        suite.must_raw_put(keys[1], b"v4");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v4".to_vec()));
    }

    // Merge region 1 to 3.
    {
        suite.must_merge_region_by_key(keys[1], keys[3]);
        suite.must_leader_on_store(keys[1], 2);

        // Write to store 2. Store 2 has a TSO batch smaller than store 1.
        suite.must_raw_put(keys[1], b"v5");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v4".to_vec()));
        suite.must_raw_put(keys[1], b"v6");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v4".to_vec()));
    }

    // Enable CausalObserver::flush_timestamp.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "off").unwrap();

    // Merge region 3 to 5.
    {
        suite.must_merge_region_by_key(keys[3], keys[5]);
        suite.must_leader_on_store(keys[1], 3);

        // Write to store 3.
        suite.must_raw_put(keys[1], b"v7");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v7".to_vec()));
        suite.must_raw_put(keys[1], b"v8");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v8".to_vec()));
    }

    fail::remove(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP);
    suite.stop();
}
