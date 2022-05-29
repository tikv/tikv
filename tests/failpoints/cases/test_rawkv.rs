// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::*;
use tikv_util::HandyRwLock;

struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub pd_client: Arc<TestPdClient>,
    api_version: ApiVersion,
}

impl TestSuite {
    pub fn new(count: usize, api_version: ApiVersion) -> Self {
        let mut cluster = new_server_cluster_with_api_ver(1, count, api_version);
        // Disable background renew by setting `renew_interval` to 0, to make timestamp allocation predictable.
        configure_for_causal_ts(&mut cluster, "0s", 100);
        configure_for_merge(&mut cluster);
        cluster.run();

        let pd_client = cluster.pd_client.clone();
        pd_client.disable_default_operator();

        Self {
            cluster,
            pd_client,
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
        let addr = self.cluster.sim.rl().get_addr(leader.store_id);
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
}

const FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP: &str = "causal_observer_flush_timestamp";

#[test]
fn test_leader_transfer() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let region_id = 1;
    let key1 = b"rk1";

    // Transfer leader to store 1.
    let peer1 = new_peer(1, 1);
    suite.cluster.must_transfer_leader(region_id, peer1.clone());
    let leader1 = suite.cluster.leader_of_region(region_id).unwrap();
    {
        suite.must_raw_put(key1, b"v1");
        suite.must_raw_put(key1, b"v2");
        suite.must_raw_put(key1, b"v3");
        suite.flush_timestamp(leader1.get_store_id()); // Flush to make ts bigger than other stores.
        suite.must_raw_put(key1, b"v4");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
    }

    // Disable CausalObserver::flush_timestamp to produce causality issue.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "return").unwrap();

    // Transfer leader to store 2.
    let peer2 = new_peer(2, 2);
    suite.cluster.must_transfer_leader(region_id, peer2.clone());
    let leader2 = suite.cluster.leader_of_region(region_id).unwrap();
    assert_ne!(leader1, leader2);
    {
        // Store 2 has a TSO batch smaller than store 1.
        suite.must_raw_put(key1, b"v5");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
        suite.must_raw_put(key1, b"v6");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
    }

    // Enable CausalObserver::flush_timestamp.
    fail::cfg(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP, "off").unwrap();
    {
        // Transfer leader again.
        suite.cluster.must_transfer_leader(region_id, peer1);
        assert_eq!(suite.cluster.leader_of_region(region_id).unwrap(), leader1);
        suite.cluster.must_transfer_leader(region_id, peer2);
        assert_eq!(suite.cluster.leader_of_region(region_id).unwrap(), leader2);

        suite.must_raw_put(key1, b"v7");
        assert_eq!(suite.must_raw_get(key1), Some(b"v7".to_vec()));
    }

    fail::remove(FP_CAUSAL_OBSERVER_FLUSH_TIMESTAMP);
    suite.stop();
}
