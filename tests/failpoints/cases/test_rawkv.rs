// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use engine_rocks::RocksEngine;
use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::*,
    metapb::{Peer, Region},
    tikvpb::TikvClient,
};
use test_raftstore::*;
use tikv_util::{time::Instant, HandyRwLock};

struct TestSuite {
    pub cluster: Cluster<RocksEngine, ServerCluster<RocksEngine>>,
    api_version: ApiVersion,
}

impl TestSuite {
    pub fn new(count: usize, api_version: ApiVersion) -> Self {
        let mut cluster = new_server_cluster_with_api_ver(1, count, api_version);
        // Disable background renew by setting `renew_interval` to 0, to make timestamp
        // allocation predictable.
        configure_for_causal_ts(&mut cluster, "0s", 100);
        configure_for_merge(&mut cluster.cfg);
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

    pub fn raw_put_err_by_timestamp_not_synced(&mut self, key: &[u8], value: &[u8]) {
        let region_id = self.cluster.get_region_id(key);
        let client = self.get_client(region_id);
        let ctx = self.get_context(region_id);

        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx);
        put_req.key = key.to_vec();
        put_req.value = value.to_vec();

        let put_resp = client.raw_put(&put_req).unwrap();
        assert!(put_resp.get_region_error().has_max_timestamp_not_synced());
        assert!(
            put_resp.get_error().is_empty(),
            "{:?}",
            put_resp.get_error()
        );
    }

    pub fn must_raw_get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let region_id = self.cluster.get_region_id(key);
        let client = self.get_client(region_id);
        let ctx = self.get_context(region_id);
        must_raw_get(&client, ctx, key.to_vec())
    }

    pub fn flush_timestamp(&mut self, node_id: u64) {
        block_on(
            self.cluster
                .sim
                .rl()
                .get_causal_ts_provider(node_id)
                .unwrap()
                .async_flush(),
        )
        .unwrap();
    }

    pub fn get_causal_ts_provider(&mut self, node_id: u64) -> Option<Arc<CausalTsProviderImpl>> {
        self.cluster.sim.rl().get_causal_ts_provider(node_id)
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
            if timer.saturating_elapsed() > Duration::from_secs(10) {
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

const FP_GET_TSO: &str = "test_raftstore_get_tso";

/// Verify correctness on leader transfer.
// TODO: simulate and test for the scenario of issue #12498.
#[test]
fn test_leader_transfer() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let key1 = b"rk1";
    let region = suite.cluster.get_region(key1);

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

    // Make causal_ts_provider.async_flush() & handle_update_max_timestamp fail.
    fail::cfg(FP_GET_TSO, "return(50)").unwrap();

    // Transfer leader and write to store 2.
    {
        suite.must_transfer_leader(&region, 2);
        suite.must_leader_on_store(key1, 2);

        // Store 2 has a TSO batch smaller than store 1.
        suite.raw_put_err_by_timestamp_not_synced(key1, b"v5");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
        suite.raw_put_err_by_timestamp_not_synced(key1, b"v6");
        assert_eq!(suite.must_raw_get(key1), Some(b"v4".to_vec()));
    }

    // Transfer leader back.
    suite.must_transfer_leader(&region, 1);
    suite.must_leader_on_store(key1, 1);
    // Make handle_update_max_timestamp succeed.
    fail::cfg(FP_GET_TSO, "off").unwrap();
    // Transfer leader and write to store 2 again.
    {
        suite.must_transfer_leader(&region, 2);
        suite.must_leader_on_store(key1, 2);

        suite.must_raw_put(key1, b"v7");
        assert_eq!(suite.must_raw_get(key1), Some(b"v7".to_vec()));
        suite.must_raw_put(key1, b"v8");
        assert_eq!(suite.must_raw_get(key1), Some(b"v8".to_vec()));
    }

    fail::remove(FP_GET_TSO);
    suite.stop();
}

/// Verify correctness on region merge.
/// See issue #12680.
#[test]
fn test_region_merge() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let keys = [b"rk0", b"rk1", b"rk2", b"rk3", b"rk4", b"rk5"];

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

    // Transfer leaders: region 1 -> store 1, region 3 -> store 2, region 5 -> store
    // 3.
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

    // Make causal_ts_provider.async_flush() & handle_update_max_timestamp fail.
    fail::cfg(FP_GET_TSO, "return(50)").unwrap();

    // Merge region 1 to 3.
    {
        suite.must_merge_region_by_key(keys[1], keys[3]);
        suite.must_leader_on_store(keys[1], 2);

        // Write to store 2. Store 2 has a TSO batch smaller than store 1.
        suite.raw_put_err_by_timestamp_not_synced(keys[1], b"v5");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v4".to_vec()));
        suite.raw_put_err_by_timestamp_not_synced(keys[1], b"v6");
        assert_eq!(suite.must_raw_get(keys[1]), Some(b"v4".to_vec()));
    }

    // Make handle_update_max_timestamp succeed.
    fail::cfg(FP_GET_TSO, "off").unwrap();

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

    fail::remove(FP_GET_TSO);
    suite.stop();
}

// Verify the raw key guard correctness in APIv2.
#[test]
fn test_raw_put_key_guard() {
    let mut suite = TestSuite::new(3, ApiVersion::V2);
    let pause_write_fp = "raftkv_async_write";

    let test_key = b"rk3".to_vec();
    let test_value = b"v3".to_vec();

    let region = suite.cluster.get_region(&test_key);
    let region_id = region.get_id();
    let client = suite.get_client(region_id);
    let ctx = suite.get_context(region_id);
    let node_id = region.get_peers()[0].get_id();
    let leader_cm = suite.cluster.sim.rl().get_concurrency_manager(node_id);
    let ts_provider = suite.get_causal_ts_provider(node_id).unwrap();
    let ts = block_on(ts_provider.async_get_ts()).unwrap();

    let copy_test_key = test_key.clone();
    let copy_test_value = test_value.clone();
    fail::cfg(pause_write_fp, "pause").unwrap();
    let handle = thread::spawn(move || {
        must_raw_put(&client, ctx, copy_test_key, copy_test_value);
    });

    // Wait for global_min_lock_ts.
    sleep_ms(500);
    let start = Instant::now();
    while leader_cm.global_min_lock_ts().is_none()
        && start.saturating_elapsed() < Duration::from_secs(5)
    {
        sleep_ms(200);
    }

    // Before raw_put finish, min_ts should be the ts of "key guard" of the raw_put
    // request.
    assert_eq!(suite.must_raw_get(&test_key), None);
    let min_ts = leader_cm.global_min_lock_ts();
    assert_eq!(min_ts.unwrap(), ts.next());

    fail::remove(pause_write_fp);
    handle.join().unwrap();

    // After raw_put is finished, "key guard" is released.
    assert_eq!(suite.must_raw_get(&test_key), Some(test_value));
    let min_ts = leader_cm.global_min_lock_ts();
    assert!(min_ts.is_none());
}
