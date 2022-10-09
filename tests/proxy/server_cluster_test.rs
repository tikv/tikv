// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksSnapshot;
use grpcio::{ChannelBuilder, ClientUnaryReceiver, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use new_mock_engine_store::{
    mock_cluster::{get_global_engine_helper_set, sleep_ms},
    server::{new_server_cluster, ServerCluster},
    *,
};
use online_config::ConfigValue;
use raftstore::coprocessor::CoprocessorHost;
use tikv::config::ResolvedTsConfig;
use tikv_util::{worker::LazyWorker, HandyRwLock};
use txn_types::TimeStamp;
static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

pub struct TestSuite {
    pub cluster: Box<Cluster<ServerCluster>>,
    tikv_cli: HashMap<u64, TikvClient>,

    env: Arc<Environment>,
}

impl TestSuite {
    pub fn new(count: usize) -> Self {
        let mut cluster = Box::new(new_server_cluster(1, count));
        Self::with_cluster(count, cluster)
    }

    pub fn with_cluster(count: usize, mut cluster: Box<Cluster<ServerCluster>>) -> Self {
        init();
        let pd_cli = cluster.pd_client.clone();

        cluster.run();

        TestSuite {
            cluster,
            tikv_cli: HashMap::default(),
            env: Arc::new(Environment::new(1)),
        }
    }

    pub fn stop(mut self) {
        self.cluster.shutdown();
    }

    fn get_client_from_store_id(&mut self, store_id: u64) -> &TikvClient {
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.tikv_cli.entry(store_id).or_insert_with(|| {
            let channel = ChannelBuilder::new(env).connect(&addr);
            TikvClient::new(channel)
        })
    }

    pub fn must_check_leader(
        &mut self,
        region_id: u64,
        resolved_ts: TimeStamp,
        applied_index: u64,
        store_id: u64,
    ) {
        let mut leader_info = LeaderInfo::default();
        leader_info.set_region_id(region_id);
        let mut read_state = ReadState::default();
        read_state.set_applied_index(applied_index);
        read_state.set_safe_ts(resolved_ts.into_inner());
        leader_info.set_read_state(read_state);

        let mut req = CheckLeaderRequest::default();
        let regions = vec![leader_info];
        req.set_regions(regions.into());
        req.set_ts(resolved_ts.into_inner());

        let _check_leader_resp = self
            .get_client_from_store_id(store_id)
            .check_leader(&req)
            .unwrap();
    }
}

#[test]
fn test_safe_ts_basic() {
    let mut suite = TestSuite::new(1);
    let physical_time = 646454654654;
    suite
        .cluster
        .set_expected_safe_ts(physical_time, physical_time);
    suite.must_check_leader(1, TimeStamp::compose(physical_time, 10), 1, 1);

    suite.stop();
}
