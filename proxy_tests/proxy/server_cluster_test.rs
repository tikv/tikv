// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(dead_code)]
#![allow(unused_variables)]

use std::sync::*;

use collections::HashMap;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use new_mock_engine_store::{
    server::{new_server_cluster, ServerCluster},
    *,
};
use tikv_util::HandyRwLock;
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
        let cluster = Box::new(new_server_cluster(1, count));
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

use std::{error::Error, net::SocketAddr, sync::Arc};

use hyper::{body, Client, StatusCode, Uri};
use proxy_server::status_server::StatusServer;
use security::SecurityConfig;
use tikv::config::ConfigController;

async fn check(authority: SocketAddr, region_id: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.to_string().as_str())
        .path_and_query("/debug/pprof/profile")
        .build()?;
    let resp = client.get(uri).await?;
    let (parts, raw_body) = resp.into_parts();
    let body = body::to_bytes(raw_body).await?;
    assert_eq!(
        StatusCode::OK,
        parts.status,
        "{}",
        String::from_utf8(body.to_vec())?
    );
    assert_eq!("image/svg+xml", parts.headers["content-type"].to_str()?);
    {
        let xmldata = body.as_ref();
        let xmlstr = std::str::from_utf8(xmldata);
        assert_eq!(xmlstr.is_ok(), true);
        let res = String::from(xmlstr.unwrap()).find("raftstore");
        assert_eq!(res.is_some(), true);
    }
    Ok(())
}

#[test]
fn test_pprof() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let region_id = region.get_id();
    let peer = region.get_peers().get(0);
    assert!(peer.is_some());
    let store_id = peer.unwrap().get_store_id();
    let router = cluster.sim.rl().get_router(store_id);
    assert!(router.is_some());
    let id = 1;
    let engine = cluster.get_engine(id);
    let mut lock = cluster.ffi_helper_set.lock().unwrap();
    let ffiset = lock.get_mut(&id).unwrap();
    let mut status_server = StatusServer::new(
        engine_store_ffi::gen_engine_store_server_helper(ffiset.engine_store_server_helper_ptr),
        1,
        ConfigController::default(),
        Arc::new(SecurityConfig::default()),
        router.unwrap(),
        std::env::temp_dir(),
    )
    .unwrap();
    let addr = format!("127.0.0.1:{}", test_util::alloc_port());
    status_server.start(addr).unwrap();
    let check_task = check(status_server.listening_addr(), region_id);
    let rt = tokio::runtime::Runtime::new().unwrap();
    if let Err(err) = rt.block_on(check_task) {
        panic!("{}", err);
    }
    status_server.stop();
}
