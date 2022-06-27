// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, ops::Range, path::Path, sync::Arc, time::Duration};

use cloud_server::TiKVServer;
use futures::executor::block_on;
use grpcio::{Channel, ChannelBuilder, EnvBuilder, Environment};
use kvproto::{
    kvrpcpb::{CommitRequest, Context, Mutation, Op, PrewriteRequest, SplitRegionRequest},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use security::SecurityManager;
use tempfile::TempDir;
use test_raftstore::TestPdClient;
use tikv::{config::TiKvConfig, import::SstImporter, storage::mvcc::TimeStamp};
use tikv_util::{thread_group::GroupProperties, time::Instant};

// Retry if encounter error
macro_rules! retry_req {
    ($call_req: expr, $check_resp: expr, $resp:ident, $retry:literal, $timeout:literal) => {
        let start = Instant::now();
        let timeout = Duration::from_millis($timeout);
        let mut tried_times = 0;
        while tried_times < $retry || start.saturating_elapsed() < timeout {
            if $check_resp {
                break;
            } else {
                std::thread::sleep(Duration::from_millis(200));
                tried_times += 1;
                $resp = $call_req;
                continue;
            }
        }
    };
}

#[allow(dead_code)]
pub struct ServerCluster {
    // node_id -> server.
    servers: HashMap<u16, TiKVServer>,
    tmp_dir: TempDir,
    env: Arc<Environment>,
    pd_client: Arc<TestPdClient>,
    channels: HashMap<u64, Channel>,
}

impl ServerCluster {
    // The node id is statically assigned, the temp dir and server address are calculated by
    // the node id.
    pub fn new<F>(nodes: Vec<u16>, update_conf: F) -> ServerCluster
    where
        F: Fn(u16, &mut TiKvConfig),
    {
        let tmp_dir = TempDir::new().unwrap();
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        let env = Arc::new(EnvBuilder::new().cq_count(2).build());
        let pd_client = Arc::new(TestPdClient::new(1, false));
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        let mut servers = HashMap::new();
        let mut channels = HashMap::new();
        for node_id in nodes {
            let mut config = new_test_config(tmp_dir.path(), node_id);
            update_conf(node_id, &mut config);
            let mut server =
                TiKVServer::setup(config, security_mgr.clone(), env.clone(), pd_client.clone());
            server.run();
            let store_id = server.get_store_id();
            let addr = node_addr(node_id);
            let channel = ChannelBuilder::new(env.clone()).connect(&addr);
            channels.insert(store_id, channel);
            servers.insert(node_id, server);
        }
        Self {
            servers,
            tmp_dir,
            env,
            pd_client,
            channels,
        }
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.channels.keys().copied().collect()
    }

    pub fn get_pd_client(&self) -> Arc<TestPdClient> {
        self.pd_client.clone()
    }

    pub fn get_kv_client(&self, store_id: u64) -> TikvClient {
        TikvClient::new(self.get_client_channel(store_id))
    }

    pub fn get_client_channel(&self, store_id: u64) -> Channel {
        self.channels.get(&store_id).unwrap().clone()
    }

    pub fn new_rpc_context(&self, key: &[u8]) -> Context {
        let region_info = self.pd_client.get_region_info(key).unwrap();
        let mut ctx = Context::new();
        ctx.set_region_id(region_info.get_id());
        ctx.set_region_epoch(region_info.get_region_epoch().clone());
        ctx.set_peer(region_info.leader.unwrap());
        ctx
    }

    pub fn stop(&mut self) {
        for (_, server) in self.servers.drain() {
            server.stop();
        }
    }

    pub fn get_ts(&self) -> TimeStamp {
        block_on(self.pd_client.get_tso()).unwrap()
    }

    pub fn kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
        let ctx = self.new_rpc_context(&pk);
        let kv_client = self.get_kv_client(ctx.get_peer().get_store_id());

        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(ctx);
        prewrite_req.set_mutations(muts.into());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = 3000;
        prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
        let mut prewrite_resp = kv_client.kv_prewrite(&prewrite_req).unwrap();
        retry_req!(
            kv_client.kv_prewrite(&prewrite_req).unwrap(),
            !prewrite_resp.has_region_error() && prewrite_resp.errors.is_empty(),
            prewrite_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    pub fn kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let ctx = self.new_rpc_context(keys.first().unwrap());
        let kv_client = self.get_kv_client(ctx.get_peer().get_store_id());

        let mut commit_req = CommitRequest::default();
        commit_req.set_context(ctx);
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into());
        commit_req.commit_version = commit_ts.into_inner();
        let mut commit_resp = kv_client.kv_commit(&commit_req).unwrap();
        retry_req!(
            kv_client.kv_commit(&commit_req).unwrap(),
            !commit_resp.has_region_error() && !commit_resp.has_error(),
            commit_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    pub fn put_kv<F, G>(&self, rng: Range<usize>, gen_key: F, gen_val: G)
    where
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        let start_key = gen_key(rng.start);
        let start_ts = self.get_ts();

        let mut mutations = vec![];
        for i in rng.clone() {
            let mut m = Mutation::default();
            m.set_op(Op::Put);
            m.set_key(gen_key(i));
            m.set_value(gen_val(i));
            mutations.push(m)
        }
        let keys = mutations.iter().map(|m| m.get_key().to_vec()).collect();
        self.kv_prewrite(mutations, start_key, start_ts);
        let commit_ts = self.get_ts();
        self.kv_commit(keys, start_ts, commit_ts);
    }

    pub fn get_kvengine(&self, node_id: u16) -> kvengine::Engine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_kv_engine()
    }

    pub fn get_snap(&self, node_id: u16, key: &[u8]) -> kvengine::SnapAccess {
        let engine = self.get_kvengine(node_id);
        let ctx = self.new_rpc_context(key);
        engine.get_snap_access(ctx.region_id).unwrap()
    }

    pub fn get_sst_importer(&self, node_id: u16) -> Arc<SstImporter> {
        let server = self.servers.get(&node_id).unwrap();
        server.get_sst_importer()
    }

    pub fn split(&self, key: &[u8]) {
        let ctx = self.new_rpc_context(key);
        let client = self.get_kv_client(ctx.get_peer().get_store_id());
        let mut split_req = SplitRegionRequest::default();
        split_req.set_context(ctx);
        split_req.set_split_key(key.to_vec());
        let resp = client.split_region(&split_req).unwrap();
        assert!(!resp.has_region_error());
    }

    pub fn get_region_id(&self, key: &[u8]) -> u64 {
        let ctx = self.new_rpc_context(key);
        ctx.region_id
    }
}

pub fn new_test_config(base_dir: &Path, node_id: u16) -> TiKvConfig {
    let mut config = TiKvConfig::default();
    config.storage.data_dir = format!("{}/{}", base_dir.to_str().unwrap(), node_id);
    std::fs::create_dir_all(&config.storage.data_dir).unwrap();
    config.server.cluster_id = 1;
    config.server.addr = node_addr(node_id);
    config.server.status_addr = node_status_addr(node_id);
    config.dfs.s3_endpoint = "memory".to_string();
    config
}

fn node_addr(node_id: u16) -> String {
    format!("127.0.0.1:2{:04}", node_id)
}

fn node_status_addr(node_id: u16) -> String {
    format!("127.0.0.1:3{:04}", node_id)
}

pub fn put_mut(key: &str, val: &str) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.op = Op::Put;
    mutation.key = key.as_bytes().to_vec();
    mutation.value = val.as_bytes().to_vec();
    mutation
}
