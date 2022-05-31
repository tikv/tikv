// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, path::Path, sync::Arc};

use cloud_server::TiKVServer;
use grpcio::{ChannelBuilder, EnvBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, Mutation, Op},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use security::SecurityManager;
use tempfile::TempDir;
use test_raftstore::TestPdClient;
use tikv::config::TiKvConfig;

#[allow(dead_code)]
pub struct ServerCluster {
    // node_id -> server.
    servers: HashMap<u8, TiKVServer>,
    tmp_dir: TempDir,
    env: Arc<Environment>,
    pd_client: Arc<TestPdClient>,
    kv_clients: HashMap<u64, TikvClient>,
}

impl ServerCluster {
    // The node id is statically assigned, the temp dir and server address are calculated by
    // the node id.
    pub fn new(nodes: Vec<u8>) -> ServerCluster {
        let tmp_dir = TempDir::new().unwrap();
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        let env = Arc::new(EnvBuilder::new().cq_count(2).build());
        let pd_client = Arc::new(TestPdClient::new(1, true));
        let mut servers = HashMap::new();
        let mut kv_clients = HashMap::new();
        for node_id in nodes {
            let config = new_test_config(tmp_dir.path(), node_id);
            let mut server =
                TiKVServer::setup(config, security_mgr.clone(), env.clone(), pd_client.clone());
            server.run();
            let store_id = server.get_store_id();
            let addr = node_addr(node_id);
            let channel = ChannelBuilder::new(env.clone()).connect(&addr);
            let kv_client = TikvClient::new(channel);
            kv_clients.insert(store_id, kv_client);
            servers.insert(node_id, server);
        }
        Self {
            servers,
            tmp_dir,
            env,
            pd_client,
            kv_clients,
        }
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.kv_clients.keys().copied().collect()
    }

    pub fn get_pd_client(&self) -> Arc<TestPdClient> {
        self.pd_client.clone()
    }

    pub fn get_kv_client(&self, store_id: u64) -> TikvClient {
        self.kv_clients.get(&store_id).unwrap().clone()
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
}

pub fn new_test_config(base_dir: &Path, node_id: u8) -> TiKvConfig {
    let mut config = TiKvConfig::default();
    config.storage.data_dir = format!("{}/{}", base_dir.to_str().unwrap(), node_id);
    config.server.cluster_id = 1;
    config.server.addr = node_addr(node_id);
    config.server.status_addr = node_status_addr(node_id);
    config.dfs.s3_endpoint = "memory".to_string();
    config
}

fn node_addr(node_id: u8) -> String {
    format!("127.0.0.1:20{:03}", node_id)
}

fn node_status_addr(node_id: u8) -> String {
    format!("127.0.0.1:21{:03}", node_id)
}

pub fn put_mut(key: &str, val: &str) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.op = Op::Put;
    mutation.key = key.as_bytes().to_vec();
    mutation.value = val.as_bytes().to_vec();
    mutation
}
