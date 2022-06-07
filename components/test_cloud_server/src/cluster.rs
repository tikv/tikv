// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, ops::Range, path::Path, sync::Arc};

use cloud_server::TiKVServer;
use futures::executor::block_on;
use grpcio::{Channel, ChannelBuilder, EnvBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, Mutation, Op},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use security::SecurityManager;
use tempfile::TempDir;
use test_raftstore::TestPdClient;
use tikv::{config::TiKvConfig, storage::mvcc::TimeStamp};

#[allow(dead_code)]
pub struct ServerCluster {
    // node_id -> server.
    servers: HashMap<u8, TiKVServer>,
    tmp_dir: TempDir,
    env: Arc<Environment>,
    pd_client: Arc<TestPdClient>,
    channels: HashMap<u64, Channel>,
}

impl ServerCluster {
    // The node id is statically assigned, the temp dir and server address are calculated by
    // the node id.
    pub fn new<F>(nodes: Vec<u8>, update_conf: F) -> ServerCluster
    where
        F: Fn(u8, &mut TiKvConfig),
    {
        let tmp_dir = TempDir::new().unwrap();
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        let env = Arc::new(EnvBuilder::new().cq_count(2).build());
        let pd_client = Arc::new(TestPdClient::new(1, true));
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

    pub fn put_kv<F, G>(&self, rng: Range<usize>, gen_key: F, gen_val: G)
    where
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        let start_key = gen_key(rng.start);
        let ctx = self.new_rpc_context(&start_key);
        let kv_client = self.get_kv_client(ctx.get_peer().get_store_id());
        let start_ts = self.get_ts().into_inner();

        let mut mutations = vec![];
        for i in rng.clone() {
            let mut m = Mutation::default();
            m.set_op(Op::Put);
            m.set_key(gen_key(i));
            m.set_value(gen_val(i));
            mutations.push(m)
        }
        test_raftstore::must_kv_prewrite(&kv_client, ctx.clone(), mutations, start_key, start_ts);
        let mut keys = vec![];
        for i in rng {
            keys.push(gen_key(i))
        }
        let commit_ts = self.get_ts().into_inner();
        test_raftstore::must_kv_commit(&kv_client, ctx, keys, start_ts, commit_ts, commit_ts);
    }

    pub fn get_kvengine(&self, node_id: u8) -> kvengine::Engine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_kv_engine()
    }

    pub fn get_snap(&self, node_id: u8, key: &[u8]) -> kvengine::SnapAccess {
        let engine = self.get_kvengine(node_id);
        let ctx = self.new_rpc_context(key);
        engine.get_snap_access(ctx.region_id).unwrap()
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
