// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, path::Path, sync::Arc, thread::sleep, time::Duration};

use cloud_server::TiKVServer;
use futures::executor::block_on;
use grpcio::{Channel, ChannelBuilder, EnvBuilder, Environment};
use kvengine::dfs::InMemFS;
use kvproto::{
    kvrpcpb::{Mutation, Op},
    raft_cmdpb::RaftCmdRequest,
};
use pd_client::PdClient;
use rfstore::{store::Callback, RaftStoreRouter};
use security::SecurityManager;
use tempfile::TempDir;
use test_raftstore::{find_peer, TestPdClient};
use tikv::{config::TiKvConfig, import::SstImporter};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    thread_group::GroupProperties,
    time::Instant,
};

use crate::client::ClusterClient;

#[allow(dead_code)]
pub struct ServerCluster {
    // node_id -> server.
    servers: HashMap<u16, TiKVServer>,
    tmp_dir: TempDir,
    env: Arc<Environment>,
    pd_client: Arc<TestPdClient>,
    security_mgr: Arc<SecurityManager>,
    dfs: Arc<InMemFS>,
    channels: HashMap<u64, Channel>,
}

impl ServerCluster {
    // The node id is statically assigned, the temp dir and server address are calculated by
    // the node id.
    pub fn new<F>(nodes: Vec<u16>, update_conf: F) -> ServerCluster
    where
        F: Fn(u16, &mut TiKvConfig),
    {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        let mut cluster = Self {
            servers: HashMap::new(),
            tmp_dir: TempDir::new().unwrap(),
            env: Arc::new(EnvBuilder::new().cq_count(2).build()),
            pd_client: Arc::new(TestPdClient::new(1, false)),
            security_mgr: Arc::new(SecurityManager::new(&Default::default()).unwrap()),
            dfs: Arc::new(InMemFS::new()),
            channels: HashMap::new(),
        };
        for node_id in nodes {
            cluster.start_node(node_id, &update_conf);
        }
        cluster.wait_pd_region_count(1);
        cluster
    }

    pub fn start_node<F>(&mut self, node_id: u16, update_conf: F)
    where
        F: Fn(u16, &mut TiKvConfig),
    {
        let mut config = new_test_config(self.tmp_dir.path(), node_id);
        update_conf(node_id, &mut config);
        let mut server = TiKVServer::setup(
            config,
            self.security_mgr.clone(),
            self.env.clone(),
            self.pd_client.clone(),
            self.dfs.clone(),
        );
        server.run();
        let store_id = server.get_store_id();
        let addr = node_addr(node_id);
        let channel = ChannelBuilder::new(self.env.clone()).connect(&addr);
        self.channels.insert(store_id, channel);
        self.servers.insert(node_id, server);
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.channels.keys().copied().collect()
    }

    pub fn get_pd_client(&self) -> Arc<TestPdClient> {
        self.pd_client.clone()
    }

    pub fn get_nodes(&self) -> Vec<u16> {
        self.servers.keys().copied().collect()
    }

    pub fn stop(&mut self) {
        let nodes = self.get_nodes();
        for node_id in nodes {
            self.stop_node(node_id);
        }
    }
    pub fn stop_node(&mut self, node_id: u16) {
        if let Some(node) = self.servers.remove(&node_id) {
            let store_id = node.get_store_id();
            self.channels.remove(&store_id);
            node.stop();
        }
    }

    pub fn get_kvengine(&self, node_id: u16) -> kvengine::Engine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_kv_engine()
    }

    pub fn get_rfengine(&self, node_id: u16) -> rfengine::RfEngine {
        let server = self.servers.get(&node_id).unwrap();
        server.get_raft_engine()
    }

    pub fn get_snap(&self, node_id: u16, key: &[u8]) -> kvengine::SnapAccess {
        let engine = self.get_kvengine(node_id);
        let region = self.pd_client.get_region(key).unwrap();
        engine.get_snap_access(region.id).unwrap()
    }

    pub fn get_sst_importer(&self, node_id: u16) -> Arc<SstImporter> {
        let server = self.servers.get(&node_id).unwrap();
        server.get_sst_importer()
    }

    pub fn send_raft_command(&self, cmd: RaftCmdRequest) {
        let store_id = cmd.get_header().get_peer().get_store_id();
        for (_, server) in &self.servers {
            if server.get_store_id() == store_id {
                server.get_raft_router().send_command(cmd, Callback::None);
                return;
            }
        }
    }

    pub fn wait_region_replicated(&self, key: &[u8], replica_cnt: usize) {
        for _ in 0..10 {
            let region_info = self.pd_client.get_region_info(key).unwrap();
            let region_id = region_info.id;
            let region_ver = region_info.get_region_epoch().version;
            if region_info.region.get_peers().len() >= replica_cnt {
                let all_applied_snapshot = region_info.get_peers().iter().all(|peer| {
                    let node_id = self.get_server_node_id(peer.store_id);
                    let kv = self.get_kvengine(node_id);
                    kv.get_shard_with_ver(region_id, region_ver).is_ok()
                });
                if all_applied_snapshot {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        panic!("region is not replicated");
    }

    pub fn wait_pd_region_count(&self, count: usize) {
        for _ in 0..10 {
            if self.pd_client.get_regions_number() == count {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("pd region count not match");
    }

    pub fn remove_node_peers(&mut self, node_id: u16) {
        let server = self.servers.get(&node_id).unwrap();
        let store_id = server.get_store_id();
        let all_id_vers = server.get_kv_engine().get_all_shard_id_vers();
        for id_ver in &all_id_vers {
            let (region, leader) = block_on(self.pd_client.get_region_leader_by_id(id_ver.id))
                .unwrap()
                .unwrap();
            if leader.store_id == store_id {
                let target = region
                    .get_peers()
                    .iter()
                    .find(|x| x.store_id != store_id)
                    .unwrap();
                self.pd_client
                    .transfer_leader(region.id, target.clone(), vec![]);
                self.pd_client
                    .region_leader_must_be(region.id, target.clone());
            }
            if let Some(peer) = find_peer(&region, store_id) {
                self.pd_client.must_remove_peer(region.id, peer.clone());
            }
        }
        let server = self.servers.get(&node_id).unwrap();
        for _ in 0..30 {
            if server.get_kv_engine().get_all_shard_id_vers().is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("kvengine is not empty");
    }

    fn get_server_node_id(&self, store_id: u64) -> u16 {
        for (node_id, server) in &self.servers {
            if server.get_store_id() == store_id {
                return *node_id;
            }
        }
        panic!("server not found");
    }

    pub fn new_client(&self) -> ClusterClient {
        ClusterClient {
            pd_client: self.pd_client.clone(),
            channels: self.channels.clone(),
            region_ranges: Default::default(),
            regions: Default::default(),
        }
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
    config.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    config.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(20);
    config.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    config.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    config.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    config.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(100);
    config.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    config.rocksdb.writecf.block_size = ReadableSize::kb(4);
    config.rocksdb.writecf.target_file_size_base = ReadableSize::kb(32);
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

pub fn must_wait<F>(f: F, seconds: usize, fail_msg: &str)
where
    F: Fn() -> bool,
{
    let begin = Instant::now_coarse();
    let timeout = Duration::from_secs(seconds as u64);
    while begin.saturating_elapsed() < timeout {
        if f() {
            return;
        }
        sleep(Duration::from_millis(100))
    }
    panic!("{}", fail_msg);
}
