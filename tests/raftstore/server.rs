// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{mpsc, Arc, RwLock};
use std::time::Duration;
use std::boxed::FnBox;

use grpc::Environment;
use rocksdb::DB;
use tempdir::TempDir;

use super::cluster::{Cluster, Simulator};
use tikv::config::TiKvConfig;
use tikv::server::{Server, ServerTransport};
use tikv::server::{create_raft_storage, Config, Node, PdStoreAddrResolver, RaftClient};
use tikv::server::resolve::{self, Task as ResolveTask};
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::transport::RaftStoreRouter;
use tikv::raftstore::{store, Error, Result};
use tikv::raftstore::store::{Msg as StoreMsg, SnapManager};
use tikv::util::transport::SendCh;
use tikv::util::worker::Worker;
use tikv::storage::{CfName, Engine, ALL_CFS};
use kvproto::raft_serverpb::{self, RaftMessage};
use kvproto::raft_cmdpb::*;

use super::pd::TestPdClient;
use super::transport_simulate::*;

type SimulateStoreTransport = SimulateTransport<StoreMsg, ServerRaftStoreRouter>;
type SimulateServerTransport = SimulateTransport<
    RaftMessage,
    ServerTransport<SimulateStoreTransport, PdStoreAddrResolver>,
>;

struct ServerMeta {
    node: Node<TestPdClient>,
    server: Server<SimulateStoreTransport, PdStoreAddrResolver>,
    router: SimulateStoreTransport,
    sim_trans: SimulateServerTransport,
    store_ch: SendCh<StoreMsg>,
    worker: Worker<ResolveTask>,
}

pub struct ServerCluster {
    metas: HashMap<u64, ServerMeta>,
    addrs: HashMap<u64, SocketAddr>,
    pub storages: HashMap<u64, Box<Engine>>,
    snap_paths: HashMap<u64, TempDir>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient,
}

impl ServerCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> ServerCluster {
        ServerCluster {
            metas: HashMap::new(),
            addrs: HashMap::new(),
            pd_client: pd_client,
            storages: HashMap::new(),
            snap_paths: HashMap::new(),
            raft_client: RaftClient::new(Arc::new(Environment::new(1)), Config::default()),
        }
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self, node_id: u64, mut cfg: TiKvConfig, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.metas.contains_key(&node_id));

        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = TempDir::new("test_cluster").unwrap();
            (p.path().to_str().unwrap().to_owned(), Some(p))
        } else {
            let p = self.snap_paths[&node_id].path().to_str().unwrap();
            (p.to_owned(), None)
        };

        // Now we cache the store address, so here we should re-use last
        // listening address for the same store.
        if let Some(addr) = self.addrs.get(&node_id) {
            cfg.server.addr = format!("{}", addr)
        }

        // Initialize raftstore channels.
        let mut event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
        let store_sendch = SendCh::new(event_loop.channel(), "raftstore");
        let raft_router = ServerRaftStoreRouter::new(store_sendch.clone());
        let sim_router = SimulateTransport::new(raft_router);
        let (snap_status_sender, snap_status_receiver) = mpsc::channel();

        // Create storage.
        let mut store =
            create_raft_storage(sim_router.clone(), engine.clone(), &cfg.storage).unwrap();
        store.start(&cfg.storage).unwrap();
        self.storages.insert(node_id, store.get_engine());

        // Create pd client, snapshot manager, server.
        let (worker, resolver) = resolve::new_resolver(self.pd_client.clone()).unwrap();
        let snap_mgr = SnapManager::new(
            tmp_str,
            Some(store_sendch),
            cfg.raft_store.use_sst_file_snapshot,
        );
        let mut server = Server::new(
            &cfg.server,
            cfg.raft_store.region_split_size.0 as usize,
            store.clone(),
            sim_router.clone(),
            snap_status_sender,
            resolver,
            snap_mgr.clone(),
        ).unwrap();
        let addr = server.listening_addr();
        cfg.server.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans.clone());

        // Create node.
        let mut node = Node::new(
            &mut event_loop,
            &cfg.server,
            &cfg.raft_store,
            self.pd_client.clone(),
        );
        node.start(
            event_loop,
            engine,
            simulate_trans.clone(),
            snap_mgr.clone(),
            snap_status_receiver,
        ).unwrap();
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }

        server.start(&cfg.server).unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                store_ch: node.get_sendch(),
                node: node,
                server: server,
                router: sim_router,
                sim_trans: simulate_trans,
                worker: worker,
            },
        );
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_paths[&node_id]
            .path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut meta) = self.metas.remove(&node_id) {
            meta.server.stop().unwrap();
            meta.node.stop().unwrap();
            meta.worker.stop().unwrap().join().unwrap();
        }
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.metas.keys().cloned().collect()
    }

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let router = match self.metas.get(&node_id) {
            None => return Err(box_err!("missing sender for store {}", node_id)),
            Some(meta) => meta.router.clone(),
        };
        wait_op!(
            |cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
                router.send_command(request, cb).unwrap()
            },
            timeout
        ).ok_or_else(|| {
            Error::Timeout(format!("request timeout for {:?}", timeout))
        })
    }

    fn send_raft_msg(&mut self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_to_peer().get_store_id();
        let addr = self.addrs[&store_id];
        self.raft_client.send(store_id, addr, raft_msg).unwrap();
        self.raft_client.flush();
        Ok(())
    }

    fn add_send_filter(&mut self, node_id: u64, filter: SendFilter) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_trans
            .add_filter(filter);
    }

    fn clear_send_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_trans
            .clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: RecvFilter) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .router
            .add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.metas.get_mut(&node_id).unwrap().router.clear_filters();
    }

    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh<StoreMsg>> {
        self.metas.get(&node_id).map(|m| m.store_ch.clone())
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    new_server_cluster_with_cfs(id, count, ALL_CFS)
}

pub fn new_server_cluster_with_cfs(
    id: u64,
    count: usize,
    cfs: &[CfName],
) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(ServerCluster::new(pd_client.clone())));
    Cluster::new(id, count, cfs, sim, pd_client)
}
