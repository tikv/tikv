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

use std::path::Path;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;
use tikv::util::collections::{HashMap, HashSet};

use grpc::{EnvBuilder, Error as GrpcError};
use tempdir::TempDir;

use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::{self, RaftMessage};

use tikv::config::TiKvConfig;
use tikv::coprocessor;
use tikv::import::{ImportSSTService, SSTImporter};
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv::raftstore::store::{Callback, Engines, Msg as StoreMsg, SnapManager};
use tikv::raftstore::{store, Result};
use tikv::server::readpool::ReadPool;
use tikv::server::resolve::{self, Task as ResolveTask};
use tikv::server::transport::RaftStoreRouter;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::{
    create_raft_storage, Config, Error, Node, PdStoreAddrResolver, RaftClient, Server,
    ServerTransport,
};
use tikv::storage::{self, Engine};
use tikv::util::security::SecurityManager;
use tikv::util::transport::SendCh;
use tikv::util::worker::{FutureWorker, Worker};

use super::cluster::{Cluster, Simulator};
use super::pd::TestPdClient;
use super::transport_simulate::*;
use super::util::create_test_engine;

type SimulateStoreTransport = SimulateTransport<StoreMsg, ServerRaftStoreRouter>;
type SimulateServerTransport =
    SimulateTransport<RaftMessage, ServerTransport<SimulateStoreTransport, PdStoreAddrResolver>>;

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
    addrs: HashMap<u64, String>,
    pub storages: HashMap<u64, Box<Engine>>,
    snap_paths: HashMap<u64, TempDir>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient,
}

impl ServerCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> ServerCluster {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!("server-cluster"))
                .build(),
        );
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        ServerCluster {
            metas: HashMap::default(),
            addrs: HashMap::default(),
            pd_client,
            storages: HashMap::default(),
            snap_paths: HashMap::default(),
            raft_client: RaftClient::new(env, Arc::new(Config::default()), security_mgr),
        }
    }

    pub fn get_addr(&self, node_id: u64) -> &str {
        &self.addrs[&node_id]
    }
}

impl Simulator for ServerCluster {
    #[cfg_attr(feature = "cargo-clippy", allow(useless_format))]
    fn run_node(
        &mut self,
        node_id: u64,
        mut cfg: TiKvConfig,
        engines: Option<Engines>,
    ) -> (u64, Engines, Option<TempDir>) {
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
            cfg.server.addr = addr.clone();
        }

        // Initialize raftstore channels.
        let mut event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
        let store_sendch = SendCh::new(event_loop.channel(), "raftstore");
        let (snap_status_sender, snap_status_receiver) = mpsc::channel();
        let raft_router = ServerRaftStoreRouter::new(store_sendch.clone(), snap_status_sender);
        let sim_router = SimulateTransport::new(raft_router);

        // Create engine
        let (engines, path) = create_test_engine(engines, store_sendch.clone(), &cfg);

        // Create storage.
        let pd_worker = FutureWorker::new("test future worker");
        let storage_read_pool =
            ReadPool::new("store-read", &cfg.readpool.storage.build_config(), || {
                || storage::ReadPoolContext::new(pd_worker.scheduler())
            });
        let mut store =
            create_raft_storage(sim_router.clone(), &cfg.storage, storage_read_pool).unwrap();
        store.start(&cfg.storage).unwrap();
        self.storages.insert(node_id, store.get_engine());

        // Create import service.
        let importer = {
            let dir = Path::new(engines.kv_engine.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir).unwrap())
        };
        let import_service = ImportSSTService::new(
            cfg.import.clone(),
            sim_router.clone(),
            Arc::clone(&engines.kv_engine),
            Arc::clone(&importer),
        );

        // Create pd client, snapshot manager, server.
        let (worker, resolver) = resolve::new_resolver(Arc::clone(&self.pd_client)).unwrap();
        let snap_mgr = SnapManager::new(tmp_str, Some(store_sendch));
        let pd_worker = FutureWorker::new("test-pd-worker");
        let server_cfg = Arc::new(cfg.server.clone());
        let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
        let cop_read_pool = ReadPool::new("cop", &cfg.readpool.coprocessor.build_config(), || {
            || coprocessor::ReadPoolContext::new(pd_worker.scheduler())
        });
        let mut server = None;
        for _ in 0..100 {
            server = Some(Server::new(
                &server_cfg,
                &security_mgr,
                cfg.coprocessor.region_split_size.0 as usize,
                store.clone(),
                cop_read_pool.clone(),
                sim_router.clone(),
                resolver.clone(),
                snap_mgr.clone(),
                Some(engines.clone()),
                Some(import_service.clone()),
            ));
            match server {
                Some(Ok(_)) => break,
                Some(Err(Error::Grpc(GrpcError::BindFail(ref addr, ref port)))) => {
                    // Servers may meet the error, when we restart them.
                    debug!("fail to create a server: bind fail {:?}", (addr, port));
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Some(Err(ref e)) => panic!("fail to create a server: {:?}", e),
                None => unreachable!(),
            }
        }
        let mut server = server.unwrap().unwrap();
        let addr = server.listening_addr();
        cfg.server.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans.clone());
        let server_cfg = Arc::new(cfg.server.clone());

        // Create node.
        let mut node = Node::new(
            &mut event_loop,
            &cfg.server,
            &cfg.raft_store,
            Arc::clone(&self.pd_client),
        );

        // Create coprocessor.
        let coprocessor_host = CoprocessorHost::new(cfg.coprocessor, node.get_sendch());

        node.start(
            event_loop,
            engines.clone(),
            simulate_trans.clone(),
            snap_mgr.clone(),
            snap_status_receiver,
            pd_worker,
            coprocessor_host,
            importer,
        ).unwrap();
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }

        server.start(server_cfg, security_mgr).unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                store_ch: node.get_sendch(),
                node,
                server,
                router: sim_router,
                sim_trans: simulate_trans,
                worker,
            },
        );
        self.addrs.insert(node_id, format!("{}", addr));

        (node_id, engines, path)
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

    fn async_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback,
    ) -> Result<()> {
        let router = match self.metas.get(&node_id) {
            None => return Err(box_err!("missing sender for store {}", node_id)),
            Some(meta) => meta.router.clone(),
        };
        router.send_command(request, cb)
    }

    fn send_raft_msg(&mut self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_to_peer().get_store_id();
        let addr = self.get_addr(store_id).to_owned();
        self.raft_client.send(store_id, &addr, raft_msg).unwrap();
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
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client)
}
