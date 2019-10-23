// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{thread, usize};

use grpcio::{EnvBuilder, Error as GrpcError};
use kvproto::debugpb::create_debug;
use kvproto::import_sstpb::create_import_sst;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use tempfile::{Builder, TempDir};

use engine::Engines;
use tikv::config::TiKvConfig;
use tikv::coprocessor;
use tikv::import::{ImportSSTService, SSTImporter};
use tikv::raftstore::coprocessor::{CoprocessorHost, RegionInfoAccessor};
use tikv::raftstore::store::fsm::{RaftBatchSystem, RaftRouter};
use tikv::raftstore::store::{Callback, LocalReader, SnapManager};
use tikv::raftstore::Result;
use tikv::server::load_statistics::ThreadLoad;
use tikv::server::resolve::{self, Task as ResolveTask};
use tikv::server::service::DebugService;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::transport::{RaftStoreBlackHole, RaftStoreRouter};
use tikv::server::Result as ServerResult;
use tikv::server::{
    create_raft_storage, Config, Error, Node, PdStoreAddrResolver, RaftClient, RaftKv, Server,
    ServerTransport,
};

use tikv::storage;
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::security::SecurityManager;
use tikv_util::worker::{FutureWorker, Worker};

use super::*;
use tikv::raftstore::store::fsm::store::{StoreMeta, PENDING_VOTES_CAP};
use tikv::server::gc_worker::GCWorker;

type SimulateStoreTransport = SimulateTransport<ServerRaftStoreRouter>;
type SimulateServerTransport =
    SimulateTransport<ServerTransport<SimulateStoreTransport, PdStoreAddrResolver>>;

pub type SimulateEngine = RaftKv<SimulateStoreTransport>;

struct ServerMeta {
    node: Node<TestPdClient>,
    server: Server<SimulateStoreTransport, PdStoreAddrResolver>,
    sim_router: SimulateStoreTransport,
    sim_trans: SimulateServerTransport,
    raw_router: RaftRouter,
    worker: Worker<ResolveTask>,
}

pub struct ServerCluster {
    metas: HashMap<u64, ServerMeta>,
    addrs: HashMap<u64, String>,
    pub storages: HashMap<u64, SimulateEngine>,
    pub region_info_accessors: HashMap<u64, RegionInfoAccessor>,
    pub importers: HashMap<u64, Arc<SSTImporter>>,
    snap_paths: HashMap<u64, TempDir>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient<RaftStoreBlackHole>,
    _stats_pool: tokio_threadpool::ThreadPool,
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
        let stats_pool = tokio_threadpool::Builder::new().pool_size(1).build();
        let raft_client = RaftClient::new(
            env,
            Arc::new(Config::default()),
            security_mgr,
            RaftStoreBlackHole,
            Arc::new(ThreadLoad::with_threshold(usize::MAX)),
            stats_pool.sender().clone(),
        );
        ServerCluster {
            metas: HashMap::default(),
            addrs: HashMap::default(),
            pd_client,
            storages: HashMap::default(),
            region_info_accessors: HashMap::default(),
            importers: HashMap::default(),
            snap_paths: HashMap::default(),
            raft_client,
            _stats_pool: stats_pool,
        }
    }

    pub fn get_addr(&self, node_id: u64) -> &str {
        &self.addrs[&node_id]
    }
}

impl Simulator for ServerCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        mut cfg: TiKvConfig,
        engines: Engines,
        router: RaftRouter,
        system: RaftBatchSystem,
    ) -> ServerResult<u64> {
        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = Builder::new().prefix("test_cluster").tempdir().unwrap();
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

        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
        let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
        let raft_router = ServerRaftStoreRouter::new(router.clone(), local_reader);
        let sim_router = SimulateTransport::new(raft_router.clone());

        let raft_engine = RaftKv::new(sim_router.clone());

        // Create storage.
        let pd_worker = FutureWorker::new("test-pd-worker");
        let storage_read_pool = storage::readpool_impl::build_read_pool_for_test(
            &tikv::config::StorageReadPoolConfig::default_for_test(),
            raft_engine.clone(),
        );

        let engine = RaftKv::new(sim_router.clone());

        let mut gc_worker =
            GCWorker::new(engine.clone(), None, None, cfg.storage.gc_ratio_threshold);
        gc_worker.start().unwrap();

        let store = create_raft_storage(engine, &cfg.storage, storage_read_pool, None)?;
        self.storages.insert(node_id, raft_engine);

        // Create import service.
        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir).unwrap())
        };
        let import_service = ImportSSTService::new(
            cfg.import.clone(),
            sim_router.clone(),
            Arc::clone(&engines.kv),
            Arc::clone(&importer),
        );
        // Create Debug service.
        let debug_service = DebugService::new(engines.clone(), raft_router.clone());

        // Create pd client, snapshot manager, server.
        let (worker, resolver) = resolve::new_resolver(Arc::clone(&self.pd_client)).unwrap();
        let snap_mgr = SnapManager::new(tmp_str, Some(router.clone()));
        let server_cfg = Arc::new(cfg.server.clone());
        let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
        let cop_read_pool = coprocessor::readpool_impl::build_read_pool_for_test(
            &tikv::config::CoprReadPoolConfig::default_for_test(),
            store.get_engine(),
        );
        let cop = coprocessor::Endpoint::new(&server_cfg, cop_read_pool);
        let mut server = None;
        for _ in 0..100 {
            let mut svr = Server::new(
                &server_cfg,
                &security_mgr,
                store.clone(),
                cop.clone(),
                sim_router.clone(),
                resolver.clone(),
                snap_mgr.clone(),
                gc_worker.clone(),
            )
            .unwrap();
            svr.register_service(create_import_sst(import_service.clone()));
            svr.register_service(create_debug(debug_service.clone()));
            match svr.build_and_bind() {
                Ok(_) => {
                    server = Some(svr);
                    break;
                }
                Err(Error::Grpc(GrpcError::BindFail(ref addr, ref port))) => {
                    // Servers may meet the error, when we restart them.
                    debug!("fail to create a server: bind fail {:?}", (addr, port));
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(ref e) => panic!("fail to create a server: {:?}", e),
            }
        }
        let mut server = server.unwrap();
        let addr = server.listening_addr();
        cfg.server.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans.clone());
        let server_cfg = Arc::new(cfg.server.clone());

        // Create node.
        let mut node = Node::new(
            system,
            &cfg.server,
            &cfg.raft_store,
            Arc::clone(&self.pd_client),
        );

        // Create coprocessor.
        let mut coprocessor_host = CoprocessorHost::new(cfg.coprocessor, router.clone());

        let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);
        region_info_accessor.start();

        node.start(
            engines.clone(),
            simulate_trans.clone(),
            snap_mgr.clone(),
            pd_worker,
            store_meta,
            coprocessor_host,
            importer.clone(),
        )?;
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }
        self.region_info_accessors
            .insert(node_id, region_info_accessor);
        self.importers.insert(node_id, importer);
        server.start(server_cfg, security_mgr).unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                raw_router: router,
                node,
                server,
                sim_router,
                sim_trans: simulate_trans,
                worker,
            },
        );
        self.addrs.insert(node_id, format!("{}", addr));

        Ok(node_id)
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
            meta.node.stop();
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
            Some(meta) => meta.sim_router.clone(),
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

    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
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

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_router
            .add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_router
            .clear_filters();
    }

    fn get_router(&self, node_id: u64) -> Option<RaftRouter> {
        self.metas.get(&node_id).map(|m| m.raw_router.clone())
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client)
}

pub fn new_incompatible_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client)
}
