// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use tempfile::{Builder, TempDir};

use kvproto::metapb;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::{self, RaftMessage};
use raft::eraftpb::MessageType;
use raft::SnapshotStatus;

use super::*;
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{Engines, MiscExt, Peekable};
use raftstore::coprocessor::config::SplitCheckConfigManager;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::errors::Error as RaftError;
use raftstore::router::{LocalReadRouter, RaftStoreRouter, ServerRaftStoreRouter};
use raftstore::store::config::RaftstoreConfigManager;
use raftstore::store::fsm::store::StoreMeta;
use raftstore::store::fsm::{RaftBatchSystem, RaftRouter};
use raftstore::store::SnapManagerBuilder;
use raftstore::store::*;
use raftstore::Result;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv::import::SSTImporter;
use tikv::server::Node;
use tikv::server::Result as ServerResult;
use tikv_util::config::VersionTrack;
use tikv_util::time::ThreadReadId;
use tikv_util::worker::{Builder as WorkerBuilder, FutureWorker};

pub struct ChannelTransportCore {
    snap_paths: HashMap<u64, (SnapManager, TempDir)>,
    routers: HashMap<u64, SimulateTransport<ServerRaftStoreRouter<RocksEngine, RocksEngine>>>,
}

#[derive(Clone)]
pub struct ChannelTransport {
    core: Arc<Mutex<ChannelTransportCore>>,
}

impl ChannelTransport {
    pub fn new() -> ChannelTransport {
        ChannelTransport {
            core: Arc::new(Mutex::new(ChannelTransportCore {
                snap_paths: HashMap::default(),
                routers: HashMap::default(),
            })),
        }
    }
}

impl Transport for ChannelTransport {
    fn send(&mut self, msg: RaftMessage) -> Result<()> {
        let from_store = msg.get_from_peer().get_store_id();
        let to_store = msg.get_to_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let region_id = msg.get_region_id();
        let is_snapshot = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;

        if is_snapshot {
            let snap = msg.get_message().get_snapshot();
            let key = SnapKey::from_snap(snap).unwrap();
            let from = match self.core.lock().unwrap().snap_paths.get(&from_store) {
                Some(p) => {
                    p.0.register(key.clone(), SnapEntry::Sending);
                    p.0.get_snapshot_for_sending(&key).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", from_store)),
            };
            let to = match self.core.lock().unwrap().snap_paths.get(&to_store) {
                Some(p) => {
                    p.0.register(key.clone(), SnapEntry::Receiving);
                    let data = msg.get_message().get_snapshot().get_data();
                    p.0.get_snapshot_for_receiving(&key, data).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", to_store)),
            };

            defer!({
                let core = self.core.lock().unwrap();
                core.snap_paths[&from_store]
                    .0
                    .deregister(&key, &SnapEntry::Sending);
                core.snap_paths[&to_store]
                    .0
                    .deregister(&key, &SnapEntry::Receiving);
            });

            copy_snapshot(from, to)?;
        }

        let core = self.core.lock().unwrap();

        match core.routers.get(&to_store) {
            Some(h) => {
                h.send_raft_msg(msg)?;
                if is_snapshot {
                    // should report snapshot finish.
                    let _ = core.routers[&from_store].report_snapshot_status(
                        region_id,
                        to_peer_id,
                        SnapshotStatus::Finish,
                    );
                }
                Ok(())
            }
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }

    fn need_flush(&self) -> bool {
        false
    }

    fn flush(&mut self) {}
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    trans: ChannelTransport,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, Node<TestPdClient, RocksEngine>>,
    simulate_trans: HashMap<u64, SimulateChannelTransport>,
    #[allow(clippy::type_complexity)]
    post_create_coprocessor_host: Option<Box<dyn Fn(u64, &mut CoprocessorHost<RocksEngine>)>>,
}

impl NodeCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> NodeCluster {
        NodeCluster {
            trans: ChannelTransport::new(),
            pd_client,
            nodes: HashMap::default(),
            simulate_trans: HashMap::default(),
            post_create_coprocessor_host: None,
        }
    }
}

impl NodeCluster {
    #[allow(dead_code)]
    pub fn get_node_router(
        &self,
        node_id: u64,
    ) -> SimulateTransport<ServerRaftStoreRouter<RocksEngine, RocksEngine>> {
        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap()
    }

    // Set a function that will be invoked after creating each CoprocessorHost. The first argument
    // of `op` is the node_id.
    // Set this before invoking `run_node`.
    #[allow(clippy::type_complexity)]
    pub fn post_create_coprocessor_host(
        &mut self,
        op: Box<dyn Fn(u64, &mut CoprocessorHost<RocksEngine>)>,
    ) {
        self.post_create_coprocessor_host = Some(op)
    }

    pub fn get_node(&mut self, node_id: u64) -> Option<&mut Node<TestPdClient, RocksEngine>> {
        self.nodes.get_mut(&node_id)
    }
}

impl Simulator for NodeCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: TiKvConfig,
        engines: Engines<RocksEngine, RocksEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<RocksEngine, RocksEngine>,
        system: RaftBatchSystem<RocksEngine, RocksEngine>,
    ) -> ServerResult<u64> {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));
        let pd_worker = FutureWorker::new("test-pd-worker");

        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let mut raft_store = cfg.raft_store.clone();
        raft_store.validate().unwrap();
        let bg_worker = WorkerBuilder::new("background").thread_count(2).create();
        let mut node = Node::new(
            system,
            &cfg.server,
            Arc::new(VersionTrack::new(raft_store)),
            Arc::clone(&self.pd_client),
            Arc::default(),
            bg_worker.clone(),
        );

        let (snap_mgr, snap_mgr_path) = if node_id == 0
            || !self
                .trans
                .core
                .lock()
                .unwrap()
                .snap_paths
                .contains_key(&node_id)
        {
            let tmp = Builder::new().prefix("test_cluster").tempdir().unwrap();
            let snap_mgr = SnapManagerBuilder::default()
                .encryption_key_manager(key_manager)
                .build(tmp.path().to_str().unwrap());
            (snap_mgr, Some(tmp))
        } else {
            let trans = self.trans.core.lock().unwrap();
            let &(ref snap_mgr, _) = &trans.snap_paths[&node_id];
            (snap_mgr.clone(), None)
        };

        // Create coprocessor.
        let mut coprocessor_host = CoprocessorHost::new(router.clone());

        if let Some(f) = self.post_create_coprocessor_host.as_ref() {
            f(node_id, &mut coprocessor_host);
        }

        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir, None).unwrap())
        };

        let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
        let cfg_controller = ConfigController::new(cfg.clone());

        let split_check_runner = SplitCheckRunner::new(
            engines.kv.clone(),
            router.clone(),
            coprocessor_host.clone(),
            cfg.coprocessor.clone(),
        );
        let split_scheduler = bg_worker.start("test-split-check", split_check_runner);
        cfg_controller.register(
            Module::Coprocessor,
            Box::new(SplitCheckConfigManager(split_scheduler.clone())),
        );

        let mut raftstore_cfg = cfg.raft_store;
        raftstore_cfg.validate().unwrap();
        let raft_store = Arc::new(VersionTrack::new(raftstore_cfg));
        cfg_controller.register(
            Module::Raftstore,
            Box::new(RaftstoreConfigManager(raft_store)),
        );

        node.start(
            engines.clone(),
            simulate_trans.clone(),
            snap_mgr.clone(),
            pd_worker,
            store_meta,
            coprocessor_host,
            importer,
            split_scheduler,
            AutoSplitController::default(),
            ConcurrencyManager::new(1.into()),
        )?;
        assert!(engines
            .kv
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_none());
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        debug!(
            "node_id: {} tmp: {:?}",
            node_id,
            snap_mgr_path
                .as_ref()
                .map(|p| p.path().to_str().unwrap().to_owned())
        );
        if let Some(tmp) = snap_mgr_path {
            self.trans
                .core
                .lock()
                .unwrap()
                .snap_paths
                .insert(node_id, (snap_mgr, tmp));
        }

        let router = ServerRaftStoreRouter::new(router, local_reader);
        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .insert(node_id, SimulateTransport::new(router));
        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, simulate_trans);

        Ok(node_id)
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.trans.core.lock().unwrap().snap_paths[&node_id]
            .1
            .path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut node) = self.nodes.remove(&node_id) {
            node.stop();
        }
        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .remove(&node_id)
            .unwrap();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn async_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
    ) -> Result<()> {
        if !self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            return Err(box_err!("missing sender for store {}", node_id));
        }

        let router = self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap();
        router.send_command(request, cb)
    }

    fn async_read(
        &self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
    ) {
        if !self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            let mut resp = RaftCmdResponse::default();
            let e: RaftError = box_err!("missing sender for store {}", node_id);
            resp.mut_header().set_error(e.into());
            cb.invoke_with_response(resp);
            return;
        }
        let mut guard = self.trans.core.lock().unwrap();
        let router = guard.routers.get_mut(&node_id).unwrap();
        router.read(batch_id, request, cb).unwrap();
    }

    fn send_raft_msg(&mut self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.trans.send(msg)
    }

    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .add_filter(filter);
    }

    fn clear_send_filters(&mut self, node_id: u64) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        let mut trans = self.trans.core.lock().unwrap();
        trans.routers.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        let mut trans = self.trans.core.lock().unwrap();
        trans.routers.get_mut(&node_id).unwrap().clear_filters();
    }

    fn get_router(&self, node_id: u64) -> Option<RaftRouter<RocksEngine, RocksEngine>> {
        self.nodes.get(&node_id).map(|node| node.get_router())
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client)
}

pub fn new_incompatible_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client)
}
