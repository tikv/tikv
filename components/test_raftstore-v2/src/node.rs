// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::{KvEngine, RaftEngine, RaftEngineReadOnly, TabletRegistry};
use futures::Future;
use kvproto::{
    kvrpcpb::ApiVersion,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use raft::{prelude::MessageType, SnapshotStatus};
use raftstore::{
    coprocessor::CoprocessorHost,
    errors::Error as RaftError,
    store::{
        AutoSplitController, GlobalReplicationState, RegionSnapshot, SplitConfigManager,
        TabletSnapKey, TabletSnapManager, Transport,
    },
    Result,
};
use raftstore_v2::{
    router::{PeerMsg, RaftRouter},
    StateStorage, StoreMeta, StoreRouter,
};
use resource_control::ResourceGroupManager;
use resource_metering::CollectorRegHandle;
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{Config, Filter};
use tikv::{
    config::{ConfigController, Module},
    import::SstImporter,
    server::{
        raftkv::ReplicaReadLockChecker, tablet_snap::copy_tablet_snapshot, NodeV2,
        Result as ServerResult,
    },
};
use tikv_util::{
    box_err,
    config::VersionTrack,
    worker::{Builder as WorkerBuilder, LazyWorker},
};

use crate::{Cluster, RaftStoreRouter, SimulateTransport, Simulator, SnapshotRouter};

#[derive(Clone)]
pub struct ChannelTransport<EK: KvEngine> {
    core: Arc<Mutex<ChannelTransportCore<EK>>>,
}

impl<EK: KvEngine> ChannelTransport<EK> {
    pub fn new() -> Self {
        ChannelTransport {
            core: Arc::new(Mutex::new(ChannelTransportCore {
                snap_paths: HashMap::default(),
                routers: HashMap::default(),
            })),
        }
    }

    pub fn core(&self) -> &Arc<Mutex<ChannelTransportCore<EK>>> {
        &self.core
    }
}

impl<EK: KvEngine> Transport for ChannelTransport<EK> {
    fn send(&mut self, msg: RaftMessage) -> raftstore::Result<()> {
        let from_store = msg.get_from_peer().get_store_id();
        let to_store = msg.get_to_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let region_id = msg.get_region_id();
        let is_snapshot = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;

        if is_snapshot {
            let snap = msg.get_message().get_snapshot();
            let key = TabletSnapKey::from_region_snap(
                msg.get_region_id(),
                msg.get_to_peer().get_id(),
                snap,
            );
            let sender_snap_mgr = match self.core.lock().unwrap().snap_paths.get(&from_store) {
                Some(snap_mgr) => snap_mgr.0.clone(),
                None => return Err(box_err!("missing snap manager for store {}", from_store)),
            };
            let recver_snap_mgr = match self.core.lock().unwrap().snap_paths.get(&to_store) {
                Some(snap_mgr) => snap_mgr.0.clone(),
                None => return Err(box_err!("missing snap manager for store {}", to_store)),
            };

            if let Err(e) =
                copy_tablet_snapshot(key, msg.clone(), &sender_snap_mgr, &recver_snap_mgr)
            {
                return Err(box_err!("copy tablet snapshot failed: {:?}", e));
            }
        }

        let core = self.core.lock().unwrap();
        match core.routers.get(&to_store) {
            Some(h) => {
                h.send_raft_msg(msg)?;
                if is_snapshot {
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

    fn set_store_allowlist(&mut self, _allowlist: Vec<u64>) {
        unimplemented!();
    }

    fn need_flush(&self) -> bool {
        false
    }

    fn flush(&mut self) {}
}

pub struct ChannelTransportCore<EK: KvEngine> {
    pub snap_paths: HashMap<u64, (TabletSnapManager, TempDir)>,
    pub routers: HashMap<u64, SimulateTransport<RaftRouter<EK, RaftTestEngine>>>,
}

impl<EK: KvEngine> Default for ChannelTransport<EK> {
    fn default() -> Self {
        Self::new()
    }
}

type SimulateChannelTransport<EK> = SimulateTransport<ChannelTransport<EK>>;

pub struct NodeCluster<EK: KvEngine> {
    trans: ChannelTransport<EK>,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, NodeV2<TestPdClient, EK, RaftTestEngine>>,
    simulate_trans: HashMap<u64, SimulateChannelTransport<EK>>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,
    snap_mgrs: HashMap<u64, TabletSnapManager>,
}

impl<EK: KvEngine> NodeCluster<EK> {
    pub fn new(pd_client: Arc<TestPdClient>) -> Self {
        NodeCluster {
            trans: ChannelTransport::new(),
            pd_client,
            nodes: HashMap::default(),
            simulate_trans: HashMap::default(),
            concurrency_managers: HashMap::default(),
            snap_mgrs: HashMap::default(),
        }
    }

    pub fn get_concurrency_manager(&self, node_id: u64) -> ConcurrencyManager {
        self.concurrency_managers.get(&node_id).unwrap().clone()
    }
}

impl<EK: KvEngine> Simulator<EK> for NodeCluster<EK> {
    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
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

    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        key_manager: Option<Arc<DataKeyManager>>,
        raft_engine: RaftTestEngine,
        tablet_registry: TabletRegistry<EK>,
        _resource_manager: &Option<Arc<ResourceGroupManager>>,
    ) -> ServerResult<u64> {
        assert!(!self.nodes.contains_key(&node_id));
        let pd_worker = LazyWorker::new("test-pd-worker");

        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let mut raft_store = cfg.raft_store.clone();
        raft_store.optimize_for(true);
        raft_store
            .validate(
                cfg.coprocessor.region_split_size(),
                cfg.coprocessor.enable_region_bucket(),
                cfg.coprocessor.region_bucket_size,
            )
            .unwrap();

        let mut node = NodeV2::new(&cfg.server, self.pd_client.clone(), None);
        node.try_bootstrap_store(&raft_store, &raft_engine).unwrap();
        assert_eq!(node.id(), node_id);

        tablet_registry
            .tablet_factory()
            .set_state_storage(Arc::new(StateStorage::new(
                raft_engine.clone(),
                node.router().clone(),
            )));

        // todo: node id 0
        let (snap_mgr, snap_mgs_path) = if node_id == 0
            || !self
                .trans
                .core
                .lock()
                .unwrap()
                .snap_paths
                .contains_key(&node_id)
        {
            let tmp = test_util::temp_dir("test_cluster", cfg.prefer_mem);
            let snap_path = tmp.path().to_str().unwrap().to_owned();
            (
                TabletSnapManager::new(snap_path, key_manager.clone())?,
                Some(tmp),
            )
        } else {
            let trans = self.trans.core.lock().unwrap();
            let &(ref snap_mgr, _) = &trans.snap_paths[&node_id];
            (snap_mgr.clone(), None)
        };
        self.snap_mgrs.insert(node_id, snap_mgr.clone());

        let raft_router = RaftRouter::new_with_store_meta(node.router().clone(), store_meta);
        // Create coprocessor.
        let mut coprocessor_host =
            CoprocessorHost::new(raft_router.store_router().clone(), cfg.coprocessor.clone());

        // if let Some(f) = self.post_create_coprocessor_host.as_ref() {
        //     f(node_id, &mut coprocessor_host);
        // }

        let cm = ConcurrencyManager::new(1.into());
        self.concurrency_managers.insert(node_id, cm.clone());

        ReplicaReadLockChecker::new(cm.clone()).register(&mut coprocessor_host);

        let cfg_controller = ConfigController::new(cfg.tikv.clone());
        // cfg_controller.register(
        //     Module::Coprocessor,
        //     Box::new(SplitCheckConfigManager(split_scheduler.clone())),
        // );

        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(cfg.tikv.split.clone())));
        cfg_controller.register(Module::Split, Box::new(split_config_manager.clone()));

        let auto_split_controller = AutoSplitController::new(
            split_config_manager,
            cfg.tikv.server.grpc_concurrency,
            cfg.tikv.readpool.unified.max_thread_count,
            // todo: Is None sufficient for test?
            None,
        );
        let importer = {
            let dir = Path::new(raft_engine.get_engine_path()).join("../import-sst");
            Arc::new(
                SstImporter::new(
                    &cfg.import,
                    dir,
                    key_manager.clone(),
                    cfg.storage.api_version(),
                )
                .unwrap(),
            )
        };

        let bg_worker = WorkerBuilder::new("background").thread_count(2).create();
        let state: Arc<Mutex<GlobalReplicationState>> = Arc::default();
        node.start(
            raft_engine.clone(),
            tablet_registry,
            &raft_router,
            simulate_trans.clone(),
            snap_mgr.clone(),
            cm,
            None,
            coprocessor_host,
            auto_split_controller,
            CollectorRegHandle::new_for_test(),
            bg_worker,
            pd_worker,
            Arc::new(VersionTrack::new(raft_store)),
            &state,
            importer,
            key_manager,
        )?;
        assert!(
            raft_engine
                .get_prepare_bootstrap_region()
                .unwrap()
                .is_none()
        );
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();

        let region_split_size = cfg.coprocessor.region_split_size();
        let enable_region_bucket = cfg.coprocessor.enable_region_bucket();
        let region_bucket_size = cfg.coprocessor.region_bucket_size;
        let mut raftstore_cfg = cfg.tikv.raft_store;
        raftstore_cfg.optimize_for(true);
        raftstore_cfg
            .validate(region_split_size, enable_region_bucket, region_bucket_size)
            .unwrap();

        // let raft_store = Arc::new(VersionTrack::new(raftstore_cfg));
        // cfg_controller.register(
        //     Module::Raftstore,
        //     Box::new(RaftstoreConfigManager::new(
        //         node.refresh_config_scheduler(),
        //         raft_store,
        //     )),
        // );

        if let Some(tmp) = snap_mgs_path {
            self.trans
                .core
                .lock()
                .unwrap()
                .snap_paths
                .insert(node_id, (snap_mgr, tmp));
        }

        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .insert(node_id, SimulateTransport::new(raft_router));

        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, simulate_trans);
        Ok(node_id)
    }

    fn async_snapshot(
        &mut self,
        node_id: u64,
        request: RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>>
    + Send
    + 'static {
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
            // return async move {Err(resp)};
        }

        let mut router = {
            let mut guard = self.trans.core.lock().unwrap();
            guard.routers.get_mut(&node_id).unwrap().clone()
        };

        router.snapshot(request)
    }

    fn async_peer_msg_on_node(&self, node_id: u64, region_id: u64, msg: PeerMsg) -> Result<()> {
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

        router.send_peer_msg(region_id, msg)
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

    fn get_router(&self, node_id: u64) -> Option<StoreRouter<EK, RaftTestEngine>> {
        self.nodes.get(&node_id).map(|node| node.router().clone())
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.trans.core.lock().unwrap().snap_paths[&node_id]
            .0
            .root_path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn get_snap_mgr(&self, node_id: u64) -> &TabletSnapManager {
        self.snap_mgrs.get(&node_id).unwrap()
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        let mut trans = self.trans.core.lock().unwrap();
        trans.routers.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        let mut trans = self.trans.core.lock().unwrap();
        trans.routers.get_mut(&node_id).unwrap().clear_filters();
    }

    fn send_raft_msg(&mut self, msg: RaftMessage) -> Result<()> {
        self.trans.send(msg)
    }
}

// Compare to server cluster, node cluster does not have server layer and
// storage layer.
pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster<RocksEngine>, RocksEngine> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(
        id,
        count,
        sim,
        pd_client,
        ApiVersion::V1,
        Box::new(&crate::create_test_engine),
    )
}

// This cluster does not support batch split, we expect it to transfer the
// `BatchSplit` request to `split` request
pub fn new_incompatible_node_cluster(
    id: u64,
    count: usize,
) -> Cluster<NodeCluster<RocksEngine>, RocksEngine> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    Cluster::new(
        id,
        count,
        sim,
        pd_client,
        ApiVersion::V1,
        Box::new(&crate::create_test_engine),
    )
}
