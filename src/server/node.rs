// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::RaftKv;
use super::Result;
use crate::import::SSTImporter;
use crate::read_pool::ReadPoolHandle;
use crate::server::lock_manager::LockManager;
use crate::server::Config as ServerConfig;
use crate::storage::{config::Config as StorageConfig, Storage};
use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use engine_traits::{Engines, KvEngine, Peekable, RaftEngine};
use kvproto::metapb;
use kvproto::raft_serverpb::StoreIdent;
use kvproto::replication_modepb::ReplicationStatus;
use pd_client::{Error as PdError, PdClient, INVALID_ID};
use raftstore::coprocessor::dispatcher::CoprocessorHost;
use raftstore::router::{LocalReadRouter, RaftStoreRouter};
use raftstore::store::fsm::store::StoreMeta;
use raftstore::store::fsm::{ApplyRouter, RaftBatchSystem, RaftRouter};
use raftstore::store::AutoSplitController;
use raftstore::store::{self, initial_region, Config as StoreConfig, SnapManager, Transport};
use raftstore::store::{GlobalReplicationState, PdTask, SplitCheckTask};
use tikv_util::config::VersionTrack;
use tikv_util::worker::{FutureWorker, Scheduler, Worker};

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS: u64 = 3;

/// Creates a new storage engine which is backed by the Raft consensus
/// protocol.
pub fn create_raft_storage<S, EK>(
    engine: RaftKv<EK, S>,
    cfg: &StorageConfig,
    read_pool: ReadPoolHandle,
    lock_mgr: LockManager,
    concurrency_manager: ConcurrencyManager,
    pipelined_pessimistic_lock: Arc<AtomicBool>,
) -> Result<Storage<RaftKv<EK, S>, LockManager>>
where
    S: RaftStoreRouter<EK> + LocalReadRouter<EK> + 'static,
    EK: KvEngine,
{
    let store = Storage::from_engine(
        engine,
        cfg,
        read_pool,
        lock_mgr,
        concurrency_manager,
        pipelined_pessimistic_lock,
    )?;
    Ok(store)
}

/// A wrapper for the raftstore which runs Multi-Raft.
// TODO: we will rename another better name like RaftStore later.
pub struct Node<C: PdClient + 'static, ER: RaftEngine> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: Arc<VersionTrack<StoreConfig>>,
    system: RaftBatchSystem<RocksEngine, ER>,
    has_started: bool,

    pd_client: Arc<C>,
    state: Arc<Mutex<GlobalReplicationState>>,
    bg_worker: Worker,
}

impl<C, ER> Node<C, ER>
where
    C: PdClient,
    ER: RaftEngine,
{
    /// Creates a new Node.
    pub fn new(
        system: RaftBatchSystem<RocksEngine, ER>,
        cfg: &ServerConfig,
        store_cfg: Arc<VersionTrack<StoreConfig>>,
        pd_client: Arc<C>,
        state: Arc<Mutex<GlobalReplicationState>>,
        bg_worker: Worker,
    ) -> Node<C, ER> {
        let mut store = metapb::Store::default();
        store.set_id(INVALID_ID);
        if cfg.advertise_addr.is_empty() {
            store.set_address(cfg.addr.clone());
        } else {
            store.set_address(cfg.advertise_addr.clone())
        }
        if cfg.advertise_status_addr.is_empty() {
            store.set_status_address(cfg.status_addr.clone());
        } else {
            store.set_status_address(cfg.advertise_status_addr.clone())
        }
        store.set_version(env!("CARGO_PKG_VERSION").to_string());

        if let Ok(path) = std::env::current_exe() {
            if let Some(path) = path.parent() {
                store.set_deploy_path(path.to_string_lossy().to_string());
            }
        };

        store.set_start_timestamp(chrono::Local::now().timestamp());
        store.set_git_hash(
            option_env!("TIKV_BUILD_GIT_HASH")
                .unwrap_or("Unknown git hash")
                .to_string(),
        );

        let mut labels = Vec::new();
        for (k, v) in &cfg.labels {
            let mut label = metapb::StoreLabel::default();
            label.set_key(k.to_owned());
            label.set_value(v.to_owned());
            labels.push(label);
        }
        store.set_labels(labels.into());

        Node {
            cluster_id: cfg.cluster_id,
            store,
            store_cfg,
            pd_client,
            system,
            has_started: false,
            state,
            bg_worker,
        }
    }

    pub fn try_bootstrap_store(&mut self, engines: Engines<RocksEngine, ER>) -> Result<()> {
        let mut store_id = self.check_store(&engines)?;
        if store_id == INVALID_ID {
            store_id = self.alloc_id()?;
            debug!("alloc store id"; "store_id" => store_id);
            store::bootstrap_store(&engines, self.cluster_id, store_id)?;
            fail_point!("node_after_bootstrap_store", |_| Err(box_err!(
                "injected error: node_after_bootstrap_store"
            )));
        }
        self.store.set_id(store_id);
        Ok(())
    }

    /// Starts the Node. It tries to bootstrap cluster if the cluster is not
    /// bootstrapped yet. Then it spawns a thread to run the raftstore in
    /// background.
    #[allow(clippy::too_many_arguments)]
    pub fn start<T>(
        &mut self,
        engines: Engines<RocksEngine, ER>,
        trans: T,
        snap_mgr: SnapManager,
        pd_worker: FutureWorker<PdTask<RocksEngine>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        coprocessor_host: CoprocessorHost<RocksEngine>,
        importer: Arc<SSTImporter>,
        split_check_scheduler: Scheduler<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.id();
        {
            let mut meta = store_meta.lock().unwrap();
            meta.store_id = Some(store_id);
        }
        if let Some(first_region) = self.check_or_prepare_bootstrap_cluster(&engines, store_id)? {
            info!("trying to bootstrap cluster"; "store_id" => store_id, "region" => ?first_region);
            // cluster is not bootstrapped, and we choose first store to bootstrap
            fail_point!("node_after_prepare_bootstrap_cluster", |_| Err(box_err!(
                "injected error: node_after_prepare_bootstrap_cluster"
            )));
            self.bootstrap_cluster(&engines, first_region)?;
        }

        // Put store only if the cluster is bootstrapped.
        info!("put store to PD"; "store" => ?&self.store);
        let status = self.pd_client.put_store(self.store.clone())?;
        self.load_all_stores(status);

        self.start_store(
            store_id,
            engines,
            trans,
            snap_mgr,
            pd_worker,
            store_meta,
            coprocessor_host,
            importer,
            split_check_scheduler,
            auto_split_controller,
            concurrency_manager,
        )?;

        Ok(())
    }

    /// Gets the store id.
    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    /// Gets a transmission end of a channel which is used to send `Msg` to the
    /// raftstore.
    pub fn get_router(&self) -> RaftRouter<RocksEngine, ER> {
        self.system.router()
    }
    /// Gets a transmission end of a channel which is used send messages to apply worker.
    pub fn get_apply_router(&self) -> ApplyRouter<RocksEngine> {
        self.system.apply_router()
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, engines: &Engines<RocksEngine, ER>) -> Result<u64> {
        let res = engines.kv.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)?;
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            return Err(box_err!(
                "cluster ID mismatch, local {} != remote {}, \
                 you are trying to connect to another cluster, please reconnect to the correct PD",
                ident.get_cluster_id(),
                self.cluster_id
            ));
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }
        Ok(store_id)
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = self.pd_client.alloc_id()?;
        Ok(id)
    }

    fn load_all_stores(&mut self, status: Option<ReplicationStatus>) {
        info!("initializing replication mode"; "status" => ?status, "store_id" => self.store.id);
        let stores = match self.pd_client.get_all_stores(false) {
            Ok(stores) => stores,
            Err(e) => panic!("failed to load all stores: {:?}", e),
        };
        let mut state = self.state.lock().unwrap();
        if let Some(s) = status {
            state.set_status(s);
        }
        for mut store in stores {
            state
                .group
                .register_store(store.id, store.take_labels().into());
        }
    }

    // Exported for tests.
    #[doc(hidden)]
    pub fn prepare_bootstrap_cluster(
        &self,
        engines: &Engines<RocksEngine, ER>,
        store_id: u64,
    ) -> Result<metapb::Region> {
        let region_id = self.alloc_id()?;
        debug!(
            "alloc first region id";
            "region_id" => region_id,
            "cluster_id" => self.cluster_id,
            "store_id" => store_id
        );
        let peer_id = self.alloc_id()?;
        debug!(
            "alloc first peer id for first region";
            "peer_id" => peer_id,
            "region_id" => region_id,
        );

        let region = initial_region(store_id, region_id, peer_id);
        store::prepare_bootstrap_cluster(&engines, &region)?;
        Ok(region)
    }

    fn check_or_prepare_bootstrap_cluster(
        &self,
        engines: &Engines<RocksEngine, ER>,
        store_id: u64,
    ) -> Result<Option<metapb::Region>> {
        if let Some(first_region) = engines.kv.get_msg(keys::PREPARE_BOOTSTRAP_KEY)? {
            Ok(Some(first_region))
        } else if self.check_cluster_bootstrapped()? {
            Ok(None)
        } else {
            self.prepare_bootstrap_cluster(engines, store_id).map(Some)
        }
    }

    fn bootstrap_cluster(
        &mut self,
        engines: &Engines<RocksEngine, ER>,
        first_region: metapb::Region,
    ) -> Result<()> {
        let region_id = first_region.get_id();
        let mut retry = 0;
        while retry < MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self
                .pd_client
                .bootstrap_cluster(self.store.clone(), first_region.clone())
            {
                Ok(_) => {
                    info!("bootstrap cluster ok"; "cluster_id" => self.cluster_id);
                    fail_point!("node_after_bootstrap_cluster", |_| Err(box_err!(
                        "injected error: node_after_bootstrap_cluster"
                    )));
                    store::clear_prepare_bootstrap_key(&engines)?;
                    return Ok(());
                }
                Err(PdError::ClusterBootstrapped(_)) => match self.pd_client.get_region(b"") {
                    Ok(region) => {
                        if region == first_region {
                            store::clear_prepare_bootstrap_key(&engines)?;
                        } else {
                            info!("cluster is already bootstrapped"; "cluster_id" => self.cluster_id);
                            store::clear_prepare_bootstrap_cluster(&engines, region_id)?;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        warn!("get the first region failed"; "err" => ?e);
                    }
                },
                // TODO: should we clean region for other errors too?
                Err(e) => error!(?e; "bootstrap cluster"; "cluster_id" => self.cluster_id,),
            }
            retry += 1;
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("bootstrapped cluster failed"))
    }

    fn check_cluster_bootstrapped(&self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!("check cluster bootstrapped failed"; "err" => ?e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    #[allow(clippy::too_many_arguments)]
    fn start_store<T>(
        &mut self,
        store_id: u64,
        engines: Engines<RocksEngine, ER>,
        trans: T,
        snap_mgr: SnapManager,
        pd_worker: FutureWorker<PdTask<RocksEngine>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        coprocessor_host: CoprocessorHost<RocksEngine>,
        importer: Arc<SSTImporter>,
        split_check_scheduler: Scheduler<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        info!("start raft store thread"; "store_id" => store_id);

        if self.has_started {
            return Err(box_err!("{} is already started", store_id));
        }
        self.has_started = true;
        let cfg = self.store_cfg.clone();
        let pd_client = Arc::clone(&self.pd_client);
        let store = self.store.clone();

        self.system.spawn(
            store,
            cfg,
            engines,
            trans,
            pd_client,
            snap_mgr,
            pd_worker,
            store_meta,
            coprocessor_host,
            importer,
            split_check_scheduler,
            self.bg_worker.clone(),
            auto_split_controller,
            self.state.clone(),
            concurrency_manager,
        )?;
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) {
        info!("stop raft store thread"; "store_id" => store_id);
        self.system.shutdown();
    }

    /// Stops the Node.
    pub fn stop(&mut self) {
        let store_id = self.store.get_id();
        self.stop_store(store_id);
        self.bg_worker.stop();
    }
}
