// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use causal_ts::CausalTsProviderImpl;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine, TabletContext, TabletRegistry};
use kvproto::{metapb, replication_modepb::ReplicationStatus};
use pd_client::PdClient;
use raftstore::store::{GlobalReplicationState, TabletSnapManager, Transport, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{router::RaftRouter, Bootstrap, LockManagerNotifier, StoreSystem};
use slog::{info, o, Logger};
use tikv_util::{config::VersionTrack, worker::Worker};

use crate::server::{node::init_store, Result};

// TODO: we will rename another better name like RaftStore later.
pub struct NodeV2<C: PdClient + 'static, EK: KvEngine, ER: RaftEngine> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: Arc<VersionTrack<raftstore_v2::Config>>,
    system: StoreSystem<EK, ER>,
    has_started: bool,

    pd_client: Arc<C>,
    state: Arc<Mutex<GlobalReplicationState>>,
    bg_worker: Worker,
    registry: TabletRegistry<EK>,
    logger: Logger,
}

impl<C, EK, ER> NodeV2<C, EK, ER>
where
    C: PdClient,
    EK: KvEngine,
    ER: RaftEngine,
{
    /// Creates a new Node.
    pub fn new(
        system: StoreSystem<EK, ER>,
        cfg: &crate::server::Config,
        store_cfg: Arc<VersionTrack<raftstore_v2::Config>>,
        pd_client: Arc<C>,
        state: Arc<Mutex<GlobalReplicationState>>,
        bg_worker: Worker,
        store: Option<metapb::Store>,
        registry: TabletRegistry<EK>,
    ) -> NodeV2<C, EK, ER> {
        let store = init_store(store, cfg);

        NodeV2 {
            cluster_id: cfg.cluster_id,
            store,
            store_cfg,
            pd_client,
            system,
            has_started: false,
            state,
            bg_worker,
            registry,
            logger: slog_global::borrow_global().new(o!()),
        }
    }

    pub fn try_bootstrap_store(&mut self, raft_engine: &ER) -> Result<()> {
        let store_id = Bootstrap::new(
            raft_engine,
            self.cluster_id,
            &*self.pd_client,
            self.logger.clone(),
        )
        .bootstrap_store()?;
        self.store.set_id(store_id);
        Ok(())
    }

    /// Starts the Node. It tries to bootstrap cluster if the cluster is not
    /// bootstrapped yet. Then it spawns a thread to run the raftstore in
    /// background.
    pub fn start<T>(
        &mut self,
        raft_engine: ER,
        trans: T,
        router: &RaftRouter<EK, ER>,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        lock_manager_observer: Arc<dyn LockManagerNotifier>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.id();
        {
            let mut meta = router.store_meta().lock().unwrap();
            meta.store_id = Some(store_id);
        }
        if let Some(region) = Bootstrap::new(
            &raft_engine,
            self.cluster_id,
            &*self.pd_client,
            self.logger.clone(),
        )
        .bootstrap_first_region(&self.store, store_id)?
        {
            let path = self
                .registry
                .tablet_path(region.get_id(), RAFT_INIT_LOG_INDEX);
            let ctx = TabletContext::new(&region, Some(RAFT_INIT_LOG_INDEX));
            // TODO: make follow line can recover from abort.
            self.registry
                .tablet_factory()
                .open_tablet(ctx, &path)
                .unwrap();
        }

        // Put store only if the cluster is bootstrapped.
        info!(self.logger, "put store to PD"; "store" => ?&self.store);
        let status = self.pd_client.put_store(self.store.clone())?;
        self.load_all_stores(status);

        self.start_store(
            raft_engine,
            trans,
            router,
            snap_mgr,
            concurrency_manager,
            causal_ts_provider,
            lock_manager_observer,
        )?;

        Ok(())
    }

    /// Gets the store id.
    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    /// Gets a copy of Store which is registered to Pd.
    pub fn store(&self) -> metapb::Store {
        self.store.clone()
    }

    // TODO: support updating dynamic configuration.

    // TODO: check api version.
    // Do we really need to do the check giving we don't consider support upgrade
    // ATM?

    fn load_all_stores(&mut self, status: Option<ReplicationStatus>) {
        info!(self.logger, "initializing replication mode"; "status" => ?status, "store_id" => self.store.id);
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

    fn start_store<T>(
        &mut self,
        raft_engine: ER,
        trans: T,
        router: &RaftRouter<EK, ER>,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        lock_manager_observer: Arc<dyn LockManagerNotifier>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.store.get_id();
        info!(self.logger, "start raft store thread"; "store_id" => store_id);

        if self.has_started {
            return Err(box_err!("{} is already started", store_id));
        }
        self.has_started = true;
        let cfg = self.store_cfg.clone();

        self.system.start(
            store_id,
            cfg,
            raft_engine,
            self.registry.clone(),
            trans,
            self.pd_client.clone(),
            router.store_router(),
            router.store_meta().clone(),
            snap_mgr,
            concurrency_manager,
            causal_ts_provider,
            lock_manager_observer,
        )?;
        Ok(())
    }

    /// Stops the Node.
    pub fn stop(&mut self) {
        let store_id = self.store.get_id();
        info!(self.logger, "stop raft store thread"; "store_id" => store_id);
        self.system.shutdown();
        self.bg_worker.stop();
    }
}
