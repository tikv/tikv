// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use engine_traits::{Engines, KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{metapb, replication_modepb::ReplicationStatus};
use pd_client::PdClient;
use raftstore::store::{GlobalReplicationState, TabletSnapManager, Transport, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{router::RaftRouter, Bootstrap, StoreMeta, StoreSystem};
use slog::{o, Logger};
use tikv_util::{config::VersionTrack, worker::Worker};

use crate::server::{node::init_store, Result};

/// A wrapper for the raftstore which runs Multi-Raft.
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
    factory: Arc<dyn TabletFactory<EK>>,
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
        factory: Arc<dyn TabletFactory<EK>>,
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
            factory,
            logger: slog_global::borrow_global().new(o!()),
        }
    }

    pub fn try_bootstrap_store(&mut self, engines: Engines<EK, ER>) -> Result<()> {
        let store_id = Bootstrap::new(
            &engines.raft,
            self.cluster_id,
            &*self.pd_client,
            self.logger.clone(),
        )
        .bootstrap_store()?;
        self.check_api_version(&engines)?;
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
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.id();
        {
            let mut meta = store_meta.lock().unwrap();
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
            // TODO: make follow line can recover from abort.
            self.factory
                .open_tablet(
                    region.get_id(),
                    Some(RAFT_INIT_LOG_INDEX),
                    OpenOptions::default().set_create_new(true),
                )
                .unwrap();
        }

        // Put store only if the cluster is bootstrapped.
        info!("put store to PD"; "store" => ?&self.store);
        let status = self.pd_client.put_store(self.store.clone())?;
        self.load_all_stores(status);

        self.start_store(raft_engine, trans, router, snap_mgr, store_meta)?;

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

    // During the api version switch only TiDB data are allowed to exist otherwise
    // returns error.
    fn check_api_version(&self, _engines: &Engines<EK, ER>) -> Result<()> {
        // TODO: check api version.
        // Do we really need to do the check giving we don't consider support upgrade
        // ATM?
        Ok(())
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

    fn start_store<T>(
        &mut self,
        raft_engine: ER,
        trans: T,
        router: &RaftRouter<EK, ER>,
        snap_mgr: TabletSnapManager,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.store.get_id();
        info!("start raft store thread"; "store_id" => store_id);

        if self.has_started {
            return Err(box_err!("{} is already started", store_id));
        }
        self.has_started = true;
        let cfg = self.store_cfg.clone();

        self.system.start(
            store_id,
            cfg,
            raft_engine,
            self.factory.clone(),
            trans,
            router.store_router(),
            store_meta,
            snap_mgr,
        )?;
        Ok(())
    }

    /// Stops the Node.
    pub fn stop(&mut self) {
        let store_id = self.store.get_id();
        info!("stop raft store thread"; "store_id" => store_id);
        self.system.shutdown();
        self.bg_worker.stop();
    }
}
