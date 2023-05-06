// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::Duration,
};

use api_version::{dispatch_api_version, KvFormat};
use causal_ts::CausalTsProviderImpl;
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use futures::{executor::block_on, Future};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, Error as GrpcError, Service};
use grpcio_health::HealthService;
use kvproto::{
    deadlock_grpc::create_deadlock,
    debugpb_grpc::{create_debug, DebugClient},
    diagnosticspb_grpc::create_diagnostics,
    import_sstpb_grpc::create_import_sst,
    kvrpcpb::{ApiVersion, Context},
    metapb,
    raft_cmdpb::RaftCmdResponse,
    raft_serverpb::RaftMessage,
    tikvpb_grpc::TikvClient,
};
use pd_client::PdClient;
use raftstore::{
    coprocessor::CoprocessorHost,
    errors::Error as RaftError,
    store::{
        region_meta, AutoSplitController, CheckLeaderRunner, FlowStatsReporter, ReadStats,
        RegionSnapshot, TabletSnapManager, WriteStats,
    },
    RegionInfoAccessor,
};
use raftstore_v2::{router::RaftRouter, StateStorage, StoreMeta, StoreRouter};
use resource_control::ResourceGroupManager;
use resource_metering::{CollectorRegHandle, ResourceTagFactory};
use security::SecurityManager;
use slog_global::debug;
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{filter_send, AddressMap, Config, Filter};
use tikv::{
    config::ConfigController,
    coprocessor, coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::ReadPool,
    server::{
        debug2::DebuggerImplV2,
        gc_worker::GcWorker,
        load_statistics::ThreadLoadPool,
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        ConnectionBuilder, Error, Extension, NodeV2, PdStoreAddrResolver, RaftClient, RaftKv2,
        Result as ServerResult, Server, ServerTransport,
    },
    storage::{
        self,
        kv::{FakeExtension, LocalTablets, RaftExtension, SnapContext},
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine, Storage,
    },
};
use tikv_util::{
    box_err,
    config::VersionTrack,
    quota_limiter::QuotaLimiter,
    sys::thread::ThreadBuildWrapper,
    thd_name,
    worker::{Builder as WorkerBuilder, LazyWorker},
    Either, HandyRwLock,
};
use tokio::runtime::{Builder as TokioBuilder, Handle};
use txn_types::TxnExtraScheduler;

use crate::{Cluster, RaftStoreRouter, SimulateTransport, Simulator, SnapshotRouter};

#[derive(Clone)]
struct DummyReporter;

impl FlowStatsReporter for DummyReporter {
    fn report_read_stats(&self, _read_stats: ReadStats) {}
    fn report_write_stats(&self, _write_stats: WriteStats) {}
}

type SimulateRaftExtension<EK> = <TestRaftKv2<EK> as Engine>::RaftExtension;
type SimulateStoreTransport<EK> = SimulateTransport<RaftRouter<EK, RaftTestEngine>>;
type SimulateServerTransport<EK> =
    SimulateTransport<ServerTransport<SimulateRaftExtension<EK>, PdStoreAddrResolver>>;

pub type SimulateEngine<EK> = RaftKv2<EK, RaftTestEngine>;

// TestRaftKvv2 behaves the same way with RaftKv2, except that it has filters
// that can mock various network conditions.
#[derive(Clone)]
pub struct TestRaftKv2<EK: KvEngine> {
    raftkv: SimulateEngine<EK>,
    filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
}

impl<EK: KvEngine> TestRaftKv2<EK> {
    pub fn new(
        raftkv: SimulateEngine<EK>,
        filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
    ) -> TestRaftKv2<EK> {
        TestRaftKv2 { raftkv, filters }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.raftkv.set_txn_extra_scheduler(txn_extra_scheduler);
    }
}

impl<EK: KvEngine> Engine for TestRaftKv2<EK> {
    type Snap = RegionSnapshot<EK::Snapshot>;
    type Local = EK;

    fn kv_engine(&self) -> Option<Self::Local> {
        self.raftkv.kv_engine()
    }

    type RaftExtension = TestExtension<EK>;
    fn raft_extension(&self) -> Self::RaftExtension {
        TestExtension::new(self.raftkv.raft_extension(), self.filters.clone())
    }

    fn modify_on_kv_engine(
        &self,
        region_modifies: HashMap<u64, Vec<storage::kv::Modify>>,
    ) -> storage::kv::Result<()> {
        self.raftkv.modify_on_kv_engine(region_modifies)
    }

    type SnapshotRes = <SimulateEngine<EK> as Engine>::SnapshotRes;
    fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
        self.raftkv.async_snapshot(ctx)
    }

    type WriteRes = <SimulateEngine<EK> as Engine>::WriteRes;
    fn async_write(
        &self,
        ctx: &Context,
        batch: storage::kv::WriteData,
        subscribed: u8,
        on_applied: Option<storage::kv::OnAppliedCb>,
    ) -> Self::WriteRes {
        self.raftkv.async_write(ctx, batch, subscribed, on_applied)
    }

    #[inline]
    fn precheck_write_with_ctx(&self, ctx: &Context) -> storage::kv::Result<()> {
        self.raftkv.precheck_write_with_ctx(ctx)
    }

    #[inline]
    fn schedule_txn_extra(&self, txn_extra: txn_types::TxnExtra) {
        self.raftkv.schedule_txn_extra(txn_extra)
    }
}

#[derive(Clone)]
pub struct TestExtension<EK: KvEngine> {
    extension: Extension<EK, RaftTestEngine>,
    filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
}

impl<EK: KvEngine> TestExtension<EK> {
    pub fn new(
        extension: Extension<EK, RaftTestEngine>,
        filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
    ) -> Self {
        TestExtension { extension, filters }
    }
}

impl<EK: KvEngine> RaftExtension for TestExtension<EK> {
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        let send = |msg| -> raftstore::Result<()> {
            self.extension.feed(msg, key_message);
            Ok(())
        };

        let _ = filter_send(&self.filters, msg, send);
    }

    #[inline]
    fn report_reject_message(&self, region_id: u64, from_peer_id: u64) {
        self.extension
            .report_reject_message(region_id, from_peer_id)
    }

    #[inline]
    fn report_peer_unreachable(&self, region_id: u64, to_peer_id: u64) {
        self.extension
            .report_peer_unreachable(region_id, to_peer_id)
    }

    #[inline]
    fn report_store_unreachable(&self, store_id: u64) {
        self.extension.report_store_unreachable(store_id)
    }

    #[inline]
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: raft::SnapshotStatus,
    ) {
        self.extension
            .report_snapshot_status(region_id, to_peer_id, status)
    }

    #[inline]
    fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.extension.report_resolved(store_id, group_id)
    }

    #[inline]
    fn split(
        &self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> futures::future::BoxFuture<'static, storage::kv::Result<Vec<metapb::Region>>> {
        self.extension
            .split(region_id, region_epoch, split_keys, source)
    }

    fn query_region(
        &self,
        region_id: u64,
    ) -> futures::future::BoxFuture<'static, storage::kv::Result<region_meta::RegionMeta>> {
        self.extension.query_region(region_id)
    }
}

pub struct ServerMeta<EK: KvEngine> {
    node: NodeV2<TestPdClient, EK, RaftTestEngine>,
    server: Server<PdStoreAddrResolver, TestRaftKv2<EK>>,
    sim_router: SimulateStoreTransport<EK>,
    sim_trans: SimulateServerTransport<EK>,
    raw_router: StoreRouter<EK, RaftTestEngine>,
    gc_worker: GcWorker<TestRaftKv2<EK>>,
    rsmeter_cleanup: Box<dyn FnOnce()>,
}

type PendingServices = Vec<Box<dyn Fn() -> Service>>;
type PendingDebugService<EK> = Box<dyn Fn(&ServerCluster<EK>, Handle) -> Service>;

pub struct ServerCluster<EK: KvEngine> {
    metas: HashMap<u64, ServerMeta<EK>>,
    addrs: AddressMap,
    pub storages: HashMap<u64, TestRaftKv2<EK>>,
    pub region_info_accessors: HashMap<u64, RegionInfoAccessor>,
    snap_paths: HashMap<u64, TempDir>,
    snap_mgrs: HashMap<u64, TabletSnapManager>,
    pd_client: Arc<TestPdClient>,
    raft_clients: HashMap<u64, RaftClient<AddressMap, FakeExtension>>,
    conn_builder: ConnectionBuilder<AddressMap, FakeExtension>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,
    env: Arc<Environment>,
    pub pending_services: HashMap<u64, PendingServices>,
    pub pending_debug_service: Option<PendingDebugService<EK>>,
    pub health_services: HashMap<u64, HealthService>,
    pub security_mgr: Arc<SecurityManager>,
    pub txn_extra_schedulers: HashMap<u64, Arc<dyn TxnExtraScheduler>>,
    pub causal_ts_providers: HashMap<u64, Arc<CausalTsProviderImpl>>,
}

impl<EK: KvEngine> ServerCluster<EK> {
    pub fn new(pd_client: Arc<TestPdClient>) -> Self {
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(2)
                .name_prefix(thd_name!("server-cluster"))
                .build(),
        );
        let security_mgr = Arc::new(SecurityManager::new(&Default::default()).unwrap());
        let map = AddressMap::default();
        // We don't actually need to handle snapshot message, just create a dead worker
        // to make it compile.
        let worker = LazyWorker::new("snap-worker");
        let conn_builder = ConnectionBuilder::new(
            env.clone(),
            Arc::default(),
            security_mgr.clone(),
            map.clone(),
            FakeExtension {},
            worker.scheduler(),
            Arc::new(ThreadLoadPool::with_threshold(usize::MAX)),
        );
        ServerCluster {
            metas: HashMap::default(),
            addrs: map,
            pd_client,
            security_mgr,
            storages: HashMap::default(),
            region_info_accessors: HashMap::default(),
            snap_mgrs: HashMap::default(),
            snap_paths: HashMap::default(),
            pending_services: HashMap::default(),
            pending_debug_service: None::<PendingDebugService<EK>>,
            health_services: HashMap::default(),
            raft_clients: HashMap::default(),
            conn_builder,
            concurrency_managers: HashMap::default(),
            env,
            txn_extra_schedulers: HashMap::default(),
            causal_ts_providers: HashMap::default(),
        }
    }

    pub fn get_addr(&self, node_id: u64) -> String {
        self.addrs.get(node_id).unwrap()
    }

    pub fn run_node_impl<F: KvFormat>(
        &mut self,
        node_id: u64,
        mut cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        key_manager: Option<Arc<DataKeyManager>>,
        raft_engine: RaftTestEngine,
        tablet_registry: TabletRegistry<EK>,
        resource_manager: &Option<Arc<ResourceGroupManager>>,
    ) -> ServerResult<u64> {
        let (snap_mgr, snap_mgs_path) = if !self.snap_mgrs.contains_key(&node_id) {
            let tmp = test_util::temp_dir("test_cluster", cfg.prefer_mem);
            let snap_path = tmp.path().to_str().unwrap().to_owned();
            (
                TabletSnapManager::new(snap_path, key_manager.clone())?,
                Some(tmp),
            )
        } else {
            (self.snap_mgrs[&node_id].clone(), None)
        };

        let bg_worker = WorkerBuilder::new("background").thread_count(2).create();

        if cfg.server.addr == "127.0.0.1:0" {
            // Now we cache the store address, so here we should re-use last
            // listening address for the same store.
            if let Some(addr) = self.addrs.get(node_id) {
                cfg.server.addr = addr;
            } else {
                cfg.server.addr = format!("127.0.0.1:{}", test_util::alloc_port());
            }
        }

        // Create node.
        let mut raft_store = cfg.raft_store.clone();
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

        let server_cfg = Arc::new(VersionTrack::new(cfg.server.clone()));

        let raft_router =
            RaftRouter::new_with_store_meta(node.router().clone(), store_meta.clone());

        // Create coprocessor.
        let mut coprocessor_host =
            CoprocessorHost::new(raft_router.store_router().clone(), cfg.coprocessor.clone());

        let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);

        let sim_router = SimulateTransport::new(raft_router.clone());
        let mut raft_kv_v2 = TestRaftKv2::new(
            RaftKv2::new(raft_router.clone(), region_info_accessor.region_leaders()),
            sim_router.filters().clone(),
        );

        // Create storage.
        let pd_worker = LazyWorker::new("test-pd-worker");
        let pd_sender = raftstore_v2::PdReporter::new(
            pd_worker.scheduler(),
            slog_global::borrow_global().new(slog::o!()),
        );
        let storage_read_pool = ReadPool::from(storage::build_read_pool(
            &tikv::config::StorageReadPoolConfig::default_for_test(),
            pd_sender,
            raft_kv_v2.clone(),
        ));

        if let Some(scheduler) = self.txn_extra_schedulers.remove(&node_id) {
            raft_kv_v2.set_txn_extra_scheduler(scheduler);
        }

        let latest_ts =
            block_on(self.pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut gc_worker = GcWorker::new(
            raft_kv_v2.clone(),
            tx,
            cfg.gc.clone(),
            Default::default(),
            Arc::new(region_info_accessor.clone()),
        );
        gc_worker.start(node_id).unwrap();

        // todo: resolved ts

        if ApiVersion::V2 == F::TAG {
            let casual_ts_provider: Arc<CausalTsProviderImpl> = Arc::new(
                block_on(causal_ts::BatchTsoProvider::new_opt(
                    self.pd_client.clone(),
                    cfg.causal_ts.renew_interval.0,
                    cfg.causal_ts.alloc_ahead_buffer.0,
                    cfg.causal_ts.renew_batch_min_size,
                    cfg.causal_ts.renew_batch_max_size,
                ))
                .unwrap()
                .into(),
            );
            self.causal_ts_providers.insert(node_id, casual_ts_provider);
        }

        // Start resource metering.
        let (res_tag_factory, collector_reg_handle, rsmeter_cleanup) =
            self.init_resource_metering(&cfg.resource_metering);

        let check_leader_runner = CheckLeaderRunner::new(store_meta, coprocessor_host.clone());
        let check_leader_scheduler = bg_worker.start("check-leader", check_leader_runner);

        let mut lock_mgr = LockManager::new(&cfg.pessimistic_txn);
        let quota_limiter = Arc::new(QuotaLimiter::new(
            cfg.quota.foreground_cpu_time,
            cfg.quota.foreground_write_bandwidth,
            cfg.quota.foreground_read_bandwidth,
            cfg.quota.background_cpu_time,
            cfg.quota.background_write_bandwidth,
            cfg.quota.background_read_bandwidth,
            cfg.quota.max_delay_duration,
            cfg.quota.enable_auto_tune,
        ));

        let casual_ts_provider = self.get_causal_ts_provider(node_id);
        let store = Storage::<_, _, F>::from_engine(
            raft_kv_v2.clone(),
            &cfg.storage,
            storage_read_pool.handle(),
            lock_mgr.clone(),
            concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            res_tag_factory.clone(),
            quota_limiter.clone(),
            self.pd_client.feature_gate().clone(),
            casual_ts_provider.clone(),
            resource_manager
                .as_ref()
                .map(|m| m.derive_controller("scheduler-worker-pool".to_owned(), true)),
        )?;
        self.storages.insert(node_id, raft_kv_v2.clone());

        ReplicaReadLockChecker::new(concurrency_manager.clone()).register(&mut coprocessor_host);

        // Create import service.
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
        let import_service = ImportSstService::new(
            cfg.import.clone(),
            cfg.raft_store.raft_entry_max_size,
            raft_kv_v2,
            LocalTablets::Registry(tablet_registry.clone()),
            Arc::clone(&importer),
        );

        // Create deadlock service.
        let deadlock_service = lock_mgr.deadlock_service();

        // Create pd client, snapshot manager, server.
        let (resolver, state) = resolve::new_resolver(
            Arc::clone(&self.pd_client),
            &bg_worker,
            store.get_engine().raft_extension(),
        );
        let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
        let cop_read_pool = ReadPool::from(coprocessor::readpool_impl::build_read_pool_for_test(
            &tikv::config::CoprReadPoolConfig::default_for_test(),
            store.get_engine(),
        ));
        let copr = coprocessor::Endpoint::new(
            &server_cfg.value().clone(),
            cop_read_pool.handle(),
            concurrency_manager.clone(),
            res_tag_factory,
            quota_limiter,
        );
        let copr_v2 = coprocessor_v2::Endpoint::new(&cfg.coprocessor_v2);
        let mut server = None;

        // Create Debug service.
        let debug_thread_pool = Arc::new(
            TokioBuilder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(|| {})
                .before_stop_wrapper(|| {})
                .build()
                .unwrap(),
        );
        let debug_thread_handle = debug_thread_pool.handle().clone();
        let diag_service = DiagnosticsService::new(
            debug_thread_handle.clone(),
            cfg.log.file.filename.clone(),
            cfg.slow_log_file.clone(),
        );

        let health_service = HealthService::default();

        for _ in 0..100 {
            let mut svr = Server::new(
                node_id,
                &server_cfg,
                &security_mgr,
                store.clone(),
                copr.clone(),
                copr_v2.clone(),
                resolver.clone(),
                Either::Right(snap_mgr.clone()),
                gc_worker.clone(),
                check_leader_scheduler.clone(),
                self.env.clone(),
                None,
                debug_thread_pool.clone(),
                health_service.clone(),
                resource_manager.clone(),
            )
            .unwrap();
            svr.register_service(create_diagnostics(diag_service.clone()));
            svr.register_service(create_deadlock(deadlock_service.clone()));
            svr.register_service(create_import_sst(import_service.clone()));
            if let Some(svcs) = self.pending_services.get(&node_id) {
                for fact in svcs {
                    svr.register_service(fact());
                }
            }
            if let Some(debug_service) = &self.pending_debug_service {
                svr.register_service(debug_service(self, debug_thread_handle.clone()));
            }
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
        assert_eq!(addr.clone().to_string(), node.store().address);
        cfg.server.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans);
        let server_cfg = Arc::new(VersionTrack::new(cfg.server.clone()));

        // Register the role change observer of the lock manager.
        lock_mgr.register_detector_role_change_observer(&mut coprocessor_host);

        let pessimistic_txn_cfg = cfg.tikv.pessimistic_txn;
        node.start(
            raft_engine,
            tablet_registry.clone(),
            &raft_router,
            simulate_trans.clone(),
            snap_mgr.clone(),
            concurrency_manager.clone(),
            casual_ts_provider,
            coprocessor_host,
            AutoSplitController::default(),
            collector_reg_handle,
            bg_worker,
            pd_worker,
            Arc::new(VersionTrack::new(raft_store)),
            &state,
            importer,
            key_manager,
        )?;
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        self.snap_mgrs.insert(node_id, snap_mgr);
        if let Some(tmp) = snap_mgs_path {
            self.snap_paths.insert(node_id, tmp);
        }
        self.region_info_accessors
            .insert(node_id, region_info_accessor);
        // todo: importer
        self.health_services.insert(node_id, health_service);

        lock_mgr
            .start(
                node.id(),
                Arc::clone(&self.pd_client),
                resolver,
                Arc::clone(&security_mgr),
                &pessimistic_txn_cfg,
            )
            .unwrap();

        server
            .start(server_cfg, security_mgr, tablet_registry)
            .unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                raw_router: raft_router.store_router().clone(),
                node,
                server,
                sim_router,
                gc_worker,
                sim_trans: simulate_trans,
                rsmeter_cleanup,
            },
        );
        self.addrs.insert(node_id, format!("{}", addr));
        self.concurrency_managers
            .insert(node_id, concurrency_manager);

        let client = RaftClient::new(node_id, self.conn_builder.clone());
        self.raft_clients.insert(node_id, client);
        Ok(node_id)
    }

    pub fn get_gc_worker(&self, node_id: u64) -> &GcWorker<TestRaftKv2<EK>> {
        &self.metas.get(&node_id).unwrap().gc_worker
    }

    pub fn get_causal_ts_provider(&self, node_id: u64) -> Option<Arc<CausalTsProviderImpl>> {
        self.causal_ts_providers.get(&node_id).cloned()
    }

    fn init_resource_metering(
        &self,
        cfg: &resource_metering::Config,
    ) -> (ResourceTagFactory, CollectorRegHandle, Box<dyn FnOnce()>) {
        let (_, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(cfg.precision.as_millis());
        let (_, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(cfg.clone(), collector_reg_handle.clone());
        let (_, single_target_worker) = resource_metering::init_single_target(
            cfg.receiver_address.clone(),
            Arc::new(Environment::new(2)),
            data_sink_reg_handle,
        );

        (
            resource_tag_factory,
            collector_reg_handle,
            Box::new(move || {
                single_target_worker.stop_worker();
                reporter_worker.stop_worker();
                recorder_worker.stop_worker();
            }),
        )
    }

    pub fn get_concurrency_manager(&self, node_id: u64) -> ConcurrencyManager {
        self.concurrency_managers.get(&node_id).unwrap().clone()
    }
}

impl<EK: KvEngine> Simulator<EK> for ServerCluster<EK> {
    fn get_node_ids(&self) -> HashSet<u64> {
        self.metas.keys().cloned().collect()
    }

    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn test_raftstore::Filter>) {
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

    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn test_raftstore::Filter>) {
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

    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        key_manager: Option<Arc<DataKeyManager>>,
        raft_engine: RaftTestEngine,
        tablet_registry: TabletRegistry<EK>,
        resource_manager: &Option<Arc<ResourceGroupManager>>,
    ) -> ServerResult<u64> {
        dispatch_api_version!(
            cfg.storage.api_version(),
            self.run_node_impl::<API>(
                node_id,
                cfg,
                store_meta,
                key_manager,
                raft_engine,
                tablet_registry,
                resource_manager
            )
        )
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut meta) = self.metas.remove(&node_id) {
            meta.server.stop().unwrap();
            meta.node.stop();
            // // resolved ts worker started, let's stop it
            // if let Some(worker) = meta.rts_worker {
            //     worker.stop_worker();
            // }
            (meta.rsmeter_cleanup)();
        }
        self.storages.remove(&node_id);
        let _ = self.raft_clients.remove(&node_id);
    }

    fn async_snapshot(
        &mut self,
        request: kvproto::raft_cmdpb::RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>> + Send
    {
        let node_id = request.get_header().get_peer().get_store_id();
        let mut router = match self.metas.get(&node_id) {
            None => {
                let mut resp = RaftCmdResponse::default();
                let e: RaftError = box_err!("missing sender for store {}", node_id);
                resp.mut_header().set_error(e.into());
                // return async move {Err(resp)};
                unreachable!()
            }
            Some(meta) => meta.sim_router.clone(),
        };

        router.snapshot(request)
    }

    fn async_peer_msg_on_node(
        &self,
        node_id: u64,
        region_id: u64,
        msg: raftstore_v2::router::PeerMsg,
    ) -> raftstore::Result<()> {
        let router = match self.metas.get(&node_id) {
            None => return Err(box_err!("missing sender for store {}", node_id)),
            Some(meta) => meta.sim_router.clone(),
        };

        router.send_peer_msg(region_id, msg)
    }

    fn send_raft_msg(&mut self, msg: RaftMessage) -> raftstore::Result<()> {
        let from_store = msg.get_from_peer().store_id;
        assert_ne!(from_store, 0);
        if let Some(client) = self.raft_clients.get_mut(&from_store) {
            client.send(msg).unwrap();
            client.flush();
        }
        Ok(())
    }

    fn get_router(&self, node_id: u64) -> Option<StoreRouter<EK, RaftTestEngine>> {
        self.metas.get(&node_id).map(|m| m.raw_router.clone())
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_mgrs[&node_id]
            .root_path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn get_snap_mgr(&self, node_id: u64) -> &TabletSnapManager {
        self.snap_mgrs.get(&node_id).unwrap()
    }
}

impl<EK: KvEngine> Cluster<ServerCluster<EK>, EK> {
    pub fn must_get_snapshot_of_region(&mut self, region_id: u64) -> RegionSnapshot<EK::Snapshot> {
        let mut try_snapshot = || -> Option<RegionSnapshot<EK::Snapshot>> {
            let leader = self.leader_of_region(region_id)?;
            let store_id = leader.store_id;
            let epoch = self.get_region_epoch(region_id);
            let mut ctx = Context::default();
            ctx.set_region_id(region_id);
            ctx.set_peer(leader);
            ctx.set_region_epoch(epoch);

            let mut storage = self.sim.rl().storages.get(&store_id).unwrap().clone();
            let snap_ctx = SnapContext {
                pb_ctx: &ctx,
                ..Default::default()
            };
            storage.snapshot(snap_ctx).ok()
        };
        for _ in 0..10 {
            if let Some(snapshot) = try_snapshot() {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(200));
        }
        panic!("failed to get snapshot of region {}", region_id);
    }

    pub fn get_addr(&self, node_id: u64) -> String {
        self.sim.rl().get_addr(node_id)
    }

    pub fn get_security_mgr(&self) -> Arc<SecurityManager> {
        self.sim.rl().security_mgr.clone()
    }
}

pub fn new_server_cluster(
    id: u64,
    count: usize,
) -> Cluster<ServerCluster<RocksEngine>, RocksEngine> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(
        id,
        count,
        sim,
        pd_client,
        ApiVersion::V1,
        Box::new(crate::create_test_engine),
    )
}

pub fn new_incompatible_server_cluster(
    id: u64,
    count: usize,
) -> Cluster<ServerCluster<RocksEngine>, RocksEngine> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(
        id,
        count,
        sim,
        pd_client,
        ApiVersion::V1,
        Box::new(crate::create_test_engine),
    )
}

pub fn new_server_cluster_with_api_ver(
    id: u64,
    count: usize,
    api_ver: ApiVersion,
) -> Cluster<ServerCluster<RocksEngine>, RocksEngine> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(
        id,
        count,
        sim,
        pd_client,
        api_ver,
        Box::new(crate::create_test_engine),
    )
}

pub fn must_new_cluster_and_kv_client() -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    TikvClient,
    Context,
) {
    must_new_cluster_and_kv_client_mul(1)
}

pub fn must_new_cluster_and_kv_client_mul(
    count: usize,
) -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    TikvClient,
    Context,
) {
    let (cluster, leader, ctx) = must_new_cluster_mul(count);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    (cluster, client, ctx)
}
pub fn must_new_cluster_mul(
    count: usize,
) -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    metapb::Peer,
    Context,
) {
    must_new_and_configure_cluster_mul(count, |_| ())
}

fn must_new_and_configure_cluster_mul(
    count: usize,
    mut configure: impl FnMut(&mut Cluster<ServerCluster<RocksEngine>, RocksEngine>),
) -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    metapb::Peer,
    Context,
) {
    let mut cluster = new_server_cluster(0, count);
    configure(&mut cluster);
    cluster.run();
    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    (cluster, leader, ctx)
}

pub fn must_new_and_configure_cluster_and_kv_client(
    configure: impl FnMut(&mut Cluster<ServerCluster<RocksEngine>, RocksEngine>),
) -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    TikvClient,
    Context,
) {
    let (cluster, leader, ctx) = must_new_and_configure_cluster(configure);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    (cluster, client, ctx)
}

pub fn must_new_and_configure_cluster(
    configure: impl FnMut(&mut Cluster<ServerCluster<RocksEngine>, RocksEngine>),
) -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    metapb::Peer,
    Context,
) {
    must_new_and_configure_cluster_mul(1, configure)
}

pub fn must_new_cluster_and_debug_client() -> (
    Cluster<ServerCluster<RocksEngine>, RocksEngine>,
    DebugClient,
    u64,
) {
    let mut cluster = new_server_cluster(0, 1);
    cluster.create_engines();
    let region_id = cluster.bootstrap_conf_change();

    {
        let mut sim = cluster.sim.wl();
        let tablet_registry = cluster.tablet_registries.get(&1).unwrap().clone();
        let raft_engine = cluster.raft_engines.get(&1).unwrap().clone();
        let debugger =
            DebuggerImplV2::new(tablet_registry, raft_engine, ConfigController::default());

        sim.pending_debug_service = Some(Box::new(move |cluster, debug_thread_handle| {
            let raft_extension = cluster.storages.get(&1).unwrap().raft_extension();

            create_debug(DebugService::new(
                debugger.clone(),
                debug_thread_handle,
                raft_extension,
            ))
        }));
    }

    cluster.start().unwrap();
    let leader = cluster.leader_of_region(region_id).unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = DebugClient::new(channel);

    (cluster, client, leader.get_store_id())
}
