// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(clippy::arc_with_non_send_sync)]

use std::{
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
    thread,
    time::Duration,
    usize,
};

use api_version::{dispatch_api_version, KvFormat};
use causal_ts::CausalTsProviderImpl;
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::RocksSnapshot;
use engine_store_ffi::core::DebugStruct;
use engine_traits::{Engines, MiscExt};
use futures::executor::block_on;
use grpcio::{ChannelBuilder, EnvBuilder, Environment, Error as GrpcError, Service};
use health_controller::HealthController;
use kvproto::{
    deadlock::create_deadlock,
    debugpb::DebugClient,
    import_sstpb::create_import_sst,
    kvrpcpb::{ApiVersion, Context},
    metapb,
    raft_cmdpb::*,
    raft_serverpb,
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CoprocessorHost, RegionInfoAccessor},
    errors::Error as RaftError,
    router::{CdcRaftRouter, LocalReadRouter, RaftStoreRouter, ReadContext, ServerRaftStoreRouter},
    store::{
        fsm::{store::StoreMeta, ApplyRouter, RaftBatchSystem, RaftRouter},
        msg::RaftCmdExtraOpts,
        AutoSplitController, Callback, CheckLeaderRunner, LocalReader, RegionSnapshot, SnapManager,
        SnapManagerBuilder, SplitCheckRunner, SplitConfigManager, StoreMetaDelegate,
    },
    Result,
};
use resource_metering::{CollectorRegHandle, ResourceTagFactory};
use security::SecurityManager;
use service::service_manager::GrpcServiceManager;
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use tikv::{
    coprocessor, coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::ReadPool,
    server::{
        gc_worker::GcWorker,
        load_statistics::ThreadLoadPool,
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve::{self, StoreAddrResolver},
        tablet_snap::NoSnapshotCache,
        ConnectionBuilder, Error, MultiRaftServer, PdStoreAddrResolver, RaftClient, RaftKv,
        Result as ServerResult, Server, ServerTransport,
    },
    storage::{
        self,
        kv::{FakeExtension, LocalTablets, SnapContext},
        txn::{
            flow_controller::{EngineFlowController, FlowController},
            txn_status_cache::TxnStatusCache,
        },
        Engine, Storage,
    },
};
use tikv_util::{
    box_err,
    config::VersionTrack,
    quota_limiter::QuotaLimiter,
    sys::thread::ThreadBuildWrapper,
    thd_name,
    time::ThreadReadId,
    worker::{Builder as WorkerBuilder, LazyWorker},
    HandyRwLock,
};
use tokio::runtime::Builder as TokioBuilder;
use transport_simulate::SimulateTransport;
use txn_types::TxnExtraScheduler;

use super::{common::*, Cluster, Simulator, *};
use crate::mock_cluster::v1::transport_simulate::Filter;

type SimulateStoreTransport =
    SimulateTransport<ServerRaftStoreRouter<TiFlashEngine, ProxyRaftEngine>, TiFlashEngine>;
type SimulateRaftExtension = <SimulateEngine as Engine>::RaftExtension;
type SimulateServerTransport =
    SimulateTransport<ServerTransport<SimulateRaftExtension, PdStoreAddrResolver>, TiFlashEngine>;

pub type SimulateEngine = RaftKv<TiFlashEngine, SimulateStoreTransport>;

#[derive(Default, Clone)]
pub struct AddressMap {
    addrs: Arc<Mutex<HashMap<u64, String>>>,
}

impl AddressMap {
    pub fn get(&self, store_id: u64) -> Option<String> {
        let addrs = self.addrs.lock().unwrap();
        addrs.get(&store_id).cloned()
    }

    pub fn insert(&mut self, store_id: u64, addr: String) {
        self.addrs.lock().unwrap().insert(store_id, addr);
    }
}

impl StoreAddrResolver for AddressMap {
    fn resolve(
        &self,
        store_id: u64,
        cb: Box<dyn FnOnce(resolve::Result<String>) + Send>,
    ) -> resolve::Result<()> {
        let addr = self.get(store_id);
        match addr {
            Some(addr) => cb(Ok(addr)),
            None => cb(Err(box_err!(
                "unable to find address for store {}",
                store_id
            ))),
        }
        Ok(())
    }
}

struct ServerMeta {
    node: MultiRaftServer<TestPdClient, TiFlashEngine, ProxyRaftEngine>,
    server: Server<PdStoreAddrResolver, SimulateEngine>,
    sim_router: SimulateStoreTransport,
    sim_trans: SimulateServerTransport,
    raw_router: RaftRouter<TiFlashEngine, ProxyRaftEngine>,
    raw_apply_router: ApplyRouter<TiFlashEngine>,
    gc_worker: GcWorker<RaftKv<TiFlashEngine, SimulateStoreTransport>>,
    rts_worker: Option<LazyWorker<resolved_ts::Task>>,
    rsmeter_cleanup: Box<dyn FnOnce()>,
}

type PendingServices = Vec<Box<dyn Fn() -> Service>>;
type CopHooks = Vec<Box<dyn Fn(&mut CoprocessorHost<TiFlashEngine>)>>;

pub struct ServerCluster {
    metas: HashMap<u64, ServerMeta>,
    addrs: AddressMap,
    pub storages: HashMap<u64, SimulateEngine>,
    pub region_info_accessors: HashMap<u64, RegionInfoAccessor>,
    pub importers: HashMap<u64, Arc<SstImporter<TiFlashEngine>>>,
    pub pending_services: HashMap<u64, PendingServices>,
    pub coprocessor_hooks: HashMap<u64, CopHooks>,
    pub health_controllers: HashMap<u64, HealthController>,
    pub security_mgr: Arc<SecurityManager>,
    pub txn_extra_schedulers: HashMap<u64, Arc<dyn TxnExtraScheduler>>,
    snap_paths: HashMap<u64, TempDir>,
    snap_mgrs: HashMap<u64, SnapManager>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient<AddressMap, FakeExtension>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,
    env: Arc<Environment>,
    pub causal_ts_providers: HashMap<u64, Arc<CausalTsProviderImpl>>,
}

impl ServerCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> ServerCluster {
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
            FakeExtension,
            worker.scheduler(),
            Arc::new(ThreadLoadPool::with_threshold(usize::MAX)),
        );
        // TODO Set store_id to 0 as a workaround.
        let raft_client = RaftClient::new(0, conn_builder);
        ServerCluster {
            metas: HashMap::default(),
            addrs: map,
            pd_client,
            security_mgr,
            storages: HashMap::default(),
            region_info_accessors: HashMap::default(),
            importers: HashMap::default(),
            snap_paths: HashMap::default(),
            snap_mgrs: HashMap::default(),
            pending_services: HashMap::default(),
            coprocessor_hooks: HashMap::default(),
            health_controllers: HashMap::default(),
            raft_client,
            concurrency_managers: HashMap::default(),
            env,
            txn_extra_schedulers: HashMap::default(),
            causal_ts_providers: HashMap::default(),
        }
    }

    pub fn get_addr(&self, node_id: u64) -> String {
        self.addrs.get(node_id).unwrap()
    }

    pub fn get_apply_router(&self, node_id: u64) -> ApplyRouter<TiFlashEngine> {
        self.metas.get(&node_id).unwrap().raw_apply_router.clone()
    }

    pub fn get_server_router(&self, node_id: u64) -> SimulateStoreTransport {
        self.metas.get(&node_id).unwrap().sim_router.clone()
    }

    /// To trigger GC manually.
    pub fn get_gc_worker(
        &self,
        node_id: u64,
    ) -> &GcWorker<RaftKv<TiFlashEngine, SimulateStoreTransport>> {
        &self.metas.get(&node_id).unwrap().gc_worker
    }

    pub fn get_concurrency_manager(&self, node_id: u64) -> ConcurrencyManager {
        self.concurrency_managers.get(&node_id).unwrap().clone()
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

    fn run_node_impl<F: KvFormat>(
        &mut self,
        node_id: u64,
        mut cfg: MixedClusterConfig,
        engines: Engines<TiFlashEngine, ProxyRaftEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<TiFlashEngine, ProxyRaftEngine>,
        system: RaftBatchSystem<TiFlashEngine, ProxyRaftEngine>,
    ) -> ServerResult<u64> {
        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = test_util::temp_dir("test_cluster", cfg.prefer_mem);
            (p.path().to_str().unwrap().to_owned(), Some(p))
        } else {
            let p = self.snap_paths[&node_id].path().to_str().unwrap();
            (p.to_owned(), None)
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

        let key_mgr_cloned = key_manager.clone();

        // Create coprocessor.
        let mut coprocessor_host = CoprocessorHost::new(router.clone(), cfg.coprocessor.clone());

        let mut tiflash_ob = engine_store_ffi::observer::TiFlashObserver::default();
        tiflash_ob.register_to(&mut coprocessor_host);

        let local_reader = LocalReader::new(
            engines.kv.clone(),
            StoreMetaDelegate::new(store_meta.clone(), engines.kv.clone()),
            router.clone(),
            coprocessor_host.clone(),
        );

        // In-memory engine
        let enable_region_stats_mgr_cb: Arc<dyn Fn() -> bool + Send + Sync> =
            if cfg.in_memory_engine.enable {
                Arc::new(|| true)
            } else {
                Arc::new(|| false)
            };

        let mut in_memory_engine_config = cfg.in_memory_engine.clone();
        in_memory_engine_config.expected_region_size = cfg.coprocessor.region_split_size();
        let in_memory_engine_config = Arc::new(VersionTrack::new(in_memory_engine_config));
        let in_memory_engine_config_clone = in_memory_engine_config.clone();

        let region_info_accessor = RegionInfoAccessor::new(
            &mut coprocessor_host,
            enable_region_stats_mgr_cb,
            Box::new(move || {
                in_memory_engine_config_clone
                    .value()
                    .mvcc_amplification_threshold
            }),
        );

        let raft_router = ServerRaftStoreRouter::new(router.clone(), local_reader);
        let sim_router = SimulateTransport::new(raft_router.clone());
        let mut engine = RaftKv::new(
            sim_router.clone(),
            engines.kv.clone(),
            region_info_accessor.region_leaders(),
        );

        if let Some(hooks) = self.coprocessor_hooks.get(&node_id) {
            for hook in hooks {
                hook(&mut coprocessor_host);
            }
        }

        // Create storage.
        let pd_worker = LazyWorker::new("test-pd-worker");
        let pd_sender = pd_worker.scheduler();
        let storage_read_pool = ReadPool::from(storage::build_read_pool(
            &tikv::config::StorageReadPoolConfig::default_for_test(),
            pd_sender.clone(),
            engine.clone(),
        ));

        let extension = engine.raft_extension().clone();
        if let Some(scheduler) = self.txn_extra_schedulers.remove(&node_id) {
            engine.set_txn_extra_scheduler(scheduler);
        }

        let latest_ts =
            block_on(self.pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut gc_worker = GcWorker::new(
            engine.clone(),
            tx,
            cfg.gc.clone(),
            Default::default(),
            Arc::new(region_info_accessor.clone()),
        );
        gc_worker.start(node_id, coprocessor_host.clone()).unwrap();

        let txn_status_cache = Arc::new(TxnStatusCache::new_for_test());
        let rts_worker = if cfg.resolved_ts.enable {
            // Resolved ts worker
            let mut rts_worker = LazyWorker::new("resolved-ts");
            let rts_ob = resolved_ts::Observer::new(rts_worker.scheduler());
            rts_ob.register_to(&mut coprocessor_host);
            // Resolved ts endpoint
            let rts_endpoint = resolved_ts::Endpoint::new(
                &cfg.resolved_ts,
                rts_worker.scheduler(),
                CdcRaftRouter(raft_router),
                store_meta.clone(),
                self.pd_client.clone(),
                concurrency_manager.clone(),
                self.env.clone(),
                self.security_mgr.clone(),
                txn_status_cache.clone(),
            );
            // Start the worker
            rts_worker.start(rts_endpoint);
            Some(rts_worker)
        } else {
            None
        };

        // Start resource metering.
        let (res_tag_factory, collector_reg_handle, rsmeter_cleanup) =
            self.init_resource_metering(&cfg.resource_metering);

        // Create import service.
        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
            Arc::new(
                SstImporter::new(
                    &cfg.import,
                    dir,
                    key_manager.clone(),
                    cfg.storage.api_version(),
                    false,
                )
                .unwrap(),
            )
        };
        let import_service = ImportSstService::new(
            cfg.import.clone(),
            cfg.raft_store.raft_entry_max_size,
            engine.clone(),
            LocalTablets::Singleton(engines.kv.clone()),
            Arc::clone(&importer),
            None,
            None, // TODO resource_ctl
            Arc::new(region_info_accessor.clone()),
        );

        let check_leader_runner =
            CheckLeaderRunner::new(store_meta.clone(), coprocessor_host.clone());
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
        let store = Storage::<_, _, F>::from_engine(
            engine.clone(),
            &cfg.storage,
            storage_read_pool.handle(),
            lock_mgr.clone(),
            concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            pd_sender,
            res_tag_factory.clone(),
            quota_limiter.clone(),
            self.pd_client.feature_gate().clone(),
            None,
            None, // TODO resource_ctl
            None, // TODO resource manager
            txn_status_cache,
        )?;
        self.storages.insert(node_id, engine.clone());

        ReplicaReadLockChecker::new(concurrency_manager.clone()).register(&mut coprocessor_host);

        // Create deadlock service.
        let deadlock_service = lock_mgr.deadlock_service();

        // Create pd client, snapshot manager, server.
        let (resolver, state) =
            resolve::new_resolver(Arc::clone(&self.pd_client), &bg_worker, extension.clone());
        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(cfg.server.snap_io_max_bytes_per_sec.0 as i64)
            .max_total_size(cfg.server.snap_max_total_size.0)
            .encryption_key_manager(key_manager)
            .max_per_file_size(cfg.raft_store.max_snapshot_file_raw_size.0)
            .enable_multi_snapshot_files(true)
            .enable_receive_tablet_snapshot(cfg.raft_store.enable_v2_compatible_learner)
            .build(tmp_str);
        self.snap_mgrs.insert(node_id, snap_mgr.clone());
        let server_cfg = Arc::new(VersionTrack::new(cfg.server.clone()));
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
            None, // TODO resource_ctl
        );
        let copr_v2 = coprocessor_v2::Endpoint::new(&cfg.coprocessor_v2);
        let mut server = None;
        // Create Debug service.
        let debug_thread_pool = Arc::new(
            TokioBuilder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .with_sys_and_custom_hooks(|| {}, || {})
                .build()
                .unwrap(),
        );

        let apply_router = system.apply_router();
        // Create node.
        let mut raft_store = cfg.raft_store.clone();
        raft_store.optimize_for(false);
        raft_store
            .validate(
                cfg.coprocessor.region_split_size(),
                cfg.coprocessor.enable_region_bucket(),
                cfg.coprocessor.region_bucket_size,
                false,
            )
            .unwrap();
        let health_controller = HealthController::new();
        let mut node = MultiRaftServer::new(
            system,
            &server_cfg.value().clone(),
            Arc::new(VersionTrack::new(raft_store)),
            cfg.storage.api_version(),
            Arc::clone(&self.pd_client),
            state,
            bg_worker.clone(),
            health_controller.clone(),
            None,
        );
        node.try_bootstrap_store(engines.clone())?;
        let node_id = node.id();

        for _ in 0..100 {
            let mut svr = Server::new(
                node_id,
                &server_cfg,
                &security_mgr,
                store.clone(),
                copr.clone(),
                copr_v2.clone(),
                resolver.clone(),
                tikv_util::Either::Left(snap_mgr.clone()),
                gc_worker.clone(),
                check_leader_scheduler.clone(),
                self.env.clone(),
                None,
                debug_thread_pool.clone(),
                health_controller.clone(),
                None,
            )
            .unwrap();
            svr.register_service(create_import_sst(import_service.clone()));
            svr.register_service(create_deadlock(deadlock_service.clone()));
            if let Some(svcs) = self.pending_services.get(&node_id) {
                for fact in svcs {
                    svr.register_service(fact());
                }
            }
            match svr.build_and_bind() {
                Ok(_) => {
                    server = Some(svr);
                    break;
                }
                Err(Error::Grpc(GrpcError::BindFail(ref addr, ref port))) => {
                    // Servers may meet the error, when we restart them.
                    tikv_util::debug!("fail to create a server: bind fail {:?}", (addr, port));
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(ref e) => panic!("fail to create a server: {:?}", e),
            }
        }
        let mut server = server.unwrap();
        let addr = server.listening_addr();
        cfg.server.addr = format!("{}", addr);
        let simulate_trans = SimulateTransport::new(server.transport());
        let max_grpc_thread_count = cfg.server.grpc_concurrency;
        let server_cfg = Arc::new(VersionTrack::new(cfg.server.clone()));

        let packed_envs = engine_store_ffi::core::PackedEnvs {
            engine_store_cfg: cfg.proxy_cfg.engine_store.clone(),
            pd_endpoints: cfg.pd.endpoints.clone(),
            snap_handle_pool_size: cfg.proxy_cfg.raft_store.snap_handle_pool_size,
        };
        tiflash_ob.init_forwarder(
            node_id,
            engines.kv.clone(),
            engines.raft.clone(),
            importer.clone(),
            simulate_trans.clone(),
            snap_mgr.clone(),
            packed_envs,
            DebugStruct::default(),
            key_mgr_cloned,
        );
        engines
            .kv
            .proxy_ext
            .engine_store_hub
            .as_ref()
            .unwrap()
            .set_store_id(node_id);

        // Register the role change observer of the lock manager.
        lock_mgr.register_detector_role_change_observer(&mut coprocessor_host);

        let max_unified_read_pool_thread_count = cfg.readpool.unified.max_thread_count;
        let pessimistic_txn_cfg = cfg.tikv.pessimistic_txn;

        let split_check_runner =
            SplitCheckRunner::new(engines.kv.clone(), router.clone(), coprocessor_host.clone());
        let split_check_scheduler = bg_worker.start("split-check", split_check_runner);
        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(cfg.tikv.split)));
        let auto_split_controller = AutoSplitController::new(
            split_config_manager,
            max_grpc_thread_count,
            max_unified_read_pool_thread_count,
            None,
        );

        let safe_point = Arc::new(AtomicU64::new(0));
        node.start(
            engines,
            simulate_trans.clone(),
            snap_mgr,
            pd_worker,
            store_meta,
            coprocessor_host,
            importer.clone(),
            split_check_scheduler,
            auto_split_controller,
            concurrency_manager.clone(),
            collector_reg_handle,
            None,
            GrpcServiceManager::dummy(),
            safe_point,
        )?;
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }
        self.region_info_accessors
            .insert(node_id, region_info_accessor);
        self.importers.insert(node_id, importer);
        self.health_controllers.insert(node_id, health_controller);

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
            .start(server_cfg, security_mgr, NoSnapshotCache)
            .unwrap();

        self.metas.insert(
            node_id,
            ServerMeta {
                raw_router: router,
                raw_apply_router: apply_router,
                node,
                server,
                sim_router,
                sim_trans: simulate_trans,
                gc_worker,
                rts_worker,
                rsmeter_cleanup,
            },
        );
        self.addrs.insert(node_id, format!("{}", addr));
        self.concurrency_managers
            .insert(node_id, concurrency_manager);

        Ok(node_id)
    }
}

impl Simulator<TiFlashEngine> for ServerCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: MixedClusterConfig,
        engines: Engines<TiFlashEngine, ProxyRaftEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<TiFlashEngine, ProxyRaftEngine>,
        system: RaftBatchSystem<TiFlashEngine, ProxyRaftEngine>,
    ) -> ServerResult<u64> {
        dispatch_api_version!(
            cfg.storage.api_version(),
            self.run_node_impl::<API>(
                node_id,
                cfg,
                engines,
                store_meta,
                key_manager,
                router,
                system,
            )
        )
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_paths[&node_id]
            .path()
            .to_str()
            .unwrap()
            .to_owned()
    }

    fn get_snap_mgr(&self, node_id: u64) -> &SnapManager {
        self.snap_mgrs.get(&node_id).unwrap()
    }

    fn stop_node(&mut self, node_id: u64) {
        if let Some(mut meta) = self.metas.remove(&node_id) {
            meta.server.stop().unwrap();
            meta.node.stop();
            // resolved ts worker started, let's stop it
            if let Some(worker) = meta.rts_worker {
                worker.stop_worker();
            }
            (meta.rsmeter_cleanup)();
        }
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.metas.keys().cloned().collect()
    }

    fn async_command_on_node_with_opts(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
        opts: RaftCmdExtraOpts,
    ) -> Result<()> {
        let router = match self.metas.get(&node_id) {
            None => return Err(box_err!("missing sender for store {}", node_id)),
            Some(meta) => meta.sim_router.clone(),
        };
        router.send_command(request, cb, opts)
    }

    fn async_read(
        &mut self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        cb: Callback<<engine_tiflash::MixedModeEngine as engine_traits::KvEngine>::Snapshot>,
    ) {
        match self.metas.get_mut(&node_id) {
            None => {
                let e: RaftError = box_err!("missing sender for store {}", node_id);
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                cb.invoke_with_response(resp);
            }
            Some(meta) => {
                let read_ctx = ReadContext::new(batch_id, None);
                meta.sim_router.read(read_ctx, request, cb).unwrap();
            }
        };
    }

    fn send_raft_msg(&mut self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.raft_client.send(raft_msg).unwrap();
        self.raft_client.flush();
        Ok(())
    }

    fn clear_send_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_trans
            .clear_filters();
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.metas
            .get_mut(&node_id)
            .unwrap()
            .sim_router
            .clear_filters();
    }

    fn get_router(&self, node_id: u64) -> Option<RaftRouter<TiFlashEngine, ProxyRaftEngine>> {
        self.metas.get(&node_id).map(|m| m.raw_router.clone())
    }

    fn async_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback<engine_rocks::RocksSnapshot>,
    ) -> Result<()> {
        self.async_command_on_node_with_opts(node_id, request, cb, Default::default())
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let (cb, mut rx) = super::cluster::make_cb::<TiFlashEngine>(&request);

        match self.async_command_on_node(node_id, request, cb) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }
        rx.recv_timeout(timeout)
            .map_err(|e| RaftError::Timeout(format!("request timeout for {:?}: {:?}", timeout, e)))
    }

    fn add_send_filter(&mut self, _node_id: u64, _filter: Box<dyn Filter>) {
        todo!()
    }

    fn add_recv_filter(&mut self, _node_id: u64, _filter: Box<dyn Filter>) {
        todo!()
    }
}

impl Cluster<ServerCluster> {
    pub fn must_get_snapshot_of_region(&mut self, region_id: u64) -> RegionSnapshot<RocksSnapshot> {
        let mut try_snapshot = || -> Option<RegionSnapshot<RocksSnapshot>> {
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

    #[allow(clippy::type_complexity)]
    pub fn register_hook(
        &self,
        node_id: u64,
        register: Box<dyn Fn(&mut CoprocessorHost<TiFlashEngine>)>,
    ) {
        self.sim
            .wl()
            .coprocessor_hooks
            .entry(node_id)
            .or_default()
            .push(register);
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ProxyConfig::default())
}

pub fn new_server_cluster_with_api_ver(
    id: u64,
    count: usize,
    _api_ver: ApiVersion,
) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ProxyConfig::default())
}

pub fn new_incompatible_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ProxyConfig::default())
}

pub fn must_new_cluster_mul(count: usize) -> (Cluster<ServerCluster>, metapb::Peer, Context) {
    must_new_and_configure_cluster_mul(count, |_| ())
}

pub fn must_new_and_configure_cluster(
    configure: impl FnMut(&mut Cluster<ServerCluster>),
) -> (Cluster<ServerCluster>, metapb::Peer, Context) {
    must_new_and_configure_cluster_mul(1, configure)
}

fn must_new_and_configure_cluster_mul(
    count: usize,
    mut configure: impl FnMut(&mut Cluster<ServerCluster>),
) -> (Cluster<ServerCluster>, metapb::Peer, Context) {
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

pub fn must_new_cluster_and_kv_client() -> (Cluster<ServerCluster>, TikvClient, Context) {
    must_new_cluster_and_kv_client_mul(1)
}

pub fn must_new_cluster_and_kv_client_mul(
    count: usize,
) -> (Cluster<ServerCluster>, TikvClient, Context) {
    let (cluster, leader, ctx) = must_new_cluster_mul(count);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    (cluster, client, ctx)
}

pub fn must_new_cluster_and_debug_client() -> (Cluster<ServerCluster>, DebugClient, u64) {
    let (cluster, leader, _) = must_new_cluster_mul(1);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = DebugClient::new(channel);

    (cluster, client, leader.get_store_id())
}

pub fn must_new_and_configure_cluster_and_kv_client(
    configure: impl FnMut(&mut Cluster<ServerCluster>),
) -> (Cluster<ServerCluster>, TikvClient, Context) {
    let (cluster, leader, ctx) = must_new_and_configure_cluster(configure);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    (cluster, client, ctx)
}
