// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::Duration,
    usize,
};

use api_version::{dispatch_api_version, KvFormat};
use causal_ts::CausalTsProvider;
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use encryption_export::DataKeyManager;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_test::raft::RaftTestEngine;
use engine_traits::{Engines, MiscExt};
use futures::executor::block_on;
use grpcio::{ChannelBuilder, EnvBuilder, Environment, Error as GrpcError, Service};
use grpcio_health::HealthService;
use kvproto::{
    deadlock::create_deadlock,
    debugpb::{create_debug, DebugClient},
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
    router::{LocalReadRouter, RaftStoreBlackHole, RaftStoreRouter, ServerRaftStoreRouter},
    store::{
        fsm::{store::StoreMeta, ApplyRouter, RaftBatchSystem, RaftRouter},
        msg::RaftCmdExtraOpts,
        AutoSplitController, Callback, CheckLeaderRunner, LocalReader, RegionSnapshot, SnapManager,
        SnapManagerBuilder, SplitCheckRunner, SplitConfigManager,
    },
    Result,
};
use resource_metering::{CollectorRegHandle, ResourceTagFactory};
use security::SecurityManager;
use tempfile::TempDir;
use tikv::{
    config::ConfigController,
    coprocessor, coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::ReadPool,
    server::{
        create_raft_storage,
        gc_worker::GcWorker,
        load_statistics::ThreadLoadPool,
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve::{self, StoreAddrResolver},
        service::DebugService,
        ConnectionBuilder, Error, Node, PdStoreAddrResolver, RaftClient, RaftKv,
        Result as ServerResult, Server, ServerTransport,
    },
    storage::{
        self,
        kv::SnapContext,
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine,
    },
};
use tikv_util::{
    config::VersionTrack,
    quota_limiter::QuotaLimiter,
    sys::thread::ThreadBuildWrapper,
    time::ThreadReadId,
    worker::{Builder as WorkerBuilder, LazyWorker},
    HandyRwLock,
};
use tokio::runtime::Builder as TokioBuilder;
use txn_types::TxnExtraScheduler;

use super::*;
use crate::Config;

type SimulateStoreTransport = SimulateTransport<ServerRaftStoreRouter<RocksEngine, RaftTestEngine>>;
type SimulateServerTransport =
    SimulateTransport<ServerTransport<SimulateStoreTransport, PdStoreAddrResolver, RocksEngine>>;

pub type SimulateEngine = RaftKv<RocksEngine, SimulateStoreTransport>;

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
        cb: Box<dyn FnOnce(ServerResult<String>) + Send>,
    ) -> ServerResult<()> {
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
    node: Node<TestPdClient, RocksEngine, RaftTestEngine>,
    server: Server<SimulateStoreTransport, PdStoreAddrResolver, SimulateEngine>,
    sim_router: SimulateStoreTransport,
    sim_trans: SimulateServerTransport,
    raw_router: RaftRouter<RocksEngine, RaftTestEngine>,
    raw_apply_router: ApplyRouter<RocksEngine>,
    gc_worker: GcWorker<RaftKv<RocksEngine, SimulateStoreTransport>, SimulateStoreTransport>,
    rts_worker: Option<LazyWorker<resolved_ts::Task<RocksSnapshot>>>,
    rsmeter_cleanup: Box<dyn FnOnce()>,
}

type PendingServices = Vec<Box<dyn Fn() -> Service>>;
type CopHooks = Vec<Box<dyn Fn(&mut CoprocessorHost<RocksEngine>)>>;

pub struct ServerCluster {
    metas: HashMap<u64, ServerMeta>,
    addrs: AddressMap,
    pub storages: HashMap<u64, SimulateEngine>,
    pub region_info_accessors: HashMap<u64, RegionInfoAccessor>,
    pub importers: HashMap<u64, Arc<SstImporter>>,
    pub pending_services: HashMap<u64, PendingServices>,
    pub coprocessor_hooks: HashMap<u64, CopHooks>,
    pub health_services: HashMap<u64, HealthService>,
    pub security_mgr: Arc<SecurityManager>,
    pub txn_extra_schedulers: HashMap<u64, Arc<dyn TxnExtraScheduler>>,
    snap_paths: HashMap<u64, TempDir>,
    snap_mgrs: HashMap<u64, SnapManager>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient<AddressMap, RaftStoreBlackHole, RocksEngine>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,
    env: Arc<Environment>,
    pub causal_ts_providers: HashMap<u64, Arc<dyn CausalTsProvider>>,
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
        // We don't actually need to handle snapshot message, just create a dead worker to make it compile.
        let worker = LazyWorker::new("snap-worker");
        let conn_builder = ConnectionBuilder::new(
            env.clone(),
            Arc::default(),
            security_mgr.clone(),
            map.clone(),
            RaftStoreBlackHole,
            worker.scheduler(),
            Arc::new(ThreadLoadPool::with_threshold(usize::MAX)),
        );
        let raft_client = RaftClient::new(conn_builder);
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
            health_services: HashMap::default(),
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

    pub fn get_apply_router(&self, node_id: u64) -> ApplyRouter<RocksEngine> {
        self.metas.get(&node_id).unwrap().raw_apply_router.clone()
    }

    pub fn get_server_router(&self, node_id: u64) -> SimulateStoreTransport {
        self.metas.get(&node_id).unwrap().sim_router.clone()
    }

    /// To trigger GC manually.
    pub fn get_gc_worker(
        &self,
        node_id: u64,
    ) -> &GcWorker<RaftKv<RocksEngine, SimulateStoreTransport>, SimulateStoreTransport> {
        &self.metas.get(&node_id).unwrap().gc_worker
    }

    pub fn get_concurrency_manager(&self, node_id: u64) -> ConcurrencyManager {
        self.concurrency_managers.get(&node_id).unwrap().clone()
    }

    pub fn get_causal_ts_provider(&self, node_id: u64) -> Option<Arc<dyn CausalTsProvider>> {
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
        mut cfg: Config,
        engines: Engines<RocksEngine, RaftTestEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<RocksEngine, RaftTestEngine>,
        system: RaftBatchSystem<RocksEngine, RaftTestEngine>,
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

        let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
        let raft_router = ServerRaftStoreRouter::new(router.clone(), local_reader);
        let sim_router = SimulateTransport::new(raft_router.clone());

        let raft_engine = RaftKv::new(sim_router.clone(), engines.kv.clone());

        // Create coprocessor.
        let mut coprocessor_host = CoprocessorHost::new(router.clone(), cfg.coprocessor.clone());
        let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);

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
            raft_engine.clone(),
        ));

        let mut engine = RaftKv::new(sim_router.clone(), engines.kv.clone());
        if let Some(scheduler) = self.txn_extra_schedulers.remove(&node_id) {
            engine.set_txn_extra_scheduler(scheduler);
        }

        let latest_ts =
            block_on(self.pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut gc_worker = GcWorker::new(
            engine.clone(),
            sim_router.clone(),
            tx,
            cfg.gc.clone(),
            Default::default(),
        );
        gc_worker.start().unwrap();
        gc_worker
            .start_observe_lock_apply(&mut coprocessor_host, concurrency_manager.clone())
            .unwrap();

        let rts_worker = if cfg.resolved_ts.enable {
            // Resolved ts worker
            let mut rts_worker = LazyWorker::new("resolved-ts");
            let rts_ob = resolved_ts::Observer::new(rts_worker.scheduler());
            rts_ob.register_to(&mut coprocessor_host);
            // Resolved ts endpoint
            let rts_endpoint = resolved_ts::Endpoint::new(
                &cfg.resolved_ts,
                rts_worker.scheduler(),
                raft_router.clone(),
                store_meta.clone(),
                self.pd_client.clone(),
                concurrency_manager.clone(),
                self.env.clone(),
                self.security_mgr.clone(),
                resolved_ts::DummySinker::new(),
            );
            // Start the worker
            rts_worker.start(rts_endpoint);
            Some(rts_worker)
        } else {
            None
        };

        if ApiVersion::V2 == F::TAG {
            let causal_ts_provider = Arc::new(
                block_on(causal_ts::BatchTsoProvider::new_opt(
                    self.pd_client.clone(),
                    cfg.causal_ts.renew_interval.0,
                    cfg.causal_ts.renew_batch_min_size,
                ))
                .unwrap(),
            );
            self.causal_ts_providers
                .insert(node_id, causal_ts_provider.clone());
            let causal_ob = causal_ts::CausalObserver::new(causal_ts_provider);
            causal_ob.register_to(&mut coprocessor_host);
        }

        // Start resource metering.
        let (res_tag_factory, collector_reg_handle, rsmeter_cleanup) =
            self.init_resource_metering(&cfg.resource_metering);

        let check_leader_runner = CheckLeaderRunner::new(store_meta.clone());
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
        let store = create_raft_storage::<_, _, _, F>(
            engine,
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
        )?;
        self.storages.insert(node_id, raft_engine);

        ReplicaReadLockChecker::new(concurrency_manager.clone()).register(&mut coprocessor_host);

        // Create import service.
        let importer = {
            let dir = Path::new(engines.kv.path()).join("import-sst");
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
            sim_router.clone(),
            engines.kv.clone(),
            Arc::clone(&importer),
        );

        // Create deadlock service.
        let deadlock_service = lock_mgr.deadlock_service();

        // Create pd client, snapshot manager, server.
        let (resolver, state) =
            resolve::new_resolver(Arc::clone(&self.pd_client), &bg_worker, router.clone());
        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(cfg.server.snap_max_write_bytes_per_sec.0 as i64)
            .max_total_size(cfg.server.snap_max_total_size.0)
            .encryption_key_manager(key_manager)
            .max_per_file_size(cfg.raft_store.max_snapshot_file_raw_size.0)
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
        let debug_service = DebugService::new(
            engines.clone(),
            debug_thread_handle,
            raft_router,
            ConfigController::default(),
        );

        let apply_router = system.apply_router();
        // Create node.
        let mut raft_store = cfg.raft_store.clone();
        raft_store
            .validate(
                cfg.coprocessor.region_split_size,
                cfg.coprocessor.enable_region_bucket,
                cfg.coprocessor.region_bucket_size,
            )
            .unwrap();
        let health_service = HealthService::default();
        let mut node = Node::new(
            system,
            &server_cfg.value().clone(),
            Arc::new(VersionTrack::new(raft_store)),
            cfg.storage.api_version(),
            Arc::clone(&self.pd_client),
            state,
            bg_worker.clone(),
            Some(health_service.clone()),
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
                sim_router.clone(),
                resolver.clone(),
                snap_mgr.clone(),
                gc_worker.clone(),
                check_leader_scheduler.clone(),
                self.env.clone(),
                None,
                debug_thread_pool.clone(),
                health_service.clone(),
            )
            .unwrap();
            svr.register_service(create_import_sst(import_service.clone()));
            svr.register_service(create_debug(debug_service.clone()));
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
        let simulate_trans = SimulateTransport::new(trans);
        let server_cfg = Arc::new(VersionTrack::new(cfg.server.clone()));

        // Register the role change observer of the lock manager.
        lock_mgr.register_detector_role_change_observer(&mut coprocessor_host);

        let pessimistic_txn_cfg = cfg.tikv.pessimistic_txn;

        let split_check_runner =
            SplitCheckRunner::new(engines.kv.clone(), router.clone(), coprocessor_host.clone());
        let split_check_scheduler = bg_worker.start("split-check", split_check_runner);
        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(cfg.tikv.split)));
        let auto_split_controller = AutoSplitController::new(split_config_manager);
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
        )?;
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }
        self.region_info_accessors
            .insert(node_id, region_info_accessor);
        self.importers.insert(node_id, importer);
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

        server.start(server_cfg, security_mgr).unwrap();

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

impl Simulator for ServerCluster {
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        engines: Engines<RocksEngine, RaftTestEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<RocksEngine, RaftTestEngine>,
        system: RaftBatchSystem<RocksEngine, RaftTestEngine>,
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
        &self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
    ) {
        match self.metas.get(&node_id) {
            None => {
                let e: RaftError = box_err!("missing sender for store {}", node_id);
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                cb.invoke_with_response(resp);
            }
            Some(meta) => {
                meta.sim_router.read(batch_id, request, cb).unwrap();
            }
        };
    }

    fn send_raft_msg(&mut self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.raft_client.send(raft_msg).unwrap();
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

    fn get_router(&self, node_id: u64) -> Option<RaftRouter<RocksEngine, RaftTestEngine>> {
        self.metas.get(&node_id).map(|m| m.raw_router.clone())
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

            let storage = self.sim.rl().storages.get(&store_id).unwrap().clone();
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
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ApiVersion::V1)
}

pub fn new_server_cluster_with_api_ver(
    id: u64,
    count: usize,
    api_ver: ApiVersion,
) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, api_ver)
}

pub fn new_incompatible_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id, true));
    let sim = Arc::new(RwLock::new(ServerCluster::new(Arc::clone(&pd_client))));
    Cluster::new(id, count, sim, pd_client, ApiVersion::V1)
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
