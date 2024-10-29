// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server
//! components, and handling errors (mostly by aborting and reporting to the
//! user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be
//! explicitly stopped. We keep these components in the `TikvServer` struct.

use std::{
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::AtomicU64,
        mpsc::{self, sync_channel},
        Arc,
    },
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use backup::disk_snap::Env;
use backup_stream::{
    config::BackupStreamConfigManager, metadata::store::PdStore, observer::BackupStreamObserver,
    BackupStreamResolver,
};
use causal_ts::CausalTsProviderImpl;
use cdc::CdcConfigManager;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{from_rocks_compression_type, RocksEngine, RocksStatistics};
use engine_traits::{Engines, KvEngine, MiscExt, RaftEngine, TabletRegistry, CF_DEFAULT, CF_WRITE};
use file_system::{get_io_rate_limiter, BytesFetcher, MetricsManager as IoMetricsManager};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use health_controller::HealthController;
use in_memory_engine::InMemoryEngineStatistics;
use kvproto::{
    brpb::create_backup, cdcpb_grpc::create_change_data, deadlock::create_deadlock,
    debugpb_grpc::create_debug, diagnosticspb::create_diagnostics,
    import_sstpb_grpc::create_import_sst, kvrpcpb::ApiVersion, logbackuppb::create_log_backup,
    resource_usage_agent::create_resource_metering_pub_sub,
};
use pd_client::{
    meta_storage::{Checked, Sourced},
    PdClient, RpcClient,
};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{
        BoxConsistencyCheckObserver, ConsistencyCheckMethod, CoprocessorHost,
        RawConsistencyCheckObserver,
    },
    store::{
        config::RaftstoreConfigManager, memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE,
        AutoSplitController, CheckLeaderRunner, SplitConfigManager, TabletSnapManager,
    },
    RegionInfoAccessor,
};
use raftstore_v2::{
    router::{DiskSnapBackupHandle, PeerMsg, RaftRouter},
    StateStorage,
};
use resolved_ts::Task;
use resource_control::{config::ResourceContrlCfgMgr, ResourceGroupManager};
use security::SecurityManager;
use service::{service_event::ServiceEvent, service_manager::GrpcServiceManager};
use tikv::{
    config::{
        loop_registry, ConfigController, ConfigurableDb, DbConfigManger, DbType, LogConfigManager,
        MemoryConfigManager, TikvConfig,
    },
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{
        build_yatp_read_pool, ReadPool, ReadPoolConfigManager, UPDATE_EWMA_TIME_SLICE_INTERVAL,
    },
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        debug::Debugger,
        debug2::DebuggerImplV2,
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        KvEngineFactoryBuilder, NodeV2, RaftKv2, Server, CPU_CORES_QUOTA_GAUGE, GRPC_THREAD_PREFIX,
        MEMORY_LIMIT_GAUGE,
    },
    storage::{
        self,
        config::EngineType,
        config_manager::StorageConfigManger,
        kv::LocalTablets,
        mvcc::MvccConsistencyCheckObserver,
        txn::{
            flow_controller::{FlowController, TabletFlowController},
            txn_status_cache::TxnStatusCache,
        },
        Engine, Storage,
    },
};
use tikv_alloc::{add_thread_memory_accessor, remove_thread_memory_accessor};
use tikv_util::{
    check_environment_variables,
    config::VersionTrack,
    memory::MemoryQuota,
    mpsc as TikvMpsc,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{disk, path_in_diff_mount_point, register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler},
    yatp_pool::CleanupMethod,
    Either,
};
use tokio::runtime::Builder;

use crate::{
    common::{
        ConfiguredRaftEngine, DiskUsageChecker, EngineMetricsManager, EnginesResourceInfo,
        TikvServerCore,
    },
    memory::*,
    setup::*,
    signal_handler,
    tikv_util::sys::thread::ThreadBuildWrapper,
    utils,
};

#[inline]
fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(
    config: TikvConfig,
    service_event_tx: TikvMpsc::Sender<ServiceEvent>,
    service_event_rx: TikvMpsc::Receiver<ServiceEvent>,
) {
    let mut tikv = TikvServer::<CER>::init::<F>(config, service_event_tx.clone());

    // Must be called after `TikvServer::init`.
    let memory_limit = tikv.core.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.core.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.core.check_conflict_addr();
    tikv.core.init_fs();
    tikv.core.init_yatp();
    tikv.core.init_encryption();
    let fetcher = tikv.core.init_io_utility();
    let listener = tikv.core.init_flow_receiver();
    let engines_info = tikv.init_engines(listener);
    let server_config = tikv.init_servers::<F>();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_cgroup_monitor();
    tikv.init_storage_stats_task();
    tikv.run_server(server_config);
    tikv.run_status_server();
    tikv.core.init_quota_tuning_task(tikv.quota_limiter.clone());

    // Build a background worker for handling signals.
    {
        let kv_statistics = tikv.kv_statistics.clone();
        let raft_statistics = tikv.raft_statistics.clone();
        // TODO: support signal dump stats
        std::thread::spawn(move || {
            signal_handler::wait_for_signal(
                None as Option<Engines<RocksEngine, CER>>,
                kv_statistics,
                raft_statistics,
                Some(service_event_tx),
            )
        });
    }
    loop {
        if let Ok(service_event) = service_event_rx.recv() {
            match service_event {
                ServiceEvent::PauseGrpc => {
                    tikv.pause();
                }
                ServiceEvent::ResumeGrpc => {
                    tikv.resume();
                }
                ServiceEvent::Exit => {
                    break;
                }
            }
        }
    }
    tikv.stop();
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(
    config: TikvConfig,
    service_event_tx: TikvMpsc::Sender<ServiceEvent>,
    service_event_rx: TikvMpsc::Receiver<ServiceEvent>,
) {
    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<RocksEngine, API>(config, service_event_tx, service_event_rx)
        } else {
            run_impl::<RaftLogEngine, API>(config, service_event_tx, service_event_rx)
        }
    })
}

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CGROUP_MONITOR_INTERVAL: Duration = Duration::from_secs(10);

/// A complete TiKV server.
struct TikvServer<ER: RaftEngine> {
    core: TikvServerCore,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: Option<RaftRouter<RocksEngine, ER>>,
    node: Option<NodeV2<RpcClient, RocksEngine, ER>>,
    resolver: Option<resolve::PdStoreAddrResolver>,
    snap_mgr: Option<TabletSnapManager>, // Will be filled in `init_servers`.
    engines: Option<TikvEngines<RocksEngine, ER>>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    in_memory_engine_statistics: Option<Arc<InMemoryEngineStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    servers: Option<Servers<RocksEngine, ER>>,
    region_info_accessor: Option<RegionInfoAccessor>,
    coprocessor_host: Option<CoprocessorHost<RocksEngine>>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    cdc_worker: Option<Box<LazyWorker<cdc::Task>>>,
    cdc_scheduler: Option<Scheduler<cdc::Task>>,
    cdc_memory_quota: Option<Arc<MemoryQuota>>,
    backup_stream_scheduler: Option<tikv_util::worker::Scheduler<backup_stream::Task>>,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
    tablet_registry: Option<TabletRegistry<RocksEngine>>,
    resolved_ts_scheduler: Option<Scheduler<Task>>,
    grpc_service_mgr: GrpcServiceManager,
}

struct TikvEngines<EK: KvEngine, ER: RaftEngine> {
    raft_engine: ER,
    engine: RaftKv2<EK, ER>,
}

struct Servers<EK: KvEngine, ER: RaftEngine> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    importer: Arc<SstImporter<EK>>,
    rsmeter_pubsub_service: resource_metering::PubSubService,
}

type LocalServer<EK, ER> = Server<resolve::PdStoreAddrResolver, RaftKv2<EK, ER>>;

impl<ER> TikvServer<ER>
where
    ER: RaftEngine,
{
    fn init<F: KvFormat>(
        mut config: TikvConfig,
        tx: TikvMpsc::Sender<ServiceEvent>,
    ) -> TikvServer<ER> {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let props = tikv_util::thread_group::current_properties();
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .after_start(move || {
                    tikv_util::thread_group::set_properties(props.clone());

                    // SAFETY: we will call `remove_thread_memory_accessor` at before_stop.
                    unsafe { add_thread_memory_accessor() };
                })
                .before_stop(|| {
                    remove_thread_memory_accessor();
                })
                .build(),
        );
        let pd_client = TikvServerCore::connect_to_pd_cluster(
            &mut config,
            env.clone(),
            Arc::clone(&security_mgr),
        );

        // Initialize and check config
        let cfg_controller = TikvServerCore::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        // use different quota for front-end and back-end requests
        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.background_cpu_time,
            config.quota.background_write_bandwidth,
            config.quota.background_read_bandwidth,
            config.quota.max_delay_duration,
            config.quota.enable_auto_tune,
        ));

        let resource_manager = if config.resource_control.enabled {
            let mgr = Arc::new(ResourceGroupManager::new(config.resource_control.clone()));
            let io_bandwidth = config.storage.io_rate_limit.max_bytes_per_sec.0;
            resource_control::start_periodic_tasks(
                &mgr,
                pd_client.clone(),
                &background_worker,
                io_bandwidth,
            );
            Some(mgr)
        } else {
            None
        };

        let mut causal_ts_provider = None;
        if let ApiVersion::V2 = F::TAG {
            let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
                pd_client.clone(),
                config.causal_ts.renew_interval.0,
                config.causal_ts.alloc_ahead_buffer.0,
                config.causal_ts.renew_batch_min_size,
                config.causal_ts.renew_batch_max_size,
            ));
            if let Err(e) = tso {
                fatal!("Causal timestamp provider initialize failed: {:?}", e);
            }
            causal_ts_provider = Some(Arc::new(tso.unwrap().into()));
            info!("Causal timestamp provider startup.");
        }

        TikvServer {
            core: TikvServerCore {
                config,
                store_path,
                lock_files: vec![],
                encryption_key_manager: None,
                flow_info_sender: None,
                flow_info_receiver: None,
                to_stop: vec![],
                background_worker,
            },
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router: None,
            node: None,
            resolver: None,
            snap_mgr: None,
            engines: None,
            kv_statistics: None,
            in_memory_engine_statistics: None,
            raft_statistics: None,
            servers: None,
            region_info_accessor: None,
            coprocessor_host: None,
            concurrency_manager,
            env,
            cdc_worker: None,
            cdc_scheduler: None,
            cdc_memory_quota: None,
            backup_stream_scheduler: None,
            sst_worker: None,
            quota_limiter,
            resource_manager,
            causal_ts_provider,
            tablet_registry: None,
            resolved_ts_scheduler: None,
            grpc_service_mgr: GrpcServiceManager::new(tx),
        }
    }

    fn init_gc_worker(&mut self) -> GcWorker<RaftKv2<RocksEngine, ER>> {
        let engines = self.engines.as_ref().unwrap();
        let gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.core.flow_info_sender.take().unwrap(),
            self.core.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
            Arc::new(self.region_info_accessor.clone().unwrap()),
        );

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers<F: KvFormat>(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let flow_controller = Arc::new(FlowController::Tablet(TabletFlowController::new(
            &self.core.config.storage.flow_control,
            self.tablet_registry.clone().unwrap(),
            self.core.flow_info_receiver.take().unwrap(),
        )));
        let mut gc_worker = self.init_gc_worker();
        let ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
        let ttl_scheduler = ttl_checker.scheduler();

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        cfg_controller.register(tikv::config::Module::Log, Box::new(LogConfigManager));
        cfg_controller.register(tikv::config::Module::Memory, Box::new(MemoryConfigManager));

        let lock_mgr = LockManager::new(&self.core.config.pessimistic_txn);
        cfg_controller.register(
            tikv::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let engines = self.engines.as_mut().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = raftstore_v2::PdReporter::new(
            pd_worker.scheduler(),
            slog_global::borrow_global().new(slog::o!()),
        );

        let unified_read_pool = if self.core.config.readpool.is_unified_pool_enabled() {
            let resource_ctl = self
                .resource_manager
                .as_ref()
                .map(|m| m.derive_controller("unified-read-pool".into(), true));
            Some(build_yatp_read_pool(
                &self.core.config.readpool.unified,
                pd_sender.clone(),
                engines.engine.clone(),
                resource_ctl,
                CleanupMethod::Remote(self.core.background_worker.remote()),
                true,
            ))
        } else {
            None
        };
        if let Some(unified_read_pool) = &unified_read_pool {
            let handle = unified_read_pool.handle();
            self.core.background_worker.spawn_interval_task(
                UPDATE_EWMA_TIME_SLICE_INTERVAL,
                move || {
                    handle.update_ewma_time_slice();
                },
            );
        }

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .with_sys_and_custom_hooks(
                    move || {
                        tikv_util::thread_group::set_properties(props.clone());
                    },
                    || {},
                )
                .build()
                .unwrap(),
        );

        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(
                self.core.config.resource_metering.precision.as_millis(),
            );
        self.core.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.core.config.resource_metering.clone(),
                collector_reg_handle.clone(),
            );
        self.core.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.core.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle.clone(),
        );
        self.core.to_stop.push(single_target_worker);
        let rsmeter_pubsub_service = resource_metering::PubSubService::new(data_sink_reg_handle);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.core.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );
        if let Some(resource_ctl) = &self.resource_manager {
            cfg_controller.register(
                tikv::config::Module::ResourceControl,
                Box::new(ResourceContrlCfgMgr::new(resource_ctl.get_config().clone())),
            );
        }

        let storage_read_pool_handle = if self.core.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.core.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };
        let txn_status_cache = Arc::new(TxnStatusCache::new(
            self.core.config.storage.txn_status_cache_capacity,
        ));

        let storage = Storage::<_, _, F>::from_engine(
            engines.engine.clone(),
            &self.core.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            flow_controller.clone(),
            pd_sender.clone(),
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            self.causal_ts_provider.clone(),
            self.resource_manager
                .as_ref()
                .map(|m| m.derive_controller("scheduler-worker-pool".to_owned(), true)),
            self.resource_manager.clone(),
            txn_status_cache.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));
        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.tablet_registry.as_ref().unwrap().clone(),
                ttl_scheduler,
                flow_controller,
                storage.get_scheduler(),
            )),
        );

        let (resolver, state) = resolve::new_resolver(
            self.pd_client.clone(),
            &self.core.background_worker,
            storage.get_engine().raft_extension(),
        );
        self.resolver = Some(resolver);

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create snapshot manager, server.
        let snap_path = self
            .core
            .store_path
            .join(Path::new("tablet_snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let snap_mgr =
            match TabletSnapManager::new(&snap_path, self.core.encryption_key_manager.clone()) {
                Ok(mgr) => mgr,
                Err(e) => fatal!("failed to create snapshot manager at {}: {}", snap_path, e),
            };

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.core.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.core.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        let mut unified_read_pool_scale_receiver = None;
        if self.core.config.readpool.is_unified_pool_enabled() {
            let (unified_read_pool_scale_notifier, rx) = mpsc::sync_channel(10);
            cfg_controller.register(
                tikv::config::Module::Readpool,
                Box::new(ReadPoolConfigManager::new(
                    unified_read_pool.as_ref().unwrap().handle(),
                    unified_read_pool_scale_notifier,
                    &self.core.background_worker,
                    self.core.config.readpool.unified.max_thread_count,
                    self.core.config.readpool.unified.auto_adjust_pool_size,
                )),
            );
            unified_read_pool_scale_receiver = Some(rx);
        }

        // Run check leader in a dedicate thread, because it is time sensitive
        // and crucial to TiCDC replication lag.
        let check_leader_worker =
            Box::new(WorkerBuilder::new("check-leader").thread_count(1).create());
        // Create check leader runer.
        let check_leader_runner = CheckLeaderRunner::new(
            self.router.as_ref().unwrap().store_meta().clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let check_leader_scheduler = check_leader_worker.start("check-leader", check_leader_runner);
        self.core.to_stop.push(check_leader_worker);

        // Create cdc worker.
        let mut cdc_worker = self.cdc_worker.take().unwrap();
        let cdc_scheduler = self.cdc_scheduler.clone().unwrap();

        let cdc_memory_quota = self.cdc_memory_quota.as_ref().unwrap();
        // Register cdc observer.
        let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone(), cdc_memory_quota.clone());
        cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // Register cdc config manager.
        cfg_controller.register(
            tikv::config::Module::Cdc,
            Box::new(CdcConfigManager(cdc_scheduler.clone())),
        );
        // Start cdc endpoint.
        let cdc_endpoint = cdc::Endpoint::new(
            self.core.config.server.cluster_id,
            &self.core.config.cdc,
            &self.core.config.resolved_ts,
            self.core.config.storage.engine == EngineType::RaftKv2,
            self.core.config.storage.api_version(),
            self.pd_client.clone(),
            cdc_scheduler,
            self.router.clone().unwrap(),
            LocalTablets::Registry(self.tablet_registry.as_ref().unwrap().clone()),
            cdc_ob,
            self.router.as_ref().unwrap().store_meta().clone(),
            self.concurrency_manager.clone(),
            self.env.clone(),
            self.security_mgr.clone(),
            cdc_memory_quota.clone(),
            self.causal_ts_provider.clone(),
        );
        cdc_worker.start_with_timer(cdc_endpoint);
        self.core.to_stop.push(cdc_worker);

        // Create resolved ts.
        if self.core.config.resolved_ts.enable {
            let mut rts_worker = Box::new(LazyWorker::new("resolved-ts"));
            // Register the resolved ts observer
            let resolved_ts_ob = resolved_ts::Observer::new(rts_worker.scheduler());
            resolved_ts_ob.register_to(self.coprocessor_host.as_mut().unwrap());
            // Register config manager for resolved ts worker
            cfg_controller.register(
                tikv::config::Module::ResolvedTs,
                Box::new(resolved_ts::ResolvedTsConfigManager::new(
                    rts_worker.scheduler(),
                )),
            );
            let rts_endpoint = resolved_ts::Endpoint::new(
                &self.core.config.resolved_ts,
                rts_worker.scheduler(),
                self.router.clone().unwrap(),
                self.router.as_ref().unwrap().store_meta().clone(),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
                self.env.clone(),
                self.security_mgr.clone(),
                storage.get_scheduler().get_txn_status_cache(),
            );
            self.resolved_ts_scheduler = Some(rts_worker.scheduler());
            rts_worker.start_with_timer(rts_endpoint);
            self.core.to_stop.push(rts_worker);
        }

        // Start backup stream
        self.backup_stream_scheduler = if self.core.config.log_backup.enable {
            // Create backup stream.
            let mut backup_stream_worker = Box::new(LazyWorker::new("backup-stream"));
            let backup_stream_scheduler = backup_stream_worker.scheduler();

            // Register backup-stream observer.
            let backup_stream_ob = BackupStreamObserver::new(backup_stream_scheduler.clone());
            backup_stream_ob.register_to(self.coprocessor_host.as_mut().unwrap());
            // Register config manager.
            cfg_controller.register(
                tikv::config::Module::BackupStream,
                Box::new(BackupStreamConfigManager::new(
                    backup_stream_worker.scheduler(),
                    self.core.config.log_backup.clone(),
                )),
            );

            // build stream backup encryption manager
            let backup_encryption_manager =
                utils::build_backup_encryption_manager(self.core.encryption_key_manager.clone())
                    .expect("failed to build backup encryption manager in server");

            let backup_stream_endpoint = backup_stream::Endpoint::new(
                self.node.as_ref().unwrap().id(),
                PdStore::new(Checked::new(Sourced::new(
                    Arc::clone(&self.pd_client),
                    pd_client::meta_storage::Source::LogBackup,
                ))),
                self.core.config.log_backup.clone(),
                self.core.config.resolved_ts.clone(),
                backup_stream_scheduler.clone(),
                backup_stream_ob,
                self.region_info_accessor.as_ref().unwrap().clone(),
                self.router.clone().unwrap(),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
                BackupStreamResolver::V2(self.router.clone().unwrap(), PhantomData),
                backup_encryption_manager.clone(),
                txn_status_cache.clone(),
            );
            backup_stream_worker.start(backup_stream_endpoint);
            self.core.to_stop.push(backup_stream_worker);
            Some(backup_stream_scheduler)
        } else {
            None
        };

        let server_config = Arc::new(VersionTrack::new(self.core.config.server.clone()));

        self.core.config.raft_store.optimize_for(true);
        self.core
            .config
            .raft_store
            .validate(
                self.core.config.coprocessor.region_split_size(),
                self.core.config.coprocessor.enable_region_bucket(),
                self.core.config.coprocessor.region_bucket_size,
                true,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.core.config.raft_store.clone()));
        let health_controller = HealthController::new();

        let node = self.node.as_ref().unwrap();

        // Create coprocessor endpoint.
        let copr = coprocessor::Endpoint::new(
            &server_config.value(),
            cop_read_pool_handle,
            self.concurrency_manager.clone(),
            resource_tag_factory,
            self.quota_limiter.clone(),
            self.resource_manager.clone(),
        );
        let copr_config_manager = copr.config_manager();

        self.snap_mgr = Some(snap_mgr.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            copr,
            coprocessor_v2::Endpoint::new(&self.core.config.coprocessor_v2),
            self.resolver.clone().unwrap(),
            Either::Right(snap_mgr.clone()),
            gc_worker.clone(),
            check_leader_scheduler,
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
            health_controller,
            self.resource_manager.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
                server.get_grpc_mem_quota().clone(),
                copr_config_manager,
            )),
        );

        let import_path = self.core.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.core.config.import,
            import_path,
            self.core.encryption_key_manager.clone(),
            self.core.config.storage.api_version(),
            true,
        )
        .unwrap();
        for (cf_name, compression_type) in &[
            (
                CF_DEFAULT,
                self.core
                    .config
                    .rocksdb
                    .defaultcf
                    .bottommost_level_compression,
            ),
            (
                CF_WRITE,
                self.core
                    .config
                    .rocksdb
                    .writecf
                    .bottommost_level_compression,
            ),
        ] {
            importer.set_compression_type(cf_name, from_rocks_compression_type(*compression_type));
        }
        let importer = Arc::new(importer);

        // V2 starts split-check worker within raftstore.

        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(self.core.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(
            split_config_manager,
            self.core.config.server.grpc_concurrency,
            self.core.config.readpool.unified.max_thread_count,
            unified_read_pool_scale_receiver,
        );

        // `ConsistencyCheckObserver` must be registered before `Node::start`.
        let safe_point = Arc::new(AtomicU64::new(0));
        let observer = match self.core.config.coprocessor.consistency_check_method {
            ConsistencyCheckMethod::Mvcc => BoxConsistencyCheckObserver::new(
                MvccConsistencyCheckObserver::new(safe_point.clone()),
            ),
            ConsistencyCheckMethod::Raw => {
                BoxConsistencyCheckObserver::new(RawConsistencyCheckObserver::default())
            }
        };
        self.coprocessor_host
            .as_mut()
            .unwrap()
            .registry
            .register_consistency_check_observer(100, observer);

        self.node
            .as_mut()
            .unwrap()
            .start(
                engines.raft_engine.clone(),
                self.tablet_registry.clone().unwrap(),
                self.router.as_ref().unwrap(),
                server.transport(),
                snap_mgr,
                self.concurrency_manager.clone(),
                self.causal_ts_provider.clone(),
                self.coprocessor_host.clone().unwrap(),
                auto_split_controller,
                collector_reg_handle,
                self.core.background_worker.clone(),
                pd_worker,
                raft_store.clone(),
                &state,
                importer.clone(),
                self.core.encryption_key_manager.clone(),
                self.grpc_service_mgr.clone(),
            )
            .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager::new(
                self.node.as_mut().unwrap().refresh_config_scheduler(),
                raft_store,
            )),
        );

        // Start auto gc. Must after `Node::start` because `node_id` is initialized
        // there.
        let store_id = self.node.as_ref().unwrap().id();
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone().unwrap(),
            store_id,
        );
        gc_worker
            .start(store_id)
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config, safe_point) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        initial_metric(&self.core.config.metric);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            importer,
            rsmeter_pubsub_service,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Backup service.
        let mut backup_worker = Box::new(self.core.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_service = backup::Service::new(
            backup_scheduler,
            Env::new(DiskSnapBackupHandle, Default::default(), None),
        );
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_endpoint = backup::Endpoint::new(
            self.node.as_ref().unwrap().id(),
            engines.engine.clone(),
            self.region_info_accessor.clone().unwrap(),
            LocalTablets::Registry(self.tablet_registry.as_ref().unwrap().clone()),
            self.core.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.core.config.storage.api_version(),
            self.causal_ts_provider.clone(),
            self.resource_manager.clone(),
        );
        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start(backup_endpoint);

        // Import SST service.
        let region_info_accessor = self.region_info_accessor.as_ref().unwrap().clone();
        let import_service = ImportSstService::new(
            self.core.config.import.clone(),
            self.core.config.raft_store.raft_entry_max_size,
            engines.engine.clone(),
            LocalTablets::Registry(self.tablet_registry.as_ref().unwrap().clone()),
            servers.importer.clone(),
            Some(self.router.as_ref().unwrap().store_meta().clone()),
            self.resource_manager.clone(),
            Arc::new(region_info_accessor),
        );
        let import_cfg_mgr = import_service.get_config_manager();

        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        if let Some(sched) = self.backup_stream_scheduler.take() {
            let pitr_service = backup_stream::BackupStreamGrpcService::new(sched);
            if servers
                .server
                .register_service(create_log_backup(pitr_service))
                .is_some()
            {
                fatal!("failed to register log backup service");
            }
        }

        self.cfg_controller
            .as_mut()
            .unwrap()
            .register(tikv::config::Module::Import, Box::new(import_cfg_mgr));

        let mut debugger = DebuggerImplV2::new(
            self.tablet_registry.clone().unwrap(),
            self.engines.as_ref().unwrap().raft_engine.clone(),
            self.cfg_controller.as_ref().unwrap().clone(),
        );
        debugger.set_kv_statistics(self.kv_statistics.clone());
        debugger.set_raft_statistics(self.raft_statistics.clone());

        // Debug service.
        let resolved_ts_scheduler = Arc::new(self.resolved_ts_scheduler.clone());
        let debug_service = DebugService::new(
            debugger,
            servers.server.get_debug_thread_pool().clone(),
            engines.engine.raft_extension(),
            self.router.as_ref().unwrap().store_meta().clone(),
            Arc::new(
                move |region_id, log_locks, min_start_ts, callback| -> bool {
                    if let Some(s) = resolved_ts_scheduler.as_ref() {
                        let res = s.schedule(Task::GetDiagnosisInfo {
                            region_id,
                            log_locks,
                            min_start_ts,
                            callback,
                        });
                        res.is_ok()
                    } else {
                        false
                    }
                },
            ),
        );
        if servers
            .server
            .register_service(create_debug(debug_service))
            .is_some()
        {
            fatal!("failed to register debug service");
        }

        let cdc_service = cdc::Service::new(
            self.cdc_scheduler.as_ref().unwrap().clone(),
            self.cdc_memory_quota.as_ref().unwrap().clone(),
        );
        if servers
            .server
            .register_service(create_change_data(cdc_service))
            .is_some()
        {
            fatal!("failed to register cdc service");
        }

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.core.config.log.file.filename.clone(),
            self.core.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Lock manager.
        if servers
            .server
            .register_service(create_deadlock(servers.lock_mgr.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .start(
                self.node.as_ref().unwrap().id(),
                self.pd_client.clone(),
                self.resolver.clone().unwrap(),
                self.security_mgr.clone(),
                &self.core.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        if servers
            .server
            .register_service(create_resource_metering_pub_sub(
                servers.rsmeter_pubsub_service.clone(),
            ))
            .is_some()
        {
            warn!("failed to register resource metering pubsub service");
        }
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.tablet_registry.clone().unwrap(),
            self.kv_statistics.clone(),
            self.in_memory_engine_statistics.clone(),
            self.core.config.rocksdb.titan.enabled.map_or(false, |v| v),
            self.engines.as_ref().unwrap().raft_engine.clone(),
            self.raft_statistics.clone(),
        );
        let mut io_metrics = IoMetricsManager::new(fetcher);
        let engines_info_clone = engines_info.clone();

        // region_id -> (suffix, tablet)
        // `update` of EnginesResourceInfo is called perodically which needs this map
        // for recording the latest tablet for each region.
        // `cached_latest_tablets` is passed to `update` to avoid memory
        // allocation each time when calling `update`.
        let mut cached_latest_tablets = HashMap::default();
        self.core.background_worker.spawn_interval_task(
            DEFAULT_METRICS_FLUSH_INTERVAL,
            move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info_clone.update(now, &mut cached_latest_tablets);
            },
        );
        if let Some(limiter) = get_io_rate_limiter() {
            limiter.set_low_priority_io_adjustor_if_needed(Some(engines_info));
        }

        let mut mem_trace_metrics = MemoryTraceManager::default();
        mem_trace_metrics.register_provider(MEMTRACE_RAFTSTORE.clone());
        mem_trace_metrics.register_provider(MEMTRACE_COPROCESSOR.clone());
        self.core.background_worker.spawn_interval_task(
            DEFAULT_MEMTRACE_FLUSH_INTERVAL,
            move || {
                let now = Instant::now();
                mem_trace_metrics.flush(now);
            },
        );
    }

    fn init_storage_stats_task(&self) {
        let config_disk_capacity: u64 = self.core.config.raft_store.capacity.0;
        let data_dir = self.core.config.storage.data_dir.clone();
        let store_path = self.core.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let reserve_space = disk::get_disk_reserved_space();
        let reserve_raft_space = disk::get_raft_disk_reserved_space();
        if reserve_space == 0 && reserve_raft_space == 0 {
            info!("disk space checker not enabled");
            return;
        }
        let raft_engine = self.engines.as_ref().unwrap().raft_engine.clone();
        let tablet_registry = self.tablet_registry.clone().unwrap();
        let raft_path = raft_engine.get_engine_path().to_string();
        let separated_raft_mount_path =
            path_in_diff_mount_point(raft_path.as_str(), tablet_registry.tablet_root());
        // If the auxiliary directory of raft engine is specified, it's needed to be
        // checked. Otherwise, it's not needed to be checked. And as the configuration
        // is static, it's safe to check it only once.
        let raft_auxiliay_path = if self.core.config.raft_engine.enable {
            self.core.config.raft_engine.config().spill_dir.clone()
        } else {
            None
        };
        let (separated_raft_auxillay_mount_path, separated_raft_auxiliary_with_kvdb) =
            raft_auxiliay_path
                .as_ref()
                .map(|path| {
                    let seperated_with_kvdb =
                        path_in_diff_mount_point(path.as_str(), tablet_registry.tablet_root());
                    let seperated_with_raft =
                        path_in_diff_mount_point(path.as_str(), raft_path.as_str());
                    (
                        seperated_with_kvdb && seperated_with_raft,
                        seperated_with_kvdb,
                    )
                })
                .unwrap_or((false, false));
        let disk_usage_checker = DiskUsageChecker::new(
            store_path.as_path().to_str().unwrap().to_string(),
            raft_path,
            raft_auxiliay_path,
            separated_raft_mount_path,
            separated_raft_auxillay_mount_path,
            separated_raft_auxiliary_with_kvdb,
            reserve_space,
            reserve_raft_space,
            config_disk_capacity,
        );
        self.core.background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let snap_size = snap_mgr.total_snap_size().unwrap();
                let mut kv_size = 0;
                tablet_registry.for_each_opened_tablet(|_, cached| {
                    if let Some(tablet) = cached.latest() {
                        kv_size += tablet.get_engine_used_size().unwrap_or(0);
                    }
                    true
                });
                let raft_size = raft_engine
                    .get_engine_size()
                    .expect("get raft engine size");
                let placeholer_file_path = PathBuf::from_str(&data_dir)
                    .unwrap()
                    .join(Path::new(file_system::SPACE_PLACEHOLDER_FILE));
                let placeholder_size: u64 =
                    file_system::get_file_size(placeholer_file_path).unwrap_or(0);

                let used_size = if !separated_raft_mount_path {
                    snap_size + kv_size + raft_size + placeholder_size
                } else {
                    snap_size + kv_size + placeholder_size
                };
                // Check the disk usage and update the disk usage status.
                let (cur_disk_status, cur_kv_disk_status, raft_disk_status, capacity, available) = disk_usage_checker.inspect(used_size, raft_size);
                let prev_disk_status = disk::get_disk_status(0); //0 no need care about failpoint.
                if prev_disk_status != cur_disk_status {
                    warn!(
                        "disk usage {:?}->{:?} (raft engine usage: {:?}, kv engine usage: {:?}), seperated raft mount={}, kv available={}, snap={}, kv={}, raft={}, capacity={}",
                        prev_disk_status,
                        cur_disk_status,
                        raft_disk_status,
                        cur_kv_disk_status,
                        separated_raft_mount_path,
                        available,
                        snap_size,
                        kv_size,
                        raft_size,
                        capacity
                    );
                }
                disk::set_disk_status(cur_disk_status);
            })
    }

    fn init_sst_recovery_sender(&mut self) -> Option<Scheduler<String>> {
        if !self
            .core
            .config
            .storage
            .background_error_recovery_window
            .is_zero()
        {
            let sst_worker = Box::new(LazyWorker::new("sst-recovery"));
            let scheduler = sst_worker.scheduler();
            self.sst_worker = Some(sst_worker);
            Some(scheduler)
        } else {
            None
        }
    }

    fn init_cgroup_monitor(&mut self) {
        let mut last_cpu_quota: f64 = 0.0;
        let mut last_memory_limit: u64 = 0;
        self.core.background_worker.spawn_interval_task(
            DEFAULT_CGROUP_MONITOR_INTERVAL,
            move || {
                let cpu_quota = SysQuota::cpu_cores_quota_current();
                if cpu_quota != last_cpu_quota {
                    info!("cpu quota set to {:?}", cpu_quota);
                    CPU_CORES_QUOTA_GAUGE.set(cpu_quota);
                    last_cpu_quota = cpu_quota;
                }
                let memory_limit = SysQuota::memory_limit_in_bytes_current();
                if memory_limit != last_memory_limit {
                    info!("memory limit set to {:?}", memory_limit);
                    MEMORY_LIMIT_GAUGE.set(memory_limit as f64);
                    last_memory_limit = memory_limit;
                }
            },
        );
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(
                server_config,
                self.security_mgr.clone(),
                self.tablet_registry.clone().unwrap(),
            )
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.core.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.core.config.server.status_thread_pool_size,
                self.cfg_controller.clone().unwrap(),
                Arc::new(self.core.config.security.clone()),
                self.engines.as_ref().unwrap().engine.raft_extension(),
                self.resource_manager.clone(),
                self.grpc_service_mgr.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.core.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.core.to_stop.push(status_server);
            }
        }
    }

    fn flush_before_stop(&mut self) {
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store_pool_size".to_owned(), "10".to_owned());
            change
        };
        if let Err(e) = self
            .cfg_controller
            .as_mut()
            .unwrap()
            .update_without_persist(change)
        {
            warn!(
                "config change failed";
                "error" => ?e,
            );
        }
        let tablet_registry = self.tablet_registry.as_ref().unwrap();
        // It should not return error.
        if let Err(e) = loop_registry(tablet_registry, |cache| {
            if let Some(latest) = cache.latest() {
                latest.set_high_priority_background_threads(10, false)?;
                Ok(false)
            } else {
                Ok(true)
            }
        }) {
            warn!(
                "increase high priority background threads failed during server stop (it will impact close speed)";
                "error" => ?e,
            );
        }

        info!("server stop: flush begin");
        let engines = self.engines.as_mut().unwrap();
        let router = self.router.as_ref().unwrap();
        let mut rxs = vec![];
        engines
            .raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                let (tx, rx) = sync_channel(1);
                let flush_msg = PeerMsg::FlushBeforeClose { tx };
                if let Err(e) = router.store_router().force_send(region_id, flush_msg) {
                    warn!(
                        "flush-before-close: force send error";
                        "error" => ?e,
                        "region_id" => region_id,
                    );
                } else {
                    rxs.push(rx);
                }

                Ok(())
            })
            .unwrap();

        for rx in rxs {
            if let Err(e) = rx.recv() {
                warn!(
                    "flush-before-close: receive error";
                    "error" => ?e,
                );
            }
        }

        info!(
            "server stop: flush done";
        );
    }

    fn stop(mut self) {
        self.flush_before_stop();
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));
        self.node.as_mut().unwrap().stop();
        self.region_info_accessor.as_mut().unwrap().stop();

        servers.lock_mgr.stop();

        if let Some(sst_worker) = self.sst_worker {
            sst_worker.stop_worker();
        }

        self.core.to_stop.into_iter().for_each(|s| s.stop());
    }

    fn pause(&mut self) {
        let server = self.servers.as_mut().unwrap();
        let r = server.server.pause();
        if let Err(e) = r {
            warn!(
                "failed to pause the server";
                "err" => ?e
            );
        }
    }

    fn resume(&mut self) {
        let server = self.servers.as_mut().unwrap();
        let r = server.server.resume();
        if let Err(e) = r {
            warn!(
                "failed to resume the server";
                "err" => ?e
            );
        }
    }
}

impl<CER: ConfiguredRaftEngine> TikvServer<CER> {
    fn init_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
    ) -> Arc<EnginesResourceInfo> {
        let block_cache = self.core.config.storage.block_cache.build_shared_cache();
        let env = self
            .core
            .config
            .build_shared_rocks_env(
                self.core.encryption_key_manager.clone(),
                get_io_rate_limiter(),
            )
            .unwrap();

        // Create raft engine
        let (raft_engine, raft_statistics) = CER::build(
            &self.core.config,
            &env,
            &self.core.encryption_key_manager,
            &block_cache,
        );
        self.raft_statistics = raft_statistics;

        // Create kv engine.
        let builder = KvEngineFactoryBuilder::new(
            env,
            &self.core.config,
            block_cache,
            self.core.encryption_key_manager.clone(),
        )
        .sst_recovery_sender(self.init_sst_recovery_sender())
        .flow_listener(flow_listener);

        let mut node = NodeV2::new(
            &self.core.config.server,
            self.pd_client.clone(),
            None,
            self.resource_manager
                .as_ref()
                .map(|r| r.derive_controller("raft-v2".into(), false)),
        );
        node.try_bootstrap_store(&self.core.config.raft_store, &raft_engine)
            .unwrap_or_else(|e| fatal!("failed to bootstrap store: {:?}", e));
        assert_ne!(node.id(), 0);

        let router = node.router().clone();

        // Create kv engine.
        let builder = builder.state_storage(Arc::new(StateStorage::new(
            raft_engine.clone(),
            router.clone(),
        )));
        let factory = Box::new(builder.build());
        self.kv_statistics = Some(factory.rocks_statistics());
        let registry = TabletRegistry::new(factory, self.core.store_path.join("tablets"))
            .unwrap_or_else(|e| fatal!("failed to create tablet registry {:?}", e));
        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DbConfigManger::new(
                cfg_controller.get_current().rocksdb,
                registry.clone(),
                DbType::Kv,
            )),
        );
        self.tablet_registry = Some(registry.clone());
        raft_engine.register_config(cfg_controller);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            &self.core.config,
            registry,
            raft_engine.as_rocks_engine().cloned(),
            180, // max_samples_to_preserve
        ));

        let router = RaftRouter::new(node.id(), router);
        let mut coprocessor_host: CoprocessorHost<RocksEngine> = CoprocessorHost::new(
            router.store_router().clone(),
            self.core.config.coprocessor.clone(),
        );
        let region_info_accessor = RegionInfoAccessor::new(
            &mut coprocessor_host,
            Arc::new(|| false), // Not applicable to v2.
            Box::new(|| {
                // v2 does not support ime
                unreachable!()
            }),
        );

        let cdc_worker = Box::new(LazyWorker::new("cdc"));
        let cdc_scheduler = cdc_worker.scheduler();
        let cdc_memory_quota = Arc::new(MemoryQuota::new(
            self.core.config.cdc.sink_memory_quota.0 as _,
        ));
        let txn_extra_scheduler = cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone(), cdc_memory_quota.clone());
        let mut engine = RaftKv2::new(router.clone(), region_info_accessor.region_leaders());
        // Set txn extra scheduler immediately to make sure every clone has the
        // scheduler.
        engine.set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        self.engines = Some(TikvEngines {
            raft_engine,
            engine,
        });
        self.router = Some(router);
        self.node = Some(node);
        self.coprocessor_host = Some(coprocessor_host);
        self.region_info_accessor = Some(region_info_accessor);
        self.cdc_worker = Some(cdc_worker);
        self.cdc_scheduler = Some(cdc_scheduler);
        self.cdc_memory_quota = Some(cdc_memory_quota);

        engines_info
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use engine_rocks::raw::Env;
    use engine_traits::{
        FlowControlFactorsExt, MiscExt, SyncMutable, TabletContext, TabletRegistry, CF_DEFAULT,
    };
    use tempfile::Builder;
    use tikv::{config::TikvConfig, server::KvEngineFactoryBuilder};
    use tikv_util::{config::ReadableSize, time::Instant};

    use super::EnginesResourceInfo;

    #[test]
    fn test_engines_resource_info_update() {
        let mut config = TikvConfig::default();
        config.rocksdb.defaultcf.disable_auto_compactions = true;
        config.rocksdb.defaultcf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        config.rocksdb.writecf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        config.rocksdb.lockcf.soft_pending_compaction_bytes_limit = Some(ReadableSize(1));
        let env = Arc::new(Env::default());
        let path = Builder::new().prefix("test-update").tempdir().unwrap();
        let cache = config.storage.block_cache.build_shared_cache();

        let factory = KvEngineFactoryBuilder::new(env, &config, cache, None).build();
        let reg = TabletRegistry::new(Box::new(factory), path.path().join("tablets")).unwrap();

        for i in 1..6 {
            let ctx = TabletContext::with_infinite_region(i, Some(10));
            reg.load(ctx, true).unwrap();
        }

        let mut cached = reg.get(1).unwrap();
        let mut tablet = cached.latest().unwrap();
        // Prepare some data for two tablets of the same region. So we can test whether
        // we fetch the bytes from the latest one.
        for i in 1..21 {
            tablet.put_cf(CF_DEFAULT, b"zkey", b"val").unwrap();
            if i % 2 == 0 {
                tablet.flush_cf(CF_DEFAULT, true).unwrap();
            }
        }
        let old_pending_compaction_bytes = tablet
            .get_cf_pending_compaction_bytes(CF_DEFAULT)
            .unwrap()
            .unwrap();

        let ctx = TabletContext::with_infinite_region(1, Some(20));
        reg.load(ctx, true).unwrap();
        tablet = cached.latest().unwrap();

        for i in 1..11 {
            tablet.put_cf(CF_DEFAULT, b"zkey", b"val").unwrap();
            if i % 2 == 0 {
                tablet.flush_cf(CF_DEFAULT, true).unwrap();
            }
        }
        let new_pending_compaction_bytes = tablet
            .get_cf_pending_compaction_bytes(CF_DEFAULT)
            .unwrap()
            .unwrap();

        assert!(old_pending_compaction_bytes > new_pending_compaction_bytes);

        let engines_info = Arc::new(EnginesResourceInfo::new(&config, reg, None, 10));

        let mut cached_latest_tablets = HashMap::default();
        engines_info.update(Instant::now(), &mut cached_latest_tablets);

        // The memory allocation should be reserved
        assert!(cached_latest_tablets.capacity() >= 5);
        // The tablet cache should be cleared
        assert!(cached_latest_tablets.is_empty());

        // The latest_normalized_pending_bytes should be equal to the pending compaction
        // bytes of tablet_1_20
        assert_eq!(
            (new_pending_compaction_bytes * 100) as u32,
            engines_info.latest_normalized_pending_bytes()
        );
    }
}
