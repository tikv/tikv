// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

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
    cmp,
    collections::HashMap,
    convert::TryFrom,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::AtomicU64, mpsc, Arc, Mutex},
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use backup_stream::{
    config::BackupStreamConfigManager, metadata::store::PdStore, observer::BackupStreamObserver,
    BackupStreamResolver,
};
use causal_ts::CausalTsProviderImpl;
use cdc::CdcConfigManager;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{
    from_rocks_compression_type, RocksCompactedEvent, RocksEngine, RocksStatistics,
};
use engine_rocks_helper::sst_recovery::{RecoveryRunner, DEFAULT_CHECK_INTERVAL};
use engine_traits::{
    Engines, KvEngine, RaftEngine, SingletonFactory, TabletContext, TabletRegistry, CF_DEFAULT,
    CF_WRITE,
};
use file_system::{get_io_rate_limiter, BytesFetcher, MetricsManager as IoMetricsManager};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use health_controller::HealthController;
use hybrid_engine::HybridEngine;
use kvproto::{
    brpb::create_backup, cdcpb::create_change_data, deadlock::create_deadlock,
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
    kvrpcpb::ApiVersion, logbackuppb::create_log_backup, recoverdatapb::create_recover_data,
    resource_usage_agent::create_resource_metering_pub_sub,
};
use pd_client::{
    meta_storage::{Checked, Sourced},
    PdClient, RpcClient,
};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{
        config::SplitCheckConfigManager, BoxConsistencyCheckObserver, ConsistencyCheckMethod,
        CoprocessorHost, RawConsistencyCheckObserver, RegionInfoAccessor,
    },
    router::{CdcRaftRouter, ServerRaftStoreRouter},
    store::{
        config::RaftstoreConfigManager,
        fsm,
        fsm::store::{
            RaftBatchSystem, RaftRouter, StoreMeta, MULTI_FILES_SNAPSHOT_FEATURE, PENDING_MSG_CAP,
        },
        memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE,
        snapshot_backup::PrepareDiskSnapObserver,
        AutoSplitController, CheckLeaderRunner, LocalReader, SnapManager, SnapManagerBuilder,
        SplitCheckRunner, SplitConfigManager, StoreMetaDelegate,
    },
    RaftRouterCompactedEventSender,
};
use region_cache_memory_engine::RangeCacheMemoryEngine;
use resolved_ts::{LeadershipResolver, Task};
use resource_control::ResourceGroupManager;
use security::SecurityManager;
use service::{service_event::ServiceEvent, service_manager::GrpcServiceManager};
use snap_recovery::RecoveryService;
use tikv::{
    config::{
        ConfigController, DbConfigManger, DbType, LogConfigManager, MemoryConfigManager, TikvConfig,
    },
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{
        build_yatp_read_pool, ReadPool, ReadPoolConfigManager, UPDATE_EWMA_TIME_SLICE_INTERVAL,
    },
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        debug::{Debugger, DebuggerImpl},
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        tablet_snap::NoSnapshotCache,
        ttl::TtlChecker,
        KvEngineFactoryBuilder, Node, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, GRPC_THREAD_PREFIX,
        MEMORY_LIMIT_GAUGE,
    },
    storage::{
        self,
        config::EngineType,
        config_manager::StorageConfigManger,
        kv::LocalTablets,
        mvcc::MvccConsistencyCheckObserver,
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine, Storage,
    },
};
use tikv_alloc::{add_thread_memory_accessor, remove_thread_memory_accessor};
use tikv_util::{
    check_environment_variables,
    config::{ReadableSize, VersionTrack},
    memory::MemoryQuota,
    mpsc as TikvMpsc,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{disk, path_in_diff_mount_point, register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler, Worker},
    yatp_pool::CleanupMethod,
    Either,
};
use tokio::runtime::Builder;

use crate::{
    common::{
        ConfiguredRaftEngine, EngineMetricsManager, EnginesResourceInfo, KvEngineBuilder,
        TikvServerCore,
    },
    memory::*,
    setup::*,
    signal_handler,
    tikv_util::sys::thread::ThreadBuildWrapper,
};

#[inline]
fn run_impl<EK, CER, F>(
    config: TikvConfig,
    service_event_tx: TikvMpsc::Sender<ServiceEvent>,
    service_event_rx: TikvMpsc::Receiver<ServiceEvent>,
) where
    EK: KvEngine<CompactedEvent = RocksCompactedEvent, DiskEngine = RocksEngine> + KvEngineBuilder,
    CER: ConfiguredRaftEngine,
    F: KvFormat,
{
    let mut tikv = TikvServer::<EK, CER, F>::init(config, service_event_tx.clone());
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
    let (engines, engines_info) = tikv.init_raw_engines(listener);
    tikv.init_engines(engines.clone());
    let server_config = tikv.init_servers();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_cgroup_monitor();
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();
    tikv.core.init_quota_tuning_task(tikv.quota_limiter.clone());

    // Build a background worker for handling signals.
    {
        let engines = tikv.engines.take().unwrap().engines;
        let kv_statistics = tikv.kv_statistics.clone();
        let raft_statistics = tikv.raft_statistics.clone();
        std::thread::spawn(move || {
            signal_handler::wait_for_signal(
                Some(engines),
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
            if cfg!(feature = "memory-engine")
                && config.region_cache_memory_limit != ReadableSize(0)
            {
                run_impl::<HybridEngine<RocksEngine, RangeCacheMemoryEngine>, RocksEngine, API>(
                    config,
                    service_event_tx,
                    service_event_rx,
                )
            } else {
                run_impl::<RocksEngine, RocksEngine, API>(
                    config,
                    service_event_tx,
                    service_event_rx,
                )
            }
        } else {
            if cfg!(feature = "memory-engine")
                && config.region_cache_memory_limit != ReadableSize(0)
            {
                run_impl::<HybridEngine<RocksEngine, RangeCacheMemoryEngine>, RaftLogEngine, API>(
                    config,
                    service_event_tx,
                    service_event_rx,
                )
            } else {
                run_impl::<RocksEngine, RaftLogEngine, API>(
                    config,
                    service_event_tx,
                    service_event_rx,
                )
            }
        }
    })
}

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CGROUP_MONITOR_INTERVAL: Duration = Duration::from_secs(10);

/// A complete TiKV server.
struct TikvServer<EK, ER, F>
where
    EK: KvEngine,
    ER: RaftEngine,
    F: KvFormat,
{
    core: TikvServerCore,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<EK, ER>,
    system: Option<RaftBatchSystem<EK, ER>>,
    resolver: Option<resolve::PdStoreAddrResolver>,
    snap_mgr: Option<SnapManager>, // Will be filled in `init_servers`.
    engines: Option<TikvEngines<EK, ER>>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    servers: Option<Servers<EK, ER, F>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<EK>>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    check_leader_worker: Worker,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
    tablet_registry: Option<TabletRegistry<RocksEngine>>,
    br_snap_recovery_mode: bool, // use for br snapshot recovery
    resolved_ts_scheduler: Option<Scheduler<Task>>,
    grpc_service_mgr: GrpcServiceManager,
    snap_br_rejector: Option<Arc<PrepareDiskSnapObserver>>,
}

struct TikvEngines<EK: KvEngine, ER: RaftEngine> {
    engines: Engines<EK, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<EK, ServerRaftStoreRouter<EK, ER>>,
}

struct Servers<EK: KvEngine, ER: RaftEngine, F: KvFormat> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    node: Node<RpcClient, EK, ER>,
    importer: Arc<SstImporter<EK>>,
    cdc_scheduler: tikv_util::worker::Scheduler<cdc::Task>,
    cdc_memory_quota: Arc<MemoryQuota>,
    rsmeter_pubsub_service: resource_metering::PubSubService,
    backup_stream_scheduler: Option<tikv_util::worker::Scheduler<backup_stream::Task>>,
    debugger: DebuggerImpl<ER, RaftKv<EK, ServerRaftStoreRouter<EK, ER>>, LockManager, F>,
}

type LocalServer<EK, ER> = Server<resolve::PdStoreAddrResolver, LocalRaftKv<EK, ER>>;
type LocalRaftKv<EK, ER> = RaftKv<EK, ServerRaftStoreRouter<EK, ER>>;

impl<EK, ER, F> TikvServer<EK, ER, F>
where
    EK: KvEngine<DiskEngine = RocksEngine>,
    ER: RaftEngine,
    F: KvFormat,
{
    fn init(mut config: TikvConfig, tx: TikvMpsc::Sender<ServiceEvent>) -> TikvServer<EK, ER, F> {
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
        // check if TiKV need to run in snapshot recovery mode
        let is_recovering_marked = match pd_client.is_recovering_marked() {
            Err(e) => {
                warn!(
                    "failed to get recovery mode from PD";
                    "error" => ?e,
                );
                false
            }
            Ok(marked) => marked,
        };

        if is_recovering_marked {
            // Run a TiKV server in recovery modeß
            info!("TiKV running in Snapshot Recovery Mode");
            snap_recovery::init_cluster::enter_snap_recovery_mode(&mut config);
            // connect_to_pd_cluster retreived the cluster id from pd
            let cluster_id = config.server.cluster_id;
            snap_recovery::init_cluster::start_recovery(
                config.clone(),
                cluster_id,
                pd_client.clone(),
            );
        }

        // Initialize and check config
        let cfg_controller = TikvServerCore::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        let resource_manager = if config.resource_control.enabled {
            let mgr = Arc::new(ResourceGroupManager::default());
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

        // Initialize raftstore channels.
        let (router, system) = fsm::create_raft_batch_system(&config.raft_store, &resource_manager);

        let mut coprocessor_host = Some(CoprocessorHost::new(
            router.clone(),
            config.coprocessor.clone(),
        ));

        // Region stats manager collects region heartbeat for use by in-memory engine.
        let region_stats_manager_enabled_cb: Arc<dyn Fn() -> bool + Send + Sync> =
            if cfg!(feature = "memory-engine") {
                let cfg_controller_clone = cfg_controller.clone();
                Arc::new(move || {
                    cfg_controller_clone.get_current().region_cache_memory_limit != ReadableSize(0)
                })
            } else {
                Arc::new(|| false)
            };

        let region_info_accessor = RegionInfoAccessor::new(
            coprocessor_host.as_mut().unwrap(),
            region_stats_manager_enabled_cb,
        );

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

        // Run check leader in a dedicate thread, because it is time sensitive
        // and crucial to TiCDC replication lag.
        let check_leader_worker = WorkerBuilder::new("check-leader").thread_count(1).create();

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
            router,
            system: Some(system),
            resolver: None,
            snap_mgr: None,
            engines: None,
            kv_statistics: None,
            raft_statistics: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            concurrency_manager,
            env,
            check_leader_worker,
            sst_worker: None,
            quota_limiter,
            resource_manager,
            causal_ts_provider,
            tablet_registry: None,
            br_snap_recovery_mode: is_recovering_marked,
            resolved_ts_scheduler: None,
            grpc_service_mgr: GrpcServiceManager::new(tx),
            snap_br_rejector: None,
        }
    }

    fn init_engines(&mut self, engines: Engines<EK, ER>) {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
        let engine = RaftKv::new(
            ServerRaftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(
                    engines.kv.clone(),
                    StoreMetaDelegate::new(store_meta.clone(), engines.kv.clone()),
                    self.router.clone(),
                ),
            ),
            engines.kv.clone(),
            self.region_info_accessor.region_leaders(),
        );

        self.engines = Some(TikvEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(&mut self) -> GcWorker<RaftKv<EK, ServerRaftStoreRouter<EK, ER>>> {
        let engines = self.engines.as_ref().unwrap();
        let gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.core.flow_info_sender.take().unwrap(),
            self.core.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
            Arc::new(self.region_info_accessor.clone()),
        );

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let flow_controller = Arc::new(FlowController::Singleton(EngineFlowController::new(
            &self.core.config.storage.flow_control,
            self.engines.as_ref().unwrap().engine.kv_engine().unwrap(),
            self.core.flow_info_receiver.take().unwrap(),
        )));
        let mut gc_worker = self.init_gc_worker();
        let mut ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
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

        // Create cdc.
        let mut cdc_worker = Box::new(LazyWorker::new("cdc"));
        let cdc_scheduler = cdc_worker.scheduler();
        let txn_extra_scheduler = cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone());

        self.engines
            .as_mut()
            .unwrap()
            .engine
            .set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        let lock_mgr = LockManager::new(&self.core.config.pessimistic_txn);
        cfg_controller.register(
            tikv::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

        if let Some(sst_worker) = &mut self.sst_worker {
            let sst_runner = RecoveryRunner::new(
                engines.engines.kv.get_disk_engine().clone(),
                engines.store_meta.clone(),
                self.core
                    .config
                    .storage
                    .background_error_recovery_window
                    .into(),
                DEFAULT_CHECK_INTERVAL,
            );
            sst_worker.start_with_timer(sst_runner);
        }

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
                .enable_time()
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
            .join(Path::new("snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let bps = i64::try_from(self.core.config.server.snap_io_max_bytes_per_sec.0)
            .unwrap_or_else(|_| fatal!("snap_io_max_bytes_per_sec > i64::max_value"));

        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(bps)
            .max_total_size(self.core.config.server.snap_max_total_size.0)
            .encryption_key_manager(self.core.encryption_key_manager.clone())
            .max_per_file_size(self.core.config.raft_store.max_snapshot_file_raw_size.0)
            .enable_multi_snapshot_files(
                self.pd_client
                    .feature_gate()
                    .can_enable(MULTI_FILES_SNAPSHOT_FEATURE),
            )
            .enable_receive_tablet_snapshot(
                self.core.config.raft_store.enable_v2_compatible_learner,
            )
            .build(snap_path);

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

        // Register cdc.
        let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone());
        cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // Register cdc config manager.
        cfg_controller.register(
            tikv::config::Module::Cdc,
            Box::new(CdcConfigManager(cdc_worker.scheduler())),
        );

        // Create resolved ts worker
        let rts_worker = if self.core.config.resolved_ts.enable {
            let worker = Box::new(LazyWorker::new("resolved-ts"));
            // Register the resolved ts observer
            let resolved_ts_ob = resolved_ts::Observer::new(worker.scheduler());
            resolved_ts_ob.register_to(self.coprocessor_host.as_mut().unwrap());
            // Register config manager for resolved ts worker
            cfg_controller.register(
                tikv::config::Module::ResolvedTs,
                Box::new(resolved_ts::ResolvedTsConfigManager::new(
                    worker.scheduler(),
                )),
            );
            Some(worker)
        } else {
            None
        };

        let check_leader_runner = CheckLeaderRunner::new(
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let check_leader_scheduler = self
            .check_leader_worker
            .start("check-leader", check_leader_runner);

        let server_config = Arc::new(VersionTrack::new(self.core.config.server.clone()));

        self.core.config.raft_store.optimize_for(false);
        self.core
            .config
            .raft_store
            .validate(
                self.core.config.coprocessor.region_split_size(),
                self.core.config.coprocessor.enable_region_bucket(),
                self.core.config.coprocessor.region_bucket_size,
                false,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.core.config.raft_store.clone()));
        let health_controller = HealthController::new();
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store.clone(),
            self.core.config.storage.api_version(),
            self.pd_client.clone(),
            state,
            self.core.background_worker.clone(),
            health_controller.clone(),
            None,
        );
        node.try_bootstrap_store(engines.engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        self.snap_mgr = Some(snap_mgr.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage.clone(),
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                resource_tag_factory,
                self.quota_limiter.clone(),
                self.resource_manager.clone(),
            ),
            coprocessor_v2::Endpoint::new(&self.core.config.coprocessor_v2),
            self.resolver.clone().unwrap(),
            Either::Left(snap_mgr.clone()),
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
            )),
        );

        let rejector = Arc::new(PrepareDiskSnapObserver::default());
        rejector.register_to(self.coprocessor_host.as_mut().unwrap());
        self.snap_br_rejector = Some(rejector);

        // Start backup stream
        let backup_stream_scheduler = if self.core.config.log_backup.enable {
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

            let region_read_progress = engines
                .store_meta
                .lock()
                .unwrap()
                .region_read_progress
                .clone();
            let leadership_resolver = LeadershipResolver::new(
                node.id(),
                self.pd_client.clone(),
                self.env.clone(),
                self.security_mgr.clone(),
                region_read_progress,
                Duration::from_secs(60),
            );

            let backup_stream_endpoint = backup_stream::Endpoint::new(
                node.id(),
                PdStore::new(Checked::new(Sourced::new(
                    Arc::clone(&self.pd_client),
                    pd_client::meta_storage::Source::LogBackup,
                ))),
                self.core.config.log_backup.clone(),
                backup_stream_scheduler.clone(),
                backup_stream_ob,
                self.region_info_accessor.clone(),
                CdcRaftRouter(self.router.clone()),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
                BackupStreamResolver::V1(leadership_resolver),
            );
            backup_stream_worker.start(backup_stream_endpoint);
            self.core.to_stop.push(backup_stream_worker);
            Some(backup_stream_scheduler)
        } else {
            None
        };

        let import_path = self.core.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.core.config.import,
            import_path,
            self.core.encryption_key_manager.clone(),
            self.core.config.storage.api_version(),
            false,
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

        let split_check_runner = SplitCheckRunner::new(
            engines.engines.kv.clone(),
            self.router.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let split_check_scheduler = self
            .core
            .background_worker
            .start("split-check", split_check_runner);
        cfg_controller.register(
            tikv::config::Module::Coprocessor,
            Box::new(SplitCheckConfigManager(split_check_scheduler.clone())),
        );

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

        node.start(
            engines.engines.clone(),
            server.transport(),
            snap_mgr,
            pd_worker,
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
            importer.clone(),
            split_check_scheduler,
            auto_split_controller,
            self.concurrency_manager.clone(),
            collector_reg_handle,
            self.causal_ts_provider.clone(),
            self.grpc_service_mgr.clone(),
            safe_point.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        // Start auto gc. Must after `Node::start` because `node_id` is initialized
        // there.
        assert!(node.id() > 0); // Node id should never be 0.
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone(),
            node.id(),
        );
        gc_worker
            .start(node.id())
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config, safe_point) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        initial_metric(&self.core.config.metric);
        if self.core.config.storage.enable_ttl {
            ttl_checker.start_with_timer(TtlChecker::new(
                self.engines.as_ref().unwrap().engine.kv_engine().unwrap(),
                self.region_info_accessor.clone(),
                self.core.config.storage.ttl_check_poll_interval.into(),
            ));
            self.core.to_stop.push(ttl_checker);
        }

        // Start CDC.
        let cdc_memory_quota = Arc::new(MemoryQuota::new(
            self.core.config.cdc.sink_memory_quota.0 as _,
        ));
        let cdc_endpoint = cdc::Endpoint::new(
            self.core.config.server.cluster_id,
            &self.core.config.cdc,
            self.core.config.storage.engine == EngineType::RaftKv2,
            self.core.config.storage.api_version(),
            self.pd_client.clone(),
            cdc_scheduler.clone(),
            CdcRaftRouter(self.router.clone()),
            LocalTablets::Singleton(self.engines.as_ref().unwrap().engines.kv.clone()),
            cdc_ob,
            engines.store_meta.clone(),
            self.concurrency_manager.clone(),
            server.env(),
            self.security_mgr.clone(),
            cdc_memory_quota.clone(),
            self.causal_ts_provider.clone(),
        );
        cdc_worker.start_with_timer(cdc_endpoint);
        self.core.to_stop.push(cdc_worker);

        // Start resolved ts
        if let Some(mut rts_worker) = rts_worker {
            let rts_endpoint = resolved_ts::Endpoint::new(
                &self.core.config.resolved_ts,
                rts_worker.scheduler(),
                CdcRaftRouter(self.router.clone()),
                engines.store_meta.clone(),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
                server.env(),
                self.security_mgr.clone(),
            );
            self.resolved_ts_scheduler = Some(rts_worker.scheduler());
            rts_worker.start_with_timer(rts_endpoint);
            self.core.to_stop.push(rts_worker);
        }

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager::new(
                node.refresh_config_scheduler(),
                raft_store,
            )),
        );

        // Create Debugger.
        let mut debugger = DebuggerImpl::new(
            Engines::new(
                engines.engines.kv.get_disk_engine().clone(),
                engines.engines.raft.clone(),
            ),
            self.cfg_controller.as_ref().unwrap().clone(),
            Some(storage),
        );
        debugger.set_kv_statistics(self.kv_statistics.clone());
        debugger.set_raft_statistics(self.raft_statistics.clone());

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
            cdc_scheduler,
            cdc_memory_quota,
            rsmeter_pubsub_service,
            backup_stream_scheduler,
            debugger,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Import SST service.
        let import_service = ImportSstService::new(
            self.core.config.import.clone(),
            self.core.config.raft_store.raft_entry_max_size,
            engines.engine.clone(),
            LocalTablets::Singleton(engines.engines.kv.clone()),
            servers.importer.clone(),
            None,
            self.resource_manager.clone(),
            Arc::new(self.region_info_accessor.clone()),
        );
        let import_cfg_mgr = import_service.get_config_manager();

        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        self.cfg_controller
            .as_mut()
            .unwrap()
            .register(tikv::config::Module::Import, Box::new(import_cfg_mgr));

        // Debug service.
        let resolved_ts_scheduler = Arc::new(self.resolved_ts_scheduler.clone());
        let debug_service = DebugService::new(
            servers.debugger.clone(),
            servers.server.get_debug_thread_pool().clone(),
            engines.engine.raft_extension(),
            self.engines.as_ref().unwrap().store_meta.clone(),
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
        info!("start register debug service");
        if servers
            .server
            .register_service(create_debug(debug_service))
            .is_some()
        {
            fatal!("failed to register debug service");
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
                servers.node.id(),
                self.pd_client.clone(),
                self.resolver.clone().unwrap(),
                self.security_mgr.clone(),
                &self.core.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        // Backup service.
        let mut backup_worker = Box::new(self.core.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_endpoint = backup::Endpoint::new(
            servers.node.id(),
            engines.engine.clone(),
            self.region_info_accessor.clone(),
            LocalTablets::Singleton(engines.engines.kv.clone()),
            self.core.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.core.config.storage.api_version(),
            self.causal_ts_provider.clone(),
            self.resource_manager.clone(),
        );
        let env = backup::disk_snap::Env::new(
            Arc::new(Mutex::new(self.router.clone())),
            self.snap_br_rejector.take().unwrap(),
            Some(backup_endpoint.io_pool_handle().clone()),
        );
        let backup_service = backup::Service::new(backup_scheduler, env);
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start(backup_endpoint);

        let cdc_service = cdc::Service::new(
            servers.cdc_scheduler.clone(),
            servers.cdc_memory_quota.clone(),
        );
        if servers
            .server
            .register_service(create_change_data(cdc_service))
            .is_some()
        {
            fatal!("failed to register cdc service");
        }
        if servers
            .server
            .register_service(create_resource_metering_pub_sub(
                servers.rsmeter_pubsub_service.clone(),
            ))
            .is_some()
        {
            warn!("failed to register resource metering pubsub service");
        }

        if let Some(sched) = servers.backup_stream_scheduler.take() {
            let pitr_service = backup_stream::Service::new(sched);
            if servers
                .server
                .register_service(create_log_backup(pitr_service))
                .is_some()
            {
                fatal!("failed to register log backup service");
            }
        }

        // the present tikv in recovery mode, start recovery service
        if self.br_snap_recovery_mode {
            let recovery_service =
                RecoveryService::new(engines.engines.clone(), self.router.clone());

            if servers
                .server
                .register_service(create_recover_data(recovery_service))
                .is_some()
            {
                fatal!("failed to register recovery service");
            }
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
            self.core.config.rocksdb.titan.enabled.map_or(false, |v| v),
            self.engines.as_ref().unwrap().engines.raft.clone(),
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

    fn init_storage_stats_task(&self, engines: Engines<EK, ER>) {
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
        let raft_path = engines.raft.get_engine_path().to_string();
        let separated_raft_mount_path =
            path_in_diff_mount_point(raft_path.as_str(), engines.kv.path());
        let raft_almost_full_threshold = reserve_raft_space;
        let raft_already_full_threshold = reserve_raft_space / 2;

        let almost_full_threshold = reserve_space;
        let already_full_threshold = reserve_space / 2;
        fn calculate_disk_usage(a: disk::DiskUsage, b: disk::DiskUsage) -> disk::DiskUsage {
            match (a, b) {
                (disk::DiskUsage::AlreadyFull, _) => disk::DiskUsage::AlreadyFull,
                (_, disk::DiskUsage::AlreadyFull) => disk::DiskUsage::AlreadyFull,
                (disk::DiskUsage::AlmostFull, _) => disk::DiskUsage::AlmostFull,
                (_, disk::DiskUsage::AlmostFull) => disk::DiskUsage::AlmostFull,
                (disk::DiskUsage::Normal, disk::DiskUsage::Normal) => disk::DiskUsage::Normal,
            }
        }
        self.core.background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let disk_stats = match fs2::statvfs(&store_path) {
                    Err(e) => {
                        error!(
                            "get disk stat for kv store failed";
                            "kv_path" => store_path.to_str(),
                            "err" => ?e
                        );
                        return;
                    }
                    Ok(stats) => stats,
                };
                let disk_cap = disk_stats.total_space();
                let snap_size = snap_mgr.get_total_snap_size().unwrap();

                let kv_size = engines
                    .kv
                    .get_engine_used_size()
                    .expect("get kv engine size");

                let raft_size = engines
                    .raft
                    .get_engine_size()
                    .expect("get raft engine size");

                let mut raft_disk_status = disk::DiskUsage::Normal;
                if separated_raft_mount_path && reserve_raft_space != 0 {
                    let raft_disk_stats = match fs2::statvfs(&raft_path) {
                        Err(e) => {
                            error!(
                                "get disk stat for raft engine failed";
                                "raft_engine_path" => raft_path.clone(),
                                "err" => ?e
                            );
                            return;
                        }
                        Ok(stats) => stats,
                    };
                    let raft_disk_cap = raft_disk_stats.total_space();
                    let mut raft_disk_available =
                        raft_disk_cap.checked_sub(raft_size).unwrap_or_default();
                    raft_disk_available = cmp::min(raft_disk_available, raft_disk_stats.available_space());
                    raft_disk_status = if raft_disk_available <= raft_already_full_threshold
                    {
                        disk::DiskUsage::AlreadyFull
                    } else if raft_disk_available <= raft_almost_full_threshold
                    {
                        disk::DiskUsage::AlmostFull
                    } else {
                        disk::DiskUsage::Normal
                    };
                }
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
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };

                let mut available = capacity.checked_sub(used_size).unwrap_or_default();
                available = cmp::min(available, disk_stats.available_space());

                let prev_disk_status = disk::get_disk_status(0); //0 no need care about failpoint.
                let cur_kv_disk_status = if available <= already_full_threshold {
                    disk::DiskUsage::AlreadyFull
                } else if available <= almost_full_threshold {
                    disk::DiskUsage::AlmostFull
                } else {
                    disk::DiskUsage::Normal
                };
                let cur_disk_status = calculate_disk_usage(raft_disk_status, cur_kv_disk_status);
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
            .start(server_config, self.security_mgr.clone(), NoSnapshotCache)
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.core.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.core.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
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

    fn prepare_stop(&self) {
        if let Some(engines) = self.engines.as_ref() {
            // Disable manul compaction jobs before shutting down the engines. And it
            // will stop the compaction thread in advance, so it won't block the
            // cleanup thread when exiting.
            let _ = engines.engines.kv.disable_manual_compaction();
        }
    }

    fn stop(self) {
        tikv_util::thread_group::mark_shutdown();
        self.prepare_stop();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.region_info_accessor.stop();

        servers.lock_mgr.stop();

        if let Some(sst_worker) = self.sst_worker {
            sst_worker.stop_worker();
        }

        self.core.to_stop.into_iter().for_each(|s| s.stop());
    }

    fn pause(&mut self) {
        let server = self.servers.as_mut().unwrap();
        if let Err(e) = server.server.pause() {
            warn!(
                "failed to pause the server";
                "err" => ?e
            );
        }
    }

    fn resume(&mut self) {
        let server = self.servers.as_mut().unwrap();
        if let Err(e) = server.server.resume() {
            warn!(
                "failed to resume the server";
                "err" => ?e
            );
        }
    }
}

impl<EK, CER, F> TikvServer<EK, CER, F>
where
    EK: KvEngine<DiskEngine = RocksEngine, CompactedEvent = RocksCompactedEvent> + KvEngineBuilder,
    CER: ConfiguredRaftEngine,
    F: KvFormat,
{
    fn init_raw_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
    ) -> (Engines<EK, CER>, Arc<EnginesResourceInfo>) {
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
        .compaction_event_sender(Arc::new(RaftRouterCompactedEventSender {
            router: Mutex::new(self.router.clone()),
        }))
        .region_info_accessor(self.region_info_accessor.clone())
        .sst_recovery_sender(self.init_sst_recovery_sender())
        .flow_listener(flow_listener);
        let factory = Box::new(builder.build());
        let disk_engine = factory
            .create_shared_db(&self.core.store_path)
            .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));
        let kv_engine: EK =
            KvEngineBuilder::build(disk_engine.clone(), Some(self.pd_client.clone()));
        self.kv_statistics = Some(factory.rocks_statistics());
        let engines = Engines::new(kv_engine, raft_engine);

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DbConfigManger::new(
                cfg_controller.get_current().rocksdb,
                disk_engine.clone(),
                DbType::Kv,
            )),
        );
        let reg = TabletRegistry::new(
            Box::new(SingletonFactory::new(disk_engine)),
            &self.core.store_path,
        )
        .unwrap();
        // It always use the singleton kv_engine, use arbitrary id and suffix.
        let ctx = TabletContext::with_infinite_region(0, Some(0));
        reg.load(ctx, false).unwrap();
        self.tablet_registry = Some(reg.clone());
        engines.raft.register_config(cfg_controller);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            &self.core.config,
            reg,
            engines.raft.as_rocks_engine().cloned(),
            180, // max_samples_to_preserve
        ));

        (engines, engines_info)
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
        config.validate().unwrap();
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
