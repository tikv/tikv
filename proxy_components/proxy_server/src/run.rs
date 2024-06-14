// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
// Some clone is indeed needless. However, we ignore here for convenience.
#![allow(clippy::redundant_clone)]
use std::{
    cmp,
    convert::TryFrom,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use concurrency_manager::ConcurrencyManager;

use health_controller::HealthController;
use encryption_export::data_key_manager_from_config;
use engine_rocks::{from_rocks_compression_type, RocksEngine, RocksStatistics};
use engine_rocks_helper::sst_recovery::{RecoveryRunner, DEFAULT_CHECK_INTERVAL};
use engine_store_ffi::{
    self,
    core::DebugStruct,
    ffi::{
        interfaces_ffi::{
            EngineStoreServerHelper, EngineStoreServerStatus, RaftProxyStatus,
            RaftStoreProxyFFIHelper,
        },
        read_index_helper::ReadIndexClient,
        RaftStoreProxy, RaftStoreProxyFFI,
    },
    TiFlashEngine,
};
use engine_tiflash::PSLogEngine;
use engine_traits::{
    Engines, KvEngine, MiscExt, RaftEngine, SingletonFactory, TabletContext, TabletRegistry,
    CF_DEFAULT, CF_WRITE,
};

use region_cache_memory_engine::{
    config::RangeCacheConfigManager, RangeCacheEngineContext, RangeCacheMemoryEngine,
    RangeCacheMemoryEngineStatistics,
};
use error_code::ErrorCodeExt;
use file_system::{get_io_rate_limiter, BytesFetcher, MetricsManager as IOMetricsManager};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use grpcio_health::HealthService;
use kvproto::{
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{config::SplitCheckConfigManager, CoprocessorHost, RegionInfoAccessor},
    router::ServerRaftStoreRouter,
    store::{
        config::RaftstoreConfigManager,
        fsm,
        fsm::store::{
            RaftBatchSystem, RaftRouter, StoreMeta, MULTI_FILES_SNAPSHOT_FEATURE, PENDING_MSG_CAP,
        },
        memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE,
        AutoSplitController, CheckLeaderRunner, LocalReader, SnapManager, SnapManagerBuilder,
        SplitCheckRunner, SplitConfigManager, StoreMetaDelegate,
    },
};
use resource_control::{
    ResourceGroupManager, ResourceManagerService, MIN_PRIORITY_UPDATE_INTERVAL,
};
use security::SecurityManager;
use server::{
    common::{check_system_config, EngineMetricsManager, TikvServerCore},
    memory::*,
};
use service::{service_event::ServiceEvent, service_manager::GrpcServiceManager};
use tikv::{
    config::{ConfigController, DbConfigManger, DbType, TikvConfig},
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{build_yatp_read_pool, ReadPool, ReadPoolConfigManager},
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        debug::{Debugger, DebuggerImpl},
        gc_worker::GcWorker,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        tablet_snap::NoSnapshotCache,
        ttl::TtlChecker,
        KvEngineFactoryBuilder, MultiRaftServer, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, GRPC_THREAD_PREFIX,
    },
    storage::{
        self,
        config_manager::StorageConfigManger,
        kv::LocalTablets,
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine, Storage,
    },
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, ReadableDuration, VersionTrack},
    error,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{disk, register_memory_usage_high_water, thread::ThreadBuildWrapper, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler},
    yatp_pool::CleanupMethod,
    Either,
};
use tokio::runtime::Builder;

use crate::{
    common_override::*, config::ProxyConfig, engine::ProxyRocksEngine, fatal,
    hacked_lock_mgr::HackedLockManager as LockManager, setup::*, status_server::StatusServer,
    util::ffi_server_info,
};

#[inline]
pub fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    let (service_event_tx, service_event_rx) = tikv_util::mpsc::unbounded(); // pipe for controling service
    let engine_store_server_helper_ptr = engine_store_server_helper as *const _ as isize;
    let mut tikv = TiKvServer::<CER, F>::init(
        config,
        proxy_config,
        engine_store_server_helper_ptr,
        service_event_tx.clone(),
    );

    // Must be called after `TiKvServer::init`.
    let memory_limit = tikv.core.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.core.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.core.check_conflict_addr();
    tikv.core.init_fs();
    tikv.core.init_yatp();
    tikv.core.init_encryption();

    // Use modified `proxy_config` here.
    let proxy_config_str = serde_json::to_string(&tikv.proxy_config).unwrap_or_default();
    let mut proxy = RaftStoreProxy::new(
        AtomicU8::new(RaftProxyStatus::Idle as u8),
        tikv.core.encryption_key_manager.clone(),
        Some(Box::new(ReadIndexClient::new(
            tikv.router.clone(),
            SysQuota::cpu_cores_quota() as usize * 2,
        ))),
        None,
        Some(tikv.pd_client.clone()),
        proxy_config_str,
    );
    info!("start probing cluster's raftstore version");
    // We wait for a maximum of 10 seconds for every store.
    proxy.refresh_cluster_raftstore_version(10 * 1000);
    info!(
        "cluster's raftstore version is {:?}",
        proxy.cluster_raftstore_version()
    );

    let proxy_ref = &proxy;
    let proxy_helper = {
        let mut proxy_helper = RaftStoreProxyFFIHelper::new(proxy_ref.into());
        proxy_helper.fn_server_info = Some(ffi_server_info);
        proxy_helper
    };

    info!("set raft-store proxy helper");

    engine_store_server_helper.handle_set_proxy(&proxy_helper);

    info!("wait for engine-store server to start");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        == EngineStoreServerStatus::Idle
    {
        thread::sleep(Duration::from_millis(200));
    }

    if engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Running
    {
        info!("engine-store server is not running, make proxy exit");
        return;
    }

    info!("engine-store server is started");

    let fetcher = tikv.core.init_io_utility();
    let listener = tikv.core.init_flow_receiver();
    let engine_store_server_helper_ptr = engine_store_server_helper as *const _ as isize;
    // Will call TiFlashEngine::init
    let (engines, engines_info) =
        tikv.init_tiflash_engines(listener, engine_store_server_helper_ptr);
    tikv.init_engines(engines.clone());
    {
        if engines.kv.element_engine.is_none() {
            error!("TiFlashEngine has empty ElementaryEngine");
        }
        proxy.set_kv_engine(
            engine_store_ffi::ffi::RaftStoreProxyEngine::from_tiflash_engine(engines.kv.clone()),
        );
    }
    let server_config = tikv.init_servers();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();

    proxy.set_status(RaftProxyStatus::Running);

    let jh = std::thread::spawn(move || {
        loop {
            if let Ok(service_event) = service_event_rx.recv() {
                match service_event {
                    ServiceEvent::PauseGrpc => {
                        debug!("don't support PauseGrpc")
                    }
                    ServiceEvent::ResumeGrpc => {
                        debug!("don't support ResumeGrpc")
                    }
                    ServiceEvent::Exit => {
                        break;
                    }
                }
            }
        }
    });

    {
        debug_assert!(
            engine_store_server_helper.handle_get_engine_store_server_status()
                == EngineStoreServerStatus::Running
        );
        let _ = tikv.engines.take().unwrap().engines;
        loop {
            if engine_store_server_helper.handle_get_engine_store_server_status()
                != EngineStoreServerStatus::Running
            {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    info!(
        "found engine-store server status is {:?}, start to stop all services",
        engine_store_server_helper.handle_get_engine_store_server_status()
    );

    if let Err(e) = service_event_tx.send(ServiceEvent::Exit) {
        warn!("failed to notify grpc server exit, {:?}", e);
    }
    jh.join().unwrap();

    tikv.stop();

    proxy.set_status(RaftProxyStatus::Stopped);

    info!("all services in raft-store proxy are stopped");

    info!("wait for engine-store server to stop");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Terminated
    {
        thread::sleep(Duration::from_millis(200));
    }
    info!("engine-store server is stopped");
}

#[inline]
fn run_impl_only_for_decryption<CER: ConfiguredRaftEngine, F: KvFormat>(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    let encryption_key_manager =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .map_err(|e| {
                panic!(
                    "Encryption failed to initialize: {}. code: {}",
                    e,
                    e.error_code()
                )
            })
            .unwrap()
            .map(Arc::new);

    let proxy_config_str = serde_json::to_string(&proxy_config).unwrap_or_default();
    let mut proxy = RaftStoreProxy::new(
        AtomicU8::new(RaftProxyStatus::Idle as u8),
        encryption_key_manager.clone(),
        Option::None,
        None,
        None,
        proxy_config_str,
    );

    let proxy_ref = &proxy;
    let proxy_helper = {
        let mut proxy_helper = RaftStoreProxyFFIHelper::new(proxy_ref.into());
        proxy_helper.fn_server_info = Some(ffi_server_info);
        proxy_helper
    };

    info!("set raft-store proxy helper");

    engine_store_server_helper.handle_set_proxy(&proxy_helper);

    info!("wait for engine-store server to start");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        == EngineStoreServerStatus::Idle
    {
        thread::sleep(Duration::from_millis(200));
    }

    if engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Running
    {
        info!("engine-store server is not running, make proxy exit");
        return;
    }

    info!("engine-store server is started");

    proxy.set_status(RaftProxyStatus::Running);

    {
        debug_assert!(
            engine_store_server_helper.handle_get_engine_store_server_status()
                == EngineStoreServerStatus::Running
        );
        loop {
            if engine_store_server_helper.handle_get_engine_store_server_status()
                != EngineStoreServerStatus::Running
            {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    info!(
        "found engine-store server status is {:?}, start to stop all services",
        engine_store_server_helper.handle_get_engine_store_server_status()
    );

    proxy.set_status(RaftProxyStatus::Stopped);

    info!("all services in raft-store proxy are stopped");

    info!("wait for engine-store server to stop");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Terminated
    {
        thread::sleep(Duration::from_millis(200));
    }
    info!("engine-store server is stopped");
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub unsafe fn run_tikv_proxy(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    crate::log_proxy_info();

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            tikv_util::info!("bootstrap with tikv-rocks-engine");
            run_impl::<engine_rocks::RocksEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        } else {
            if proxy_config.engine_store.enable_unips {
                tikv_util::info!("bootstrap with pagestorage");
                run_impl::<PSLogEngine, API>(config, proxy_config, engine_store_server_helper)
            } else {
                tikv_util::info!("bootstrap with tikv-raft-engine");
                run_impl::<RaftLogEngine, API>(config, proxy_config, engine_store_server_helper)
            }
        }
    })
}

/// Run a TiKV server only for decryption. Returns when the server is shutdown
/// by the user, in which case the server will be properly stopped.
pub unsafe fn run_tikv_only_decryption(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    crate::log_proxy_info();

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl_only_for_decryption::<RocksEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        } else {
            run_impl_only_for_decryption::<RaftLogEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        }
    })
}

impl<CER: ConfiguredRaftEngine, F: KvFormat> TiKvServer<CER, F> {
    fn init_tiflash_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
        engine_store_server_helper: isize,
    ) -> (Engines<TiFlashEngine, CER>, Arc<EnginesResourceInfo>) {
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
        let (mut raft_engine, raft_statistics) = CER::build(
            &self.core.config,
            &env,
            &self.core.encryption_key_manager,
            &block_cache,
        );
        self.raft_statistics = raft_statistics;

        match raft_engine.as_ps_engine() {
            None => {}
            Some(ps_engine) => {
                ps_engine.init(engine_store_server_helper);
            }
        }

        // Create kv engine.
        let builder = KvEngineFactoryBuilder::new(env, &self.core.config, block_cache, self.core.encryption_key_manager.clone())
            // TODO(tiflash) check if we need a old version of RocksEngine, or if we need to upgrade
            // .compaction_filter_router(self.router.clone())
            .region_info_accessor(self.region_info_accessor.clone())
            .sst_recovery_sender(self.init_sst_recovery_sender())
            .flow_listener(flow_listener);
        let factory = Box::new(builder.build());
        let kv_engine = factory
            .create_shared_db(&self.core.store_path)
            .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let range_cache_engine_config = Arc::new(VersionTrack::new(
            self.core.config.range_cache_engine.clone(),
        ));
        let range_cache_engine_context =
            RangeCacheEngineContext::new(range_cache_engine_config.clone());
        let range_cache_engine_statistics = range_cache_engine_context.statistics();
        self.kv_statistics = Some(factory.rocks_statistics());
        self.range_cache_engine_statistics = Some(range_cache_engine_statistics);

        let helper =
            engine_store_ffi::ffi::gen_engine_store_server_helper(engine_store_server_helper);
        let engine_store_hub = Arc::new(engine_store_ffi::engine::TiFlashEngineStoreHub {
            engine_store_server_helper: helper,
            store_id: std::cell::RefCell::new(0),
        });
        // engine_tiflash::MixedModeEngine has engine_rocks::RocksEngine inside
        let mut kv_engine = TiFlashEngine::from_rocks(kv_engine);
        let proxy_config_set = Arc::new(engine_tiflash::ProxyEngineConfigSet {
            engine_store: self.proxy_config.engine_store.clone(),
        });
        kv_engine.init(
            engine_store_server_helper,
            self.proxy_config.raft_store.snap_handle_pool_size,
            Some(engine_store_hub),
            Some(proxy_config_set),
        );

        let engines = Engines::new(kv_engine.clone(), raft_engine);

        let proxy_rocks_engine = ProxyRocksEngine::new(kv_engine.clone());
        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DbConfigManger::new(
                cfg_controller.get_current().rocksdb,
                proxy_rocks_engine,
                DbType::Kv,
            )),
        );

        let reg = TabletRegistry::new(
            Box::new(SingletonFactory::new(kv_engine.rocks.clone())),
            &self.core.store_path,
        )
        .unwrap();
        // It always use the singleton kv_engine, use arbitrary id and suffix.
        let ctx = TabletContext::with_infinite_region(0, Some(0));
        reg.load(ctx, false).unwrap();
        self.tablet_registry = Some(reg.clone());
        engines.raft.register_config(cfg_controller);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            &engines, 180, // max_samples_to_preserve
        ));

        (engines, engines_info)
    }
}

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);

/// A complete TiKV server.
struct TiKvServer<ER: RaftEngine, F: KvFormat> {
    proxy_config: ProxyConfig,
    engine_store_server_helper_ptr: isize,
    core: TikvServerCore,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<TiFlashEngine, ER>,
    system: Option<RaftBatchSystem<TiFlashEngine, ER>>,
    resolver: Option<resolve::PdStoreAddrResolver>,
    snap_mgr: Option<SnapManager>, // Will be filled in `init_servers`.
    engines: Option<TiKvEngines<TiFlashEngine, ER>>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    range_cache_engine_statistics: Option<Arc<RangeCacheMemoryEngineStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    servers: Option<Servers<TiFlashEngine, ER, F>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<TiFlashEngine>>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    tablet_registry: Option<TabletRegistry<RocksEngine>>,
    grpc_service_mgr: GrpcServiceManager,
}

struct TiKvEngines<EK: KvEngine, ER: RaftEngine> {
    engines: Engines<EK, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<EK, ServerRaftStoreRouter<EK, ER>>,
}

struct Servers<EK: KvEngine, ER: RaftEngine, F: KvFormat> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    node: MultiRaftServer<RpcClient, EK, ER>,
    importer: Arc<SstImporter<TiFlashEngine>>,
    debugger: DebuggerImpl<ER, RaftKv<EK, ServerRaftStoreRouter<EK, ER>>, LockManager, F>,
}

type LocalServer<EK, ER> = Server<resolve::PdStoreAddrResolver, LocalRaftKv<EK, ER>>;
type LocalRaftKv<EK, ER> = RaftKv<EK, ServerRaftStoreRouter<EK, ER>>;

impl<ER: RaftEngine, F: KvFormat> TiKvServer<ER, F> {
    fn init(
        mut config: TikvConfig,
        proxy_config: ProxyConfig,
        engine_store_server_helper_ptr: isize,
        tx: tikv_util::mpsc::Sender<ServiceEvent>,
    ) -> TiKvServer<ER, F> {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let pd_client = TikvServerCore::connect_to_pd_cluster(
            &mut config,
            env.clone(),
            Arc::clone(&security_mgr),
        );

        #[cfg(feature = "external-jemalloc")]
        info!("linked with external jemalloc");
        #[cfg(not(feature = "external-jemalloc"))]
        info!("linked without external jemalloc");

        // Initialize and check config
        info!("using proxy config"; "config" => ?proxy_config);
        crate::config::address_proxy_config(&mut config, &proxy_config);
        info!("after address config"; "config" => ?config);

        // NOTE: Compat disagg arch upgraded from * to 8.0.
        {
            let raft_engine_path = config.raft_engine.config().dir + "/ps_engine";
            let path = Path::new(&raft_engine_path);
            if path.exists() {
                let new_raft_engine_path = config.raft_engine.config().dir + "/ps_engine.raftlog";
                let new_path = Path::new(&new_raft_engine_path);
                if !new_path.exists() {
                    info!("creating ps_engine.raftlog for upgraded cluster");
                    std::fs::File::create(new_path).unwrap();
                }
            }
        }

        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        let resource_manager = if config.resource_control.enabled {
            let mgr = Arc::new(ResourceGroupManager::default());
            let mut resource_mgr_service =
                ResourceManagerService::new(mgr.clone(), pd_client.clone());
            // spawn a task to periodically update the minimal virtual time of all resource
            // groups.
            let resource_mgr = mgr.clone();
            background_worker.spawn_interval_task(MIN_PRIORITY_UPDATE_INTERVAL, move || {
                resource_mgr.advance_min_virtual_time();
            });
            // spawn a task to watch all resource groups update.
            background_worker.spawn_async_task(async move {
                resource_mgr_service.watch_resource_groups().await;
            });
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
                cfg_controller_clone
                    .get_current()
                    .range_cache_engine
                    .enabled
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

        TiKvServer {
            proxy_config,
            engine_store_server_helper_ptr,
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
            range_cache_engine_statistics: None,
            raft_statistics: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            concurrency_manager,
            env,
            sst_worker: None,
            quota_limiter,
            resource_manager,
            tablet_registry: None,
            grpc_service_mgr: GrpcServiceManager::new(tx),
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TikvConfig) -> ConfigController {
        crate::config::validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    pub fn init_engines(&mut self, engines: Engines<TiFlashEngine, ER>) {
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

        self.engines = Some(TiKvEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(
        &mut self,
    ) -> GcWorker<RaftKv<TiFlashEngine, ServerRaftStoreRouter<TiFlashEngine, ER>>> {
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

        // Create cdc.
        // let mut cdc_worker = Box::new(LazyWorker::new("cdc"));
        // let cdc_scheduler = cdc_worker.scheduler();
        // let txn_extra_scheduler =
        // cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone());
        //
        // self.engines
        //     .as_mut()
        //     .unwrap()
        //     .engine
        //     .set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        // let lock_mgr = LockManager::new(&self.core.config.pessimistic_txn);
        let lock_mgr = LockManager::new();
        // cfg_controller.register(
        //     tikv::config::Module::PessimisticTxn,
        //     Box::new(lock_mgr.config_manager()),
        // );
        // lock_mgr.register_detector_role_change_observer(self.coprocessor_host.
        // as_mut().unwrap());

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

        if let Some(sst_worker) = &mut self.sst_worker {
            let sst_runner = RecoveryRunner::new(
                engines.engines.kv.rocks.clone(),
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

        // TODO(tiflash) Maybe we can remove this service.
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

        // we don't care since we don't start this service
        let dummy_dynamic_configs = tikv::storage::DynamicConfigs {
            pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
            in_memory_pessimistic_lock: Arc::new(AtomicBool::new(true)),
            wake_up_delay_duration_ms: Arc::new(AtomicU64::new(
                ReadableDuration::millis(20).as_millis(),
            )),
        };

        let storage = Storage::<_, _, F>::from_engine(
            engines.engine.clone(),
            &self.core.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            dummy_dynamic_configs,
            flow_controller.clone(),
            pd_sender.clone(),
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            None, // causal_ts_provider
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
            storage.get_engine().raft_extension().clone(),
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

        // // Register causal observer for RawKV API V2
        // if let ApiVersion::V2 = F::TAG {
        //     let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
        //         self.pd_client.clone(),
        //         self.core.config.causal_ts.renew_interval.0,
        //         self.core.config.causal_ts.renew_batch_min_size,
        //     ));
        //     if let Err(e) = tso {
        //         panic!("Causal timestamp provider initialize failed: {:?}", e);
        //     }
        //     let causal_ts_provider = Arc::new(tso.unwrap());
        //     info!("Causal timestamp provider startup.");
        //
        //     let causal_ob = causal_ts::CausalObserver::new(causal_ts_provider);
        //     causal_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // }

        // // Register cdc.
        // let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone());
        // cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // // Register cdc config manager.
        // cfg_controller.register(
        //     tikv::config::Module::CDC,
        //     Box::new(CdcConfigManager(cdc_worker.scheduler())),
        // );

        // // Create resolved ts worker
        // let rts_worker = if self.core.config.resolved_ts.enable {
        //     let worker = Box::new(LazyWorker::new("resolved-ts"));
        //     // Register the resolved ts observer
        //     let resolved_ts_ob = resolved_ts::Observer::new(worker.scheduler());
        //     resolved_ts_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        //     // Register config manager for resolved ts worker
        //     cfg_controller.register(
        //         tikv::config::Module::ResolvedTs,
        //         Box::new(resolved_ts::ResolvedTsConfigManager::new(
        //             worker.scheduler(),
        //         )),
        //     );
        //     Some(worker)
        // } else {
        //     None
        // };

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
        let health_service = HealthService::default();
        let mut default_store = kvproto::metapb::Store::default();

        if !self.proxy_config.server.engine_store_version.is_empty() {
            default_store.set_version(self.proxy_config.server.engine_store_version.clone());
        }
        if !self.proxy_config.server.engine_store_git_hash.is_empty() {
            default_store.set_git_hash(self.proxy_config.server.engine_store_git_hash.clone());
        }
        // addr -> store.peer_address
        if self.core.config.server.advertise_addr.is_empty() {
            default_store.set_peer_address(self.core.config.server.addr.clone());
        } else {
            default_store.set_peer_address(self.core.config.server.advertise_addr.clone())
        }
        // engine_addr -> store.addr
        if !self.proxy_config.server.engine_addr.is_empty() {
            default_store.set_address(self.proxy_config.server.engine_addr.clone());
        } else {
            panic!("engine address is empty");
        }

        let health_controller = HealthController::new();
        let mut node = MultiRaftServer::new(
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

        {
            engine_store_ffi::ffi::gen_engine_store_server_helper(
                self.engine_store_server_helper_ptr,
            )
            .set_store(node.store());
            info!("set store {} to engine-store", node.id());
        }

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

        let check_leader_runner = CheckLeaderRunner::new(
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let check_leader_scheduler = self
            .core
            .background_worker
            .start("check-leader", check_leader_runner);

        self.snap_mgr = Some(snap_mgr.clone());

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

        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage.clone(),
            copr,
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

        let packed_envs = engine_store_ffi::core::PackedEnvs {
            engine_store_cfg: self.proxy_config.engine_store.clone(),
            pd_endpoints: self.core.config.pd.endpoints.clone(),
            snap_handle_pool_size: self.proxy_config.raft_store.snap_handle_pool_size,
        };
        let tiflash_ob = engine_store_ffi::observer::TiFlashObserver::new(
            node.id(),
            self.engines.as_ref().unwrap().engines.kv.clone(),
            self.engines.as_ref().unwrap().engines.raft.clone(),
            importer.clone(),
            server.transport().clone(),
            snap_mgr.clone(),
            packed_envs,
            DebugStruct::default(),
            self.core.encryption_key_manager.clone(),
        );
        tiflash_ob.register_to(self.coprocessor_host.as_mut().unwrap());


        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
                server.get_grpc_mem_quota().clone(),
                copr_config_manager,
            )),
        );

        // // Start backup stream
        // if self.core.config.backup_stream.enable {
        //     // Create backup stream.
        //     let mut backup_stream_worker =
        // Box::new(LazyWorker::new("backup-stream"));
        //     let backup_stream_scheduler = backup_stream_worker.scheduler();
        //
        //     // Register backup-stream observer.
        //     let backup_stream_ob =
        // BackupStreamObserver::new(backup_stream_scheduler.clone());
        //     backup_stream_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        //     // Register config manager.
        //     cfg_controller.register(
        //         tikv::config::Module::BackupStream,
        //         Box::new(BackupStreamConfigManager(backup_stream_worker.
        // scheduler())),     );
        //
        //     let backup_stream_endpoint = backup_stream::Endpoint::new::<String>(
        //         node.id(),
        //         &self.core.config.pd.endpoints,
        //         self.core.config.backup_stream.clone(),
        //         backup_stream_scheduler,
        //         backup_stream_ob,
        //         self.region_info_accessor.clone(),
        //         self.router.clone(),
        //         self.pd_client.clone(),
        //         self.concurrency_manager.clone(),
        //     );
        //     backup_stream_worker.start(backup_stream_endpoint);
        //     self.core.to_stop.push(backup_stream_worker);
        // }

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

        let safe_point = Arc::new(AtomicU64::new(0));
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
            None,
            self.grpc_service_mgr.clone(),
            safe_point,
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        gc_worker
            .start(node.id())
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));

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
        // Start resolved ts

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager::new(
                node.refresh_config_scheduler(),
                raft_store,
            )),
        );

        let mut debugger = DebuggerImpl::new(
            Engines {
                kv: engines.engines.kv.rocks.clone(),
                raft: engines.engines.raft.clone(),
            },
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

        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        // Debug service.
        let debug_service = DebugService::new(
            servers.debugger.clone(),
            servers.server.get_debug_thread_pool().clone(),
            engines.engine.raft_extension(),
            self.engines.as_ref().unwrap().store_meta.clone(),
            Arc::new(|_, _, _, _| false),
        );
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
        // Backup service.
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.tablet_registry.clone().unwrap(),
            self.kv_statistics.clone(),
            self.range_cache_engine_statistics.clone(),
            self.core.config.rocksdb.titan.enabled.map_or(false, |v| v),
            self.engines.as_ref().unwrap().engines.raft.clone(),
            self.raft_statistics.clone(),
        );
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let engines_info_clone = engines_info.clone();
        self.core.background_worker.spawn_interval_task(
            DEFAULT_METRICS_FLUSH_INTERVAL,
            move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info_clone.update(now);
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

    fn init_storage_stats_task(&self, engines: Engines<TiFlashEngine, ER>) {
        let config_disk_capacity: u64 = self.core.config.raft_store.capacity.0;
        let data_dir = self.core.config.storage.data_dir.clone();
        let store_path = self.core.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let reserve_space = disk::get_disk_reserved_space();
        if reserve_space == 0 {
            info!("disk space checker not enabled");
            return;
        }

        let almost_full_threshold = reserve_space;
        let already_full_threshold = reserve_space / 2;
        self.core
            .background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let disk_stats = match fs2::statvfs(&store_path) {
                    Err(e) => {
                        error!(
                            "get disk stat for kv store failed";
                            "kv path" => store_path.to_str(),
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

                let placeholer_file_path = PathBuf::from_str(&data_dir)
                    .unwrap()
                    .join(Path::new(file_system::SPACE_PLACEHOLDER_FILE));

                let placeholder_size: u64 =
                    file_system::get_file_size(&placeholer_file_path).unwrap_or(0);

                let used_size = snap_size + kv_size + raft_size + placeholder_size;
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };

                let mut available = capacity.checked_sub(used_size).unwrap_or_default();
                available = cmp::min(available, disk_stats.available_space());

                let prev_disk_status = disk::get_disk_status(0); //0 no need care about failpoint.
                let cur_disk_status = if available <= already_full_threshold {
                    disk::DiskUsage::AlreadyFull
                } else if available <= almost_full_threshold {
                    disk::DiskUsage::AlmostFull
                } else {
                    disk::DiskUsage::Normal
                };
                if prev_disk_status != cur_disk_status {
                    warn!(
                        "disk usage {:?}->{:?}, available={},snap={},kv={},raft={},capacity={}",
                        prev_disk_status,
                        cur_disk_status,
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
                engine_store_ffi::ffi::gen_engine_store_server_helper(
                    self.engine_store_server_helper_ptr,
                ),
                self.core.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.core.config.security.clone()),
                self.router.clone(),
                self.core.store_path.clone(),
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

    fn stop(self) {
        tikv_util::thread_group::mark_shutdown();
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
