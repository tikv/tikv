// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server components,
//! and handling errors (mostly by aborting and reporting to the user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be explicitly stopped.
//! We keep these components in the `TiKvServer` struct.

use std::{
    cmp,
    convert::TryFrom,
    env, fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use backup_stream::{
    config::BackupStreamConfigManager,
    metadata::{ConnectionConfig, LazyEtcdClient},
    observer::BackupStreamObserver,
};
use cdc::{CdcConfigManager, MemoryQuota};
use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    from_rocks_compression_type,
    raw::{Cache, Env},
    FlowInfo, RocksEngine,
};
use engine_rocks_helper::sst_recovery::{RecoveryRunner, DEFAULT_CHECK_INTERVAL};
use engine_traits::{
    CFOptionsExt, ColumnFamilyOptions, Engines, FlowControlFactorsExt, KvEngine, MiscExt,
    RaftEngine, TabletFactory, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use error_code::ErrorCodeExt;
use file_system::{
    get_io_rate_limiter, set_io_rate_limiter, BytesFetcher, File, IOBudgetAdjustor,
    MetricsManager as IOMetricsManager,
};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use grpcio_health::HealthService;
use kvproto::{
    brpb::create_backup, cdcpb::create_change_data, deadlock::create_deadlock,
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
    kvrpcpb::ApiVersion, resource_usage_agent::create_resource_metering_pub_sub,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{
        config::SplitCheckConfigManager, BoxConsistencyCheckObserver, ConsistencyCheckMethod,
        CoprocessorHost, RawConsistencyCheckObserver, RegionInfoAccessor,
    },
    router::ServerRaftStoreRouter,
    store::{
        config::RaftstoreConfigManager,
        fsm,
        fsm::store::{
            RaftBatchSystem, RaftRouter, StoreMeta, MULTI_FILES_SNAPSHOT_FEATURE, PENDING_MSG_CAP,
        },
        memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE,
        AutoSplitController, CheckLeaderRunner, GlobalReplicationState, LocalReader, SnapManager,
        SnapManagerBuilder, SplitCheckRunner, SplitConfigManager,
    },
};
use security::SecurityManager;
use tikv::{
    config::{ConfigController, DBConfigManger, DBType, TiKvConfig},
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{build_yatp_read_pool, ReadPool, ReadPoolConfigManager},
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        create_raft_storage,
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        ttl::TtlChecker,
        KvEngineFactoryBuilder, Node, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID,
        GRPC_THREAD_PREFIX,
    },
    storage::{
        self,
        config_manager::StorageConfigManger,
        mvcc::MvccConsistencyCheckObserver,
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine,
    },
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, RaftDataStateMachine, VersionTrack},
    math::MovingAvgU32,
    metrics::INSTANCE_BACKEND_CPU_QUOTA,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{cpu_time::ProcessStat, disk, register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler, Worker},
};
use tokio::runtime::Builder;

use crate::{
    memory::*, raft_engine_switch::*, setup::*, signal_handler,
    tikv_util::sys::thread::ThreadBuildWrapper,
};

// minimum number of core kept for background requests
const BACKGROUND_REQUEST_CORE_LOWER_BOUND: f64 = 1.0;
// max ratio of core quota for background requests
const BACKGROUND_REQUEST_CORE_MAX_RATIO: f64 = 0.95;
// default ratio of core quota for background requests = core_number * 0.5
const BACKGROUND_REQUEST_CORE_DEFAULT_RATIO: f64 = 0.5;
// indication of TiKV instance is short of cpu
const SYSTEM_BUSY_THRESHOLD: f64 = 0.80;
// indication of TiKV instance in healthy state when cpu usage is in [0.5, 0.80)
const SYSTEM_HEALTHY_THRESHOLD: f64 = 0.50;
// pace of cpu quota adjustment
const CPU_QUOTA_ADJUSTMENT_PACE: f64 = 200.0; // 0.2 vcpu

#[inline]
fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(config: TiKvConfig) {
    let mut tikv = TiKvServer::<CER>::init(config);

    // Must be called after `TiKvServer::init`.
    let memory_limit = tikv.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.check_conflict_addr();
    tikv.init_fs();
    tikv.init_yatp();
    tikv.init_encryption();
    let fetcher = tikv.init_io_utility();
    let listener = tikv.init_flow_receiver();
    let (engines, engines_info) = tikv.init_raw_engines(listener);
    tikv.init_engines(engines.clone());
    let server_config = tikv.init_servers::<F>();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();
    tikv.init_quota_tuning_task(tikv.quota_limiter.clone());

    signal_handler::wait_for_signal(Some(tikv.engines.take().unwrap().engines));
    tikv.stop();
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TiKvConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info(build_timestamp);

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<RocksEngine, API>(config)
        } else {
            run_impl::<RaftLogEngine, API>(config)
        }
    })
}

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL: Duration = Duration::from_secs(5);

/// A complete TiKV server.
struct TiKvServer<ER: RaftEngine> {
    config: TiKvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<RocksEngine, ER>,
    flow_info_sender: Option<mpsc::Sender<FlowInfo>>,
    flow_info_receiver: Option<mpsc::Receiver<FlowInfo>>,
    system: Option<RaftBatchSystem<RocksEngine, ER>>,
    resolver: resolve::PdStoreAddrResolver,
    state: Arc<Mutex<GlobalReplicationState>>,
    store_path: PathBuf,
    snap_mgr: Option<SnapManager>, // Will be filled in `init_servers`.
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    engines: Option<TiKvEngines<RocksEngine, ER>>,
    servers: Option<Servers<RocksEngine, ER>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<RocksEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
}

struct TiKvEngines<EK: KvEngine, ER: RaftEngine> {
    engines: Engines<EK, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<EK, ServerRaftStoreRouter<EK, ER>>,
}

struct Servers<EK: KvEngine, ER: RaftEngine> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    node: Node<RpcClient, EK, ER>,
    importer: Arc<SstImporter>,
    cdc_scheduler: tikv_util::worker::Scheduler<cdc::Task>,
    cdc_memory_quota: MemoryQuota,
    rsmeter_pubsub_service: resource_metering::PubSubService,
}

type LocalServer<EK, ER> =
    Server<RaftRouter<EK, ER>, resolve::PdStoreAddrResolver, LocalRaftKv<EK, ER>>;
type LocalRaftKv<EK, ER> = RaftKv<EK, ServerRaftStoreRouter<EK, ER>>;

impl<ER: RaftEngine> TiKvServer<ER> {
    fn init(mut config: TiKvConfig) -> TiKvServer<ER> {
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
        let pd_client =
            Self::connect_to_pd_cluster(&mut config, env.clone(), Arc::clone(&security_mgr));

        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        // Initialize raftstore channels.
        let (router, system) = fsm::create_raft_batch_system(&config.raft_store);

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();
        let (resolver, state) =
            resolve::new_resolver(Arc::clone(&pd_client), &background_worker, router.clone());

        let mut coprocessor_host = Some(CoprocessorHost::new(
            router.clone(),
            config.coprocessor.clone(),
        ));
        let region_info_accessor = RegionInfoAccessor::new(coprocessor_host.as_mut().unwrap());

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
            config,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver,
            state,
            store_path,
            snap_mgr: None,
            encryption_key_manager: None,
            engines: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            flow_info_sender: None,
            flow_info_receiver: None,
            sst_worker: None,
            quota_limiter,
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TiKvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

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

    fn connect_to_pd_cluster(
        config: &mut TiKvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(&lock_dir);
        file_system::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in file_system::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let _ = try_lock_conflict_addr(file_path);
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have enough space
        // to do compaction and region migration when TiKV recover. This file is created in
        // data_dir rather than db_path, because we must not increase store size of db_path.
        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        let mut reserve_space = self.config.storage.reserve_space.0;
        if self.config.storage.reserve_space.0 != 0 {
            reserve_space = cmp::max(
                (capacity as f64 * 0.05) as u64,
                self.config.storage.reserve_space.0,
            );
        }
        disk::set_disk_reserved_space(reserve_space);
        let path =
            Path::new(&self.config.storage.data_dir).join(file_system::SPACE_PLACEHOLDER_FILE);
        if let Err(e) = file_system::remove_file(&path) {
            warn!("failed to remove space holder on starting: {}", e);
        }

        let available = disk_stats.available_space();
        // place holder file size is 20% of total reserved space.
        if available > reserve_space {
            file_system::reserve_space_for_recover(
                &self.config.storage.data_dir,
                reserve_space / 5,
            )
            .map_err(|e| panic!("Failed to reserve space for recovery: {}.", e))
            .unwrap();
        } else {
            warn!("no enough disk space left to create the place holder file");
        }
    }

    fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("tikv"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
    }

    fn init_encryption(&mut self) {
        self.encryption_key_manager = data_key_manager_from_config(
            &self.config.security.encryption,
            &self.config.storage.data_dir,
        )
        .map_err(|e| {
            panic!(
                "Encryption failed to initialize: {}. code: {}",
                e,
                e.error_code()
            )
        })
        .unwrap()
        .map(Arc::new);
    }

    fn init_flow_receiver(&mut self) -> engine_rocks::FlowListener {
        let (tx, rx) = mpsc::channel();
        self.flow_info_sender = Some(tx.clone());
        self.flow_info_receiver = Some(rx);
        engine_rocks::FlowListener::new(tx)
    }

    fn init_engines(&mut self, engines: Engines<RocksEngine, ER>) {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
        let engine = RaftKv::new(
            ServerRaftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(engines.kv.clone(), store_meta.clone(), self.router.clone()),
            ),
            engines.kv.clone(),
        );

        self.engines = Some(TiKvEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(
        &mut self,
    ) -> GcWorker<
        RaftKv<RocksEngine, ServerRaftStoreRouter<RocksEngine, ER>>,
        RaftRouter<RocksEngine, ER>,
    > {
        let engines = self.engines.as_ref().unwrap();
        let mut gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.router.clone(),
            self.flow_info_sender.take().unwrap(),
            self.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
        );
        gc_worker
            .start()
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));
        gc_worker
            .start_observe_lock_apply(
                self.coprocessor_host.as_mut().unwrap(),
                self.concurrency_manager.clone(),
            )
            .unwrap_or_else(|e| fatal!("gc worker failed to observe lock apply: {}", e));

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers<F: KvFormat>(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let flow_controller = Arc::new(FlowController::Singleton(EngineFlowController::new(
            &self.config.storage.flow_control,
            self.engines.as_ref().unwrap().engine.kv_engine(),
            self.flow_info_receiver.take().unwrap(),
        )));
        let gc_worker = self.init_gc_worker();
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
        let mut cdc_worker = Box::new(LazyWorker::new("cdc"));
        let cdc_scheduler = cdc_worker.scheduler();
        let txn_extra_scheduler = cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone());

        self.engines
            .as_mut()
            .unwrap()
            .engine
            .set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        let lock_mgr = LockManager::new(&self.config.pessimistic_txn);
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
                engines.engines.kv.get_sync_db(),
                engines.store_meta.clone(),
                self.config.storage.background_error_recovery_window.into(),
                DEFAULT_CHECK_INTERVAL,
            );
            sst_worker.start_with_timer(sst_runner);
        }

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                pd_sender.clone(),
                engines.engine.clone(),
            ))
        } else {
            None
        };

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );

        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(self.config.resource_metering.precision.as_millis());
        self.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.config.resource_metering.clone(),
                collector_reg_handle.clone(),
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle.clone(),
        );
        self.to_stop.push(single_target_worker);
        let rsmeter_pubsub_service = resource_metering::PubSubService::new(data_sink_reg_handle);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        let storage_read_pool_handle = if self.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };

        let storage = create_raft_storage::<_, _, _, F>(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            flow_controller.clone(),
            pd_sender.clone(),
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));
        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.config.storage.block_cache.shared,
                ttl_scheduler,
                flow_controller,
                storage.get_scheduler(),
            )),
        );

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create snapshot manager, server.
        let snap_path = self
            .store_path
            .join(Path::new("snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let bps = i64::try_from(self.config.server.snap_max_write_bytes_per_sec.0)
            .unwrap_or_else(|_| fatal!("snap_max_write_bytes_per_sec > i64::max_value"));

        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(bps)
            .max_total_size(self.config.server.snap_max_total_size.0)
            .encryption_key_manager(self.encryption_key_manager.clone())
            .max_per_file_size(self.config.raft_store.max_snapshot_file_raw_size.0)
            .enable_multi_snapshot_files(
                self.pd_client
                    .feature_gate()
                    .can_enable(MULTI_FILES_SNAPSHOT_FEATURE),
            )
            .build(snap_path);

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        if self.config.readpool.is_unified_pool_enabled() {
            cfg_controller.register(
                tikv::config::Module::Readpool,
                Box::new(ReadPoolConfigManager(
                    unified_read_pool.as_ref().unwrap().handle(),
                )),
            );
        }

        // Register causal observer for RawKV API V2
        if let ApiVersion::V2 = F::TAG {
            let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
                self.pd_client.clone(),
                self.config.causal_ts.renew_interval.0,
                self.config.causal_ts.renew_batch_min_size,
            ));
            if let Err(e) = tso {
                panic!("Causal timestamp provider initialize failed: {:?}", e);
            }
            let causal_ts_provider = Arc::new(tso.unwrap());
            info!("Causal timestamp provider startup.");

            let causal_ob = causal_ts::CausalObserver::new(causal_ts_provider);
            causal_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        }

        // Register cdc.
        let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone());
        cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // Register cdc config manager.
        cfg_controller.register(
            tikv::config::Module::CDC,
            Box::new(CdcConfigManager(cdc_worker.scheduler())),
        );

        // Create resolved ts worker
        let rts_worker = if self.config.resolved_ts.enable {
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

        let check_leader_runner = CheckLeaderRunner::new(engines.store_meta.clone());
        let check_leader_scheduler = self
            .background_worker
            .start("check-leader", check_leader_runner);

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate(
                self.config.coprocessor.region_split_size,
                self.config.coprocessor.enable_region_bucket,
                self.config.coprocessor.region_bucket_size,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.config.raft_store.clone()));
        let health_service = HealthService::default();
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store.clone(),
            self.config.storage.api_version(),
            self.pd_client.clone(),
            self.state.clone(),
            self.background_worker.clone(),
            Some(health_service.clone()),
        );
        node.try_bootstrap_store(engines.engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        self.snap_mgr = Some(snap_mgr.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                resource_tag_factory,
                Arc::clone(&self.quota_limiter),
            ),
            coprocessor_v2::Endpoint::new(&self.config.coprocessor_v2),
            self.router.clone(),
            self.resolver.clone(),
            snap_mgr.clone(),
            gc_worker.clone(),
            check_leader_scheduler,
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
            health_service,
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

        // Start backup stream
        if self.config.backup_stream.enable {
            // Create backup stream.
            let mut backup_stream_worker = Box::new(LazyWorker::new("backup-stream"));
            let backup_stream_scheduler = backup_stream_worker.scheduler();

            // Register backup-stream observer.
            let backup_stream_ob = BackupStreamObserver::new(backup_stream_scheduler.clone());
            backup_stream_ob.register_to(self.coprocessor_host.as_mut().unwrap());
            // Register config manager.
            cfg_controller.register(
                tikv::config::Module::BackupStream,
                Box::new(BackupStreamConfigManager(backup_stream_worker.scheduler())),
            );

            let etcd_cli = LazyEtcdClient::new(
                self.config.pd.endpoints.as_slice(),
                ConnectionConfig {
                    keep_alive_interval: self.config.server.grpc_keepalive_time.0,
                    keep_alive_timeout: self.config.server.grpc_keepalive_timeout.0,
                    tls: self.security_mgr.tonic_tls_config(),
                },
            );
            let backup_stream_endpoint = backup_stream::Endpoint::new(
                node.id(),
                etcd_cli,
                self.config.backup_stream.clone(),
                backup_stream_scheduler,
                backup_stream_ob,
                self.region_info_accessor.clone(),
                self.router.clone(),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
            );
            backup_stream_worker.start(backup_stream_endpoint);
            self.to_stop.push(backup_stream_worker);
        }

        let import_path = self.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.config.import,
            import_path,
            self.encryption_key_manager.clone(),
            self.config.storage.api_version(),
        )
        .unwrap();
        for (cf_name, compression_type) in &[
            (
                CF_DEFAULT,
                self.config.rocksdb.defaultcf.bottommost_level_compression,
            ),
            (
                CF_WRITE,
                self.config.rocksdb.writecf.bottommost_level_compression,
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
            .background_worker
            .start("split-check", split_check_runner);
        cfg_controller.register(
            tikv::config::Module::Coprocessor,
            Box::new(SplitCheckConfigManager(split_check_scheduler.clone())),
        );

        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(self.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(split_config_manager);

        // `ConsistencyCheckObserver` must be registered before `Node::start`.
        let safe_point = Arc::new(AtomicU64::new(0));
        let observer = match self.config.coprocessor.consistency_check_method {
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
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        // Start auto gc. Must after `Node::start` because `node_id` is initialized there.
        assert!(node.id() > 0); // Node id should never be 0.
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone(),
            node.id(),
        );
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config, safe_point) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        initial_metric(&self.config.metric);
        if self.config.storage.enable_ttl {
            ttl_checker.start_with_timer(TtlChecker::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.region_info_accessor.clone(),
                self.config.storage.ttl_check_poll_interval.into(),
            ));
            self.to_stop.push(ttl_checker);
        }

        // Start CDC.
        let cdc_memory_quota = MemoryQuota::new(self.config.cdc.sink_memory_quota.0 as _);
        let cdc_endpoint = cdc::Endpoint::new(
            self.config.server.cluster_id,
            &self.config.cdc,
            self.config.storage.api_version(),
            self.pd_client.clone(),
            cdc_scheduler.clone(),
            self.router.clone(),
            self.engines.as_ref().unwrap().engines.kv.clone(),
            cdc_ob,
            engines.store_meta.clone(),
            self.concurrency_manager.clone(),
            server.env(),
            self.security_mgr.clone(),
            cdc_memory_quota.clone(),
        );
        cdc_worker.start_with_timer(cdc_endpoint);
        self.to_stop.push(cdc_worker);

        // Start resolved ts
        if let Some(mut rts_worker) = rts_worker {
            let rts_endpoint = resolved_ts::Endpoint::new(
                &self.config.resolved_ts,
                rts_worker.scheduler(),
                self.router.clone(),
                engines.store_meta.clone(),
                self.pd_client.clone(),
                self.concurrency_manager.clone(),
                server.env(),
                self.security_mgr.clone(),
                // TODO: replace to the cdc sinker
                resolved_ts::DummySinker::new(),
            );
            rts_worker.start_with_timer(rts_endpoint);
            self.to_stop.push(rts_worker);
        }

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager::new(
                node.refresh_config_scheduler(),
                raft_store,
            )),
        );

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
            cdc_scheduler,
            cdc_memory_quota,
            rsmeter_pubsub_service,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Import SST service.
        let import_service = ImportSstService::new(
            self.config.import.clone(),
            self.config.raft_store.raft_entry_max_size,
            self.router.clone(),
            engines.engines.kv.clone(),
            servers.importer.clone(),
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
            engines.engines.clone(),
            servers.server.get_debug_thread_pool().clone(),
            self.router.clone(),
            self.cfg_controller.as_ref().unwrap().clone(),
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
            self.config.log.file.filename.clone(),
            self.config.slow_log_file.clone(),
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
                self.resolver.clone(),
                self.security_mgr.clone(),
                &self.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        // Backup service.
        let mut backup_worker = Box::new(self.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_service = backup::Service::new(backup_scheduler);
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_endpoint = backup::Endpoint::new(
            servers.node.id(),
            engines.engine.clone(),
            self.region_info_accessor.clone(),
            engines.engines.kv.as_inner().clone(),
            self.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.config.storage.api_version(),
        );
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
    }

    fn init_io_utility(&mut self) -> BytesFetcher {
        let stats_collector_enabled = file_system::init_io_stats_collector()
            .map_err(|e| warn!("failed to init I/O stats collector: {}", e))
            .is_ok();

        let limiter = Arc::new(
            self.config
                .storage
                .io_rate_limit
                .build(!stats_collector_enabled /*enable_statistics*/),
        );
        let fetcher = if stats_collector_enabled {
            BytesFetcher::FromIOStatsCollector()
        } else {
            BytesFetcher::FromRateLimiter(limiter.statistics().unwrap())
        };
        // Set up IO limiter even when rate limit is disabled, so that rate limits can be
        // dynamically applied later on.
        set_io_rate_limiter(Some(limiter));
        fetcher
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.engines.as_ref().unwrap().engines.clone(),
        );
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let engines_info_clone = engines_info.clone();
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info_clone.update(now);
            });
        if let Some(limiter) = get_io_rate_limiter() {
            limiter.set_low_priority_io_adjustor_if_needed(Some(engines_info));
        }

        let mut mem_trace_metrics = MemoryTraceManager::default();
        mem_trace_metrics.register_provider(MEMTRACE_RAFTSTORE.clone());
        mem_trace_metrics.register_provider(MEMTRACE_COPROCESSOR.clone());
        self.background_worker
            .spawn_interval_task(DEFAULT_MEMTRACE_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                mem_trace_metrics.flush(now);
            });
    }

    // Only background cpu quota tuning is implemented at present. iops and frontend quota tuning is on the way
    fn init_quota_tuning_task(&self, quota_limiter: Arc<QuotaLimiter>) {
        // No need to do auto tune when capacity is really low
        if SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO
            < BACKGROUND_REQUEST_CORE_LOWER_BOUND
        {
            return;
        };

        // Determine the base cpu quota
        let base_cpu_quota = {
            // if cpu quota is not specified, start from optimistic case
            if quota_limiter.cputime_limiter(false).is_infinite() {
                let quota = 1000_f64
                    * f64::max(
                        BACKGROUND_REQUEST_CORE_LOWER_BOUND,
                        SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_DEFAULT_RATIO,
                    );
                quota_limiter.set_cpu_time_limit(quota as usize, false);
                quota
            } else {
                quota_limiter.cputime_limiter(false) / 1000_f64
            }
        };

        // Calculate the celling and floor quota
        let celling_quota = f64::min(
            base_cpu_quota * 2.0,
            1_000_f64 * SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO,
        );
        let floor_quota = f64::max(
            base_cpu_quota * 0.5,
            1_000_f64 * BACKGROUND_REQUEST_CORE_LOWER_BOUND,
        );

        let mut proc_stats: ProcessStat = ProcessStat::cur_proc_stat().unwrap();
        self.background_worker.spawn_interval_task(
            DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL,
            move || {
                if quota_limiter.auto_tune_enabled() {
                    let old_quota = quota_limiter.cputime_limiter(false) / 1000_f64;
                    let cpu_usage = match proc_stats.cpu_usage() {
                        Ok(r) => r,
                        Err(_e) => 0.0,
                    };
                    // Try tuning quota when cpu_usage is correctly collected.
                    // rule based tuning:
                    //      1) if instance is busy, shrink cpu quota for analyze by one quota pace until lower bound is hit;
                    //      2) if instance cpu usage is healthy, no op;
                    //      3) if instance is idle, increase cpu quota by one quota pace  until upper bound is hit.
                    if cpu_usage > 0.0f64 {
                        let mut target_quota = old_quota;

                        let cpu_util = cpu_usage / SysQuota::cpu_cores_quota();
                        if cpu_util >= SYSTEM_BUSY_THRESHOLD {
                            target_quota =
                                f64::max(target_quota - CPU_QUOTA_ADJUSTMENT_PACE, floor_quota);
                        } else if cpu_util < SYSTEM_HEALTHY_THRESHOLD {
                            target_quota =
                                f64::min(target_quota + CPU_QUOTA_ADJUSTMENT_PACE, celling_quota);
                        }

                        if old_quota != target_quota {
                            quota_limiter.set_cpu_time_limit(target_quota as usize, false);
                            debug!(
                                "cpu_time_limiter tuned for backend request";
                                "cpu_util" => ?cpu_util,
                                "new quota" => ?target_quota);
                            INSTANCE_BACKEND_CPU_QUOTA.set(target_quota as i64);
                        }
                    }
                }
            },
        );
    }

    fn init_storage_stats_task(&self, engines: Engines<RocksEngine, ER>) {
        let config_disk_capacity: u64 = self.config.raft_store.capacity.0;
        let data_dir = self.config.storage.data_dir.clone();
        let store_path = self.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let reserve_space = disk::get_disk_reserved_space();
        if reserve_space == 0 {
            info!("disk space checker not enabled");
            return;
        }

        let almost_full_threshold = reserve_space;
        let already_full_threshold = reserve_space / 2;
        self.background_worker
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
            .start(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
                self.store_path.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
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

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

pub trait ConfiguredRaftEngine: RaftEngine {
    fn build(
        _: &TiKvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Option<Cache>,
    ) -> Self;
    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        None
    }
    fn register_config(&self, _cfg_controller: &mut ConfigController, _share_cache: bool) {}
}

impl ConfiguredRaftEngine for RocksEngine {
    fn build(
        config: &TiKvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Option<Cache>,
    ) -> Self {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_engine.config().dir,
            &config.raft_store.raftdb_path,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_db_path = &config.raft_store.raftdb_path;
        let config_raftdb = &config.raftdb;
        let mut raft_db_opts = config_raftdb.build_opt();
        raft_db_opts.set_env(env.clone());
        let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
        let raftdb =
            engine_rocks::raw_util::new_engine_opt(raft_db_path, raft_db_opts, raft_cf_opts)
                .expect("failed to open raftdb");
        let mut raftdb = RocksEngine::from_db(Arc::new(raftdb));
        raftdb.set_shared_block_cache(block_cache.is_some());

        if should_dump {
            let raft_engine =
                RaftLogEngine::new(config.raft_engine.config(), key_manager.clone(), None)
                    .expect("failed to open raft engine for migration");
            dump_raft_engine_to_raftdb(&raft_engine, &raftdb, 8 /*threads*/);
            raft_engine.stop();
            drop(raft_engine);
            raft_data_state_machine.after_dump_data();
        }
        raftdb
    }

    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        Some(self)
    }

    fn register_config(&self, cfg_controller: &mut ConfigController, share_cache: bool) {
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DBConfigManger::new(self.clone(), DBType::Raft, share_cache)),
        );
    }
}

impl ConfiguredRaftEngine for RaftLogEngine {
    fn build(
        config: &TiKvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Option<Cache>,
    ) -> Self {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_store.raftdb_path,
            &config.raft_engine.config().dir,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_config = config.raft_engine.config();
        let raft_engine =
            RaftLogEngine::new(raft_config, key_manager.clone(), get_io_rate_limiter())
                .expect("failed to open raft engine");

        if should_dump {
            let config_raftdb = &config.raftdb;
            let mut raft_db_opts = config_raftdb.build_opt();
            raft_db_opts.set_env(env.clone());
            let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
            let raftdb = engine_rocks::raw_util::new_engine_opt(
                &config.raft_store.raftdb_path,
                raft_db_opts,
                raft_cf_opts,
            )
            .expect("failed to open raftdb for migration");
            let raftdb = RocksEngine::from_db(Arc::new(raftdb));
            dump_raftdb_to_raft_engine(&raftdb, &raft_engine, 8 /*threads*/);
            raftdb.stop();
            drop(raftdb);
            raft_data_state_machine.after_dump_data();
        }
        raft_engine
    }
}

impl<CER: ConfiguredRaftEngine> TiKvServer<CER> {
    fn init_raw_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
    ) -> (Engines<RocksEngine, CER>, Arc<EnginesResourceInfo>) {
        let block_cache = self.config.storage.block_cache.build_shared_cache();
        let env = self
            .config
            .build_shared_rocks_env(self.encryption_key_manager.clone(), get_io_rate_limiter())
            .unwrap();

        // Create raft engine
        let raft_engine = CER::build(
            &self.config,
            &env,
            &self.encryption_key_manager,
            &block_cache,
        );

        // Create kv engine.
        let mut builder = KvEngineFactoryBuilder::new(env, &self.config, &self.store_path)
            .compaction_filter_router(self.router.clone())
            .region_info_accessor(self.region_info_accessor.clone())
            .sst_recovery_sender(self.init_sst_recovery_sender())
            .flow_listener(flow_listener);
        if let Some(cache) = block_cache {
            builder = builder.block_cache(cache);
        }
        let factory = builder.build();
        let kv_engine = factory
            .create_shared_db()
            .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));
        let engines = Engines::new(kv_engine, raft_engine);

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.storage.block_cache.shared,
            )),
        );
        engines
            .raft
            .register_config(cfg_controller, self.config.storage.block_cache.shared);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            &engines, 180, /*max_samples_to_preserve*/
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

fn check_system_config(config: &TiKvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl<E, R> Stop for StatusServer<E, R>
where
    E: 'static,
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

pub struct EngineMetricsManager<EK: KvEngine, R: RaftEngine> {
    engines: Engines<EK, R>,
    last_reset: Instant,
}

impl<EK: KvEngine, R: RaftEngine> EngineMetricsManager<EK, R> {
    pub fn new(engines: Engines<EK, R>) -> Self {
        EngineMetricsManager {
            engines,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        KvEngine::flush_metrics(&self.engines.kv, "kv");
        self.engines.raft.flush_metrics("raft");
        if now.saturating_duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            KvEngine::reset_statistics(&self.engines.kv);
            self.engines.raft.reset_statistics();
            self.last_reset = now;
        }
    }
}

pub struct EnginesResourceInfo {
    kv_engine: RocksEngine,
    raft_engine: Option<RocksEngine>,
    latest_normalized_pending_bytes: AtomicU32,
    normalized_pending_bytes_collector: MovingAvgU32,
}

impl EnginesResourceInfo {
    const SCALE_FACTOR: u64 = 100;

    fn new<CER: ConfiguredRaftEngine>(
        engines: &Engines<RocksEngine, CER>,
        max_samples_to_preserve: usize,
    ) -> Self {
        let raft_engine = engines.raft.as_rocks_engine().cloned();
        EnginesResourceInfo {
            kv_engine: engines.kv.clone(),
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(&self, _now: Instant) {
        let mut normalized_pending_bytes = 0;

        fn fetch_engine_cf(engine: &RocksEngine, cf: &str, normalized_pending_bytes: &mut u32) {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                if let Ok(Some(b)) = engine.get_cf_pending_compaction_bytes(cf) {
                    if cf_opts.get_soft_pending_compaction_bytes_limit() > 0 {
                        *normalized_pending_bytes = std::cmp::max(
                            *normalized_pending_bytes,
                            (b * EnginesResourceInfo::SCALE_FACTOR
                                / cf_opts.get_soft_pending_compaction_bytes_limit())
                                as u32,
                        );
                    }
                }
            }
        }

        if let Some(raft_engine) = &self.raft_engine {
            fetch_engine_cf(raft_engine, CF_DEFAULT, &mut normalized_pending_bytes);
        }
        for cf in &[CF_DEFAULT, CF_WRITE, CF_LOCK] {
            fetch_engine_cf(&self.kv_engine, cf, &mut normalized_pending_bytes);
        }
        let (_, avg) = self
            .normalized_pending_bytes_collector
            .add(normalized_pending_bytes);
        self.latest_normalized_pending_bytes.store(
            std::cmp::max(normalized_pending_bytes, avg),
            Ordering::Relaxed,
        );
    }
}

impl IOBudgetAdjustor for EnginesResourceInfo {
    fn adjust(&self, total_budgets: usize) -> usize {
        let score = self.latest_normalized_pending_bytes.load(Ordering::Relaxed) as f32
            / Self::SCALE_FACTOR as f32;
        // Two reasons for adding `sqrt` on top:
        // 1) In theory the convergence point is independent of the value of pending
        //    bytes (as long as backlog generating rate equals consuming rate, which is
        //    determined by compaction budgets), a convex helps reach that point while
        //    maintaining low level of pending bytes.
        // 2) Variance of compaction pending bytes grows with its magnitude, a filter
        //    with decreasing derivative can help balance such trend.
        let score = score.sqrt();
        // The target global write flow slides between Bandwidth / 2 and Bandwidth.
        let score = 0.5 + score / 2.0;
        (total_budgets as f32 * score) as usize
    }
}
