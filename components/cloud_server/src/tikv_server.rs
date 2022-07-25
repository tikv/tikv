// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server components,
//! and handling errors (mostly by aborting and reporting to the user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be explicitly stopped.
//! We keep these components in the `TiKVServer` struct.

use std::{
    env, fmt,
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Once},
    u64,
};

use api_version::{ApiV1, KvFormat};
use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::from_rocks_compression_type;
use engine_traits::{KvEngine, RaftEngine, CF_DEFAULT, CF_WRITE};
use error_code::ErrorCodeExt;
use file_system::{
    BytesFetcher, IORateLimitMode, IORateLimiter, MetricsManager as IOMetricsManager,
};
use fs2::FileExt;
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use kvengine::dfs::DFS;
use kvproto::{
    brpb_grpc::create_backup, deadlock::create_deadlock, import_sstpb_grpc::create_import_sst,
};
use pd_client::{PdClient, RpcClient};
use raftstore::{
    coprocessor::{
        BoxConsistencyCheckObserver, ConsistencyCheckMethod, CoprocessorHost,
        RawConsistencyCheckObserver,
    },
    RegionInfoAccessor,
};
use rfengine::RfEngine;
use rfstore::{
    store::{
        Engines, LocalReader, MetaChangeListener, RaftBatchSystem, StoreMeta, PENDING_MSG_CAP,
    },
    RaftRouter, ServerRaftStoreRouter,
};
use security::SecurityManager;
use sst_importer::SstImporter;
use tikv::{
    config::{ConfigController, TiKvConfig},
    coprocessor, coprocessor_v2,
    read_pool::{build_yatp_read_pool, ReadPool},
    server::{
        config::Config as ServerConfig, lock_manager::LockManager, raftkv::ReplicaReadLockChecker,
        CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID, GRPC_THREAD_PREFIX,
    },
    storage::{self, mvcc::MvccConsistencyCheckObserver, txn::flow_controller::FlowController},
};
use tikv_kv::Engine;
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, VersionTrack},
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::{Duration, Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Worker},
};
use tokio::runtime::Builder;

use crate::{
    node::*,
    raftkv::*,
    resolve,
    server::Server,
    service::ImportSstService,
    setup::{initial_logger, initial_metric, validate_and_persist_config},
    status_server::StatusServer,
};

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);

/// A complete TiKV server.
pub struct TiKVServer {
    config: TiKvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<dyn PdClient>,
    system: Option<RaftBatchSystem>,
    router: RaftRouter,
    resolver: resolve::PdStoreAddrResolver,
    store_path: PathBuf,
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    raw_engines: Engines,
    engines: Option<TiKVEngines>,
    servers: Option<Servers>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<kvengine::Engine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    quota_limiter: Arc<QuotaLimiter>,
    io_rate_limiter: Arc<IORateLimiter>,
}

struct TiKVEngines {
    store_meta: Option<StoreMeta>,
    engine: RaftKv,
}

struct Servers {
    lock_mgr: LockManager,
    server: Server<RaftRouter, resolve::PdStoreAddrResolver>,
    node: Node,
    importer: Arc<SstImporter>,
}

impl TiKVServer {
    pub fn new(mut config: TiKvConfig) -> TiKVServer {
        let (security_mgr, env, pd, dfs) = Self::prepare(&mut config);
        Self::setup(config, security_mgr, env, pd, dfs)
    }

    pub fn prepare(
        config: &mut TiKvConfig,
    ) -> (
        Arc<SecurityManager>,
        Arc<Environment>,
        Arc<dyn PdClient>,
        Arc<dyn DFS>,
    ) {
        // Sets the global logger ASAP.
        // It is okay to use the config w/o `validate()`,
        // because `initial_logger()` handles various conditions.
        initial_logger(config);

        // Print version information.
        let build_timestamp = option_env!("TIKV_BUILD_TIME");
        tikv::log_tikv_info(build_timestamp);

        // Print resource quota.
        SysQuota::log_quota();
        CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

        // Do some prepare works before start.
        pre_start();

        let _m = Monitor::default();
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
            TiKVServer::connect_to_pd_cluster(config, env.clone(), Arc::clone(&security_mgr));

        let dfs_conf = &config.dfs;
        let dfs: Arc<dyn DFS> = if dfs_conf.s3_bucket.is_empty() && dfs_conf.s3_endpoint.is_empty()
            || dfs_conf.s3_endpoint == "local"
        {
            let local_path = PathBuf::from(&config.storage.data_dir).join(Path::new("local"));
            Arc::new(kvengine::dfs::LocalFS::new(&local_path))
        } else if dfs_conf.s3_endpoint == "memory" {
            Arc::new(kvengine::dfs::InMemFS::new())
        } else {
            Arc::new(kvengine::dfs::S3FS::new(
                dfs_conf.prefix.clone(),
                dfs_conf.s3_endpoint.clone(),
                dfs_conf.s3_key_id.clone(),
                dfs_conf.s3_secret_key.clone(),
                dfs_conf.s3_region.clone(),
                dfs_conf.s3_bucket.clone(),
            ))
        };

        (security_mgr, env, pd_client, dfs)
    }

    pub fn setup(
        config: TiKvConfig,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        pd_client: Arc<dyn PdClient>,
        dfs: Arc<dyn DFS>,
    ) -> TiKVServer {
        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();
        let io_rate_limiter = Arc::new(IORateLimiter::new(IORateLimitMode::WriteOnly, true, true));
        io_rate_limiter
            .set_io_rate_limit(config.storage.io_rate_limit.max_bytes_per_sec.0 as usize);
        let raw_engines =
            Self::init_raw_engines(pd_client.clone(), &config, dfs, io_rate_limiter.clone());

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        // Initialize raftstore channels.
        let rfstore_conf =
            rfstore::store::Config::from_old(&config.raft_store, &config.coprocessor);
        let system = rfstore::store::RaftBatchSystem::new(&raw_engines, &rfstore_conf);
        let router = system.router();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        let resolver =
            resolve::new_resolver(Arc::clone(&pd_client), &background_worker, router.clone());
        let mut coprocessor_host = Some(CoprocessorHost::default());
        let region_info_accessor = RegionInfoAccessor::new(coprocessor_host.as_mut().unwrap());

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.max_delay_duration,
        ));
        info!("created tikv server");
        TiKVServer {
            config,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver,
            store_path,
            encryption_key_manager: None,
            raw_engines,
            engines: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            quota_limiter,
            io_rate_limiter,
        }
    }

    pub fn run(&mut self) {
        let memory_limit = self.config.memory_usage_limit.unwrap().0;
        let high_water = (self.config.memory_usage_high_water * memory_limit as f64) as u64;
        register_memory_usage_high_water(high_water);

        self.check_conflict_addr();
        self.init_fs();
        self.init_yatp();
        self.init_encryption();
        self.init_engines();
        let server_config = self.init_servers::<ApiV1>();
        self.register_services();
        let fetcher = self.init_io_utility();
        self.init_metrics_flusher(fetcher);
        self.run_server(server_config);
        self.run_status_server();
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
        std::fs::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in fs::read_dir(&search_base).unwrap().flatten() {
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
    }

    fn init_yatp(&self) {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            yatp::metrics::set_namespace(Some("tikv"));
            prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone()))
                .unwrap();
            prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone()))
                .unwrap();
        })
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

    fn init_engines(&mut self) {
        info!("init engines");
        let store_meta = StoreMeta::new(
            PENDING_MSG_CAP,
            self.coprocessor_host.as_ref().unwrap().clone(),
        );
        let engine = RaftKv::new(
            ServerRaftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(
                    self.raw_engines.kv.clone(),
                    store_meta.readers.clone(),
                    self.router.clone(),
                ),
            ),
            self.raw_engines.kv.clone(),
        );
        let store_meta = Some(store_meta);
        self.engines = Some(TiKVEngines { store_meta, engine });
    }

    fn init_servers<F: KvFormat>(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        info!("init servers");

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        let lock_mgr = LockManager::new(&self.config.pessimistic_txn);
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let engines = self.engines.as_mut().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();
        let flow_reporter = rfstore::store::worker::FlowStatsReporter::new(pd_sender.clone());

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                flow_reporter.clone(),
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
                .on_thread_start(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .on_thread_stop(tikv_alloc::remove_thread_memory_accessor)
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
                collector_reg_handle,
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle,
        );
        self.to_stop.push(single_target_worker);

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
                flow_reporter.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };
        let reporter = rfstore::store::FlowStatsReporter::new(pd_sender);
        let storage = create_raft_storage::<_, F>(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            Arc::new(FlowController::empty()),
            reporter,
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                flow_reporter,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate(self.config.coprocessor.region_split_size)
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(rfstore::store::Config::from_old(
            &self.config.raft_store,
            &self.config.coprocessor,
        )));
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store,
            self.pd_client.clone(),
            self.background_worker.clone(),
        );
        info!("bootstrap store");
        node.try_bootstrap_store(self.raw_engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));
        info!("store bootstrapped");

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
                Arc::new(QuotaLimiter::default()),
            ),
            coprocessor_v2::Endpoint::new(&self.config.coprocessor_v2),
            self.router.clone(),
            self.resolver.clone(),
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

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

        // `ConsistencyCheckObserver` must be registered before `Node::start`.
        let safe_point = Arc::new(AtomicU64::new(0));
        let observer = match self.config.coprocessor.consistency_check_method {
            ConsistencyCheckMethod::Mvcc => {
                BoxConsistencyCheckObserver::new(MvccConsistencyCheckObserver::new(safe_point))
            }
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
            self.raw_engines.clone(),
            Box::new(server.transport()),
            pd_worker,
            engines.store_meta.take().unwrap(),
            self.coprocessor_host.clone().unwrap(),
            importer.clone(),
            self.concurrency_manager.clone(),
        )
        .unwrap_or_else(|e| panic!("failed to start node: {:?}", e));

        initial_metric(&self.config.metric);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
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
            engines.engine.kv_engine(),
            servers.importer.clone(),
        );
        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
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
            engines.engine.kv_engine(),
            self.config.backup.clone(),
            self.concurrency_manager.clone(),
            self.config.storage.api_version(),
        );
        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start(backup_endpoint);
    }

    fn init_io_utility(&mut self) -> BytesFetcher {
        let stats_collector_enabled = file_system::init_io_stats_collector()
            .map_err(|e| warn!("failed to init I/O stats collector: {}", e))
            .is_ok();
        let fetcher = if stats_collector_enabled {
            BytesFetcher::FromIOStatsCollector()
        } else {
            BytesFetcher::FromRateLimiter(self.io_rate_limiter.statistics().unwrap())
        };
        fetcher
    }

    fn init_metrics_flusher(&mut self, fetcher: BytesFetcher) {
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let kv = self.raw_engines.kv.clone();
        let raft = self.raw_engines.raft.clone();
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                KvEngine::flush_metrics(&kv, "kv");
                RaftEngine::flush_metrics(&raft, "raft");
                io_metrics.flush(now);
            });
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
                self.raw_engines.kv.clone(),
                self.raw_engines.raft.clone(),
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

    pub fn stop(self) {
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.region_info_accessor.stop();

        servers.lock_mgr.stop();

        self.to_stop.into_iter().for_each(|s| s.stop());
    }

    pub fn get_kv_engine(&self) -> kvengine::Engine {
        self.raw_engines.kv.clone()
    }

    pub fn get_raft_engine(&self) -> rfengine::RfEngine {
        self.raw_engines.raft.clone()
    }

    pub fn get_store_id(&self) -> u64 {
        self.servers.as_ref().unwrap().node.id()
    }

    pub fn get_sst_importer(&self) -> Arc<SstImporter> {
        self.servers.as_ref().unwrap().importer.clone()
    }

    pub fn get_raft_router(&self) -> RaftRouter {
        self.router.clone()
    }
}

impl TiKVServer {
    fn init_raw_engines(
        pd: Arc<dyn pd_client::PdClient>,
        conf: &TiKvConfig,
        dfs: Arc<dyn DFS>,
        rate_limiter: Arc<IORateLimiter>,
    ) -> Engines {
        // Create raft engine.
        let raft_db_path = Path::new(&conf.raft_store.raftdb_path);
        let kv_engine_path = PathBuf::from(&conf.storage.data_dir).join(Path::new("db"));
        let wal_size = conf.raft_engine.config().target_file_size.0 as usize;
        let rf_engine = RfEngine::open(raft_db_path, wal_size).unwrap();
        let mut kv_opts = kvengine::Options::default();
        let capacity = match conf.storage.block_cache.capacity {
            None => {
                let total_mem = SysQuota::memory_limit_in_bytes();
                ((total_mem as f64) * tikv::config::BLOCK_CACHE_RATE) as usize
            }
            Some(c) => c.0 as usize,
        };
        kv_opts.local_dir = kv_engine_path;
        kv_opts.num_compactors = conf.rocksdb.max_background_jobs as usize;
        kv_opts.max_mem_table_size = conf.rocksdb.writecf.write_buffer_size.0;
        kv_opts.base_size = conf.coprocessor.region_split_size.0 / 16;
        kv_opts.max_block_cache_size = capacity as i64;
        kv_opts.remote_compactor_addr = conf.dfs.remote_compactor_addr.clone();
        let cf_opt = &conf.rocksdb.writecf;
        kv_opts.table_builder_options.block_size = cf_opt.block_size.0 as usize;
        kv_opts.table_builder_options.max_table_size = cf_opt.target_file_size_base.0 as usize;
        let opts = Arc::new(kv_opts);
        let recoverer = rfstore::store::RecoverHandler::new(rf_engine.clone());
        let meta_iter = recoverer.clone();
        let id_allocator = Arc::new(PdIDAllocator { pd });
        let (sender, receiver) = tikv_util::mpsc::unbounded();
        let meta_change_listener = Box::new(MetaChangeListener {
            sender: sender.clone(),
        });
        let kv_engine = kvengine::Engine::open(
            dfs,
            opts,
            meta_iter,
            recoverer,
            id_allocator,
            meta_change_listener,
            rate_limiter,
        )
        .unwrap();
        Engines::new(kv_engine, rf_engine, (sender, receiver))
    }
}

struct PdIDAllocator {
    pd: Arc<dyn pd_client::PdClient>,
}

const ALLOCATE_ID_TIMEOUT: Duration = Duration::from_secs(10 * 60);

impl kvengine::IDAllocator for PdIDAllocator {
    fn alloc_id(&self, count: usize) -> Vec<u64> {
        let start = Instant::now();
        loop {
            match block_on(self.pd.batch_get_tso(count as u32)) {
                Ok(ts) => {
                    let last = ts.into_inner();
                    let first = last - count as u64 + 1;
                    return (first..=last).collect();
                }
                Err(err) => {
                    error!("failed to allocate file id from PD {:?}", err);
                    std::thread::sleep(Duration::from_secs(1));
                    if start.saturating_elapsed() > ALLOCATE_ID_TIMEOUT {
                        panic!("allocate file id timeout");
                    }
                }
            }
        }
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

impl Stop for StatusServer {
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
