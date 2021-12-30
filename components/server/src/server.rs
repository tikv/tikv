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

use std::thread;
use std::{
    cmp,
    convert::TryFrom,
    env, fmt,
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
    u64,
};

use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    encryption::get_env as get_encrypted_env, file_system::get_env as get_inspected_env,
    RocksEngine,
};
use engine_traits::{
    compaction_job::CompactionJobInfo, CFOptionsExt, ColumnFamilyOptions, Engines, MiscExt,
    RaftEngine, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use error_code::ErrorCodeExt;
use file_system::{
    set_io_rate_limiter, BytesFetcher, IOBudgetAdjustor, IORateLimiter,
    MetricsManager as IOMetricsManager,
};
use fs2::FileExt;
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use kvproto::{
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
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
        fsm::store::{RaftBatchSystem, RaftRouter, StoreMeta, PENDING_MSG_CAP},
        memory::MEMTRACE_ROOT,
        AutoSplitController, CheckLeaderRunner, GlobalReplicationState, LocalReader, SnapManager,
        SnapManagerBuilder, SplitCheckRunner, SplitConfigManager, StoreMsg,
    },
};
use security::SecurityManager;
use tikv::server::gc_worker::AutoGcConfig;
use tikv::{
    config::{ConfigController, DBConfigManger, DBType, TiKvConfig, DEFAULT_ROCKSDB_SUB_DIR},
    coprocessor, coprocessor_v2,
    import::{ImportSSTService, SSTImporter},
    read_pool::{build_yatp_read_pool, ReadPool},
    server::raftkv::ReplicaReadLockChecker,
    server::{
        config::Config as ServerConfig,
        config::ServerConfigManager,
        create_raft_storage,
        gc_worker::GcWorker,
        lock_manager::HackedLockManager as LockManager,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        ttl::TTLChecker,
        Node, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID, GRPC_THREAD_PREFIX,
    },
    storage::{self, config::StorageConfigManger, mvcc::MvccConsistencyCheckObserver, Engine},
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, VersionTrack},
    math::MovingAvgU32,
    sys::{disk, register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::Monitor,
    worker::{Builder as WorkerBuilder, FutureWorker, LazyWorker, Worker},
};
use tokio::runtime::Builder;

use crate::raft_engine_switch::{check_and_dump_raft_db, check_and_dump_raft_engine};
use crate::util::ffi_server_info;
use crate::{memory::*, setup::*};
use raftstore::engine_store_ffi::{
    EngineStoreServerHelper, EngineStoreServerStatus, RaftProxyStatus, RaftStoreProxy,
    RaftStoreProxyFFIHelper, ReadIndexClient,
};
use std::sync::atomic::{AtomicBool, AtomicU8};

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub unsafe fn run_tikv(config: TiKvConfig, engine_store_server_helper: &EngineStoreServerHelper) {
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

    macro_rules! run_impl {
        ($ER: ty) => {{
            let mut tikv = TiKVServer::<$ER>::init(config);

            // Must be called after `TiKVServer::init`.
            let memory_limit = tikv.config.memory_usage_limit.0.unwrap().0;
            let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
            register_memory_usage_high_water(high_water);

            tikv.check_conflict_addr();
            tikv.init_fs();
            tikv.init_yatp();
            tikv.init_encryption();
            let mut proxy = RaftStoreProxy {
                status: AtomicU8::new(RaftProxyStatus::Idle as u8),
                key_manager: tikv.encryption_key_manager.clone(),
                read_index_client: Box::new(ReadIndexClient::new(
                    tikv.router.clone(),
                    SysQuota::cpu_cores_quota() as usize * 2,
                )),
            };

            let proxy_helper = {
                let mut proxy_helper = RaftStoreProxyFFIHelper::new(&proxy);
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
            let (limiter, fetcher) = tikv.init_io_utility();
            let (engines, engines_info) = tikv.init_raw_engines(Some(limiter.clone()));
            limiter.set_low_priority_io_adjustor_if_needed(Some(engines_info.clone()));
            tikv.init_engines(engines.clone());
            let server_config = tikv.init_servers();
            tikv.register_services();
            tikv.init_metrics_flusher(fetcher, engines_info);
            tikv.init_storage_stats_task(engines);
            tikv.run_server(server_config);
            tikv.run_status_server();

            proxy.set_status(RaftProxyStatus::Running);

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
        }};
    }

    if !config.raft_engine.enable {
        run_impl!(RocksEngine)
    } else {
        run_impl!(RaftLogEngine)
    }
}

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);

/// A complete TiKV server.
struct TiKVServer<ER: RaftEngine> {
    config: TiKvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<RocksEngine, ER>,
    system: Option<RaftBatchSystem<RocksEngine, ER>>,
    resolver: resolve::PdStoreAddrResolver,
    state: Arc<Mutex<GlobalReplicationState>>,
    store_path: PathBuf,
    snap_mgr: Option<SnapManager>, // Will be filled in `init_servers`.
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    engines: Option<TiKVEngines<ER>>,
    servers: Option<Servers<ER>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<RocksEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
}

struct TiKVEngines<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<RocksEngine, ServerRaftStoreRouter<RocksEngine, ER>>,
}

struct Servers<ER: RaftEngine> {
    lock_mgr: LockManager,
    server: Server<RaftRouter<RocksEngine, ER>, resolve::PdStoreAddrResolver>,
    node: Node<RpcClient, ER>,
    importer: Arc<SSTImporter>,
}

impl<ER: RaftEngine> TiKVServer<ER> {
    fn init(mut config: TiKvConfig) -> TiKVServer<ER> {
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

        TiKVServer {
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

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have enough space
        // to do compaction and region migration when TiKV recover. This file is created in
        // data_dir rather than db_path, because we must not increase store size of db_path.
        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        //TODO after disk full readonly impl, such file should be removed.
        file_system::reserve_space_for_recover(
            &self.config.storage.data_dir,
            cmp::min(
                tikv_util::config::ReadableSize::gb(2).0,
                if self.config.storage.reserve_space.0 == 0 {
                    0
                } else {
                    // Max one of configured `reserve_space` and `storage.capacity * 5%`.
                    cmp::max(
                        (capacity as f64 * 0.05) as u64,
                        self.config.storage.reserve_space.0,
                    )
                },
            ),
        )
        .unwrap();
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

    fn create_raftstore_compaction_listener(&self) -> engine_rocks::CompactionListener {
        fn size_change_filter(info: &engine_rocks::RocksCompactionJobInfo) -> bool {
            // When calculating region size, we only consider write and default
            // column families.
            let cf = info.cf_name();
            if cf != CF_WRITE && cf != CF_DEFAULT {
                return false;
            }
            // Compactions in level 0 and level 1 are very frequently.
            if info.output_level() < 2 {
                return false;
            }

            true
        }

        let ch = Mutex::new(self.router.clone());
        let compacted_handler =
            Box::new(move |compacted_event: engine_rocks::RocksCompactedEvent| {
                let ch = ch.lock().unwrap();
                let event = StoreMsg::CompactedEvent(compacted_event);
                if let Err(e) = ch.send_control(event) {
                    error_unknown!(?e; "send compaction finished event to raftstore failed");
                }
            });
        engine_rocks::CompactionListener::new(compacted_handler, Some(size_change_filter))
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

        self.engines = Some(TiKVEngines {
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
            self.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
        );
        gc_worker
            .start()
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let gc_worker = self.init_gc_worker();
        let mut ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
        let ttl_scheduler = ttl_checker.scheduler();

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.config.storage.block_cache.shared,
                ttl_scheduler,
            )),
        );

        let lock_mgr = LockManager::new();

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = FutureWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

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
                .on_thread_start(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .on_thread_stop(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
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

        let storage = create_raft_storage(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            Arc::from(AtomicBool::new(self.config.pessimistic_txn.pipelined)),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

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

        let check_leader_runner = CheckLeaderRunner::new(engines.store_meta.clone());
        let check_leader_scheduler = self
            .background_worker
            .start("check-leader", check_leader_runner);

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate()
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.config.raft_store.clone()));
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store.clone(),
            self.pd_client.clone(),
            self.state.clone(),
            self.background_worker.clone(),
        );
        node.try_bootstrap_store(engines.engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        {
            raftstore::engine_store_ffi::gen_engine_store_server_helper(
                self.config.raft_store.engine_store_server_helper,
            )
            .set_store(node.store());
            info!("set store {} to engine-store", node.id());
        }

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
                engine_rocks::raw_util::to_raw_perf_level(self.config.coprocessor.perf_level),
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
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
            )),
        );

        let import_path = self.store_path.join("import");
        let importer = Arc::new(
            SSTImporter::new(
                &self.config.import,
                import_path,
                self.encryption_key_manager.clone(),
            )
            .unwrap(),
        );

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

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager(raft_store)),
        );

        let split_config_manager =
            SplitConfigManager(Arc::new(VersionTrack::new(self.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(split_config_manager);

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
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        initial_metric(&self.config.metric);
        if self.config.storage.enable_ttl {
            ttl_checker.start_with_timer(TTLChecker::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.region_info_accessor.clone(),
                self.config.storage.ttl_check_poll_interval.into(),
            ));
            self.to_stop.push(ttl_checker);
        }

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
        let import_service = ImportSSTService::new(
            self.config.import.clone(),
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
            self.config.log_file.clone(),
            self.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }
    }

    fn init_io_utility(&mut self) -> (Arc<IORateLimiter>, BytesFetcher) {
        let io_snooper_on = self.config.enable_io_snoop
            && file_system::init_io_snooper()
                .map_err(|e| error_unknown!(%e; "failed to init io snooper"))
                .is_ok();
        let limiter = Arc::new(
            self.config
                .storage
                .io_rate_limit
                .build(!io_snooper_on /*enable_statistics*/),
        );
        let fetcher = if io_snooper_on {
            BytesFetcher::FromIOSnooper()
        } else {
            BytesFetcher::FromRateLimiter(limiter.statistics().unwrap())
        };
        // Set up IO limiter even when rate limit is disabled, so that rate limits can be
        // dynamically applied later on.
        set_io_rate_limiter(Some(limiter.clone()));
        (limiter, fetcher)
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics =
            EngineMetricsManager::new(self.engines.as_ref().unwrap().engines.clone());
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let mut mem_trace_metrics = MemoryTraceManager::default();
        mem_trace_metrics.register_provider((&*MEMTRACE_ROOT).to_owned());
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info.update(now);
                mem_trace_metrics.flush(now);
            });
    }

    fn init_storage_stats_task(&self, engines: Engines<RocksEngine, ER>) {
        let config_disk_capacity: u64 = self.config.raft_store.capacity.0;
        let store_path = self.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let disk_reserved = self.config.storage.reserve_space.0;
        if disk_reserved == 0 {
            info!("disk space checker not enabled");
            return;
        }
        //TODO wal size ignore?
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

                let used_size = snap_size + kv_size + raft_size;
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };

                let mut available = capacity.checked_sub(used_size).unwrap_or_default();
                available = cmp::min(available, disk_stats.available_space());
                if available <= disk_reserved {
                    warn!(
                        "disk full, available={},snap={},engine={},capacity={}",
                        available, snap_size, kv_size, capacity
                    );
                    disk::set_disk_full();
                } else if disk::is_disk_full() {
                    info!(
                        "disk normalized, available={},snap={},engine={},capacity={}",
                        available, snap_size, kv_size, capacity
                    );
                    disk::clear_disk_full();
                }
            })
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
                raftstore::engine_store_ffi::gen_engine_store_server_helper(
                    self.config.raft_store.engine_store_server_helper,
                ),
                self.config.server.status_thread_pool_size,
                Some(self.pd_client.clone()),
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(
                self.config.server.status_addr.clone(),
                self.config.server.advertise_status_addr.clone(),
            ) {
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

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

impl TiKVServer<RocksEngine> {
    fn init_raw_engines(
        &mut self,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> (Engines<RocksEngine, RocksEngine>, Arc<EnginesResourceInfo>) {
        let env =
            get_encrypted_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let env = get_inspected_env(Some(env), limiter).unwrap();
        let block_cache = self.config.storage.block_cache.build_shared_cache();

        // Create raft engine.
        let raft_db_path = Path::new(&self.config.raft_store.raftdb_path);
        let config_raftdb = &self.config.raftdb;
        let mut raft_db_opts = config_raftdb.build_opt();
        raft_db_opts.set_env(env.clone());
        let raft_db_cf_opts = config_raftdb.build_cf_opts(&block_cache);
        let raft_engine = engine_rocks::raw_util::new_engine_opt(
            raft_db_path.to_str().unwrap(),
            raft_db_opts,
            raft_db_cf_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create raft engine: {}", s));

        // Create kv engine.
        let mut kv_db_opts = self.config.rocksdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_raftstore_compaction_listener());
        let kv_cfs_opts = self.config.rocksdb.build_cf_opts(
            &block_cache,
            Some(&self.region_info_accessor),
            self.config.storage.enable_ttl,
        );
        let db_path = self.store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
        let kv_engine = engine_rocks::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
        let mut raft_engine = RocksEngine::from_db(Arc::new(raft_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        raft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);

        check_and_dump_raft_engine(&self.config, &engines.raft, 8);

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.storage.block_cache.shared,
            )),
        );
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DBConfigManger::new(
                engines.raft.clone(),
                DBType::Raft,
                self.config.storage.block_cache.shared,
            )),
        );

        let engines_info = Arc::new(EnginesResourceInfo::new(
            engines.kv.clone(),
            Some(engines.raft.clone()),
            180, /*max_samples_to_preserve*/
        ));

        (engines, engines_info)
    }
}

impl TiKVServer<RaftLogEngine> {
    fn init_raw_engines(
        &mut self,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> (
        Engines<RocksEngine, RaftLogEngine>,
        Arc<EnginesResourceInfo>,
    ) {
        let env =
            get_encrypted_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let env = get_inspected_env(Some(env), limiter).unwrap();
        let block_cache = self.config.storage.block_cache.build_shared_cache();

        // Create raft engine.
        let raft_config = self.config.raft_engine.config();
        let raft_engine = RaftLogEngine::new(raft_config);

        // Try to dump and recover raft data.
        check_and_dump_raft_db(&self.config, &raft_engine, &env, 8);

        // Create kv engine.
        let mut kv_db_opts = self.config.rocksdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_raftstore_compaction_listener());
        let kv_cfs_opts = self.config.rocksdb.build_cf_opts(
            &block_cache,
            Some(&self.region_info_accessor),
            self.config.storage.enable_ttl,
        );
        let db_path = self.store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
        let kv_engine = engine_rocks::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
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

        let engines_info = Arc::new(EnginesResourceInfo::new(
            engines.kv.clone(),
            None, /*raft_engine*/
            180,  /*max_samples_to_preserve*/
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

pub struct EngineMetricsManager<R: RaftEngine> {
    engines: Engines<RocksEngine, R>,
    last_reset: Instant,
}

impl<R: RaftEngine> EngineMetricsManager<R> {
    pub fn new(engines: Engines<RocksEngine, R>) -> Self {
        EngineMetricsManager {
            engines,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        self.engines.kv.flush_metrics("kv");
        self.engines.raft.flush_metrics("raft");
        if now.duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            self.engines.kv.reset_statistics();
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

    pub fn new(
        kv_engine: RocksEngine,
        raft_engine: Option<RocksEngine>,
        max_samples_to_preserve: usize,
    ) -> Self {
        EnginesResourceInfo {
            kv_engine,
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(&self, _now: Instant) {
        let mut normalized_pending_bytes = 0;

        fn fetch_engine_cf(engine: &RocksEngine, cf: &str, normalized_pending_bytes: &mut u32) {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                if let Ok(Some(b)) = engine.get_cf_compaction_pending_bytes(cf) {
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
