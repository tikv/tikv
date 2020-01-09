//! Conveniences for creating a TiKV server

use super::setup::*;
use super::signal_handler;
use engine::rocks;
use engine::rocks::util::metrics_flusher::{MetricsFlusher, DEFAULT_FLUSHER_INTERVAL};
use engine::rocks::util::security::encrypted_env_from_cipher_file;
use engine::Engines;
use fs2::FileExt;
use futures_cpupool::Builder;
use kvproto::backup::create_backup;
use kvproto::deadlock::create_deadlock;
use kvproto::debugpb::create_debug;
use kvproto::diagnosticspb::create_diagnostics;
use kvproto::import_sstpb::create_import_sst;
use pd_client::{PdClient, RpcClient};
<<<<<<< HEAD
use std::convert::TryFrom;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tikv::config::TiKvConfig;
use tikv::coprocessor;
use tikv::import::{ImportSSTService, SSTImporter};
use tikv::raftstore::coprocessor::{CoprocessorHost, RegionInfoAccessor};
use tikv::raftstore::store::fsm::store::{StoreMeta, PENDING_VOTES_CAP};
use tikv::raftstore::store::{fsm, LocalReader};
use tikv::raftstore::store::{new_compaction_listener, SnapManagerBuilder};
use tikv::server::gc_worker::{AutoGCConfig, GCWorker};
use tikv::server::lock_manager::LockManager;
use tikv::server::resolve;
use tikv::server::service::{DebugService, DiagnosticsService};
use tikv::server::status_server::StatusServer;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::DEFAULT_CLUSTER_ID;
use tikv::server::{create_raft_storage, Node, RaftKv, Server};
use tikv::storage::{self, DEFAULT_ROCKSDB_SUB_DIR};
use tikv_util::check_environment_variables;
use tikv_util::security::SecurityManager;
use tikv_util::time::Monitor;
use tikv_util::worker::FutureWorker;
=======
use std::{
    convert::TryFrom,
    fmt,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use tikv::config::ConfigHandler;
use tikv::raftstore::router::ServerRaftStoreRouter;
use tikv::{
    config::{ConfigController, TiKvConfig},
    coprocessor,
    import::{ImportSSTService, SSTImporter},
    raftstore::{
        coprocessor::{CoprocessorHost, RegionInfoAccessor},
        store::{
            fsm,
            fsm::store::{RaftBatchSystem, RaftRouter, StoreMeta, PENDING_VOTES_CAP},
            new_compaction_listener, LocalReader, SnapManagerBuilder,
        },
    },
    read_pool::{ReadPool, ReadPoolRunner},
    server::{
        config::Config as ServerConfig,
        create_raft_storage,
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        Node, RaftKv, Server, DEFAULT_CLUSTER_ID,
    },
    storage,
};
use tikv_util::{
    check_environment_variables,
    security::SecurityManager,
    time::Monitor,
    worker::{FutureWorker, Worker},
};
use yatp::{
    pool::CloneRunnerBuilder,
    queue::{multilevel, QueueType},
};

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TiKvConfig) {
    // Do some prepare works before start.
    pre_start(&config);

    let mut tikv = TiKVServer::init(config);

    let _m = Monitor::default();

    tikv.init_fs();
    tikv.init_engines();
    let gc_worker = tikv.init_gc_worker();
    let server_config = tikv.init_servers(&gc_worker);
    tikv.register_services(gc_worker);
    tikv.init_metrics_flusher();

    tikv.run_server(server_config);

    signal_handler::wait_for_signal(Some(tikv.engines.take().unwrap().engines));

    tikv.stop();
}
>>>>>>> ea5d5cc0... *: allow coprocessor to use yatp (#6375)

const RESERVED_OPEN_FDS: u64 = 1000;

pub fn run_tikv(mut config: TiKvConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);
    tikv_util::set_panic_hook(false, &config.storage.data_dir);

    // Print version information.
    tikv::log_tikv_info();
    info!(
        "using config";
        "config" => serde_json::to_string(&config).unwrap(),
    );

    config.write_into_metrics();
    // Do some prepare works before start.
    pre_start(&config);

    let security_mgr = Arc::new(
        SecurityManager::new(&config.security)
            .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e.description())),
    );
    let pd_client = RpcClient::new(&config.pd, Arc::clone(&security_mgr))
        .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e));
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

    let _m = Monitor::default();
    run_raft_server(pd_client, &config, security_mgr);
}

fn run_raft_server(pd_client: RpcClient, cfg: &TiKvConfig, security_mgr: Arc<SecurityManager>) {
    let store_path = Path::new(&cfg.storage.data_dir);
    let lock_path = store_path.join(Path::new("LOCK"));
    let db_path = store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
    let snap_path = store_path.join(Path::new("snap"));
    let raft_db_path = Path::new(&cfg.raft_store.raftdb_path);
    let import_path = store_path.join("import");

    let f = File::create(lock_path.as_path())
        .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
    if f.try_lock_exclusive().is_err() {
        fatal!(
            "lock {} failed, maybe another instance is using this directory.",
            store_path.display()
        );
    }

    if tikv_util::panic_mark_file_exists(&cfg.storage.data_dir) {
        fatal!(
            "panic_mark_file {} exists, there must be something wrong with the db.",
            tikv_util::panic_mark_file_path(&cfg.storage.data_dir).display()
        );
    }

    // Initialize raftstore channels.
    let (router, system) = fsm::create_raft_batch_system(&cfg.raft_store);

    let compaction_listener = new_compaction_listener(router.clone());

    // Create pd client and pd worker
    let pd_client = Arc::new(pd_client);
    let pd_worker = FutureWorker::new("pd-worker");
    let (mut worker, resolver) = resolve::new_resolver(Arc::clone(&pd_client))
        .unwrap_or_else(|e| fatal!("failed to start address resolver: {}", e));
    let pd_sender = pd_worker.scheduler();

    // Create encrypted env from cipher file
    let encrypted_env = if !cfg.security.cipher_file.is_empty() {
        match encrypted_env_from_cipher_file(&cfg.security.cipher_file, None) {
            Err(e) => fatal!(
                "failed to create encrypted env from cipher file, err {:?}",
                e
            ),
            Ok(env) => Some(env),
        }
    } else {
        None
    };

    // Create block cache.
    let cache = cfg.storage.block_cache.build_shared_cache();

    // Create raft engine.
    let mut raft_db_opts = cfg.raftdb.build_opt();
    if let Some(ref ec) = encrypted_env {
        raft_db_opts.set_env(ec.clone());
    }
    let raft_db_cf_opts = cfg.raftdb.build_cf_opts(&cache);
    let raft_engine = rocks::util::new_engine_opt(
        raft_db_path.to_str().unwrap(),
        raft_db_opts,
        raft_db_cf_opts,
    )
    .unwrap_or_else(|s| fatal!("failed to create raft engine: {}", s));

    // Create kv engine, storage.
    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.add_event_listener(compaction_listener);
    if let Some(ec) = encrypted_env {
        kv_db_opts.set_env(ec);
    }

<<<<<<< HEAD
    // Before create kv engine we need to check whether it needs to upgrade from v2.x to v3.x.
    // if let Err(e) = tikv::raftstore::store::maybe_upgrade_from_2_to_3(
    //     &raft_engine,
    //     db_path.to_str().unwrap(),
    //     kv_db_opts.clone(),
    //     &cfg.rocksdb,
    //     &cache,
    // ) {
    //     fatal!("failed to upgrade from v2.x to v3.x: {:?}", e);
    // };

    // Create kv engine, storage.
    let kv_cfs_opts = cfg.rocksdb.build_cf_opts(&cache);
    let kv_engine = rocks::util::new_engine_opt(db_path.to_str().unwrap(), kv_db_opts, kv_cfs_opts)
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

    let engines = Engines::new(Arc::new(kv_engine), Arc::new(raft_engine), cache.is_some());
    let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_VOTES_CAP)));
    let local_reader = LocalReader::new(engines.kv.clone(), store_meta.clone(), router.clone());
    let raft_router = ServerRaftStoreRouter::new(router.clone(), local_reader);

    let engine = RaftKv::new(raft_router.clone());

    let storage_read_pool = storage::readpool_impl::build_read_pool(
        &cfg.readpool.storage,
        pd_sender.clone(),
        engine.clone(),
    );

    let mut lock_mgr = if cfg.pessimistic_txn.enabled {
        Some(LockManager::new())
    } else {
        None
    };

    let mut gc_worker = GCWorker::new(
        engine.clone(),
        Some(engines.kv.clone()),
        Some(raft_router.clone()),
        cfg.gc.clone(),
    );
    gc_worker
        .start()
        .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));

    let storage = create_raft_storage(
        engine.clone(),
        &cfg.storage,
        storage_read_pool,
        lock_mgr.clone(),
    )
    .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

    let bps = i64::try_from(cfg.server.snap_max_write_bytes_per_sec.0)
        .unwrap_or_else(|_| fatal!("snap_max_write_bytes_per_sec > i64::max_value"));

    // Create snapshot manager, server.
    let snap_mgr = SnapManagerBuilder::default()
        .max_write_bytes_per_sec(bps)
        .max_total_size(cfg.server.snap_max_total_size.0)
        .build(
            snap_path.as_path().to_str().unwrap().to_owned(),
            Some(router.clone()),
        );
<<<<<<< HEAD
=======
=======
    fn init_servers(
        &mut self,
        gc_worker: &GcWorker<RaftKv<ServerRaftStoreRouter>>,
    ) -> Arc<ServerConfig> {
        let mut cfg_controller = self.cfg_controller.take().unwrap();
        cfg_controller.register("gc", Box::new(gc_worker.get_config_manager()));

        // Create CoprocessorHost.
        let mut coprocessor_host = self.coprocessor_host.take().unwrap();

        let lock_mgr = if self.config.pessimistic_txn.enabled {
            let lock_mgr = LockManager::new();
            cfg_controller.register("pessimistic_txn", Box::new(lock_mgr.config_manager()));
            lock_mgr.register_detector_role_change_observer(&mut coprocessor_host);
            Some(lock_mgr)
        } else {
            None
        };

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = FutureWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

        let unified_read_pool = if self.config.readpool.unify_read_pool {
            let unified_read_pool_cfg = &self.config.readpool.unified;
            let mut builder = yatp::Builder::new("unified-read-pool");
            builder
                .min_thread_count(unified_read_pool_cfg.min_thread_count)
                .max_thread_count(unified_read_pool_cfg.max_thread_count);
            let multilevel_builder = multilevel::Builder::new(Default::default());
            let read_pool_runner = ReadPoolRunner::new(engines.engine.clone(), Default::default());
            let runner_builder =
                multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
            Some(builder.build_with_queue_and_runner(
                QueueType::Multilevel(multilevel_builder),
                runner_builder,
            ))
        } else {
            None
        };

        let storage_read_pool = if self.config.readpool.unify_read_pool {
            ReadPool::from(unified_read_pool.as_ref().unwrap().remote().clone())
        } else {
            let cop_read_pools = storage::build_read_pool(
                &self.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            );
            ReadPool::from(cop_read_pools)
        };

>>>>>>> 15d7a7fc... *: allow replacing all the readpools with yatp
        let storage = create_raft_storage(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool,
            lock_mgr.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

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
            .build(snap_path, Some(self.router.clone()));

        // Create coprocessor endpoint.
        let cop_read_pool = if self.config.readpool.unify_read_pool {
            ReadPool::from(unified_read_pool.as_ref().unwrap().remote().clone())
        } else {
            let cop_read_pools = coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            );
            ReadPool::from(cop_read_pools)
        };

        let server_config = Arc::new(self.config.server.clone());

        // Create server
        let server = Server::new(
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(&server_config, cop_read_pool),
            engines.raft_router.clone(),
            self.resolver.clone(),
            snap_mgr.clone(),
            gc_worker.clone(),
            unified_read_pool,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

        let import_path = self.store_path.join("import");
        let importer = Arc::new(SSTImporter::new(import_path).unwrap());
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config,
            &self.config.raft_store,
            self.pd_client.clone(),
        );
        node.start(
            engines.engines.clone(),
            server.transport(),
            snap_mgr,
            pd_worker,
            engines.store_meta.clone(),
            coprocessor_host,
            importer.clone(),
            cfg_controller,
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        initial_metric(&self.config.metric, Some(node.id()));

        // Start auto gc
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone(),
            node.id(),
        );
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
        });
>>>>>>> ea5d5cc0... *: allow coprocessor to use yatp (#6375)

    let server_cfg = Arc::new(cfg.server.clone());

    // Create coprocessor endpoint.
    let cop_read_pool = coprocessor::readpool_impl::build_read_pool(
        &cfg.readpool.coprocessor,
        pd_sender.clone(),
        engine.clone(),
    );
    let cop = coprocessor::Endpoint::new(&server_cfg, cop_read_pool, storage.get_ts_cache());

    let importer = Arc::new(SSTImporter::new(import_path).unwrap());
    let import_service = ImportSSTService::new(
        cfg.import.clone(),
        raft_router.clone(),
        engines.kv.clone(),
        Arc::clone(&importer),
    );

    // The `DebugService` and `DiagnosticsService` will share the same thread pool
    let pool = Builder::new()
        .name_prefix(thd_name!("debugger"))
        .pool_size(1)
        .create();
    // Create Debug service.
    let debug_service = DebugService::new(
        engines.clone(),
        pool.clone(),
        raft_router.clone(),
        gc_worker.clone(),
    );

    // Create Diagnostics service
    let diag_service = DiagnosticsService::new(pool, cfg.log_file.clone());

    // Create Backup service.
    let mut backup_worker = tikv_util::worker::Worker::new("backup-endpoint");
    let backup_scheduler = backup_worker.scheduler();
    let backup_service = backup::Service::new(backup_scheduler);

    // Create server
    let mut server = Server::new(
        &server_cfg,
        &security_mgr,
        storage.clone(),
        cop,
        raft_router,
        resolver.clone(),
        snap_mgr.clone(),
        gc_worker.clone(),
    )
    .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

    // Register services.
    if server
        .register_service(create_import_sst(import_service))
        .is_some()
    {
        fatal!("failed to register import service");
    }
    if server
        .register_service(create_debug(debug_service))
        .is_some()
    {
        fatal!("failed to register debug service");
    }
    if server
        .register_service(create_diagnostics(diag_service))
        .is_some()
    {
        fatal!("failed to register diagnostics service");
    }
    if let Some(lm) = lock_mgr.as_ref() {
        if server
            .register_service(create_deadlock(lm.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }
    }
    if server
        .register_service(create_backup(backup_service))
        .is_some()
    {
        fatal!("failed to register backup service");
    }

    let trans = server.transport();

    // Create node.
    let mut node = Node::new(system, &server_cfg, &cfg.raft_store, pd_client.clone());

    // Create CoprocessorHost.
    let mut coprocessor_host = CoprocessorHost::new(cfg.coprocessor.clone(), router);

    // Create region collection.
    let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);
    region_info_accessor.start();

    // Register the role change observer of the lock manager.
    if let Some(lm) = lock_mgr.as_ref() {
        lm.register_detector_role_change_observer(&mut coprocessor_host);
    }

    node.start(
        engines.clone(),
        trans,
        snap_mgr,
        pd_worker,
        store_meta,
        coprocessor_host,
        importer,
    )
    .unwrap_or_else(|e| fatal!("failed to start node: {}", e));
    initial_metric(&cfg.metric, Some(node.id()));

    // Start backup endpoint.
    let backup_endpoint = backup::Endpoint::new(
        node.id(),
        engine.clone(),
        region_info_accessor.clone(),
        engines.kv.clone(),
    );
    let backup_timer = backup_endpoint.new_timer();
    backup_worker
        .start_with_timer(backup_endpoint, backup_timer)
        .unwrap_or_else(|e| fatal!("failed to start backup endpoint: {}", e));

    // Start auto gc
    let auto_gc_cfg = AutoGCConfig::new(
        Arc::clone(&pd_client),
        region_info_accessor.clone(),
        node.id(),
    );
    if let Err(e) = gc_worker.start_auto_gc(auto_gc_cfg) {
        fatal!("failed to start auto_gc on storage, error: {}", e);
    }

    let mut metrics_flusher = MetricsFlusher::new(
        engines.clone(),
        Duration::from_millis(DEFAULT_FLUSHER_INTERVAL),
    );

    // Start metrics flusher
    if let Err(e) = metrics_flusher.start() {
        error!(
            "failed to start metrics flusher";
            "err" => %e
        );
    }

    if let Some(lock_mgr) = lock_mgr.as_mut() {
        lock_mgr
            .start(
                node.id(),
                pd_client,
                resolver,
                Arc::clone(&security_mgr),
                &cfg.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));
    }

    // Run server.
    server
        .build_and_bind()
        .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
    server
        .start(server_cfg, security_mgr)
        .unwrap_or_else(|e| fatal!("failed to start server: {}", e));

    let server_cfg = cfg.server.clone();
    let mut status_enabled = cfg.metric.address.is_empty() && !server_cfg.status_addr.is_empty();

    // Create a status server.
    // TODO: How to keep cfg updated?
    let mut status_server = StatusServer::new(server_cfg.status_thread_pool_size, cfg.clone());
    if status_enabled {
        // Start the status server.
        if let Err(e) = status_server.start(server_cfg.status_addr) {
            error!(
                "failed to bind addr for status service";
                "err" => %e
            );
            status_enabled = false;
        }
    }

    signal_handler::handle_signal(Some(engines));

    // Stop backup worker.
    if let Some(j) = backup_worker.stop() {
        j.join()
            .unwrap_or_else(|e| fatal!("failed to stop backup: {:?}", e))
    }

    // Stop server.
    server
        .stop()
        .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

    if status_enabled {
        // Stop the status server.
        status_server.stop()
    }

    metrics_flusher.stop();

    node.stop();

    region_info_accessor.stop();

    if let Some(lm) = lock_mgr.as_mut() {
        lm.stop();
    }

    if let Some(Err(e)) = worker.stop().map(JoinHandle::join) {
        info!(
            "ignore failure when stopping resolver";
            "err" => ?e
        );
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged and fatal errors exit.
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
///
/// # Fatal errors
///
/// If the max open file descriptor limit is not high enough to support
/// the main database and the raft database.
fn pre_start(cfg: &TiKvConfig) {
    // Before any startup, check system configuration and environment variables.
    check_system_config(&cfg);
    check_environment_variables();

    if cfg.panic_when_unexpected_key_or_data {
        info!("panic-when-unexpected-key-or-data is on");
        tikv_util::set_panic_when_unexpected_key_or_data(true);
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

    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
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
