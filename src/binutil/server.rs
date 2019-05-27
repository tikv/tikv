//! Conveniences for creating a TiKV server

use super::setup::*;
use super::signal_handler;
use crate::binutil::setup::initial_logger;
use crate::config::{check_and_persist_critical_config, TiKvConfig};
use crate::coprocessor;
use crate::fatal;
use crate::import::{ImportSSTService, SSTImporter};
use crate::pd::{PdClient, RpcClient};
use crate::raftstore::coprocessor::{CoprocessorHost, RegionInfoAccessor};
use crate::raftstore::store::fsm::store::{StoreMeta, PENDING_VOTES_CAP};
use crate::raftstore::store::{fsm, LocalReader};
use crate::raftstore::store::{new_compaction_listener, SnapManagerBuilder};
use crate::server::resolve;
use crate::server::status_server::StatusServer;
use crate::server::transport::ServerRaftStoreRouter;
use crate::server::DEFAULT_CLUSTER_ID;
use crate::server::{create_raft_storage, Node, Server};
use crate::storage::lock_manager::{
    Detector, DetectorScheduler, Service as DeadlockService, WaiterManager, WaiterMgrScheduler,
};
use crate::storage::{self, AutoGCConfig, RaftKv, DEFAULT_ROCKSDB_SUB_DIR};
use engine::rocks;
use engine::rocks::util::metrics_flusher::{MetricsFlusher, DEFAULT_FLUSHER_INTERVAL};
use engine::rocks::util::security::encrypted_env_from_cipher_file;
use engine::Engines;
use fs2::FileExt;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tikv_util::check_environment_variables;
use tikv_util::security::SecurityManager;
use tikv_util::time::Monitor;
use tikv_util::worker::FutureWorker;

const RESERVED_OPEN_FDS: u64 = 1000;

pub fn run_tikv(mut config: TiKvConfig) {
    if let Err(e) = check_and_persist_critical_config(&config) {
        fatal!("critical config check failed: {}", e);
    }

    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);
    tikv_util::set_panic_hook(false, &config.storage.data_dir);

    // Print version information.
    super::log_tikv_info();

    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e.description());
    }
    debug!(
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
        &cfg.readpool.storage.build_config(),
        pd_sender.clone(),
        engine.clone(),
    );

    // Create waiter manager worker and deadlock detector worker if pessimistic-txn is enabled
    // Make clippy happy
    let (mut waiter_mgr_worker, mut detector_worker) = if cfg.pessimistic_txn.enabled {
        (
            Some(FutureWorker::new("waiter-manager")),
            Some(FutureWorker::new("deadlock-detector")),
        )
    } else {
        (None, None)
    };
    // Make clippy happy
    let deadlock_service = if cfg.pessimistic_txn.enabled {
        Some(DeadlockService::new(
            WaiterMgrScheduler::new(waiter_mgr_worker.as_ref().unwrap().scheduler()),
            DetectorScheduler::new(detector_worker.as_ref().unwrap().scheduler()),
        ))
    } else {
        None
    };

    let storage = create_raft_storage(
        engine.clone(),
        &cfg.storage,
        storage_read_pool,
        Some(engines.kv.clone()),
        Some(raft_router.clone()),
        waiter_mgr_worker
            .as_ref()
            .map(|worker| WaiterMgrScheduler::new(worker.scheduler())),
        detector_worker
            .as_ref()
            .map(|worker| DetectorScheduler::new(worker.scheduler())),
    )
    .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

    // Create snapshot manager, server.
    let snap_mgr = SnapManagerBuilder::default()
        .max_write_bytes_per_sec(cfg.server.snap_max_write_bytes_per_sec.0)
        .max_total_size(cfg.server.snap_max_total_size.0)
        .build(
            snap_path.as_path().to_str().unwrap().to_owned(),
            Some(router.clone()),
        );

    let importer = Arc::new(SSTImporter::new(import_path).unwrap());
    let import_service = ImportSSTService::new(
        cfg.import.clone(),
        raft_router.clone(),
        engines.kv.clone(),
        Arc::clone(&importer),
    );

    let server_cfg = Arc::new(cfg.server.clone());
    // Create server
    let cop_read_pool = coprocessor::readpool_impl::build_read_pool(
        &cfg.readpool.coprocessor.build_config(),
        pd_sender.clone(),
        engine.clone(),
    );
    let cop = coprocessor::Endpoint::new(&server_cfg, cop_read_pool);
    let mut server = Server::new(
        &server_cfg,
        &security_mgr,
        storage.clone(),
        cop,
        raft_router,
        resolver,
        snap_mgr.clone(),
        Some(engines.clone()),
        Some(import_service),
        deadlock_service,
    )
    .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
    let trans = server.transport();

    // Create node.
    let mut node = Node::new(system, &server_cfg, &cfg.raft_store, pd_client.clone());

    // Create CoprocessorHost.
    let mut coprocessor_host = CoprocessorHost::new(cfg.coprocessor.clone(), router);

    // Create region collection.
    let region_info_accessor = RegionInfoAccessor::new(&mut coprocessor_host);
    region_info_accessor.start();

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

    // Start auto gc
    let auto_gc_cfg = AutoGCConfig::new(
        Arc::clone(&pd_client),
        region_info_accessor.clone(),
        node.id(),
    );
    if let Err(e) = storage.start_auto_gc(auto_gc_cfg) {
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

    // Start waiter manager and deadlock detector
    if cfg.pessimistic_txn.enabled {
        let waiter_mgr_runner = WaiterManager::new(
            DetectorScheduler::new(detector_worker.as_ref().unwrap().scheduler()),
            cfg.pessimistic_txn.wait_for_lock_timeout,
            cfg.pessimistic_txn.wake_up_delay_duration,
        );
        let detector_runner = Detector::new(
            node.id(),
            WaiterMgrScheduler::new(waiter_mgr_worker.as_ref().unwrap().scheduler()),
            Arc::clone(&security_mgr),
            pd_client,
            cfg.pessimistic_txn.monitor_membership_interval,
        );
        waiter_mgr_worker
            .as_mut()
            .unwrap()
            .start(waiter_mgr_runner)
            .unwrap_or_else(|e| fatal!("failed to start waiter manager: {}", e));
        detector_worker
            .as_mut()
            .unwrap()
            .start(detector_runner)
            .unwrap_or_else(|e| fatal!("failed to start deadlock detector: {}", e));
    }

    // Run server.
    server
        .start(server_cfg, security_mgr)
        .unwrap_or_else(|e| fatal!("failed to start server: {}", e));

    let server_cfg = cfg.server.clone();
    let mut status_enabled = cfg.metric.address.is_empty() && !server_cfg.status_addr.is_empty();

    // Create a status server.
    let mut status_server = StatusServer::new(server_cfg.status_thread_pool_size);
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

    // Stop.
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

    if let Some(Err(e)) = worker.stop().map(JoinHandle::join) {
        info!(
            "ignore failure when stopping resolver";
            "err" => ?e
        );
    }

    if cfg.pessimistic_txn.enabled {
        if let Some(Err(e)) = waiter_mgr_worker.unwrap().stop().map(JoinHandle::join) {
            info!(
                "ignore failure when stopping waiter manager worker";
                "err" => ?e
            );
        }
        if let Some(Err(e)) = detector_worker.unwrap().stop().map(JoinHandle::join) {
            info!(
                "ignore failure when stopping deadlock detector worker";
                "err" => ?e
            );
        }
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
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (config.rocksdb.max_open_files + config.raftdb.max_open_files) as u64,
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
