// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(plugin)]
#![feature(slice_patterns)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]
// TODO: remove this once rust-lang/rust#43268 is resolved.
#![allow(logic_bug)]

#[macro_use]
extern crate clap;
extern crate fs2;
#[cfg(feature = "mem-profiling")]
extern crate jemallocator;
extern crate libc;
#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
extern crate slog;
#[cfg(unix)]
extern crate nix;
extern crate prometheus;
extern crate rocksdb;
extern crate serde_json;
#[cfg(unix)]
extern crate signal;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;
extern crate tikv;
extern crate toml;

#[cfg(unix)]
#[macro_use]
mod util;
use util::setup::*;
use util::signal_handler;

use std::env;
use std::fs::File;
use std::path::Path;
use std::process;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use std::usize;

use clap::{App, Arg};
use fs2::FileExt;

use tikv::config::{check_and_persist_critical_config, TiKvConfig};
use tikv::coprocessor;
use tikv::import::{ImportSSTService, SSTImporter};
use tikv::pd::{PdClient, RpcClient};
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv::raftstore::store::{self, new_compaction_listener, Engines, SnapManagerBuilder};
use tikv::server::readpool::ReadPool;
use tikv::server::resolve;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::{create_raft_storage, Node, Server, DEFAULT_CLUSTER_ID};
use tikv::storage::{self, DEFAULT_ROCKSDB_SUB_DIR};
use tikv::util::rocksdb::metrics_flusher::{MetricsFlusher, DEFAULT_FLUSHER_INTERVAL};
use tikv::util::security::SecurityManager;
use tikv::util::time::Monitor;
use tikv::util::transport::SendCh;
use tikv::util::worker::FutureWorker;
use tikv::util::{self as tikv_util, panic_hook, rocksdb as rocksdb_util};

const RESERVED_OPEN_FDS: u64 = 1000;

fn check_system_config(config: &TiKvConfig) {
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (config.rocksdb.max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{:?}", e);
    }

    for e in tikv_util::config::check_kernel() {
        warn!("{:?}", e);
    }

    if cfg!(unix) && env::var("TZ").is_err() {
        env::set_var("TZ", ":/etc/localtime");
        warn!("environment variable `TZ` is missing, using `/etc/localtime`");
    }

    // check rocksdb data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!("{:?}", e);
    }
    // check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!("{:?}", e);
    }
}

fn run_raft_server(pd_client: RpcClient, cfg: &TiKvConfig, security_mgr: Arc<SecurityManager>) {
    let store_path = Path::new(&cfg.storage.data_dir);
    let lock_path = store_path.join(Path::new("LOCK"));
    let db_path = store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
    let snap_path = store_path.join(Path::new("snap"));
    let raft_db_path = Path::new(&cfg.raft_store.raftdb_path);
    let import_path = store_path.join("import");

    let f = File::create(lock_path.as_path())
        .unwrap_or_else(|e| fatal!("failed to create lock at {}: {:?}", lock_path.display(), e));
    if f.try_lock_exclusive().is_err() {
        fatal!(
            "lock {:?} failed, maybe another instance is using this directory.",
            store_path
        );
    }

    // Initialize raftstore channels.
    let mut event_loop = store::create_event_loop(&cfg.raft_store)
        .unwrap_or_else(|e| fatal!("failed to create event loop: {:?}", e));
    let store_sendch = SendCh::new(event_loop.channel(), "raftstore");
    let (significant_msg_sender, significant_msg_receiver) = mpsc::channel();
    let raft_router = ServerRaftStoreRouter::new(store_sendch.clone(), significant_msg_sender);
    let compaction_listener = new_compaction_listener(store_sendch.clone());

    // Create pd client and pd worker
    let pd_client = Arc::new(pd_client);
    let pd_worker = FutureWorker::new("pd worker");
    let (mut worker, resolver) = resolve::new_resolver(Arc::clone(&pd_client))
        .unwrap_or_else(|e| fatal!("failed to start address resolver: {:?}", e));
    let pd_sender = pd_worker.scheduler();

    // Create kv engine, storage.
    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.add_event_listener(compaction_listener);
    let kv_cfs_opts = cfg.rocksdb.build_cf_opts();
    let kv_engine = Arc::new(
        rocksdb_util::new_engine_opt(db_path.to_str().unwrap(), kv_db_opts, kv_cfs_opts)
            .unwrap_or_else(|s| fatal!("failed to create kv engine: {:?}", s)),
    );
    let storage_read_pool =
        ReadPool::new("store-read", &cfg.readpool.storage.build_config(), || {
            let pd_sender = pd_sender.clone();
            move || storage::ReadPoolContext::new(pd_sender.clone())
        });
    let mut storage = create_raft_storage(raft_router.clone(), &cfg.storage, storage_read_pool)
        .unwrap_or_else(|e| fatal!("failed to create raft stroage: {:?}", e));

    // Create raft engine.
    let raft_db_opts = cfg.raftdb.build_opt();
    let raft_db_cf_opts = cfg.raftdb.build_cf_opts();
    let raft_engine = Arc::new(
        rocksdb_util::new_engine_opt(
            raft_db_path.to_str().unwrap(),
            raft_db_opts,
            raft_db_cf_opts,
        ).unwrap_or_else(|s| fatal!("failed to create raft engine: {:?}", s)),
    );
    let engines = Engines::new(Arc::clone(&kv_engine), Arc::clone(&raft_engine));

    // Create snapshot manager, server.
    let snap_mgr = SnapManagerBuilder::default()
        .max_write_bytes_per_sec(cfg.server.snap_max_write_bytes_per_sec.0)
        .max_total_size(cfg.server.snap_max_total_size.0)
        .build(
            snap_path.as_path().to_str().unwrap().to_owned(),
            Some(store_sendch),
        );

    let importer = Arc::new(SSTImporter::new(import_path).unwrap());
    let import_service = ImportSSTService::new(
        cfg.import.clone(),
        raft_router.clone(),
        Arc::clone(&kv_engine),
        Arc::clone(&importer),
    );

    let server_cfg = Arc::new(cfg.server.clone());
    // Create server
    let cop_read_pool = ReadPool::new("cop", &cfg.readpool.coprocessor.build_config(), || {
        let pd_sender = pd_sender.clone();
        move || coprocessor::ReadPoolContext::new(pd_sender.clone())
    });
    let mut server = Server::new(
        &server_cfg,
        &security_mgr,
        cfg.coprocessor.region_split_size.0 as usize,
        storage.clone(),
        cop_read_pool,
        raft_router,
        resolver,
        snap_mgr.clone(),
        Some(engines.clone()),
        Some(import_service),
    ).unwrap_or_else(|e| fatal!("failed to create server: {:?}", e));
    let trans = server.transport();

    // Create node.
    let mut node = Node::new(&mut event_loop, &server_cfg, &cfg.raft_store, pd_client);

    // Create CoprocessorHost.
    let coprocessor_host = CoprocessorHost::new(cfg.coprocessor.clone(), node.get_sendch());

    node.start(
        event_loop,
        engines.clone(),
        trans,
        snap_mgr,
        significant_msg_receiver,
        pd_worker,
        coprocessor_host,
        importer,
    ).unwrap_or_else(|e| fatal!("failed to start node: {:?}", e));
    initial_metric(&cfg.metric, Some(node.id()));

    // Start storage.
    info!("start storage");
    if let Err(e) = storage.start(&cfg.storage) {
        fatal!("failed to start storage, error: {:?}", e);
    }

    let mut metrics_flusher = MetricsFlusher::new(
        engines.clone(),
        Duration::from_millis(DEFAULT_FLUSHER_INTERVAL),
    );

    // Start metrics flusher
    if let Err(e) = metrics_flusher.start() {
        error!("failed to start metrics flusher, error: {:?}", e);
    }

    // Run server.
    server
        .start(server_cfg, security_mgr)
        .unwrap_or_else(|e| fatal!("failed to start server: {:?}", e));
    signal_handler::handle_signal(Some(engines));

    // Stop.
    server
        .stop()
        .unwrap_or_else(|e| fatal!("failed to stop server: {:?}", e));

    metrics_flusher.stop();

    node.stop()
        .unwrap_or_else(|e| fatal!("failed to stop node: {:?}", e));
    if let Some(Err(e)) = worker.stop().map(|j| j.join()) {
        info!("ignore failure when stopping resolver: {:?}", e);
    }
}

fn main() {
    let long_version: String = {
        let (hash, branch, time, rust_ver) = tikv_util::build_info();
        format!(
            "\nRelease Version:   {}\
             \nGit Commit Hash:   {}\
             \nGit Commit Branch: {}\
             \nUTC Build Time:    {}\
             \nRust Version:      {}",
            crate_version!(),
            hash,
            branch,
            time,
            rust_ver
        )
    };
    let matches = App::new("TiKV")
        .long_version(long_version.as_ref())
        .author("PingCAP Inc. <info@pingcap.com>")
        .about("A Distributed transactional key-value database powered by Rust and Raft")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Sets config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Sets listening address"),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Sets advertise listening address for client communication"),
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&[
                    "trace", "debug", "info", "warn", "warning", "error", "critical",
                ])
                .help("Sets log level"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Sets log file. If not set, output log to stderr"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Sets the path to store directory"),
        )
        .arg(
            Arg::with_name("capacity")
                .long("capacity")
                .takes_value(true)
                .value_name("CAPACITY")
                .help("Sets the store capacity")
                .long_help("Sets the store capacity. If not set, use entire partition"),
        )
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .aliases(&["pd", "pd-endpoint"])
                .takes_value(true)
                .value_name("PD_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets PD endpoints")
                .long_help("Sets PD endpoints. Uses `,` to separate multiple PDs"),
        )
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .value_name("KEY=VALUE")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets server labels")
                .long_help(
                    "Sets server labels. Uses `,` to separate kv pairs, like \
                     `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        )
        .get_matches();

    if matches.is_present("print-sample-config") {
        let config = TiKvConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }

    let mut config = matches
        .value_of("config")
        .map_or_else(TiKvConfig::default, |path| TiKvConfig::from_file(&path));

    overwrite_config_with_cmd_args(&mut config, &matches);

    if let Err(e) = check_and_persist_critical_config(&config) {
        fatal!("check critical config failed, error {:?}", e);
    }

    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validata()`,
    // because `init_log()` handles various conditions.
    init_log(&config);

    // Print version information.
    tikv_util::print_tikv_info();

    panic_hook::set_exit_hook(false);

    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {:?}", e);
    }
    info!(
        "using config: {}",
        serde_json::to_string_pretty(&config).unwrap()
    );

    // Before any startup, check system configuration.
    check_system_config(&config);

    configure_grpc_poll_strategy();

    let security_mgr = Arc::new(
        SecurityManager::new(&config.security)
            .unwrap_or_else(|e| fatal!("failed to create security manager: {:?}", e)),
    );
    let pd_client = RpcClient::new(&config.pd, Arc::clone(&security_mgr))
        .unwrap_or_else(|e| fatal!("failed to create rpc client: {:?}", e));
    let cluster_id = pd_client
        .get_cluster_id()
        .unwrap_or_else(|e| fatal!("failed to get cluster id: {:?}", e));
    if cluster_id == DEFAULT_CLUSTER_ID {
        fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
    }
    config.server.cluster_id = cluster_id;
    info!("connect to PD cluster {}", cluster_id);

    let _m = Monitor::default();
    run_raft_server(pd_client, &config, security_mgr);
}
