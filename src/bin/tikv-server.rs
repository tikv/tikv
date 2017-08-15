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
#[cfg(feature = "mem-profiling")]
extern crate jemallocator;
extern crate tikv;
#[macro_use]
extern crate log;
extern crate rocksdb;
extern crate mio;
extern crate toml;
extern crate libc;
extern crate fs2;
#[cfg(unix)]
extern crate signal;
#[cfg(unix)]
extern crate nix;
extern crate prometheus;
extern crate sys_info;
extern crate futures;
extern crate serde_json;
extern crate tokio_core;
#[cfg(test)]
extern crate tempdir;
extern crate grpcio as grpc;

mod signal_handler;
#[cfg(unix)]
mod profiling;

use std::error::Error;
use std::process;
use std::fs::File;
use std::usize;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::io::Read;
use std::env;

use clap::{App, Arg, ArgMatches};
use fs2::FileExt;

use tikv::config::{MetricConfig, TiKvConfig};
use tikv::util::{self, panic_hook, rocksdb as rocksdb_util};
use tikv::util::collections::HashMap;
use tikv::util::logger::{self, StderrLogger};
use tikv::util::file_log::RotatingFileLogger;
use tikv::util::transport::SendCh;
use tikv::server::{create_raft_storage, Node, Server, DEFAULT_CLUSTER_ID};
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::resolve;
use tikv::raftstore::store::{self, SnapManager};
use tikv::pd::{PdClient, RpcClient};
use tikv::util::time::Monitor;

fn exit_with_err<E: Error>(e: E) -> ! {
    exit_with_msg(format!("{:?}", e))
}

fn exit_with_msg(msg: String) -> ! {
    error!("{}", msg);
    process::exit(1)
}

fn init_log(config: &TiKvConfig) {
    if config.log_file.is_empty() {
        logger::init_log(StderrLogger, config.log_level).unwrap_or_else(|e| exit_with_err(e));
    } else {
        let w = RotatingFileLogger::new(&config.log_file).unwrap_or_else(|e| exit_with_err(e));
        logger::init_log(w, config.log_level).unwrap_or_else(|e| exit_with_err(e));
    }
}

fn initial_metric(cfg: &MetricConfig, node_id: Option<u64>) {
    if cfg.interval.as_secs() == 0 || cfg.address.is_empty() {
        return;
    }

    let mut push_job = cfg.job.clone();
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("start prometheus client");

    util::monitor_threads("tikv").unwrap_or_else(|e| exit_with_err(e));

    util::run_prometheus(cfg.interval.0, &cfg.address, &push_job);
}

fn check_system_config(config: &TiKvConfig) {
    if let Err(e) = util::config::check_max_open_fds(config.rocksdb.max_open_files as u64) {
        exit_with_msg(format!("{:?}", e));
    }

    for e in util::config::check_kernel() {
        warn!("{:?}", e);
    }

    if cfg!(unix) && env::var("TZ").is_err() {
        env::set_var("TZ", ":/etc/localtime");
        warn!("environment variable `TZ` is missing, use `/etc/localtime`");
    }
}

fn run_raft_server(pd_client: RpcClient, cfg: &TiKvConfig) {
    let store_path = Path::new(&cfg.storage.data_dir);
    let lock_path = store_path.join(Path::new("LOCK"));
    let db_path = store_path.join(Path::new("db"));
    let snap_path = store_path.join(Path::new("snap"));

    let f = File::create(lock_path).unwrap_or_else(|e| exit_with_err(e));
    if f.try_lock_exclusive().is_err() {
        exit_with_msg(format!(
            "lock {:?} failed, maybe another instance is using this directory.",
            store_path
        ));
    }

    // Initialize raftstore channels.
    let mut event_loop =
        store::create_event_loop(&cfg.raft_store).unwrap_or_else(|e| exit_with_err(e));
    let store_sendch = SendCh::new(event_loop.channel(), "raftstore");
    let raft_router = ServerRaftStoreRouter::new(store_sendch.clone());
    let (snap_status_sender, snap_status_receiver) = mpsc::channel();

    // Create engine, storage.
    let opts = cfg.rocksdb.build_opt();
    let cfs_opts = cfg.rocksdb.build_cf_opts();
    let engine = Arc::new(
        rocksdb_util::new_engine_opt(db_path.to_str().unwrap(), opts, cfs_opts)
            .unwrap_or_else(|s| exit_with_msg(s)),
    );
    let mut storage = create_raft_storage(raft_router.clone(), engine.clone(), &cfg.storage)
        .unwrap_or_else(|e| exit_with_err(e));

    // Create pd client, snapshot manager, server.
    let pd_client = Arc::new(pd_client);
    let (mut worker, resolver) =
        resolve::new_resolver(pd_client.clone()).unwrap_or_else(|e| exit_with_err(e));
    let snap_mgr = SnapManager::new(
        snap_path.as_path().to_str().unwrap().to_owned(),
        Some(store_sendch),
        cfg.raft_store.use_sst_file_snapshot,
    );
    let mut server = Server::new(
        &cfg.server,
        cfg.raft_store.region_split_size.0 as usize,
        storage.clone(),
        raft_router,
        snap_status_sender,
        resolver,
        snap_mgr.clone(),
    ).unwrap_or_else(|e| exit_with_err(e));
    let trans = server.transport();

    // Create node.
    let mut node = Node::new(&mut event_loop, &cfg.server, &cfg.raft_store, pd_client);
    node.start(
        event_loop,
        engine.clone(),
        trans,
        snap_mgr,
        snap_status_receiver,
    ).unwrap_or_else(|e| exit_with_err(e));
    initial_metric(&cfg.metric, Some(node.id()));

    // Start storage.
    info!("start storage");
    if let Err(e) = storage.start(&cfg.storage) {
        panic!("failed to start storage, error = {:?}", e);
    }

    // Run server.
    server
        .start(&cfg.server)
        .unwrap_or_else(|e| exit_with_err(e));
    signal_handler::handle_signal(engine, &cfg.rocksdb.backup_dir);

    // Stop.
    server.stop().unwrap_or_else(|e| exit_with_err(e));
    node.stop().unwrap_or_else(|e| exit_with_err(e));
    if let Some(Err(e)) = worker.stop().map(|j| j.join()) {
        info!("ignore failure when stopping resolver: {:?}", e);
    }
}

fn overwrite_config_with_cmd_args(config: &mut TiKvConfig, matches: &ArgMatches) {
    if let Some(level) = matches.value_of("log-level") {
        config.log_level = logger::get_level_by_string(level);
    }

    if let Some(file) = matches.value_of("log-file") {
        config.log_file = file.to_owned();
    }

    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
    }

    if let Some(advertise_addr) = matches.value_of("advertise-addr") {
        config.server.advertise_addr = advertise_addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.storage.data_dir = data_dir.to_owned();
    }

    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(|e| e.to_owned()).collect();
    }

    if let Some(labels_vec) = matches.values_of("labels") {
        let mut labels = HashMap::new();
        labels_vec
            .map(|s| {
                let mut parts = s.split('=');
                let key = parts.next().unwrap().to_owned();
                let value = match parts.next() {
                    None => exit_with_msg(format!("invalid label: {:?}", s)),
                    Some(v) => v.to_owned(),
                };
                if parts.next().is_some() {
                    exit_with_msg(format!("invalid label: {:?}", s));
                }
                labels.insert(key, value);
            })
            .count();
        config.server.labels = labels;
    }

    if let Some(capacity_str) = matches.value_of("capacity") {
        let capacity = capacity_str.parse().unwrap_or_else(|e| exit_with_err(e));
        config.raft_store.capacity.0 = capacity;
    }
}

fn main() {
    let long_version: String = {
        let (hash, time, rust_ver) = util::build_info();
        format!(
            "{}\nGit Commit Hash: {}\nUTC Build Time:  {}\nRust Version:    {}",
            crate_version!(),
            hash,
            time,
            rust_ver
        )
    };
    let matches = App::new("TiKV")
        .long_version(long_version.as_ref())
        .author("PingCAP Inc. <info@pingcap.com>")
        .about(
            "A Distributed transactional key-value database powered by Rust and Raft",
        )
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
                .possible_values(&["trace", "debug", "info", "warn", "error", "off"])
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

    let mut config = matches.value_of("config").map_or_else(
        TiKvConfig::default,
        |path| {
            File::open(&path)
                .map_err::<Box<Error>, _>(|e| Box::new(e))
                .and_then(|mut f| {
                    let mut s = String::new();
                    try!(f.read_to_string(&mut s));
                    let c = try!(toml::from_str(&s));
                    Ok(c)
                })
                .unwrap_or_else(|e| {
                    eprintln!("{:?}", e);
                    process::exit(-1);
                })
        },
    );

    overwrite_config_with_cmd_args(&mut config, &matches);

    if let Err(e) = config.validate() {
        eprintln!("{:?}", e);
        process::exit(-1);
    }

    init_log(&config);

    // Print version information.
    util::print_tikv_info();

    panic_hook::set_exit_hook();

    info!(
        "using config: {}",
        serde_json::to_string_pretty(&config).unwrap()
    );

    // Before any startup, check system configuration.
    check_system_config(&config);

    let pd_client = RpcClient::new(&config.pd.endpoints).unwrap_or_else(|e| exit_with_err(e));
    let cluster_id = pd_client
        .get_cluster_id()
        .unwrap_or_else(|e| exit_with_err(e));
    if cluster_id == DEFAULT_CLUSTER_ID {
        exit_with_msg(format!("cluster id can't be {}", DEFAULT_CLUSTER_ID));
    }
    config.server.cluster_id = cluster_id;
    info!("connect to PD cluster {}", cluster_id);

    let _m = Monitor::default();
    run_raft_server(pd_client, &config);
}
