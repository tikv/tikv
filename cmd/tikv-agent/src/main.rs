// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_hygiene)]

mod utils;

use std::{path::Path, process};

use clap::{crate_authors, App, Arg};
use encryption_export::data_key_manager_from_config;
use engine_traits::Peekable;
use kvproto::raft_serverpb::StoreIdent;
use raft_engine::ReadableSize as RaftEngineCfgSize;
use tikv::{config::TikvConfig, server::KvEngineFactoryBuilder, storage::config::EngineType};
use tikv_util::config::{ReadableDuration, ReadableSize};
use utils::{dup_kv_engine_files, dup_raft_engine_files, remove_and_recreate_dir, symlink_snaps};

fn main() {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    let version_info = tikv::tikv_version_info(build_timestamp);

    let matches = App::new("TiKV agent")
        .about("Start a TiKV shadow (of config) instance at agent-dir")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .arg(
            Arg::with_name("agent-dir")
                .required(true)
                .long("agent-dir")
                .takes_value(true)
                .help("agent directory, will be cleaned if exists"),
            )
        .arg(
            Arg::with_name("skip-build-agent-dir")
                .long("skip-build-agent-dir")
                .takes_value(false)
                .help("whether to skip building the agent directory"),
            )
        .arg(
            Arg::with_name("show-cluster-id")
                .long("show-cluster-id")
                .takes_value(false)
                .help("show cluster id"),
        )
        .arg(
            Arg::with_name("omit-logs")
                .long("omit-logs")
                .takes_value(false)
                .help("whether to omit logs"),
            )
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&[
                    "trace", "debug", "info", "warn", "warning", "error", "critical", "fatal",
                ])
                .help("Set the log level"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Set the log file path. If not set, logs will output to stderr"),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for client communication"),
        )
        .arg(
            Arg::with_name("status-addr")
                .long("status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the HTTP listening address for the status report service"),
        )
        .arg(
            Arg::with_name("advertise-status-addr")
                .long("advertise-status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for the client communication of status report service"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory used to store data"),
        )
        .arg(
            Arg::with_name("capacity")
                .long("capacity")
                .takes_value(true)
                .value_name("CAPACITY")
                .help("Set the store capacity")
                .long_help("Set the store capacity to use. If not set, use entire partition"),
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
                .long_help("Set the PD endpoints to use. Use `,` to separate multiple PDs"),
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
                    "Set the server labels. Uses `,` to separate kv pairs, like \
                     `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("metrics-addr")
                .long("metrics-addr")
                .value_name("IP:PORT")
                .hidden(true)
                .help("Sets Prometheus Pushgateway address")
                .long_help(
                    "Sets push address to the Prometheus Pushgateway, \
                     leaves it empty will disable Prometheus push",
                ),
        )
        .get_matches();

    let mut config = matches
        .value_of_os("config")
        .map_or_else(TikvConfig::default, |path| {
            let path = Path::new(path);
            TikvConfig::from_file(path, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    path.display(),
                    e
                );
            })
        });

    server::setup::overwrite_config_with_cmd_args(&mut config, &matches);
    config.logger_compatible_adjust();

    if data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
        .unwrap()
        .is_some()
    {
        eprintln!("agent mode isn't available with encryption");
        process::exit(-1);
    }

    let agent_dir = matches.value_of("agent-dir").unwrap();

    if !matches.is_present("skip-build-agent-dir") {
        if let Err(e) = remove_and_recreate_dir(agent_dir) {
            eprintln!("remove and re-create agent directory fail: {}", e);
            process::exit(-1);
        }
        println!("remove and re-create agent directory success");

        if let Err(e) = symlink_snaps(&config, agent_dir) {
            eprintln!("symlink snapshot files fail: {}", e);
            process::exit(-1);
        }
        println!("symlink snapshot files success");

        if let Err(e) = dup_kv_engine_files(&config, agent_dir) {
            eprintln!("symlink kv engine files fail: {}", e);
            process::exit(-1);
        }
        eprintln!("symlink kv engine files success");

        if let Err(e) = dup_raft_engine_files(&config, agent_dir) {
            eprintln!("symlink raft engine fail: {}", e);
            process::exit(-1);
        }
        println!("symlink raft engine success");
    }

    config.storage.data_dir = agent_dir.to_owned();
    config.rocksdb.wal_dir = "".to_owned();
    config.raftdb.wal_dir = "".to_owned();
    config.raft_engine.mut_config().dir = "".to_owned();

    if matches.is_present("show-cluster-id") {
        println!("cluster-id: {:?}", get_cluster_id(&config));
        process::exit(0);
    }

    if matches.is_present("omit-logs") {
        config.log.file.filename = "/dev/null".to_owned();
        config.log.file.max_backups = 0;
        config.slow_log_file = "/dev/null".to_owned();
        config.rocksdb.info_log_dir = "/tmp/".to_owned();
        config.raftdb.info_log_dir = "/tmp/".to_owned();
    } else {
        config.log.file.filename = "".to_owned();
        config.log.file.max_backups = 0;
        config.slow_log_file = "".to_owned();
        config.rocksdb.info_log_dir = "".to_owned();
        config.raftdb.info_log_dir = "".to_owned();
    }

    // Try to avoid generating new disk files, or delete new generated files ASAP.
    config.storage.reserve_space = ReadableSize(0);
    config.storage.reserve_raft_space = ReadableSize(0);
    config.rocksdb.defaultcf.disable_auto_compactions = true;
    config.rocksdb.writecf.disable_auto_compactions = true;
    config.rocksdb.lockcf.disable_auto_compactions = true;
    config.rocksdb.raftcf.disable_auto_compactions = true;
    if config.raft_engine.enable {
        config.raft_engine.mut_config().purge_threshold = RaftEngineCfgSize::gb(1);
        config.raft_engine.mut_config().purge_rewrite_garbage_ratio = 0.99;
        config.raft_engine.mut_config().enable_log_recycle = false;
    } else {
        config.raftdb.defaultcf.disable_auto_compactions = true;
    }

    // To limit memory components.
    config.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1);
    config.storage.block_cache.capacity = Some(ReadableSize::gb(1));

    // For snapshot apply.
    config.raft_store.snap_apply_copy_symlink = true;

    match config.storage.engine {
        EngineType::RaftKv => server::server::run_tikv(config),
        EngineType::RaftKv2 => server::server2::run_tikv(config),
    }
}

fn get_cluster_id(config: &TikvConfig) -> Result<u64, String> {
    let env = config
        .build_shared_rocks_env(None, None)
        .map_err(|e| format!("build_shared_rocks_env fail: {}", e))?;
    let cache = config
        .storage
        .block_cache
        .build_shared_cache(config.storage.engine);
    let kv_engine = KvEngineFactoryBuilder::new(env, config, cache)
        .build()
        .create_shared_db(&config.storage.data_dir)
        .map_err(|e| format!("create_shared_db fail: {}", e))?;
    let ident = kv_engine
        .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    Ok(ident.cluster_id)
}
