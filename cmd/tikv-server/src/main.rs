// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_hygiene)]

mod fork;

use std::{path::Path, process};

use clap::{crate_authors, App, Arg};
use encryption_export::data_key_manager_from_config;
use engine_traits::Peekable;
use fork::{create_dir, dup_kv_engine_files, dup_raft_engine_files, symlink_snaps};
use kvproto::raft_serverpb::StoreIdent;
use serde_json::{Map, Value};
use server::setup::{ensure_no_unrecognized_config, validate_and_persist_config};
use tikv::{
    config::{to_flatten_config_info, TikvConfig},
    server::KvEngineFactoryBuilder,
    storage::config::EngineType,
};

fn main() {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    let version_info = tikv::tikv_version_info(build_timestamp);

    let matches = App::new("TiKV")
        .about("A distributed transactional key-value database powered by Rust and Raft")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .arg(
            Arg::with_name("create-fork")
                .required(false)
                .long("create-fork")
                .takes_value(true)
                .value_name("DIR")
                .help("create a fork instance from exists data and exit"),
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
            Arg::with_name("config-check")
                .required(false)
                .long("config-check")
                .takes_value(false)
                .help("Check config file validity and exit"),
        )
        .arg(
            Arg::with_name("config-info")
                .required(false)
                .long("config-info")
                .takes_value(true)
                .value_name("FORMAT")
                .possible_values(&["json"])
                .help("print configuration information with specified format")
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
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
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

    if matches.is_present("print-sample-config") {
        let config = TikvConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }

    let mut unrecognized_keys = Vec::new();
    let is_config_check = matches.is_present("config-check");

    let mut config = matches
        .value_of_os("config")
        .map_or_else(TikvConfig::default, |path| {
            let path = Path::new(path);
            TikvConfig::from_file(
                path,
                if is_config_check {
                    Some(&mut unrecognized_keys)
                } else {
                    None
                },
            )
            .unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    path.display(),
                    e
                );
            })
        });

    server::setup::overwrite_config_with_cmd_args(&mut config, &matches);
    config.logger_compatible_adjust();

    if is_config_check {
        validate_and_persist_config(&mut config, false);
        ensure_no_unrecognized_config(&unrecognized_keys);
        println!("config check successful");
        process::exit(0)
    }

    let is_config_info = matches.is_present("config-info");
    if is_config_info {
        let config_infos = to_flatten_config_info(&config);
        let mut result = Map::new();
        result.insert("Component".into(), "TiKV Server".into());
        result.insert("Version".into(), tikv::tikv_build_version().into());
        result.insert("Parameters".into(), Value::Array(config_infos));
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
        process::exit(0);
    }

    // engine config needs to be validated
    // so that it can adjust the engine type before too late
    if let Err(e) = config.storage.validate_engine_type() {
        println!("invalid storage.engine configuration: {}", e);
        process::exit(1)
    }

    // This is for creating a shadow instance from exists data, which is specified
    // by `config.data_dir`. When duplicating all data files, *symlink* will be used
    // if possible, otherwise *copy* will be used instead.
    if let Some(agent_dir) = matches.value_of("create-fork") {
        if data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .unwrap()
            .is_some()
        {
            eprintln!("fork with encryption enabled is not expected");
            process::exit(-1);
        }

        if let Err(e) = create_dir(agent_dir) {
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
        println!("symlink kv engine files success");

        if let Err(e) = dup_raft_engine_files(&config, agent_dir) {
            eprintln!("symlink raft engine fail: {}", e);
            process::exit(-1);
        }
        println!("symlink raft engine success");

        // Disable background routines and read cluster ID.
        config.storage.data_dir = agent_dir.to_owned();
        config.rocksdb.wal_dir = "".to_owned();
        config.rocksdb.defaultcf.disable_auto_compactions = true;
        config.rocksdb.writecf.disable_auto_compactions = true;
        config.rocksdb.lockcf.disable_auto_compactions = true;
        config.rocksdb.raftcf.disable_auto_compactions = true;
        config.rocksdb.titan.disable_gc = true;
        match read_cluster_id(&config) {
            Ok(id) => {
                println!("cluster-id: {}", id);
                process::exit(0);
            }
            Err(e) => {
                eprintln!("read cluster ID fail: {}", e);
                process::exit(-1);
            }
        }
    }

    match config.storage.engine {
        EngineType::RaftKv => server::server::run_tikv(config),
        EngineType::RaftKv2 => server::server2::run_tikv(config),
    }
}

// Open KV engine and read cluster ID.
fn read_cluster_id(config: &TikvConfig) -> Result<u64, String> {
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
