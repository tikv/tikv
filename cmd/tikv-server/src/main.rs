// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_hygiene)]

use std::{path::Path, process};

use clap::{crate_authors, App, Arg};
use crypto::fips;
use serde_json::{Map, Value};
use server::setup::{ensure_no_unrecognized_config, validate_and_persist_config};
use tikv::{
    config::{to_flatten_config_info, TikvConfig},
    storage::config::EngineType,
};

fn main() {
    // OpenSSL FIPS mode should be enabled at the very start.
    fips::maybe_enable();

    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    let version_info = tikv::tikv_version_info(build_timestamp);

    let matches = App::new("TiKV")
        .about("A distributed transactional key-value database powered by Rust and Raft")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
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

    // Initialize the async-backtrace.
    #[cfg(feature = "trace-async-tasks")]
    {
        use tracing_subscriber::prelude::*;
        tracing_subscriber::registry()
            .with(tracing_active_tree::layer::global().clone())
            .init();
    }

    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    server::setup::initial_logger(&config);

    // Print version information.
    tikv::log_tikv_info(build_timestamp);

    // Print OpenSSL FIPS mode status.
    fips::log_status();

    // Init memory related settings.
    config.memory.init();

    let (service_event_tx, service_event_rx) = tikv_util::mpsc::unbounded(); // pipe for controling service
    match config.storage.engine {
        EngineType::RaftKv => server::server::run_tikv(config, service_event_tx, service_event_rx),
        EngineType::RaftKv2 => {
            server::server2::run_tikv(config, service_event_tx, service_event_rx)
        }
    }
}
