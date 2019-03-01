// Copyright 2018 PingCAP, Inc.
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

#![feature(slice_patterns)]
#![feature(proc_macro_hygiene)]

use serde_json;

#[macro_use(
    slog_kv,
    slog_error,
    slog_info,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;

#[macro_use]
extern crate slog_global;

#[cfg(unix)]
#[macro_use]
mod util;
use crate::util::setup::*;
use crate::util::signal_handler;

use std::process;
use std::sync::atomic::Ordering;

use clap::{crate_authors, crate_version, App, Arg, ArgMatches};

use tikv::config::TiKvConfig;
use tikv::import::ImportKVServer;
use tikv::util::{self as tikv_util, check_environment_variables};

fn main() {
    let matches = App::new("TiKV Importer")
        .about("The importer server for TiKV")
        .author(crate_authors!())
        .version(crate_version!())
        .long_version(util::tikv_version_info().as_ref())
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-file")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Set the log file"),
        )
        .arg(
            Arg::with_name("log-level")
                .long("log-level")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&["trace", "debug", "info", "warn", "error", "off"])
                .help("Set the log level"),
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
            Arg::with_name("import-dir")
                .long("import-dir")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory to store importing kv data"),
        )
        .get_matches();

    let config = setup_config(&matches);
    initial_logger(&config);
    tikv_util::set_panic_hook(false, &config.storage.data_dir);

    initial_metric(&config.metric, None);
    util::log_tikv_info();
    check_environment_variables();

    if tikv_util::panic_mark_file_exists(&config.storage.data_dir) {
        fatal!(
            "panic_mark_file {} exists, there must be something wrong with the db.",
            tikv_util::panic_mark_file_path(&config.storage.data_dir).display()
        );
    }

    run_import_server(&config);
}

fn setup_config(matches: &ArgMatches<'_>) -> TiKvConfig {
    let mut config = matches
        .value_of("config")
        .map_or_else(TiKvConfig::default, |path| TiKvConfig::from_file(&path));

    overwrite_config_with_cmd_args(&mut config, matches);

    if let Err(e) = config.import.validate() {
        fatal!("invalid configuration: {:?}", e);
    }
    info!(
        "using config";
        "config" => serde_json::to_string(&config).unwrap(),
    );

    config.write_into_metrics();

    config
}

fn run_import_server(config: &TiKvConfig) {
    let mut server = ImportKVServer::new(config);
    server.start();
    info!("import server started");
    signal_handler::handle_signal(None);
    server.shutdown();
    info!("import server shutdown");
}
