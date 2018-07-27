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

extern crate clap;
#[macro_use]
extern crate log;
extern crate serde_json;
extern crate tikv;
#[macro_use]
extern crate tikv_bin;

use tikv_bin::util;
use tikv_bin::util::setup::*;
use tikv_bin::util::signal_handler;

use std::process;
use std::sync::atomic::Ordering;

use clap::{App, Arg, ArgMatches};

use tikv::config::TiKvConfig;
use tikv::import::ImportKVServer;
use tikv::util::panic_hook;

fn main() {
    let matches = App::new("TiKV Importer")
        .long_version(util::build_info().as_ref())
        .author("PingCAP Inc. <info@pingcap.com>")
        .about("An import server for TiKV")
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Sets listening address"),
        )
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Sets configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-file")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file"),
        )
        .arg(
            Arg::with_name("log-level")
                .long("log-level")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&["trace", "debug", "info", "warn", "error", "off"])
                .help("Sets log level"),
        )
        .arg(
            Arg::with_name("import-dir")
                .long("import-dir")
                .takes_value(true)
                .value_name("PATH")
                .help("Sets the directory to store importing kv data"),
        )
        .get_matches();

    let config = setup_config(&matches);
    init_log(&config);
    initial_metric(&config.metric, None);

    util::print_tikv_info();
    panic_hook::set_exit_hook(false);
    check_environment_variables();

    run_import_server(&config);
}

fn setup_config(matches: &ArgMatches) -> TiKvConfig {
    let mut config = matches
        .value_of("config")
        .map_or_else(TiKvConfig::default, |path| TiKvConfig::from_file(&path));

    overwrite_config_with_cmd_args(&mut config, matches);

    if let Err(e) = config.import.validate() {
        fatal!("invalid configuration: {:?}", e);
    }
    info!(
        "using config: {}",
        serde_json::to_string_pretty(&config).unwrap()
    );

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
