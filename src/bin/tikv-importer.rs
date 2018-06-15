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
#![cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value, unreadable_literal))]

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

use std::process;
use std::sync::atomic::Ordering;

use clap::{App, Arg, ArgMatches};

use tikv::config::TiKvConfig;
use tikv::import::ImportKVServer;
use tikv::util::{self as tikv_util, panic_hook};

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

    let matches = App::new("TiKV Importer")
        .long_version(long_version.as_ref())
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

    tikv_util::print_tikv_info();
    panic_hook::set_exit_hook(false);
    configure_grpc_poll_strategy();

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
