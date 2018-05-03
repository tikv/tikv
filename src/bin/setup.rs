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

use slog::{self, Drain};
use slog_async;
use slog_term;
use std::env;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering, ATOMIC_BOOL_INIT};

use clap::ArgMatches;

use tikv::config::{MetricConfig, TiKvConfig};
use tikv::util;
use tikv::util::collections::HashMap;
use tikv::util::file_log::RotatingFileLogger;
use tikv::util::logger;

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = ATOMIC_BOOL_INIT;

macro_rules! fatal {
    ($lvl:expr, $($arg:tt)+) => ({
        if LOG_INITIALIZED.load(Ordering::SeqCst) {
            error!($lvl, $($arg)+);
        } else {
            eprintln!($lvl, $($arg)+);
        }
        process::exit(1)
    })
}

pub fn init_log(config: &TiKvConfig) {
    if config.log_file.is_empty() {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let logger = slog::Logger::root_typed(drain, slog_o!());
        logger::init_log(logger, config.log_level).unwrap_or_else(|e| {
            fatal!("failed to initialize log: {:?}", e);
        });
    } else {
        let logger = RotatingFileLogger::new(&config.log_file).unwrap_or_else(|e| {
            fatal!(
                "failed to initialize log with file {:?}: {:?}",
                config.log_file,
                e
            );
        });
        let decorator = slog_term::PlainDecorator::new(logger);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let logger = slog::Logger::root_typed(drain, slog_o!());
        logger::init_log(logger, config.log_level).unwrap_or_else(|e| {
            fatal!("failed to initialize log: {:?}", e);
        });
    }
    LOG_INITIALIZED.store(true, Ordering::SeqCst);
}

pub fn initial_metric(cfg: &MetricConfig, node_id: Option<u64>) {
    if cfg.interval.as_secs() == 0 || cfg.address.is_empty() {
        return;
    }

    let mut push_job = cfg.job.clone();
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("start prometheus client");

    util::metrics::monitor_threads("tikv")
        .unwrap_or_else(|e| fatal!("failed to start monitor thread: {:?}", e));

    util::metrics::run_prometheus(cfg.interval.0, &cfg.address, &push_job);
}

pub fn overwrite_config_with_cmd_args(config: &mut TiKvConfig, matches: &ArgMatches) {
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
        let mut labels = HashMap::default();
        labels_vec
            .map(|s| {
                let mut parts = s.split('=');
                let key = parts.next().unwrap().to_owned();
                let value = match parts.next() {
                    None => fatal!("invalid label: {:?}", s),
                    Some(v) => v.to_owned(),
                };
                if parts.next().is_some() {
                    fatal!("invalid label: {:?}", s);
                }
                labels.insert(key, value);
            })
            .count();
        config.server.labels = labels;
    }

    if let Some(capacity_str) = matches.value_of("capacity") {
        let capacity = capacity_str.parse().unwrap_or_else(|e| {
            fatal!("invalid capacity: {}", e);
        });
        config.raft_store.capacity = capacity;
    }

    if let Some(import_dir) = matches.value_of("import-dir") {
        config.import.import_dir = import_dir.to_owned();
    }

    if matches.is_present("import-mode") {
        config.tune_for_import_mode();
    }
}

// Set gRPC event engine to epollsig.
// See more: https://github.com/grpc/grpc/blob/486761d04e03a9183d8013eddd86c3134d52d459\
//           /src/core/lib/iomgr/ev_posix.cc#L149
pub fn configure_grpc_poll_strategy() {
    const GRPC_POLL_STRATEGY: &str = "GRPC_POLL_STRATEGY";
    const DEFAULT_ENGINE: &str = "epollsig";
    if cfg!(target_os = "linux") && env::var(GRPC_POLL_STRATEGY).is_err() {
        // Set to epollsig if it is not specified.
        env::set_var(GRPC_POLL_STRATEGY, DEFAULT_ENGINE);
    }
}
