// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::ToOwned,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use chrono::Local;
use clap::ArgMatches;
use collections::HashMap;
pub use server::setup::{
    ensure_no_unrecognized_config, initial_logger, initial_metric, validate_and_persist_config,
};
use tikv::config::{check_critical_config, persist_config, MetricConfig, TiKvConfig};
use tikv_util::{self, config, logger};

use crate::config::ProxyConfig;
pub use crate::fatal;

#[allow(dead_code)]
pub fn overwrite_config_with_cmd_args(
    config: &mut TiKvConfig,
    proxy_config: &mut ProxyConfig,
    matches: &ArgMatches<'_>,
) {
    if let Some(level) = matches.value_of("log-level") {
        config.log.level = logger::get_level_by_string(level).unwrap();
        config.log_level = slog::Level::Info;
    }

    if let Some(file) = matches.value_of("log-file") {
        config.log.file.filename = file.to_owned();
        config.log_file = "".to_owned();
    }

    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
    }

    if let Some(advertise_addr) = matches.value_of("advertise-addr") {
        config.server.advertise_addr = advertise_addr.to_owned();
    }

    if let Some(status_addr) = matches.value_of("status-addr") {
        config.server.status_addr = status_addr.to_owned();
    }

    if let Some(advertise_status_addr) = matches.value_of("advertise-status-addr") {
        config.server.advertise_status_addr = advertise_status_addr.to_owned();
    }

    if let Some(engine_store_version) = matches.value_of("engine-version") {
        proxy_config.engine_store_version = engine_store_version.to_owned();
    }

    if let Some(engine_store_git_hash) = matches.value_of("engine-git-hash") {
        proxy_config.engine_store_git_hash = engine_store_git_hash.to_owned();
    }

    if proxy_config.engine_addr.is_empty() {
        if let Some(engine_addr) = matches.value_of("engine-addr") {
            proxy_config.engine_addr = engine_addr.to_owned();
        }
    }

    if let Some(engine_addr) = matches.value_of("advertise-engine-addr") {
        proxy_config.engine_addr = engine_addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.storage.data_dir = data_dir.to_owned();
    }

    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(ToOwned::to_owned).collect();
    }

    if let Some(labels_vec) = matches.values_of("labels") {
        let mut labels = HashMap::default();
        for label in labels_vec {
            let mut parts = label.split('=');
            let key = parts.next().unwrap().to_owned();
            let value = match parts.next() {
                None => fatal!("invalid label: {}", label),
                Some(v) => v.to_owned(),
            };
            if parts.next().is_some() {
                fatal!("invalid label: {}", label);
            }
            labels.insert(key, value);
        }
        config.server.labels = labels;
    }

    if let Some(capacity_str) = matches.value_of("capacity") {
        let capacity = capacity_str.parse().unwrap_or_else(|e| {
            fatal!("invalid capacity: {}", e);
        });
        config.raft_store.capacity = capacity;
    }

    if matches.value_of("metrics-addr").is_some() {
        warn!("metrics push is not supported any more.");
    }
}
