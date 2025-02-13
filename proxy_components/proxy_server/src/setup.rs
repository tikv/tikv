// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::borrow::ToOwned;

use clap::ArgMatches;
pub use server::setup::initial_logger;
use tikv::config::{MetricConfig, TikvConfig, MEMORY_USAGE_LIMIT_RATE};
use tikv_util::{self, config::ReadableSize, logger, sys::SysQuota};

use crate::config::ProxyConfig;
pub use crate::fatal;

#[allow(dead_code)]
pub fn initial_metric(cfg: &MetricConfig) {
    tikv_util::metrics::monitor_process()
        .unwrap_or_else(|e| fatal!("failed to start process monitor: {}", e));
    tikv_util::metrics::monitor_threads("")
        .unwrap_or_else(|e| fatal!("failed to start thread monitor: {}", e));
    tikv_util::metrics::monitor_allocator_stats("")
        .unwrap_or_else(|e| fatal!("failed to monitor allocator stats: {}", e));

    if cfg.interval.as_secs() == 0 || cfg.address.is_empty() {
        return;
    }

    warn!("metrics push is not supported any more.");
}

#[allow(dead_code)]
pub fn overwrite_config_with_cmd_args(
    config: &mut TikvConfig,
    proxy_config: &mut ProxyConfig,
    matches: &ArgMatches<'_>,
) {
    info!("arg matches is {:?}", matches);
    println!("arg matches is {:?}", matches);
    if let Some(level) = matches.value_of("log-level") {
        config.log.level = logger::get_level_by_string(level).unwrap().into();
        // For backward compating
        #[allow(deprecated)]
        config.log_level = slog::Level::Info.into();
    }

    if let Some(file) = matches.value_of("log-file") {
        config.log.file.filename = file.to_owned();
        // For backward compating
        #[allow(deprecated)]
        config.log_file = "".to_owned();
    }

    // See https://github.com/pingcap/tidb-engine-ext/blob/raftstore-proxy-5.4/components/server/src/setup.rs
    // For addr/advertise-addr/status-addr/advertise-status-addr, we firstly use
    // what TiFlash gives us.
    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
        proxy_config.server.addr = addr.to_owned();
    }

    if let Some(advertise_addr) = matches.value_of("advertise-addr") {
        config.server.advertise_addr = advertise_addr.to_owned();
        proxy_config.server.advertise_addr = advertise_addr.to_owned();
    }

    if let Some(status_addr) = matches.value_of("status-addr") {
        config.server.status_addr = status_addr.to_owned();
        proxy_config.server.status_addr = status_addr.to_owned();
    }

    if let Some(advertise_status_addr) = matches.value_of("advertise-status-addr") {
        config.server.advertise_status_addr = advertise_status_addr.to_owned();
        proxy_config.server.advertise_status_addr = advertise_status_addr.to_owned();
    }

    if let Some(engine_store_version) = matches.value_of("engine-version") {
        proxy_config.server.engine_store_version = engine_store_version.to_owned();
    }

    if let Some(engine_store_git_hash) = matches.value_of("engine-git-hash") {
        proxy_config.server.engine_store_git_hash = engine_store_git_hash.to_owned();
    }

    // The special case is engine-addr:
    // 1. If we have set our own engine-addr, we just ignore what TiFlash gives us.
    // 2. However, if we have not set our own value, we use what TiFlash gives us.
    if proxy_config.server.engine_addr.is_empty() {
        if let Some(engine_addr) = matches.value_of("engine-addr") {
            proxy_config.server.engine_addr = engine_addr.to_owned();
        }
    }

    // About `flash.proxy.advertise-engine-addr` (on TiFlash's side):
    // 1. If `flash.proxy.server.engine-addr` is not set, use `flash.service_addr`
    // as `engine-addr`.
    // 2. If `flash.proxy.server.engine-addr` is set, use
    // `flash.proxy.server.engine-addr` as `advertise-engine-addr`.
    if let Some(engine_addr) = matches.value_of("advertise-engine-addr") {
        proxy_config.server.engine_addr = engine_addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.storage.data_dir = data_dir.to_owned();
    }

    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(ToOwned::to_owned).collect();
    }

    if let Some(labels_vec) = matches.values_of("labels") {
        // labels_vec is a vector of string like ["k1=v1", "k1=v2", ...]
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
            if config.server.labels.contains_key(&key) {
                warn!(
                    "label is ignored due to duplicated key defined in `server.labels`, key={} value={}",
                    key, value
                );
                continue; // ignore duplicated label
            }
            // only set the label when `server.labels` does not contain the label with the
            // same key
            config.server.labels.insert(key, value);
        }
    }
    // User can specify engine label, because we need to distinguish TiFlash role
    // (tiflash-compute or tiflash-storage) in the disaggregated architecture.
    // If no engine label is specified, we use 'ENGINE_LABEL_VALUE'(env variable
    // specified at compile time).
    const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";
    config.server.labels.insert(
        DEFAULT_ENGINE_LABEL_KEY.to_owned(),
        String::from(
            matches
                .value_of("engine-label")
                .or(option_env!("ENGINE_LABEL_VALUE"))
                .unwrap(),
        ),
    );

    if let Some(capacity_str) = matches.value_of("capacity") {
        let capacity = capacity_str.parse().unwrap_or_else(|e| {
            fatal!("invalid capacity: {}", e);
        });
        config.raft_store.capacity = capacity;
    }

    if matches.value_of("metrics-addr").is_some() {
        warn!("metrics push is not supported any more.");
    }

    if let Some(unips_enabled_str) = matches.value_of("unips-enabled") {
        let enabled: u64 = unips_enabled_str.parse().unwrap_or_else(|e| {
            fatal!("invalid unips-enabled: {}", e);
        });
        proxy_config.engine_store.enable_unips = enabled == 1;
    }

    let mut memory_limit_set = config.memory_usage_limit.is_some();
    if !memory_limit_set {
        if let Some(s) = matches.value_of("memory-limit-size") {
            let result: Result<u64, _> = s.parse();
            if let Ok(memory_limit_size) = result {
                info!(
                    "overwrite memory_usage_limit by `memory-limit-size` to {}",
                    memory_limit_size
                );
                config.memory_usage_limit = Some(ReadableSize(memory_limit_size));
                memory_limit_set = true;
            } else {
                info!("overwrite memory_usage_limit by `memory-limit-size` failed"; "memory_limit_size" => s);
            }
        }
    }

    let total = SysQuota::memory_limit_in_bytes();
    if !memory_limit_set {
        if let Some(s) = matches.value_of("memory-limit-ratio") {
            let result: Result<f64, _> = s.parse();
            if let Ok(memory_limit_ratio) = result {
                if memory_limit_ratio <= 0.0 || memory_limit_ratio > 1.0 {
                    info!("overwrite memory_usage_limit meets error ratio"; "ratio" => memory_limit_ratio);
                } else {
                    let limit = (total as f64 * memory_limit_ratio) as u64;
                    info!(
                        "overwrite memory_usage_limit by `memory-limit-ratio`={} to {}",
                        memory_limit_ratio, limit
                    );
                    config.memory_usage_limit = Some(ReadableSize(limit));
                    memory_limit_set = true;
                }
            } else {
                info!("overwrite memory_usage_limit meets error ratio"; "ratio" => s);
            }
        }
    }

    if !memory_limit_set && config.memory_usage_limit.is_none() {
        let limit = (total as f64 * MEMORY_USAGE_LIMIT_RATE) as u64;
        info!("overwrite memory_usage_limit failed, use TiKV's default"; "limit" => limit);
        config.memory_usage_limit = Some(ReadableSize(limit));
    }
}
