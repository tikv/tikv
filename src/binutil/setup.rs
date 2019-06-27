// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::ToOwned;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono;
use clap::ArgMatches;

use crate::config::{MetricConfig, TiKvConfig};
use slog::Drain;
use tikv_util::collections::HashMap;
use tikv_util::{self, logger};

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

#[macro_export]
macro_rules! fatal {
    ($lvl:expr, $($arg:tt)+) => ({
        if $crate::binutil::setup::LOG_INITIALIZED.load(::std::sync::atomic::Ordering::SeqCst) {
            crit!($lvl, $($arg)+);
        } else {
            eprintln!($lvl, $($arg)+);
        }
        slog_global::clear_global();
        ::std::process::exit(1)
    })
}

#[allow(dead_code)]
pub fn initial_logger(config: &TiKvConfig) {
    let log_rotation_timespan =
        chrono::Duration::from_std(config.log_rotation_timespan.clone().into())
            .expect("config.log_rotation_timespan is an invalid duration.");

    // Collects following targets.
    let mut enabled_targets = vec![
        "tikv::".to_owned(),
        "raft::".to_owned(),
        "tests::".to_owned(),
        "benches::".to_owned(),
        "integrations::".to_owned(),
        "failpoints::".to_owned(),
        // Collects logs for test components.
        "test_".to_owned(),
    ];
    // Only for debug purpose, so use environment instead of configuration file.
    if let Ok(extra_modules) = env::var("TIKV_EXTRA_LOG_TARGETS") {
        enabled_targets.extend(extra_modules.split(',').map(ToOwned::to_owned));
    }
    if config.log_file.is_empty() {
        let drainer = logger::term_drainer();
        let filtered = drainer.filter(move |record| {
            enabled_targets
                .iter()
                .any(|target| record.module().starts_with(target))
        });
        // use async drainer and init std log.
        logger::init_log(filtered, config.log_level, true, true).unwrap_or_else(|e| {
            fatal!("failed to initialize log: {}", e);
        });
    } else {
        let drainer =
            logger::file_drainer(&config.log_file, log_rotation_timespan).unwrap_or_else(|e| {
                fatal!(
                    "failed to initialize log with file {}: {}",
                    config.log_file,
                    e
                );
            });

        let filtered = drainer.filter(move |record| {
            enabled_targets
                .iter()
                .any(|target| record.module().starts_with(target))
        });
        // use async drainer and init std log.
        logger::init_log(filtered, config.log_level, true, true).unwrap_or_else(|e| {
            fatal!("failed to initialize log: {}", e);
        });
    };
    LOG_INITIALIZED.store(true, Ordering::SeqCst);
}

#[allow(dead_code)]
pub fn initial_metric(cfg: &MetricConfig, node_id: Option<u64>) {
    tikv_util::metrics::monitor_threads("tikv")
        .unwrap_or_else(|e| fatal!("failed to start monitor thread: {}", e));
    tikv_util::metrics::monitor_allocator_stats("tikv")
        .unwrap_or_else(|e| fatal!("failed to monitor allocator stats: {}", e));

    if cfg.interval.as_secs() == 0 || cfg.address.is_empty() {
        return;
    }

    let mut push_job = cfg.job.clone();
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("start prometheus client");
    tikv_util::metrics::run_prometheus(cfg.interval.0, &cfg.address, &push_job);
}

#[allow(dead_code)]
pub fn overwrite_config_with_cmd_args(config: &mut TiKvConfig, matches: &ArgMatches<'_>) {
    if let Some(level) = matches.value_of("log-level") {
        config.log_level = logger::get_level_by_string(level).unwrap();
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

    if let Some(status_addr) = matches.value_of("status-addr") {
        config.server.status_addr = status_addr.to_owned();
    }

    if let Some(data_dir) = matches.value_of("data-dir") {
        config.storage.data_dir = data_dir.to_owned();
    }

    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(ToOwned::to_owned).collect();
    }

    if let Some(labels_vec) = matches.values_of("labels") {
        let mut labels = HashMap::default();
        labels_vec
            .map(|s| {
                let mut parts = s.split('=');
                let key = parts.next().unwrap().to_owned();
                let value = match parts.next() {
                    None => fatal!("invalid label: {}", s),
                    Some(v) => v.to_owned(),
                };
                if parts.next().is_some() {
                    fatal!("invalid label: {}", s);
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

    if let Some(metrics_addr) = matches.value_of("metrics-addr") {
        config.metric.address = metrics_addr.to_owned()
    }
}
