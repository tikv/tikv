// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::ToOwned;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Local;
use clap::ArgMatches;
use tikv::config::{check_critical_config, persist_config, MetricConfig, TiKvConfig};
use tikv_util::collections::HashMap;
use tikv_util::{self, logger};

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

#[macro_export]
macro_rules! fatal {
    ($lvl:expr $(, $arg:expr)*) => ({
        if $crate::setup::LOG_INITIALIZED.load(::std::sync::atomic::Ordering::SeqCst) {
            crit!($lvl $(, $arg)*);
        } else {
            eprintln!($lvl $(, $arg)*);
        }
        slog_global::clear_global();
        ::std::process::exit(1)
    })
}

// TODO: There is a very small chance that duplicate files will be generated if there are
// a lot of logs written in a very short time. Consider rename the rotated file with a version
// number while rotate by size.
fn rename_by_timestamp(path: &Path) -> io::Result<PathBuf> {
    let mut new_path = path.to_path_buf().into_os_string();
    new_path.push(format!(
        ".{}",
        Local::now().format(logger::DATETIME_ROTATE_SUFFIX)
    ));
    Ok(PathBuf::from(new_path))
}

#[allow(dead_code)]
pub fn initial_logger(config: &TiKvConfig) {
    if config.log_file.is_empty() {
        let drainer = logger::term_drainer();
        // use async drainer and init std log.
        logger::init_log(
            drainer,
            config.log_level,
            true,
            true,
            vec![],
            config.slow_log_threshold.as_millis(),
        )
        .unwrap_or_else(|e| {
            fatal!("failed to initialize log: {}", e);
        });
    } else {
        let drainer = logger::file_drainer(
            &config.log_file,
            config.log_rotation_timespan,
            config.log_rotation_size,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize log with file {}: {}",
                config.log_file,
                e
            );
        });
        if config.slow_log_file.is_empty() {
            logger::init_log(
                drainer,
                config.log_level,
                true,
                true,
                vec![],
                config.slow_log_threshold.as_millis(),
            )
            .unwrap_or_else(|e| {
                fatal!("failed to initialize log: {}", e);
            });
        } else {
            let slow_log_drainer = logger::file_drainer(
                &config.slow_log_file,
                config.log_rotation_timespan,
                config.log_rotation_size,
                rename_by_timestamp,
            )
            .unwrap_or_else(|e| {
                fatal!(
                    "failed to initialize log with file {}: {}",
                    config.slow_log_file,
                    e
                );
            });
            let drainer = logger::LogDispatcher::new(drainer, slow_log_drainer);
            logger::init_log(
                drainer,
                config.log_level,
                true,
                true,
                vec![],
                config.slow_log_threshold.as_millis(),
            )
            .unwrap_or_else(|e| {
                fatal!("failed to initialize log: {}", e);
            });
        };
    };
    LOG_INITIALIZED.store(true, Ordering::SeqCst);
}

#[allow(dead_code)]
pub fn initial_metric(cfg: &MetricConfig, node_id: Option<u64>) {
    tikv_util::metrics::monitor_memory()
        .unwrap_or_else(|e| fatal!("failed to start memory monitor: {}", e));
    tikv_util::metrics::monitor_threads("tikv")
        .unwrap_or_else(|e| fatal!("failed to start thread monitor: {}", e));
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

    if let Some(metrics_addr) = matches.value_of("metrics-addr") {
        config.metric.address = metrics_addr.to_owned()
    }
}

#[allow(dead_code)]
pub fn validate_and_persist_config(config: &mut TiKvConfig, persist: bool) {
    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e.description());
    }

    if let Err(e) = check_critical_config(config) {
        fatal!("critical config check failed: {}", e);
    }

    if persist {
        if let Err(e) = persist_config(&config) {
            fatal!("persist critical config failed: {}", e);
        }
    }
}
