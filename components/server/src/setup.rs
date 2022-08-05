// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::ToOwned,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use chrono::Local;
use clap::ArgMatches;
use collections::HashMap;
use tikv::config::{check_critical_config, persist_config, MetricConfig, TiKvConfig};
use tikv_util::{self, config, logger};

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

// The info log file names does not end with ".log" since it conflict with rocksdb WAL files.
pub const DEFAULT_ROCKSDB_LOG_FILE: &str = "rocksdb.info";
pub const DEFAULT_RAFTDB_LOG_FILE: &str = "raftdb.info";

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
//
// The file name format after rotated is as follows: "{original name}.{"%Y-%m-%dT%H-%M-%S%.3f"}"
fn rename_by_timestamp(path: &Path) -> io::Result<PathBuf> {
    let mut new_path = path.parent().unwrap().to_path_buf();
    let mut new_fname = path.file_stem().unwrap().to_os_string();
    let dt = Local::now().format("%Y-%m-%dT%H-%M-%S%.3f");
    new_fname.push(format!("-{}", dt));
    if let Some(ext) = path.extension() {
        new_fname.push(".");
        new_fname.push(ext);
    };
    new_path.push(new_fname);
    Ok(new_path)
}

fn make_engine_log_path(path: &str, sub_path: &str, filename: &str) -> String {
    let mut path = Path::new(path).to_path_buf();
    if !sub_path.is_empty() {
        path = path.join(Path::new(sub_path));
    }
    let path = path.to_str().unwrap_or_else(|| {
        fatal!(
            "failed to construct engine log dir {:?}, {:?}",
            path,
            sub_path
        );
    });
    config::ensure_dir_exist(path).unwrap_or_else(|e| {
        fatal!("failed to create engine log dir: {}", e);
    });
    config::canonicalize_log_dir(path, filename).unwrap_or_else(|e| {
        fatal!("failed to canonicalize engine log dir {:?}: {}", path, e);
    })
}

#[allow(dead_code)]
pub fn initial_logger(config: &TiKvConfig) {
    let rocksdb_info_log_path = if !config.rocksdb.info_log_dir.is_empty() {
        make_engine_log_path(&config.rocksdb.info_log_dir, "", DEFAULT_ROCKSDB_LOG_FILE)
    } else {
        // Don't use `DEFAULT_ROCKSDB_SUB_DIR`, because of the logic of `RocksEngine::exists`.
        make_engine_log_path(&config.storage.data_dir, "", DEFAULT_ROCKSDB_LOG_FILE)
    };
    let raftdb_info_log_path = if !config.raftdb.info_log_dir.is_empty() {
        make_engine_log_path(&config.raftdb.info_log_dir, "", DEFAULT_RAFTDB_LOG_FILE)
    } else {
        make_engine_log_path(&config.storage.data_dir, "", DEFAULT_RAFTDB_LOG_FILE)
    };
    let rocksdb = logger::file_writer(
        &rocksdb_info_log_path,
        config.log.file.max_size,
        config.log.file.max_backups,
        config.log.file.max_days,
        rename_by_timestamp,
    )
    .unwrap_or_else(|e| {
        fatal!(
            "failed to initialize rocksdb log with file {}: {}",
            rocksdb_info_log_path,
            e
        );
    });

    let raftdb = logger::file_writer(
        &raftdb_info_log_path,
        config.log.file.max_size,
        config.log.file.max_backups,
        config.log.file.max_days,
        rename_by_timestamp,
    )
    .unwrap_or_else(|e| {
        fatal!(
            "failed to initialize raftdb log with file {}: {}",
            raftdb_info_log_path,
            e
        );
    });

    let slow_log_writer = if config.slow_log_file.is_empty() {
        None
    } else {
        let slow_log_writer = logger::file_writer(
            &config.slow_log_file,
            config.log.file.max_size,
            config.log.file.max_backups,
            config.log.file.max_days,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize slow-log with file {}: {}",
                config.slow_log_file,
                e
            );
        });
        Some(slow_log_writer)
    };

    fn build_logger_with_slow_log<N, R, S, T>(
        normal: N,
        rocksdb: R,
        raftdb: T,
        slow: Option<S>,
        config: &TiKvConfig,
    ) where
        N: slog::Drain<Ok = (), Err = io::Error> + Send + 'static,
        R: slog::Drain<Ok = (), Err = io::Error> + Send + 'static,
        S: slog::Drain<Ok = (), Err = io::Error> + Send + 'static,
        T: slog::Drain<Ok = (), Err = io::Error> + Send + 'static,
    {
        // Use async drainer and init std log.
        let drainer = logger::LogDispatcher::new(normal, rocksdb, raftdb, slow);
        let level = config.log.level;
        let slow_threshold = config.slow_log_threshold.as_millis();
        logger::init_log(drainer, level, true, true, vec![], slow_threshold).unwrap_or_else(|e| {
            fatal!("failed to initialize log: {}", e);
        });
    }

    macro_rules! do_build {
        ($log:expr, $rocksdb:expr, $raftdb:expr, $slow:expr, $enable_timestamp:expr) => {
            match config.log.format {
                config::LogFormat::Text => build_logger_with_slow_log(
                    logger::text_format($log, $enable_timestamp),
                    logger::rocks_text_format($rocksdb, $enable_timestamp),
                    logger::rocks_text_format($raftdb, $enable_timestamp),
                    $slow.map(logger::slow_log_text_format),
                    config,
                ),
                config::LogFormat::Json => build_logger_with_slow_log(
                    logger::json_format($log, $enable_timestamp),
                    logger::json_format($rocksdb, $enable_timestamp),
                    logger::json_format($raftdb, $enable_timestamp),
                    $slow.map(logger::slow_log_json_format),
                    config,
                ),
            }
        };
    }

    if config.log.file.filename.is_empty() {
        let log = logger::term_writer();
        do_build!(
            log,
            rocksdb,
            raftdb,
            slow_log_writer,
            config.log.enable_timestamp
        );
    } else {
        let log = logger::file_writer(
            &config.log.file.filename,
            config.log.file.max_size,
            config.log.file.max_backups,
            config.log.file.max_days,
            rename_by_timestamp,
        )
        .unwrap_or_else(|e| {
            fatal!(
                "failed to initialize log with file {}: {}",
                config.log.file.filename,
                e
            );
        });
        do_build!(
            log,
            rocksdb,
            raftdb,
            slow_log_writer,
            config.log.enable_timestamp
        );
    }

    // Set redact_info_log.
    let redact_info_log = config.security.redact_info_log.unwrap_or(false);
    log_wrappers::set_redact_info_log(redact_info_log);

    LOG_INITIALIZED.store(true, Ordering::SeqCst);
}

#[allow(dead_code)]
pub fn initial_metric(cfg: &MetricConfig) {
    tikv_util::metrics::monitor_process()
        .unwrap_or_else(|e| fatal!("failed to start process monitor: {}", e));
    tikv_util::metrics::monitor_threads("tikv")
        .unwrap_or_else(|e| fatal!("failed to start thread monitor: {}", e));
    tikv_util::metrics::monitor_allocator_stats("tikv")
        .unwrap_or_else(|e| fatal!("failed to monitor allocator stats: {}", e));

    if cfg.interval.as_secs() == 0 || cfg.address.is_empty() {
        return;
    }

    warn!("metrics push is not supported any more.");
}

#[allow(dead_code)]
pub fn overwrite_config_with_cmd_args(config: &mut TiKvConfig, matches: &ArgMatches<'_>) {
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

#[allow(dead_code)]
pub fn validate_and_persist_config(config: &mut TiKvConfig, persist: bool) {
    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e);
    }

    if let Err(e) = check_critical_config(config) {
        fatal!("critical config check failed: {}", e);
    }

    if persist {
        if let Err(e) = persist_config(config) {
            fatal!("persist critical config failed: {}", e);
        }
    }
}

pub fn ensure_no_unrecognized_config(unrecognized_keys: &[String]) {
    if !unrecognized_keys.is_empty() {
        fatal!(
            "unknown configuration options: {}",
            unrecognized_keys.join(", ")
        );
    }
}
