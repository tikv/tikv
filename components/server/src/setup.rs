// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::ToOwned,
    io,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use chrono::Local;
use clap::ArgMatches;
use collections::HashMap;
use fail;
use health_controller::slow_score::NETWORK_TIMEOUT_THRESHOLD;
use tikv::{
    config::{MetricConfig, TikvConfig},
    server::metrics::ADVERTISE_ADDR_PROBE_FAILURE_COUNTER,
};
use tikv_util::{self, config, logger, sys::thread::StdThreadBuildWrapper};

// A workaround for checking if log is initialized.
pub static LOG_INITIALIZED: AtomicBool = AtomicBool::new(false);

// The info log file names does not end with ".log" since it conflict with
// rocksdb WAL files.
pub const DEFAULT_ROCKSDB_LOG_FILE: &str = "rocksdb.info";
pub const DEFAULT_RAFTDB_LOG_FILE: &str = "raftdb.info";

// Keep this advisory lookup aligned with TiKV's network inspection threshold.
// The budget applies to each endpoint and does not cancel the system resolver.
const ADVERTISE_ADDR_PROBE_TIMEOUT: Duration = NETWORK_TIMEOUT_THRESHOLD;

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

// TODO: There is a very small chance that duplicate files will be generated if
// there are a lot of logs written in a very short time. Consider rename the
// rotated file with a version number while rotate by size.
//
// The file name format after rotated is as follows:
// "{original name}.{"%Y-%m-%dT%H-%M-%S%.3f"}"
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

pub fn initial_logger(config: &TikvConfig) {
    fail::fail_point!("mock_force_uninitial_logger", |_| {
        LOG_INITIALIZED.store(false, Ordering::SeqCst);
    });
    let rocksdb_info_log_path = if !config.rocksdb.info_log_dir.is_empty() {
        make_engine_log_path(&config.rocksdb.info_log_dir, "", DEFAULT_ROCKSDB_LOG_FILE)
    } else {
        // Don't use `DEFAULT_ROCKSDB_SUB_DIR`, because of the logic of
        // `RocksEngine::exists`.
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
        config: &TikvConfig,
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
        logger::init_log(drainer, level.into(), true, true, vec![], slow_threshold).unwrap_or_else(
            |e| {
                fatal!("failed to initialize log: {}", e);
            },
        );
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
    log_wrappers::set_redact_info_log(config.security.redact_info_log.clone());

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
pub fn overwrite_config_with_cmd_args(config: &mut TikvConfig, matches: &ArgMatches<'_>) {
    if let Some(level) = matches.value_of("log-level") {
        config.log.level = logger::get_level_by_string(level).unwrap().into();
    }

    if let Some(file) = matches.value_of("log-file") {
        config.log.file.filename = file.to_owned();
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

pub fn validate_and_persist_config(config: &mut TikvConfig, persist: bool) {
    if let Err(e) = tikv::config::validate_and_persist_config(config, persist) {
        fatal!("failed to validate config: {}", e);
    }
}

#[derive(Debug, PartialEq)]
enum AdvertiseAddrProbe {
    Resolved,
    Loopback(SocketAddr),
    Unresolved(String),
    TimedOut,
}

fn classify_advertise_addr(addrs: io::Result<Vec<SocketAddr>>) -> AdvertiseAddrProbe {
    match addrs {
        Ok(addrs) => {
            let mut resolved = false;
            for addr in addrs {
                resolved = true;
                let is_loopback = match addr.ip() {
                    IpAddr::V4(ip) => ip.is_loopback(),
                    IpAddr::V6(ip) => ip
                        .to_ipv4_mapped()
                        .map_or_else(|| ip.is_loopback(), |ip| ip.is_loopback()),
                };
                if is_loopback {
                    return AdvertiseAddrProbe::Loopback(addr);
                }
            }
            if resolved {
                AdvertiseAddrProbe::Resolved
            } else {
                AdvertiseAddrProbe::Unresolved("DNS returned no addresses".to_owned())
            }
        }
        Err(err) => AdvertiseAddrProbe::Unresolved(err.to_string()),
    }
}

fn probe_advertise_addr_with_resolver<R>(
    addr: &str,
    timeout: Duration,
    resolver: R,
) -> AdvertiseAddrProbe
where
    R: FnOnce(String) -> io::Result<Vec<SocketAddr>> + Send + 'static,
{
    // Numeric addresses do not need system name resolution.
    if let Ok(addr) = addr.parse::<SocketAddr>() {
        return classify_advertise_addr(Ok(vec![addr]));
    }

    // System name resolution cannot be cancelled. If this wait times out, the
    // resolver thread may continue until the operating system returns.
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let addr = addr.to_owned();
    let spawn_result = thread::Builder::new()
        .name("addr-probe".to_owned())
        .spawn_wrapper(move || {
            let _ = result_tx.send(resolver(addr));
        });
    if let Err(err) = spawn_result {
        return AdvertiseAddrProbe::Unresolved(format!(
            "failed to start address resolver thread: {}",
            err
        ));
    }

    match result_rx.recv_timeout(timeout) {
        Ok(addrs) => classify_advertise_addr(addrs),
        Err(RecvTimeoutError::Timeout) => AdvertiseAddrProbe::TimedOut,
        Err(RecvTimeoutError::Disconnected) => AdvertiseAddrProbe::Unresolved(
            "address resolver thread exited without a result".to_owned(),
        ),
    }
}

fn probe_advertise_addr(addr: &str) -> AdvertiseAddrProbe {
    probe_advertise_addr_with_resolver(addr, ADVERTISE_ADDR_PROBE_TIMEOUT, |addr| {
        addr.to_socket_addrs()
            .map(|resolved_addrs| resolved_addrs.collect())
    })
}

fn report_advertise_addr_probe_result(endpoint: &str, addr: &str, result: AdvertiseAddrProbe) {
    match result {
        AdvertiseAddrProbe::Resolved => {}
        AdvertiseAddrProbe::Loopback(resolved_addr) => {
            ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                .with_label_values(&[endpoint, "loopback"])
                .inc();
            warn!(
                "advertised address resolves to a loopback address; remote cluster components \
                cannot reach this TiKV endpoint through that address";
                "endpoint" => endpoint,
                "advertise_addr" => addr,
                "resolved_addr" => %resolved_addr,
            );
        }
        AdvertiseAddrProbe::Unresolved(err) => {
            ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                .with_label_values(&[endpoint, "unresolved"])
                .inc();
            warn!(
                "failed to resolve advertised address; cluster components may be unable to \
                reach this TiKV endpoint";
                "endpoint" => endpoint,
                "advertise_addr" => addr,
                "err" => %err,
            );
        }
        AdvertiseAddrProbe::TimedOut => {
            ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                .with_label_values(&[endpoint, "timeout"])
                .inc();
            warn!(
                "timed out resolving advertised address; cluster components may be unable to \
                reach this TiKV endpoint";
                "endpoint" => endpoint,
                "advertise_addr" => addr,
                "timeout" => ?ADVERTISE_ADDR_PROBE_TIMEOUT,
            );
        }
    }
}

fn report_advertise_addr_probe(endpoint: &str, addr: &str) {
    report_advertise_addr_probe_result(endpoint, addr, probe_advertise_addr(addr));
}

/// Probes and reports the advertised addresses of the live server config.
pub(crate) fn report_advertise_addr_probe_failures(config: &TikvConfig) {
    report_advertise_addr_probe("store", &config.server.advertise_addr);
    if !config.server.advertise_status_addr.is_empty() {
        report_advertise_addr_probe("status", &config.server.advertise_status_addr);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_advertise_addr() {
        assert!(matches!(
            probe_advertise_addr("127.0.0.1:20160"),
            AdvertiseAddrProbe::Loopback(_)
        ));
        assert!(matches!(
            probe_advertise_addr("[::1]:20160"),
            AdvertiseAddrProbe::Loopback(_)
        ));
        assert!(matches!(
            probe_advertise_addr("[::ffff:127.0.0.1]:20160"),
            AdvertiseAddrProbe::Loopback(_)
        ));
        assert_eq!(
            probe_advertise_addr("192.0.2.1:20160"),
            AdvertiseAddrProbe::Resolved
        );

        let numeric = probe_advertise_addr_with_resolver(
            "192.0.2.1:20160",
            Duration::ZERO,
            |_| -> io::Result<Vec<SocketAddr>> {
                panic!("numeric addresses must not invoke the resolver")
            },
        );
        assert_eq!(numeric, AdvertiseAddrProbe::Resolved);

        let resolved = probe_advertise_addr_with_resolver(
            "store.example.com:20160",
            Duration::from_secs(1),
            |_| Ok(vec!["192.0.2.1:20160".parse().unwrap()]),
        );
        assert_eq!(resolved, AdvertiseAddrProbe::Resolved);

        assert!(matches!(
            probe_advertise_addr_with_resolver(
                "unresolvable.invalid:20160",
                Duration::from_secs(1),
                |_| Err(io::Error::other("injected resolution failure")),
            ),
            AdvertiseAddrProbe::Unresolved(_)
        ));
    }

    #[test]
    fn test_probe_advertise_addr_timeout() {
        let result = probe_advertise_addr_with_resolver(
            "slow.example.com:20160",
            Duration::from_millis(10),
            |_| {
                thread::sleep(Duration::from_millis(100));
                Ok(vec!["192.0.2.1:20160".parse().unwrap()])
            },
        );
        assert_eq!(result, AdvertiseAddrProbe::TimedOut);
    }

    #[test]
    fn test_report_advertise_addr_probe_failures() {
        let labels = [
            ["store", "loopback"],
            ["store", "unresolved"],
            ["store", "timeout"],
            ["status", "loopback"],
            ["status", "unresolved"],
            ["status", "timeout"],
        ];
        let before = labels.map(|labels| {
            ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                .with_label_values(&labels)
                .get()
        });

        let mut status_disabled = TikvConfig::default();
        status_disabled.server.advertise_addr = "192.0.2.1:20160".to_owned();
        status_disabled.server.advertise_status_addr.clear();
        report_advertise_addr_probe_failures(&status_disabled);
        for (labels, before) in labels.iter().zip(before) {
            if labels[0] == "status" {
                assert_eq!(
                    ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                        .with_label_values(labels)
                        .get(),
                    before
                );
            }
        }

        let mut loopback = TikvConfig::default();
        loopback.server.advertise_addr = "127.0.0.1:20160".to_owned();
        loopback.server.advertise_status_addr = "[::ffff:127.0.0.1]:20180".to_owned();
        report_advertise_addr_probe_failures(&loopback);

        let mut unresolved = TikvConfig::default();
        unresolved.server.advertise_addr = "invalid-store-address".to_owned();
        unresolved.server.advertise_status_addr = "invalid-status-address".to_owned();
        report_advertise_addr_probe_failures(&unresolved);

        report_advertise_addr_probe_result(
            "store",
            "slow-store.example.com:20160",
            AdvertiseAddrProbe::TimedOut,
        );
        report_advertise_addr_probe_result(
            "status",
            "slow-status.example.com:20180",
            AdvertiseAddrProbe::TimedOut,
        );

        for (labels, before) in labels.iter().zip(before) {
            assert!(
                ADVERTISE_ADDR_PROBE_FAILURE_COUNTER
                    .with_label_values(labels)
                    .get()
                    > before
            );
        }
    }
}
