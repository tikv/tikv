// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::{crit, debug, error, info, warn};
use tirocks::env::logger::{LogLevel, Logger};

pub struct RocksDbLogger;

impl Logger for RocksDbLogger {
    #[inline]
    fn logv(&self, log_level: LogLevel, data: &[u8]) {
        match log_level {
            LogLevel::HEADER_LEVEL => {
                info!(#"rocksdb_log_header", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::DEBUG_LEVEL => {
                debug!(#"rocksdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::INFO_LEVEL => {
                info!(#"rocksdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::WARN_LEVEL => {
                warn!(#"rocksdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::ERROR_LEVEL => {
                error!(#"rocksdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::FATAL_LEVEL => {
                crit!(#"rocksdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::NUM_INFO_LOG_LEVELS => (),
        }
    }
}

pub struct RaftDbLogger;

impl Logger for RaftDbLogger {
    #[inline]
    fn logv(&self, log_level: LogLevel, data: &[u8]) {
        match log_level {
            LogLevel::HEADER_LEVEL => {
                info!(#"raftdb_log_header", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::DEBUG_LEVEL => {
                debug!(#"raftdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::INFO_LEVEL => {
                info!(#"raftdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::WARN_LEVEL => {
                warn!(#"raftdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::ERROR_LEVEL => {
                error!(#"raftdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::FATAL_LEVEL => {
                crit!(#"raftdb_log", "{}", String::from_utf8_lossy(data));
            }
            LogLevel::NUM_INFO_LOG_LEVELS => (),
        }
    }
}
