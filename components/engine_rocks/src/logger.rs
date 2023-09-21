// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use rocksdb::{DBInfoLogLevel as InfoLogLevel, Logger};
use tikv_util::{crit, debug, error, info, warn};

// TODO(yiwu): abstract the Logger interface.
#[derive(Default)]
pub struct RocksdbLogger;

impl Logger for RocksdbLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Header => info!(#"rocksdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"rocksdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"rocksdb_log", "{}", log),
            _ => {}
        }
    }
}

pub struct TabletLogger {
    tablet_name: String,
}

impl TabletLogger {
    pub fn new(tablet_name: String) -> Self {
        Self { tablet_name }
    }
}

impl Logger for TabletLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Header => info!(#"rocksdb_log_header", "[{}]{}", self.tablet_name, log),
            InfoLogLevel::Debug => debug!(#"rocksdb_log", "[{}]{}", self.tablet_name, log),
            InfoLogLevel::Info => info!(#"rocksdb_log", "[{}]{}", self.tablet_name, log),
            InfoLogLevel::Warn => warn!(#"rocksdb_log", "[{}]{}", self.tablet_name, log),
            InfoLogLevel::Error => error!(#"rocksdb_log", "[{}]{}", self.tablet_name, log),
            InfoLogLevel::Fatal => crit!(#"rocksdb_log", "[{}]{}", self.tablet_name, log),
            _ => {}
        }
    }
}

#[derive(Default)]
pub struct RaftDbLogger;

impl Logger for RaftDbLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Header => info!(#"raftdb_log_header", "{}", log),
            InfoLogLevel::Debug => debug!(#"raftdb_log", "{}", log),
            InfoLogLevel::Info => info!(#"raftdb_log", "{}", log),
            InfoLogLevel::Warn => warn!(#"raftdb_log", "{}", log),
            InfoLogLevel::Error => error!(#"raftdb_log", "{}", log),
            InfoLogLevel::Fatal => crit!(#"raftdb_log", "{}", log),
            _ => {}
        }
    }
}
