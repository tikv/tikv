// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use rocksdb::{DBInfoLogLevel as InfoLogLevel, Logger};

// TODO(yiwu): abstract the Logger interface.
#[derive(Default)]
pub struct RocksdbLogger;

impl Logger for RocksdbLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Debug => debug!(#"rocksdb_log","{}", log),
            InfoLogLevel::Info | InfoLogLevel::Header | InfoLogLevel::NumInfoLog => {
                info!(#"rocksdb_log","{}", log)
            }
            InfoLogLevel::Warn => warn!(#"rocksdb_log","{}", log),
            InfoLogLevel::Error => error!(#"rocksdb_log","{}", log),
            InfoLogLevel::Fatal => crit!(#"rocksdb_log","{}", log),
        }
    }
}

#[derive(Default)]
pub struct RaftDBLogger;

impl Logger for RaftDBLogger {
    fn logv(&self, log_level: InfoLogLevel, log: &str) {
        match log_level {
            InfoLogLevel::Debug => debug!(#"raftdb_log","{}", log),
            InfoLogLevel::Info | InfoLogLevel::Header | InfoLogLevel::NumInfoLog => {
                info!(#"raft_log","{}", log)
            }
            InfoLogLevel::Warn => warn!(#"raftdb_log","{}", log),
            InfoLogLevel::Error => error!(#"raftdb_log","{}", log),
            InfoLogLevel::Fatal => crit!(#"raftdb_log","{}", log),
        }
    }
}
