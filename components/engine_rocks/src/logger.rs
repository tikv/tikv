// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use rocksdb::{DBInfoLogLevel as InfoLogLevel, Logger};

#[derive(Default)]
pub struct RocksdbLogger();

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
