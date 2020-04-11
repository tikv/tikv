// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use libc;
use rocksdb::{DBInfoLogLevel as InfoLogLevel, Logger};
use std::ffi::VaList;
use std::mem;

#[derive(Default)]
pub struct RocksdbLogger();

impl Logger for RocksdbLogger {
    fn logv(&self, log_level: InfoLogLevel, format: &str, ap: VaList) {
        const BUF_SIZE: usize = 1024;
        let mut buffer = Vec::<u8>::with_capacity(BUF_SIZE);
        let buffer_ptr = buffer.as_mut_ptr();
        unsafe {
            // We're passing the buffer to C, so let's make
            // Rust forget about it for a while.
            mem::forget(buffer);

            let bytes_written = libc::snprintf(
                buffer_ptr as *mut libc::c_char,
                BUF_SIZE as libc::size_t,
                format.as_ptr() as *const i8,
                ap,
            ) as usize;

            let buffer = Vec::from_raw_parts(buffer_ptr, bytes_written, BUF_SIZE);
            let log = String::from_utf8_lossy(&buffer).into_owned();
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
}
