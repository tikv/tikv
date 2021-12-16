// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use server::setup::initial_logger;
use std::borrow::ToOwned;
use std::error::Error;
use std::str::FromStr;
use std::{process, str, u64};

use tikv::config::TiKvConfig;

const LOG_DIR: &str = "./ctl-engine-info-log";

pub fn init_ctl_logger(level: &str) {
    let mut cfg = TiKvConfig::default();
    cfg.log_level = slog::Level::from_str(level).unwrap();
    cfg.rocksdb.info_log_dir = LOG_DIR.to_owned();
    cfg.raftdb.info_log_dir = LOG_DIR.to_owned();
    initial_logger(&cfg);
}

pub fn warning_prompt(message: &str) -> bool {
    const EXPECTED: &str = "I consent";
    println!("{}", message);
    let input: String = promptly::prompt(format!(
        "Type \"{}\" to continue, anything else to exit",
        EXPECTED
    ))
    .unwrap();
    if input == EXPECTED {
        true
    } else {
        println!("exit.");
        false
    }
}

pub fn from_hex(key: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if key.starts_with("0x") || key.starts_with("0X") {
        return hex::decode(&key[2..]);
    }
    hex::decode(key)
}

pub fn convert_gbmb(mut bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes < MIB {
        return format!("{}B", bytes);
    }
    let mb = if bytes % GIB == 0 {
        String::from("")
    } else {
        format!("{:.3}MiB", (bytes % GIB) as f64 / MIB as f64)
    };
    bytes /= GIB;
    let gb = if bytes == 0 {
        String::from("")
    } else {
        format!("{}GiB ", bytes)
    };
    format!("{}{}", gb, mb)
}

pub fn perror_and_exit<E: Error>(prefix: &str, e: E) -> ! {
    println!("{}: {}", prefix, e);
    process::exit(-1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_hex() {
        let result = vec![0x74];
        assert_eq!(from_hex("74").unwrap(), result);
        assert_eq!(from_hex("0x74").unwrap(), result);
        assert_eq!(from_hex("0X74").unwrap(), result);
    }
}
