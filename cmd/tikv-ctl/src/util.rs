// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::ToOwned, error::Error, str, str::FromStr, u64};

use server::setup::initial_logger;
use tikv::{config::TikvConfig, server::debug::RegionInfo};

const LOG_DIR: &str = "./ctl-engine-info-log";

#[allow(clippy::field_reassign_with_default)]
pub fn init_ctl_logger(level: &str) {
    let mut cfg = TikvConfig::default();
    cfg.log.level = slog::Level::from_str(level).unwrap().into();
    cfg.rocksdb.info_log_dir = LOG_DIR.to_owned();
    cfg.raftdb.info_log_dir = LOG_DIR.to_owned();
    initial_logger(&cfg);
}

pub fn warning_prompt(message: &str) -> bool {
    const EXPECTED: &str = "I consent";
    println!("{}", message);
    println!("Type \"{}\" to continue, anything else to exit", EXPECTED);
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).unwrap();
    if answer.trim_end_matches('\n') == EXPECTED {
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
    tikv_util::logger::exit_process_gracefully(-1);
}

// Check if the region is in the specified range
pub fn included_region_in_range(r: &RegionInfo, start_key: &Vec<u8>, end_key: &Vec<u8>) -> bool {
    let region = r
        .region_local_state
        .as_ref()
        .map(|s| s.get_region().clone())
        .unwrap();
    if !end_key.is_empty() && region.get_start_key() >= end_key.as_slice() {
        return false;
    }
    if start_key.as_slice() >= region.get_start_key()
        && (region.get_end_key().is_empty() || start_key.as_slice() < region.get_end_key())
    {
        return true;
    }
    false
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
