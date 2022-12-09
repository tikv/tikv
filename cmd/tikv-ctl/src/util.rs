// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::ToOwned, error::Error, str, str::FromStr, u64};

use kvproto::kvrpcpb::KeyRange;
use server::setup::initial_logger;
use tikv::config::TikvConfig;

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

// Check if region's `key_range` intersects with `key_range_limit`.
pub fn check_intersect_of_range(key_range: &KeyRange, key_range_limit: &KeyRange) -> bool {
    if !key_range.get_end_key().is_empty()
        && !key_range_limit.get_start_key().is_empty()
        && key_range.get_end_key() <= key_range_limit.get_start_key()
    {
        return false;
    }
    if !key_range_limit.get_end_key().is_empty()
        && !key_range.get_start_key().is_empty()
        && key_range_limit.get_end_key() < key_range.get_start_key()
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use raftstore::store::util::build_key_range;

    use super::*;

    #[test]
    fn test_from_hex() {
        let result = vec![0x74];
        assert_eq!(from_hex("74").unwrap(), result);
        assert_eq!(from_hex("0x74").unwrap(), result);
        assert_eq!(from_hex("0X74").unwrap(), result);
    }

    #[test]
    fn test_included_region_in_range() {
        // To avoid unfolding the code when `make format` is called
        fn range(start: &[u8], end: &[u8]) -> KeyRange {
            build_key_range(start, end, false)
        }
        let mut region = range(&[0x02], &[0x05]);
        // region absolutely in range
        assert!(check_intersect_of_range(&region, &range(&[0x02], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[0x01], &[])));
        assert!(check_intersect_of_range(&region, &range(&[0x02], &[])));
        assert!(check_intersect_of_range(&region, &range(&[], &[])));
        assert!(check_intersect_of_range(&region, &range(&[0x02], &[0x06])));
        assert!(check_intersect_of_range(&region, &range(&[0x01], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[], &[0x05])));
        // region intersects with range
        assert!(check_intersect_of_range(&region, &range(&[0x04], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[0x04], &[])));
        assert!(check_intersect_of_range(&region, &range(&[0x01], &[0x03])));
        assert!(check_intersect_of_range(&region, &range(&[], &[0x03])));
        assert!(check_intersect_of_range(&region, &range(&[], &[0x02]))); // region is left-closed and right-open interval
        // range absolutely in region also need to return true
        assert!(check_intersect_of_range(&region, &range(&[0x03], &[0x04])));
        // region not intersects with range
        assert!(!check_intersect_of_range(&region, &range(&[0x05], &[]))); // region is left-closed and right-open interval
        assert!(!check_intersect_of_range(&region, &range(&[0x06], &[])));
        assert!(!check_intersect_of_range(&region, &range(&[], &[0x01])));
        // check last region
        region = range(&[0x02], &[]);
        assert!(check_intersect_of_range(&region, &range(&[0x02], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[0x02], &[])));
        assert!(check_intersect_of_range(&region, &range(&[0x01], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[], &[0x05])));
        assert!(check_intersect_of_range(&region, &range(&[], &[0x02])));
        assert!(check_intersect_of_range(&region, &range(&[], &[])));
        assert!(!check_intersect_of_range(&region, &range(&[], &[0x01])));
    }
}
