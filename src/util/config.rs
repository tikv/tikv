// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;
use std::mem;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::collections::HashMap;

use url;
use rocksdb::{DBCompressionType, DBRecoveryMode};
use regex::Regex;

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        RocksDB
        ReadableNumber
        Limit(msg: String) {
            description(&msg)
            display("{}", msg)
        }
        Address(msg: String) {
            description(&msg)
            display("{}", msg)
        }
        StoreLabels(msg: String) {
            description(&msg)
            display("{}", msg)
        }
    }
}

pub fn parse_rocksdb_compression(tp: &str) -> Result<DBCompressionType, ConfigError> {
    match &*tp.to_lowercase() {
        "no" => Ok(DBCompressionType::DBNo),
        "snappy" => Ok(DBCompressionType::DBSnappy),
        "zlib" => Ok(DBCompressionType::DBZlib),
        "bzip2" => Ok(DBCompressionType::DBBz2),
        "lz4" => Ok(DBCompressionType::DBLz4),
        "lz4hc" => Ok(DBCompressionType::DBLz4hc),
        _ => Err(ConfigError::RocksDB),
    }
}

pub fn parse_rocksdb_per_level_compression(tp: &str)
                                           -> Result<Vec<DBCompressionType>, ConfigError> {
    let mut result: Vec<DBCompressionType> = vec![];
    let v: Vec<&str> = tp.split(':').collect();
    for i in &v {
        match &*i.to_lowercase() {
            "no" => result.push(DBCompressionType::DBNo),
            "snappy" => result.push(DBCompressionType::DBSnappy),
            "zlib" => result.push(DBCompressionType::DBZlib),
            "bzip2" => result.push(DBCompressionType::DBBz2),
            "lz4" => result.push(DBCompressionType::DBLz4),
            "lz4hc" => result.push(DBCompressionType::DBLz4hc),
            _ => return Err(ConfigError::RocksDB),
        }
    }

    Ok(result)
}

pub fn parse_rocksdb_wal_recovery_mode(mode: i64) -> Result<DBRecoveryMode, ConfigError> {
    match mode {
        0 => Ok(DBRecoveryMode::TolerateCorruptedTailRecords),
        1 => Ok(DBRecoveryMode::AbsoluteConsistency),
        2 => Ok(DBRecoveryMode::PointInTime),
        3 => Ok(DBRecoveryMode::SkipAnyCorruptedRecords),
        _ => Err(ConfigError::RocksDB),
    }
}

fn split_property(property: &str) -> Result<(f64, &str), ConfigError> {
    let mut indx = 0;
    for s in property.chars() {
        match s {
            '0'...'9' | '.' => {
                indx += 1;
            }
            _ => {
                break;
            }
        }
    }

    let (num, unit) = property.split_at(indx);
    num.parse::<f64>().map(|f| (f, unit)).or(Err(ConfigError::ReadableNumber))
}

const UNIT: usize = 1;
const DATA_MAGNITUDE: usize = 1024;
const KB: usize = UNIT * DATA_MAGNITUDE;
const MB: usize = KB * DATA_MAGNITUDE;
const GB: usize = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: usize = 1000;
const TIME_MAGNITUDE_2: usize = 60;
const MS: usize = UNIT;
const SECOND: usize = MS * TIME_MAGNITUDE_1;
const MINTUE: usize = SECOND * TIME_MAGNITUDE_2;
const HOUR: usize = MINTUE * TIME_MAGNITUDE_2;

pub fn parse_readable_int(size: &str) -> Result<i64, ConfigError> {
    let (num, unit) = try!(split_property(size));

    match &*unit.to_lowercase() {
        // file size
        "kb" => Ok((num * (KB as f64)) as i64),
        "mb" => Ok((num * (MB as f64)) as i64),
        "gb" => Ok((num * (GB as f64)) as i64),
        "tb" => Ok((num * (TB as f64)) as i64),
        "pb" => Ok((num * (PB as f64)) as i64),

        // time
        "ms" => Ok((num * (MS as f64)) as i64),
        "s" => Ok((num * (SECOND as f64)) as i64),
        "m" => Ok((num * (MINTUE as f64)) as i64),
        "h" => Ok((num * (HOUR as f64)) as i64),

        _ => Err(ConfigError::ReadableNumber),
    }
}

pub fn parse_store_labels(labels: &str) -> Result<HashMap<String, String>, ConfigError> {
    let mut map = HashMap::new();

    let re = Regex::new(r"^[a-z0-9]([a-z0-9-._]*[a-z0-9])?$").unwrap();
    for label in labels.split(',') {
        if label.is_empty() {
            continue;
        }
        let label = label.to_lowercase();
        let kv: Vec<_> = label.split('=').collect();
        match kv[..] {
            [k, v] => {
                if !re.is_match(k) || !re.is_match(v) {
                    return Err(ConfigError::StoreLabels(format!("invalid label {}", label)));
                }
                if map.insert(k.to_owned(), v.to_owned()).is_some() {
                    return Err(ConfigError::StoreLabels(format!("duplicated label {}", label)));
                }
            }
            _ => {
                return Err(ConfigError::StoreLabels(format!("invalid label {}", label)));
            }
        }
    }

    Ok(map)
}

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    use libc;

    unsafe {
        let mut fd_limit = mem::zeroed();
        let mut err = libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit);
        if err != 0 {
            return Err(ConfigError::Limit("check_max_open_fds failed".to_owned()));
        }
        if fd_limit.rlim_cur >= expect {
            return Ok(());
        }

        let prev_limit = fd_limit.rlim_cur;
        fd_limit.rlim_cur = expect;
        if fd_limit.rlim_max < expect {
            // If the process is not started by privileged user, this will fail.
            fd_limit.rlim_max = expect;
        }
        err = libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limit);
        if err == 0 {
            return Ok(());
        }
        Err(ConfigError::Limit(format!("the maximum number of open file descriptors is too \
                                        small, got {}, expect greater or equal to {}",
                                       prev_limit,
                                       expect)))
    }
}

#[cfg(not(unix))]
pub fn check_max_open_fds(expect: u64) -> Result<(), ConfigError> {
    Ok(())
}

#[cfg(target_os = "linux")]
mod check_kernel {
    use std::fs;
    use std::io::Read;

    use super::ConfigError;

    // pub for tests.
    pub type Checker = Fn(i64, i64) -> bool;

    // pub for tests.
    pub fn check_kernel_params(param_path: &str,
                               expect: i64,
                               checker: Box<Checker>)
                               -> Result<(), ConfigError> {
        let mut buffer = String::new();
        try!(fs::File::open(param_path)
            .and_then(|mut f| f.read_to_string(&mut buffer))
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e))));

        let got = try!(buffer.trim_matches('\n')
            .parse::<i64>()
            .map_err(|e| ConfigError::Limit(format!("check_kernel_params failed {}", e))));

        let mut param = String::new();
        // skip 3, ["", "proc", "sys", ...]
        for path in param_path.split('/').skip(3) {
            param.push_str(path);
            param.push('.');
        }
        param.pop();

        if !checker(got, expect) {
            return Err(ConfigError::Limit(format!("kernel parameters {} got {}, expect {}",
                                                  param,
                                                  got,
                                                  expect)));
        }

        info!("kernel parameters {}: {}", param, got);
        Ok(())
    }

    /// `check_kernel_params` checks kernel parameters, following are checked so far:
    ///   - `net.core.somaxconn` should be greater or equak to 32768.
    ///   - `net.ipv4.tcp_syncookies` should be 0
    ///   - `vm.swappiness` shoud be 0
    ///
    /// Note that: It works on **Linux** only.
    pub fn check_kernel() -> Vec<ConfigError> {
        let params: Vec<(&str, i64, Box<Checker>)> = vec![
            // Check net.core.somaxconn.
            ("/proc/sys/net/core/somaxconn", 32768, box |got, expect| got >= expect),
            // Check net.ipv4.tcp_syncookies.
            ("/proc/sys/net/ipv4/tcp_syncookies", 0, box |got, expect| got == expect),
            // Check vm.swappiness.
            ("/proc/sys/vm/swappiness", 0, box |got, expect| got == expect),
        ];

        let mut errors = Vec::with_capacity(params.len());
        for (param_path, expect, checker) in params {
            if let Err(e) = check_kernel_params(param_path, expect, checker) {
                errors.push(e);
            }
        }

        errors
    }
}

#[cfg(target_os = "linux")]
pub use self::check_kernel::check_kernel;

#[cfg(not(target_os = "linux"))]
pub fn check_kernel() -> Vec<ConfigError> {
    Vec::new()
}

/// `check_addr` validates an address. Addresses are formed like "Host:Port".
/// More details about **Host** and **Port** can be found in WHATWG URL Standard.
pub fn check_addr(addr: &str) -> Result<(), ConfigError> {
    // Try to validate "IPv4:Port" and "[IPv6]:Port".
    if SocketAddrV4::from_str(addr).is_ok() {
        return Ok(());
    }
    if SocketAddrV6::from_str(addr).is_ok() {
        return Ok(());
    }

    let parts: Vec<&str> = addr.split(':')
            .filter(|s| !s.is_empty()) // "Host:" or ":Port" are invalid.
            .collect();

    // ["Host", "Port"]
    if parts.len() != 2 {
        return Err(ConfigError::Address(format!("invalid addr: {}", addr)));
    }

    // Check Port.
    let port: u16 = try!(parts[1]
        .parse()
        .map_err(|_| ConfigError::Address(format!("invalid addr, parse port failed: {}", addr))));
    // Port = 0 is invalid.
    if port == 0 {
        return Err(ConfigError::Address(format!("invalid addr, port can not be 0: {}", addr)));
    }

    // Check Host.
    if let Err(e) = url::Host::parse(parts[0]) {
        return Err(ConfigError::Address(format!("{:?}", e)));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use rocksdb::DBRecoveryMode;

    #[test]
    fn test_parse_readable_int() {
        // file size
        assert!(1_024 == parse_readable_int("1KB").unwrap());
        assert!(1_048_576 == parse_readable_int("1MB").unwrap());
        assert!(1_073_741_824 == parse_readable_int("1GB").unwrap());
        assert!(1_099_511_627_776 == parse_readable_int("1TB").unwrap());
        assert!(1_125_899_906_842_624 == parse_readable_int("1PB").unwrap());

        assert!(1_024 == parse_readable_int("1kb").unwrap());
        assert!(1_048_576 == parse_readable_int("1mb").unwrap());
        assert!(1_073_741_824 == parse_readable_int("1gb").unwrap());
        assert!(1_099_511_627_776 == parse_readable_int("1tb").unwrap());
        assert!(1_125_899_906_842_624 == parse_readable_int("1pb").unwrap());

        assert!(1_536 == parse_readable_int("1.5KB").unwrap());
        assert!(1_572_864 == parse_readable_int("1.5MB").unwrap());
        assert!(1_610_612_736 == parse_readable_int("1.5GB").unwrap());
        assert!(1_649_267_441_664 == parse_readable_int("1.5TB").unwrap());
        assert!(1_688_849_860_263_936 == parse_readable_int("1.5PB").unwrap());

        assert!(100_663_296 == parse_readable_int("96MB").unwrap());

        assert!(1_429_365_116_108 == parse_readable_int("1.3TB").unwrap());
        assert!(1_463_669_878_895_411 == parse_readable_int("1.3PB").unwrap());

        assert!(parse_readable_int("KB").is_err());
        assert!(parse_readable_int("MB").is_err());
        assert!(parse_readable_int("GB").is_err());
        assert!(parse_readable_int("TB").is_err());
        assert!(parse_readable_int("PB").is_err());

        // time
        assert!(1 == parse_readable_int("1ms").unwrap());
        assert!(1_000 == parse_readable_int("1s").unwrap());
        assert!(60_000 == parse_readable_int("1m").unwrap());
        assert!(3_600_000 == parse_readable_int("1h").unwrap());

        assert!(1 == parse_readable_int("1.3ms").unwrap());
        assert!(1_000 == parse_readable_int("1000ms").unwrap());
        assert!(1_300 == parse_readable_int("1.3s").unwrap());
        assert!(1_500 == parse_readable_int("1.5s").unwrap());
        assert!(10_000 == parse_readable_int("10s").unwrap());
        assert!(78_000 == parse_readable_int("1.3m").unwrap());
        assert!(90_000 == parse_readable_int("1.5m").unwrap());
        assert!(4_680_000 == parse_readable_int("1.3h").unwrap());
        assert!(5_400_000 == parse_readable_int("1.5h").unwrap());

        assert!(parse_readable_int("ms").is_err());
        assert!(parse_readable_int("s").is_err());
        assert!(parse_readable_int("m").is_err());
        assert!(parse_readable_int("h").is_err());

        assert!(parse_readable_int("1").is_err());
        assert!(parse_readable_int("foo").is_err());
    }

    #[test]
    fn test_parse_rocksdb_wal_recovery_mode() {
        assert!(DBRecoveryMode::TolerateCorruptedTailRecords ==
                parse_rocksdb_wal_recovery_mode(0).unwrap());
        assert!(DBRecoveryMode::AbsoluteConsistency == parse_rocksdb_wal_recovery_mode(1).unwrap());
        assert!(DBRecoveryMode::PointInTime == parse_rocksdb_wal_recovery_mode(2).unwrap());
        assert!(DBRecoveryMode::SkipAnyCorruptedRecords ==
                parse_rocksdb_wal_recovery_mode(3).unwrap());

        assert!(parse_rocksdb_wal_recovery_mode(4).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_check_kernel() {
        use std::i64;
        use super::check_kernel::{check_kernel_params, Checker};

        // The range of vm.swappiness is from 0 to 100.
        let table: Vec<(&str, i64, Box<Checker>, bool)> = vec![
            ("/proc/sys/vm/swappiness", i64::MAX, Box::new(|got, expect| got == expect), false),

            ("/proc/sys/vm/swappiness", i64::MAX, Box::new(|got, expect| got < expect), true),
        ];

        for (path, expect, checker, is_ok) in table {
            assert_eq!(check_kernel_params(path, expect, checker).is_ok(), is_ok);
        }
    }

    #[test]
    fn test_check_addrs() {
        let table = vec![
            ("127.0.0.1:8080",true),
            ("[::1]:8080",true),
            ("localhost:8080",true),
            ("pingcap.com:8080",true),
            ("funnydomain:8080",true),

            ("127.0.0.1",false),
            ("[::1]",false),
            ("localhost",false),
            ("pingcap.com",false),
            ("funnydomain",false),
            ("funnydomain:",false),

            ("root@google.com:8080",false),
            ("http://google.com:8080",false),
            ("google.com:8080/path",false),
            ("http://google.com:8080/path",false),
            ("http://google.com:8080/path?lang=en",false),
            ("http://google.com:8080/path?lang=en#top",false),

            ("ftp://ftp.is.co.za/rfc/rfc1808.txt",false),
            ("http://www.ietf.org/rfc/rfc2396.txt",false),
            ("ldap://[2001:db8::7]/c=GB?objectClass?one",false),
            ("mailto:John.Doe@example.com",false),
            ("news:comp.infosystems.www.servers.unix",false),
            ("tel:+1-816-555-1212",false),
            ("telnet://192.0.2.16:80/",false),
            ("urn:oasis:names:specification:docbook:dtd:xml:4.1.2",false),

            (":8080",false),
            ("8080",false),
            ("8080:",false),
        ];

        for (addr, is_ok) in table {
            assert_eq!(check_addr(addr).is_ok(), is_ok);
        }
    }

    #[test]
    fn test_store_labels() {
        let cases = vec![
            "abc",
            "abc=",
            "abc.012",
            "abc,012",
            "abc=123*",
            ".123=-abc",
            "abc,123=123.abc",
            "abc=123,abc=abc",
        ];

        for case in cases {
            assert!(parse_store_labels(case).is_err());
        }

        let map = parse_store_labels("").unwrap();
        assert!(map.is_empty());

        let map = parse_store_labels("a=0").unwrap();
        assert_eq!(map.get("a").unwrap(), "0");

        let map = parse_store_labels("a.1-2=b_1.2").unwrap();
        assert_eq!(map.get("a.1-2").unwrap(), "b_1.2");

        let map = parse_store_labels("a.1-2=b_1.2,cab-012=3ac.8b2").unwrap();
        assert_eq!(map.get("a.1-2").unwrap(), "b_1.2");
        assert_eq!(map.get("cab-012").unwrap(), "3ac.8b2");

        let map = parse_store_labels("zone=us-west-1,disk=ssd,Test=Test").unwrap();
        assert_eq!(map.get("zone").unwrap(), "us-west-1");
        assert_eq!(map.get("disk").unwrap(), "ssd");
        assert_eq!(map.get("test").unwrap(), "test");
    }
}
