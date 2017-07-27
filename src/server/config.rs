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

use std::ascii::AsciiExt;
use std::cmp;

use util::collections::HashMap;
use util::config::{self, ReadableSize, ReadableDuration};

use super::Result;

pub use raftstore::store::Config as RaftStoreConfig;
pub use storage::Config as StorageConfig;

pub const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &'static str = "";
const DEFAULT_NOTIFY_CAPACITY: usize = 40960;
const DEFAULT_END_POINT_CONCURRENCY: usize = 8;
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 10;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: u64 = 2 * 1024 * 1024;
const DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO: f64 = 0.25;
const DEFAULT_END_POINT_SMALL_TXN_TASKS_LIMIT: usize = 2;
const DEFAULT_MESSAGES_PER_TICK: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(skip)]
    pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    // Server labels to specify some attributes about this server.
    #[serde(with = "config::order_map_serde")]
    pub labels: HashMap<String, String>,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    pub advertise_addr: String,
    pub notify_capacity: usize,
    pub messages_per_tick: usize,
    pub grpc_concurrency: usize,
    pub grpc_concurrent_stream: usize,
    pub grpc_raft_conn_num: usize,
    pub grpc_stream_initial_window_size: ReadableSize,
    pub end_point_concurrency: usize,
    pub end_point_txn_concurrency_on_busy: usize,
    pub end_point_small_txn_tasks_limit: usize,
}

impl Default for Config {
    fn default() -> Config {
        let concurrency = (DEFAULT_END_POINT_CONCURRENCY as f64 * DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO) as usize;
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            labels: HashMap::default(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
            messages_per_tick: DEFAULT_MESSAGES_PER_TICK,
            grpc_concurrency: DEFAULT_GRPC_CONCURRENCY,
            grpc_concurrent_stream: DEFAULT_GRPC_CONCURRENT_STREAM,
            grpc_raft_conn_num: DEFAULT_GRPC_RAFT_CONN_NUM,
            grpc_stream_initial_window_size: ReadableSize(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE),
            end_point_concurrency: DEFAULT_END_POINT_CONCURRENCY,
            end_point_txn_concurrency_on_busy: cmp::max(concurrency, 1),
            end_point_small_txn_tasks_limit: DEFAULT_END_POINT_SMALL_TXN_TASKS_LIMIT,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        box_try!(config::check_addr(&self.addr));
        if !self.advertise_addr.is_empty() {
            box_try!(config::check_addr(&self.advertise_addr));
            if self.advertise_addr.starts_with("0.") {
                return Err(box_err!("invalid advertise-addr: {:?}", self.advertise_addr));
            }
        }
        
        if self.end_point_concurrency == 0 {
            return Err(box_err!("server.server.end-point-concurrency: {} is invalid, \
                                 shouldn't be 0",
                                self.end_point_concurrency));
        }

        if self.end_point_txn_concurrency_on_busy > self.end_point_concurrency ||
           self.end_point_txn_concurrency_on_busy == 0 {
            return Err(box_err!("server.end-point-txn-concurrency-on-busy: {} is invalid, \
                                 should be in [1,{}]",
                                self.end_point_txn_concurrency_on_busy,
                                self.end_point_concurrency));
        }

        if self.end_point_small_txn_tasks_limit == 0 {
            return Err(box_err!("server.end-point-small-txn-tasks-limit: \
                                    shouldn't be 0"));
        }

        for (k, v) in &self.labels {
            try!(validate_label(k, "key"));
            try!(validate_label(v, "value"));
        }

        Ok(())
    }
}

fn validate_label(s: &str, tp: &str) -> Result<()> {
    let report_err = || {
        box_err!("store label {}: {:?} not match ^[a-z0-9]([a-z0-9-._]*[a-z0-9])?", tp, s)
    };
    if s.is_empty() {
        return Err(report_err());
    }
    let mut chrs = s.chars();
    let first_char = chrs.next().unwrap();
    if !first_char.is_ascii_lowercase() && !first_char.is_ascii_digit() {
        return Err(report_err());
    }
    let last_char = match chrs.next_back() {
        None => return Ok(()),
        Some(c) => c,
    };
    if !last_char.is_ascii_lowercase() && !last_char.is_ascii_digit() {
        return Err(report_err());
    }
    while let Some(c) = chrs.next() {
        if !c.is_ascii_lowercase() && !c.is_ascii_digit() && !"-._".contains(c) {
            return Err(report_err());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());

        cfg.raft_store.raft_heartbeat_ticks = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_end_point_txn_concurrency() {
        let mut cfg = Config::new();
        let expect = ((cfg.end_point_concurrency as f64) *
                      DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO) as usize;
        assert_eq!(cfg.end_point_txn_concurrency_on_busy, expect);
        cfg.end_point_concurrency = 18;
        cfg.auto_adjust_end_point_txn_concurrency();
        let expect = ((cfg.end_point_concurrency as f64) *
                      DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO) as usize;
        assert_eq!(cfg.end_point_txn_concurrency_on_busy, expect);

        cfg.end_point_concurrency = 2;
        cfg.auto_adjust_end_point_txn_concurrency();
        assert_eq!(cfg.end_point_txn_concurrency_on_busy, 1);
    }

    #[test]
    fn test_validate_endpoint_cfg() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());

        // invalid end-point-concurrency
        cfg.end_point_concurrency = 0;
        assert!(cfg.validate().is_err());
        cfg.end_point_concurrency = DEFAULT_END_POINT_CONCURRENCY;

        // invalid end-point-txn-concurrency-on-busy
        cfg.end_point_txn_concurrency_on_busy = cfg.end_point_concurrency + 1;
        assert!(cfg.validate().is_err());
        cfg.end_point_txn_concurrency_on_busy = 0;
        assert!(cfg.validate().is_err());
        cfg.auto_adjust_end_point_txn_concurrency();

        // invalid end-point-small-txn-tasks-limit
        cfg.end_point_small_txn_tasks_limit = 0;
        assert!(cfg.validate().is_err());
        cfg.end_point_small_txn_tasks_limit = DEFAULT_END_POINT_SMALL_TXN_TASKS_LIMIT;

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_store_labels() {
        let invalid_cases = vec![
            "",
            "123*",
            ".123",
            "Cab",
            "abC",
            "💖",
        ];

        for case in invalid_cases {
            assert!(validate_label(case, "dummy").is_err());
        }

        let valid_cases = vec![
            "a",
            "0",
            "a.1-2",
            "b_1.2",
            "cab-012",
            "3ac.8b2",
        ];

        for case in valid_cases {
            validate_label(case, "dummy").unwrap();
        }
    }
}
