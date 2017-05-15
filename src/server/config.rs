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

use util::collections::HashMap;

use super::Result;

pub use raftstore::store::Config as RaftStoreConfig;
pub use storage::Config as StorageConfig;

pub const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &'static str = "";
const DEFAULT_NOTIFY_CAPACITY: usize = 40960;
const DEFAULT_END_POINT_CONCURRENCY: usize = 8;
const DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO: f64 = 0.25;
const DEFAULT_END_POINT_SMALL_TXN_TASKS_LIMIT: usize = 2;
const DEFAULT_MESSAGES_PER_TICK: usize = 4096;
const DEFAULT_SEND_BUFFER_SIZE: usize = 128 * 1024;
const DEFAULT_RECV_BUFFER_SIZE: usize = 128 * 1024;

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    // Server labels to specify some attributes about this server.
    pub labels: HashMap<String, String>,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    pub advertise_addr: String,
    pub notify_capacity: usize,
    pub messages_per_tick: usize,
    pub send_buffer_size: usize,
    pub recv_buffer_size: usize,
    pub storage: StorageConfig,
    pub raft_store: RaftStoreConfig,
    pub end_point_concurrency: usize,
    pub end_point_txn_concurrency_on_busy: usize,
    pub end_point_small_txn_tasks_limit: usize,
}

impl Default for Config {
    fn default() -> Config {
        let mut cfg = Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            labels: HashMap::default(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
            messages_per_tick: DEFAULT_MESSAGES_PER_TICK,
            send_buffer_size: DEFAULT_SEND_BUFFER_SIZE,
            recv_buffer_size: DEFAULT_RECV_BUFFER_SIZE,
            end_point_concurrency: DEFAULT_END_POINT_CONCURRENCY,
            end_point_txn_concurrency_on_busy: usize::default(),
            end_point_small_txn_tasks_limit: DEFAULT_END_POINT_SMALL_TXN_TASKS_LIMIT,
            storage: StorageConfig::default(),
            raft_store: RaftStoreConfig::default(),
        };
        cfg.auto_adjust_end_point_txn_concurrency();
        cfg
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        try!(self.raft_store.validate());
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

        Ok(())
    }

    pub fn auto_adjust_end_point_txn_concurrency(&mut self) {
        self.end_point_txn_concurrency_on_busy =
            ((self.end_point_concurrency as f64) *
             DEFAULT_END_POINT_TXN_CONCURRENCY_RATIO) as usize;
        if self.end_point_txn_concurrency_on_busy == 0 {
            self.end_point_txn_concurrency_on_busy = 1;
        }
    }
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
        let expect = 1;
        assert_eq!(cfg.end_point_txn_concurrency_on_busy, expect);
    }

    #[test]
    fn test_validate_endpoint_cfg() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());
        cfg.end_point_small_txn_tasks_limit = 0;
        assert!(cfg.validate().is_err());
        cfg.end_point_small_txn_tasks_limit = 1;
        cfg.end_point_txn_concurrency_on_busy = cfg.end_point_concurrency + 1;
        assert!(cfg.validate().is_err());
        cfg.end_point_txn_concurrency_on_busy = 0;
        assert!(cfg.validate().is_err());
        cfg.auto_adjust_end_point_txn_concurrency();
        assert!(cfg.validate().is_ok());
    }
}
