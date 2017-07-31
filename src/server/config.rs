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
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 10;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: usize = 2 * 1024 * 1024;
const DEFAULT_MESSAGES_PER_TICK: usize = 4096;

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
    pub grpc_concurrency: usize,
    pub grpc_concurrent_stream: usize,
    pub grpc_raft_conn_num: usize,
    pub grpc_stream_initial_window_size: usize,
    pub storage: StorageConfig,
    pub raft_store: RaftStoreConfig,
    pub end_point_concurrency: usize,
}

impl Default for Config {
    fn default() -> Config {
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
            grpc_stream_initial_window_size: DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE,
            end_point_concurrency: DEFAULT_END_POINT_CONCURRENCY,
            storage: StorageConfig::default(),
            raft_store: RaftStoreConfig::default(),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        try!(self.raft_store.validate());
        if self.end_point_concurrency == 0 {
            return Err(box_err!("server.server.end-point-concurrency: {} is invalid, \
                                 shouldn't be 0",
                                self.end_point_concurrency));
        }

        Ok(())
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
}
