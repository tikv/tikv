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

use std::collections::HashMap;

pub use raftstore::store::Config as RaftStoreConfig;
pub use storage::Config as StorageConfig;
use super::Result;

pub const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &'static str = "";
const DEFAULT_NOTIFY_CAPACITY: usize = 4096;
const DEFAULT_END_POINT_CONCURRENCY: usize = 8;
const DEFAULT_MESSAGES_PER_TICK: usize = 256;
const DEFAULT_SEND_BUFFER_SIZE: usize = 128 * 1024;
const DEFAULT_RECV_BUFFER_SIZE: usize = 128 * 1024;
const DEFAULT_NETWORK_MONITOR_MAX_STORE_NUMBER: usize = 10000;
const DEFAULT_NETWORK_MONITOR_KEEPALIVE_TIMETOUT_SEC: u64 = 60;
const DEFAULT_NETWORK_MONITOR_CONNECTION_TIMEOUT_SEC: u64 = 5 * 60;

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
    pub network_monitor_max_store_number: usize,
    pub network_monitor_keepalive_timeout: u64, // in second
    pub network_monitor_connection_timeout: u64, // in second
}

impl Default for Config {
    fn default() -> Config {
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            labels: HashMap::new(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
            messages_per_tick: DEFAULT_MESSAGES_PER_TICK,
            send_buffer_size: DEFAULT_SEND_BUFFER_SIZE,
            recv_buffer_size: DEFAULT_RECV_BUFFER_SIZE,
            end_point_concurrency: DEFAULT_END_POINT_CONCURRENCY,
            storage: StorageConfig::default(),
            raft_store: RaftStoreConfig::default(),
            network_monitor_max_store_number: DEFAULT_NETWORK_MONITOR_MAX_STORE_NUMBER,
            network_monitor_keepalive_timeout: DEFAULT_NETWORK_MONITOR_KEEPALIVE_TIMETOUT_SEC,
            network_monitor_connection_timeout: DEFAULT_NETWORK_MONITOR_CONNECTION_TIMEOUT_SEC,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        try!(self.raft_store.validate());

        Ok(())
    }
}
