// Copyright 2017 PingCAP, Inc.
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

use std::fmt;
use std::path::PathBuf;
use std::net::SocketAddrV4;
use std::time::Duration;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor};

use self::types::{Size, ServerLabels, WalRecoveryMode};

mod types;


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
// #[serde(default)]
pub struct Config {
    server: ServerConfig,
    metric: MetricConfig,

    #[serde(rename = "raftstore")]
    raft_store: RaftStoreConfig,
    rocksdb: RocksdbConfig,
    storage: StorageConfig,

    #[serde(rename = "pd")]
    #[serde(skip_serializing)]
    pd_depercated: Option<PdConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(default, rename_all = "kebab-case")]
struct ServerConfig {
    #[serde(default, skip_serializing, skip_deserializing)]
    cluster_id: Option<u64>,

    // TODO: IPv6 support via std::net::SocketAddr
    addr: SocketAddrV4,

    #[serde(deserialize_with = "types::deserialize_opt_addr")]
    advertise_addr: Option<SocketAddrV4>,

    #[serde(with = "types::addrs")]
    pd_endpoints: Vec<SocketAddrV4>,

    data_dir: PathBuf,

    labels: ServerLabels,

    log_level: String,

    notify_capacity: usize,
    messages_per_tick: usize,
    send_buffer_size: Size,
    recv_buffer_size: Size,

    end_point_concurrency: u32,
    capacity: Size,
    backup: PathBuf,

    #[serde(rename = "store")]
    #[serde(skip_serializing)]
    store_deprecated: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            cluster_id: None,
            addr: "127.0.0.1:20160".parse().unwrap(),
            advertise_addr: None,
            data_dir: PathBuf::from("/tmp/tikv"),
            labels: ServerLabels::default(),
            pd_endpoints: Vec::new(),
            log_level: "info".to_owned(),
            notify_capacity: 40960,
            messages_per_tick: 4096,
            send_buffer_size: Size::kibibyte(128),
            recv_buffer_size: Size::kibibyte(128),
            end_point_concurrency: 0,
            capacity: Size::default(),
            backup: PathBuf::from("/tmp/backup"),

            store_deprecated: None,
        }
    }
}



#[derive(Serialize, Deserialize, Debug)]
struct MetricConfig {
    #[serde(with = "types::duration")]
    interval: Duration,
    #[serde(deserialize_with = "types::deserialize_opt_addr")]
    address: Option<SocketAddrV4>,
    job: String,
}

impl Default for MetricConfig {
    fn default() -> Self {
        MetricConfig {
            interval: Duration::default(),
            address: None,
            job: "tikv".to_owned(),
        }
    }
}

/// Serializable configuration for raft store.
#[derive(Serialize, Deserialize, Debug)]
#[serde(default, rename_all = "kebab-case")]
pub struct RaftStoreConfig {
    #[serde(with = "types::duration")]
    pub raft_base_tick_interval: Duration,
    pub raft_heartbeat_ticks: usize,
    /// Election timeout ticks needs to be the same across all the cluster,
    /// otherwise it may lead to inconsistency.
    pub raft_election_timeout_ticks: usize,

    #[serde(skip_serializing, skip_deserializing)]
    pub raft_max_size_per_msg: Size,
    #[serde(skip_serializing, skip_deserializing)]
    pub raft_max_inflight_msgs: usize,
    /// When the entry exceed the max size, reject to propose it.
    pub raft_entry_max_size: Size,

    /// Interval to gc unnecessary raft log (ms).
    #[serde(with = "types::duration")]
    pub raft_log_gc_tick_interval: Duration,
    /// A threshold to gc stale raft log, must >= 1.
    pub raft_log_gc_threshold: u64,
    /// When entry count exceed this value, gc will be forced trigger.
    pub raft_log_gc_count_limit: u64,
    /// When the approximate size of raft log entries exceed this value,
    /// gc will be forced trigger.
    pub raft_log_gc_size_limit: Size,

    /// Interval (ms) to check region whether need to be split or not.
    #[serde(with = "types::duration")]
    pub split_region_check_tick_interval: Duration,

    /// When region [a, b) size meets region_max_size, it will be split
    /// into two region into [a, c), [c, b). And the size of [a, c) will
    /// be region_split_size (or a little bit smaller).
    pub region_max_size: Size,
    pub region_split_size: Size,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    #[serde(rename = "region-split-check-diff")]
    pub region_split_check_size_diff: Size,
    /// Interval to check whether start compaction for a region.
    #[serde(with = "types::duration")]
    pub region_compact_check_interval: Duration,
    /// When delete keys of a region exceeds the size, a compaction will
    /// be started.
    pub region_compact_delete_keys_count: u64,

    #[serde(with = "types::duration")]
    pub pd_heartbeat_tick_interval: Duration,
    #[serde(with = "types::duration")]
    pub pd_store_heartbeat_tick_interval: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub snap_mgr_gc_tick_interval: Duration,
    #[serde(skip_serializing, skip_deserializing)]
    pub snap_gc_timeout: Duration,

    #[serde(with = "types::duration")]
    pub lock_cf_compact_interval: Duration,
    pub lock_cf_compact_threshold: Size,

    pub notify_capacity: usize,
    pub messages_per_tick: usize,

    /// When a peer is not active for max_peer_down_duration,
    /// the peer is considered to be down and is reported to PD.
    #[serde(with = "types::duration")]
    pub max_peer_down_duration: Duration,

    /// If the leader of a peer is missing for longer than max_leader_missing_duration,
    /// the peer would ask pd to confirm whether it is valid in any region.
    /// If the peer is stale and is not valid in any region, it will destroy itself.
    #[serde(skip_serializing, skip_deserializing)]
    pub max_leader_missing_duration: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub snap_apply_batch_size: Size,

    /// Interval to check region whether the data is consistent.
    #[serde(with = "types::duration")]
    #[serde(rename = "consistency-check-interval")]
    pub consistency_check_tick_interval: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub report_region_flow_interval: Duration,

    /// The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: Duration,

    pub use_sst_file_snapshot: bool, // false
}

const REGION_SPLIT_SIZE: u64 = 64 * 1024 * 1024;

impl Default for RaftStoreConfig {
    fn default() -> Self {
        RaftStoreConfig {
            raft_base_tick_interval: Duration::from_millis(1000),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_max_size_per_msg: Size::kibibyte(1),
            raft_max_inflight_msgs: 256,
            raft_entry_max_size: Size::mebibyte(8),
            raft_log_gc_tick_interval: Duration::from_millis(10000),
            raft_log_gc_threshold: 50,
            raft_log_gc_count_limit: REGION_SPLIT_SIZE * 3 / 4 / 1024,
            raft_log_gc_size_limit: Size::byte(REGION_SPLIT_SIZE * 3 / 4),
            split_region_check_tick_interval: Duration::from_millis(10000),
            region_max_size: Size::mebibyte(80),
            region_split_size: Size::mebibyte(64),
            region_split_check_size_diff: Size::mebibyte(8),
            region_compact_check_interval: Duration::default(),
            region_compact_delete_keys_count: 1_000_000,
            pd_heartbeat_tick_interval: Duration::from_millis(60000),
            pd_store_heartbeat_tick_interval: Duration::from_millis(10000),
            notify_capacity: 40960,
            snap_mgr_gc_tick_interval: Duration::from_millis(60000),
            snap_gc_timeout: Duration::from_secs(4 * 60 * 60), // 4 hours
            messages_per_tick: 4096,
            max_peer_down_duration: Duration::from_secs(300),
            max_leader_missing_duration: Duration::from_secs(2 * 60 * 60),
            snap_apply_batch_size: Size::mebibyte(10),
            lock_cf_compact_interval: Duration::from_secs(10 * 60), // 10 min
            lock_cf_compact_threshold: Size::mebibyte(256),
            consistency_check_tick_interval: Duration::default(),
            report_region_flow_interval: Duration::from_millis(60000),
            raft_store_max_leader_lease: Duration::from_secs(9),
            use_sst_file_snapshot: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PdConfig {
    endpoints: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(default, rename_all = "kebab-case")]
struct RocksdbConfig {
    wal_recovery_mode: WalRecoveryMode,

    wal_dir: Option<PathBuf>,
    wal_ttl_seconds: u64,
    wal_size_limit: Size,

    max_total_wal_size: Size,

    max_background_compactions: i32,
    max_background_flushes: i32,
    max_manifest_file_size: Size,

    create_if_missing: bool,

    max_open_files: i32,
    enable_statistics: bool,

    #[serde(with = "types::duration")]
    stats_dump_period_sec: Duration,

    compaction_readahead_size: Size,

    info_log_max_size: Size,
    #[serde(with = "types::duration")]
    info_log_roll_time: Duration,
    info_log_dir: Option<PathBuf>,

    rate_bytes_per_sec: i64,

    // cf
    defaultcf: ColumnFamilyConfig,
    writecf: ColumnFamilyConfig,
    raftcf: ColumnFamilyConfig,
    lockcf: ColumnFamilyConfig,
}

impl Default for RocksdbConfig {
    fn default() -> Self {
        RocksdbConfig {
            wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,

            wal_dir: None,
            wal_ttl_seconds: 0,
            wal_size_limit: Size::default(),

            max_total_wal_size: Size::gigibyte(4),

            max_background_compactions: 6,
            max_background_flushes: 2,
            max_manifest_file_size: Size::mebibyte(20),

            create_if_missing: true,

            max_open_files: 40960,
            enable_statistics: true,

            stats_dump_period_sec: Duration::from_secs(600),

            compaction_readahead_size: Size::default(),

            info_log_max_size: Size::default(),
            info_log_roll_time: Duration::default(),
            info_log_dir: None,

            rate_bytes_per_sec: 0,

            defaultcf: ColumnFamilyConfig::default(),
            writecf: ColumnFamilyConfig::default(),
            raftcf: ColumnFamilyConfig::default(),
            lockcf: ColumnFamilyConfig::default(),
        }
    }
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(default, rename_all = "kebab-case")]
struct ColumnFamilyConfig {
    block_size: Size, // 64KiB
    block_cache_size: Size,
    cache_index_and_filter_blocks: bool, // true

    bloom_filter_bits_per_key: i32, // 10
    block_based_bloom_filter: bool, // false

    compression_per_level: String,

    write_buffer_size: Size, // 128MiB

    max_write_buffer_number: i32, // 5
    min_write_buffer_number_to_merge: i32, // 1

    max_bytes_for_level_base: Size, // 128MiB
    target_file_size_base: Size, // 32MiB

    level0_slowdown_writes_trigger: i32, // 20
    level0_stop_writes_trigger: i32, // 36
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        ColumnFamilyConfig {
            block_size: Size::kibibyte(64),
            block_cache_size: Size::default(), // TODO: calculate
            cache_index_and_filter_blocks: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            compression_per_level: "lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned(),
            write_buffer_size: Size::mebibyte(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: Size::mebibyte(128),
            target_file_size_base: Size::mebibyte(32),
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
        }
    }
}


const DEFAULT_SCHED_CAPACITY: usize = 10240;
const DEFAULT_SCHED_MSG_PER_TICK: usize = 1024;
const DEFAULT_SCHED_CONCURRENCY: usize = 102400;
const DEFAULT_SCHED_WORKER_POOL_SIZE: usize = 4;
const DEFAULT_SCHED_TOO_BUSY_THRESHOLD: usize = 1000;

#[derive(Serialize, Deserialize, Debug)]
#[serde(default, rename_all = "kebab-case")]
struct StorageConfig {
    scheduler_notify_capacity: usize,
    scheduler_messages_per_tick: usize,
    scheduler_concurrency: usize,
    scheduler_worker_pool_size: usize,
    scheduler_too_busy_threshold: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            scheduler_notify_capacity: 10240,
            scheduler_messages_per_tick: 1024,
            scheduler_concurrency: 102400,
            scheduler_worker_pool_size: 4, // TODO: calculate
            scheduler_too_busy_threshold: 1000,
        }
    }
}
