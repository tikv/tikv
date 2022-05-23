// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, i32, isize, sync::Arc, time::Duration};

use collections::HashMap;
use engine_traits::{perf_level_serde, PerfLevel};
use grpcio::{CompressionAlgorithms, ResourceQuota};
use online_config::{ConfigChange, ConfigManager, OnlineConfig};
pub use raftstore::store::Config as RaftStoreConfig;
use regex::Regex;
use tikv_util::{
    config::{self, ReadableDuration, ReadableSize, VersionTrack},
    sys::SysQuota,
    worker::Scheduler,
};

use super::{snap::Task as SnapTask, Result};
pub use crate::storage::config::Config as StorageConfig;

pub const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &str = "";
const DEFAULT_STATUS_ADDR: &str = "127.0.0.1:20180";
const DEFAULT_GRPC_CONCURRENCY: usize = 5;
const DEFAULT_GRPC_CONCURRENT_STREAM: i32 = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 1;
const DEFAULT_GRPC_MEMORY_POOL_QUOTA: u64 = isize::MAX as u64;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: u64 = 2 * 1024 * 1024;

// Number of rows in each chunk.
const DEFAULT_ENDPOINT_BATCH_ROW_LIMIT: usize = 64;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const DEFAULT_ENDPOINT_REQUEST_MAX_HANDLE_SECS: u64 = 60;

// Number of rows in each chunk for streaming coprocessor.
const DEFAULT_ENDPOINT_STREAM_BATCH_ROW_LIMIT: usize = 128;

// At least 4 long coprocessor requests are allowed to run concurrently.
const MIN_ENDPOINT_MAX_CONCURRENCY: usize = 4;

const DEFAULT_SNAP_MAX_BYTES_PER_SEC: u64 = 100 * 1024 * 1024;

const DEFAULT_MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;

/// A clone of `grpc::CompressionAlgorithms` with serde supports.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GrpcCompressionType {
    None,
    Deflate,
    Gzip,
}

/// OnlineConfig for the `server` module.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(skip)]
    #[online_config(skip)]
    pub cluster_id: u64,

    // Server listening address.
    #[online_config(skip)]
    pub addr: String,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    #[online_config(skip)]
    pub advertise_addr: String,

    // These are related to TiKV status.
    #[online_config(skip)]
    pub status_addr: String,

    // Status server's advertise listening address for outer communication.
    // If not set, the status server's listening address will be used.
    #[online_config(skip)]
    pub advertise_status_addr: String,

    #[online_config(skip)]
    pub status_thread_pool_size: usize,

    pub max_grpc_send_msg_len: i32,

    // When merge raft messages into a batch message, leave a buffer.
    #[online_config(skip)]
    pub raft_client_grpc_send_msg_buffer: usize,

    #[online_config(skip)]
    pub raft_client_queue_size: usize,

    pub raft_msg_max_batch_size: usize,

    // TODO: use CompressionAlgorithms instead once it supports traits like Clone etc.
    #[online_config(skip)]
    pub grpc_compression_type: GrpcCompressionType,
    #[online_config(skip)]
    pub grpc_concurrency: usize,
    #[online_config(skip)]
    pub grpc_concurrent_stream: i32,
    #[online_config(skip)]
    pub grpc_raft_conn_num: usize,
    pub grpc_memory_pool_quota: ReadableSize,
    #[online_config(skip)]
    pub grpc_stream_initial_window_size: ReadableSize,
    #[online_config(skip)]
    pub grpc_keepalive_time: ReadableDuration,
    #[online_config(skip)]
    pub grpc_keepalive_timeout: ReadableDuration,
    /// How many snapshots can be sent concurrently.
    pub concurrent_send_snap_limit: usize,
    /// How many snapshots can be recv concurrently.
    pub concurrent_recv_snap_limit: usize,
    #[online_config(skip)]
    pub end_point_recursion_limit: u32,
    #[online_config(skip)]
    pub end_point_stream_channel_size: usize,
    #[online_config(skip)]
    pub end_point_batch_row_limit: usize,
    #[online_config(skip)]
    pub end_point_stream_batch_row_limit: usize,
    #[online_config(skip)]
    pub end_point_enable_batch_if_possible: bool,
    #[online_config(skip)]
    pub end_point_request_max_handle_duration: ReadableDuration,
    #[online_config(skip)]
    pub end_point_max_concurrency: usize,
    #[serde(with = "perf_level_serde")]
    #[online_config(skip)]
    pub end_point_perf_level: PerfLevel,
    pub snap_max_write_bytes_per_sec: ReadableSize,
    pub snap_max_total_size: ReadableSize,
    #[online_config(skip)]
    pub stats_concurrency: usize,
    #[online_config(skip)]
    pub heavy_load_threshold: usize,
    #[online_config(skip)]
    pub heavy_load_wait_duration: Option<ReadableDuration>,
    #[online_config(skip)]
    pub enable_request_batch: bool,
    #[online_config(skip)]
    pub background_thread_count: usize,
    // If handle time is larger than the threshold, it will print slow log in end point.
    #[online_config(skip)]
    pub end_point_slow_log_threshold: ReadableDuration,
    /// Max connections per address for forwarding request.
    #[online_config(skip)]
    pub forward_max_connections_per_address: usize,

    // Test only.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub raft_client_backoff_step: ReadableDuration,

    #[doc(hidden)]
    #[online_config(skip)]
    /// When TiKV memory usage reaches `memory_usage_high_water` it will try to limit memory
    /// increasing. For server layer some messages will be rejected or droped, if they utilize
    /// memory more than `reject_messages_on_memory_ratio` * total.
    ///
    /// Set it to 0 can disable message rejecting.
    // By default it's 0.2. So for different memory capacity, messages are rejected when:
    // * system=8G,  memory_usage_limit=6G,  reject_at=1.2G
    // * system=16G, memory_usage_limit=12G, reject_at=2.4G
    // * system=32G, memory_usage_limit=24G, reject_at=4.8G
    pub reject_messages_on_memory_ratio: f64,

    // Server labels to specify some attributes about this server.
    #[online_config(skip)]
    pub labels: HashMap<String, String>,

    // deprecated. use readpool.coprocessor.xx_concurrency.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub end_point_concurrency: Option<usize>,

    // deprecated. use readpool.coprocessor.stack_size.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub end_point_stack_size: Option<ReadableSize>,

    // deprecated. use readpool.coprocessor.max_tasks_per_worker_xx.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub end_point_max_tasks: Option<usize>,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = SysQuota::cpu_cores_quota();
        let background_thread_count = if cpu_num > 16.0 { 3 } else { 2 };
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            labels: HashMap::default(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            status_addr: DEFAULT_STATUS_ADDR.to_owned(),
            advertise_status_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            status_thread_pool_size: 1,
            max_grpc_send_msg_len: DEFAULT_MAX_GRPC_SEND_MSG_LEN,
            raft_client_grpc_send_msg_buffer: 512 * 1024,
            raft_client_queue_size: 8192,
            raft_msg_max_batch_size: 128,
            grpc_compression_type: GrpcCompressionType::None,
            grpc_concurrency: DEFAULT_GRPC_CONCURRENCY,
            grpc_concurrent_stream: DEFAULT_GRPC_CONCURRENT_STREAM,
            grpc_raft_conn_num: DEFAULT_GRPC_RAFT_CONN_NUM,
            grpc_stream_initial_window_size: ReadableSize(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE),
            grpc_memory_pool_quota: ReadableSize(DEFAULT_GRPC_MEMORY_POOL_QUOTA),
            // There will be a heartbeat every secs, it's weird a connection will be idle for more
            // than 10 senconds.
            grpc_keepalive_time: ReadableDuration::secs(10),
            grpc_keepalive_timeout: ReadableDuration::secs(3),
            concurrent_send_snap_limit: 32,
            concurrent_recv_snap_limit: 32,
            end_point_concurrency: None, // deprecated
            end_point_max_tasks: None,   // deprecated
            end_point_stack_size: None,  // deprecated
            end_point_recursion_limit: 1000,
            end_point_stream_channel_size: 8,
            end_point_batch_row_limit: DEFAULT_ENDPOINT_BATCH_ROW_LIMIT,
            end_point_stream_batch_row_limit: DEFAULT_ENDPOINT_STREAM_BATCH_ROW_LIMIT,
            end_point_enable_batch_if_possible: true,
            end_point_request_max_handle_duration: ReadableDuration::secs(
                DEFAULT_ENDPOINT_REQUEST_MAX_HANDLE_SECS,
            ),
            end_point_max_concurrency: cmp::max(cpu_num as usize, MIN_ENDPOINT_MAX_CONCURRENCY),
            end_point_perf_level: PerfLevel::Uninitialized,
            snap_max_write_bytes_per_sec: ReadableSize(DEFAULT_SNAP_MAX_BYTES_PER_SEC),
            snap_max_total_size: ReadableSize(0),
            stats_concurrency: 1,
            // 75 means a gRPC thread is under heavy load if its total CPU usage
            // is greater than 75%.
            heavy_load_threshold: 75,
            heavy_load_wait_duration: None,
            enable_request_batch: true,
            raft_client_backoff_step: ReadableDuration::secs(1),
            reject_messages_on_memory_ratio: 0.2,
            background_thread_count,
            end_point_slow_log_threshold: ReadableDuration::secs(1),
            // Go tikv client uses 4 as well.
            forward_max_connections_per_address: 4,
        }
    }
}

impl Config {
    #[inline]
    pub fn heavy_load_wait_duration(&self) -> Duration {
        self.heavy_load_wait_duration
            .unwrap_or_else(|| ReadableDuration::micros(50))
            .0
    }

    /// Validates the configuration and returns an error if it is misconfigured.
    pub fn validate(&mut self) -> Result<()> {
        box_try!(config::check_addr(&self.addr));
        if !self.advertise_addr.is_empty() {
            box_try!(config::check_addr(&self.advertise_addr));
        } else {
            info!(
                "no advertise-addr is specified, falling back to default addr";
                "addr" => %self.addr
            );
            self.advertise_addr = self.addr.clone();
        }
        if box_try!(config::check_addr(&self.advertise_addr)) {
            return Err(box_err!(
                "invalid advertise-addr: {:?}",
                self.advertise_addr
            ));
        }
        if self.status_addr.is_empty() && !self.advertise_status_addr.is_empty() {
            return Err(box_err!("status-addr can not be empty"));
        }
        if !self.status_addr.is_empty() {
            let status_addr_unspecified = box_try!(config::check_addr(&self.status_addr));
            if !self.advertise_status_addr.is_empty() {
                if box_try!(config::check_addr(&self.advertise_status_addr)) {
                    return Err(box_err!(
                        "invalid advertise-status-addr: {:?}",
                        self.advertise_status_addr
                    ));
                }
            } else if !status_addr_unspecified {
                info!(
                    "no advertise-status-addr is specified, falling back to status-addr";
                    "status-addr" => %self.status_addr
                );
                self.advertise_status_addr = self.status_addr.clone();
            } else {
                info!(
                    "no advertise-status-addr is specified, and we can't falling back to \
                    status-addr because it is invalid as advertise-status-addr";
                    "status-addr" => %self.status_addr
                );
            }
        }
        if self.advertise_status_addr == self.advertise_addr {
            return Err(box_err!(
                "advertise-status-addr has already been used: {:?}",
                self.advertise_addr
            ));
        }
        let non_zero_entries = vec![
            (
                "concurrent-send-snap-limit",
                self.concurrent_send_snap_limit,
            ),
            (
                "concurrent-recv-snap-limit",
                self.concurrent_recv_snap_limit,
            ),
            (
                "grpc-memory-pool-quota",
                self.grpc_memory_pool_quota.0 as usize,
            ),
        ];
        for (label, value) in non_zero_entries {
            if value == 0 {
                return Err(box_err!("server.{} should not be 0.", label));
            }
        }

        if self.end_point_recursion_limit < 100 {
            return Err(box_err!("server.end-point-recursion-limit is too small"));
        }

        if self.end_point_request_max_handle_duration.as_secs()
            < DEFAULT_ENDPOINT_REQUEST_MAX_HANDLE_SECS
        {
            return Err(box_err!(
                "server.end-point-request-max-handle-secs is too small."
            ));
        }

        if self.max_grpc_send_msg_len <= 0 {
            return Err(box_err!(
                "server.max-grpc-send-msg-len must be bigger than 0."
            ));
        }

        if self.grpc_stream_initial_window_size.0 > i32::MAX as u64 {
            return Err(box_err!(
                "server.grpc-stream-initial-window-size is too large."
            ));
        }

        for (k, v) in &self.labels {
            validate_label_key(k)?;
            validate_label_value(v)?;
        }

        if self.forward_max_connections_per_address == 0 {
            return Err(box_err!(
                "server.forward-max-connections-per-address can't be 0."
            ));
        }

        if self.reject_messages_on_memory_ratio < 0.0 {
            return Err(box_err!(
                "server.reject_messages_on_memory_ratio must be greater than 0"
            ));
        }

        if self.heavy_load_threshold > 100 {
            // The configuration has been changed to describe CPU usage of a single thread instead
            // of all threads. So migrate from the old style.
            self.heavy_load_threshold = 75;
        }

        Ok(())
    }

    /// Gets configured grpc compression algorithm.
    pub fn grpc_compression_algorithm(&self) -> CompressionAlgorithms {
        match self.grpc_compression_type {
            GrpcCompressionType::None => CompressionAlgorithms::GRPC_COMPRESS_NONE,
            GrpcCompressionType::Deflate => CompressionAlgorithms::GRPC_COMPRESS_DEFLATE,
            GrpcCompressionType::Gzip => CompressionAlgorithms::GRPC_COMPRESS_GZIP,
        }
    }
}

pub struct ServerConfigManager {
    tx: Scheduler<SnapTask>,
    config: Arc<VersionTrack<Config>>,
    grpc_mem_quota: ResourceQuota,
}

unsafe impl Send for ServerConfigManager {}
unsafe impl Sync for ServerConfigManager {}

impl ServerConfigManager {
    pub fn new(
        tx: Scheduler<SnapTask>,
        config: Arc<VersionTrack<Config>>,
        grpc_mem_quota: ResourceQuota,
    ) -> ServerConfigManager {
        ServerConfigManager {
            tx,
            config,
            grpc_mem_quota,
        }
    }
}

impl ConfigManager for ServerConfigManager {
    fn dispatch(&mut self, c: ConfigChange) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = c.clone();
            self.config.update(move |cfg| cfg.update(change));
            if let Some(value) = c.get("grpc_memory_pool_quota") {
                let mem_quota: ReadableSize = value.clone().into();
                // the resize is done inplace indeed, but grpc-rs's api need self, so we just
                // clone it here, but this no extra side effect here.
                self.grpc_mem_quota
                    .clone()
                    .resize_memory(mem_quota.0 as usize);
            }
            if let Err(e) = self.tx.schedule(SnapTask::RefreshConfigEvent) {
                error!("server configuration manager schedule refresh snapshot work task failed"; "err"=> ?e);
            }
        }
        info!("server configuration changed"; "change" => ?c);
        Ok(())
    }
}

lazy_static! {
    static ref LABEL_KEY_FORMAT: Regex =
        Regex::new("^[$]?[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?$").unwrap();
    static ref LABEL_VALUE_FORMAT: Regex = Regex::new("^[-A-Za-z0-9_./]*$").unwrap();
}

fn validate_label_key(s: &str) -> Result<()> {
    if LABEL_KEY_FORMAT.is_match(s) {
        Ok(())
    } else {
        Err(box_err!(
            "store label key: {:?} not match {}",
            s,
            *LABEL_KEY_FORMAT
        ))
    }
}

fn validate_label_value(s: &str) -> Result<()> {
    if LABEL_VALUE_FORMAT.is_match(s) {
        Ok(())
    } else {
        Err(box_err!(
            "store label value: {:?} not match {}",
            s,
            *LABEL_VALUE_FORMAT
        ))
    }
}

#[cfg(test)]
mod tests {
    use tikv_util::config::ReadableDuration;

    use super::*;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::default();
        assert!(cfg.advertise_addr.is_empty());
        assert!(cfg.advertise_status_addr.is_empty());
        cfg.validate().unwrap();
        assert_eq!(cfg.addr, cfg.advertise_addr);
        assert_eq!(cfg.status_addr, cfg.advertise_status_addr);

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.concurrent_send_snap_limit = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.concurrent_recv_snap_limit = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_recursion_limit = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.grpc_memory_pool_quota = ReadableSize::mb(0);
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_request_max_handle_duration = ReadableDuration::secs(0);
        assert!(invalid_cfg.validate().is_err());

        invalid_cfg = Config::default();
        invalid_cfg.addr = "0.0.0.0:1000".to_owned();
        assert!(invalid_cfg.validate().is_err());
        invalid_cfg.advertise_addr = "127.0.0.1:1000".to_owned();
        invalid_cfg.validate().unwrap();

        invalid_cfg = Config::default();
        invalid_cfg.status_addr = "0.0.0.0:1000".to_owned();
        for _ in 0..10 {
            invalid_cfg.validate().unwrap();
        }
        assert!(invalid_cfg.advertise_status_addr.is_empty());
        invalid_cfg.advertise_status_addr = "0.0.0.0:1000".to_owned();
        assert!(invalid_cfg.validate().is_err());

        invalid_cfg = Config::default();
        invalid_cfg.advertise_addr = "127.0.0.1:1000".to_owned();
        invalid_cfg.advertise_status_addr = "127.0.0.1:1000".to_owned();
        assert!(invalid_cfg.validate().is_err());

        invalid_cfg = Config::default();
        invalid_cfg.max_grpc_send_msg_len = 0;
        assert!(invalid_cfg.validate().is_err());

        invalid_cfg = Config::default();
        invalid_cfg.grpc_stream_initial_window_size = ReadableSize(i32::MAX as u64 + 1);
        assert!(invalid_cfg.validate().is_err());

        cfg.labels.insert("k1".to_owned(), "v1".to_owned());
        cfg.validate().unwrap();
        cfg.labels.insert("k2".to_owned(), "v2?".to_owned());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_store_labels() {
        let cases = vec![
            ("", false, true),
            ("123*", false, false),
            (".123", false, true),
            ("💖", false, false),
            ("a", true, true),
            ("0", true, true),
            ("a.1-2", true, true),
            ("Cab", true, true),
            ("abC", true, true),
            ("b_1.2", true, true),
            ("cab-012", true, true),
            ("3ac.8b2", true, true),
            ("/abc", false, true),
            ("abc/", false, true),
            ("abc/def", true, true),
            ("-abc", false, true),
            ("abc-", false, true),
            ("abc$def", false, false),
            ("$abc", true, false),
            ("$a.b-c/d_e", true, false),
            (".-_/", false, true),
        ];

        for (text, can_be_key, can_be_value) in cases {
            assert_eq!(validate_label_key(text).is_ok(), can_be_key);
            assert_eq!(validate_label_value(text).is_ok(), can_be_value);
        }
    }
}
