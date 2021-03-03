// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, i32, isize};

use super::Result;
use grpcio::CompressionAlgorithms;
use regex::Regex;

use collections::HashMap;
use tikv_util::config::{self, ReadableDuration, ReadableSize};
use tikv_util::sys::sys_quota::SysQuota;

pub use crate::storage::config::Config as StorageConfig;
pub use raftstore::store::Config as RaftStoreConfig;

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

/// Configuration for the `server` module.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(skip)]
    pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    pub advertise_addr: String,

    // These are related to TiKV status.
    pub status_addr: String,

    // Status server's advertise listening address for outer communication.
    // If not set, the status server's listening address will be used.
    pub advertise_status_addr: String,

    pub status_thread_pool_size: usize,

    pub max_grpc_send_msg_len: i32,

    // TODO: use CompressionAlgorithms instead once it supports traits like Clone etc.
    pub grpc_compression_type: GrpcCompressionType,
    pub grpc_concurrency: usize,
    pub grpc_concurrent_stream: i32,
    pub grpc_raft_conn_num: usize,
    pub grpc_memory_pool_quota: ReadableSize,
    pub grpc_stream_initial_window_size: ReadableSize,
    pub grpc_keepalive_time: ReadableDuration,
    pub grpc_keepalive_timeout: ReadableDuration,
    /// How many snapshots can be sent concurrently.
    pub concurrent_send_snap_limit: usize,
    /// How many snapshots can be recv concurrently.
    pub concurrent_recv_snap_limit: usize,
    pub end_point_recursion_limit: u32,
    pub end_point_stream_channel_size: usize,
    pub end_point_batch_row_limit: usize,
    pub end_point_stream_batch_row_limit: usize,
    pub end_point_enable_batch_if_possible: bool,
    pub end_point_request_max_handle_duration: ReadableDuration,
    pub end_point_max_concurrency: usize,
    pub snap_max_write_bytes_per_sec: ReadableSize,
    pub snap_max_total_size: ReadableSize,
    pub stats_concurrency: usize,
    pub heavy_load_threshold: usize,
    pub heavy_load_wait_duration: ReadableDuration,
    pub enable_request_batch: bool,
    pub background_thread_count: usize,
    // If handle time is larger than the threshold, it will print slow log in end point.
    pub end_point_slow_log_threshold: ReadableDuration,

    // Test only.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub raft_client_backoff_step: ReadableDuration,

    // Server labels to specify some attributes about this server.
    pub labels: HashMap<String, String>,

    // deprecated. use readpool.coprocessor.xx_concurrency.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub end_point_concurrency: Option<usize>,

    // deprecated. use readpool.coprocessor.stack_size.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub end_point_stack_size: Option<ReadableSize>,

    // deprecated. use readpool.coprocessor.max_tasks_per_worker_xx.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub end_point_max_tasks: Option<usize>,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = SysQuota::new().cpu_cores_quota();
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
            snap_max_write_bytes_per_sec: ReadableSize(DEFAULT_SNAP_MAX_BYTES_PER_SEC),
            snap_max_total_size: ReadableSize(0),
            stats_concurrency: 1,
            // 300 means gRPC threads are under heavy load if their total CPU usage
            // is greater than 300%.
            heavy_load_threshold: 300,
            // The resolution of timer in tokio is 1ms.
            heavy_load_wait_duration: ReadableDuration::millis(1),
            enable_request_batch: true,
            raft_client_backoff_step: ReadableDuration::secs(1),
            background_thread_count,
            end_point_slow_log_threshold: ReadableDuration::secs(1),
        }
    }
}

impl Config {
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

        if self.grpc_stream_initial_window_size.0 > i32::MAX as u64 {
            return Err(box_err!(
                "server.grpc_stream_initial_window_size is too large."
            ));
        }

        for (k, v) in &self.labels {
            validate_label_key(k)?;
            validate_label_value(v)?;
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
    use super::*;
    use tikv_util::config::ReadableDuration;

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
