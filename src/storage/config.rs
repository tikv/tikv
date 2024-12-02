// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use std::{cmp::max, error::Error, path::Path};

use engine_rocks::raw::{Cache, LRUCacheOptions, MemoryAllocator};
use file_system::{IoPriority, IoRateLimitMode, IoRateLimiter, IoType};
use kvproto::kvrpcpb::ApiVersion;
use libc::c_int;
use online_config::OnlineConfig;
use tikv_util::{
    config::{self, ReadableDuration, ReadableSize},
    sys::SysQuota,
};

use crate::config::{DEFAULT_ROCKSDB_SUB_DIR, DEFAULT_TABLET_SUB_DIR, MIN_BLOCK_CACHE_SHARD_SIZE};

pub const DEFAULT_DATA_DIR: &str = "./";
const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
const DEFAULT_MAX_KEY_SIZE: usize = 8 * 1024;
const DEFAULT_SCHED_CONCURRENCY: usize = 1024 * 512;
const MAX_SCHED_CONCURRENCY: usize = 2 * 1024 * 1024;

// According to "Little's law", assuming you can write 100MB per
// second, and it takes about 100ms to process the write requests
// on average, in that situation the writing bytes estimated 10MB,
// here we use 100MB as default value for tolerate 1s latency.
const DEFAULT_SCHED_PENDING_WRITE_MB: u64 = 100;

// The default memory quota for pending and running storage commands kv_get,
// kv_prewrite, kv_commit, etc.
//
// The memory usage of a tikv::storage::txn::commands::Commands can be broken
// down into:
//
// * The size of key-value pair which is assumed to be 1KB.
// * The size of Command itself is approximately 448 bytes.
// * The size of a future that executes Command, about 6184 bytes (see
//   TxnScheduler::execute).
//
// Given the total memory capacity of 256MB, TiKV can support around 35,000
// concurrently running commands or 182,000 commands waiting to be executed.
//
// With the default config on a single-node TiKV cluster, an empirical
// memory quota usage for TPCC prepare with --threads 500 is about 50MB.
// 256MB is large enough for most scenarios.
const DEFAULT_TXN_MEMORY_QUOTA_CAPACITY: ReadableSize = ReadableSize::mb(256);

const DEFAULT_RESERVED_SPACE_GB: u64 = 5;
const DEFAULT_RESERVED_RAFT_SPACE_GB: u64 = 1;

// In tests, we've observed 1.2M entries in the TxnStatusCache. We
// conservatively set the limit to 5M entries in total.
// As TxnStatusCache have 128 slots by default. We round it to 5.12M.
// This consumes at most around 300MB memory theoretically, but usually it's
// much less as it's hard to see the capacity being used up.
const DEFAULT_TXN_STATUS_CACHE_CAPACITY: usize = 40_000 * 128;

// Block cache capacity used when TikvConfig isn't validated. It should only
// occur in tests.
const FALLBACK_BLOCK_CACHE_CAPACITY: ReadableSize = ReadableSize::mb(128);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EngineType {
    RaftKv,
    #[serde(alias = "partitioned-raft-kv")]
    RaftKv2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub data_dir: String,
    #[online_config(skip)]
    pub engine: EngineType,
    // Replaced by `GcConfig.ratio_threshold`. Keep it for backward compatibility.
    #[online_config(skip)]
    pub gc_ratio_threshold: f64,
    #[online_config(skip)]
    pub max_key_size: usize,
    #[online_config(skip)]
    pub scheduler_concurrency: usize,
    pub scheduler_worker_pool_size: usize,
    #[online_config(skip)]
    pub scheduler_pending_write_threshold: ReadableSize,
    #[online_config(skip)]
    // Reserve disk space to make tikv would have enough space to compact when disk is full.
    pub reserve_space: ReadableSize,
    #[online_config(skip)]
    pub reserve_raft_space: ReadableSize,
    #[online_config(skip)]
    pub enable_async_apply_prewrite: bool,
    #[online_config(skip)]
    pub api_version: u8,
    #[online_config(skip)]
    pub enable_ttl: bool,
    #[online_config(skip)]
    pub background_error_recovery_window: ReadableDuration,
    /// Interval to check TTL for all SSTs,
    pub ttl_check_poll_interval: ReadableDuration,
    #[online_config(skip)]
    pub txn_status_cache_capacity: usize,
    pub memory_quota: ReadableSize,
    /// Maximum max_ts deviation allowed from PD timestamp (in seconds)
    #[online_config(skip)]
    pub max_ts_allowance_secs: u64,
    /// How often to refresh the max_ts limit from PD (in seconds)
    #[online_config(skip)]
    pub max_ts_sync_interval_secs: u64,
    #[online_config(skip)]
    pub panic_on_invalid_max_ts: bool,
    #[online_config(submodule)]
    pub flow_control: FlowControlConfig,
    #[online_config(submodule)]
    pub block_cache: BlockCacheConfig,
    #[online_config(submodule)]
    pub io_rate_limit: IoRateLimitConfig,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = SysQuota::cpu_cores_quota();
        Config {
            data_dir: DEFAULT_DATA_DIR.to_owned(),
            engine: EngineType::RaftKv,
            gc_ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            scheduler_concurrency: DEFAULT_SCHED_CONCURRENCY,
            scheduler_worker_pool_size: if cpu_num >= 16.0 {
                8
            } else {
                cpu_num.clamp(1., 4.) as usize
            },
            scheduler_pending_write_threshold: ReadableSize::mb(DEFAULT_SCHED_PENDING_WRITE_MB),
            reserve_space: ReadableSize::gb(DEFAULT_RESERVED_SPACE_GB),
            reserve_raft_space: ReadableSize::gb(DEFAULT_RESERVED_RAFT_SPACE_GB),
            enable_async_apply_prewrite: false,
            api_version: 1,
            enable_ttl: false,
            ttl_check_poll_interval: ReadableDuration::hours(12),
            txn_status_cache_capacity: DEFAULT_TXN_STATUS_CACHE_CAPACITY,
            flow_control: FlowControlConfig::default(),
            block_cache: BlockCacheConfig::default(),
            io_rate_limit: IoRateLimitConfig::default(),
            background_error_recovery_window: ReadableDuration::hours(1),
            memory_quota: DEFAULT_TXN_MEMORY_QUOTA_CAPACITY,
            max_ts_allowance_secs: 300,
            max_ts_sync_interval_secs: 30,
            panic_on_invalid_max_ts: true,
        }
    }
}

impl Config {
    pub fn validate_engine_type(&mut self) -> Result<(), Box<dyn Error>> {
        if self.data_dir != DEFAULT_DATA_DIR {
            self.data_dir = config::canonicalize_path(&self.data_dir)?
        }

        let v1_kv_db_path =
            config::canonicalize_sub_path(&self.data_dir, DEFAULT_ROCKSDB_SUB_DIR).unwrap();
        let v2_tablet_path =
            config::canonicalize_sub_path(&self.data_dir, DEFAULT_TABLET_SUB_DIR).unwrap();

        let kv_data_exists = Path::new(&v1_kv_db_path).exists();
        let v2_tablet_exists = Path::new(&v2_tablet_path).exists();
        if kv_data_exists && v2_tablet_exists {
            return Err("Both raft-kv and partitioned-raft-kv's data folders exist".into());
        }

        // v1's data exists, but the engine type is v2
        if kv_data_exists && self.engine == EngineType::RaftKv2 {
            info!(
                "TiKV has data for raft-kv engine but the engine type in config is partitioned-raft-kv. Ignore the config and keep raft-kv instead"
            );
            self.engine = EngineType::RaftKv;
        }

        // if v2's data exists, but the engine type is v1
        if v2_tablet_exists && self.engine == EngineType::RaftKv {
            info!(
                "TiKV has data for partitioned-raft-kv engine but the engine type in config is raft-kv. Ignore the config and keep partitioned-raft-kv instead"
            );
            self.engine = EngineType::RaftKv2;
        }
        Ok(())
    }

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.validate_engine_type()?;

        if self.scheduler_concurrency > MAX_SCHED_CONCURRENCY {
            warn!(
                "TiKV has optimized latch since v4.0, so it is not necessary to set large schedule \
                concurrency. To save memory, change it from {:?} to {:?}",
                self.scheduler_concurrency, MAX_SCHED_CONCURRENCY
            );
            self.scheduler_concurrency = MAX_SCHED_CONCURRENCY;
        }
        if !matches!(self.api_version, 1 | 2) {
            return Err("storage.api_version can only be set to 1 or 2.".into());
        }
        if self.api_version == 2 && !self.enable_ttl {
            return Err(
                "storage.enable_ttl must be true in API V2 because API V2 forces to enable TTL."
                    .into(),
            );
        };
        // max worker pool size should be at least 4.
        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);
        if self.scheduler_worker_pool_size == 0 || self.scheduler_worker_pool_size > max_pool_size {
            return Err(
                format!(
                    "storage.scheduler-worker-pool-size should be greater than 0 and less than or equal to {}",
                    max_pool_size
                ).into()
            );
        }
        self.io_rate_limit.validate()?;
        if self.memory_quota < self.scheduler_pending_write_threshold {
            warn!(
                "scheduler.memory-quota {:?} is smaller than scheduler.scheduler-pending-write-threshold, \
                increase to {:?}",
                self.memory_quota, self.scheduler_pending_write_threshold,
            );
            self.memory_quota = self.scheduler_pending_write_threshold;
        }

        Ok(())
    }

    pub fn api_version(&self) -> ApiVersion {
        match self.api_version {
            1 if self.enable_ttl => ApiVersion::V1ttl,
            1 => ApiVersion::V1,
            2 => ApiVersion::V2,
            _ => unreachable!(),
        }
    }

    pub fn set_api_version(&mut self, api_version: ApiVersion) {
        match api_version {
            ApiVersion::V1 => {
                self.api_version = 1;
                self.enable_ttl = false;
            }
            ApiVersion::V1ttl => {
                self.api_version = 1;
                self.enable_ttl = true;
            }
            ApiVersion::V2 => {
                self.api_version = 2;
                self.enable_ttl = true;
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FlowControlConfig {
    pub enable: bool,
    pub soft_pending_compaction_bytes_limit: ReadableSize,
    pub hard_pending_compaction_bytes_limit: ReadableSize,
    pub memtables_threshold: u64,
    pub l0_files_threshold: u64,
}

impl Default for FlowControlConfig {
    fn default() -> FlowControlConfig {
        FlowControlConfig {
            enable: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(1024),
            memtables_threshold: 5,
            l0_files_threshold: 20,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    #[online_config(skip)]
    pub shared: Option<bool>,
    pub capacity: Option<ReadableSize>,
    #[online_config(skip)]
    pub num_shard_bits: i32,
    #[online_config(skip)]
    pub strict_capacity_limit: bool,
    #[online_config(skip)]
    pub high_pri_pool_ratio: f64,
    #[online_config(skip)]
    pub low_pri_pool_ratio: f64,
    #[online_config(skip)]
    pub memory_allocator: Option<String>,
}

impl Default for BlockCacheConfig {
    fn default() -> BlockCacheConfig {
        BlockCacheConfig {
            shared: None,
            capacity: None,
            num_shard_bits: 6,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.8,
            low_pri_pool_ratio: 0.2,
            memory_allocator: Some(String::from("nodump")),
        }
    }
}

impl BlockCacheConfig {
    fn adjust_shard_bits(&self, capacity: usize) -> i32 {
        let max_shard_count = capacity / MIN_BLOCK_CACHE_SHARD_SIZE;
        if max_shard_count < (2 << self.num_shard_bits) {
            max((max_shard_count as f64).log2() as i32, 1)
        } else {
            self.num_shard_bits
        }
    }

    pub fn build_shared_cache(&self) -> Cache {
        if self.shared == Some(false) {
            warn!("storage.block-cache.shared is deprecated, cache is always shared.");
        }
        let capacity = self.capacity.unwrap_or(FALLBACK_BLOCK_CACHE_CAPACITY).0 as usize;
        let mut cache_opts = LRUCacheOptions::new();
        cache_opts.set_capacity(capacity);
        cache_opts.set_num_shard_bits(self.adjust_shard_bits(capacity) as c_int);
        cache_opts.set_strict_capacity_limit(self.strict_capacity_limit);
        cache_opts.set_high_pri_pool_ratio(self.high_pri_pool_ratio);
        cache_opts.set_low_pri_pool_ratio(self.low_pri_pool_ratio);
        if let Some(allocator) = self.new_memory_allocator() {
            cache_opts.set_memory_allocator(allocator);
        }
        Cache::new_lru_cache(cache_opts)
    }

    fn new_memory_allocator(&self) -> Option<MemoryAllocator> {
        if let Some(ref alloc) = self.memory_allocator {
            match alloc.as_str() {
                #[cfg(feature = "jemalloc")]
                "nodump" => match MemoryAllocator::new_jemalloc_memory_allocator() {
                    Ok(allocator) => {
                        return Some(allocator);
                    }
                    Err(e) => {
                        warn!(
                            "Create jemalloc nodump allocator for block cache failed: {}, continue with default allocator",
                            e
                        );
                    }
                },
                "" => {}
                other => {
                    warn!(
                        "Memory allocator {} is not supported, continue with default allocator",
                        other
                    );
                }
            }
        };
        None
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct IoRateLimitConfig {
    pub max_bytes_per_sec: ReadableSize,
    #[online_config(skip)]
    pub mode: IoRateLimitMode,
    /// When this flag is off, high-priority IOs are counted but not limited.
    /// Default set to false because the optimal throughput target provided
    /// by user might not be the maximum available bandwidth. For
    /// multi-tenancy use case, this flag should be turned on.
    #[online_config(skip)]
    pub strict: bool,
    pub foreground_read_priority: IoPriority,
    pub foreground_write_priority: IoPriority,
    pub flush_priority: IoPriority,
    pub level_zero_compaction_priority: IoPriority,
    pub compaction_priority: IoPriority,
    pub replication_priority: IoPriority,
    pub load_balance_priority: IoPriority,
    pub gc_priority: IoPriority,
    pub import_priority: IoPriority,
    pub export_priority: IoPriority,
    pub other_priority: IoPriority,
}

impl Default for IoRateLimitConfig {
    fn default() -> IoRateLimitConfig {
        IoRateLimitConfig {
            max_bytes_per_sec: ReadableSize::mb(0),
            mode: IoRateLimitMode::WriteOnly,
            strict: false,
            foreground_read_priority: IoPriority::High,
            foreground_write_priority: IoPriority::High,
            flush_priority: IoPriority::High,
            level_zero_compaction_priority: IoPriority::Medium,
            compaction_priority: IoPriority::Low,
            replication_priority: IoPriority::High,
            load_balance_priority: IoPriority::High,
            gc_priority: IoPriority::High,
            import_priority: IoPriority::Medium,
            export_priority: IoPriority::Medium,
            other_priority: IoPriority::High,
        }
    }
}

impl IoRateLimitConfig {
    pub fn build(&self, enable_statistics: bool) -> IoRateLimiter {
        let limiter = IoRateLimiter::new(self.mode, self.strict, enable_statistics);
        limiter.set_io_rate_limit(self.max_bytes_per_sec.0 as usize);
        limiter.set_io_priority(IoType::ForegroundRead, self.foreground_read_priority);
        limiter.set_io_priority(IoType::ForegroundWrite, self.foreground_write_priority);
        limiter.set_io_priority(IoType::Flush, self.flush_priority);
        limiter.set_io_priority(
            IoType::LevelZeroCompaction,
            self.level_zero_compaction_priority,
        );
        limiter.set_io_priority(IoType::Compaction, self.compaction_priority);
        limiter.set_io_priority(IoType::Replication, self.replication_priority);
        limiter.set_io_priority(IoType::LoadBalance, self.load_balance_priority);
        limiter.set_io_priority(IoType::Gc, self.gc_priority);
        limiter.set_io_priority(IoType::Import, self.import_priority);
        limiter.set_io_priority(IoType::Export, self.export_priority);
        limiter.set_io_priority(IoType::RewriteLog, self.compaction_priority);
        limiter.set_io_priority(IoType::Other, self.other_priority);
        limiter
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.other_priority != IoPriority::High {
            warn!(
                "Occasionally some critical IO operations are tagged as IOType::Other, \
                  e.g. IOs are fired from unmanaged threads, thread-local type storage exceeds \
                  capacity. To be on the safe side, change priority for IOType::Other from \
                  {:?} to {:?}",
                self.other_priority,
                IoPriority::High
            );
            self.other_priority = IoPriority::High;
        }
        if self.gc_priority != self.foreground_write_priority {
            warn!(
                "GC writes are merged with foreground writes. To avoid priority inversion, change \
                  priority for IOType::Gc from {:?} to {:?}",
                self.gc_priority, self.foreground_write_priority,
            );
            self.gc_priority = self.foreground_write_priority;
        }
        if self.mode != IoRateLimitMode::WriteOnly {
            return Err(
                "storage.io-rate-limit.mode other than write-only is not supported.".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_validate_storage_config() {
        let mut cfg = Config::default();
        cfg.validate().unwrap();

        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);
        cfg.scheduler_worker_pool_size = max_pool_size;
        cfg.validate().unwrap();

        cfg.scheduler_worker_pool_size = 0;
        cfg.validate().unwrap_err();

        cfg.scheduler_worker_pool_size = max_pool_size + 1;
        cfg.validate().unwrap_err();
    }

    #[test]
    fn test_validate_engine_type_config() {
        let mut cfg = Config::default();
        cfg.engine = EngineType::RaftKv;
        cfg.validate().unwrap();
        assert_eq!(cfg.engine, EngineType::RaftKv);

        cfg.engine = EngineType::RaftKv2;
        cfg.validate().unwrap();
        assert_eq!(cfg.engine, EngineType::RaftKv2);

        let v1_kv_db_path =
            config::canonicalize_sub_path(&cfg.data_dir, DEFAULT_ROCKSDB_SUB_DIR).unwrap();
        fs::create_dir_all(&v1_kv_db_path).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.engine, EngineType::RaftKv);
        fs::remove_dir_all(&v1_kv_db_path).unwrap();

        let v2_tablet_path =
            config::canonicalize_sub_path(&cfg.data_dir, DEFAULT_TABLET_SUB_DIR).unwrap();
        fs::create_dir_all(&v2_tablet_path).unwrap();
        cfg.engine = EngineType::RaftKv;
        cfg.validate().unwrap();
        assert_eq!(cfg.engine, EngineType::RaftKv2);

        // both v1 and v2 data exists, throw error
        fs::create_dir_all(&v1_kv_db_path).unwrap();
        cfg.validate().unwrap_err();
        fs::remove_dir_all(&v1_kv_db_path).unwrap();
        fs::remove_dir_all(&v2_tablet_path).unwrap();
    }

    #[test]
    fn test_adjust_shard_bits() {
        let config = BlockCacheConfig::default();
        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(1).0 as usize);
        assert_eq!(shard_bits, 3);

        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(4).0 as usize);
        assert_eq!(shard_bits, 5);

        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(8).0 as usize);
        assert_eq!(shard_bits, 6);

        let shard_bits = config.adjust_shard_bits(ReadableSize::mb(1).0 as usize);
        assert_eq!(shard_bits, 1);
    }
}
