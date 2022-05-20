// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TiKvConfig` type, which is in turn
//! made up of many other configuration types.

use std::{
    cmp,
    collections::HashMap,
    error::Error,
    fs, i32,
    io::{Error as IoError, ErrorKind, Write},
    path::Path,
    sync::{Arc, RwLock},
    usize,
};

use api_version::ApiV1Ttl;
use causal_ts::Config as CausalTsConfig;
use encryption_export::DataKeyManager;
use engine_rocks::{
    config::{self as rocks_config, BlobRunMode, CompressionType, LogLevel},
    get_env,
    properties::MvccPropertiesCollectorFactory,
    raw::{
        BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
        DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, Env, LRUCacheOptions,
        TitanDBOptions,
    },
    raw_util::CFOptions,
    util::{FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform},
    RaftDBLogger, RangePropertiesCollectorFactory, RocksEngine, RocksEventListener,
    RocksSstPartitionerFactory, RocksdbLogger, TtlPropertiesCollectorFactory,
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use engine_traits::{
    CFOptionsExt, ColumnFamilyOptions as ColumnFamilyOptionsTrait, DBOptionsExt, CF_DEFAULT,
    CF_LOCK, CF_RAFT, CF_WRITE,
};
use file_system::{IOPriority, IORateLimiter};
use keys::region_raft_prefix_len;
use kvproto::kvrpcpb::ApiVersion;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig, Result as CfgResult};
use pd_client::Config as PdConfig;
use raft_log_engine::{
    RaftEngineConfig as RawRaftEngineConfig, ReadableSize as RaftEngineReadableSize,
};
use raftstore::{
    coprocessor::{Config as CopConfig, RegionInfoAccessor},
    store::{CompactionGuardGeneratorFactory, Config as RaftstoreConfig, SplitConfig},
};
use resource_metering::Config as ResourceMeteringConfig;
use security::SecurityConfig;
use tikv_util::{
    config::{
        self, LogFormat, RaftDataStateMachine, ReadableDuration, ReadableSize, TomlWriter, GIB, MIB,
    },
    sys::SysQuota,
    time::duration_to_sec,
    yatp_pool,
};

use crate::{
    coprocessor_v2::Config as CoprocessorV2Config,
    import::Config as ImportConfig,
    server::{
        gc_worker::{GcConfig, RawCompactionFilterFactory, WriteCompactionFilterFactory},
        lock_manager::Config as PessimisticTxnConfig,
        ttl::TtlCompactionFilterFactory,
        Config as ServerConfig, CONFIG_ROCKSDB_GAUGE,
    },
    storage::config::{Config as StorageConfig, DEFAULT_DATA_DIR},
};

pub const DEFAULT_ROCKSDB_SUB_DIR: &str = "db";

/// By default, block cache size will be set to 45% of system memory.
pub const BLOCK_CACHE_RATE: f64 = 0.45;
/// By default, TiKV will try to limit memory usage to 75% of system memory.
pub const MEMORY_USAGE_LIMIT_RATE: f64 = 0.75;

/// Min block cache shard's size. If a shard is too small, the index/filter data may not fit one shard
pub const MIN_BLOCK_CACHE_SHARD_SIZE: usize = 128 * MIB as usize;

/// Maximum of 15% of system memory can be used by Raft Engine. Normally its
/// memory usage is much smaller than that.
const RAFT_ENGINE_MEMORY_LIMIT_RATE: f64 = 0.15;

const LOCKCF_MIN_MEM: usize = 256 * MIB as usize;
const LOCKCF_MAX_MEM: usize = GIB as usize;
const RAFT_MIN_MEM: usize = 256 * MIB as usize;
const RAFT_MAX_MEM: usize = 2 * GIB as usize;
const LAST_CONFIG_FILE: &str = "last_tikv.toml";
const TMP_CONFIG_FILE: &str = "tmp_tikv.toml";
const MAX_BLOCK_SIZE: usize = 32 * MIB as usize;

fn memory_limit_for_cf(is_raft_db: bool, cf: &str, total_mem: u64) -> ReadableSize {
    let (ratio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.02, RAFT_MIN_MEM, RAFT_MAX_MEM),
        (false, CF_DEFAULT) => (0.25, 0, usize::MAX),
        (false, CF_LOCK) => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        (false, CF_WRITE) => (0.15, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    ReadableSize::mb(size as u64 / MIB)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanCfConfig {
    #[online_config(skip)]
    pub min_blob_size: ReadableSize,
    #[online_config(skip)]
    pub blob_file_compression: CompressionType,
    #[online_config(skip)]
    pub blob_cache_size: ReadableSize,
    #[online_config(skip)]
    pub min_gc_batch_size: ReadableSize,
    #[online_config(skip)]
    pub max_gc_batch_size: ReadableSize,
    #[online_config(skip)]
    pub discardable_ratio: f64,
    #[online_config(skip)]
    pub sample_ratio: f64,
    #[online_config(skip)]
    pub merge_small_file_threshold: ReadableSize,
    pub blob_run_mode: BlobRunMode,
    #[online_config(skip)]
    pub level_merge: bool,
    #[online_config(skip)]
    pub range_merge: bool,
    #[online_config(skip)]
    pub max_sorted_runs: i32,
    #[online_config(skip)]
    pub gc_merge_rewrite: bool,
}

impl Default for TitanCfConfig {
    fn default() -> Self {
        Self {
            min_blob_size: ReadableSize::kb(1), // disable titan default
            blob_file_compression: CompressionType::Lz4,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_batch_size: ReadableSize::mb(16),
            max_gc_batch_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: 0.1,
            merge_small_file_threshold: ReadableSize::mb(8),
            blob_run_mode: BlobRunMode::Normal,
            level_merge: false,
            range_merge: true,
            max_sorted_runs: 20,
            gc_merge_rewrite: false,
        }
    }
}

impl TitanCfConfig {
    fn build_opts(&self) -> TitanDBOptions {
        let mut opts = TitanDBOptions::new();
        opts.set_min_blob_size(self.min_blob_size.0 as u64);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        opts.set_blob_cache(self.blob_cache_size.0 as usize, -1, false, 0.0);
        opts.set_min_gc_batch_size(self.min_gc_batch_size.0 as u64);
        opts.set_max_gc_batch_size(self.max_gc_batch_size.0 as u64);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_sample_ratio(self.sample_ratio);
        opts.set_merge_small_file_threshold(self.merge_small_file_threshold.0 as u64);
        opts.set_blob_run_mode(self.blob_run_mode.into());
        opts.set_level_merge(self.level_merge);
        opts.set_range_merge(self.range_merge);
        opts.set_max_sorted_runs(self.max_sorted_runs);
        opts.set_gc_merge_rewrite(self.gc_merge_rewrite);
        opts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackgroundJobLimits {
    max_background_jobs: u32,
    max_background_flushes: u32,
    max_sub_compactions: u32,
    max_titan_background_gc: u32,
}

const KVDB_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 9,
    max_background_flushes: 3,
    max_sub_compactions: 3,
    max_titan_background_gc: 4,
};

const RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 4,
    max_background_flushes: 1,
    max_sub_compactions: 2,
    max_titan_background_gc: 4,
};

// `defaults` serves as an upper bound for returning limits.
fn get_background_job_limits_impl(
    cpu_num: u32,
    defaults: &BackgroundJobLimits,
) -> BackgroundJobLimits {
    // At the minimum, we should have two background jobs: one for flush and one for compaction.
    // Otherwise, the number of background jobs should not exceed cpu_num - 1.
    let max_background_jobs = cmp::max(2, cmp::min(defaults.max_background_jobs, cpu_num - 1));
    // Scale flush threads proportionally to cpu cores. Also make sure the number of flush
    // threads doesn't exceed total jobs.
    let max_background_flushes = cmp::min(
        (max_background_jobs + 3) / 4,
        defaults.max_background_flushes,
    );
    // Cap max_sub_compactions to allow at least two compactions.
    let max_compactions = max_background_jobs - max_background_flushes;
    let max_sub_compactions: u32 = cmp::max(
        1,
        cmp::min(defaults.max_sub_compactions, (max_compactions - 1) as u32),
    );
    // Maximum background GC threads for Titan
    let max_titan_background_gc = cmp::min(defaults.max_titan_background_gc, cpu_num);

    BackgroundJobLimits {
        max_background_jobs,
        max_background_flushes,
        max_sub_compactions,
        max_titan_background_gc,
    }
}

fn get_background_job_limits(defaults: &BackgroundJobLimits) -> BackgroundJobLimits {
    let cpu_num = cmp::max(SysQuota::cpu_cores_quota() as u32, 1);
    get_background_job_limits_impl(cpu_num, defaults)
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            #[online_config(skip)]
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            #[online_config(skip)]
            pub disable_block_cache: bool,
            #[online_config(skip)]
            pub cache_index_and_filter_blocks: bool,
            #[online_config(skip)]
            pub pin_l0_filter_and_index_blocks: bool,
            #[online_config(skip)]
            pub use_bloom_filter: bool,
            #[online_config(skip)]
            pub optimize_filters_for_hits: bool,
            #[online_config(skip)]
            pub whole_key_filtering: bool,
            #[online_config(skip)]
            pub bloom_filter_bits_per_key: i32,
            #[online_config(skip)]
            pub block_based_bloom_filter: bool,
            #[online_config(skip)]
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "rocks_config::compression_type_level_serde")]
            #[online_config(skip)]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            #[online_config(skip)]
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "rocks_config::compaction_pri_serde")]
            #[online_config(skip)]
            pub compaction_pri: CompactionPriority,
            #[online_config(skip)]
            pub dynamic_level_bytes: bool,
            #[online_config(skip)]
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "rocks_config::compaction_style_serde")]
            #[online_config(skip)]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub disable_write_stall: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
            #[online_config(skip)]
            pub force_consistency_checks: bool,
            #[online_config(skip)]
            pub prop_size_index_distance: u64,
            #[online_config(skip)]
            pub prop_keys_index_distance: u64,
            #[online_config(skip)]
            pub enable_doubly_skiplist: bool,
            #[online_config(skip)]
            pub enable_compaction_guard: bool,
            #[online_config(skip)]
            pub compaction_guard_min_output_file_size: ReadableSize,
            #[online_config(skip)]
            pub compaction_guard_max_output_file_size: ReadableSize,
            #[serde(with = "rocks_config::compression_type_serde")]
            #[online_config(skip)]
            pub bottommost_level_compression: DBCompressionType,
            #[online_config(skip)]
            pub bottommost_zstd_compression_dict_size: i32,
            #[online_config(skip)]
            pub bottommost_zstd_compression_sample_size: i32,
            #[online_config(submodule)]
            pub titan: TitanCfConfig,
        }

        impl $name {
            fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.block_size.0 as usize > MAX_BLOCK_SIZE {
                    return Err(format!(
                        "invalid block-size {} for {}, exceed max size {}",
                        self.block_size.0,
                        stringify!($name),
                        MAX_BLOCK_SIZE
                    )
                    .into());
                }
                Ok(())
            }
        }
    };
}

macro_rules! write_into_metrics {
    ($cf:expr, $tag:expr, $metrics:expr) => {{
        $metrics
            .with_label_values(&[$tag, "block_size"])
            .set($cf.block_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "block_cache_size"])
            .set($cf.block_cache_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "disable_block_cache"])
            .set(($cf.disable_block_cache as i32).into());

        $metrics
            .with_label_values(&[$tag, "cache_index_and_filter_blocks"])
            .set(($cf.cache_index_and_filter_blocks as i32).into());
        $metrics
            .with_label_values(&[$tag, "pin_l0_filter_and_index_blocks"])
            .set(($cf.pin_l0_filter_and_index_blocks as i32).into());

        $metrics
            .with_label_values(&[$tag, "use_bloom_filter"])
            .set(($cf.use_bloom_filter as i32).into());
        $metrics
            .with_label_values(&[$tag, "optimize_filters_for_hits"])
            .set(($cf.optimize_filters_for_hits as i32).into());
        $metrics
            .with_label_values(&[$tag, "whole_key_filtering"])
            .set(($cf.whole_key_filtering as i32).into());
        $metrics
            .with_label_values(&[$tag, "bloom_filter_bits_per_key"])
            .set($cf.bloom_filter_bits_per_key.into());
        $metrics
            .with_label_values(&[$tag, "block_based_bloom_filter"])
            .set(($cf.block_based_bloom_filter as i32).into());

        $metrics
            .with_label_values(&[$tag, "read_amp_bytes_per_bit"])
            .set($cf.read_amp_bytes_per_bit.into());
        $metrics
            .with_label_values(&[$tag, "write_buffer_size"])
            .set($cf.write_buffer_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "max_write_buffer_number"])
            .set($cf.max_write_buffer_number.into());
        $metrics
            .with_label_values(&[$tag, "min_write_buffer_number_to_merge"])
            .set($cf.min_write_buffer_number_to_merge.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_base"])
            .set($cf.max_bytes_for_level_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "target_file_size_base"])
            .set($cf.target_file_size_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "level0_file_num_compaction_trigger"])
            .set($cf.level0_file_num_compaction_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_slowdown_writes_trigger"])
            .set($cf.level0_slowdown_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_stop_writes_trigger"])
            .set($cf.level0_stop_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "max_compaction_bytes"])
            .set($cf.max_compaction_bytes.0 as f64);
        $metrics
            .with_label_values(&[$tag, "dynamic_level_bytes"])
            .set(($cf.dynamic_level_bytes as i32).into());
        $metrics
            .with_label_values(&[$tag, "num_levels"])
            .set($cf.num_levels.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_multiplier"])
            .set($cf.max_bytes_for_level_multiplier.into());

        $metrics
            .with_label_values(&[$tag, "disable_auto_compactions"])
            .set(($cf.disable_auto_compactions as i32).into());
        $metrics
            .with_label_values(&[$tag, "disable_write_stall"])
            .set(($cf.disable_write_stall as i32).into());
        $metrics
            .with_label_values(&[$tag, "soft_pending_compaction_bytes_limit"])
            .set($cf.soft_pending_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "hard_pending_compaction_bytes_limit"])
            .set($cf.hard_pending_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "force_consistency_checks"])
            .set(($cf.force_consistency_checks as i32).into());
        $metrics
            .with_label_values(&[$tag, "enable_doubly_skiplist"])
            .set(($cf.enable_doubly_skiplist as i32).into());
        $metrics
            .with_label_values(&[$tag, "titan_min_blob_size"])
            .set($cf.titan.min_blob_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_blob_cache_size"])
            .set($cf.titan.blob_cache_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_min_gc_batch_size"])
            .set($cf.titan.min_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_max_gc_batch_size"])
            .set($cf.titan.max_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_discardable_ratio"])
            .set($cf.titan.discardable_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_sample_ratio"])
            .set($cf.titan.sample_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_merge_small_file_threshold"])
            .set($cf.titan.merge_small_file_threshold.0 as f64);
    }};
}

macro_rules! build_cf_opt {
    ($opt:ident, $cf_name:ident, $cache:ident, $region_info_provider:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        if let Some(cache) = $cache {
            block_base_opts.set_block_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity($opt.block_cache_size.0 as usize);
            block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
        }
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_key,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.set_num_levels($opt.num_levels);
        assert!($opt.compression_per_level.len() >= $opt.num_levels as usize);
        let compression_per_level = $opt.compression_per_level[..$opt.num_levels as usize].to_vec();
        cf_opts.compression_per_level(compression_per_level.as_slice());
        cf_opts.bottommost_compression($opt.bottommost_level_compression);
        // To set for bottommost level sst compression. The first 3 parameters refer to the
        // default value in `CompressionOptions` in `rocksdb/include/rocksdb/advanced_options.h`.
        cf_opts.set_bottommost_level_compression_options(
            -14,   /* window_bits */
            32767, /* level */
            0,     /* strategy */
            $opt.bottommost_zstd_compression_dict_size,
            $opt.bottommost_zstd_compression_sample_size,
        );
        cf_opts.set_write_buffer_size($opt.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        cf_opts.set_compaction_style($opt.compaction_style);
        cf_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        cf_opts.set_disable_write_stall($opt.disable_write_stall);
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit($opt.hard_pending_compaction_bytes_limit.0);
        cf_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        cf_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            cf_opts.set_doubly_skiplist();
        }
        if $opt.enable_compaction_guard {
            if let Some(provider) = $region_info_provider {
                let factory = CompactionGuardGeneratorFactory::new(
                    $cf_name,
                    provider.clone(),
                    $opt.compaction_guard_min_output_file_size.0,
                )
                .unwrap();
                cf_opts.set_sst_partitioner_factory(RocksSstPartitionerFactory(factory));
                cf_opts.set_target_file_size_base($opt.compaction_guard_max_output_file_size.0);
            } else {
                warn!("compaction guard is disabled due to region info provider not available")
            }
        }
        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_cf(false, CF_DEFAULT, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: true,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            titan: TitanCfConfig::default(),
            bottommost_level_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
    ) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, CF_DEFAULT, cache, region_info_accessor);
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        match api_version {
            ApiVersion::V1 => {
                // nothing to do
            }
            ApiVersion::V1ttl => {
                cf_opts.add_table_properties_collector_factory(
                    "tikv.ttl-properties-collector",
                    TtlPropertiesCollectorFactory::<ApiV1Ttl>::default(),
                );
                cf_opts
                    .set_compaction_filter_factory(
                        "ttl_compaction_filter_factory",
                        TtlCompactionFilterFactory::<ApiV1Ttl>::default(),
                    )
                    .unwrap();
            }
            ApiVersion::V2 => {
                cf_opts
                    .set_compaction_filter_factory(
                        "apiv2_gc_compaction_filter_factory",
                        RawCompactionFilterFactory,
                    )
                    .unwrap();
            }
        }
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        // Setting blob_run_mode=read_only effectively disable Titan.
        let titan = TitanCfConfig {
            blob_run_mode: BlobRunMode::ReadOnly,
            ..Default::default()
        };

        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_cf(false, CF_WRITE, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: true,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            titan,
            bottommost_level_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
    ) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, CF_WRITE, cache, region_info_accessor);
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                FixedSuffixSliceTransform::new(8),
            )
            .unwrap();
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        cf_opts.add_table_properties_collector_factory(
            "tikv.mvcc-properties-collector",
            MvccPropertiesCollectorFactory::default(),
        );
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts
            .set_compaction_filter_factory(
                "write_compaction_filter_factory",
                WriteCompactionFilterFactory,
            )
            .unwrap();
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        // Setting blob_run_mode=read_only effectively disable Titan.
        let titan = TitanCfConfig {
            blob_run_mode: BlobRunMode::ReadOnly,
            ..Default::default()
        };

        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: memory_limit_for_cf(false, CF_LOCK, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(32),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: false,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            titan,
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(self, CF_LOCK, cache, no_region_info_accessor);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let titan = TitanCfConfig {
            blob_run_mode: BlobRunMode::ReadOnly,
            ..Default::default()
        };
        RaftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: false,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            titan,
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(self, CF_RAFT, cache, no_region_info_accessor);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
// Note that Titan is still an experimental feature. Once enabled, it can't fall back.
// Forced fallback may result in data loss.
pub struct TitanDBConfig {
    pub enabled: bool,
    pub dirname: String,
    pub disable_gc: bool,
    pub max_background_gc: i32,
    // The value of this field will be truncated to seconds.
    pub purge_obsolete_files_period: ReadableDuration,
}

impl Default for TitanDBConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dirname: "".to_owned(),
            disable_gc: false,
            max_background_gc: 4,
            purge_obsolete_files_period: ReadableDuration::secs(10),
        }
    }
}

impl TitanDBConfig {
    fn build_opts(&self) -> TitanDBOptions {
        let mut opts = TitanDBOptions::new();
        opts.set_dirname(&self.dirname);
        opts.set_disable_background_gc(self.disable_gc);
        opts.set_max_background_gc(self.max_background_gc);
        opts.set_purge_obsolete_files_period(self.purge_obsolete_files_period.as_secs() as usize);
        opts
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[online_config(skip)]
    pub info_log_level: LogLevel,
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_max_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[online_config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    #[online_config(skip)]
    pub rate_limiter_refill_period: ReadableDuration,
    #[serde(with = "rocks_config::rate_limiter_mode_serde")]
    #[online_config(skip)]
    pub rate_limiter_mode: DBRateLimiterMode,
    // deprecated. use rate_limiter_auto_tuned.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub auto_tuned: Option<bool>,
    pub rate_limiter_auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(skip)]
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    // deprecated. TiKV will use a new write mode when set `enable_pipelined_write` false and fall
    // back to write mode in 3.0 when set `enable_pipelined_write` true. The code of multi-batch-write
    // in RocksDB has been removed.
    #[online_config(skip)]
    pub enable_multi_batch_write: bool,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(submodule)]
    pub defaultcf: DefaultCfConfig,
    #[online_config(submodule)]
    pub writecf: WriteCfConfig,
    #[online_config(submodule)]
    pub lockcf: LockCfConfig,
    #[online_config(submodule)]
    pub raftcf: RaftCfConfig,
    #[online_config(skip)]
    pub titan: TitanDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        let bg_job_limits = get_background_job_limits(&KVDB_DEFAULT_BACKGROUND_JOB_LIMITS);
        let titan_config = TitanDBConfig {
            max_background_gc: bg_job_limits.max_titan_background_gc as i32,
            ..Default::default()
        };
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: bg_job_limits.max_background_jobs as i32,
            max_background_flushes: bg_job_limits.max_background_flushes as i32,
            max_manifest_file_size: ReadableSize::mb(128),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: LogLevel::Info,
            rate_bytes_per_sec: ReadableSize::gb(10),
            rate_limiter_refill_period: ReadableDuration::millis(100),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: None, // deprecated
            rate_limiter_auto_tuned: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: bg_job_limits.max_sub_compactions as u32,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: false,
            enable_multi_batch_write: true, // deprecated
            enable_unordered_write: false,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
            raftcf: RaftCfConfig::default(),
            titan: titan_config,
        }
    }
}

impl DbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        // RocksDB will cap flush and compaction threads to at least one
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_compactions(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if self.rate_bytes_per_sec.0 > 0 {
            if self.rate_limiter_auto_tuned {
                opts.set_writeampbasedratelimiter_with_auto_tuned(
                    self.rate_bytes_per_sec.0 as i64,
                    (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                    self.rate_limiter_mode,
                    self.rate_limiter_auto_tuned,
                );
            } else {
                opts.set_ratelimiter_with_auto_tuned(
                    self.rate_bytes_per_sec.0 as i64,
                    (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                    self.rate_limiter_mode,
                    self.rate_limiter_auto_tuned,
                );
            }
        }

        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        let enable_pipelined_commit = !self.enable_pipelined_write && !self.enable_unordered_write;
        opts.enable_pipelined_commit(enable_pipelined_commit);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.set_info_log(RocksdbLogger::default());
        opts.set_info_log_level(self.info_log_level.into());
        if self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }
        opts
    }

    pub fn build_cf_opts(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
    ) -> Vec<CFOptions<'_>> {
        vec![
            CFOptions::new(
                CF_DEFAULT,
                self.defaultcf
                    .build_opt(cache, region_info_accessor, api_version),
            ),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt(cache)),
            CFOptions::new(
                CF_WRITE,
                self.writecf.build_opt(cache, region_info_accessor),
            ),
            // TODO: remove CF_RAFT.
            CFOptions::new(CF_RAFT, self.raftcf.build_opt(cache)),
        ]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        self.lockcf.validate()?;
        self.writecf.validate()?;
        self.raftcf.validate()?;
        self.titan.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("RocksDB.unordered_write does not support Titan".into());
            }
            self.enable_pipelined_write = false;
        }

        // Since the following configuration supports online update, in order to
        // prevent mistakenly inputting too large values, the max limit is made
        // according to the cpu quota * 10. Notice 10 is only an estimate, not an
        // empirical value.
        let limit = SysQuota::cpu_cores_quota() as i32 * 10;
        if self.max_background_jobs <= 0 || self.max_background_jobs > limit {
            return Err(format!(
                "max_background_jobs should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        if self.max_background_flushes <= 0 || self.max_background_flushes > limit {
            return Err(format!(
                "max_background_flushes should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        Ok(())
    }

    fn write_into_metrics(&self) {
        write_into_metrics!(self.defaultcf, CF_DEFAULT, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.lockcf, CF_LOCK, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.writecf, CF_WRITE, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.raftcf, CF_RAFT, CONFIG_ROCKSDB_GAUGE);
    }
}

cf_config!(RaftDefaultCfConfig);

impl Default for RaftDefaultCfConfig {
    fn default() -> RaftDefaultCfConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_cf(true, CF_DEFAULT, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: false,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            titan: TitanCfConfig::default(),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl RaftDefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(self, CF_DEFAULT, cache, no_region_info_accessor);
        let f = FixedPrefixSliceTransform::new(region_raft_prefix_len());
        cf_opts
            .set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform", f)
            .unwrap();
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

// RocksDB Env associate thread pools of multiple instances from the same process.
// When construct Options, options.env is set to same singleton Env::Default() object.
// So total max_background_jobs = max(rocksdb.max_background_jobs, raftdb.max_background_jobs)
// But each instance will limit their background jobs according to their own max_background_jobs
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_max_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[online_config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    #[online_config(skip)]
    pub info_log_level: LogLevel,
    #[online_config(skip)]
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(skip)]
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(submodule)]
    pub defaultcf: RaftDefaultCfConfig,
    #[online_config(skip)]
    pub titan: TitanDBConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        let bg_job_limits = get_background_job_limits(&RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS);
        let titan_config = TitanDBConfig {
            max_background_gc: bg_job_limits.max_titan_background_gc as i32,
            ..Default::default()
        };
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: bg_job_limits.max_background_jobs as i32,
            max_background_flushes: bg_job_limits.max_background_flushes as i32,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: LogLevel::Info,
            max_sub_compactions: bg_job_limits.max_sub_compactions as u32,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
            allow_concurrent_memtable_write: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcf: RaftDefaultCfConfig::default(),
            titan: titan_config,
        }
    }
}

impl RaftDbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_compactions(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        opts.set_info_log(RaftDBLogger::default());
        opts.set_info_log_level(self.info_log_level.into());
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(RocksEventListener::new("raft", None));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        // TODO maybe create a new env for raft engine
        if self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }

        opts
    }

    pub fn build_cf_opts(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache))]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("raftdb: unordered_write is not compatible with Titan".into());
            }
            if self.enable_pipelined_write {
                return Err(
                    "raftdb: pipelined_write is not compatible with unordered_write".into(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default, rename_all = "kebab-case")]
pub struct RaftEngineConfig {
    pub enable: bool,
    #[serde(flatten)]
    config: RawRaftEngineConfig,
}

impl Default for RaftEngineConfig {
    fn default() -> Self {
        Self {
            enable: true,
            config: RawRaftEngineConfig::default(),
        }
    }
}

impl RaftEngineConfig {
    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.config.sanitize().map_err(Box::new)?;
        if self.config.memory_limit.is_none() {
            let total_mem = SysQuota::memory_limit_in_bytes() as f64;
            let memory_limit = total_mem * RAFT_ENGINE_MEMORY_LIMIT_RATE;
            self.config.memory_limit = Some(RaftEngineReadableSize(memory_limit as u64));
        }
        Ok(())
    }

    pub fn config(&self) -> RawRaftEngineConfig {
        self.config.clone()
    }

    pub fn mut_config(&mut self) -> &mut RawRaftEngineConfig {
        &mut self.config
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DBType {
    Kv,
    Raft,
}

pub struct DBConfigManger {
    db: RocksEngine,
    db_type: DBType,
    shared_block_cache: bool,
}

impl DBConfigManger {
    pub fn new(db: RocksEngine, db_type: DBType, shared_block_cache: bool) -> Self {
        DBConfigManger {
            db,
            db_type,
            shared_block_cache,
        }
    }
}

impl DBConfigManger {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.db.set_db_options(opts)?;
        Ok(())
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        self.db.set_options_cf(cf, opts)?;
        // Write config to metric
        for (cfg_name, cfg_value) in opts {
            let cfg_value = match cfg_value {
                v if *v == "true" => Ok(1f64),
                v if *v == "false" => Ok(0f64),
                v => v.parse::<f64>(),
            };
            if let Ok(v) = cfg_value {
                CONFIG_ROCKSDB_GAUGE
                    .with_label_values(&[cf, cfg_name])
                    .set(v);
            }
        }
        Ok(())
    }

    fn set_block_cache_size(&self, cf: &str, size: ReadableSize) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        if self.shared_block_cache {
            return Err("shared block cache is enabled, change cache size through \
                 block-cache.capacity in storage module instead"
                .into());
        }
        let opt = self.db.get_options_cf(cf)?;
        opt.set_block_cache_capacity(size.0)?;
        // Write config to metric
        CONFIG_ROCKSDB_GAUGE
            .with_label_values(&[cf, "block_cache_size"])
            .set(size.0 as f64);
        Ok(())
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> Result<(), Box<dyn Error>> {
        let mut opt = self.db.as_inner().get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)?;
        Ok(())
    }

    fn set_rate_limiter_auto_tuned(
        &self,
        rate_limiter_auto_tuned: bool,
    ) -> Result<(), Box<dyn Error>> {
        let mut opt = self.db.as_inner().get_db_options();
        opt.set_auto_tuned(rate_limiter_auto_tuned)?;
        // double check the new state
        let new_auto_tuned = opt.get_auto_tuned();
        if new_auto_tuned.is_none() || new_auto_tuned.unwrap() != rate_limiter_auto_tuned {
            return Err("fail to set rate_limiter_auto_tuned".into());
        }
        Ok(())
    }

    fn set_max_background_jobs(&self, max_background_jobs: i32) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[("max_background_jobs", &max_background_jobs.to_string())])?;
        Ok(())
    }

    fn set_max_background_flushes(
        &self,
        max_background_flushes: i32,
    ) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[(
            "max_background_flushes",
            &max_background_flushes.to_string(),
        )])?;
        Ok(())
    }

    fn validate_cf(&self, cf: &str) -> Result<(), Box<dyn Error>> {
        match (self.db_type, cf) {
            (DBType::Kv, CF_DEFAULT)
            | (DBType::Kv, CF_WRITE)
            | (DBType::Kv, CF_LOCK)
            | (DBType::Kv, CF_RAFT)
            | (DBType::Raft, CF_DEFAULT) => Ok(()),
            _ => Err(format!("invalid cf {:?} for db {:?}", cf, self.db_type).into()),
        }
    }
}

impl ConfigManager for DBConfigManger {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let change_str = format!("{:?}", change);
        let mut change: Vec<(String, ConfigValue)> = change.into_iter().collect();
        let cf_config = change.drain_filter(|(name, _)| name.ends_with("cf"));
        for (cf_name, cf_change) in cf_config {
            if let ConfigValue::Module(mut cf_change) = cf_change {
                // defaultcf -> default
                let cf_name = &cf_name[..(cf_name.len() - 2)];
                if let Some(v) = cf_change.remove("block_cache_size") {
                    // currently we can't modify block_cache_size via set_options_cf
                    self.set_block_cache_size(cf_name, v.into())?;
                }
                if let Some(ConfigValue::Module(titan_change)) = cf_change.remove("titan") {
                    for (name, value) in titan_change {
                        cf_change.insert(name, value);
                    }
                }
                if !cf_change.is_empty() {
                    let cf_change = config_value_to_string(cf_change.into_iter().collect());
                    let cf_change_slice = config_to_slice(&cf_change);
                    self.set_cf_config(cf_name, &cf_change_slice)?;
                }
            }
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_bytes_per_sec")
            .next()
        {
            let rate_bytes_per_sec: ReadableSize = rate_bytes_config.1.into();
            self.set_rate_bytes_per_sec(rate_bytes_per_sec.0 as i64)?;
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_limiter_auto_tuned")
            .next()
        {
            let rate_limiter_auto_tuned: bool = rate_bytes_config.1.into();
            self.set_rate_limiter_auto_tuned(rate_limiter_auto_tuned)?;
        }

        if let Some(background_jobs_config) = change
            .drain_filter(|(name, _)| name == "max_background_jobs")
            .next()
        {
            let max_background_jobs = background_jobs_config.1.into();
            self.set_max_background_jobs(max_background_jobs)?;
        }

        if let Some(background_flushes_config) = change
            .drain_filter(|(name, _)| name == "max_background_flushes")
            .next()
        {
            let max_background_flushes = background_flushes_config.1.into();
            self.set_max_background_flushes(max_background_flushes)?;
        }

        if !change.is_empty() {
            let change = config_value_to_string(change);
            let change_slice = config_to_slice(&change);
            self.set_db_config(&change_slice)?;
        }
        info!(
            "rocksdb config changed";
            "db" => ?self.db_type,
            "change" => change_str
        );
        Ok(())
    }
}

fn config_to_slice(config_change: &[(String, String)]) -> Vec<(&str, &str)> {
    config_change
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

// Convert `ConfigValue` to formatted String that can pass to `DB::set_db_options`
fn config_value_to_string(config_change: Vec<(String, ConfigValue)>) -> Vec<(String, String)> {
    config_change
        .into_iter()
        .filter_map(|(name, value)| {
            let v = match value {
                d @ ConfigValue::Duration(_) => {
                    let d: ReadableDuration = d.into();
                    Some(d.as_secs().to_string())
                }
                s @ ConfigValue::Size(_) => {
                    let s: ReadableSize = s.into();
                    Some(s.0.to_string())
                }
                ConfigValue::Module(_) => unreachable!(),
                v => Some(format!("{}", v)),
            };
            v.map(|v| (name, v))
        })
        .collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub job: String,

    // Push is deprecated.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub interval: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub address: String,
}

impl Default for MetricConfig {
    fn default() -> MetricConfig {
        MetricConfig {
            interval: ReadableDuration::secs(15),
            address: "".to_owned(),
            job: "tikv".to_owned(),
        }
    }
}

pub mod log_level_serde {
    use serde::{
        de::{Error, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use slog::Level;
    use tikv_util::logger::{get_level_by_string, get_string_by_level};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_level_by_string(&string)
            .ok_or_else(|| D::Error::invalid_value(Unexpected::Str(&string), &"a valid log level"))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_level(*value).serialize(serializer)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct UnifiedReadPoolConfig {
    #[online_config(skip)]
    pub min_thread_count: usize,
    pub max_thread_count: usize,
    #[online_config(skip)]
    pub stack_size: ReadableSize,
    #[online_config(skip)]
    pub max_tasks_per_worker: usize,
    // FIXME: Add more configs when they are effective in yatp
}

impl UnifiedReadPoolConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.min_thread_count == 0 {
            return Err("readpool.unified.min-thread-count should be > 0"
                .to_string()
                .into());
        }
        if self.max_thread_count < self.min_thread_count {
            return Err(
                "readpool.unified.max-thread-count should be >= readpool.unified.min-thread-count"
                    .to_string()
                    .into(),
            );
        }
        let limit = cmp::max(
            UNIFIED_READPOOL_MIN_CONCURRENCY,
            SysQuota::cpu_cores_quota() as usize,
        );
        if self.max_thread_count > limit {
            return Err(format!(
                "readpool.unified.max-thread-count should be smaller than {}",
                limit
            )
            .into());
        }
        if self.stack_size.0 < ReadableSize::mb(2).0 {
            return Err("readpool.unified.stack-size should be >= 2mb"
                .to_string()
                .into());
        }
        if self.max_tasks_per_worker <= 1 {
            return Err("readpool.unified.max-tasks-per-worker should be > 1"
                .to_string()
                .into());
        }
        Ok(())
    }
}

pub const UNIFIED_READPOOL_MIN_CONCURRENCY: usize = 4;

// FIXME: Use macros to generate it if yatp is used elsewhere besides readpool.
impl Default for UnifiedReadPoolConfig {
    fn default() -> UnifiedReadPoolConfig {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(UNIFIED_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            min_thread_count: 1,
            max_thread_count: concurrency,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
            max_tasks_per_worker: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
        }
    }
}

#[cfg(test)]
mod unified_read_pool_tests {
    use super::*;

    #[test]
    fn test_validate() {
        let cfg = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2000,
        };
        assert!(cfg.validate().is_ok());
        let cfg = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: cmp::max(
                UNIFIED_READPOOL_MIN_CONCURRENCY,
                SysQuota::cpu_cores_quota() as usize,
            ),
            ..cfg
        };
        assert!(cfg.validate().is_ok());

        let invalid_cfg = UnifiedReadPoolConfig {
            min_thread_count: 0,
            ..cfg
        };
        assert!(invalid_cfg.validate().is_err());

        let invalid_cfg = UnifiedReadPoolConfig {
            min_thread_count: 2,
            max_thread_count: 1,
            ..cfg
        };
        assert!(invalid_cfg.validate().is_err());

        let invalid_cfg = UnifiedReadPoolConfig {
            stack_size: ReadableSize::mb(1),
            ..cfg
        };
        assert!(invalid_cfg.validate().is_err());

        let invalid_cfg = UnifiedReadPoolConfig {
            max_tasks_per_worker: 1,
            ..cfg
        };
        assert!(invalid_cfg.validate().is_err());
        let invalid_cfg = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: cmp::max(
                UNIFIED_READPOOL_MIN_CONCURRENCY,
                SysQuota::cpu_cores_quota() as usize,
            ) + 1,
            ..cfg
        };
        assert!(invalid_cfg.validate().is_err());
    }
}

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub use_unified_pool: Option<bool>,
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            /// Builds configurations for low, normal and high priority pools.
            pub fn to_yatp_pool_configs(self) -> Vec<yatp_pool::Config> {
                vec![
                    yatp_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    use_unified_pool: None,
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn use_unified_pool(&self) -> bool {
                // The unified pool is used by default unless the corresponding module has
                // customized configurations.
                self.use_unified_pool
                    .unwrap_or_else(|| *self == Default::default())
            }

            pub fn adjust_use_unified_pool(&mut self) {
                if self.use_unified_pool.is_none() {
                    // The unified pool is used by default unless the corresponding module has customized configurations.
                    if *self == Default::default() {
                        info!("readpool.{}.use-unified-pool is not set, set to true by default", $display_name);
                        self.use_unified_pool = Some(true);
                    } else {
                        info!("readpool.{}.use-unified-pool is not set, set to false because there are other customized configurations", $display_name);
                        self.use_unified_pool = Some(false);
                    }
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.use_unified_pool() {
                    return Ok(());
                }
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.low_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.low-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.stack_size.0 < ReadableSize::mb(MIN_READPOOL_STACK_SIZE_MB).0 {
                    return Err(format!(
                        "readpool.{}.stack-size should be >= {}mb",
                        $display_name, MIN_READPOOL_STACK_SIZE_MB
                    )
                    .into());
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    )
                    .into());
                }

                Ok(())
            }
        }

        #[cfg(test)]
        mod $test_mod_name {
            use super::*;

            #[test]
            fn test_validate() {
                let cfg = $struct_name::default();
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.high_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.normal_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.low_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.stack_size = ReadableSize::mb(1);
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_high = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_normal = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_low = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_but_unified = cfg.clone();
                invalid_but_unified.use_unified_pool = Some(true);
                invalid_but_unified.low_concurrency = 0;
                assert!(invalid_but_unified.validate().is_ok());
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 * 1000;

const MIN_READPOOL_STACK_SIZE_MB: u64 = 2;
const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, storage_read_pool_test, "storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

const DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY: usize = 2;

readpool_config!(
    CoprReadPoolConfig,
    coprocessor_read_pool_test,
    "coprocessor"
);

impl Default for CoprReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    #[online_config(submodule)]
    pub unified: UnifiedReadPoolConfig,
    #[online_config(skip)]
    pub storage: StorageReadPoolConfig,
    #[online_config(skip)]
    pub coprocessor: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn is_unified_pool_enabled(&self) -> bool {
        self.storage.use_unified_pool() || self.coprocessor.use_unified_pool()
    }

    pub fn adjust_use_unified_pool(&mut self) {
        self.storage.adjust_use_unified_pool();
        self.coprocessor.adjust_use_unified_pool();
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.is_unified_pool_enabled() {
            self.unified.validate()?;
        }
        self.storage.validate()?;
        self.coprocessor.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod readpool_tests {
    use super::*;

    #[test]
    fn test_unified_disabled() {
        // Allow invalid yatp config when yatp is not used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            stack_size: ReadableSize::mb(0),
            max_tasks_per_worker: 0,
        };
        assert!(unified.validate().is_err());
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(storage.validate().is_ok());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(coprocessor.validate().is_ok());
        let cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        assert!(!cfg.is_unified_pool_enabled());
        assert!(cfg.validate().is_ok());

        // Storage and coprocessor config must be valid when yatp is not used.
        let unified = UnifiedReadPoolConfig::default();
        assert!(unified.validate().is_ok());
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 0,
            ..Default::default()
        };
        assert!(storage.validate().is_err());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        let invalid_cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        assert!(!invalid_cfg.is_unified_pool_enabled());
        assert!(invalid_cfg.validate().is_err());
    }

    #[test]
    fn test_unified_enabled() {
        // Yatp config must be valid when yatp is used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            ..Default::default()
        };
        assert!(unified.validate().is_err());
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(storage.validate().is_ok());
        let coprocessor = CoprReadPoolConfig::default();
        assert!(coprocessor.validate().is_ok());
        let mut cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        cfg.adjust_use_unified_pool();
        assert!(cfg.is_unified_pool_enabled());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_is_unified() {
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(!storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig::default();
        assert!(coprocessor.use_unified_pool());

        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());

        cfg.storage.use_unified_pool = Some(false);
        cfg.coprocessor.use_unified_pool = Some(false);
        assert!(!cfg.is_unified_pool_enabled());
    }

    #[test]
    fn test_partially_unified() {
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(coprocessor.use_unified_pool());
        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());
        assert!(cfg.validate().is_err());
        cfg.storage.low_concurrency = 1;
        assert!(cfg.validate().is_ok());

        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!coprocessor.use_unified_pool());
        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());
        assert!(cfg.validate().is_err());
        cfg.coprocessor.low_concurrency = 1;
        assert!(cfg.validate().is_ok());
    }
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct HadoopConfig {
    pub home: String,
    pub linux_user: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupConfig {
    pub num_threads: usize,
    pub batch_size: usize,
    pub sst_max_size: ReadableSize,
    pub enable_auto_tune: bool,
    pub auto_tune_remain_threads: usize,
    pub auto_tune_refresh_interval: ReadableDuration,
    pub io_thread_size: usize,
    // Do not expose this config to user.
    // It used to debug s3 503 error.
    pub s3_multi_part_size: ReadableSize,
    #[online_config(submodule)]
    pub hadoop: HadoopConfig,
}

impl BackupConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        let limit = SysQuota::cpu_cores_quota() as usize;
        if self.num_threads == 0 || self.num_threads > limit {
            return Err(format!("backup.num_threads cannot be 0 or larger than {}", limit).into());
        }
        if self.batch_size == 0 {
            return Err("backup.batch_size cannot be 0".into());
        }
        if self.s3_multi_part_size.0 > ReadableSize::gb(5).0 {
            return Err("backup.s3_multi_part_size cannot larger than 5GB".into());
        }

        Ok(())
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        let default_coprocessor = CopConfig::default();
        let cpu_num = SysQuota::cpu_cores_quota();
        Self {
            // use at most 50% of vCPU by default
            num_threads: (cpu_num * 0.5).clamp(1.0, 8.0) as usize,
            batch_size: 8,
            sst_max_size: default_coprocessor.region_max_size(),
            enable_auto_tune: true,
            auto_tune_remain_threads: (cpu_num * 0.2).round() as usize,
            auto_tune_refresh_interval: ReadableDuration::secs(60),
            io_thread_size: 2,
            // 5MB is the minimum part size that S3 allowed.
            s3_multi_part_size: ReadableSize::mb(5),
            hadoop: Default::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupStreamConfig {
    #[online_config(skip)]
    pub max_flush_interval: ReadableDuration,
    #[online_config(skip)]
    pub num_threads: usize,
    #[online_config(skip)]
    pub io_threads: usize,
    #[online_config(skip)]
    pub enable: bool,
    #[online_config(skip)]
    pub temp_path: String,
    #[online_config(skip)]
    pub temp_file_size_limit_per_task: ReadableSize,
    #[online_config(skip)]
    pub initial_scan_pending_memory_quota: ReadableSize,
}

impl BackupStreamConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("backup.num_threads cannot be 0".into());
        }
        Ok(())
    }
}

impl Default for BackupStreamConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let total_mem = SysQuota::memory_limit_in_bytes();
        let quota_size = (total_mem as f64 * 0.1).min(ReadableSize::mb(512).0 as _);
        Self {
            max_flush_interval: ReadableDuration::minutes(5),
            // use at most 50% of vCPU by default
            num_threads: (cpu_num * 0.5).clamp(1.0, 8.0) as usize,
            io_threads: 2,
            enable: false,
            // TODO: may be use raft store directory
            temp_path: String::new(),
            temp_file_size_limit_per_task: ReadableSize::mb(128),
            initial_scan_pending_memory_quota: ReadableSize(quota_size as _),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CdcConfig {
    pub min_ts_interval: ReadableDuration,
    pub hibernate_regions_compatible: bool,
    // TODO(hi-rustin): Consider resizing the thread pool based on `incremental_scan_threads`.
    #[online_config(skip)]
    pub incremental_scan_threads: usize,
    pub incremental_scan_concurrency: usize,
    pub incremental_scan_speed_limit: ReadableSize,
    /// `TsFilter` can increase speed and decrease resource usage when incremental content is much
    /// less than total content. However in other cases, `TsFilter` can make performance worse
    /// because it needs to re-fetch old row values if they are required.
    ///
    /// `TsFilter` will be enabled if `incremental/total <= incremental_scan_ts_filter_ratio`.
    /// Set `incremental_scan_ts_filter_ratio` to 0 will disable it.
    pub incremental_scan_ts_filter_ratio: f64,
    pub sink_memory_quota: ReadableSize,
    pub old_value_cache_memory_quota: ReadableSize,
    // Deprecated! preserved for compatibility check.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub old_value_cache_size: usize,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            min_ts_interval: ReadableDuration::secs(1),
            hibernate_regions_compatible: true,
            // 4 threads for incremental scan.
            incremental_scan_threads: 4,
            // At most 6 concurrent running tasks.
            incremental_scan_concurrency: 6,
            // TiCDC requires a SSD, the typical write speed of SSD
            // is more than 500MB/s, so 128MB/s is enough.
            incremental_scan_speed_limit: ReadableSize::mb(128),
            incremental_scan_ts_filter_ratio: 0.2,
            // 512MB memory for CDC sink.
            sink_memory_quota: ReadableSize::mb(512),
            // 512MB memory for old value cache.
            old_value_cache_memory_quota: ReadableSize::mb(512),
            // Deprecated! preserved for compatibility check.
            old_value_cache_size: 0,
        }
    }
}

impl CdcConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.min_ts_interval.is_zero() {
            return Err("cdc.min-ts-interval can't be 0".into());
        }
        if self.incremental_scan_threads == 0 {
            return Err("cdc.incremental-scan-threads can't be 0".into());
        }
        if self.incremental_scan_concurrency < self.incremental_scan_threads {
            return Err(
                "cdc.incremental-scan-concurrency must be larger than cdc.incremental-scan-threads"
                    .into(),
            );
        }
        if self.incremental_scan_ts_filter_ratio < 0.0
            || self.incremental_scan_ts_filter_ratio > 1.0
        {
            return Err(
                "cdc.incremental-scan-ts-filter-ratio should be larger than 0 and less than 1"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ResolvedTsConfig {
    #[online_config(skip)]
    pub enable: bool,
    pub advance_ts_interval: ReadableDuration,
    #[online_config(skip)]
    pub scan_lock_pool_size: usize,
}

impl ResolvedTsConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.advance_ts_interval.is_zero() {
            return Err("resolved-ts.advance-ts-interval can't be zero".into());
        }
        if self.scan_lock_pool_size == 0 {
            return Err("resolved-ts.scan-lock-pool-size can't be zero".into());
        }
        Ok(())
    }
}

impl Default for ResolvedTsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            advance_ts_interval: ReadableDuration::secs(1),
            scan_lock_pool_size: 2,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct File {
    pub filename: String,
    // The unit is MB
    pub max_size: u64,
    // The unit is Day
    pub max_days: u64,
    pub max_backups: usize,
}

impl Default for File {
    fn default() -> Self {
        Self {
            filename: "".to_owned(),
            max_size: 300,
            max_days: 0,
            max_backups: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LogConfig {
    #[serde(with = "log_level_serde")]
    pub level: slog::Level,
    pub format: LogFormat,
    pub enable_timestamp: bool,
    pub file: File,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: slog::Level::Info,
            format: LogFormat::Text,
            enable_timestamp: true,
            file: File::default(),
        }
    }
}

impl LogConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.file.max_size > 4096 {
            return Err("Max log file size upper limit to 4096MB".to_string().into());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct QuotaConfig {
    pub foreground_cpu_time: usize,
    pub foreground_write_bandwidth: ReadableSize,
    pub foreground_read_bandwidth: ReadableSize,
    pub max_delay_duration: ReadableDuration,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            foreground_cpu_time: 0,
            foreground_write_bandwidth: ReadableSize(0),
            foreground_read_bandwidth: ReadableSize(0),
            max_delay_duration: ReadableDuration::millis(500),
        }
    }
}

impl QuotaConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        const MAX_DELAY_DURATION: ReadableDuration = ReadableDuration::micros(u64::MAX / 1000);

        if self.max_delay_duration > MAX_DELAY_DURATION {
            return Err(format!("quota.max-delay-duration must <= {}", MAX_DELAY_DURATION).into());
        }

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    pub cfg_path: String,

    // Deprecated! These configuration has been moved to LogConfig.
    // They are preserved for compatibility check.
    #[doc(hidden)]
    #[online_config(skip)]
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_file: String,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_format: LogFormat,
    #[online_config(skip)]
    pub log_rotation_timespan: ReadableDuration,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_rotation_size: ReadableSize,

    #[online_config(skip)]
    pub slow_log_file: String,

    #[online_config(skip)]
    pub slow_log_threshold: ReadableDuration,

    #[online_config(hidden)]
    pub panic_when_unexpected_key_or_data: bool,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub enable_io_snoop: bool,

    #[online_config(skip)]
    pub abort_on_panic: bool,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_limit: Option<ReadableSize>,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_high_water: f64,

    #[online_config(skip)]
    pub log: LogConfig,

    #[online_config(submodule)]
    pub quota: QuotaConfig,

    #[online_config(submodule)]
    pub readpool: ReadPoolConfig,

    #[online_config(submodule)]
    pub server: ServerConfig,

    #[online_config(submodule)]
    pub storage: StorageConfig,

    #[online_config(skip)]
    pub pd: PdConfig,

    #[online_config(hidden)]
    pub metric: MetricConfig,

    #[online_config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,

    #[online_config(submodule)]
    pub coprocessor: CopConfig,

    #[online_config(skip)]
    pub coprocessor_v2: CoprocessorV2Config,

    #[online_config(submodule)]
    pub rocksdb: DbConfig,

    #[online_config(submodule)]
    pub raftdb: RaftDbConfig,

    #[online_config(skip)]
    pub raft_engine: RaftEngineConfig,

    #[online_config(skip)]
    pub security: SecurityConfig,

    #[online_config(skip)]
    pub import: ImportConfig,

    #[online_config(submodule)]
    pub backup: BackupConfig,

    #[online_config(submodule)]
    // The term "log backup" and "backup stream" are identity.
    // The "log backup" should be the only product name exposed to the user.
    #[serde(rename = "log-backup")]
    pub backup_stream: BackupStreamConfig,

    #[online_config(submodule)]
    pub pessimistic_txn: PessimisticTxnConfig,

    #[online_config(submodule)]
    pub gc: GcConfig,

    #[online_config(submodule)]
    pub split: SplitConfig,

    #[online_config(submodule)]
    pub cdc: CdcConfig,

    #[online_config(submodule)]
    pub resolved_ts: ResolvedTsConfig,

    #[online_config(submodule)]
    pub resource_metering: ResourceMeteringConfig,

    #[online_config(skip)]
    pub causal_ts: CausalTsConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            cfg_path: "".to_owned(),
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            log_format: LogFormat::Text,
            log_rotation_timespan: ReadableDuration::hours(0),
            log_rotation_size: ReadableSize::mb(300),
            slow_log_file: "".to_owned(),
            slow_log_threshold: ReadableDuration::secs(1),
            panic_when_unexpected_key_or_data: false,
            enable_io_snoop: true,
            abort_on_panic: false,
            memory_usage_limit: None,
            memory_usage_high_water: 0.9,
            log: LogConfig::default(),
            quota: QuotaConfig::default(),
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            raft_store: RaftstoreConfig::default(),
            coprocessor: CopConfig::default(),
            coprocessor_v2: CoprocessorV2Config::default(),
            pd: PdConfig::default(),
            rocksdb: DbConfig::default(),
            raftdb: RaftDbConfig::default(),
            raft_engine: RaftEngineConfig::default(),
            storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            import: ImportConfig::default(),
            backup: BackupConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
            gc: GcConfig::default(),
            split: SplitConfig::default(),
            cdc: CdcConfig::default(),
            resolved_ts: ResolvedTsConfig::default(),
            resource_metering: ResourceMeteringConfig::default(),
            backup_stream: BackupStreamConfig::default(),
            causal_ts: CausalTsConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn infer_raft_db_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.raft_store.raftdb_path.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
            config::canonicalize_sub_path(data_dir, "raft")
        } else {
            config::canonicalize_path(&self.raft_store.raftdb_path)
        }
    }

    pub fn infer_raft_engine_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.raft_engine.config.dir.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
            config::canonicalize_sub_path(data_dir, "raft-engine")
        } else {
            config::canonicalize_path(&self.raft_engine.config.dir)
        }
    }

    pub fn infer_kv_engine_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
        config::canonicalize_sub_path(data_dir, DEFAULT_ROCKSDB_SUB_DIR)
    }

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.log.validate()?;
        self.readpool.validate()?;
        self.storage.validate()?;

        if self.cfg_path.is_empty() {
            self.cfg_path = Path::new(&self.storage.data_dir)
                .join(LAST_CONFIG_FILE)
                .to_str()
                .unwrap()
                .to_owned();
        }

        self.raft_store.raftdb_path = self.infer_raft_db_path(None)?;
        self.raft_engine.config.dir = self.infer_raft_engine_path(None)?;

        if self.raft_engine.config.dir == self.raft_store.raftdb_path {
            return Err("raft_engine.config.dir can't be same as raft_store.raftdb_path".into());
        }

        let kv_db_path = self.infer_kv_engine_path(None)?;
        if kv_db_path == self.raft_store.raftdb_path {
            return Err("raft_store.raftdb_path can't be same as storage.data_dir/db".into());
        }

        let kv_db_wal_path = if self.rocksdb.wal_dir.is_empty() {
            config::canonicalize_path(&kv_db_path)?
        } else {
            config::canonicalize_path(&self.rocksdb.wal_dir)?
        };
        let raft_db_wal_path = if self.raftdb.wal_dir.is_empty() {
            config::canonicalize_path(&self.raft_store.raftdb_path)?
        } else {
            config::canonicalize_path(&self.raftdb.wal_dir)?
        };
        if kv_db_wal_path == raft_db_wal_path {
            return Err("raftdb.wal_dir can't be same as rocksdb.wal_dir".into());
        }

        RaftDataStateMachine::new(
            &self.storage.data_dir,
            &self.raft_store.raftdb_path,
            &self.raft_engine.config.dir,
        )
        .validate(RocksEngine::exists(&kv_db_path))?;

        // Check blob file dir is empty when titan is disabled
        if !self.rocksdb.titan.enabled {
            let titandb_path = if self.rocksdb.titan.dirname.is_empty() {
                Path::new(&kv_db_path).join("titandb")
            } else {
                Path::new(&self.rocksdb.titan.dirname).to_path_buf()
            };
            if let Err(e) =
                tikv_util::config::check_data_dir_empty(titandb_path.to_str().unwrap(), "blob")
            {
                return Err(format!(
                    "check: titandb-data-dir-empty; err: \"{}\"; \
                     hint: You have disabled titan when its data directory is not empty. \
                     To properly shutdown titan, please enter fallback blob-run-mode and \
                     wait till titandb files are all safely ingested.",
                    e
                )
                .into());
            }
        }

        let expect_keepalive = self.raft_store.raft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time is too small, it should not less than the double of \
                 raft tick interval (>= {})",
                duration_to_sec(expect_keepalive)
            )
            .into());
        }

        if self.raft_store.hibernate_regions && !self.cdc.hibernate_regions_compatible {
            warn!(
                "raftstore.hibernate-regions was enabled but cdc.hibernate-regions-compatible \
                was disabled, hibernate regions may be broken up if you want to deploy a cdc cluster"
            );
        }

        if self.backup_stream.temp_path.is_empty() {
            self.backup_stream.temp_path =
                config::canonicalize_sub_path(&self.storage.data_dir, "log-backup-tmp")?;
        }

        self.rocksdb.validate()?;
        self.raftdb.validate()?;
        self.raft_engine.validate()?;
        self.server.validate()?;
        self.pd.validate()?;
        self.coprocessor.validate()?;
        self.raft_store
            .validate(self.coprocessor.region_split_size)?;
        self.security.validate()?;
        self.import.validate()?;
        self.backup.validate()?;
        self.backup_stream.validate()?;
        self.cdc.validate()?;
        self.pessimistic_txn.validate()?;
        self.gc.validate()?;
        self.resolved_ts.validate()?;
        self.resource_metering.validate()?;
        self.quota.validate()?;
        self.causal_ts.validate()?;

        if self.storage.flow_control.enable {
            // using raftdb write stall to control memtables as a safety net
            self.raftdb.defaultcf.level0_slowdown_writes_trigger = 10000;
            self.raftdb.defaultcf.level0_stop_writes_trigger = 10000;
            self.raftdb.defaultcf.soft_pending_compaction_bytes_limit = ReadableSize(0);
            self.raftdb.defaultcf.hard_pending_compaction_bytes_limit = ReadableSize(0);

            // disable kvdb write stall, and override related configs
            self.rocksdb.defaultcf.disable_write_stall = true;
            self.rocksdb.defaultcf.level0_slowdown_writes_trigger =
                self.storage.flow_control.l0_files_threshold as i32;
            self.rocksdb.defaultcf.soft_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .soft_pending_compaction_bytes_limit;
            self.rocksdb.defaultcf.hard_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .hard_pending_compaction_bytes_limit;
            self.rocksdb.writecf.disable_write_stall = true;
            self.rocksdb.writecf.level0_slowdown_writes_trigger =
                self.storage.flow_control.l0_files_threshold as i32;
            self.rocksdb.writecf.soft_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .soft_pending_compaction_bytes_limit;
            self.rocksdb.writecf.hard_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .hard_pending_compaction_bytes_limit;
            self.rocksdb.lockcf.disable_write_stall = true;
            self.rocksdb.lockcf.level0_slowdown_writes_trigger =
                self.storage.flow_control.l0_files_threshold as i32;
            self.rocksdb.lockcf.soft_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .soft_pending_compaction_bytes_limit;
            self.rocksdb.lockcf.hard_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .hard_pending_compaction_bytes_limit;
            self.rocksdb.raftcf.disable_write_stall = true;
            self.rocksdb.raftcf.level0_slowdown_writes_trigger =
                self.storage.flow_control.l0_files_threshold as i32;
            self.rocksdb.raftcf.soft_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .soft_pending_compaction_bytes_limit;
            self.rocksdb.raftcf.hard_pending_compaction_bytes_limit = self
                .storage
                .flow_control
                .hard_pending_compaction_bytes_limit;
        }

        if let Some(memory_usage_limit) = self.memory_usage_limit {
            let total = SysQuota::memory_limit_in_bytes();
            if memory_usage_limit.0 > total {
                // Explicitly exceeds system memory capacity is not allowed.
                return Err(format!(
                    "memory_usage_limit is greater than system memory capacity {}",
                    total
                )
                .into());
            }
        } else {
            // Adjust `memory_usage_limit` if necessary.
            if self.storage.block_cache.shared {
                if let Some(cap) = self.storage.block_cache.capacity {
                    let limit = (cap.0 as f64 / BLOCK_CACHE_RATE * MEMORY_USAGE_LIMIT_RATE) as u64;
                    self.memory_usage_limit = Some(ReadableSize(limit));
                } else {
                    self.memory_usage_limit = Some(Self::suggested_memory_usage_limit());
                }
            } else {
                let cap = self.rocksdb.defaultcf.block_cache_size.0
                    + self.rocksdb.writecf.block_cache_size.0
                    + self.rocksdb.lockcf.block_cache_size.0
                    + self.raftdb.defaultcf.block_cache_size.0;
                let limit = (cap as f64 / BLOCK_CACHE_RATE * MEMORY_USAGE_LIMIT_RATE) as u64;
                self.memory_usage_limit = Some(ReadableSize(limit));
            }
        }

        let mut limit = self.memory_usage_limit.unwrap();
        let total = ReadableSize(SysQuota::memory_limit_in_bytes());
        if limit.0 > total.0 {
            warn!(
                "memory_usage_limit:{:?} > total:{:?}, fallback to total",
                limit, total,
            );
            self.memory_usage_limit = Some(total);
            limit = total;
        }

        let default = Self::suggested_memory_usage_limit();
        if limit.0 > default.0 {
            warn!(
                "memory_usage_limit:{:?} > recommanded:{:?}, maybe page cache isn't enough",
                limit, default,
            );
        }

        Ok(())
    }

    // As the init of `logger` is very early, this adjust needs to be separated and called
    // immediately after parsing the command line.
    pub fn logger_compatible_adjust(&mut self) {
        let default_tikv_cfg = TiKvConfig::default();
        let default_log_cfg = LogConfig::default();
        if self.log_level != default_tikv_cfg.log_level {
            eprintln!("deprecated configuration, log-level has been moved to log.level");
            if self.log.level == default_log_cfg.level {
                eprintln!("override log.level with log-level, {:?}", self.log_level);
                self.log.level = self.log_level;
            }
            self.log_level = default_tikv_cfg.log_level;
        }
        if self.log_file != default_tikv_cfg.log_file {
            eprintln!("deprecated configuration, log-file has been moved to log.file.filename");
            if self.log.file.filename == default_log_cfg.file.filename {
                eprintln!(
                    "override log.file.filename with log-file, {:?}",
                    self.log_file
                );
                self.log.file.filename = self.log_file.clone();
            }
            self.log_file = default_tikv_cfg.log_file;
        }
        if self.log_format != default_tikv_cfg.log_format {
            eprintln!("deprecated configuration, log-format has been moved to log.format");
            if self.log.format == default_log_cfg.format {
                eprintln!("override log.format with log-format, {:?}", self.log_format);
                self.log.format = self.log_format;
            }
            self.log_format = default_tikv_cfg.log_format;
        }
        if self.log_rotation_timespan.as_secs() > 0 {
            eprintln!(
                "deprecated configuration, log-rotation-timespan is no longer used and ignored."
            );
        }
        if self.log_rotation_size != default_tikv_cfg.log_rotation_size {
            eprintln!(
                "deprecated configuration, \
                 log-ratation-size has been moved to log.file.max-size"
            );
            if self.log.file.max_size == default_log_cfg.file.max_size {
                eprintln!(
                    "override log.file.max_size with log-rotation-size, {:?}",
                    self.log_rotation_size
                );
                self.log.file.max_size = self.log_rotation_size.as_mb();
            }
            self.log_rotation_size = default_tikv_cfg.log_rotation_size;
        }
    }

    pub fn compatible_adjust(&mut self) {
        let default_raft_store = RaftstoreConfig::default();
        let default_coprocessor = CopConfig::default();
        if self.raft_store.region_max_size != default_raft_store.region_max_size {
            warn!(
                "deprecated configuration, \
                 raftstore.region-max-size has been moved to coprocessor"
            );
            if self.coprocessor.region_max_size == default_coprocessor.region_max_size {
                warn!(
                    "override coprocessor.region-max-size with raftstore.region-max-size, {:?}",
                    self.raft_store.region_max_size
                );
                self.coprocessor.region_max_size = Some(self.raft_store.region_max_size);
            }
            self.raft_store.region_max_size = default_raft_store.region_max_size;
        }
        if self.raft_store.region_split_size != default_raft_store.region_split_size {
            warn!(
                "deprecated configuration, \
                 raftstore.region-split-size has been moved to coprocessor",
            );
            if self.coprocessor.region_split_size == default_coprocessor.region_split_size {
                warn!(
                    "override coprocessor.region-split-size with raftstore.region-split-size, {:?}",
                    self.raft_store.region_split_size
                );
                self.coprocessor.region_split_size = self.raft_store.region_split_size;
            }
            self.raft_store.region_split_size = default_raft_store.region_split_size;
        }
        if self.server.end_point_concurrency.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-concurrency", "readpool.coprocessor.xxx-concurrency",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.coprocessor.xxx-concurrency",
                "server.end-point-concurrency",
                self.server.end_point_concurrency
            );
            let concurrency = self.server.end_point_concurrency.take().unwrap();
            self.readpool.coprocessor.high_concurrency = concurrency;
            self.readpool.coprocessor.normal_concurrency = concurrency;
            self.readpool.coprocessor.low_concurrency = concurrency;
        }
        if self.server.end_point_stack_size.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-stack-size", "readpool.coprocessor.stack-size",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.coprocessor.stack-size",
                "server.end-point-stack-size",
                self.server.end_point_stack_size
            );
            self.readpool.coprocessor.stack_size = self.server.end_point_stack_size.take().unwrap();
        }
        if self.server.end_point_max_tasks.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "server.end-point-max-tasks", "readpool.coprocessor.max-tasks-per-worker-xxx",
            );
            // Note:
            // Our `end_point_max_tasks` is mostly mistakenly configured, so we don't override
            // new configuration using old values.
            self.server.end_point_max_tasks = None;
        }
        if self.raft_store.clean_stale_peer_delay.as_secs() > 0 {
            warn!(
                "deprecated configuration, {} is no longer used and ignored.",
                "raft_store.clean_stale_peer_delay",
            );
        }
        if self.rocksdb.auto_tuned.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "rocksdb.auto_tuned", "rocksdb.rate_limiter_auto_tuned",
            );
            self.rocksdb.auto_tuned = None;
        }
        // When shared block cache is enabled, if its capacity is set, it overrides individual
        // block cache sizes. Otherwise use the sum of block cache size of all column families
        // as the shared cache size.
        let cache_cfg = &mut self.storage.block_cache;
        if cache_cfg.shared && cache_cfg.capacity.is_none() {
            cache_cfg.capacity = Some(ReadableSize(
                self.rocksdb.defaultcf.block_cache_size.0
                    + self.rocksdb.writecf.block_cache_size.0
                    + self.rocksdb.lockcf.block_cache_size.0
                    + self.raftdb.defaultcf.block_cache_size.0,
            ));
        }
        if self.backup.sst_max_size.0 < default_coprocessor.region_max_size().0 / 10 {
            warn!(
                "override backup.sst-max-size with min sst-max-size, {:?}",
                default_coprocessor.region_max_size() / 10
            );
            self.backup.sst_max_size = default_coprocessor.region_max_size() / 10;
        } else if self.backup.sst_max_size.0 > default_coprocessor.region_max_size().0 * 2 {
            warn!(
                "override backup.sst-max-size with max sst-max-size, {:?}",
                default_coprocessor.region_max_size() * 2
            );
            self.backup.sst_max_size = default_coprocessor.region_max_size() * 2;
        }

        self.readpool.adjust_use_unified_pool();
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), String> {
        if last_cfg.storage.data_dir != self.storage.data_dir {
            // In tikv 3.0 the default value of storage.data-dir changed
            // from "" to "./"
            let using_default_after_upgrade =
                last_cfg.storage.data_dir.is_empty() && self.storage.data_dir == DEFAULT_DATA_DIR;

            if !using_default_after_upgrade {
                return Err(format!(
                    "storage data dir have been changed, former data dir is {}, \
                     current data dir is {}, please check if it is expected.",
                    last_cfg.storage.data_dir, self.storage.data_dir
                ));
            }
        }
        if last_cfg.rocksdb.wal_dir != self.rocksdb.wal_dir {
            return Err(format!(
                "db wal dir have been changed, former is '{}', \
                 current db wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_cfg.rocksdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

        // It's possible that `last_cfg` is not fully validated.
        let last_raftdb_dir = last_cfg
            .infer_raft_db_path(None)
            .map_err(|e| e.to_string())?;
        let last_raft_engine_dir = last_cfg
            .infer_raft_engine_path(None)
            .map_err(|e| e.to_string())?;

        // FIXME: We cannot reliably determine the actual value of
        // `last_cfg.raft_engine.enable`, because some old versions don't have
        // this field (so it is automatically interpreted as the current
        // default value). To be safe, we will check both engines regardless
        // of whether raft engine is enabled.
        if last_raftdb_dir != self.raft_store.raftdb_path {
            return Err(format!(
                "raft db dir have been changed, former is '{}', \
                current is '{}', please check if it is expected.",
                last_raftdb_dir, self.raft_store.raftdb_path
            ));
        }
        if last_cfg.raftdb.wal_dir != self.raftdb.wal_dir {
            return Err(format!(
                "raft db wal dir have been changed, former is '{}', \
                current is '{}', please check if it is expected.",
                last_cfg.raftdb.wal_dir, self.raftdb.wal_dir
            ));
        }
        if last_raft_engine_dir != self.raft_engine.config.dir {
            return Err(format!(
                "raft engine dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_raft_engine_dir, self.raft_engine.config.dir
            ));
        }

        if last_cfg.storage.enable_ttl && !self.storage.enable_ttl {
            return Err("can't disable ttl on a ttl instance".to_owned());
        } else if !last_cfg.storage.enable_ttl && self.storage.enable_ttl {
            return Err("can't enable ttl on a non-ttl instance".to_owned());
        }

        Ok(())
    }

    pub fn from_file(
        path: &Path,
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn Error>> {
        let s = fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let mut cfg = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <TiKvConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        cfg.cfg_path = path.display().to_string();
        Ok(cfg)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = ::toml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn write_into_metrics(&self) {
        self.raft_store.write_into_metrics();
        self.rocksdb.write_into_metrics();
    }

    pub fn with_tmp() -> Result<(TiKvConfig, tempfile::TempDir), IoError> {
        let tmp = tempfile::tempdir()?;
        let mut cfg = TiKvConfig::default();
        cfg.storage.data_dir = tmp.path().display().to_string();
        cfg.cfg_path = tmp.path().join(LAST_CONFIG_FILE).display().to_string();
        Ok((cfg, tmp))
    }

    fn suggested_memory_usage_limit() -> ReadableSize {
        let total = SysQuota::memory_limit_in_bytes();
        // Reserve some space for page cache. The
        ReadableSize((total as f64 * MEMORY_USAGE_LIMIT_RATE) as u64)
    }

    pub fn build_shared_rocks_env(
        &self,
        key_manager: Option<Arc<DataKeyManager>>,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> Result<Arc<Env>, String> {
        let env = get_env(key_manager, limiter)?;
        if !self.raft_engine.enable {
            // RocksDB makes sure there are at least `max_background_flushes`
            // high-priority workers in env. That is not enough when multiple
            // RocksDB instances share the same env. We manually configure the
            // worker count in this case.
            env.set_high_priority_background_threads(
                self.raftdb.max_background_flushes + self.rocksdb.max_background_flushes,
            );
        }
        Ok(env)
    }
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &TiKvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    if let Some(mut cfg) = get_last_config(&config.storage.data_dir) {
        cfg.compatible_adjust();
        if let Err(e) = cfg.validate() {
            warn!("last_tikv.toml is invalid but ignored: {:?}", e);
        }
        config.check_critical_cfg_with(&cfg)?;
    }
    Ok(())
}

fn get_last_config(data_dir: &str) -> Option<TiKvConfig> {
    let store_path = Path::new(data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    if last_cfg_path.exists() {
        return Some(
            TiKvConfig::from_file(&last_cfg_path, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    last_cfg_path.display(),
                    e
                );
            }),
        );
    }
    None
}

/// Persists config to `last_tikv.toml`
pub fn persist_config(config: &TiKvConfig) -> Result<(), String> {
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    let tmp_cfg_path = store_path.join(TMP_CONFIG_FILE);

    let same_as_last_cfg = fs::read_to_string(&last_cfg_path).map_or(false, |last_cfg| {
        toml::to_string(&config).unwrap() == last_cfg
    });
    if same_as_last_cfg {
        return Ok(());
    }

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current configurations to temporary file.
    if let Err(e) = config.write_to_file(&tmp_cfg_path) {
        return Err(format!(
            "persist config to '{}' failed: {}",
            tmp_cfg_path.to_str().unwrap(),
            e
        ));
    }

    // Rename temporary file to last config file.
    if let Err(e) = fs::rename(&tmp_cfg_path, &last_cfg_path) {
        return Err(format!(
            "rename config file from '{}' to '{}' failed: {}",
            tmp_cfg_path.to_str().unwrap(),
            last_cfg_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

pub fn write_config<P: AsRef<Path>>(path: P, content: &[u8]) -> CfgResult<()> {
    let tmp_cfg_path = match path.as_ref().parent() {
        Some(p) => p.join(TMP_CONFIG_FILE),
        None => {
            return Err(Box::new(IoError::new(
                ErrorKind::Other,
                format!(
                    "failed to get parent path of config file: {}",
                    path.as_ref().display()
                ),
            )));
        }
    };
    {
        let mut f = fs::File::create(&tmp_cfg_path)?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_cfg_path, &path)?;
    Ok(())
}

lazy_static! {
    pub static ref TIKVCONFIG_TYPED: ConfigChange = TiKvConfig::default().typed();
}

fn serde_to_online_config(name: String) -> String {
    let res = name.replace("raftstore", "raft_store").replace('-', "_");
    match res.as_ref() {
        "raft_store.store_pool_size" | "raft_store.store_max_batch_size" => {
            res.replace("store_", "store_batch_system.")
        }
        "raft_store.apply_pool_size" | "raft_store.apply_max_batch_size" => {
            res.replace("apply_", "apply_batch_system.")
        }
        _ => res,
    }
}

fn to_config_change(change: HashMap<String, String>) -> CfgResult<ConfigChange> {
    fn helper(
        mut fields: Vec<String>,
        dst: &mut ConfigChange,
        typed: &ConfigChange,
        value: String,
    ) -> CfgResult<()> {
        if let Some(field) = fields.pop() {
            return match typed.get(&field) {
                None => Err(format!("unexpect fields: {}", field).into()),
                Some(ConfigValue::Skip) => {
                    Err(format!("config {} can not be changed", field).into())
                }
                Some(ConfigValue::Module(m)) => {
                    if let ConfigValue::Module(n_dst) = dst
                        .entry(field)
                        .or_insert_with(|| ConfigValue::Module(HashMap::new()))
                    {
                        return helper(fields, n_dst, m, value);
                    }
                    panic!("unexpect config value");
                }
                Some(v) => {
                    if fields.is_empty() {
                        return match to_change_value(&value, v) {
                            Err(_) => Err(format!("failed to parse: {}", value).into()),
                            Ok(v) => {
                                dst.insert(field, v);
                                Ok(())
                            }
                        };
                    }
                    let c: Vec<_> = fields.into_iter().rev().collect();
                    Err(format!("unexpect fields: {}", c[..].join(".")).into())
                }
            };
        }
        Ok(())
    }
    let mut res = HashMap::new();
    for (mut name, value) in change {
        name = serde_to_online_config(name);
        let fields: Vec<_> = name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        helper(fields, &mut res, &TIKVCONFIG_TYPED, value)?;
    }
    Ok(res)
}

fn to_change_value(v: &str, typed: &ConfigValue) -> CfgResult<ConfigValue> {
    let v = v.trim_matches('\"');
    let res = match typed {
        ConfigValue::Duration(_) => ConfigValue::from(v.parse::<ReadableDuration>()?),
        ConfigValue::Size(_) => ConfigValue::from(v.parse::<ReadableSize>()?),
        ConfigValue::U64(_) => ConfigValue::from(v.parse::<u64>()?),
        ConfigValue::F64(_) => ConfigValue::from(v.parse::<f64>()?),
        ConfigValue::U32(_) => ConfigValue::from(v.parse::<u32>()?),
        ConfigValue::I32(_) => ConfigValue::from(v.parse::<i32>()?),
        ConfigValue::Usize(_) => ConfigValue::from(v.parse::<usize>()?),
        ConfigValue::Bool(_) => ConfigValue::from(v.parse::<bool>()?),
        ConfigValue::BlobRunMode(_) => ConfigValue::from(v.parse::<BlobRunMode>()?),
        ConfigValue::IOPriority(_) => ConfigValue::from(v.parse::<IOPriority>()?),
        ConfigValue::String(_) => ConfigValue::String(v.to_owned()),
        _ => unreachable!(),
    };
    Ok(res)
}

fn to_toml_encode(change: HashMap<String, String>) -> CfgResult<HashMap<String, String>> {
    fn helper(mut fields: Vec<String>, typed: &ConfigChange) -> CfgResult<bool> {
        if let Some(field) = fields.pop() {
            match typed.get(&field) {
                None | Some(ConfigValue::Skip) => Err(Box::new(IoError::new(
                    ErrorKind::Other,
                    format!("failed to get field: {}", field),
                ))),
                Some(ConfigValue::Module(m)) => helper(fields, m),
                Some(c) => {
                    if !fields.is_empty() {
                        return Err(Box::new(IoError::new(
                            ErrorKind::Other,
                            format!("unexpect fields: {:?}", fields),
                        )));
                    }
                    match c {
                        ConfigValue::Duration(_)
                        | ConfigValue::Size(_)
                        | ConfigValue::String(_)
                        | ConfigValue::BlobRunMode(_)
                        | ConfigValue::IOPriority(_) => Ok(true),
                        ConfigValue::None => Err(Box::new(IoError::new(
                            ErrorKind::Other,
                            format!("unexpect none field: {:?}", c),
                        ))),
                        _ => Ok(false),
                    }
                }
            }
        } else {
            Err(Box::new(IoError::new(
                ErrorKind::Other,
                "failed to get field",
            )))
        }
    }
    let mut dst = HashMap::new();
    for (name, value) in change {
        let online_config_name = serde_to_online_config(name.clone());
        let fields: Vec<_> = online_config_name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        if helper(fields, &TIKVCONFIG_TYPED)? {
            dst.insert(name.replace('_', "-"), format!("\"{}\"", value));
        } else {
            dst.insert(name.replace('_', "-"), value);
        }
    }
    Ok(dst)
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Module {
    Readpool,
    Server,
    Metric,
    Raftstore,
    Coprocessor,
    Pd,
    Rocksdb,
    Raftdb,
    RaftEngine,
    Storage,
    Security,
    Encryption,
    Import,
    Backup,
    PessimisticTxn,
    Gc,
    Split,
    CDC,
    ResolvedTs,
    ResourceMetering,
    BackupStream,
    Quota,
    Unknown(String),
}

impl From<&str> for Module {
    fn from(m: &str) -> Module {
        match m {
            "readpool" => Module::Readpool,
            "server" => Module::Server,
            "metric" => Module::Metric,
            "raft_store" => Module::Raftstore,
            "coprocessor" => Module::Coprocessor,
            "pd" => Module::Pd,
            "split" => Module::Split,
            "rocksdb" => Module::Rocksdb,
            "raftdb" => Module::Raftdb,
            "raft_engine" => Module::RaftEngine,
            "storage" => Module::Storage,
            "security" => Module::Security,
            "import" => Module::Import,
            "backup" => Module::Backup,
            "backup_stream" => Module::BackupStream,
            "pessimistic_txn" => Module::PessimisticTxn,
            "gc" => Module::Gc,
            "cdc" => Module::CDC,
            "resolved_ts" => Module::ResolvedTs,
            "resource_metering" => Module::ResourceMetering,
            "quota" => Module::Quota,
            n => Module::Unknown(n.to_owned()),
        }
    }
}

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default, Clone)]
pub struct ConfigController {
    inner: Arc<RwLock<ConfigInner>>,
}

#[derive(Default)]
struct ConfigInner {
    current: TiKvConfig,
    config_mgrs: HashMap<Module, Box<dyn ConfigManager>>,
}

impl ConfigController {
    pub fn new(current: TiKvConfig) -> Self {
        ConfigController {
            inner: Arc::new(RwLock::new(ConfigInner {
                current,
                config_mgrs: HashMap::new(),
            })),
        }
    }

    pub fn update(&self, change: HashMap<String, String>) -> CfgResult<()> {
        let diff = to_config_change(change.clone())?;
        self.update_impl(diff, Some(change))
    }

    pub fn update_from_toml_file(&self) -> CfgResult<()> {
        let current = self.get_current();
        match TiKvConfig::from_file(Path::new(&current.cfg_path), None) {
            Ok(incoming) => {
                let diff = current.diff(&incoming);
                self.update_impl(diff, None)
            }
            Err(e) => Err(e),
        }
    }

    fn update_impl(
        &self,
        mut diff: HashMap<String, ConfigValue>,
        change: Option<HashMap<String, String>>,
    ) -> CfgResult<()> {
        diff = {
            let incoming = self.get_current();
            let mut updated = incoming.clone();
            updated.update(diff);
            // Config might be adjusted in `validate`.
            updated.validate()?;
            incoming.diff(&updated)
        };
        let mut inner = self.inner.write().unwrap();
        let mut to_update = HashMap::with_capacity(diff.len());
        for (name, change) in diff.into_iter() {
            match change {
                ConfigValue::Module(change) => {
                    // update a submodule's config only if changes had been successfully
                    // dispatched to corresponding config manager, to avoid dispatch change twice
                    if let Some(mgr) = inner.config_mgrs.get_mut(&Module::from(name.as_str())) {
                        if let Err(e) = mgr.dispatch(change.clone()) {
                            inner.current.update(to_update);
                            return Err(e);
                        }
                    }
                    to_update.insert(name, ConfigValue::Module(change));
                }
                _ => {
                    let _ = to_update.insert(name, change);
                }
            }
        }
        debug!("all config change had been dispatched"; "change" => ?to_update);
        inner.current.update(to_update);
        // Write change to the config file
        if let Some(change) = change {
            let content = {
                let change = to_toml_encode(change)?;
                let src = if Path::new(&inner.current.cfg_path).exists() {
                    fs::read_to_string(&inner.current.cfg_path)?
                } else {
                    String::new()
                };
                let mut t = TomlWriter::new();
                t.write_change(src, change);
                t.finish()
            };
            write_config(&inner.current.cfg_path, &content)?;
        }
        Ok(())
    }

    pub fn update_config(&self, name: &str, value: &str) -> CfgResult<()> {
        let mut m = HashMap::new();
        m.insert(name.to_owned(), value.to_owned());
        self.update(m)
    }

    pub fn register(&self, module: Module, cfg_mgr: Box<dyn ConfigManager>) {
        let mut inner = self.inner.write().unwrap();
        if inner.config_mgrs.insert(module.clone(), cfg_mgr).is_some() {
            warn!("config manager for module {:?} already registered", module)
        }
    }

    pub fn get_current(&self) -> TiKvConfig {
        self.inner.read().unwrap().current.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use api_version::{ApiV1, KvFormat};
    use case_macros::*;
    use engine_traits::{DBOptions as DBOptionsTrait, ALL_CFS};
    use futures::executor::block_on;
    use grpcio::ResourceQuota;
    use itertools::Itertools;
    use kvproto::kvrpcpb::CommandPri;
    use raftstore::coprocessor::region_info_accessor::MockRegionInfoProvider;
    use slog::Level;
    use tempfile::Builder;
    use tikv_kv::RocksEngine as RocksDBEngine;
    use tikv_util::{
        config::VersionTrack,
        quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
        sys::SysQuota,
        worker::{dummy_scheduler, ReceiverWrapper},
    };

    use super::*;
    use crate::{
        server::{config::ServerConfigManager, ttl::TtlCheckerTask},
        storage::{
            config_manager::StorageConfigManger, lock_manager::DummyLockManager,
            txn::flow_controller::FlowController, Storage, TestStorageBuilder,
        },
    };

    #[test]
    fn test_case_macro() {
        let h = kebab_case!(HelloWorld);
        assert_eq!(h, "hello-world");

        let h = kebab_case!(WelcomeToMyHouse);
        assert_eq!(h, "welcome-to-my-house");

        let h = snake_case!(HelloWorld);
        assert_eq!(h, "hello_world");

        let h = snake_case!(WelcomeToMyHouse);
        assert_eq!(h, "welcome_to_my_house");
    }

    #[test]
    fn test_check_critical_cfg_with() {
        let mut tikv_cfg = TiKvConfig::default();
        let last_cfg = TiKvConfig::default();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        tikv_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        tikv_cfg.storage.data_dir = "/data1".to_owned();
        tikv_cfg.validate().unwrap();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.storage.data_dir = "/data1".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        // Enable Raft Engine.
        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        tikv_cfg.raft_engine.enable = true;
        last_cfg.raft_engine.enable = true;

        tikv_cfg.raft_engine.mut_config().dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raft_engine.mut_config().dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        // Disable Raft Engine and uses RocksDB.
        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        tikv_cfg.raft_engine.enable = false;
        last_cfg.raft_engine.enable = false;

        tikv_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        tikv_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        tikv_cfg.validate().unwrap();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
    }

    #[test]
    fn test_last_cfg_modified() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        let store_path = Path::new(&cfg.storage.data_dir);
        let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

        cfg.write_to_file(&last_cfg_path).unwrap();

        let mut last_cfg_metadata = last_cfg_path.metadata().unwrap();
        let first_modified = last_cfg_metadata.modified().unwrap();

        // not write to file when config is the equivalent of last one.
        assert!(persist_config(&cfg).is_ok());
        last_cfg_metadata = last_cfg_path.metadata().unwrap();
        assert_eq!(last_cfg_metadata.modified().unwrap(), first_modified);

        // write to file when config is the inequivalent of last one.
        cfg.log_level = slog::Level::Warning;
        assert!(persist_config(&cfg).is_ok());
        last_cfg_metadata = last_cfg_path.metadata().unwrap();
        assert_ne!(last_cfg_metadata.modified().unwrap(), first_modified);
    }

    #[test]
    fn test_persist_cfg() {
        let dir = Builder::new().prefix("test_persist_cfg").tempdir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut tikv_cfg = TiKvConfig::default();

        tikv_cfg.rocksdb.wal_dir = s1.clone();
        tikv_cfg.raftdb.wal_dir = s2.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s1);
        assert_eq!(cfg_from_file.raftdb.wal_dir, s2);

        // write critical config when exist.
        tikv_cfg.rocksdb.wal_dir = s2.clone();
        tikv_cfg.raftdb.wal_dir = s1.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s2);
        assert_eq!(cfg_from_file.raftdb.wal_dir, s1);
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .tempdir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        assert!(persist_config(&tikv_cfg).is_ok());
    }

    #[test]
    fn test_keepalive_check() {
        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        let dur = tikv_cfg.raft_store.raft_heartbeat_interval();
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur);
        assert!(tikv_cfg.validate().is_err());
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur * 2);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_block_size() {
        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::gb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::gb(10);
        assert!(tikv_cfg.validate().is_err());
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::kb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_parse_log_level() {
        #[derive(Serialize, Deserialize, Debug)]
        struct LevelHolder {
            #[serde(with = "log_level_serde")]
            v: Level,
        }

        let legal_cases = vec![
            ("fatal", Level::Critical),
            ("error", Level::Error),
            ("warn", Level::Warning),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
            ("info", Level::Info),
        ];
        for (serialized, deserialized) in legal_cases {
            let holder = LevelHolder { v: deserialized };
            let res_string = toml::to_string(&holder).unwrap();
            let exp_string = format!("v = \"{}\"\n", serialized);
            assert_eq!(res_string, exp_string);
            let res_value: LevelHolder = toml::from_str(&exp_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let compatibility_cases = vec![("warning", Level::Warning), ("critical", Level::Critical)];
        for (serialized, deserialized) in compatibility_cases {
            let variant_string = format!("v = \"{}\"\n", serialized);
            let res_value: LevelHolder = toml::from_str(&variant_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let illegal_cases = vec!["foobar", ""];
        for case in illegal_cases {
            let string = format!("v = \"{}\"\n", case);
            toml::from_str::<LevelHolder>(&string).unwrap_err();
        }
    }

    #[test]
    fn test_to_config_change() {
        assert_eq!(
            to_change_value("10h", &ConfigValue::Duration(0)).unwrap(),
            ConfigValue::from(ReadableDuration::hours(10))
        );
        assert_eq!(
            to_change_value("100MB", &ConfigValue::Size(0)).unwrap(),
            ConfigValue::from(ReadableSize::mb(100))
        );
        assert_eq!(
            to_change_value("10000", &ConfigValue::U64(0)).unwrap(),
            ConfigValue::from(10000u64)
        );

        let old = TiKvConfig::default();
        let mut incoming = TiKvConfig::default();
        incoming.coprocessor.region_split_keys = Some(10000);
        incoming.gc.max_write_bytes_per_sec = ReadableSize::mb(100);
        incoming.rocksdb.defaultcf.block_cache_size = ReadableSize::mb(500);
        incoming.storage.io_rate_limit.import_priority = file_system::IOPriority::High;
        let diff = old.diff(&incoming);
        let mut change = HashMap::new();
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "rocksdb.defaultcf.block-cache-size".to_owned(),
            "500MB".to_owned(),
        );
        change.insert(
            "storage.io-rate-limit.import-priority".to_owned(),
            "high".to_owned(),
        );
        let res = to_config_change(change).unwrap();
        assert_eq!(diff, res);

        // illegal cases
        let cases = vec![
            // wrong value type
            ("gc.max-write-bytes-per-sec".to_owned(), "10s".to_owned()),
            (
                "pessimistic-txn.wait-for-lock-timeout".to_owned(),
                "1MB".to_owned(),
            ),
            // missing or unknown config fields
            ("xxx.yyy".to_owned(), "12".to_owned()),
            (
                "rocksdb.defaultcf.block-cache-size.xxx".to_owned(),
                "50MB".to_owned(),
            ),
            ("rocksdb.xxx.block-cache-size".to_owned(), "50MB".to_owned()),
            ("rocksdb.block-cache-size".to_owned(), "50MB".to_owned()),
            // not support change config
            (
                "raftstore.raft-heartbeat-ticks".to_owned(),
                "100".to_owned(),
            ),
            ("raftstore.prevote".to_owned(), "false".to_owned()),
        ];
        for (name, value) in cases {
            let mut change = HashMap::new();
            change.insert(name, value);
            assert!(to_config_change(change).is_err());
        }
    }

    #[test]
    fn test_to_toml_encode() {
        let mut change = HashMap::new();
        change.insert(
            "raftstore.pd-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "rocksdb.defaultcf.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        change.insert("raftstore.apply_pool_size".to_owned(), "7".to_owned());
        change.insert("raftstore.store-pool-size".to_owned(), "17".to_owned());
        let res = to_toml_encode(change).unwrap();
        assert_eq!(
            res.get("raftstore.pd-heartbeat-tick-interval"),
            Some(&"\"1h\"".to_owned())
        );
        assert_eq!(
            res.get("coprocessor.region-split-keys"),
            Some(&"10000".to_owned())
        );
        assert_eq!(
            res.get("gc.max-write-bytes-per-sec"),
            Some(&"\"100MB\"".to_owned())
        );
        assert_eq!(
            res.get("rocksdb.defaultcf.titan.blob-run-mode"),
            Some(&"\"read-only\"".to_owned())
        );
        assert_eq!(res.get("raftstore.apply-pool-size"), Some(&"7".to_owned()));
        assert_eq!(res.get("raftstore.store-pool-size"), Some(&"17".to_owned()));
    }

    fn new_engines<F: KvFormat>(
        cfg: TiKvConfig,
    ) -> (
        Storage<RocksDBEngine, DummyLockManager, F>,
        ConfigController,
        ReceiverWrapper<TtlCheckerTask>,
        Arc<FlowController>,
    ) {
        assert_eq!(F::TAG, cfg.storage.api_version());
        let engine = RocksDBEngine::new(
            &cfg.storage.data_dir,
            ALL_CFS,
            Some(cfg.rocksdb.build_cf_opts(
                &cfg.storage.block_cache.build_shared_cache(),
                None,
                cfg.storage.api_version(),
            )),
            true,
            None,
            Some(cfg.rocksdb.build_opt()),
        )
        .unwrap();
        let storage =
            TestStorageBuilder::<_, _, F>::from_engine_and_lock_mgr(engine, DummyLockManager)
                .config(cfg.storage.clone())
                .build()
                .unwrap();
        let engine = storage.get_engine().get_rocksdb();
        let (_tx, rx) = std::sync::mpsc::channel();
        let flow_controller = Arc::new(FlowController::new(
            &cfg.storage.flow_control,
            engine.clone(),
            rx,
        ));

        let (shared, cfg_controller) = (cfg.storage.block_cache.shared, ConfigController::new(cfg));
        cfg_controller.register(
            Module::Rocksdb,
            Box::new(DBConfigManger::new(engine.clone(), DBType::Kv, shared)),
        );
        let (scheduler, receiver) = dummy_scheduler();
        cfg_controller.register(
            Module::Storage,
            Box::new(StorageConfigManger::new(
                engine,
                shared,
                scheduler,
                flow_controller.clone(),
                storage.get_scheduler(),
            )),
        );
        (storage, cfg_controller, receiver, flow_controller)
    }

    #[test]
    fn test_flow_control() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.storage.flow_control.l0_files_threshold = 50;
        cfg.validate().unwrap();
        let (storage, cfg_controller, _, flow_controller) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_level_zero_slowdown_writes_trigger(),
            50
        );
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_level_zero_stop_writes_trigger(),
            50
        );

        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            true
        );
        assert_eq!(flow_controller.enabled(), true);
        cfg_controller
            .update_config("storage.flow-control.enable", "false")
            .unwrap();
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            false
        );
        assert_eq!(flow_controller.enabled(), false);
        cfg_controller
            .update_config("storage.flow-control.enable", "true")
            .unwrap();
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            true
        );
        assert_eq!(flow_controller.enabled(), true);
    }

    #[test]
    fn test_change_resolved_ts_config() {
        use crossbeam::channel;

        pub struct TestConfigManager(channel::Sender<ConfigChange>);
        impl ConfigManager for TestConfigManager {
            fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
                self.0.send(change).unwrap();
                Ok(())
            }
        }

        let (cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        let cfg_controller = ConfigController::new(cfg);
        let (tx, rx) = channel::unbounded();
        cfg_controller.register(Module::ResolvedTs, Box::new(TestConfigManager(tx)));

        // Return error if try to update not support config or unknow config
        assert!(
            cfg_controller
                .update_config("resolved-ts.enable", "false")
                .is_err()
        );
        assert!(
            cfg_controller
                .update_config("resolved-ts.scan-lock-pool-size", "10")
                .is_err()
        );
        assert!(
            cfg_controller
                .update_config("resolved-ts.xxx", "false")
                .is_err()
        );

        let mut resolved_ts_cfg = cfg_controller.get_current().resolved_ts;
        // Default value
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::secs(1)
        );

        // Update `advance-ts-interval` to 100ms
        cfg_controller
            .update_config("resolved-ts.advance-ts-interval", "100ms")
            .unwrap();
        resolved_ts_cfg.update(rx.recv().unwrap());
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // Return error if try to update `advance-ts-interval` to an invalid value
        assert!(
            cfg_controller
                .update_config("resolved-ts.advance-ts-interval", "0m")
                .is_err()
        );
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // Update `advance-ts-interval` to 3s
        cfg_controller
            .update_config("resolved-ts.advance-ts-interval", "3s")
            .unwrap();
        resolved_ts_cfg.update(rx.recv().unwrap());
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::secs(3)
        );
    }

    #[test]
    fn test_change_rocksdb_config() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.rocksdb.max_background_jobs = 4;
        cfg.rocksdb.max_background_flushes = 2;
        cfg.rocksdb.defaultcf.disable_auto_compactions = false;
        cfg.rocksdb.defaultcf.target_file_size_base = ReadableSize::mb(64);
        cfg.rocksdb.defaultcf.block_cache_size = ReadableSize::mb(8);
        cfg.rocksdb.rate_bytes_per_sec = ReadableSize::mb(64);
        cfg.rocksdb.rate_limiter_auto_tuned = false;
        cfg.storage.block_cache.shared = false;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // update max_background_jobs
        assert_eq!(db.get_db_options().get_max_background_jobs(), 4);

        cfg_controller
            .update_config("rocksdb.max-background-jobs", "8")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_jobs(), 8);

        // update max_background_flushes, set to a bigger value
        assert_eq!(db.get_db_options().get_max_background_flushes(), 2);

        cfg_controller
            .update_config("rocksdb.max-background-flushes", "5")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_flushes(), 5);

        // update rate_bytes_per_sec
        assert_eq!(
            db.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(64).0 as i64
        );

        cfg_controller
            .update_config("rocksdb.rate-bytes-per-sec", "128MB")
            .unwrap();
        assert_eq!(
            db.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(128).0 as i64
        );

        // update some configs on default cf
        let cf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert_eq!(cf_opts.get_disable_auto_compactions(), false);
        assert_eq!(cf_opts.get_target_file_size_base(), ReadableSize::mb(64).0);
        assert_eq!(cf_opts.get_block_cache_capacity(), ReadableSize::mb(8).0);

        let mut change = HashMap::new();
        change.insert(
            "rocksdb.defaultcf.disable-auto-compactions".to_owned(),
            "true".to_owned(),
        );
        change.insert(
            "rocksdb.defaultcf.target-file-size-base".to_owned(),
            "32MB".to_owned(),
        );
        change.insert(
            "rocksdb.defaultcf.block-cache-size".to_owned(),
            "256MB".to_owned(),
        );
        cfg_controller.update(change).unwrap();

        let cf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert_eq!(cf_opts.get_disable_auto_compactions(), true);
        assert_eq!(cf_opts.get_target_file_size_base(), ReadableSize::mb(32).0);
        assert_eq!(cf_opts.get_block_cache_capacity(), ReadableSize::mb(256).0);

        // Can not update block cache through storage module
        // when shared block cache is disabled
        assert!(
            cfg_controller
                .update_config("storage.block-cache.capacity", "512MB")
                .is_err()
        );
    }

    #[test]
    fn test_change_rate_limiter_auto_tuned() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        // vanilla limiter does not support dynamically changing auto-tuned mode.
        cfg.rocksdb.rate_limiter_auto_tuned = true;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // update rate_limiter_auto_tuned
        assert_eq!(
            db.get_db_options().get_rate_limiter_auto_tuned().unwrap(),
            true
        );

        cfg_controller
            .update_config("rocksdb.rate_limiter_auto_tuned", "false")
            .unwrap();
        assert_eq!(
            db.get_db_options().get_rate_limiter_auto_tuned().unwrap(),
            false
        );
    }

    #[test]
    fn test_change_shared_block_cache() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.storage.block_cache.shared = true;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // Can not update shared block cache through rocksdb module
        assert!(
            cfg_controller
                .update_config("rocksdb.defaultcf.block-cache-size", "256MB")
                .is_err()
        );

        cfg_controller
            .update_config("storage.block-cache.capacity", "256MB")
            .unwrap();

        let defaultcf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert_eq!(
            defaultcf_opts.get_block_cache_capacity(),
            ReadableSize::mb(256).0
        );
    }

    #[test]
    fn test_dispatch_titan_blob_run_mode_config() {
        let mut cfg = TiKvConfig::default();
        let mut incoming = cfg.clone();
        cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Normal;
        incoming.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Fallback;

        let diff = cfg
            .rocksdb
            .defaultcf
            .titan
            .diff(&incoming.rocksdb.defaultcf.titan);
        assert_eq!(diff.len(), 1);

        let diff = config_value_to_string(diff.into_iter().collect());
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0.as_str(), "blob_run_mode");
        assert_eq!(diff[0].1.as_str(), "kFallback");
    }

    #[test]
    fn test_change_ttl_check_poll_interval() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.storage.block_cache.shared = true;
        cfg.validate().unwrap();
        let (_, cfg_controller, mut rx, _) = new_engines::<ApiV1>(cfg);

        // Can not update shared block cache through rocksdb module
        cfg_controller
            .update_config("storage.ttl_check_poll_interval", "10s")
            .unwrap();
        match rx.recv() {
            None => unreachable!(),
            Some(TtlCheckerTask::UpdatePollInterval(d)) => assert_eq!(d, Duration::from_secs(10)),
        }
    }

    #[test]
    fn test_change_store_scheduler_worker_pool_size() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.storage.scheduler_worker_pool_size = 4;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let scheduler = storage.get_scheduler();

        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);

        let check_scale_pool_size = |size: usize, ok: bool| {
            let origin_pool_size = scheduler
                .get_sched_pool(CommandPri::Normal)
                .pool
                .get_pool_size();
            let origin_pool_size_high = scheduler
                .get_sched_pool(CommandPri::High)
                .pool
                .get_pool_size();
            let res = cfg_controller
                .update_config("storage.scheduler-worker-pool-size", &format!("{}", size));
            let (expected_size, expected_size_high) = if ok {
                res.unwrap();
                (size, std::cmp::max(size / 2, 1))
            } else {
                assert!(res.is_err());
                (origin_pool_size, origin_pool_size_high)
            };
            assert_eq!(
                scheduler
                    .get_sched_pool(CommandPri::Normal)
                    .pool
                    .get_pool_size(),
                expected_size
            );
            assert_eq!(
                scheduler
                    .get_sched_pool(CommandPri::High)
                    .pool
                    .get_pool_size(),
                expected_size_high
            );
        };

        check_scale_pool_size(0, false);
        check_scale_pool_size(max_pool_size + 1, false);
        check_scale_pool_size(1, true);
        check_scale_pool_size(max_pool_size, true);
    }

    #[test]
    fn test_change_quota_config() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.quota.foreground_cpu_time = 1000;
        cfg.quota.foreground_write_bandwidth = ReadableSize::mb(128);
        cfg.quota.foreground_read_bandwidth = ReadableSize::mb(256);
        cfg.quota.max_delay_duration = ReadableDuration::secs(1);
        cfg.validate().unwrap();

        let quota_limiter = Arc::new(QuotaLimiter::new(
            cfg.quota.foreground_cpu_time,
            cfg.quota.foreground_write_bandwidth,
            cfg.quota.foreground_read_bandwidth,
            cfg.quota.max_delay_duration,
        ));

        let cfg_controller = ConfigController::new(cfg.clone());
        cfg_controller.register(
            Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(&quota_limiter))),
        );
        assert_eq!(cfg_controller.get_current(), cfg);

        // u64::MAX ns casts to 213503d.
        assert!(
            cfg_controller
                .update_config("quota.max-delay-duration", "213504d")
                .is_err()
        );
        assert_eq!(cfg_controller.get_current(), cfg);

        cfg_controller
            .update_config("quota.foreground-cpu-time", "2000")
            .unwrap();
        cfg.quota.foreground_cpu_time = 2000;
        assert_eq!(cfg_controller.get_current(), cfg);

        cfg_controller
            .update_config("quota.foreground-write-bandwidth", "256MB")
            .unwrap();
        cfg.quota.foreground_write_bandwidth = ReadableSize::mb(256);
        assert_eq!(cfg_controller.get_current(), cfg);

        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(ReadableSize::mb(32).0 as usize);
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::from_millis(125));

        cfg_controller
            .update_config("quota.foreground-read-bandwidth", "512MB")
            .unwrap();
        cfg.quota.foreground_read_bandwidth = ReadableSize::mb(512);
        assert_eq!(cfg_controller.get_current(), cfg);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::from_millis(250));

        cfg_controller
            .update_config("quota.max-delay-duration", "50ms")
            .unwrap();
        cfg.quota.max_delay_duration = ReadableDuration::millis(50);
        assert_eq!(cfg_controller.get_current(), cfg);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::from_millis(50));
    }

    #[test]
    fn test_change_server_config() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.validate().unwrap();
        let cfg_controller = ConfigController::new(cfg.clone());
        let (scheduler, _receiver) = dummy_scheduler();
        let version_tracker = Arc::new(VersionTrack::new(cfg.server.clone()));
        cfg_controller.register(
            Module::Server,
            Box::new(ServerConfigManager::new(
                scheduler,
                version_tracker.clone(),
                ResourceQuota::new(None),
            )),
        );

        let check_cfg = |cfg: &TiKvConfig| {
            assert_eq!(&cfg_controller.get_current(), cfg);
            assert_eq!(&*version_tracker.value(), &cfg.server);
        };

        cfg_controller
            .update_config("server.max-grpc-send-msg-len", "10000")
            .unwrap();
        cfg.server.max_grpc_send_msg_len = 10000;
        check_cfg(&cfg);

        cfg_controller
            .update_config("server.raft-msg-max-batch-size", "32")
            .unwrap();
        cfg.server.raft_msg_max_batch_size = 32;
        assert_eq!(cfg_controller.get_current(), cfg);
        check_cfg(&cfg);
    }

    #[test]
    fn test_compatible_adjust_validate_equal() {
        // After calling many time of `compatible_adjust` and `validate` should has
        // the same effect as calling `compatible_adjust` and `validate` one time
        let mut c = TiKvConfig::default();
        let mut cfg = c.clone();
        c.compatible_adjust();
        c.validate().unwrap();

        for _ in 0..10 {
            cfg.compatible_adjust();
            cfg.validate().unwrap();
            assert_eq!(c, cfg);
        }
    }

    #[test]
    fn test_readpool_compatible_adjust_config() {
        let content = r#"
        [readpool.storage]
        [readpool.coprocessor]
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.compatible_adjust();
        assert_eq!(cfg.readpool.storage.use_unified_pool, Some(true));
        assert_eq!(cfg.readpool.coprocessor.use_unified_pool, Some(true));

        let content = r#"
        [readpool.storage]
        stack-size = "1MB"
        [readpool.coprocessor]
        normal-concurrency = 1
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.compatible_adjust();
        assert_eq!(cfg.readpool.storage.use_unified_pool, Some(false));
        assert_eq!(cfg.readpool.coprocessor.use_unified_pool, Some(false));
    }

    #[test]
    fn test_unrecognized_config_keys() {
        let mut temp_config_file = tempfile::NamedTempFile::new().unwrap();
        let temp_config_writer = temp_config_file.as_file_mut();
        temp_config_writer
            .write_all(
                br#"
                    log-level = "debug"
                    log-fmt = "json"
                    [readpool.unified]
                    min-threads-count = 5
                    stack-size = "20MB"
                    [import]
                    num_threads = 4
                    [gcc]
                    batch-keys = 1024
                    [[security.encryption.master-keys]]
                    type = "file"
                "#,
            )
            .unwrap();
        temp_config_writer.sync_data().unwrap();

        let mut unrecognized_keys = Vec::new();
        let _ = TiKvConfig::from_file(temp_config_file.path(), Some(&mut unrecognized_keys));

        assert_eq!(
            unrecognized_keys,
            vec![
                "log-fmt".to_owned(),
                "readpool.unified.min-threads-count".to_owned(),
                "import.num_threads".to_owned(),
                "gcc".to_owned(),
                "security.encryption.master-keys".to_owned(),
            ],
        );
    }

    #[test]
    fn test_raft_engine_dir() {
        let content = r#"
            [raft-engine]
            enable = true
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.raft_engine.config.dir,
            config::canonicalize_sub_path(&cfg.storage.data_dir, "raft-engine").unwrap()
        );
    }

    #[test]
    fn test_compaction_guard() {
        // Test comopaction guard disabled.
        {
            let config = DefaultCfConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_compaction_guard: false,
                ..Default::default()
            };
            let provider = Some(MockRegionInfoProvider::new(vec![]));
            let cf_opts = build_cf_opt!(config, CF_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.target_file_size_base.0,
                cf_opts.get_target_file_size_base()
            );
        }
        // Test compaction guard enabled but region info provider is missing.
        {
            let config = DefaultCfConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_compaction_guard: true,
                ..Default::default()
            };
            let provider: Option<MockRegionInfoProvider> = None;
            let cf_opts = build_cf_opt!(config, CF_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.target_file_size_base.0,
                cf_opts.get_target_file_size_base()
            );
        }
        // Test compaction guard enabled.
        {
            let config = DefaultCfConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_compaction_guard: true,
                compaction_guard_min_output_file_size: ReadableSize::mb(4),
                compaction_guard_max_output_file_size: ReadableSize::mb(64),
                ..Default::default()
            };
            let provider = Some(MockRegionInfoProvider::new(vec![]));
            let cf_opts = build_cf_opt!(config, CF_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.compaction_guard_max_output_file_size.0,
                cf_opts.get_target_file_size_base()
            );
        }
    }

    #[test]
    fn test_validate_tikv_config() {
        let mut cfg = TiKvConfig::default();
        assert!(cfg.validate().is_ok());
        let default_region_split_check_diff = cfg.raft_store.region_split_check_diff().0;
        cfg.raft_store.region_split_check_diff =
            Some(ReadableSize(cfg.raft_store.region_split_check_diff().0 + 1));
        assert!(cfg.validate().is_ok());
        assert_eq!(
            cfg.raft_store.region_split_check_diff().0,
            default_region_split_check_diff + 1
        );

        // Test validating memory_usage_limit when it's greater than max.
        cfg.memory_usage_limit = Some(ReadableSize(SysQuota::memory_limit_in_bytes() * 2));
        assert!(cfg.validate().is_err());

        // Test memory_usage_limit is based on block cache size if it's not configured.
        cfg.memory_usage_limit = None;
        cfg.storage.block_cache.capacity = Some(ReadableSize(3 * GIB));
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.memory_usage_limit.unwrap(), ReadableSize(5 * GIB));

        // Test memory_usage_limit will fallback to system memory capacity with huge block cache.
        cfg.memory_usage_limit = None;
        let system = SysQuota::memory_limit_in_bytes();
        cfg.storage.block_cache.capacity = Some(ReadableSize(system * 3 / 4));
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.memory_usage_limit.unwrap(), ReadableSize(system));
    }

    #[test]
    fn test_validate_tikv_wal_config() {
        let tmp_path = tempfile::Builder::new().tempdir().unwrap().into_path();
        macro_rules! tmp_path_string_generate {
            ($base:expr, $($sub:expr),+) => {{
                let mut path: ::std::path::PathBuf = $base.clone();
                $(
                    path.push($sub);
                )*
                String::from(path.to_str().unwrap())
            }}
        }

        {
            let mut cfg = TiKvConfig::default();
            assert!(cfg.validate().is_ok());
        }

        {
            let mut cfg = TiKvConfig::default();
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data");
            cfg.raft_store.raftdb_path = tmp_path_string_generate!(tmp_path, "data", "db");
            assert!(cfg.validate().is_err());
        }

        {
            let mut cfg = TiKvConfig::default();
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            assert!(cfg.validate().is_err());
        }

        {
            let mut cfg = TiKvConfig::default();
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "db");
            assert!(cfg.validate().is_err());
        }

        {
            let mut cfg = TiKvConfig::default();
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            assert!(cfg.validate().is_err());
        }

        {
            let mut cfg = TiKvConfig::default();
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "db");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            assert!(cfg.validate().is_ok());
        }
    }

    #[test]
    fn test_background_job_limits() {
        // cpu num = 1
        assert_eq!(
            get_background_job_limits_impl(1 /*cpu_num*/, &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                1, /*cpu_num*/
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        // cpu num = 2
        assert_eq!(
            get_background_job_limits_impl(2 /*cpu_num*/, &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 2,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                2, /*cpu_num*/
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 2,
            }
        );
        // cpu num = 4
        assert_eq!(
            get_background_job_limits_impl(4 /*cpu_num*/, &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 4,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                4, /*cpu_num*/
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 4,
            }
        );
        // cpu num = 8
        assert_eq!(
            get_background_job_limits_impl(8 /*cpu_num*/, &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 7,
                max_background_flushes: 2,
                max_sub_compactions: 3,
                max_titan_background_gc: 4,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                8, /*cpu_num*/
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        // cpu num = 16
        assert_eq!(
            get_background_job_limits_impl(
                16, /*cpu_num*/
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            KVDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        assert_eq!(
            get_background_job_limits_impl(
                16, /*cpu_num*/
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
    }

    #[test]
    fn test_config_template_is_valid() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut cfg: TiKvConfig = toml::from_str(&template_config).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn test_config_template_no_superfluous_keys() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut deserializer = toml::Deserializer::new(&template_config);
        let mut unrecognized_keys = Vec::new();
        let _: TiKvConfig = serde_ignored::deserialize(&mut deserializer, |key| {
            unrecognized_keys.push(key.to_string())
        })
        .unwrap();

        // Don't use `is_empty()` so we see which keys are superfluous on failure.
        assert_eq!(unrecognized_keys, Vec::<String>::new());
    }

    #[test]
    fn test_config_template_matches_default() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut cfg: TiKvConfig = toml::from_str(&template_config).unwrap();
        let mut default_cfg = TiKvConfig::default();

        // Some default values are computed based on the environment.
        // Because we can't set config values for these in `config-template.toml`, we will handle
        // them manually.
        cfg.readpool.unified.max_thread_count = default_cfg.readpool.unified.max_thread_count;
        cfg.readpool.storage.high_concurrency = default_cfg.readpool.storage.high_concurrency;
        cfg.readpool.storage.normal_concurrency = default_cfg.readpool.storage.normal_concurrency;
        cfg.readpool.storage.low_concurrency = default_cfg.readpool.storage.low_concurrency;
        cfg.readpool.coprocessor.high_concurrency =
            default_cfg.readpool.coprocessor.high_concurrency;
        cfg.readpool.coprocessor.normal_concurrency =
            default_cfg.readpool.coprocessor.normal_concurrency;
        cfg.readpool.coprocessor.low_concurrency = default_cfg.readpool.coprocessor.low_concurrency;
        cfg.server.grpc_memory_pool_quota = default_cfg.server.grpc_memory_pool_quota;
        cfg.server.background_thread_count = default_cfg.server.background_thread_count;
        cfg.server.end_point_max_concurrency = default_cfg.server.end_point_max_concurrency;
        cfg.storage.scheduler_worker_pool_size = default_cfg.storage.scheduler_worker_pool_size;
        cfg.rocksdb.max_background_jobs = default_cfg.rocksdb.max_background_jobs;
        cfg.rocksdb.max_background_flushes = default_cfg.rocksdb.max_background_flushes;
        cfg.rocksdb.max_sub_compactions = default_cfg.rocksdb.max_sub_compactions;
        cfg.rocksdb.titan.max_background_gc = default_cfg.rocksdb.titan.max_background_gc;
        cfg.raftdb.max_background_jobs = default_cfg.raftdb.max_background_jobs;
        cfg.raftdb.max_background_flushes = default_cfg.raftdb.max_background_flushes;
        cfg.raftdb.max_sub_compactions = default_cfg.raftdb.max_sub_compactions;
        cfg.raftdb.titan.max_background_gc = default_cfg.raftdb.titan.max_background_gc;
        cfg.backup.num_threads = default_cfg.backup.num_threads;
        cfg.backup_stream.num_threads = default_cfg.backup_stream.num_threads;

        // There is another set of config values that we can't directly compare:
        // When the default values are `None`, but are then resolved to `Some(_)` later on.
        default_cfg.readpool.storage.adjust_use_unified_pool();
        default_cfg.readpool.coprocessor.adjust_use_unified_pool();
        default_cfg.security.redact_info_log = Some(false);
        default_cfg.coprocessor.region_max_size = Some(default_cfg.coprocessor.region_max_size());
        default_cfg.coprocessor.region_max_keys = Some(default_cfg.coprocessor.region_max_keys());
        default_cfg.coprocessor.region_split_keys =
            Some(default_cfg.coprocessor.region_split_keys());
        default_cfg.raft_store.raft_log_gc_size_limit =
            Some(default_cfg.coprocessor.region_split_size * 3 / 4);
        default_cfg.raft_store.raft_log_gc_count_limit =
            Some(default_cfg.coprocessor.region_split_size * 3 / 4 / ReadableSize::kb(1));
        default_cfg.raft_store.region_split_check_diff =
            Some(default_cfg.coprocessor.region_split_size / 16);

        // Other special cases.
        cfg.pd.retry_max_count = default_cfg.pd.retry_max_count; // Both -1 and isize::MAX are the same.
        cfg.storage.block_cache.capacity = None; // Either `None` and a value is computed or `Some(_)` fixed value.
        cfg.memory_usage_limit = None;
        cfg.raft_engine.mut_config().memory_limit = None;
        cfg.coprocessor_v2.coprocessor_plugin_directory = None; // Default is `None`, which is represented by not setting the key.

        assert_eq!(cfg, default_cfg);
    }

    #[test]
    fn test_compatibility_with_old_config_template() {
        let mut buf = Vec::new();
        let resp = reqwest::blocking::get(
            "https://raw.githubusercontent.com/tikv/tikv/master/etc/config-template.toml",
        );
        match resp {
            Ok(mut resp) => {
                std::io::copy(&mut resp, &mut buf).expect("failed to copy content");
                let template_config = std::str::from_utf8(&buf)
                    .unwrap()
                    .lines()
                    .map(|l| l.strip_prefix('#').unwrap_or(l))
                    .join("\n");
                let _: TiKvConfig = toml::from_str(&template_config).unwrap();
            }
            Err(e) => {
                if e.is_timeout() {
                    println!("warn: fail to download latest config template due to timeout");
                } else {
                    panic!("fail to download latest config template");
                }
            }
        }
    }

    #[test]
    fn test_cdc() {
        let content = r#"
            [cdc]
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // old-value-cache-size is deprecated, 0 must not report error.
        let content = r#"
            [cdc]
            old-value-cache-size = 0
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            min-ts-interval = "0s"
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap_err();

        let content = r#"
            [cdc]
            incremental-scan-threads = 0
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap_err();

        let content = r#"
            [cdc]
            incremental-scan-concurrency = 0
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap_err();

        let content = r#"
            [cdc]
            incremental-scan-concurrency = 1
            incremental-scan-threads = 2
        "#;
        let mut cfg: TiKvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap_err();
    }

    #[test]
    fn test_module_from_str() {
        let cases = vec![
            ("readpool", Module::Readpool),
            ("server", Module::Server),
            ("metric", Module::Metric),
            ("raft_store", Module::Raftstore),
            ("coprocessor", Module::Coprocessor),
            ("pd", Module::Pd),
            ("split", Module::Split),
            ("rocksdb", Module::Rocksdb),
            ("raft_engine", Module::RaftEngine),
            ("storage", Module::Storage),
            ("security", Module::Security),
            ("import", Module::Import),
            ("backup", Module::Backup),
            ("backup_stream", Module::BackupStream),
            ("pessimistic_txn", Module::PessimisticTxn),
            ("gc", Module::Gc),
            ("cdc", Module::CDC),
            ("resolved_ts", Module::ResolvedTs),
            ("resource_metering", Module::ResourceMetering),
            ("unknown", Module::Unknown("unknown".to_string())),
        ];
        for (name, module) in cases {
            assert_eq!(Module::from(name), module);
        }
    }

    #[test]
    fn test_numeric_enum_serializing() {
        let normal_string_config = r#"
            compaction-style = 1
        "#;
        let config: DefaultCfConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.compaction_style, DBCompactionStyle::Universal);

        // Test if we support string value
        let normal_string_config = r#"
            compaction-style = "universal"
        "#;
        let config: DefaultCfConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.compaction_style, DBCompactionStyle::Universal);
        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("compaction-style = 1")
        );

        let bad_string_config = r#"
            compaction-style = "level1"
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultCfConfig = toml::from_str(bad_string_config).unwrap();
        });
        assert!(r.is_err());

        let bad_string_config = r#"
            compaction-style = 4
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultCfConfig = toml::from_str(bad_string_config).unwrap();
        });
        assert!(r.is_err());

        // rate-limiter-mode default values is 2
        let config_str = r#"
            rate-limiter-mode = 1
        "#;

        let config: DbConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.rate_limiter_mode, DBRateLimiterMode::ReadOnly);

        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("rate-limiter-mode = 1")
        );
    }

    #[test]
    fn test_serde_to_online_config() {
        let cases = vec![
            (
                "raftstore.store_pool_size",
                "raft_store.store_batch_system.pool_size",
            ),
            (
                "raftstore.store-pool-size",
                "raft_store.store_batch_system.pool_size",
            ),
            (
                "raftstore.store_max_batch_size",
                "raft_store.store_batch_system.max_batch_size",
            ),
            (
                "raftstore.store-max-batch-size",
                "raft_store.store_batch_system.max_batch_size",
            ),
            (
                "raftstore.apply_pool_size",
                "raft_store.apply_batch_system.pool_size",
            ),
            (
                "raftstore.apply-pool-size",
                "raft_store.apply_batch_system.pool_size",
            ),
            (
                "raftstore.apply_max_batch_size",
                "raft_store.apply_batch_system.max_batch_size",
            ),
            (
                "raftstore.apply-max-batch-size",
                "raft_store.apply_batch_system.max_batch_size",
            ),
            (
                "raftstore.store_io_pool_size",
                "raft_store.store_io_pool_size",
            ),
            (
                "raftstore.store-io-pool-size",
                "raft_store.store_io_pool_size",
            ),
            (
                "raftstore.apply_yield_duration",
                "raft_store.apply_yield_duration",
            ),
            (
                "raftstore.apply-yield-duration",
                "raft_store.apply_yield_duration",
            ),
            (
                "raftstore.raft_store_max_leader_lease",
                "raft_store.raft_store_max_leader_lease",
            ),
        ];

        for (name, res) in cases {
            assert_eq!(serde_to_online_config(name.into()).as_str(), res);
        }
    }
}
