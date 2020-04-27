// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TiKvConfig` type, which is in turn
//! made up of many other configuration types.

use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Error as IoError;
use std::io::Write;
use std::path::Path;
use std::usize;

use configuration::{
    rollback_or, ConfigChange, ConfigManager, ConfigValue, Configuration, Result as CfgResult,
    RollbackCollector,
};
use engine::rocks::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, LRUCacheOptions,
    TitanDBOptions,
};
use slog;

use crate::import::Config as ImportConfig;
use crate::server::gc_worker::GcConfig;
use crate::server::lock_manager::Config as PessimisticTxnConfig;
use crate::server::Config as ServerConfig;
use crate::server::CONFIG_ROCKSDB_GAUGE;
use crate::storage::config::{Config as StorageConfig, DEFAULT_DATA_DIR, DEFAULT_ROCKSDB_SUB_DIR};
use encryption::EncryptionConfig;
use engine::rocks::util::{
    db_exist, CFOptions, FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform,
};
use engine_rocks::config::{self as rocks_config, BlobRunMode, CompressionType};
use engine_rocks::properties::MvccPropertiesCollectorFactory;
use engine_rocks::{
    RangePropertiesCollectorFactory, RocksEngine, RocksEventListener,
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use engine_traits::{CFHandleExt, ColumnFamilyOptions as ColumnFamilyOptionsTrait, DBOptionsExt};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use keys::region_raft_prefix_len;
use pd_client::Config as PdConfig;
use raftstore::coprocessor::Config as CopConfig;
use raftstore::store::Config as RaftstoreConfig;
use raftstore::store::SplitConfig;
use tikv_util::config::{self, ReadableDuration, ReadableSize, TomlWriter, GB, MB};
use tikv_util::future_pool;
use tikv_util::security::SecurityConfig;
use tikv_util::sys::sys_quota::SysQuota;
use tikv_util::time::duration_to_sec;

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;
const LAST_CONFIG_FILE: &str = "last_tikv.toml";
const TMP_CONFIG_FILE: &str = "tmp_tikv.toml";
const MAX_BLOCK_SIZE: usize = 32 * MB as usize;

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = SysQuota::new().memory_limit_in_bytes();
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
    size / MB as usize
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanCfConfig {
    #[config(skip)]
    pub min_blob_size: ReadableSize,
    #[config(skip)]
    pub blob_file_compression: CompressionType,
    #[config(skip)]
    pub blob_cache_size: ReadableSize,
    #[config(skip)]
    pub min_gc_batch_size: ReadableSize,
    #[config(skip)]
    pub max_gc_batch_size: ReadableSize,
    #[config(skip)]
    pub discardable_ratio: f64,
    #[config(skip)]
    pub sample_ratio: f64,
    #[config(skip)]
    pub merge_small_file_threshold: ReadableSize,
    pub blob_run_mode: BlobRunMode,
    #[config(skip)]
    pub level_merge: bool,
    #[config(skip)]
    pub range_merge: bool,
    #[config(skip)]
    pub max_sorted_runs: i32,
    #[config(skip)]
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

fn get_background_job_limit(
    default_background_jobs: i32,
    default_sub_compactions: u32,
    default_background_gc: i32,
) -> (i32, u32, i32) {
    let cpu_num = SysQuota::new().cpu_cores_quota();
    // At the minimum, we should have two background jobs: one for flush and one for compaction.
    // Otherwise, the number of background jobs should not exceed cpu_num - 1.
    // By default, rocksdb assign (max_background_jobs / 4) threads dedicated for flush, and
    // the rest shared by flush and compaction.
    let max_background_jobs: i32 =
        cmp::max(2, cmp::min(default_background_jobs, (cpu_num - 1) as i32));
    // Cap max_sub_compactions to allow at least two compactions.
    let max_compactions = max_background_jobs - max_background_jobs / 4;
    let max_sub_compactions: u32 = cmp::max(
        1,
        cmp::min(default_sub_compactions, (max_compactions - 1) as u32),
    );
    // Maximum background GC threads for Titan
    let max_background_gc: i32 = cmp::min(default_background_gc, cpu_num as i32);

    (max_background_jobs, max_sub_compactions, max_background_gc)
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            #[config(skip)]
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            #[config(skip)]
            pub disable_block_cache: bool,
            #[config(skip)]
            pub cache_index_and_filter_blocks: bool,
            #[config(skip)]
            pub pin_l0_filter_and_index_blocks: bool,
            #[config(skip)]
            pub use_bloom_filter: bool,
            #[config(skip)]
            pub optimize_filters_for_hits: bool,
            #[config(skip)]
            pub whole_key_filtering: bool,
            #[config(skip)]
            pub bloom_filter_bits_per_key: i32,
            #[config(skip)]
            pub block_based_bloom_filter: bool,
            #[config(skip)]
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "rocks_config::compression_type_level_serde")]
            #[config(skip)]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            #[config(skip)]
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "rocks_config::compaction_pri_serde")]
            #[config(skip)]
            pub compaction_pri: CompactionPriority,
            #[config(skip)]
            pub dynamic_level_bytes: bool,
            #[config(skip)]
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "rocks_config::compaction_style_serde")]
            #[config(skip)]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
            #[config(skip)]
            pub force_consistency_checks: bool,
            #[config(skip)]
            pub prop_size_index_distance: u64,
            #[config(skip)]
            pub prop_keys_index_distance: u64,
            #[config(skip)]
            pub enable_doubly_skiplist: bool,
            #[config(submodule)]
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
    ($opt:ident, $cache:ident) => {{
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
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit($opt.hard_pending_compaction_bytes_limit.0);
        cf_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        cf_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            cf_opts.set_doubly_skiplist();
        }
        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64),
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
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: TitanCfConfig::default(),
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        });
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_WRITE) as u64),
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
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        // Prefix extractor(trim the timestamp at tail) for write cf.
        let e = Box::new(FixedSuffixSliceTransform::new(8));
        cf_opts
            .set_prefix_extractor("FixedSuffixSliceTransform", e)
            .unwrap();
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let f = Box::new(RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        });
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_LOCK) as u64),
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
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
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
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[config(skip)]
    pub wal_dir: String,
    #[config(skip)]
    pub wal_ttl_seconds: u64,
    #[config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    #[config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[config(skip)]
    pub enable_statistics: bool,
    #[config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[config(skip)]
    pub info_log_max_size: ReadableSize,
    #[config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[config(skip)]
    pub info_log_dir: String,
    #[config(skip)]
    pub rate_bytes_per_sec: ReadableSize,
    #[serde(with = "rocks_config::rate_limiter_mode_serde")]
    #[config(skip)]
    pub rate_limiter_mode: DBRateLimiterMode,
    #[config(skip)]
    pub auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[config(skip)]
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[config(skip)]
    pub enable_pipelined_write: bool,
    #[config(skip)]
    pub enable_multi_batch_write: bool,
    #[config(skip)]
    pub enable_unordered_write: bool,
    #[config(submodule)]
    pub defaultcf: DefaultCfConfig,
    #[config(submodule)]
    pub writecf: WriteCfConfig,
    #[config(submodule)]
    pub lockcf: LockCfConfig,
    #[config(submodule)]
    pub raftcf: RaftCfConfig,
    #[config(skip)]
    pub titan: TitanDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        let (max_background_jobs, max_sub_compactions, max_background_gc) =
            get_background_job_limit(8, 3, 4);
        let mut titan_config = TitanDBConfig::default();
        titan_config.max_background_gc = max_background_gc;
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
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
            rate_bytes_per_sec: ReadableSize::kb(0),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_multi_batch_write: true,
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
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir, e
                    );
                })
        }

        if self.rate_bytes_per_sec.0 > 0 {
            opts.set_ratelimiter_with_auto_tuned(
                self.rate_bytes_per_sec.0 as i64,
                self.rate_limiter_mode,
                self.auto_tuned,
            );
        }

        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(
            (self.enable_pipelined_write || self.enable_multi_batch_write)
                && !self.enable_unordered_write,
        );
        opts.enable_multi_batch_write(self.enable_multi_batch_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.add_event_listener(RocksEventListener::new("kv"));

        if self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }
        opts
    }

    pub fn build_cf_opts(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache)),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt(cache)),
            CFOptions::new(CF_WRITE, self.writecf.build_opt(cache)),
            // TODO: remove CF_RAFT.
            CFOptions::new(CF_RAFT, self.raftcf.build_opt(cache)),
        ]
    }

    pub fn build_cf_opts_v2(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache)),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt(cache)),
            CFOptions::new(CF_WRITE, self.writecf.build_opt(cache)),
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
            if self.enable_pipelined_write || self.enable_multi_batch_write {
                return Err("pipelined_write is not compatible with unordered_write".into());
            }
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
        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(true, CF_DEFAULT) as u64),
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
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: TitanCfConfig::default(),
        }
    }
}

impl RaftDefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(FixedPrefixSliceTransform::new(region_raft_prefix_len()));
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
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[config(skip)]
    pub wal_dir: String,
    #[config(skip)]
    pub wal_ttl_seconds: u64,
    #[config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    #[config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[config(skip)]
    pub enable_statistics: bool,
    #[config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[config(skip)]
    pub info_log_max_size: ReadableSize,
    #[config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[config(skip)]
    pub info_log_dir: String,
    #[config(skip)]
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[config(skip)]
    pub enable_pipelined_write: bool,
    #[config(skip)]
    pub enable_unordered_write: bool,
    #[config(skip)]
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[config(submodule)]
    pub defaultcf: RaftDefaultCfConfig,
    #[config(skip)]
    pub titan: TitanDBConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        let (max_background_jobs, max_sub_compactions, max_background_gc) =
            get_background_job_limit(4, 2, 4);
        let mut titan_config = TitanDBConfig::default();
        titan_config.max_background_gc = max_background_gc;
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
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
            max_sub_compactions,
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
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir, e
                    );
                })
        }
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(RocksEventListener::new("raft"));
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

#[derive(Clone, Copy, Debug)]
pub enum DBType {
    Kv,
    Raft,
}

pub struct DBConfigManger {
    db: RocksEngine,
    db_type: DBType,
}

impl DBConfigManger {
    pub fn new(db: RocksEngine, db_type: DBType) -> Self {
        DBConfigManger { db, db_type }
    }
}

impl DBConfigManger {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.db.set_db_options(opts)?;
        Ok(())
    }

    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        let handle = self.db.cf_handle(cf)?;
        self.db.set_options_cf(handle, opts)?;
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
        let handle = self.db.cf_handle(cf)?;
        let opt = self.db.get_options_cf(handle);
        opt.set_block_cache_capacity(size.0)?;
        // Write config to metric
        CONFIG_ROCKSDB_GAUGE
            .with_label_values(&[cf, "block_cache_size"])
            .set(size.0 as f64);
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
                    self.set_block_cache_size(&cf_name, v.into())?;
                }
                if let Some(ConfigValue::Module(titan_change)) = cf_change.remove("titan") {
                    for (name, value) in titan_change {
                        cf_change.insert(name, value);
                    }
                }
                if !cf_change.is_empty() {
                    let cf_change = config_value_to_string(cf_change.into_iter().collect());
                    let cf_change_slice = config_to_slice(&cf_change);
                    self.set_cf_config(&cf_name, &cf_change_slice)?;
                }
            }
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
        .map(|(name, value)| {
            let v = match value {
                d @ ConfigValue::Duration(_) => {
                    let d: ReadableDuration = d.into();
                    d.as_secs().to_string()
                }
                s @ ConfigValue::Size(_) => {
                    let s: ReadableSize = s.into();
                    s.0.to_string()
                }
                ConfigValue::Module(_) => unreachable!(),
                v => format!("{}", v),
            };
            (name, v)
        })
        .collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub interval: ReadableDuration,
    pub address: String,
    pub job: String,
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

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct UnifiedReadPoolConfig {
    pub min_thread_count: usize,
    pub max_thread_count: usize,
    pub stack_size: ReadableSize,
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

const UNIFIED_READPOOL_MIN_CONCURRENCY: usize = 4;

// FIXME: Use macros to generate it if yatp is used elsewhere besides readpool.
impl Default for UnifiedReadPoolConfig {
    fn default() -> UnifiedReadPoolConfig {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num as f64 * 0.8) as usize;
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
            pub fn to_future_pool_configs(&self) -> Vec<future_pool::Config> {
                vec![
                    future_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
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
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num as f64 * 0.5) as usize;
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

impl StorageReadPoolConfig {
    pub fn use_unified_pool(&self) -> bool {
        // The storage module does not use the unified pool by default.
        self.use_unified_pool.unwrap_or(false)
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
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num as f64 * 0.8) as usize;
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

impl CoprReadPoolConfig {
    pub fn use_unified_pool(&self) -> bool {
        // The coprocessor module uses the unified pool unless it has customized configurations.
        self.use_unified_pool
            .unwrap_or_else(|| *self == Default::default())
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub unified: UnifiedReadPoolConfig,
    pub storage: StorageReadPoolConfig,
    pub coprocessor: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn is_unified_pool_enabled(&self) -> bool {
        self.storage.use_unified_pool() || self.coprocessor.use_unified_pool()
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
        let cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        assert!(cfg.is_unified_pool_enabled());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_is_unified() {
        let storage = StorageReadPoolConfig::default();
        assert!(!storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig::default();
        assert!(coprocessor.use_unified_pool());

        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());

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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[config(hidden)]
    pub cfg_path: String,

    #[config(skip)]
    pub enable_dynamic_config: bool,

    #[config(skip)]
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,

    #[config(skip)]
    pub log_file: String,

    #[config(skip)]
    pub slow_log_file: String,

    #[config(skip)]
    pub slow_log_threshold: ReadableDuration,

    #[config(skip)]
    pub log_rotation_timespan: ReadableDuration,

    #[config(skip)]
    pub log_rotation_size: ReadableSize,

    #[config(hidden)]
    pub panic_when_unexpected_key_or_data: bool,

    pub refresh_config_interval: ReadableDuration,

    #[config(skip)]
    pub readpool: ReadPoolConfig,

    #[config(skip)]
    pub server: ServerConfig,

    #[config(skip)]
    pub storage: StorageConfig,

    #[config(skip)]
    pub pd: PdConfig,

    #[config(hidden)]
    pub metric: MetricConfig,

    #[config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,

    #[config(submodule)]
    pub coprocessor: CopConfig,

    #[config(submodule)]
    pub rocksdb: DbConfig,

    #[config(submodule)]
    pub raftdb: RaftDbConfig,

    #[config(skip)]
    pub security: SecurityConfig,

    #[config(skip)]
    pub encryption: EncryptionConfig,

    #[config(skip)]
    pub import: ImportConfig,

    #[config(submodule)]
    pub pessimistic_txn: PessimisticTxnConfig,

    #[config(submodule)]
    pub gc: GcConfig,

    #[config(submodule)]
    pub split: SplitConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            cfg_path: "".to_owned(),
            enable_dynamic_config: true,
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            slow_log_file: "".to_owned(),
            slow_log_threshold: ReadableDuration::secs(1),
            log_rotation_timespan: ReadableDuration::hours(24),
            log_rotation_size: ReadableSize::mb(300),
            panic_when_unexpected_key_or_data: false,
            refresh_config_interval: ReadableDuration::secs(30),
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            raft_store: RaftstoreConfig::default(),
            coprocessor: CopConfig::default(),
            pd: PdConfig::default(),
            rocksdb: DbConfig::default(),
            raftdb: RaftDbConfig::default(),
            storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            encryption: EncryptionConfig::default(),
            import: ImportConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
            gc: GcConfig::default(),
            split: SplitConfig::default(),
        }
    }
}

impl TiKvConfig {
    // TODO: change to validate(&self)
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.validate_or_rollback(None)
    }

    /// Validate the config, if encounter invalid config try to fallback to
    /// the valid config. Caller should not rely on this method for validating
    /// because some configs not implement rollback collector yet.
    pub fn validate_with_rollback(&mut self, cfg: &TiKvConfig) -> ConfigChange {
        let mut c = HashMap::new();
        let rb_collector = RollbackCollector::new(cfg, &mut c);
        if let Err(e) = self.validate_or_rollback(Some(rb_collector)) {
            warn!("Invalid config"; "err" => ?e)
        }
        c
    }

    // If `rb_collector` is `Some`, when encounter some invalid config, instead of
    // return an Error, a fallback from these invalid configs to the valid config
    // will be collected and insert into `rb_collector`. For configs that not implement
    // rollback collector yet, an `Err` will return if encounter invalid config
    // TODO: implement rollback collector for more config
    fn validate_or_rollback(
        &mut self,
        mut rb_collector: Option<RollbackCollector<TiKvConfig>>,
    ) -> Result<(), Box<dyn Error>> {
        self.readpool.validate()?;
        self.storage.validate()?;

        self.raft_store.region_split_check_diff = self.coprocessor.region_split_size / 16;

        if self.cfg_path.is_empty() {
            self.cfg_path = Path::new(&self.storage.data_dir)
                .join(LAST_CONFIG_FILE)
                .to_str()
                .unwrap()
                .to_owned();
        }

        let default_raftdb_path = config::canonicalize_sub_path(&self.storage.data_dir, "raft")?;
        if self.raft_store.raftdb_path.is_empty() {
            self.raft_store.raftdb_path = default_raftdb_path;
        } else if self.raft_store.raftdb_path != default_raftdb_path {
            self.raft_store.raftdb_path = config::canonicalize_path(&self.raft_store.raftdb_path)?;
        }

        let kv_db_path =
            config::canonicalize_sub_path(&self.storage.data_dir, DEFAULT_ROCKSDB_SUB_DIR)?;

        if kv_db_path == self.raft_store.raftdb_path {
            return Err("raft_store.raftdb_path can not same with storage.data_dir/db".into());
        }
        if db_exist(&kv_db_path) && !db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb exist, buf raftdb not exist".into());
        }
        if !db_exist(&kv_db_path) && db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb not exist, buf raftdb exist".into());
        }

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

        rollback_or!(
            rb_collector,
            raft_store,
            |r| { self.raft_store.validate_or_rollback(r) },
            self.raft_store.validate()?
        );
        rollback_or!(
            rb_collector,
            coprocessor,
            |r| { self.coprocessor.validate_or_rollback(r) },
            self.coprocessor.validate()?
        );
        rollback_or!(
            rb_collector,
            pessimistic_txn,
            |r| { self.pessimistic_txn.validate_or_rollback(r) },
            self.pessimistic_txn.validate()?
        );
        rollback_or!(
            rb_collector,
            gc,
            |r| { self.gc.validate_or_rollback(r) },
            self.gc.validate()?
        );
        self.rocksdb.validate()?;
        self.raftdb.validate()?;
        self.server.validate()?;
        self.pd.validate()?;
        self.security.validate()?;
        self.import.validate()?;
        Ok(())
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
                self.coprocessor.region_max_size = self.raft_store.region_max_size;
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

        // When shared block cache is enabled, if its capacity is set, it overrides individual
        // block cache sizes. Otherwise use the sum of block cache size of all column families
        // as the shared cache size.
        let cache_cfg = &mut self.storage.block_cache;
        if cache_cfg.shared && cache_cfg.capacity.is_none() {
            cache_cfg.capacity = Some(ReadableSize {
                0: self.rocksdb.defaultcf.block_cache_size.0
                    + self.rocksdb.writecf.block_cache_size.0
                    + self.rocksdb.lockcf.block_cache_size.0
                    + self.raftdb.defaultcf.block_cache_size.0,
            });
        }
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), String> {
        if last_cfg.rocksdb.wal_dir != self.rocksdb.wal_dir {
            return Err(format!(
                "db wal_dir have been changed, former db wal_dir is '{}', \
                 current db wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_cfg.rocksdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

        if last_cfg.raftdb.wal_dir != self.raftdb.wal_dir {
            return Err(format!(
                "raftdb wal_dir have been changed, former raftdb wal_dir is '{}', \
                 current raftdb wal_dir is '{}', please guarantee all raft wal logs \
                 have been moved to destination directory.",
                last_cfg.raftdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

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

        if last_cfg.raft_store.raftdb_path != self.raft_store.raftdb_path {
            return Err(format!(
                "raft dir have been changed, former raft dir is '{}', \
                 current raft dir is '{}', please check if it is expected.",
                last_cfg.raft_store.raftdb_path, self.raft_store.raftdb_path
            ));
        }

        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        (|| -> Result<Self, Box<dyn Error>> {
            let s = fs::read_to_string(&path)?;
            let mut cfg: TiKvConfig = toml::from_str(&s)?;
            cfg.cfg_path = path.as_ref().display().to_string();
            Ok(cfg)
        })()
        .unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                path.as_ref().display(),
                e
            );
        })
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
        let _ = cfg.validate();
        config.check_critical_cfg_with(&cfg)?;
    }
    Ok(())
}

fn get_last_config(data_dir: &str) -> Option<TiKvConfig> {
    let store_path = Path::new(data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    if last_cfg_path.exists() {
        return Some(TiKvConfig::from_file(&last_cfg_path));
    }
    None
}

/// Persists config to `last_tikv.toml`
pub fn persist_config(config: &TiKvConfig) -> Result<(), String> {
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    let tmp_cfg_path = store_path.join(TMP_CONFIG_FILE);

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

pub fn write_config<P: AsRef<Path>>(tem_dir: P, path: P, content: &[u8]) -> CfgResult<()> {
    let tmp_cfg_path = tem_dir.as_ref().join(TMP_CONFIG_FILE);
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

fn to_config_change(change: HashMap<String, String>) -> CfgResult<ConfigChange> {
    fn helper(
        mut fields: Vec<String>,
        dst: &mut ConfigChange,
        typed: &ConfigChange,
        value: String,
    ) -> CfgResult<()> {
        if let Some(mut f) = fields.pop() {
            if f == "raftstore" {
                f = "raft_store".to_owned();
            } else {
                f = f.replace("-", "_");
            }
            return match typed.get(&f) {
                None => Err(format!("unexpect fields: {}", f).into()),
                Some(ConfigValue::Skip) => {
                    Err(format!("fields {:?} can not be change", fields).into())
                }
                Some(ConfigValue::Module(m)) => {
                    if let ConfigValue::Module(n_dst) = dst
                        .entry(f)
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
                                dst.insert(f, v);
                                Ok(())
                            }
                        };
                    }
                    Err(format!("unexpect fields: {:?}", fields).into())
                }
            };
        }
        Ok(())
    }
    let mut res = HashMap::new();
    for (name, value) in change {
        let fields: Vec<_> = name.as_str().split('.').collect();
        let fields: Vec<_> = fields.into_iter().map(|s| s.to_owned()).rev().collect();
        helper(fields, &mut res, &TIKVCONFIG_TYPED, value)?;
    }
    Ok(res)
}

fn to_change_value(v: &str, typed: &ConfigValue) -> CfgResult<ConfigValue> {
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
        ConfigValue::String(_) => ConfigValue::String(v.to_owned()),
        _ => unreachable!(),
    };
    Ok(res)
}

fn to_toml_encode(change: HashMap<String, String>) -> CfgResult<HashMap<String, String>> {
    fn helper(mut fields: Vec<String>, typed: &ConfigChange) -> CfgResult<bool> {
        if let Some(mut f) = fields.pop() {
            if f == "raftstore" {
                f = "raft_store".to_owned();
            } else {
                f = f.replace("-", "_");
            }
            match typed.get(&f) {
                None | Some(ConfigValue::Skip) => {
                    Err(format!("failed to get fields: {}", f).into())
                }
                Some(ConfigValue::Module(m)) => helper(fields, m),
                Some(c) => {
                    if !fields.is_empty() {
                        return Err(format!("unexpect fields: {:?}", fields).into());
                    }
                    match c {
                        ConfigValue::Duration(_)
                        | ConfigValue::Size(_)
                        | ConfigValue::String(_)
                        | ConfigValue::BlobRunMode(_) => Ok(true),
                        _ => Ok(false),
                    }
                }
            }
        } else {
            Err("failed to get field".to_owned().into())
        }
    };
    let mut dst = HashMap::new();
    for (name, value) in change {
        let fields: Vec<_> = name.as_str().split('.').collect();
        let fields: Vec<_> = fields.into_iter().map(|s| s.to_owned()).rev().collect();
        if helper(fields, &TIKVCONFIG_TYPED)? {
            dst.insert(name, format!("\"{}\"", value));
        } else {
            dst.insert(name, value);
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
    Storage,
    Security,
    Encryption,
    Import,
    PessimisticTxn,
    Gc,
    Split,
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
            "storage" => Module::Storage,
            "security" => Module::Security,
            "encryption" => Module::Encryption,
            "import" => Module::Import,
            "pessimistic_txn" => Module::PessimisticTxn,
            "gc" => Module::Gc,
            n => Module::Unknown(n.to_owned()),
        }
    }
}

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default)]
pub struct ConfigController {
    current: TiKvConfig,
    config_mgrs: HashMap<Module, Box<dyn ConfigManager>>,
}

impl ConfigController {
    pub fn new(current: TiKvConfig) -> Self {
        ConfigController {
            current,
            config_mgrs: HashMap::new(),
        }
    }

    pub fn update(&mut self, change: HashMap<String, String>) -> CfgResult<()> {
        let diff = to_config_change(change.clone())?;
        {
            let mut incoming = self.current.clone();
            incoming.update(diff.clone());
            incoming.validate()?;
        }
        let mut to_update = HashMap::with_capacity(diff.len());
        for (name, change) in diff.into_iter() {
            match change {
                ConfigValue::Module(change) => {
                    // update a submodule's config only if changes had been sucessfully
                    // dispatched to corresponding config manager, to avoid double dispatch change
                    if let Some(mgr) = self.config_mgrs.get_mut(&Module::from(name.as_str())) {
                        if let Err(e) = mgr.dispatch(change.clone()) {
                            self.current.update(to_update);
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
        self.current.update(to_update);
        // Write change to the config file
        let content = {
            let change = to_toml_encode(change)?;
            let src = if Path::new(&self.current.cfg_path).exists() {
                fs::read_to_string(&self.current.cfg_path)?
            } else {
                String::new()
            };
            let mut t = TomlWriter::new();
            t.write_change(src, change);
            t.finish()
        };
        write_config(
            &self.current.storage.data_dir,
            &self.current.cfg_path,
            &content,
        )?;
        Ok(())
    }

    pub fn update_config(&mut self, name: &str, value: &str) -> CfgResult<()> {
        let mut m = HashMap::new();
        m.insert(name.to_owned(), value.to_owned());
        self.update(m)
    }

    pub fn register(&mut self, module: Module, cfg_mgr: Box<dyn ConfigManager>) {
        if self.config_mgrs.insert(module.clone(), cfg_mgr).is_some() {
            warn!("config manager for module {:?} already registered", module)
        }
    }

    pub fn get_current(&self) -> &TiKvConfig {
        &self.current
    }

    pub fn get_current_mut(&mut self) -> &mut TiKvConfig {
        &mut self.current
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use engine::rocks::util::new_engine_opt;
    use engine_traits::DBOptions as DBOptionsTrait;
    use slog::Level;
    use std::sync::Arc;
    use toml;

    #[test]
    fn test_check_critical_cfg_with() {
        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.storage.data_dir = "/data1".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.storage.data_dir = "/data1".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());
    }

    #[test]
    fn test_persist_cfg() {
        let dir = Builder::new().prefix("test_persist_cfg").tempdir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path().to_str().unwrap();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut tikv_cfg = TiKvConfig::default();

        tikv_cfg.rocksdb.wal_dir = s1.clone();
        tikv_cfg.raftdb.wal_dir = s2.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file);
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s1.clone());
        assert_eq!(cfg_from_file.raftdb.wal_dir, s2.clone());

        // write critical config when exist.
        tikv_cfg.rocksdb.wal_dir = s2.clone();
        tikv_cfg.raftdb.wal_dir = s1.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file);
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
            ("critical", Level::Critical),
            ("error", Level::Error),
            ("warning", Level::Warning),
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

        let compatibility_cases = vec![("warn", Level::Warning)];
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
        incoming.refresh_config_interval = ReadableDuration::hours(10);
        incoming.coprocessor.region_split_keys = 10000;
        incoming.gc.max_write_bytes_per_sec = ReadableSize::mb(100);
        incoming.raft_store.sync_log = false;
        incoming.rocksdb.defaultcf.block_cache_size = ReadableSize::mb(500);
        let diff = old.diff(&incoming);
        let mut change = HashMap::new();
        change.insert("refresh-config-interval".to_owned(), "10h".to_owned());
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert("raftstore.sync-log".to_owned(), "false".to_owned());
        change.insert(
            "rocksdb.defaultcf.block-cache-size".to_owned(),
            "500MB".to_owned(),
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
        change.insert("raftstore.sync-log".to_owned(), "false".to_owned());
        change.insert(
            "rocksdb.defaultcf.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
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
        assert_eq!(res.get("raftstore.sync-log"), Some(&"false".to_owned()));
        assert_eq!(
            res.get("rocksdb.defaultcf.titan.blob-run-mode"),
            Some(&"\"read-only\"".to_owned())
        );
    }

    fn new_engines(cfg: TiKvConfig) -> (RocksEngine, ConfigController) {
        let engine = RocksEngine::from_db(Arc::new(
            new_engine_opt(
                &cfg.storage.data_dir,
                DBOptions::new(),
                vec![
                    CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
                ],
            )
            .unwrap(),
        ));

        let mut cfg_controller = ConfigController::new(cfg);
        cfg_controller.register(
            Module::Rocksdb,
            Box::new(DBConfigManger::new(engine.clone(), DBType::Kv)),
        );
        (engine, cfg_controller)
    }

    #[test]
    fn test_change_rocksdb_config() {
        let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
        cfg.rocksdb.max_background_jobs = 2;
        cfg.rocksdb.defaultcf.disable_auto_compactions = false;
        cfg.rocksdb.defaultcf.target_file_size_base = ReadableSize::mb(64);
        cfg.rocksdb.defaultcf.block_cache_size = ReadableSize::mb(8);
        cfg.validate().unwrap();
        let (db, mut cfg_controller) = new_engines(cfg);

        // update max_background_jobs
        let db_opts = db.get_db_options();
        assert_eq!(db_opts.get_max_background_jobs(), 2);

        cfg_controller
            .update_config("rocksdb.max-background-jobs", "8")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_jobs(), 8);

        // update some configs on default cf
        let defaultcf = db.cf_handle(CF_DEFAULT).unwrap();
        let cf_opts = db.get_options_cf(defaultcf);
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

        let cf_opts = db.get_options_cf(defaultcf);
        assert_eq!(cf_opts.get_disable_auto_compactions(), true);
        assert_eq!(cf_opts.get_target_file_size_base(), ReadableSize::mb(32).0);
        assert_eq!(cf_opts.get_block_cache_capacity(), ReadableSize::mb(256).0);
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
}
