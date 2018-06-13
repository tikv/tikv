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

extern crate toml;

use std::error::Error;
use std::fmt;
use std::fs;
use std::i32;
use std::io::Error as IoError;
use std::io::{Read, Write};
use std::path::Path;
use std::usize;

use rocksdb::{
    BlockBasedOptions, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBOptions, DBRecoveryMode,
};
use slog;
use sys_info;

use import::Config as ImportConfig;
use pd::Config as PdConfig;
use raftstore::coprocessor::Config as CopConfig;
use raftstore::store::keys::region_raft_prefix_len;
use raftstore::store::Config as RaftstoreConfig;
use server::readpool;
use server::Config as ServerConfig;
use storage::{
    Config as StorageConfig, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, DEFAULT_ROCKSDB_SUB_DIR,
};
use util::config::{
    self, compression_type_level_serde, ReadableDuration, ReadableSize, GB, KB, MB,
};
use util::properties::{MvccPropertiesCollectorFactory, SizePropertiesCollectorFactory};
use util::rocksdb::{
    db_exist, CFOptions, EventListener, FixedPrefixSliceTransform, FixedSuffixSliceTransform,
    NoopSliceTransform,
};
use util::security::SecurityConfig;
use util::time::duration_to_sec;

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;
pub const LAST_CONFIG_FILE: &str = "last_tikv.toml";

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
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

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            pub disable_block_cache: bool,
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "compression_type_level_serde")]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "config::compaction_pri_serde")]
            pub compaction_pri: CompactionPriority,
            pub dynamic_level_bytes: bool,
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "config::compaction_style_serde")]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
        }
    };
}

macro_rules! build_cf_opt {
    ($opt:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        block_base_opts.set_lru_cache($opt.block_cache_size.0 as usize, -1, 0, 0.0);
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
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-properties-collector", f);
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_WRITE) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
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
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
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
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-properties-collector", f);
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_LOCK) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
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
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        RaftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
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
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[serde(with = "config::recovery_mode_serde")]
    pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub defaultcf: DefaultCfConfig,
    pub writecf: WriteCfConfig,
    pub lockcf: LockCfConfig,
    pub raftcf: RaftCfConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: 6,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::kb(0),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_dir: "".to_owned(),
            rate_bytes_per_sec: ReadableSize::kb(0),
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
            raftcf: RaftCfConfig::default(),
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
            opts.set_ratelimiter(self.rate_bytes_per_sec.0 as i64);
        }
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.add_event_listener(EventListener::new("kv"));
        opts
    }

    pub fn build_cf_opts(&self) -> Vec<CFOptions> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt()),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt()),
            CFOptions::new(CF_WRITE, self.writecf.build_opt()),
            CFOptions::new(CF_RAFT, self.raftcf.build_opt()),
        ]
    }

    fn validate(&mut self) -> Result<(), Box<Error>> {
        Ok(())
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
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl RaftDefaultCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(FixedPrefixSliceTransform::new(region_raft_prefix_len()));
        cf_opts
            .set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform", f)
            .unwrap();
        cf_opts
    }
}

// RocksDB Env associate thread pools of multiple instances from the same process.
// When construct Options, options.env is set to same singleton Env::Default() object.
// If we set same env parameter in different instance, we may overwrite other instance's config.
// So we only set max_background_jobs in default rocksdb.
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[serde(with = "config::recovery_mode_serde")]
    pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_dir: String,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub defaultcf: RaftDefaultCfConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::kb(0),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_dir: "".to_owned(),
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            allow_concurrent_memtable_write: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcf: RaftDefaultCfConfig::default(),
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
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
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
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(EventListener::new("raft"));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);

        opts
    }

    pub fn build_cf_opts(&self) -> Vec<CFOptions> {
        vec![CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt())]
    }
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
    use util::logger::{get_level_by_string, get_string_by_level};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_level_by_string(&string)
            .ok_or_else(|| D::Error::invalid_value(Unexpected::Str(&string), &"a valid log level"))
    }

    pub fn serialize<S>(value: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_level(value).serialize(serializer)
    }
}

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            pub fn build_config(&self) -> readpool::Config {
                readpool::Config {
                    high_concurrency: self.high_concurrency,
                    normal_concurrency: self.normal_concurrency,
                    low_concurrency: self.low_concurrency,
                    max_tasks_per_worker_high: self.max_tasks_per_worker_high,
                    max_tasks_per_worker_normal: self.max_tasks_per_worker_normal,
                    max_tasks_per_worker_low: self.max_tasks_per_worker_low,
                    stack_size: self.stack_size,
                }
            }

            pub fn validate(&self) -> Result<(), Box<Error>> {
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    ).into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    ).into());
                }
                if self.low_concurrency == 0 {
                    return Err(
                        format!("readpool.{}.low-concurrency should be > 0", $display_name).into(),
                    );
                }
                if self.stack_size.0 < ReadableSize::mb(2).0 {
                    return Err(
                        format!("readpool.{}.stack-size should be >= 2mb", $display_name).into(),
                    );
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    ).into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    ).into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    ).into());
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
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_CONCURRENCY: usize = 4;

readpool_config!(StorageReadPoolConfig, storage_read_pool_test, "storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        Self {
            high_concurrency: DEFAULT_STORAGE_READPOOL_CONCURRENCY,
            normal_concurrency: DEFAULT_STORAGE_READPOOL_CONCURRENCY,
            low_concurrency: DEFAULT_STORAGE_READPOOL_CONCURRENCY,
            max_tasks_per_worker_high: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(readpool::config::DEFAULT_STACK_SIZE_MB),
        }
    }
}

const DEFAULT_COPROCESSOR_READPOOL_CONCURRENCY: usize = 8;

readpool_config!(
    CoprocessorReadPoolConfig,
    coprocessor_read_pool_test,
    "coprocessor"
);

impl Default for CoprocessorReadPoolConfig {
    fn default() -> Self {
        let cpu_num = sys_info::cpu_num().unwrap();
        let concurrency = if cpu_num > 8 {
            (f64::from(cpu_num) * 0.8) as usize
        } else {
            DEFAULT_COPROCESSOR_READPOOL_CONCURRENCY
        };
        Self {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: readpool::config::DEFAULT_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(readpool::config::DEFAULT_STACK_SIZE_MB),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub storage: StorageReadPoolConfig,
    pub coprocessor: CoprocessorReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
        self.storage.validate()?;
        self.coprocessor.validate()?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
    pub log_file: String,
    pub readpool: ReadPoolConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub pd: PdConfig,
    pub metric: MetricConfig,
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,
    pub coprocessor: CopConfig,
    pub rocksdb: DbConfig,
    pub raftdb: RaftDbConfig,
    pub security: SecurityConfig,
    pub import: ImportConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
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
            import: ImportConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
        self.readpool.validate()?;
        self.storage.validate()?;

        self.raft_store.region_split_check_diff = self.coprocessor.region_split_size / 16;
        self.raft_store.raftdb_path = if self.raft_store.raftdb_path.is_empty() {
            config::canonicalize_sub_path(&self.storage.data_dir, "raft")?
        } else {
            config::canonicalize_path(&self.raft_store.raftdb_path)?
        };

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

        let expect_keepalive = self.raft_store.raft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time is too small, it should not less than the double of \
                 raft tick interval (>= {})",
                duration_to_sec(expect_keepalive)
            ).into());
        }

        self.rocksdb.validate()?;
        self.server.validate()?;
        self.raft_store.validate()?;
        self.pd.validate()?;
        self.coprocessor.validate()?;
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
            let concurrency = self.server.end_point_concurrency.unwrap();
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
            self.readpool.coprocessor.stack_size = self.server.end_point_stack_size.unwrap();
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
            let delay_secs = self.raft_store.clean_stale_peer_delay.as_secs()
                + self.server.end_point_request_max_handle_duration.as_secs();
            self.raft_store.clean_stale_peer_delay = ReadableDuration::secs(delay_secs);
        }
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), Box<Error>> {
        if last_cfg.rocksdb.wal_dir != self.rocksdb.wal_dir {
            return Err(format!(
                "db wal_dir have been changed, former db wal_dir is {}, \
                 current db wal_dir is {}, please guarantee all data wal log \
                 have been moved to destination directory.",
                last_cfg.rocksdb.wal_dir, self.rocksdb.wal_dir
            ).into());
        }

        if last_cfg.raftdb.wal_dir != self.raftdb.wal_dir {
            return Err(format!(
                "raftdb wal_dir have been changed, former raftdb wal_dir is {}, \
                 current raftdb wal_dir is {}, please guarantee all raft wal log \
                 have been moved to destination directory.",
                last_cfg.raftdb.wal_dir, self.rocksdb.wal_dir
            ).into());
        }

        if last_cfg.storage.data_dir != self.storage.data_dir {
            return Err(format!(
                "storage data dir have been changed, former data dir is {}, \
                 current data dir is {}, please check if it is expected.",
                last_cfg.storage.data_dir, self.storage.data_dir
            ).into());
        }

        if last_cfg.raft_store.raftdb_path != self.raft_store.raftdb_path {
            return Err(format!(
                "raft dir have been changed, former raft dir is {}, \
                 current raft dir is {}, please check if it is expected.",
                last_cfg.raft_store.raftdb_path, self.raft_store.raftdb_path
            ).into());
        }

        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Self
    where
        P: fmt::Debug,
    {
        fs::File::open(&path)
            .map_err::<Box<Error>, _>(|e| Box::new(e))
            .and_then(|mut f| {
                let mut s = String::new();
                f.read_to_string(&mut s)?;
                let c = toml::from_str(&s)?;
                Ok(c)
            })
            .unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {:?}, err {}",
                    path, e
                );
            })
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = toml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }
}

pub fn check_and_persist_critical_config(config: &TiKvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    if last_cfg_path.exists() {
        let last_cfg = TiKvConfig::from_file(&last_cfg_path);
        if let Err(e) = config.check_critical_cfg_with(&last_cfg) {
            return Err(format!("check critical config failed, err {:?}", e));
        }
    }

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&store_path) {
        return Err(format!(
            "create parent directory {} failed, err {:?}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current critical configurations to file.
    if let Err(e) = config.write_to_file(&last_cfg_path) {
        return Err(format!(
            "persist critical config to {} failed, err {:?}",
            last_cfg_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use super::*;
    use slog::Level;
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
        let dir = TempDir::new("test_persist_cfg").unwrap();
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
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s2.clone());
        assert_eq!(cfg_from_file.raftdb.wal_dir, s1.clone());
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let tmp_path = TempDir::new("test_create_parent_dir_if_missing").unwrap();
        let pathbuf = tmp_path.into_path().join("not_exist_dir");

        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.storage.data_dir = pathbuf.as_path().to_str().unwrap().to_owned();
        assert!(check_and_persist_critical_config(&tikv_cfg).is_ok());
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
}
