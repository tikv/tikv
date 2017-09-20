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

use std::error::Error;
use std::path::Path;
use std::usize;

use log::LogLevelFilter;
use rocksdb::{BlockBasedOptions, ColumnFamilyOptions, CompactionPriority, DBCompressionType,
              DBOptions, DBRecoveryMode};
use sys_info;

use server::Config as ServerConfig;
use raftstore::store::Config as RaftstoreConfig;
use raftstore::store::keys::region_raft_prefix_len;
use storage::{Config as StorageConfig, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, DEFAULT_DATA_DIR,
              DEFAULT_ROCKSDB_SUB_DIR};
use util::config::{self, compression_type_level_serde, ReadableDuration, ReadableSize, GB, KB, MB};
use util::properties::{MvccPropertiesCollectorFactory, SizePropertiesCollectorFactory};
use util::rocksdb::{db_exist, CFOptions, EventListener, FixedPrefixSliceTransform,
                    FixedSuffixSliceTransform, NoopSliceTransform};

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
    let (radio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.02, RAFT_MIN_MEM, RAFT_MAX_MEM),
        (false, CF_DEFAULT) => (0.25, 0, usize::MAX),
        (false, CF_LOCK) => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        (false, CF_WRITE) => (0.15, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * radio) as usize;
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
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
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
        }
    }
}

macro_rules! build_cf_opt {
    ($opt:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_lru_cache($opt.block_cache_size.0 as usize);
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts.set_pin_l0_filter_and_index_blocks_in_cache(
            $opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter($opt.bloom_filter_bits_per_key,
                                             $opt.block_based_bloom_filter);
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.compression_per_level(&$opt.compression_per_level);
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
        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64),
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub backup_dir: String,
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
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            backup_dir: "".to_owned(),
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
        if self.enable_statistics {
            opts.enable_statistics();
            opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        }
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir).unwrap_or_else(
                |e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir,
                        e
                    );
                },
            )
        }
        if self.rate_bytes_per_sec.0 > 0 {
            opts.set_ratelimiter(self.rate_bytes_per_sec.0 as i64);
        }
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
        if !self.backup_dir.is_empty() {
            self.backup_dir = try!(config::canonicalize_path(&self.backup_dir));
        }
        Ok(())
    }
}

cf_config!(RaftDefaultCfConfig);

impl Default for RaftDefaultCfConfig {
    fn default() -> RaftDefaultCfConfig {
        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(true, CF_DEFAULT) as u64),
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
        if self.enable_statistics {
            opts.enable_statistics();
            opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        }
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir).unwrap_or_else(
                |e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir,
                        e
                    );
                },
            )
        }
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(EventListener::new("raft"));
        opts
    }

    pub fn build_cf_opts(&self) -> Vec<CFOptions> {
        vec![CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt())]
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PdConfig {
    pub endpoints: Vec<String>,
}

impl PdConfig {
    fn validate(&self) -> Result<(), Box<Error>> {
        if self.endpoints.is_empty() {
            return Err("please specify pd.endpoints.".into());
        }
        for addr in &self.endpoints {
            try!(config::check_addr(addr));
        }
        Ok(())
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

#[derive(Serialize, Deserialize)]
#[serde(remote = "LogLevelFilter")]
#[serde(rename_all = "kebab-case")]
pub enum LogLevel {
    Info,
    Trace,
    Debug,
    Warn,
    Error,
    Off,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[serde(with = "LogLevel")]
    pub log_level: LogLevelFilter,
    pub log_file: String,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub pd: PdConfig,
    pub metric: MetricConfig,
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,
    pub rocksdb: DbConfig,
    pub raftdb: RaftDbConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            log_level: LogLevelFilter::Info,
            log_file: "".to_owned(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            raft_store: RaftstoreConfig::default(),
            pd: PdConfig::default(),
            rocksdb: DbConfig::default(),
            raftdb: RaftDbConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
        try!(self.storage.validate());
        if self.rocksdb.backup_dir.is_empty() && self.storage.data_dir != DEFAULT_DATA_DIR {
            self.rocksdb.backup_dir = format!(
                "{}",
                Path::new(&self.storage.data_dir).join("backup").display()
            );
        }

        self.raft_store.raftdb_path = if self.raft_store.raftdb_path.is_empty() {
            try!(config::canonicalize_sub_path(
                &self.storage.data_dir,
                "raft"
            ))
        } else {
            try!(config::canonicalize_path(&self.raft_store.raftdb_path))
        };

        let kv_db_path = try!(config::canonicalize_sub_path(
            &self.storage.data_dir,
            DEFAULT_ROCKSDB_SUB_DIR
        ));

        if kv_db_path == self.raft_store.raftdb_path {
            return Err(
                "raft_store.raftdb_path can not same with storage.data_dir/db".into(),
            );
        }
        if db_exist(&kv_db_path) && !db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb exist, buf raftdb not exist".into());
        }
        if !db_exist(&kv_db_path) && db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb not exist, buf raftdb exist".into());
        }

        try!(self.rocksdb.validate());
        try!(self.server.validate());
        try!(self.raft_store.validate());
        try!(self.pd.validate());
        Ok(())
    }
}
