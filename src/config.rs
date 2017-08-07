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
use rocksdb::{DBCompressionType, DBRecoveryMode, DBOptions, ColumnFamilyOptions,
              BlockBasedOptions, CompactionPriority};
use sys_info;

use server::Config as ServerConfig;
use raftstore::store::Config as RaftstoreConfig;
use raftstore::store::keys::region_raft_prefix_len;
use storage::{Config as StorageConfig, CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT, DEFAULT_DATA_DIR};
use util::config::{self, ReadableDuration, ReadableSize, KB, MB, GB, compression_type_level_serde};
use util::properties::MvccPropertiesCollectorFactory;
use util::rocksdb::{FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform,
                    CFOptions};

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFTCF_MIN_MEM: usize = 256 * MB as usize;
const RAFTCF_MAX_MEM: usize = 2 * GB as usize;

fn memory_for_cf(cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
    let (radio, min, max) = match cf {
        CF_DEFAULT => (0.25, 0, usize::MAX),
        CF_LOCK => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        CF_WRITE => (0.15, 0, usize::MAX),
        CF_RAFT => (0.02, RAFTCF_MIN_MEM, RAFTCF_MAX_MEM),
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CfConfig {
    #[serde(skip)]
    pub cf: String,
    pub block_size: ReadableSize,
    pub block_cache_size: ReadableSize,
    pub cache_index_and_filter_blocks: bool,
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

impl Default for CfConfig {
    fn default() -> CfConfig {
        CfConfig {
            cf: CF_DEFAULT.to_owned(),
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_for_cf(CF_DEFAULT) as u64),
            cache_index_and_filter_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            compression_per_level: [DBCompressionType::No,
                                    DBCompressionType::No,
                                    DBCompressionType::Lz4,
                                    DBCompressionType::Lz4,
                                    DBCompressionType::Lz4,
                                    DBCompressionType::Zstd,
                                    DBCompressionType::Zstd],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(32),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
        }
    }
}

impl CfConfig {
    fn write_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_WRITE.to_owned();
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_WRITE) as u64);
        cfg.whole_key_filtering = false;
        cfg
    }

    fn raft_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_RAFT.to_owned();
        cfg.use_bloom_filter = false;
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_RAFT) as u64);
        cfg.compaction_pri = CompactionPriority::ByCompensatedSize;
        cfg
    }

    fn lock_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_LOCK.to_owned();
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_LOCK) as u64);
        cfg.block_size = ReadableSize::kb(16);
        cfg.whole_key_filtering = true;
        cfg.compression_per_level = [DBCompressionType::No; 7];
        cfg.level0_file_num_compaction_trigger = 1;
        cfg.max_bytes_for_level_base = ReadableSize::mb(128);
        cfg.compaction_pri = CompactionPriority::ByCompensatedSize;
        cfg
    }

    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size(self.block_size.0 as usize);
        block_base_opts.set_lru_cache(self.block_cache_size.0 as usize);
        block_base_opts.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);
        if self.use_bloom_filter {
            block_base_opts.set_bloom_filter(self.bloom_filter_bits_per_key,
                                             self.block_based_bloom_filter);
            block_base_opts.set_whole_key_filtering(self.whole_key_filtering);
        }
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.compression_per_level(&self.compression_per_level);
        cf_opts.set_write_buffer_size(self.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number(self.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge(self.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base(self.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base(self.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger(self.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger(self.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger(self.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes(self.max_compaction_bytes.0);
        cf_opts.compaction_priority(self.compaction_pri);

        match self.cf.as_str() {
            CF_DEFAULT => {}
            CF_WRITE => {
                // Prefix extractor(trim the timestamp at tail) for write cf.
                let e = Box::new(FixedSuffixSliceTransform::new(8));
                cf_opts.set_prefix_extractor("FixedSuffixSliceTransform", e).unwrap();
                // Create prefix bloom filter for memtable.
                cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
                // Collects user defined properties.
                let f = Box::new(MvccPropertiesCollectorFactory::default());
                cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
            }
            CF_RAFT => {
                let f = Box::new(FixedPrefixSliceTransform::new(region_raft_prefix_len()));
                cf_opts.set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform", f)
                    .unwrap();
            }
            CF_LOCK => {
                let f = Box::new(NoopSliceTransform);
                cf_opts.set_prefix_extractor("NoopSliceTransform", f).unwrap();
                cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
            }
            _ => unreachable!(),
        }

        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize)]
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
    pub rate_bytes_per_sec: i64,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub backup_dir: String,
    pub defaultcf: CfConfig,
    pub writecf: CfConfig,
    pub raftcf: CfConfig,
    pub lockcf: CfConfig,
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
            rate_bytes_per_sec: 0,
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            backup_dir: "".to_owned(),
            defaultcf: CfConfig::default(),
            writecf: CfConfig::write_config(),
            raftcf: CfConfig::raft_config(),
            lockcf: CfConfig::lock_config(),
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
            opts.create_info_log(&self.info_log_dir).unwrap_or_else(|e| {
                panic!("create RocksDB info log {} error: {:?}",
                       self.info_log_dir,
                       e);
            })
        }
        if self.rate_bytes_per_sec > 0 {
            opts.set_ratelimiter(self.rate_bytes_per_sec);
        }
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(self.use_direct_io_for_flush_and_compaction);
        opts.enable_pipelined_write(self.enable_pipelined_write);
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

#[derive(Clone, Serialize, Deserialize, Default)]
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

#[derive(Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize)]
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
            storage: StorageConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
        try!(self.storage.validate());
        if self.rocksdb.backup_dir.is_empty() && self.storage.data_dir != DEFAULT_DATA_DIR {
            self.rocksdb.backup_dir =
                format!("{}",
                        Path::new(&self.storage.data_dir).join("backup").display());
        }
        try!(self.rocksdb.validate());
        try!(self.server.validate());
        try!(self.raft_store.validate());
        try!(self.pd.validate());
        Ok(())
    }
}
