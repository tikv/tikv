// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod config_manager;
pub mod coprocessor;
pub mod gc_worker;
pub mod lock_manager;
pub mod raftstore;
pub mod read_pool;
pub mod rocksdb;
pub mod server;
pub mod storage;

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TiKvConfig` type, which is in turn
//! made up of many other configuration types.

// use std::cmp::{self, Ord, Ordering};
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Error as IoError;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::usize;

// use kvproto::configpb::{self, StatusCode};

// use configuration::{ConfigChange, ConfigValue, Configuration};
// use engine::rocks::{
//     BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
//     DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, LRUCacheOptions,
//     TitanDBOptions,
// };
use slog;
use sys_info;

use crate::import::Config as ImportConfig;
// use crate::raftstore::coprocessor::properties::{
//     MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
// };
// use crate::raftstore::coprocessor::properties::{
//     DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
// };
use crate::raftstore::coprocessor::Config as CopConfig;
use crate::raftstore::store::Config as RaftstoreConfig;
// use crate::raftstore::store::PdTask;
use crate::server::gc_worker::GcConfig;
use crate::server::lock_manager::Config as PessimisticTxnConfig;
use crate::server::Config as ServerConfig;
// use crate::server::CONFIG_ROCKSDB_GAUGE;
use crate::storage::config::{Config as StorageConfig, DEFAULT_DATA_DIR, DEFAULT_ROCKSDB_SUB_DIR};
// use engine::rocks::util::config::{self as rocks_config, BlobRunMode, CompressionType};
// use engine::rocks::util::{
//     db_exist, CFOptions, EventListener, FixedPrefixSliceTransform, FixedSuffixSliceTransform,
//     NoopSliceTransform,
// };
// use engine::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
// use keys::region_raft_prefix_len;
use pd_client::{Config as PdConfig, PdClient};
use tikv_util::config::{self, ReadableDuration, ReadableSize};
// use tikv_util::future_pool;
use tikv_util::security::SecurityConfig;
use tikv_util::time::duration_to_sec;
// use tikv_util::worker::FutureScheduler;

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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[config(skip)]
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
    #[config(skip)]
    pub log_file: String,
    #[config(skip)]
    pub log_rotation_timespan: ReadableDuration,
    #[config(skip)]
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
    #[config(skip)]
    pub metric: MetricConfig,
    #[config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,
    #[config(skip)]
    pub coprocessor: CopConfig,
    #[config(skip)]
    pub rocksdb: DbConfig,
    #[config(skip)]
    pub raftdb: RaftDbConfig,
    #[config(skip)]
    pub security: SecurityConfig,
    #[config(skip)]
    pub import: ImportConfig,
    #[config(skip)]
    pub pessimistic_txn: PessimisticTxnConfig,
    #[config(skip)]
    pub gc: GcConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            log_rotation_timespan: ReadableDuration::hours(24),
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
            import: ImportConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
            gc: GcConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
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

        self.rocksdb.validate()?;
        self.raftdb.validate()?;
        self.server.validate()?;
        self.raft_store.validate()?;
        self.pd.validate()?;
        self.coprocessor.validate()?;
        self.security.validate()?;
        self.import.validate()?;
        self.pessimistic_txn.validate()?;
        self.gc.validate()?;
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
            Ok(::toml::from_str(&s)?)
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
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &TiKvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

    if last_cfg_path.exists() {
        let last_cfg = TiKvConfig::from_file(&last_cfg_path);
        config.check_critical_cfg_with(&last_cfg)?;
    }

    Ok(())
}

/// Persists critical config to `last_tikv.toml`
pub fn persist_critical_config(config: &TiKvConfig) -> Result<(), String> {
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current critical configurations to file.
    if let Err(e) = config.write_to_file(&last_cfg_path) {
        return Err(format!(
            "persist critical config to '{}' failed: {}",
            last_cfg_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use kvproto::configpb::Version;
    use slog::Level;
    use std::cmp::Ordering;
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
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s2.clone());
        assert_eq!(cfg_from_file.raftdb.wal_dir, s1.clone());
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
        assert!(persist_critical_config(&tikv_cfg).is_ok());
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
}
