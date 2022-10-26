// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, iter::FromIterator, path::Path};

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use itertools::Itertools;
use online_config::OnlineConfig;
use serde_derive::{Deserialize, Serialize};
use serde_with::with_prefix;
use tikv::config::{TikvConfig, LAST_CONFIG_FILE};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    crit,
    sys::SysQuota,
};

use crate::fatal;

with_prefix!(prefix_apply "apply-");
with_prefix!(prefix_store "store-");
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftstoreConfig {
    pub snap_handle_pool_size: usize,

    #[doc(hidden)]
    #[online_config(skip)]
    pub region_worker_tick_interval: ReadableDuration,
    pub apply_low_priority_pool_size: usize,
}

impl Default for RaftstoreConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();

        RaftstoreConfig {
            region_worker_tick_interval: ReadableDuration::millis(500),
            // This pool is used when handling raft Snapshots, e.g. when
            // adding a new TiFlash replica, or scaling TiFlash instances.
            // The rate limit is by default controlled by PD scheduling limit,
            // so it is safe to have a large default here.
            snap_handle_pool_size: (cpu_num * 0.7).clamp(2.0, 16.0) as usize,
            // This pool is used when handling ingest SST raft messages, e.g.
            // when using BR / lightning.
            apply_low_priority_pool_size: (cpu_num * 0.3).clamp(2.0, 8.0) as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub engine_addr: String,
    pub engine_store_version: String,
    pub engine_store_git_hash: String,
    pub addr: String,
    pub status_addr: String,
    pub advertise_status_addr: String,
    pub advertise_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            engine_addr: DEFAULT_ENGINE_ADDR.to_string(),
            engine_store_version: String::default(),
            engine_store_git_hash: String::default(),
            addr: TIFLASH_DEFAULT_LISTENING_ADDR.to_string(),
            status_addr: TIFLASH_DEFAULT_STATUS_ADDR.to_string(),
            advertise_status_addr: TIFLASH_DEFAULT_STATUS_ADDR.to_string(),
            advertise_addr: TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    #[online_config(skip)]
    // Reserve disk space to make tikv would have enough space to compact when disk is full.
    pub reserve_space: ReadableSize,
}

impl Default for StorageConfig {
    fn default() -> StorageConfig {
        let _cpu_num = SysQuota::cpu_cores_quota();
        StorageConfig {
            reserve_space: ReadableSize::gb(1), // No longer use DEFAULT_RESERVED_SPACE_GB
        }
    }
}

pub const DEFAULT_ENGINE_ADDR: &str = if cfg!(feature = "failpoints") {
    "127.0.0.1:20206"
} else {
    ""
};

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

pub fn memory_limit_for_cf(is_raft_db: bool, cf: &str, total_mem: u64) -> ReadableSize {
    let (ratio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.05, 256 * MIB as usize, usize::MAX),
        (false, CF_DEFAULT) => (0.25, 0, 128 * MIB as usize),
        (false, CF_LOCK) => (0.02, 0, 128 * MIB as usize),
        (false, CF_WRITE) => (0.15, 0, 128 * MIB as usize),
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

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DefaultCfConfig {
    pub block_cache_size: ReadableSize,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LockCfConfig {
    pub block_cache_size: ReadableSize,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct WriteCfConfig {
    pub block_cache_size: ReadableSize,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[online_config(submodule)]
    pub defaultcf: DefaultCfConfig,
}

impl Default for RaftDbConfig {
    fn default() -> Self {
        let total_mem = SysQuota::memory_limit_in_bytes();
        RaftDbConfig {
            defaultcf: DefaultCfConfig {
                block_cache_size: memory_limit_for_cf(true, CF_DEFAULT, total_mem),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RocksDbConfig {
    #[online_config(submodule)]
    pub lockcf: LockCfConfig,
    #[online_config(submodule)]
    pub writecf: WriteCfConfig,
    #[online_config(submodule)]
    pub defaultcf: DefaultCfConfig,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        let total_mem = SysQuota::memory_limit_in_bytes();
        RocksDbConfig {
            defaultcf: DefaultCfConfig {
                block_cache_size: memory_limit_for_cf(false, CF_DEFAULT, total_mem),
            },
            writecf: WriteCfConfig {
                block_cache_size: memory_limit_for_cf(false, CF_WRITE, total_mem),
            },
            lockcf: LockCfConfig {
                block_cache_size: memory_limit_for_cf(false, CF_LOCK, total_mem),
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ProxyConfig {
    #[online_config(submodule)]
    pub server: ServerConfig,

    #[online_config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,

    #[online_config(submodule)]
    pub rocksdb: RocksDbConfig,
    #[online_config(submodule)]
    pub raftdb: RaftDbConfig,

    #[online_config(submodule)]
    pub storage: StorageConfig,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub enable_io_snoop: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            raft_store: RaftstoreConfig::default(),
            server: ServerConfig::default(),
            rocksdb: RocksDbConfig::default(),
            raftdb: RaftDbConfig::default(),
            storage: StorageConfig::default(),
            enable_io_snoop: false,
        }
    }
}

impl ProxyConfig {
    pub fn from_file(
        path: &Path,
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let cfg: ProxyConfig = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <ProxyConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        Ok(cfg)
    }
}

pub fn ensure_no_common_unrecognized_keys(
    proxy_unrecognized_keys: &[String],
    unrecognized_keys: &[String],
) -> Result<(), String> {
    // We can't just compute intersection, since `rocksdb.z` equals not `rocksdb`.
    let proxy_part = HashSet::<_>::from_iter(proxy_unrecognized_keys.iter());
    let inter = unrecognized_keys
        .iter()
        .filter(|s| {
            let mut pref: String = String::from("");
            for p in s.split('.') {
                if !pref.is_empty() {
                    pref += "."
                }
                pref += p;
                if proxy_part.contains(&pref) {
                    // common unrecognized by both config.
                    return true;
                }
            }
            false
        })
        .collect::<Vec<_>>();
    if inter.len() != 0 {
        return Err(inter.iter().join(", "));
    }
    Ok(())
}

// Not the same as TiKV
pub const TIFLASH_DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:20170";
pub const TIFLASH_DEFAULT_STATUS_ADDR: &str = "127.0.0.1:20292";
// Same as TiKV
pub const TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR: &str = "";

pub fn make_tikv_config() -> TikvConfig {
    let mut default = TikvConfig::default();
    setup_default_tikv_config(&mut default);
    default
}

pub fn setup_default_tikv_config(default: &mut TikvConfig) {
    // Compat CI test. If there is no config file given, we will use this default.
    default.server.addr = TIFLASH_DEFAULT_LISTENING_ADDR.to_string();
    default.server.advertise_addr = TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR.to_string();
    default.server.status_addr = TIFLASH_DEFAULT_STATUS_ADDR.to_string();
    default.server.advertise_status_addr = TIFLASH_DEFAULT_STATUS_ADDR.to_string();
    // Do not add here, try use `address_proxy_config`.
}

/// This function changes TiKV's config according to ProxyConfig.
pub fn address_proxy_config(config: &mut TikvConfig, proxy_config: &ProxyConfig) {
    // We must add engine label to our TiFlash config
    pub const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";
    let engine_name = match option_env!("ENGINE_LABEL_VALUE") {
        None => {
            fatal!("should set engine name with env variable `ENGINE_LABEL_VALUE`");
        }
        Some(name) => name.to_owned(),
    };
    config
        .server
        .labels
        .insert(DEFAULT_ENGINE_LABEL_KEY.to_owned(), engine_name);
    config.raft_store.region_worker_tick_interval =
        proxy_config.raft_store.region_worker_tick_interval;
    let clean_stale_ranges_tick =
        (10_000 / config.raft_store.region_worker_tick_interval.as_millis()) as usize;
    config.raft_store.clean_stale_ranges_tick = clean_stale_ranges_tick;
    config.raft_store.apply_batch_system.low_priority_pool_size =
        proxy_config.raft_store.apply_low_priority_pool_size;
    config.raftdb.defaultcf.block_cache_size = proxy_config.raftdb.defaultcf.block_cache_size;
    config.rocksdb.defaultcf.block_cache_size = proxy_config.rocksdb.defaultcf.block_cache_size;
    config.rocksdb.writecf.block_cache_size = proxy_config.rocksdb.writecf.block_cache_size;
    config.rocksdb.lockcf.block_cache_size = proxy_config.rocksdb.lockcf.block_cache_size;

    config.storage.reserve_space = proxy_config.storage.reserve_space;

    config.enable_io_snoop = proxy_config.enable_io_snoop;
    config.server.addr = proxy_config.server.addr.clone();
    config.server.advertise_addr = proxy_config.server.advertise_addr.clone();
    config.server.status_addr = proxy_config.server.status_addr.clone();
    config.server.advertise_status_addr = proxy_config.server.advertise_status_addr.clone();
}

pub fn validate_and_persist_config(config: &mut TikvConfig, persist: bool) {
    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e);
    }

    if let Err(e) = check_critical_config(config) {
        fatal!("critical config check failed: {}", e);
    }

    if persist {
        if let Err(e) = tikv::config::persist_config(config) {
            fatal!("persist critical config failed: {}", e);
        }
    }
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &TikvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    if let Some(mut cfg) = get_last_config(&config.storage.data_dir) {
        info!("check_critical_config finished compatible_adjust");
        cfg.compatible_adjust();
        if let Err(e) = cfg.validate() {
            warn!("last_tikv.toml is invalid but ignored: {:?}", e);
        }
        info!("check_critical_config finished validate");
        config.check_critical_cfg_with(&cfg)?;
        info!("check_critical_config finished check_critical_cfg_with");
    }
    Ok(())
}

pub fn get_last_config(data_dir: &str) -> Option<TikvConfig> {
    let store_path = Path::new(data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    let mut v: Vec<String> = vec![];
    if last_cfg_path.exists() {
        let s = TikvConfig::from_file(&last_cfg_path, Some(&mut v)).unwrap_or_else(|e| {
            error!(
                "invalid auto generated configuration file {}, err {}",
                last_cfg_path.display(),
                e
            );
            std::process::exit(1)
        });
        if !v.is_empty() {
            info!("unrecognized in last config";
                "config" => ?v,
                "file" => last_cfg_path.display(),
            );
        }
        return Some(s);
    }
    None
}
