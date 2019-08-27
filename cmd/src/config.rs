// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::fs;
use std::io::{Error as IoError, Write};
use std::path::Path;

use tikv::config::TiKvConfig;

const LAST_CONFIG_FILE: &str = "last_tikv.toml";

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(flatten)]
    pub tikv_cfg: TiKvConfig,
    // Add more config here.
}

impl Default for Config {
    fn default() -> Config {
        Config {
            tikv_cfg: TiKvConfig::default(),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.tikv_cfg.validate()
    }

    pub fn compatible_adjust(&mut self) {
        self.tikv_cfg.compatible_adjust();
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), String> {
        self.tikv_cfg.check_critical_cfg_with(&last_cfg.tikv_cfg)
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
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &Config) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    let store_path = Path::new(&config.tikv_cfg.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

    if last_cfg_path.exists() {
        let last_cfg = Config::from_file(&last_cfg_path);
        config.check_critical_cfg_with(&last_cfg)?;
    }

    Ok(())
}

/// Persists critical config to `last_tikv.toml`
pub fn persist_critical_config(config: &Config) -> Result<(), String> {
    let store_path = Path::new(&config.tikv_cfg.storage.data_dir);
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
    use super::*;
    use tempfile::Builder;
    use toml;

    #[test]
    fn test_persist_cfg() {
        let dir = Builder::new().prefix("test_persist_cfg").tempdir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path().to_str().unwrap();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut cfg = Config::default();

        cfg.tikv_cfg.rocksdb.wal_dir = s1.clone();
        cfg.tikv_cfg.raftdb.wal_dir = s2.clone();
        cfg.write_to_file(file).unwrap();
        let cfg_from_file = Config::from_file(file);
        assert_eq!(cfg_from_file.tikv_cfg.rocksdb.wal_dir, s1.clone());
        assert_eq!(cfg_from_file.tikv_cfg.raftdb.wal_dir, s2.clone());

        // write critical config when exist.
        cfg.tikv_cfg.rocksdb.wal_dir = s2.clone();
        cfg.tikv_cfg.raftdb.wal_dir = s1.clone();
        cfg.write_to_file(file).unwrap();
        let cfg_from_file = Config::from_file(file);
        assert_eq!(cfg_from_file.tikv_cfg.rocksdb.wal_dir, s2.clone());
        assert_eq!(cfg_from_file.tikv_cfg.raftdb.wal_dir, s1.clone());
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .tempdir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut cfg = Config::default();
        cfg.tikv_cfg.storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        assert!(check_critical_config(&cfg).is_ok());
    }

    #[test]
    fn test_flatten() {
        // With flatten feature, the output string should be the same.
        let tikv_str = toml::to_string(&TiKvConfig::default()).unwrap();
        let cfg_str = toml::to_string(&Config::default()).unwrap();
        assert_eq!(tikv_str, cfg_str);

        // A serialized TiKvConfig should be able to deserialize to a Config.
        assert_eq!(Config::default(), toml::from_str(&tikv_str).unwrap());
    }
}
