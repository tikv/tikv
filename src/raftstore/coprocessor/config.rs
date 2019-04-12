// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use tikv_util::config::ReadableSize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a region with table prefix if
    /// that region crosses tables.
    pub split_region_on_table: bool,

    /// For once split check, there are several split_key produced for batch.
    /// batch_split_limit limits the number of produced split-key for one batch.
    pub batch_split_limit: u64,

    /// When region [a,e) size meets region_max_size, it will be split into
    /// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
    /// [b,c), [c,d) will be region_split_size (maybe a little larger).
    pub region_max_size: ReadableSize,
    pub region_split_size: ReadableSize,

    /// When the number of keys in region [a,e) meets the region_max_keys,
    /// it will be split into two several regions [a,b), [b,c), [c,d), [d,e).
    /// And the number of keys in [a,b), [b,c), [c,d) will be region_split_keys.
    pub region_max_keys: u64,
    pub region_split_keys: u64,
}

/// Default region split size.
pub const SPLIT_SIZE_MB: u64 = 96;
/// Default region split keys.
pub const SPLIT_KEYS: u64 = 960000;
/// Default batch split limit.
pub const BATCH_SPLIT_LIMIT: u64 = 10;

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(SPLIT_SIZE_MB);
        Config {
            split_region_on_table: true,
            batch_split_limit: BATCH_SPLIT_LIMIT,
            region_split_size: split_size,
            region_max_size: split_size / 2 * 3,
            region_split_keys: SPLIT_KEYS,
            region_max_keys: SPLIT_KEYS / 2 * 3,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.region_max_size.0 < self.region_split_size.0 {
            return Err(box_err!(
                "region max size {} must >= split size {}",
                self.region_max_size.0,
                self.region_split_size.0
            ));
        }
        if self.region_max_keys < self.region_split_keys {
            return Err(box_err!(
                "region max keys {} must >= split keys {}",
                self.region_max_keys,
                self.region_split_keys
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::default();
        cfg.validate().unwrap();

        cfg = Config::default();
        cfg.region_max_size = ReadableSize(10);
        cfg.region_split_size = ReadableSize(20);
        assert!(cfg.validate().is_err());

        cfg = Config::default();
        cfg.region_max_keys = 10;
        cfg.region_split_keys = 20;
        assert!(cfg.validate().is_err());
    }
}
