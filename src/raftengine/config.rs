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

use super::Result;
use crate::util::config::ReadableSize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub dir: String,
    pub recovery_mode: i32,
    pub bytes_per_sync: ReadableSize,
    pub target_file_size: ReadableSize,
    pub cache_size_limit: ReadableSize,
    pub total_size_limit: ReadableSize,

    // Use raftstore.cfg.raft_log_gc_threshold
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub compact_threshold: usize,

    // Use raftstore.cfg.region_split_size
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub region_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            recovery_mode: 0,
            bytes_per_sync: ReadableSize::kb(256),
            target_file_size: ReadableSize::mb(128),
            cache_size_limit: ReadableSize::gb(2),
            total_size_limit: ReadableSize::gb(20),
            compact_threshold: 0,
            region_size: ReadableSize::mb(0),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        if self.total_size_limit.0 <= self.target_file_size.0 {
            return Err(box_err!(
                "Total size limit {:?} less than target file size {:?}",
                self.total_size_limit,
                self.target_file_size
            ));
        }

        if self.cache_size_limit.0 < self.target_file_size.0 {
            return Err(box_err!(
                "Cache size limit {:?} less than target file size {:?}",
                self.cache_size_limit,
                self.target_file_size
            ));
        }

        if self.total_size_limit.0 < self.cache_size_limit.0 {
            return Err(box_err!(
                "Total size limit {:?} is less than cache limit size {:?}",
                self.total_size_limit,
                self.cache_size_limit
            ));
        }

        if self.recovery_mode < 0 || self.recovery_mode > 1 {
            return Err(box_err!(
                "Unknown recovery mode {} for raftengine",
                self.recovery_mode
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::config::ReadableSize;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());

        cfg.recovery_mode = 1;
        assert!(cfg.validate().is_ok());

        cfg.recovery_mode = 2;
        assert!(cfg.validate().is_err());

        cfg.recovery_mode = -1;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.target_file_size = ReadableSize::kb(20);
        cfg.total_size_limit = ReadableSize::kb(10);
        assert!(cfg.validate().is_err());

        cfg.cache_size_limit = ReadableSize::mb(10);
        cfg.total_size_limit = ReadableSize::mb(1);
        assert!(cfg.validate().is_err());

        cfg.cache_size_limit = ReadableSize::mb(1);
        cfg.total_size_limit = ReadableSize::mb(10);
        assert!(cfg.validate().is_ok());
    }
}
