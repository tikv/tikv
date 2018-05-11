// Copyright 2018 PingCAP, Inc.
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

use serde::{Deserialize, Deserializer};
use sys_info;

use util::config::ReadableSize;

const DEFAULT_STORE_CONCURRENCY: usize = 4;
const DEFAULT_COP_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
pub const DEFAULT_MAX_TASKS_PER_CORE: usize = 2 as usize * 1000;

const DEFAULT_STACK_SIZE_MB: u64 = 10;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        ConfigInvalid(desc: String) {
            description(desc)
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub high_concurrency: usize,
    pub normal_concurrency: usize,
    pub low_concurrency: usize,
    pub max_tasks_high: usize,
    pub max_tasks_normal: usize,
    pub max_tasks_low: usize,
    pub stack_size: ReadableSize,
}

impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if self.high_concurrency == 0 {
            return Err(Error::ConfigInvalid(String::from(
                "readpool.*.high-concurrency should not be 0.",
            )));
        }
        if self.normal_concurrency == 0 {
            return Err(Error::ConfigInvalid(String::from(
                "readpool.*.normal-concurrency should not be 0.",
            )));
        }
        if self.low_concurrency == 0 {
            return Err(Error::ConfigInvalid(String::from(
                "readpool.*.low-concurrency should not be 0.",
            )));
        }

        // 2MB is the default stack size for threads in rust, but endpoints may occur
        // very deep recursion, 2MB considered too small.
        //
        // See more: https://doc.rust-lang.org/std/thread/struct.Builder.html#method.stack_size
        if self.stack_size.0 < ReadableSize::mb(2).0 {
            return Err(Error::ConfigInvalid(String::from(
                "readpool.*.stack-size is too small.",
            )));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
/// A helper struct to handle deserialization default values.
pub struct SerdeConfigHelper {
    pub high_concurrency: Option<usize>,
    pub normal_concurrency: Option<usize>,
    pub low_concurrency: Option<usize>,
    pub max_tasks_high: Option<usize>,
    pub max_tasks_normal: Option<usize>,
    pub max_tasks_low: Option<usize>,
    pub stack_size: Option<ReadableSize>,
}

impl SerdeConfigHelper {
    /// Convert into `Config` with default values in missing fields.
    /// Default concurrency can be specified.
    #[allow(or_fun_call)]
    pub fn to_config(&self, default_concurrency: usize) -> Config {
        let high_concurrency = self.high_concurrency.unwrap_or(default_concurrency);
        let normal_concurrency = self.normal_concurrency.unwrap_or(default_concurrency);
        let low_concurrency = self.low_concurrency.unwrap_or(default_concurrency);
        Config {
            high_concurrency,
            normal_concurrency,
            low_concurrency,
            max_tasks_high: self.max_tasks_high
                .unwrap_or(high_concurrency * DEFAULT_MAX_TASKS_PER_CORE),
            max_tasks_normal: self.max_tasks_normal
                .unwrap_or(normal_concurrency * DEFAULT_MAX_TASKS_PER_CORE),
            max_tasks_low: self.max_tasks_low
                .unwrap_or(low_concurrency * DEFAULT_MAX_TASKS_PER_CORE),
            stack_size: self.stack_size
                .unwrap_or(ReadableSize::mb(DEFAULT_STACK_SIZE_MB)),
        }
    }

    pub fn to_storage_config(&self) -> Config {
        self.to_config(DEFAULT_STORE_CONCURRENCY)
    }

    pub fn deserialize_storage<'de, D>(deserializer: D) -> Result<Config, D::Error>
    where
        D: Deserializer<'de>,
    {
        let cfg = SerdeConfigHelper::deserialize(deserializer)?;
        Ok(cfg.to_storage_config())
    }

    pub fn to_coprocessor_config(&self) -> Config {
        let cpu_num = sys_info::cpu_num().unwrap();
        let concurrency = if cpu_num > 8 {
            (f64::from(cpu_num) * 0.8) as usize
        } else {
            DEFAULT_COP_CONCURRENCY
        };
        self.to_config(concurrency)
    }

    pub fn deserialize_coprocessor<'de, D>(deserializer: D) -> Result<Config, D::Error>
    where
        D: Deserializer<'de>,
    {
        let cfg = SerdeConfigHelper::deserialize(deserializer)?;
        Ok(cfg.to_coprocessor_config())
    }

    pub fn with_concurrency(self, concurrency: usize) -> Self {
        Self {
            high_concurrency: Some(concurrency),
            normal_concurrency: Some(concurrency),
            low_concurrency: Some(concurrency),
            ..self
        }
    }

    /// Tests are run in parallel so that we need a lower concurrency
    /// to prevent resource exhausting.
    pub fn default_for_test() -> SerdeConfigHelper {
        SerdeConfigHelper::default().with_concurrency(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_base() {
        let cfg = SerdeConfigHelper {
            high_concurrency: Some(5),
            max_tasks_low: Some(100),
            ..SerdeConfigHelper::default()
        };
        let base = cfg.to_config(2);
        assert_eq!(base.high_concurrency, 5);
        assert_eq!(base.normal_concurrency, 2);
        assert_eq!(base.low_concurrency, 2);
        assert_eq!(base.max_tasks_high, 5 * DEFAULT_MAX_TASKS_PER_CORE);
        assert_eq!(base.max_tasks_normal, 2 * DEFAULT_MAX_TASKS_PER_CORE);
        assert_eq!(base.max_tasks_low, 100);
    }

    #[test]
    fn test_validate() {
        let cfg = SerdeConfigHelper::default().to_storage_config();
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
    }
}
