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

use sys_info;

use util::config::ReadableSize;

const DEFAULT_CONCURRENCY: usize = 4;
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
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

impl Default for Config {
    fn default() -> Config {
        Config {
            high_concurrency: DEFAULT_CONCURRENCY,
            normal_concurrency: DEFAULT_CONCURRENCY,
            low_concurrency: DEFAULT_CONCURRENCY,
            max_tasks_high: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_CONCURRENCY,
            max_tasks_normal: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_CONCURRENCY,
            max_tasks_low: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_CONCURRENCY,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }
}

impl Config {
    pub fn default_for_coprocessor() -> Config {
        let cpu_num = sys_info::cpu_num().unwrap();
        let concurrency = if cpu_num > 8 {
            (f64::from(cpu_num) * 0.8) as usize
        } else {
            DEFAULT_COP_CONCURRENCY
        };
        Config {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_high: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            max_tasks_normal: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            max_tasks_low: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }

    pub fn default_for_storage() -> Config {
        Config::default()
    }

    pub fn default_with_concurrency(concurrency: usize) -> Config {
        Config {
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_high: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            max_tasks_normal: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            max_tasks_low: DEFAULT_MAX_TASKS_PER_CORE * concurrency,
            ..Config::default()
        }
    }

    /// Tests are run in parallel so that we need a lower concurrency
    /// to prevent resource exhausting.
    pub fn default_for_test() -> Config {
        Config::default_with_concurrency(2)
    }

    pub fn validate(&mut self) -> Result<(), Error> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();

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
