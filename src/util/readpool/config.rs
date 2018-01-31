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

use util::config::ReadableSize;

const DEFAULT_HIGH_CONCURRENCY: usize = 8;
const DEFAULT_NORMAL_CONCURRENCY: usize = 8;
const DEFAULT_LOW_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
pub const DEFAULT_MAX_TASKS_PER_CORE: usize = 2 as usize * 1000;

const DEFAULT_STACK_SIZE_MB: u64 = 10;

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
            high_concurrency: DEFAULT_HIGH_CONCURRENCY,
            normal_concurrency: DEFAULT_NORMAL_CONCURRENCY,
            low_concurrency: DEFAULT_LOW_CONCURRENCY,
            max_tasks_high: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_HIGH_CONCURRENCY,
            max_tasks_normal: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_NORMAL_CONCURRENCY,
            max_tasks_low: DEFAULT_MAX_TASKS_PER_CORE * DEFAULT_LOW_CONCURRENCY,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }
}

impl Config {
    /// Tests are run in parallel so that we need a lower concurrency
    /// to prevent resource exhausting.
    pub fn default_for_test() -> Config {
        Config {
            high_concurrency: 2,
            normal_concurrency: 2,
            low_concurrency: 2,
            max_tasks_high: DEFAULT_MAX_TASKS_PER_CORE * 2,
            max_tasks_normal: DEFAULT_MAX_TASKS_PER_CORE * 2,
            max_tasks_low: DEFAULT_MAX_TASKS_PER_CORE * 2,
            ..Config::default()
        }
    }
}
