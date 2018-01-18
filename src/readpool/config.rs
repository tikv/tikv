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

const DEFAULT_READ_CRITICAL_CONCURRENCY: usize = 1;
const DEFAULT_READ_HIGH_CONCURRENCY: usize = 1;
const DEFAULT_READ_NORMAL_CONCURRENCY: usize = 1;
const DEFAULT_READ_LOW_CONCURRENCY: usize = 1;
const DEFAULT_MAX_READ_TASKS: usize = 20480;
const DEFAULT_STACK_SIZE_MB: u64 = 10;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub read_critical_concurrency: usize,
    pub read_high_concurrency: usize,
    pub read_normal_concurrency: usize,
    pub read_low_concurrency: usize,
    pub max_read_tasks: usize,
    pub stack_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            read_critical_concurrency: DEFAULT_READ_CRITICAL_CONCURRENCY,
            read_high_concurrency: DEFAULT_READ_HIGH_CONCURRENCY,
            read_normal_concurrency: DEFAULT_READ_NORMAL_CONCURRENCY,
            read_low_concurrency: DEFAULT_READ_LOW_CONCURRENCY,
            max_read_tasks: DEFAULT_MAX_READ_TASKS,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }
}
