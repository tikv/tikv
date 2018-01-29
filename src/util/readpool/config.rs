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

const DEFAULT_STACK_SIZE_MB: u64 = 10;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub high_concurrency: usize,
    pub normal_concurrency: usize,
    pub low_concurrency: usize,
    pub stack_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            high_concurrency: DEFAULT_HIGH_CONCURRENCY,
            normal_concurrency: DEFAULT_NORMAL_CONCURRENCY,
            low_concurrency: DEFAULT_LOW_CONCURRENCY,
            stack_size: ReadableSize::mb(DEFAULT_STACK_SIZE_MB),
        }
    }
}
