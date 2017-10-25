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

use util::config::ReadableSize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a region with table prefix if
    /// that region crosses tables.
    pub split_region_on_table: bool,
    pub region_max_size: ReadableSize,
    pub region_split_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(96);
        Config {
            split_region_on_table: false,
            region_max_size: split_size / 2 * 3,
            region_split_size: split_size,
        }
    }
}
