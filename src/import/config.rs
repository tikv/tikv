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

use std::error::Error;
use std::result::Result;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub import_dir: String,
    pub num_threads: usize,
    pub stream_channel_window: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            import_dir: "/tmp/tikv/import".to_owned(),
            num_threads: 8,
            stream_channel_window: 128,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        Ok(())
    }
}
