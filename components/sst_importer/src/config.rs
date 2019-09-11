// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::result::Result;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub num_threads: usize,
    pub stream_channel_window: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_threads: 8,
            stream_channel_window: 128,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        Ok(())
    }
}
