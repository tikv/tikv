// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error, result::Result};

use tikv_util::config::ReadableDuration;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub num_threads: usize,
    pub stream_channel_window: usize,
    /// The timeout for going back into normal mode from import mode.
    ///
    /// Default is 10m.
    pub import_mode_timeout: ReadableDuration,
    /// the ratio of system memory used for import.
    pub memory_use_ratio: f64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            num_threads: 8,
            stream_channel_window: 128,
            import_mode_timeout: ReadableDuration::minutes(10),
            memory_use_ratio: 0.3,
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        let default_cfg = Config::default();
        if self.num_threads == 0 {
            warn!(
                "import.num_threads can not be 0, change it to {}",
                default_cfg.num_threads
            );
            self.num_threads = default_cfg.num_threads;
        }
        if self.stream_channel_window == 0 {
            warn!(
                "import.stream_channel_window can not be 0, change it to {}",
                default_cfg.stream_channel_window
            );
            self.stream_channel_window = default_cfg.stream_channel_window;
        }
        if self.memory_use_ratio > 0.5 || self.memory_use_ratio < 0.0 {
            warn!(
                "import.mem_ratio should belong to [0.0, 0.5], change it to {}",
                default_cfg.memory_use_ratio,
            );
            self.memory_use_ratio = default_cfg.memory_use_ratio;
        }
        Ok(())
    }
}
