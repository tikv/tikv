// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: Option<usize>,
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
}

impl Config {
    pub fn max_batch_size(&self) -> usize {
        if let Some(size) = self.max_batch_size {
            size
        } else {
            // `Config::validate` is not called for test so the `max_batch_size` is None.
            256
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: None,
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
        }
    }
}
