// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: usize,
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: 1024,
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
        }
    }
}
