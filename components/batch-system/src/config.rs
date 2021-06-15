// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
    pub low_priority_pool_size: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
            low_priority_pool_size: 1,
        }
    }
}
