// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: Option<usize>,
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
    pub low_priority_pool_size: usize,
    #[doc(hidden)]
    #[serde(skip)]
    pub before_pause_wait: Option<Duration>,
}

impl Config {
    pub fn max_batch_size(&self) -> usize {
        // `Config::validate` is not called for test so the `max_batch_size` is None.
        self.max_batch_size.unwrap_or(256)
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: None,
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
            low_priority_pool_size: 1,
            before_pause_wait: None,
        }
    }
}
