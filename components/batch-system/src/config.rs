// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use online_config::OnlineConfig;
use serde::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: Option<usize>,
    pub pool_size: usize,
    #[online_config(skip)]
    pub reschedule_duration: ReadableDuration,
    #[online_config(skip)]
    pub low_priority_pool_size: usize,
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
        }
    }
}
