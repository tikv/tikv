// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The renew interval of BatchTsoProvider.
    ///
    /// Default is 100ms, to adjust batch size rapidly enough.
    pub renew_interval: ReadableDuration,
    /// The minimal renew batch size of BatchTsoProvider.
    ///
    /// Default is 100.
    /// One TSO is required for every batch of Raft put messages, so by default 1K tso/s should be enough.
    /// Benchmark showed that with a 8.6w raw_put per second, the TSO requirement is 600 per second.
    pub renew_batch_min_size: u32,
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.renew_interval.is_zero() {
            return Err("causal-ts.renew_interval can't be zero".into());
        }
        if self.renew_batch_min_size == 0 {
            return Err("causal-ts.renew_batch_init_size should be greater than 0".into());
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            renew_interval: ReadableDuration::millis(crate::tso::TSO_BATCH_RENEW_INTERVAL_DEFAULT),
            renew_batch_min_size: crate::tso::TSO_BATCH_MIN_SIZE_DEFAULT,
        }
    }
}
