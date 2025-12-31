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
    /// One TSO is required for every batch of Raft put messages, so by default
    /// 1K tso/s should be enough. Benchmark showed that with a 8.6w raw_put
    /// per second, the TSO requirement is 600 per second.
    pub renew_batch_min_size: u32,
    /// The maximum renew batch size of BatchTsoProvider.
    ///
    /// Default is 8192.
    /// PD provides 262144 TSO per 50ms for the whole cluster. Exceed this space
    /// will cause PD to sleep for 50ms, waiting for physical update
    /// interval. The 50ms limitation can not be broken through now (see
    /// `tso-update-physical-interval`).
    pub renew_batch_max_size: u32,
    /// The size (in duration) of TSO buffer allocated ahead for
    /// BatchTsoProvider.
    ///
    /// Default is 3s.
    /// The longer of the value will help to improve tolerance against PD
    /// failure, but more overhead of `TsoBatchList` & pressure to TSO
    /// service.
    pub alloc_ahead_buffer: ReadableDuration,
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.renew_interval.is_zero() {
            return Err("causal-ts.renew-interval can't be zero".into());
        }
        if self.renew_batch_min_size == 0 {
            return Err("causal-ts.renew-batch-min-size should be greater than 0".into());
        }
        if self.renew_batch_max_size == 0 {
            return Err("causal-ts.renew-batch-max-size should be greater than 0".into());
        }
        if self.alloc_ahead_buffer.is_zero() {
            return Err("causal-ts.alloc-ahead-buffer can't be zero".into());
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            renew_interval: ReadableDuration::millis(
                crate::tso::DEFAULT_TSO_BATCH_RENEW_INTERVAL_MS,
            ),
            renew_batch_min_size: crate::tso::DEFAULT_TSO_BATCH_MIN_SIZE,
            renew_batch_max_size: crate::tso::DEFAULT_TSO_BATCH_MAX_SIZE,
            alloc_ahead_buffer: ReadableDuration::millis(
                crate::tso::DEFAULT_TSO_BATCH_ALLOC_AHEAD_BUFFER_MS,
            ),
        }
    }
}
