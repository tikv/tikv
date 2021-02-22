// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use serde_derive::{Deserialize, Serialize};
use std::error::Error;
use tikv_util::config::ReadableDuration;
/// The configuration for a PD Client.
///
/// By default during initialization the client will attempt to reconnect every 300s
/// for infinity, logging only every 10th duplicate error.
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The PD endpoints for the client.
    ///
    /// Default is empty.
    pub endpoints: Vec<String>,
    /// The interval at which to retry a PD connection initialization.
    ///
    /// Default is 300ms.
    pub retry_interval: ReadableDuration,
    /// The maximum number of times to retry a PD connection initialization.
    ///
    /// Default is isize::MAX, represented by -1.
    pub retry_max_count: isize,
    /// If the client observes the same error message on retry, it can repeat the message only
    /// every `n` times.
    ///
    /// Default is 10. Set to 1 to disable this feature.
    pub retry_log_every: usize,
    /// The interval at which to update PD information.
    ///
    /// Default is 10m.
    pub update_interval: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoints: vec!["127.0.0.1:2379".to_string()],
            retry_interval: ReadableDuration::millis(300),
            retry_max_count: std::isize::MAX,
            retry_log_every: 10,
            update_interval: ReadableDuration::minutes(10),
        }
    }
}

impl Config {
    pub fn new(endpoints: Vec<String>) -> Self {
        Config {
            endpoints,
            ..Default::default()
        }
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.endpoints.is_empty() {
            return Err("please specify pd.endpoints.".into());
        }

        if self.retry_log_every == 0 {
            return Err("pd.retry_log_every cannot be <=0".into());
        }

        if self.retry_max_count < -1 {
            return Err("pd.retry_max_count cannot be < -1".into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pd_cfg() {
        let cfg = Config::default();
        cfg.validate().unwrap();
    }
}
