// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::util::config::ReadableDuration;
use std::error::Error;

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
    /// Default is 300ms. Setting this to 0 disables retry.
    pub retry_interval: ReadableDuration,
    /// The maximum number of times to retry a PD connection initialization.
    ///
    /// Default is 10.
    pub retry_max_count: Option<usize>,
    /// If the client observes the same error message on retry, it can repeat the message only
    /// every `n` times.
    ///
    /// Default is 10. Set to 1 to disable this feature.
    pub retry_log_every: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoints: Default::default(),
            retry_interval: ReadableDuration::millis(300),
            retry_max_count: Some(10),
            retry_log_every: 10,
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

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.endpoints.is_empty() {
            return Err("please specify pd.endpoints.".into());
        }

        if self.retry_log_every == 0 {
            return Err("pd.retry_log_every cannot be 0".into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pd_cfg() {
        let mut cfg = Config::default();
        // endpoints is required.
        cfg.validate().unwrap_err();
        cfg.endpoints = vec!["127.0.0.1:2333".to_owned()];
        cfg.validate().unwrap();
    }
}
