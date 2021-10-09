// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::recorder::RecorderHandle;
use crate::reporter::Task;

use std::error::Error;

use online_config::{ConfigChange, OnlineConfig};
use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;
use tikv_util::worker::Scheduler;

const MIN_PRECISION: ReadableDuration = ReadableDuration::secs(1);
const MAX_PRECISION: ReadableDuration = ReadableDuration::hours(1);
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_RECEIVER_INTERVAL: ReadableDuration = ReadableDuration::secs(5);

/// Public configuration of resource metering module.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// Turn on resource metering.
    ///
    /// This configuration will affect all resource modules such as cpu/summary.
    pub enabled: bool,

    /// Data reporting destination address.
    pub receiver_address: String,

    /// Data reporting interval.
    pub report_receiver_interval: ReadableDuration,

    /// The maximum number of groups by [ResourceMeteringTag].
    ///
    /// [ResourceMeteringTag]: crate::ResourceMeteringTag
    pub max_resource_groups: usize,

    /// Sampling window. (only for cpu module)
    pub precision: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            enabled: false,
            receiver_address: "".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        }
    }
}

impl Config {
    /// Check whether the configuration is legal.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if !self.receiver_address.is_empty() {
            tikv_util::config::check_addr(&self.receiver_address)?;
        }
        if self.precision < MIN_PRECISION || self.precision > MAX_PRECISION {
            return Err(format!(
                "precision must between {} and {}",
                MIN_PRECISION, MAX_PRECISION
            )
            .into());
        }
        if self.max_resource_groups > MAX_MAX_RESOURCE_GROUPS {
            return Err(format!(
                "max resource groups must between {} and {}",
                0, MAX_MAX_RESOURCE_GROUPS
            )
            .into());
        }
        if self.report_receiver_interval < MIN_REPORT_RECEIVER_INTERVAL
            || self.report_receiver_interval > self.precision * 500
        {
            return Err(format!(
                "report interval seconds must between {} and {}",
                MIN_REPORT_RECEIVER_INTERVAL,
                self.precision * 500
            )
            .into());
        }
        Ok(())
    }
}

/// ConfigManager implements [online_config::ConfigManager], which is used
/// to control the dynamic update of the configuration.
pub struct ConfigManager {
    current_config: Config,
    scheduler: Scheduler<Task>,
    recorder: RecorderHandle,
}

impl ConfigManager {
    pub fn new(
        current_config: Config,
        scheduler: Scheduler<Task>,
        recorder: RecorderHandle,
    ) -> Self {
        ConfigManager {
            current_config,
            scheduler,
            recorder,
        }
    }
}

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let mut new_config = self.current_config.clone();
        new_config.update(change);
        new_config.validate()?;
        // Pause or resume the recorder thread.
        if self.current_config.enabled != new_config.enabled {
            if new_config.enabled {
                self.recorder.resume();
            } else {
                self.recorder.pause();
            }
        }
        if self.current_config.precision != new_config.precision {
            self.recorder.precision(new_config.precision.0);
        }
        // Notify reporter that the configuration has changed.
        self.scheduler
            .schedule(Task::ConfigChange(new_config.clone()))
            .ok();
        self.current_config = new_config;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_util::config::ReadableDuration;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        assert!(cfg.validate().is_ok()); // Empty address is allowed.
        let cfg = Config {
            enabled: false,
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_ok());
        let cfg = Config {
            enabled: false,
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::days(999), // invalid
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_err());
        let cfg = Config {
            enabled: false,
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: usize::MAX, // invalid
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_err());
        let cfg = Config {
            enabled: false,
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::days(999), // invalid
        };
        assert!(cfg.validate().is_err());
    }
}
