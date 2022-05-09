// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

use online_config::{ConfigChange, OnlineConfig};
use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

use crate::{
    recorder::ConfigChangeNotifier as RecorderConfigChangeNotifier,
    reporter::ConfigChangeNotifier as ReporterConfigChangeNotifier, AddressChangeNotifier,
};

const MIN_PRECISION: ReadableDuration = ReadableDuration::millis(100);
const MAX_PRECISION: ReadableDuration = ReadableDuration::hours(1);
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_RECEIVER_INTERVAL: ReadableDuration = ReadableDuration::millis(500);

/// Public configuration of resource metering module.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
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
            receiver_address: "".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 100,
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

    recorder_notifier: RecorderConfigChangeNotifier,
    reporter_notifier: ReporterConfigChangeNotifier,
    address_notifier: AddressChangeNotifier,
}

impl ConfigManager {
    pub fn new(
        current_config: Config,
        recorder_notifier: RecorderConfigChangeNotifier,
        reporter_notifier: ReporterConfigChangeNotifier,
        address_notifier: AddressChangeNotifier,
    ) -> Self {
        ConfigManager {
            current_config,
            recorder_notifier,
            reporter_notifier,
            address_notifier,
        }
    }
}

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let mut new_config = self.current_config.clone();
        new_config.update(change);
        new_config.validate()?;
        if self.current_config.receiver_address != new_config.receiver_address {
            self.address_notifier
                .notify(new_config.receiver_address.clone());
        }
        // Notify reporter that the configuration has changed.
        self.recorder_notifier.notify(new_config.clone());
        self.reporter_notifier.notify(new_config.clone());
        self.current_config = new_config;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tikv_util::config::ReadableDuration;

    use super::*;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        assert!(cfg.validate().is_ok()); // Empty address is allowed.
        let cfg = Config {
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_ok());
        let cfg = Config {
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::days(999), // invalid
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_err());
        let cfg = Config {
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: usize::MAX, // invalid
            precision: ReadableDuration::secs(1),
        };
        assert!(cfg.validate().is_err());
        let cfg = Config {
            receiver_address: "127.0.0.1:6666".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::days(999), // invalid
        };
        assert!(cfg.validate().is_err());
    }
}
