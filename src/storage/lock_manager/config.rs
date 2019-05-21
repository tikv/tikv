// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub wait_for_lock_timeout: u64,
    pub wake_up_dealy_duration: u64,
    pub monitor_membership_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            wait_for_lock_timeout: 1000,
            wake_up_dealy_duration: 1,
            monitor_membership_interval: 3000,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.wait_for_lock_timeout == 0 {
            return Err("pessimistic-txn.wait-for-lock-timeout can not be 0".into());
        }
        if self.monitor_membership_interval < 1000 {
            return Err("pessimistic-txn.monitor-membership-interval is too small".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = Config::default();
        invalid_cfg.wait_for_lock_timeout = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = Config::default();
        invalid_cfg.monitor_membership_interval = 500;
        assert!(invalid_cfg.validate().is_err());
    }
}
