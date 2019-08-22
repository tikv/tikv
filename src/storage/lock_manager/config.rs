// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub wait_for_lock_timeout: u64,
    pub wake_up_delay_duration: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            wait_for_lock_timeout: 3000,
            wake_up_delay_duration: 1,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.wait_for_lock_timeout == 0 {
            return Err("pessimistic-txn.wait-for-lock-timeout can not be 0".into());
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
    }
}
