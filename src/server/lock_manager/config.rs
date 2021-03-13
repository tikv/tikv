// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::deadlock::Scheduler as DeadlockScheduler;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use configuration::{ConfigChange, ConfigManager, Configuration};
use serde::de::{Deserialize, Deserializer, IntoDeserializer};

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tikv_util::config::ReadableDuration;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(deserialize_with = "readable_duration_or_u64")]
    pub wait_for_lock_timeout: ReadableDuration,
    #[serde(deserialize_with = "readable_duration_or_u64")]
    pub wake_up_delay_duration: ReadableDuration,
    pub pipelined: bool,
}

// u64 is for backward compatibility since v3.x uses it.
fn readable_duration_or_u64<'de, D>(deserializer: D) -> Result<ReadableDuration, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    use serde_json::Value;

    let v = Value::deserialize(deserializer)?;
    match v {
        Value::String(s) => ReadableDuration::deserialize(s.into_deserializer()),
        Value::Number(n) => n
            .as_u64()
            .map(ReadableDuration::millis)
            .ok_or_else(|| serde::de::Error::custom(format!("expect unsigned integer: {}", n))),
        other => Err(serde::de::Error::custom(format!(
            "expect ReadableDuration or unsigned integer: {}",
            other
        ))),
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wait_for_lock_timeout: ReadableDuration::millis(1000),
            wake_up_delay_duration: ReadableDuration::millis(20),
            pipelined: true,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.wait_for_lock_timeout.as_millis() == 0 {
            return Err("pessimistic-txn.wait-for-lock-timeout can not be 0".into());
        }
        Ok(())
    }
}

pub struct LockManagerConfigManager {
    pub waiter_mgr_scheduler: WaiterMgrScheduler,
    pub detector_scheduler: DeadlockScheduler,
    pub pipelined: Arc<AtomicBool>,
}

impl LockManagerConfigManager {
    pub fn new(
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: DeadlockScheduler,
        pipelined: Arc<AtomicBool>,
    ) -> Self {
        LockManagerConfigManager {
            waiter_mgr_scheduler,
            detector_scheduler,
            pipelined,
        }
    }
}

impl ConfigManager for LockManagerConfigManager {
    fn dispatch(&mut self, mut change: ConfigChange) -> Result<(), Box<dyn Error>> {
        match (
            change.remove("wait_for_lock_timeout").map(Into::into),
            change.remove("wake_up_delay_duration").map(Into::into),
        ) {
            (timeout @ Some(_), delay) => {
                self.waiter_mgr_scheduler.change_config(timeout, delay);
                self.detector_scheduler.change_ttl(timeout.unwrap().into());
            }
            (None, delay @ Some(_)) => self.waiter_mgr_scheduler.change_config(None, delay),
            (None, None) => {}
        };
        if let Some(p) = change.remove("pipelined").map(Into::into) {
            self.pipelined.store(p, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn test_config_deserialize() {
        let conf = r#"
        enabled = false
        wait-for-lock-timeout = "10ms"
        wake-up-delay-duration = 100
        pipelined = false
        "#;

        let config: Config = toml::from_str(conf).unwrap();
        assert_eq!(config.wait_for_lock_timeout.as_millis(), 10);
        assert_eq!(config.wake_up_delay_duration.as_millis(), 100);
        assert_eq!(config.pipelined, false);
    }
}
