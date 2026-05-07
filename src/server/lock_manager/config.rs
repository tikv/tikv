// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use serde::de::{Deserialize, Deserializer, IntoDeserializer};
use tikv_util::config::{ReadableDuration, ReadableSize};

use super::{
    deadlock::Scheduler as DeadlockScheduler, waiter_manager::Scheduler as WaiterMgrScheduler,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(deserialize_with = "readable_duration_or_u64")]
    pub wait_for_lock_timeout: ReadableDuration,
    #[serde(deserialize_with = "readable_duration_or_u64")]
    pub wake_up_delay_duration: ReadableDuration,
    /// Whether to enable the pipelined pessimistic lock feature.
    pub pipelined: bool,
    /// Whether to enable the in-memory pessimistic lock feature.
    /// It will take effect only if the `pipelined` config is true because we
    /// assume that the success rate of pessimistic transactions is important
    /// to people who disable the pipelined pessimistic lock feature.
    pub in_memory: bool,
    /// The maximum size of the in-memory pessimistic locks of one region peer.
    pub in_memory_peer_size_limit: ReadableSize,
    /// The maximum size of the in-memory pessimistic locks in the TiKV
    /// instance.
    pub in_memory_instance_size_limit: ReadableSize,
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
            in_memory: true,
            in_memory_peer_size_limit: ReadableSize::kb(512),
            in_memory_instance_size_limit: ReadableSize::mb(100),
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
    pub in_memory: Arc<AtomicBool>,
    pub wake_up_delay_duration_ms: Arc<AtomicU64>,
    pub in_memory_peer_size_limit: Arc<AtomicU64>,
    pub in_memory_instance_size_limit: Arc<AtomicU64>,
}

impl LockManagerConfigManager {
    pub fn new(
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: DeadlockScheduler,
        pipelined: Arc<AtomicBool>,
        in_memory: Arc<AtomicBool>,
        wake_up_delay_duration_ms: Arc<AtomicU64>,
        in_memory_peer_size_limit: Arc<AtomicU64>,
        in_memory_instance_size_limit: Arc<AtomicU64>,
    ) -> Self {
        LockManagerConfigManager {
            waiter_mgr_scheduler,
            detector_scheduler,
            pipelined,
            in_memory,
            wake_up_delay_duration_ms,
            in_memory_peer_size_limit,
            in_memory_instance_size_limit,
        }
    }
}

impl ConfigManager for LockManagerConfigManager {
    fn dispatch(&mut self, mut change: ConfigChange) -> Result<(), Box<dyn Error>> {
        if let Some(p) = change.remove("wait_for_lock_timeout").map(Into::into) {
            self.waiter_mgr_scheduler.change_config(Some(p));
            self.detector_scheduler.change_ttl(p.into());
        }
        if let Some(p) = change
            .remove("wake_up_delay_duration")
            .map(ReadableDuration::from)
        {
            info!(
                "Waiter manager config changed";
                "wake_up_delay_duration" => %p,
            );
            self.wake_up_delay_duration_ms
                .store(p.as_millis(), Ordering::Relaxed);
        }
        if let Some(p) = change.remove("pipelined").map(Into::into) {
            self.pipelined.store(p, Ordering::Relaxed);
        }
        if let Some(p) = change.remove("in_memory").map(Into::into) {
            self.in_memory.store(p, Ordering::Relaxed);
        }
        if let Some(p) = change
            .remove("in_memory_peer_size_limit")
            .map(ReadableSize::from)
        {
            self.in_memory_peer_size_limit.store(p.0, Ordering::Relaxed);
        }
        if let Some(p) = change
            .remove("in_memory_instance_size_limit")
            .map(ReadableSize::from)
        {
            self.in_memory_instance_size_limit
                .store(p.0, Ordering::Relaxed);
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
        in-memory = false
        in-memory-peer-size-limit = "512KiB"
        in-memory-instance-size-limit = "100MiB"
        "#;

        let config: Config = toml::from_str(conf).unwrap();
        assert_eq!(config.wait_for_lock_timeout.as_millis(), 10);
        assert_eq!(config.wake_up_delay_duration.as_millis(), 100);
        assert_eq!(config.pipelined, false);
        assert_eq!(config.in_memory, false);
        assert_eq!(config.in_memory_peer_size_limit.0, 512 << 10);
        assert_eq!(config.in_memory_instance_size_limit.0, 100 << 20);
    }
}
