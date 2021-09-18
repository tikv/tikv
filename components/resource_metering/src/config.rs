// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::recorder::RecorderHandle;
use crate::reporter::Task;
use online_config::{ConfigChange, OnlineConfig};
use serde_derive::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use tikv_util::config::ReadableDuration;
use tikv_util::worker::Scheduler;

const MIN_PRECISION: ReadableDuration = ReadableDuration::secs(1);
const MAX_PRECISION: ReadableDuration = ReadableDuration::hours(1);
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_AGENT_INTERVAL: ReadableDuration = ReadableDuration::secs(5);

/// The reason for the existence of this switch is to avoid the continuous
/// accumulation of summary data in the local logic of all threads.
///
/// Note all possible changes to `Config.enable`, need to modify `GLOBAL_ENABLE` together.
pub static GLOBAL_ENABLE: AtomicBool = AtomicBool::new(false);

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
    pub agent_address: String,

    /// Data reporting interval.
    pub report_agent_interval: ReadableDuration,

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
            agent_address: "".to_string(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        }
    }
}

impl Config {
    /// Check whether the configuration is legal.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if !self.agent_address.is_empty() {
            tikv_util::config::check_addr(&self.agent_address)?;
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
        if self.report_agent_interval < MIN_REPORT_AGENT_INTERVAL
            || self.report_agent_interval > self.precision * 500
        {
            return Err(format!(
                "report interval seconds must between {} and {}",
                MIN_REPORT_AGENT_INTERVAL,
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
        GLOBAL_ENABLE.store(new_config.enabled, Relaxed);
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
    use crate::RecorderHandle;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;
    use tikv_util::config::ReadableDuration;
    use tikv_util::worker::{LazyWorker, Runnable};

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        assert!(matches!(cfg.validate(), Ok(_))); // Empty address is allowed.
        let cfg = Config {
            enabled: false,
            agent_address: "127.0.0.1:6666".to_string(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(matches!(cfg.validate(), Ok(_)));
        let cfg = Config {
            enabled: false,
            agent_address: "127.0.0.1:6666".to_string(),
            report_agent_interval: ReadableDuration::days(999), // invalid
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        };
        assert!(matches!(cfg.validate(), Err(_)));
        let cfg = Config {
            enabled: false,
            agent_address: "127.0.0.1:6666".to_string(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: usize::MAX, // invalid
            precision: ReadableDuration::secs(1),
        };
        assert!(matches!(cfg.validate(), Err(_)));
        let cfg = Config {
            enabled: false,
            agent_address: "127.0.0.1:6666".to_string(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::days(999), // invalid
        };
        assert!(matches!(cfg.validate(), Err(_)));
    }

    struct MockRunner;

    impl Runnable for MockRunner {
        type Task = Task;

        fn run(&mut self, task: Self::Task) {
            assert!(matches!(task, Task::ConfigChange(_)));
            match task {
                Task::ConfigChange(cfg) => assert!(cfg.enabled),
                _ => {}
            }
        }
    }

    #[test]
    fn test_config_manager_dispatch() {
        let join_handle = std::thread::spawn(|| {});
        let pause = Arc::new(AtomicBool::new(true));
        let precision_ms = Arc::new(AtomicU64::new(0));
        let handle = RecorderHandle::new(join_handle, pause.clone(), precision_ms.clone());
        let mut worker = LazyWorker::new("test-worker");
        worker.start(MockRunner);
        let mut cm = ConfigManager::new(Config::default(), worker.scheduler(), handle);
        let mut change = HashMap::new();
        change.insert("enabled".to_owned(), true.into());
        change.insert("precision".to_owned(), ReadableDuration::secs(2).into());
        assert!(pause.load(SeqCst));
        assert_eq!(precision_ms.load(SeqCst), 0);
        online_config::ConfigManager::dispatch(&mut cm, change).expect("dispatch failed");
        assert!(!pause.load(SeqCst));
        assert_eq!(precision_ms.load(SeqCst), 2000);
        worker.stop();
    }
}
