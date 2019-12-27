// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::deadlock::Scheduler as DeadlockScheduler;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use crate::config::ConfigManager;

use configuration::{ConfigChange, Configuration};

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[config(skip)]
    pub enabled: bool,
    pub wait_for_lock_timeout: u64,
    pub wake_up_delay_duration: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            wait_for_lock_timeout: 3000,
            wake_up_delay_duration: 100,
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

pub struct LockManagerConfigManager {
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: DeadlockScheduler,
}

impl LockManagerConfigManager {
    pub fn new(
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: DeadlockScheduler,
    ) -> Self {
        LockManagerConfigManager {
            waiter_mgr_scheduler,
            detector_scheduler,
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
                self.detector_scheduler.change_ttl(timeout.unwrap());
            }
            (None, delay @ Some(_)) => self.waiter_mgr_scheduler.change_config(None, delay),
            (None, None) => {}
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    use super::super::LockManager;
    use super::*;
    use crate::config::*;
    use crate::server::resolve;
    use pd_client::PdClient;
    use tikv_util::security::SecurityManager;

    #[test]
    fn test_config_validate() {
        let cfg = Config::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = Config::default();
        invalid_cfg.wait_for_lock_timeout = 0;
        assert!(invalid_cfg.validate().is_err());
    }

    struct MockPdClient;
    impl PdClient for MockPdClient {}

    fn setup(cfg: TiKvConfig) -> (ConfigController, WaiterMgrScheduler, DeadlockScheduler) {
        let mut lock_mgr = LockManager::new();
        let pd_client = Arc::new(MockPdClient);
        let (_, resolver) = resolve::new_resolver(Arc::clone(&pd_client)).unwrap();
        let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
        lock_mgr
            .start(1, pd_client, resolver, security_mgr, &cfg.pessimistic_txn)
            .unwrap();

        let mgr = lock_mgr.config_manager();
        let (w, d) = (
            mgr.waiter_mgr_scheduler.clone(),
            mgr.detector_scheduler.clone(),
        );
        let mut cfg_controller = ConfigController::new(cfg);
        cfg_controller.register("pessimistic_txn", Box::new(mgr));

        (cfg_controller, w, d)
    }

    fn validate_waiter<F>(router: &WaiterMgrScheduler, f: F)
    where
        F: FnOnce(u64, u64) + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        router.validate(Box::new(move |v1, v2| {
            f(v1, v2);
            tx.send(()).unwrap();
        }));
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    fn validate_dead_lock<F>(router: &DeadlockScheduler, f: F)
    where
        F: FnOnce(u64) + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        router.validate(Box::new(move |v| {
            f(v);
            tx.send(()).unwrap();
        }));
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    #[test]
    fn test_lock_manager_cfg_update() {
        const DEFAULT_TIMEOUT: u64 = 3000;
        const DEFAULT_DELAY: u64 = 100;
        let mut cfg = TiKvConfig::default();
        cfg.pessimistic_txn.wait_for_lock_timeout = DEFAULT_TIMEOUT;
        cfg.pessimistic_txn.wake_up_delay_duration = DEFAULT_DELAY;
        cfg.validate().unwrap();
        let (mut cfg_controller, waiter, deadlock) = setup(cfg.clone());

        // update of other module's config should not effect lock manager config
        let mut incoming = cfg.clone();
        incoming.raft_store.raft_log_gc_threshold = 2000;
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert_eq!(rollback.right(), Some(true));
        validate_waiter(&waiter, move |timeout: u64, delay: u64| {
            assert_eq!(timeout, DEFAULT_TIMEOUT);
            assert_eq!(delay, DEFAULT_DELAY);
        });
        validate_dead_lock(&deadlock, move |ttl: u64| {
            assert_eq!(ttl, DEFAULT_TIMEOUT);
        });

        // only update wake_up_delay_duration
        let mut incoming = cfg.clone();
        incoming.pessimistic_txn.wake_up_delay_duration = 500;
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert_eq!(rollback.right(), Some(true));
        validate_waiter(&waiter, move |timeout: u64, delay: u64| {
            assert_eq!(timeout, DEFAULT_TIMEOUT);
            assert_eq!(delay, 500);
        });
        validate_dead_lock(&deadlock, move |ttl: u64| {
            // dead lock ttl should not change
            assert_eq!(ttl, DEFAULT_TIMEOUT);
        });

        // only update wait_for_lock_timeout
        let mut incoming = cfg.clone();
        incoming.pessimistic_txn.wait_for_lock_timeout = 4000;
        // keep wake_up_delay_duration the same as last update
        incoming.pessimistic_txn.wake_up_delay_duration = 500;
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert_eq!(rollback.right(), Some(true));
        validate_waiter(&waiter, move |timeout: u64, delay: u64| {
            assert_eq!(timeout, 4000);
            // wake_up_delay_duration should be the same as last update
            assert_eq!(delay, 500);
        });
        validate_dead_lock(&deadlock, move |ttl: u64| {
            assert_eq!(ttl, 4000);
        });

        // update both config
        let mut incoming = cfg;
        incoming.pessimistic_txn.wait_for_lock_timeout = 4321;
        incoming.pessimistic_txn.wake_up_delay_duration = 123;
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert_eq!(rollback.right(), Some(true));
        validate_waiter(&waiter, move |timeout: u64, delay: u64| {
            assert_eq!(timeout, 4321);
            assert_eq!(delay, 123);
        });
        validate_dead_lock(&deadlock, move |ttl: u64| {
            assert_eq!(ttl, 4321);
        });
    }
}
