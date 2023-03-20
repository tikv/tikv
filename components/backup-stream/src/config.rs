// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv::config::BackupStreamConfig;
use tikv_util::{info, worker::Scheduler};

use crate::endpoint::Task;

#[derive(Clone)]
pub struct BackupStreamConfigManager {
    pub scheduler: Scheduler<Task>,
    pub config: Arc<RwLock<BackupStreamConfig>>,
}

impl BackupStreamConfigManager {
    pub fn new(scheduler: Scheduler<Task>, cfg: BackupStreamConfig) -> Self {
        let config = Arc::new(RwLock::new(cfg));
        Self { scheduler, config }
    }
}

impl ConfigManager for BackupStreamConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        info!(
            "log backup config changed";
            "change" => ?change,
        );
        let mut cfg = self.config.as_ref().write().unwrap();
        cfg.update(change)?;
        cfg.validate()?;

        self.scheduler.schedule(Task::ChangeConfig(cfg.clone()))?;
        Ok(())
    }
}
