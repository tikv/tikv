// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use online_config::{ConfigChange, ConfigManager};
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

pub struct BackupStreamConfigManager(pub Scheduler<Task>);

impl ConfigManager for BackupStreamConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.0.schedule(Task::ChangeConfig(change))?;
        Ok(())
    }
}

impl std::ops::Deref for BackupStreamConfigManager {
    type Target = Scheduler<Task>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
