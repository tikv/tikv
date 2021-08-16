// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use online_config::{ConfigChange, ConfigManager};
use tikv_util::worker::Scheduler;

use crate::Task;

pub struct CdcConfigManager(pub Scheduler<Task>);

impl ConfigManager for CdcConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.0.schedule(Task::ChangeConfig(change))?;
        Ok(())
    }
}

impl std::ops::Deref for CdcConfigManager {
    type Target = Scheduler<Task>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
