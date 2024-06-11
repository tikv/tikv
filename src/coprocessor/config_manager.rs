// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Coprocessor online config manager.

use std::sync::Arc;

use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use tikv_util::{config::ReadableSize, memory::MemoryQuota};

pub(super) struct CopConfigManager {
    memory_quota: Arc<MemoryQuota>,
}

impl CopConfigManager {
    pub fn new(memory_quota: Arc<MemoryQuota>) -> Self {
        Self { memory_quota }
    }
}

impl ConfigManager for CopConfigManager {
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(quota) = change.remove("end_point_memory_quota") {
            if quota != ConfigValue::None {
                let cap: ReadableSize = quota.into();
                self.memory_quota.set_capacity(cap.0 as _);
            }
        }
        Ok(())
    }
}
