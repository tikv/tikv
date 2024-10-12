use std::sync::Arc;

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{config::VersionTrack, info};

use crate::InMemoryEngineConfig;

#[derive(Clone)]
pub struct InMemoryEngineConfigManager(pub Arc<VersionTrack<InMemoryEngineConfig>>);

impl InMemoryEngineConfigManager {
    pub fn new(config: Arc<VersionTrack<InMemoryEngineConfig>>) -> Self {
        Self(config)
    }
}

impl ConfigManager for InMemoryEngineConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0
                .update(move |cfg: &mut InMemoryEngineConfig| cfg.update(change))?;
        }
        info!("ime config changed"; "change" => ?change);
        Ok(())
    }
}

impl std::ops::Deref for InMemoryEngineConfigManager {
    type Target = Arc<VersionTrack<InMemoryEngineConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
