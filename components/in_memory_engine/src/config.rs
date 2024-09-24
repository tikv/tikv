use std::sync::Arc;

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{config::VersionTrack, info};

use crate::RegionCacheEngineConfig;

#[derive(Clone)]
pub struct RegionCacheConfigManager(pub Arc<VersionTrack<RegionCacheEngineConfig>>);

impl RegionCacheConfigManager {
    pub fn new(config: Arc<VersionTrack<RegionCacheEngineConfig>>) -> Self {
        Self(config)
    }
}

impl ConfigManager for RegionCacheConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0
                .update(move |cfg: &mut RegionCacheEngineConfig| cfg.update(change))?;
        }
        info!(
            "ime range cache config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for RegionCacheConfigManager {
    type Target = Arc<VersionTrack<RegionCacheEngineConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
