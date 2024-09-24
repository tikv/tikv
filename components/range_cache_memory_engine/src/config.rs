use std::sync::Arc;

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{config::VersionTrack, info};

use crate::RangeCacheEngineConfig;

#[derive(Clone)]
pub struct RangeCacheConfigManager(pub Arc<VersionTrack<RangeCacheEngineConfig>>);

impl RangeCacheConfigManager {
    pub fn new(config: Arc<VersionTrack<RangeCacheEngineConfig>>) -> Self {
        Self(config)
    }
}

impl ConfigManager for RangeCacheConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0.update(move |cfg: &mut RangeCacheEngineConfig| {
                // Verify the config change is valid.
                let mut cfg_verify = cfg.clone();
                cfg_verify.update(change.clone())?;
                cfg_verify.validate()?;
                cfg.update(change)
            })?;
        }
        info!(
            "ime range cache config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for RangeCacheConfigManager {
    type Target = Arc<VersionTrack<RangeCacheEngineConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
