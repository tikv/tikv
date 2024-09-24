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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use online_config::{ConfigManager, ConfigValue};
    use tikv_util::config::{ReadableSize, VersionTrack};

    use super::RangeCacheConfigManager;
    use crate::RangeCacheEngineConfig;

    #[test]
    fn test_invalid_config_change() {
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.stop_load_limit_threshold = Some(ReadableSize::gb(1));
        config.soft_limit_threshold = Some(ReadableSize::gb(2));
        config.hard_limit_threshold = Some(ReadableSize::gb(3));
        let config = Arc::new(VersionTrack::new(config));
        let mut mgr = RangeCacheConfigManager(config);

        let mut config_change = HashMap::default();
        config_change.insert(
            "stop_load_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(2200).0),
        );
        mgr.dispatch(config_change).unwrap_err();

        let mut config_change = HashMap::default();
        config_change.insert(
            "stop_load_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(1500).0),
        );
        mgr.dispatch(config_change).unwrap();

        let mut config_change = HashMap::default();
        config_change.insert(
            "soft_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(3200).0),
        );
        mgr.dispatch(config_change).unwrap_err();

        let mut config_change = HashMap::default();
        config_change.insert(
            "soft_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(2500).0),
        );
        mgr.dispatch(config_change).unwrap();

        let mut config_change = HashMap::default();
        config_change.insert(
            "hard_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(2400).0),
        );
        mgr.dispatch(config_change).unwrap_err();

        let mut config_change = HashMap::default();
        config_change.insert(
            "soft_limit_threshold".to_string(),
            ConfigValue::Size(ReadableSize::mb(2600).0),
        );
        mgr.dispatch(config_change).unwrap();
    }
}
