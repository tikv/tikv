// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, ReadableSize, VersionTrack},
    info, warn,
};

// The minimum hard limit threshold, 1GB.
const MIN_HARD_LIMIT_THRESHOLD: u64 = 1024 * 1024 * 1024;
// The default ratio of soft limit threshold to hard limit threshold.
const SOFT_LIMIT_THRESHOLD_RATIO: f64 = 0.9;
// The default ratio of stop limit threshold to hard limit threshold.
const STOP_LIMIT_THRESHOLD_RATIO: f64 = 0.8;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, OnlineConfig)]
#[serde(default, rename_all = "kebab-case")]
pub struct RangeCacheEngineConfig {
    pub enabled: bool,
    pub gc_interval: ReadableDuration,
    pub load_evict_interval: ReadableDuration,
    // TODO(SpadeA): ultimately we only expose one memory limit to user.
    // When memory usage reaches this amount, no further load will be performed.
    pub stop_load_limit_threshold: Option<ReadableSize>,
    // When memory usage reaches this amount, we start to pick some ranges to evict.
    pub soft_limit_threshold: Option<ReadableSize>,
    pub hard_limit_threshold: Option<ReadableSize>,
    pub expected_region_size: Option<ReadableSize>,
    // used in getting top regions to filter those with less mvcc amplification. Here, we define
    // mvcc amplification to be '(next + prev) / processed_keys'.
    pub mvcc_amplification_threshold: usize,
}

impl Default for RangeCacheEngineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gc_interval: ReadableDuration(Duration::from_secs(180)),
            stop_load_limit_threshold: None,
            // Each load/evict operation should run within five minutes.
            load_evict_interval: ReadableDuration(Duration::from_secs(300)),
            soft_limit_threshold: None,
            hard_limit_threshold: None,
            expected_region_size: None,
            mvcc_amplification_threshold: 100,
        }
    }
}

impl RangeCacheEngineConfig {
    pub fn validate(&mut self, capacity: u64, region_size: u64) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        if (capacity < MIN_HARD_LIMIT_THRESHOLD || capacity <= region_size)
            && self.hard_limit_threshold.is_none()
        {
            self.enabled = false;
            warn!(
                "in-memory engine is disabled because capacity {} is too small, \
                try set `hard-limit-threshold` manually and make sure it's \
                larger than {} and region size {}",
                ReadableSize(capacity), ReadableSize(MIN_HARD_LIMIT_THRESHOLD),
                ReadableSize(region_size);
            );
            return Ok(());
        }

        if self.hard_limit_threshold.is_none() {
            self.hard_limit_threshold = Some(ReadableSize(capacity));
        }

        if self.soft_limit_threshold.is_none() {
            self.soft_limit_threshold = Some(ReadableSize(
                (self.hard_limit_threshold.as_ref().unwrap().0 as f64 * SOFT_LIMIT_THRESHOLD_RATIO)
                    as u64,
            ));
        }

        if self.stop_load_limit_threshold.is_none() {
            self.stop_load_limit_threshold = Some(ReadableSize(
                (self.hard_limit_threshold.as_ref().unwrap().0 as f64 * STOP_LIMIT_THRESHOLD_RATIO)
                    as u64,
            ));
        }

        if self.stop_load_limit_threshold.as_ref().unwrap()
            > self.soft_limit_threshold.as_ref().unwrap()
        {
            return Err(format!(
                "stop-load-limit-threshold {:?} is larger to soft-limit-threshold {:?}",
                self.stop_load_limit_threshold.as_ref().unwrap(),
                self.soft_limit_threshold.as_ref().unwrap()
            ));
        }

        if self.soft_limit_threshold.as_ref().unwrap()
            >= self.hard_limit_threshold.as_ref().unwrap()
        {
            return Err(format!(
                "soft-limit-threshold {:?} is larger or equal to hard-limit-threshold {:?}",
                self.soft_limit_threshold.as_ref().unwrap(),
                self.hard_limit_threshold.as_ref().unwrap()
            ));
        }

        Ok(())
    }

    pub fn stop_load_limit_threshold(&self) -> usize {
        self.stop_load_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn soft_limit_threshold(&self) -> usize {
        self.soft_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn hard_limit_threshold(&self) -> usize {
        self.hard_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn expected_region_size(&self) -> usize {
        self.expected_region_size.map_or(
            raftstore::coprocessor::config::SPLIT_SIZE.0 as usize,
            |r: ReadableSize| r.0 as usize,
        )
    }

    pub fn config_for_test() -> RangeCacheEngineConfig {
        RangeCacheEngineConfig {
            enabled: true,
            gc_interval: ReadableDuration(Duration::from_secs(180)),
            load_evict_interval: ReadableDuration(Duration::from_secs(300)), /* Should run within
                                                                              * five minutes */
            stop_load_limit_threshold: Some(ReadableSize::gb(1)),
            soft_limit_threshold: Some(ReadableSize::gb(1)),
            hard_limit_threshold: Some(ReadableSize::gb(2)),
            expected_region_size: Some(ReadableSize::mb(20)),
            mvcc_amplification_threshold: 10,
        }
    }
}

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
            self.0
                .update(move |cfg: &mut RangeCacheEngineConfig| cfg.update(change))?;
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
    use super::*;

    #[test]
    fn test_range_cache_engine_config_validate() {
        struct Case {
            stop_load_limit_threshold: Option<ReadableSize>,
            soft_limit_threshold: Option<ReadableSize>,
            hard_limit_threshold: Option<ReadableSize>,
            capacity: u64,
            region_size: u64,
            expect_enabled: bool,
            expect_err: bool,
        }

        let cases = [
            Case {
                stop_load_limit_threshold: None,
                soft_limit_threshold: None,
                hard_limit_threshold: None,
                capacity: ReadableSize::gb(2).0,
                region_size: ReadableSize::mb(256).0,
                expect_enabled: true,
                expect_err: false,
            },
            // Capacity is too small.
            Case {
                stop_load_limit_threshold: None,
                soft_limit_threshold: None,
                hard_limit_threshold: None,
                capacity: MIN_HARD_LIMIT_THRESHOLD / 2,
                region_size: ReadableSize::mb(256).0,
                expect_enabled: false,
                expect_err: false,
            },
            Case {
                stop_load_limit_threshold: None,
                soft_limit_threshold: None,
                hard_limit_threshold: None,
                capacity: ReadableSize::gb(2).0,
                region_size: ReadableSize::gb(2).0,
                expect_enabled: false,
                expect_err: false,
            },
            // do not override hard_limit_threshold too large.
            Case {
                stop_load_limit_threshold: None,
                soft_limit_threshold: None,
                hard_limit_threshold: Some(ReadableSize::gb(2)),
                capacity: MIN_HARD_LIMIT_THRESHOLD / 2,
                region_size: ReadableSize::mb(256).0,
                expect_enabled: true,
                expect_err: false,
            },
            // stop_load_limit_threshold too large.
            Case {
                stop_load_limit_threshold: Some(ReadableSize::gb(2)),
                soft_limit_threshold: None,
                hard_limit_threshold: None,
                capacity: ReadableSize::gb(2).0,
                region_size: ReadableSize::mb(256).0,
                expect_enabled: true,
                expect_err: true,
            },
            // soft_limit_threshold too large.
            Case {
                stop_load_limit_threshold: None,
                soft_limit_threshold: Some(ReadableSize::gb(2)),
                hard_limit_threshold: None,
                capacity: ReadableSize::gb(2).0,
                region_size: ReadableSize::mb(256).0,
                expect_enabled: true,
                expect_err: true,
            },
        ];

        for c in cases {
            let mut cfg = RangeCacheEngineConfig::default();
            cfg.enabled = true;
            cfg.hard_limit_threshold = c.hard_limit_threshold;
            cfg.soft_limit_threshold = c.soft_limit_threshold;
            cfg.stop_load_limit_threshold = c.stop_load_limit_threshold;

            let res = cfg.validate(c.capacity, c.region_size);
            if c.expect_err {
                res.unwrap_err();
            } else {
                res.unwrap();
                assert_eq!(cfg.enabled, c.expect_enabled);
                if cfg.enabled {
                    assert!(cfg.hard_limit_threshold.is_some());
                    assert!(cfg.soft_limit_threshold.is_some());
                    assert!(cfg.stop_load_limit_threshold.is_some(),);
                }
            }
        }
    }
}
