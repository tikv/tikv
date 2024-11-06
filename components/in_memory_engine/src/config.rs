use std::{error::Error, sync::Arc, time::Duration};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, ReadableSize, VersionTrack},
    info,
};

const DEFAULT_GC_RUN_INTERVAL: Duration = Duration::from_secs(180);
// The minimum interval for GC run is 10 seconds. Shorter interval is not
// meaningful because the GC process is CPU intensive and may not complete in
// 10 seconds.
const MIN_GC_RUN_INTERVAL: Duration = Duration::from_secs(10);
// The maximum interval for GC run is 10 minutes which equals to the minimum
// value of TiDB GC lifetime.
const MAX_GC_RUN_INTERVAL: Duration = Duration::from_secs(600);
// the maximum write kv throughput(20MiB), this is an empirical value.
const MAX_WRITE_KV_SPEED: u64 = 20 * 1024 * 1024;
// The maximum duration in seconds we expect IME to release enough memory after
// memory usage reaches `evict_threshold`. This is an empirical value.
// We use this value to determine the default value of `evict_threshold` based
// on `capacity`.
const MAX_RESERVED_DURATION_FOR_WRITE: u64 = 10;
// Regions' mvcc read amplification statistics is updated every 1min, so we set
// the minimal load&evict check duration to 2min.
const MIN_LOAD_EVICT_INTERVAL: Duration = Duration::from_secs(120);
// The default threshold for mvcc amplification. Test shows setting it to 10
// can benefit common workloads, eg, TPCc (50 warehouse), saving about 20% of
// unified read pool CPU usage.
const DEFAULT_MVCC_AMPLIFICATION_THRESHOLD: usize = 10;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, OnlineConfig)]
#[serde(default, rename_all = "kebab-case")]
pub struct InMemoryEngineConfig {
    /// Determines whether to enable the in memory engine feature.
    pub enable: bool,
    /// The maximum memory usage of the engine.
    pub capacity: Option<ReadableSize>,
    /// When memory usage reaches this amount, we start to pick some regions to
    /// evict.
    /// Default value: `capacity` - min(10 * MAX_WRITE_BYTES_SEC, capacity *
    /// 0.1).
    pub evict_threshold: Option<ReadableSize>,
    /// When memory usage reaches this amount, we stop loading regions.
    // TODO(SpadeA): ultimately we only expose one memory limit to user.
    // When memory usage reaches this amount, no further load will be
    // performed.
    // Default value: `evict_threshold` - min(RegionSplitSize(256MB) * 2 + MAX_WRITE_BYTES_SEC *
    // MAX_EVICT_REGION_DUR_SECS，`evict_threshold` * 0.15)
    pub stop_load_threshold: Option<ReadableSize>,
    /// Determines the oldest timestamp (approximately, now - gc_run_interval)
    /// of the read request the in memory engine can serve.
    pub gc_run_interval: ReadableDuration,
    pub load_evict_interval: ReadableDuration,
    /// used in getting top regions to filter those with less mvcc
    /// amplification. Here, we define mvcc amplification to be
    /// '(next + prev) / processed_keys'.
    pub mvcc_amplification_threshold: usize,
    /// Cross check is only for test usage and should not be turned on in
    /// production environment. Interval 0 means it is turned off, which is
    /// the default value.
    #[online_config(skip)]
    pub cross_check_interval: ReadableDuration,

    // It's always set to region split size, should not be modified manually.
    #[online_config(skip)]
    #[serde(skip)]
    #[doc(hidden)]
    pub expected_region_size: ReadableSize,
}

impl Default for InMemoryEngineConfig {
    fn default() -> Self {
        Self {
            enable: false,
            gc_run_interval: ReadableDuration(DEFAULT_GC_RUN_INTERVAL),
            stop_load_threshold: None,
            // Each load/evict operation should run within five minutes.
            load_evict_interval: ReadableDuration(Duration::from_secs(300)),
            evict_threshold: None,
            capacity: None,
            mvcc_amplification_threshold: DEFAULT_MVCC_AMPLIFICATION_THRESHOLD,
            cross_check_interval: ReadableDuration(Duration::from_secs(0)),
            expected_region_size: raftstore::coprocessor::config::SPLIT_SIZE,
        }
    }
}

impl InMemoryEngineConfig {
    pub fn validate(&mut self, region_split_size: ReadableSize) -> Result<(), Box<dyn Error>> {
        if !self.enable {
            return Ok(());
        }

        if self.capacity.is_none() {
            return Err("evict-threshold or capacity not set".into());
        }

        if self.evict_threshold.is_none() {
            let capacity = self.capacity.unwrap().0;
            let delta = std::cmp::min(
                capacity / 10,
                MAX_RESERVED_DURATION_FOR_WRITE * MAX_WRITE_KV_SPEED,
            );
            self.evict_threshold = Some(ReadableSize(capacity - delta));
        } else if self.evict_threshold.as_ref().unwrap() >= self.capacity.as_ref().unwrap() {
            return Err(format!(
                "evict-threshold {:?} is larger or equal to capacity {:?}",
                self.evict_threshold.as_ref().unwrap(),
                self.capacity.as_ref().unwrap()
            )
            .into());
        }

        if self.stop_load_threshold.is_none() {
            let delta = std::cmp::min(
                self.capacity.unwrap().0 * 15 / 100,
                region_split_size.0 * 2 + MAX_RESERVED_DURATION_FOR_WRITE * MAX_WRITE_KV_SPEED,
            );
            self.stop_load_threshold = Some(ReadableSize(self.evict_threshold.unwrap().0 - delta));
        } else if self.stop_load_threshold.unwrap() > self.evict_threshold.unwrap() {
            return Err(format!(
                "stop-load-threshold {:?} is larger to evict-threshold {:?}",
                self.stop_load_threshold.as_ref().unwrap(),
                self.evict_threshold.as_ref().unwrap()
            )
            .into());
        }

        // The GC interval should be in the range
        // [MIN_GC_RUN_INTERVAL, MAX_GC_RUN_INTERVAL].
        if self.gc_run_interval.0 < MIN_GC_RUN_INTERVAL
            || self.gc_run_interval.0 > MAX_GC_RUN_INTERVAL
        {
            return Err(format!(
                "gc-run-interval {:?} should be in the range [{:?}, {:?}]",
                self.gc_run_interval, MIN_GC_RUN_INTERVAL, MAX_GC_RUN_INTERVAL
            )
            .into());
        }

        if self.load_evict_interval.0 < MIN_LOAD_EVICT_INTERVAL {
            return Err(format!(
                "load-evict-interval {:?} should be greater or equal to {:?}",
                self.load_evict_interval, MIN_LOAD_EVICT_INTERVAL
            )
            .into());
        }

        Ok(())
    }

    pub fn stop_load_threshold(&self) -> usize {
        self.stop_load_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn evict_threshold(&self) -> usize {
        self.evict_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn capacity(&self) -> usize {
        self.capacity.map_or(0, |r| r.0 as usize)
    }

    pub fn config_for_test() -> InMemoryEngineConfig {
        InMemoryEngineConfig {
            enable: true,
            gc_run_interval: ReadableDuration(Duration::from_secs(180)),
            load_evict_interval: ReadableDuration(Duration::from_secs(300)),
            stop_load_threshold: Some(ReadableSize::gb(1)),
            evict_threshold: Some(ReadableSize::gb(1)),
            capacity: Some(ReadableSize::gb(2)),
            expected_region_size: ReadableSize::mb(20),
            mvcc_amplification_threshold: DEFAULT_MVCC_AMPLIFICATION_THRESHOLD,
            cross_check_interval: ReadableDuration(Duration::from_secs(0)),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_REGION_SPLIT_SIZE: ReadableSize = ReadableSize::mb(256);

    #[test]
    fn test_validate() {
        let mut cfg = InMemoryEngineConfig::default();
        cfg.validate(DEFAULT_REGION_SPLIT_SIZE).unwrap();

        cfg.enable = true;
        assert!(cfg.validate(DEFAULT_REGION_SPLIT_SIZE).is_err());

        cfg.capacity = Some(ReadableSize::gb(2));
        cfg.evict_threshold = Some(ReadableSize::gb(1));
        cfg.stop_load_threshold = Some(ReadableSize::gb(1));
        cfg.validate(DEFAULT_REGION_SPLIT_SIZE).unwrap();

        // Error if less than MIN_GC_RUN_INTERVAL.
        cfg.gc_run_interval = ReadableDuration(Duration::ZERO);
        assert!(cfg.validate(DEFAULT_REGION_SPLIT_SIZE).is_err());
        cfg.gc_run_interval = ReadableDuration(Duration::from_secs(9));
        assert!(cfg.validate(DEFAULT_REGION_SPLIT_SIZE).is_err());

        // Error if larger than MIN_GC_RUN_INTERVAL.
        cfg.gc_run_interval = ReadableDuration(Duration::from_secs(601));
        assert!(cfg.validate(DEFAULT_REGION_SPLIT_SIZE).is_err());
        cfg.gc_run_interval = ReadableDuration(Duration::MAX);
        assert!(cfg.validate(DEFAULT_REGION_SPLIT_SIZE).is_err());

        cfg.gc_run_interval = ReadableDuration(Duration::from_secs(180));
        cfg.validate(DEFAULT_REGION_SPLIT_SIZE).unwrap();

        #[track_caller]
        fn check_delta(
            cfg: &InMemoryEngineConfig,
            evict_delta: ReadableSize,
            load_delta: ReadableSize,
        ) {
            let real_evict_delta = cfg.capacity.unwrap() - cfg.evict_threshold.unwrap();
            assert_eq!(real_evict_delta, evict_delta);
            let real_load_delta = cfg.evict_threshold.unwrap() - cfg.stop_load_threshold.unwrap();
            assert_eq!(real_load_delta, load_delta);
        }

        let mut cfg = InMemoryEngineConfig::default();
        cfg.enable = true;
        cfg.capacity = Some(ReadableSize::gb(1));
        cfg.validate(DEFAULT_REGION_SPLIT_SIZE).unwrap();
        check_delta(
            &cfg,
            ReadableSize::gb(1) / 10,
            ReadableSize::gb(1) * 15 / 100,
        );

        let mut cfg = InMemoryEngineConfig::default();
        cfg.enable = true;
        cfg.capacity = Some(ReadableSize::gb(5));
        cfg.validate(DEFAULT_REGION_SPLIT_SIZE).unwrap();
        check_delta(&cfg, ReadableSize::mb(200), ReadableSize::mb(712));

        let mut cfg = InMemoryEngineConfig::default();
        cfg.enable = true;
        cfg.capacity = Some(ReadableSize::gb(5));
        cfg.validate(ReadableSize::mb(96)).unwrap();
        check_delta(&cfg, ReadableSize::mb(200), ReadableSize::mb(392));
    }
}
