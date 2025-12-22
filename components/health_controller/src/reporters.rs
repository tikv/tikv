// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use collections::HashMap;
use kvproto::pdpb;
use pdpb::SlowTrend as SlowTrendPb;
use prometheus::IntGauge;

use crate::{
    HealthController, HealthControllerInner, RaftstoreDuration,
    slow_score::*,
    trend::{RequestPerSecRecorder, Trend},
    types::InspectFactor,
};

/// The parameters for building a [`RaftstoreReporter`].
///
/// For slow trend related parameters (unsensitive_cause, unsensitive_result,
/// cause_*, result_*), please refer to : [`SlowTrendStatistics::new`] and
/// [`Trend`].
pub struct RaftstoreReporterConfig {
    /// The interval to tick the [`RaftstoreReporter`].
    ///
    /// The `RaftstoreReporter` doesn't tick by itself, the caller (the PD
    /// worker) is expected to tick it. But the interval is necessary in
    /// some internal calculations.
    pub inspect_interval: Duration,
    pub inspect_kvdb_interval: Duration,
    pub inspect_network_interval: Duration,

    pub unsensitive_cause: f64,
    pub unsensitive_result: f64,
    pub net_io_factor: f64,

    // Metrics about slow trend.
    pub cause_spike_filter_value_gauge: IntGauge,
    pub cause_spike_filter_count_gauge: IntGauge,
    pub cause_l1_gap_gauges: IntGauge,
    pub cause_l2_gap_gauges: IntGauge,
    pub result_spike_filter_value_gauge: IntGauge,
    pub result_spike_filter_count_gauge: IntGauge,
    pub result_l1_gap_gauges: IntGauge,
    pub result_l2_gap_gauges: IntGauge,
}

/// A unified slow score that combines multiple slow scores.
///
/// For disk factor, it calculates the final slow score of a store by picking
/// the maximum score among multiple disk_factors. Each factor represents a
/// different aspect of the store's performance. Typically, we have two
/// disk_factors: Raft Disk I/O and KvDB Disk I/O.
/// For network factor, it calculates all stores' slow score and report them
/// which is greater than 1 to PD.
pub struct UnifiedSlowScore {
    disk_factors: Vec<SlowScore>,
    // store_id -> SlowScore
    network_factors: Arc<Mutex<HashMap<u64, SlowScore>>>,
}

impl UnifiedSlowScore {
    pub fn new(cfg: &RaftstoreReporterConfig) -> Self {
        let mut unified_slow_score = UnifiedSlowScore {
            disk_factors: Vec::new(),
            network_factors: Arc::new(Mutex::new(HashMap::default())),
        };
        // The first factor is for Raft Disk I/O.
        unified_slow_score.disk_factors.push(SlowScore::new(
            cfg.inspect_interval, // timeout
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        ));
        // The second factor is for KvDB Disk I/O.
        unified_slow_score.disk_factors.push(SlowScore::new(
            cfg.inspect_kvdb_interval, // timeout
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        ));

        unified_slow_score
    }

    #[inline]
    pub fn record_disk(
        &mut self,
        id: u64,
        factor: InspectFactor,
        duration: &RaftstoreDuration,
        not_busy: bool,
    ) {
        // For disk disk_factors, we care about the raftstore duration.
        let dur = duration.delays_on_disk_io(false);
        self.disk_factors[factor as usize].record(id, dur, not_busy);
    }

    pub fn record_network(
        &mut self,
        id: u64,
        durations: HashMap<u64, Duration>, // store_id -> duration
    ) {
        let mut network_factors = self.network_factors.lock().unwrap();

        // Remove store_ids that exist in network_factors but not in durations
        let store_ids_in_durations: HashSet<u64> = durations.keys().cloned().collect();
        network_factors.retain(|store_id, _| store_ids_in_durations.contains(store_id));

        // Record durations for each store
        for (store_id, duration) in durations.iter() {
            network_factors
                .entry(*store_id)
                .or_insert_with(|| {
                    SlowScore::new(
                        NETWORK_TIMEOUT_THRESHOLD,
                        NETWORK_TIMEOUT_RATIO_THRESHOLD,
                        NETWORK_ROUND_TICKS,
                        NETWORK_RECOVERY_INTERVALS,
                    )
                })
                .record(id, *duration, true);
        }
    }

    #[inline]
    pub fn get(&self, factor: InspectFactor) -> &SlowScore {
        &self.disk_factors[factor as usize]
    }

    #[inline]
    pub fn get_mut(&mut self, factor: InspectFactor) -> &mut SlowScore {
        &mut self.disk_factors[factor as usize]
    }

    // Returns the maximum score of disk disk_factors.
    pub fn get_disk_score(&self) -> f64 {
        self.disk_factors
            .iter()
            .map(|factor| factor.get())
            .fold(1.0, f64::max)
    }

    pub fn get_network_score(&self) -> HashMap<u64, u64> {
        self.network_factors
            .lock()
            .unwrap()
            .iter_mut()
            .map(|(k, v)| (*k, v.get() as u64))
            .collect()
    }

    pub fn last_tick_finished(&self) -> bool {
        self.disk_factors.iter().all(SlowScore::last_tick_finished)
    }

    pub fn record_timeout(&mut self, factor: InspectFactor) {
        // We should be able to get the latency from HealthChecker
        // every time, so there is no need to explicitly mark timeout
        if factor == InspectFactor::Network {
            return;
        }

        self.disk_factors[factor as usize].record_timeout();
    }

    pub fn tick(&mut self, factor: InspectFactor) -> SlowScoreTickResult {
        if factor == InspectFactor::Network {
            self.tick_network()
        } else {
            self.disk_factors[factor as usize].tick()
        }
    }

    fn tick_network(&mut self) -> SlowScoreTickResult {
        let mut slow_score_tick_result = SlowScoreTickResult::default();
        // For network factor, we need to tick all network factors and return the avg
        // result.
        let mut network_factors = self.network_factors.lock().unwrap();
        let mut total_score = 0.0;
        let mut total_count = 0;

        // We need to sync last_tick_id among all stores' network factors, because
        // we record network duration based on the last_tick_id and record network
        // duration at the same time. If the last_tick_id is different, we can't
        // record network duration correctly. In addition, the same last_tick_id
        // makes it easier for us to return the result to the upper layer to record
        // metrics.
        let mut need_sync = false;
        let mut last_tick_id = 0;
        for (_, factor) in network_factors.iter_mut() {
            if last_tick_id != 0 {
                need_sync = need_sync || !last_tick_id.eq(&factor.get_last_tick_id());
            }
            last_tick_id = factor.get_last_tick_id().max(last_tick_id);
        }
        if need_sync {
            for (_, factor) in network_factors.iter_mut() {
                factor.set_last_tick_id(last_tick_id);
            }
        }
        for (_, factor) in network_factors.iter_mut() {
            let factor_tick_result = factor.tick();
            if factor_tick_result.updated_score.is_some() {
                slow_score_tick_result.has_new_record = true;
                total_score += factor_tick_result.updated_score.unwrap_or(1.0);
                total_count += 1;
            }
            slow_score_tick_result.tick_id = factor_tick_result
                .tick_id
                .max(slow_score_tick_result.tick_id);
        }

        // To ensure that score can display complete information
        if total_count == network_factors.len() {
            slow_score_tick_result.updated_score = Some(total_score / total_count as f64);
        }
        slow_score_tick_result
    }
}

pub struct RaftstoreReporter {
    health_controller_inner: Arc<HealthControllerInner>,
    slow_score: UnifiedSlowScore,
    slow_trend: SlowTrendStatistics,
    is_healthy: bool,
}

impl RaftstoreReporter {
    const MODULE_NAME: &'static str = "raftstore";

    pub fn new(health_controller: &HealthController, cfg: RaftstoreReporterConfig) -> Self {
        Self {
            health_controller_inner: health_controller.inner.clone(),
            slow_score: UnifiedSlowScore::new(&cfg),
            slow_trend: SlowTrendStatistics::new(cfg),
            is_healthy: true,
        }
    }

    pub fn get_disk_slow_score(&mut self) -> f64 {
        self.slow_score.get_disk_score()
    }

    pub fn get_network_slow_score(&mut self) -> HashMap<u64, u64> {
        self.slow_score.get_network_score()
    }

    pub fn get_slow_trend(&self) -> &SlowTrendStatistics {
        &self.slow_trend
    }

    pub fn record_duration(
        &mut self,
        id: u64,
        factor: InspectFactor,
        duration: RaftstoreDuration,
        store_not_busy: bool,
    ) {
        // Fine-tuned, `SlowScore` only takes the I/O jitters on the disk into account.

        self.slow_trend.record(&duration);

        if factor == InspectFactor::RaftDisk || factor == InspectFactor::KvDisk {
            self.slow_score
                .record_disk(id, factor, &duration, store_not_busy);
            // Publish slow score to health controller
            self.health_controller_inner
                .update_raftstore_slow_score(self.slow_score.get_disk_score());
        }
    }

    pub fn record_network_duration(&mut self, id: u64) -> HashMap<u64, Duration> {
        let network_durations = self.health_controller_inner.get_network_latencies();
        self.slow_score
            .record_network(id, network_durations.clone());
        network_durations
    }

    fn is_healthy(&self) -> bool {
        self.is_healthy
    }

    fn set_is_healthy(&mut self, is_healthy: bool) {
        if is_healthy == self.is_healthy {
            return;
        }

        self.is_healthy = is_healthy;
        if is_healthy {
            self.health_controller_inner
                .remove_unhealthy_module(Self::MODULE_NAME);
        } else {
            self.health_controller_inner
                .add_unhealthy_module(Self::MODULE_NAME);
        }
    }

    pub fn tick(&mut self, store_maybe_busy: bool, factor: InspectFactor) -> SlowScoreTickResult {
        match factor {
            InspectFactor::Network => self.slow_score.tick(factor),
            InspectFactor::RaftDisk | InspectFactor::KvDisk => {
                // Record a fairly great value when timeout
                self.slow_trend.slow_cause.record(500_000, Instant::now());

                // healthy: The health status of the current store.
                // all_ticks_finished: The last tick of all disk_factors is finished.
                // factor_tick_finished: The last tick of the current factor is finished.
                let (healthy, all_ticks_finished, factor_tick_finished) = (
                    self.is_healthy(),
                    self.slow_score.last_tick_finished(),
                    self.slow_score.get(factor).last_tick_finished(),
                );
                // The health status is recovered to serving as long as any tick
                // does not timeout.
                if !healthy && all_ticks_finished {
                    self.set_is_healthy(true);
                }
                if !all_ticks_finished {
                    // If the last tick is not finished, it means that the current store might
                    // be busy on handling requests or delayed on I/O operations. And only when
                    // the current store is not busy, it should record the last_tick as a timeout.
                    if !store_maybe_busy && !factor_tick_finished {
                        self.slow_score.get_mut(factor).record_timeout();
                    }
                }

                let slow_score_tick_result = self.slow_score.tick(factor);
                if slow_score_tick_result.updated_score.is_some()
                    && !slow_score_tick_result.has_new_record
                {
                    self.set_is_healthy(false);
                }

                // Publish the slow score to health controller
                if slow_score_tick_result.updated_score.is_some() {
                    // Publish slow score to health controller
                    self.health_controller_inner
                        .update_raftstore_slow_score(self.slow_score.get_disk_score());
                }

                slow_score_tick_result
            }
        }
    }

    pub fn update_slow_trend(
        &mut self,
        observed_request_count: u64,
        now: Instant,
    ) -> (Option<f64>, SlowTrendPb) {
        let requests_per_sec = self
            .slow_trend
            .slow_result_recorder
            .record_and_get_current_rps(observed_request_count, now);

        let slow_trend_cause_rate = self.slow_trend.slow_cause.increasing_rate();
        let mut slow_trend_pb = SlowTrendPb::default();
        slow_trend_pb.set_cause_rate(slow_trend_cause_rate);
        slow_trend_pb.set_cause_value(self.slow_trend.slow_cause.l0_avg());
        if let Some(requests_per_sec) = requests_per_sec {
            self.slow_trend
                .slow_result
                .record(requests_per_sec as u64, Instant::now());
            slow_trend_pb.set_result_value(self.slow_trend.slow_result.l0_avg());
            let slow_trend_result_rate = self.slow_trend.slow_result.increasing_rate();
            slow_trend_pb.set_result_rate(slow_trend_result_rate);
        }

        // Publish the result to health controller.
        self.health_controller_inner
            .update_raftstore_slow_trend(slow_trend_pb.clone());

        (requests_per_sec, slow_trend_pb)
    }
}

pub struct SlowTrendStatistics {
    net_io_factor: f64,
    /// Detector to detect NetIo&DiskIo jitters.
    pub slow_cause: Trend,
    /// Reactor as an assistant detector to detect the QPS jitters.
    pub slow_result: Trend,
    pub slow_result_recorder: RequestPerSecRecorder,
}

impl SlowTrendStatistics {
    #[inline]
    pub fn new(config: RaftstoreReporterConfig) -> Self {
        Self {
            slow_cause: Trend::new(
                // Disable SpikeFilter for now
                Duration::from_secs(0),
                config.cause_spike_filter_value_gauge,
                config.cause_spike_filter_count_gauge,
                Duration::from_secs(180),
                Duration::from_secs(30),
                Duration::from_secs(120),
                Duration::from_secs(600),
                1,
                tikv_util::time::duration_to_us(Duration::from_micros(500)),
                config.cause_l1_gap_gauges,
                config.cause_l2_gap_gauges,
                config.unsensitive_cause,
            ),
            slow_result: Trend::new(
                // Disable SpikeFilter for now
                Duration::from_secs(0),
                config.result_spike_filter_value_gauge,
                config.result_spike_filter_count_gauge,
                Duration::from_secs(120),
                Duration::from_secs(15),
                Duration::from_secs(60),
                Duration::from_secs(300),
                1,
                2000,
                config.result_l1_gap_gauges,
                config.result_l2_gap_gauges,
                config.unsensitive_result,
            ),
            slow_result_recorder: RequestPerSecRecorder::new(),
            net_io_factor: config.net_io_factor, /* FIXME: add extra parameter in
                                                  * Config to control it. */
        }
    }

    #[inline]
    pub fn record(&mut self, duration: &RaftstoreDuration) {
        // TODO: It's more appropriate to divide the factor into `Disk IO factor` and
        // `Net IO factor`.
        // Currently, when `network ratio == 1`, it summarizes all disk_factors by `sum`
        // simplily, approved valid to common cases when there exists IO jitters on
        // Network or Disk.
        let latency = || -> u64 {
            if self.net_io_factor as u64 >= 1 {
                return tikv_util::time::duration_to_us(duration.sum());
            }
            let disk_io_latency =
                tikv_util::time::duration_to_us(duration.delays_on_disk_io(true)) as f64;
            let network_io_latency =
                tikv_util::time::duration_to_us(duration.delays_on_net_io()) as f64;
            (disk_io_latency + network_io_latency * self.net_io_factor) as u64
        }();
        self.slow_cause.record(latency, Instant::now());
    }
}

/// A reporter that can set states directly, for testing purposes.
pub struct TestReporter {
    health_controller_inner: Arc<HealthControllerInner>,
}

impl TestReporter {
    pub fn new(health_controller: &HealthController) -> Self {
        Self {
            health_controller_inner: health_controller.inner.clone(),
        }
    }

    pub fn set_raftstore_slow_score(&self, slow_score: f64) {
        self.health_controller_inner
            .update_raftstore_slow_score(slow_score);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use prometheus::IntGauge;

    use super::*;

    fn create_test_config() -> RaftstoreReporterConfig {
        RaftstoreReporterConfig {
            inspect_interval: Duration::from_secs(1),
            inspect_kvdb_interval: Duration::from_secs(1),
            inspect_network_interval: Duration::from_secs(1),
            unsensitive_cause: 0.5,
            unsensitive_result: 0.5,
            net_io_factor: 1.0,
            cause_spike_filter_value_gauge: IntGauge::new("cause_spike_filter_value", "help")
                .unwrap(),
            cause_spike_filter_count_gauge: IntGauge::new("cause_spike_filter_count", "help")
                .unwrap(),
            cause_l1_gap_gauges: IntGauge::new("cause_l1_gap", "help").unwrap(),
            cause_l2_gap_gauges: IntGauge::new("cause_l2_gap", "help").unwrap(),
            result_spike_filter_value_gauge: IntGauge::new("result_spike_filter_value", "help")
                .unwrap(),
            result_spike_filter_count_gauge: IntGauge::new("result_spike_filter_count", "help")
                .unwrap(),
            result_l1_gap_gauges: IntGauge::new("result_l1_gap", "help").unwrap(),
            result_l2_gap_gauges: IntGauge::new("result_l2_gap", "help").unwrap(),
        }
    }

    #[test]
    fn test_unified_slow_score_record_network() {
        let cfg = create_test_config();
        let mut unified_slow_score = UnifiedSlowScore::new(&cfg);

        // Test recording network durations for multiple stores
        let mut durations = HashMap::default();
        durations.insert(1u64, Duration::from_millis(100));
        durations.insert(2u64, Duration::from_millis(200));
        durations.insert(3u64, Duration::from_millis(300));

        // First tick to establish baseline
        let result = unified_slow_score.tick_network();
        assert_eq!(result.tick_id, 0);

        // Record network durations for current tick
        unified_slow_score.record_network(0, durations.clone());

        // Verify that network disk_factors are created
        let network_factors = unified_slow_score.network_factors.lock().unwrap();
        assert_eq!(network_factors.len(), 3);
        assert!(network_factors.contains_key(&1));
        assert!(network_factors.contains_key(&2));
        assert!(network_factors.contains_key(&3));
        assert!(!network_factors.contains_key(&4));
        drop(network_factors);

        // Tick again to advance
        let result2 = unified_slow_score.tick_network();
        assert_eq!(result2.tick_id, 1);

        durations.insert(4u64, Duration::from_millis(400));
        unified_slow_score.record_network(1, durations.clone());

        // Test updating with fewer stores (should remove missing stores)
        let mut new_durations = HashMap::default();
        new_durations.insert(1u64, Duration::from_millis(150));
        new_durations.insert(2u64, Duration::from_millis(250));
        // Store 3 is not included, should be removed

        unified_slow_score.record_network(2, new_durations);

        let network_factors = unified_slow_score.network_factors.lock().unwrap();
        assert_eq!(network_factors.len(), 2);
        assert!(network_factors.contains_key(&1));
        assert!(network_factors.contains_key(&2));
        assert!(!network_factors.contains_key(&3));
    }

    #[test]
    fn test_unified_slow_score_tick_network_synchronization() {
        let cfg = create_test_config();
        let mut unified_slow_score = UnifiedSlowScore::new(&cfg);

        // Record network durations
        let mut durations = HashMap::default();
        durations.insert(1u64, Duration::from_millis(100));
        durations.insert(2u64, Duration::from_millis(200));

        // First tick to establish baseline
        let result1 = unified_slow_score.tick_network();
        assert_eq!(result1.tick_id, 0);

        unified_slow_score.record_network(1, durations);

        // Manually set different tick IDs to test synchronization
        {
            let mut network_factors = unified_slow_score.network_factors.lock().unwrap();
            let factor_1 = network_factors.get_mut(&1).unwrap();
            factor_1.set_last_tick_id(5);
            let factor_2 = network_factors.get_mut(&2).unwrap();
            factor_2.set_last_tick_id(3);
        }

        // Tick should synchronize the tick IDs
        let result2 = unified_slow_score.tick_network();

        // Verify all disk_factors have the same (maximum) tick ID
        {
            let network_factors = unified_slow_score.network_factors.lock().unwrap();
            let factor_1_id = network_factors.get(&1).unwrap().get_last_tick_id();
            let factor_2_id = network_factors.get(&2).unwrap().get_last_tick_id();
            assert_eq!(factor_1_id, factor_2_id);
            assert_eq!(factor_1_id, 6); // Should be the maximum (5 + 1 from tick)
            assert_eq!(result2.tick_id, 6);
        }
    }

    #[test]
    fn test_unified_slow_score_get_network_score() {
        let cfg = create_test_config();
        let mut unified_slow_score = UnifiedSlowScore::new(&cfg);

        // Initially should return empty scores
        let scores = unified_slow_score.get_network_score();
        assert!(scores.is_empty());

        // Record network durations
        let mut durations = HashMap::default();
        durations.insert(1u64, Duration::from_millis(100));
        durations.insert(2u64, Duration::from_millis(200));
        unified_slow_score.record_network(1, durations);

        // Get network scores
        let scores = unified_slow_score.get_network_score();
        assert_eq!(scores.len(), 2);
        assert!(scores.contains_key(&1));
        assert!(scores.contains_key(&2));

        // All scores should start at 1 (default slow score)
        assert_eq!(scores[&1], 1);
        assert_eq!(scores[&2], 1);

        // Test multiple ticks to see score evolution
        // NETWORK_ROUND_TICKS = 3, so scores will update every 3 ticks

        // First 2 ticks - no score update yet
        let result1 = unified_slow_score.tick_network();
        assert!(result1.updated_score.is_none());
        assert_eq!(result1.tick_id, 1);

        let result2 = unified_slow_score.tick_network();
        assert!(result2.updated_score.is_none());
        assert_eq!(result2.tick_id, 2);

        // 3rd tick - should trigger score update
        let result3 = unified_slow_score.tick_network();
        assert!(result3.updated_score.is_some());
        assert_eq!(result3.tick_id, 3);
        // Since no timeout requests were recorded, scores should still be 1.0
        assert_eq!(result3.updated_score.unwrap(), 1.0);

        // Verify scores remain at 1
        let scores_after_ticks = unified_slow_score.get_network_score();
        assert_eq!(scores_after_ticks[&1], 1);
        assert_eq!(scores_after_ticks[&2], 1);

        // Now record some timeout durations to test score increase
        let mut timeout_durations = HashMap::default();
        timeout_durations.insert(1u64, Duration::from_secs(2)); // > NETWORK_TIMEOUT_THRESHOLD (1s)
        timeout_durations.insert(2u64, Duration::from_secs(3)); // > NETWORK_TIMEOUT_THRESHOLD (1s)

        // Tick to next round (ticks 4, 5, 6)
        let result4 = unified_slow_score.tick_network();
        assert!(result4.updated_score.is_none());
        assert_eq!(result4.tick_id, 4);

        // Record timeouts for tick 4 (current tick)
        unified_slow_score.record_network(4, timeout_durations.clone());

        let result5 = unified_slow_score.tick_network();
        assert!(result5.updated_score.is_none());
        assert_eq!(result5.tick_id, 5);

        // Record timeouts for tick 5 (current tick)
        unified_slow_score.record_network(5, timeout_durations.clone());

        let result6 = unified_slow_score.tick_network();
        assert!(result6.updated_score.is_some());
        assert_eq!(result6.tick_id, 6);
        // Score should increase due to timeout requests
        assert!(result6.updated_score.unwrap() > 1.0);

        // Verify scores have increased
        let scores_after_timeout = unified_slow_score.get_network_score();
        assert!(scores_after_timeout[&1] > 1);
        assert!(scores_after_timeout[&2] > 1);

        // Test recovery - record normal durations (no timeouts)
        let mut normal_durations = HashMap::default();
        normal_durations.insert(1u64, Duration::from_millis(100));
        normal_durations.insert(2u64, Duration::from_millis(200));

        // Record for several rounds to test score recovery
        for i in 7..=15 {
            unified_slow_score.record_network(i, normal_durations.clone());
            let result = unified_slow_score.tick_network();

            // Every 3rd tick should have updated score
            if i % 3 == 0 {
                assert!(result.updated_score.is_some());
                // Scores should gradually decrease back towards 1.0
                if i >= 12 {
                    // After several rounds without timeouts, scores should be decreasing
                    let current_scores = unified_slow_score.get_network_score();
                    // Scores should be lower than after the timeout round
                    assert!(
                        current_scores[&1] <= scores_after_timeout[&1] || current_scores[&1] == 1
                    );
                    assert!(
                        current_scores[&2] <= scores_after_timeout[&2] || current_scores[&2] == 1
                    );
                }
            } else {
                assert!(result.updated_score.is_none());
            }
        }
    }
}
