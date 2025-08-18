// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    collections::HashMap,
    time::{Duration, Instant},
};
use std::sync::Mutex;

use kvproto::pdpb;
use pdpb::SlowTrend as SlowTrendPb;
use prometheus::IntGauge;

use tikv_util::info;

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
/// It calculates the final slow score of a store by picking the maximum
/// score among multiple factors. Each factor represents a different aspect of
/// the store's performance. Typically, we have two factors: Raft Disk I/O and
/// KvDB Disk I/O. If there are more factors in the future, we can add them
/// here.
pub struct UnifiedSlowScore {
    factors: Vec<SlowScore>,
    network_factors: Arc<Mutex<HashMap<u64, SlowScore>>>,
}

impl UnifiedSlowScore {
    pub fn new(cfg: &RaftstoreReporterConfig) -> Self {
        let mut unified_slow_score = UnifiedSlowScore {
            factors: Vec::new(),
            network_factors: Arc::new(Mutex::new(HashMap::new())),
        };
        // The first factor is for Raft Disk I/O.
        unified_slow_score.factors.push(SlowScore::new(
            cfg.inspect_interval, // timeout
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        ));
        // The second factor is for KvDB Disk I/O.
        unified_slow_score.factors.push(SlowScore::new(
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
        // For disk factors, we care about the raftstore duration.
        let dur = duration.delays_on_disk_io(false);   
        self.factors[factor as usize].record(id, dur, not_busy);
    }

    pub fn record_network(
        &mut self,
        id: u64,
        durations: HashMap<u64, Duration>, // store_id -> duration
    ) {
        for (store_id, duration) in durations.iter() {
            self.network_factors.lock().unwrap().entry(*store_id).or_insert_with(|| {
                SlowScore::new(
                    NETWORK_TIMEOUT_THRESHOLD,
                    NETWORK_TIMEOUT_RATIO_THRESHOLD,
                    NETWORK_ROUND_TICKS,
                    NETWORK_RECOVERY_INTERVALS,
                )
            }).record(id, *duration, true);
        }
    }

    #[inline]
    pub fn get(&self, factor: InspectFactor) -> &SlowScore {
        &self.factors[factor as usize]
    }

    #[inline]
    pub fn get_mut(&mut self, factor: InspectFactor) -> &mut SlowScore {
        &mut self.factors[factor as usize]
    }

    // Returns the maximum score of disk factors.
    pub fn get_disk_score(&mut self) -> f64 {
        (self.factors[InspectFactor::RaftDisk as usize].get() + 
            self.factors[InspectFactor::KvDisk as usize].get()) / 2.0
    }

    pub fn get_network_score(&self) -> HashMap<u64, u64> {
        self.network_factors.lock().unwrap()
            .iter_mut()
            .map(|(k, v)| (*k, v.get() as u64)) 
            .collect()
    }

    pub fn last_tick_finished(&self) -> bool {
        self.factors.iter().all(SlowScore::last_tick_finished)
    }

    pub fn record_timeout(&mut self, factor: InspectFactor) {
        if factor == InspectFactor::Network {
            return;
        }

        self.factors[factor as usize].record_timeout();
    }

    pub fn tick(&mut self, factor: InspectFactor) -> SlowScoreTickResult {
        let mut slow_score_tick_result = SlowScoreTickResult::default();
        if factor == InspectFactor::Network {
            // For network factor, we need to tick all network factors and return the avg result.
            let mut network_factors = self.network_factors.lock().unwrap();
            let mut total_score = 0.0;
            let mut total_count = 0;

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
                    total_score += factor_tick_result.updated_score.unwrap_or(0.0);
                    total_count += 1;
                }
                slow_score_tick_result.tick_id = factor_tick_result.tick_id.max(slow_score_tick_result.tick_id);
            }

            // To ensure that score can display complete information
            if total_count == network_factors.len() {
                slow_score_tick_result.updated_score = Some(total_score / total_count as f64);
            }
            info!(
                "network slow score tick: {:?}, total_score: {}, total_count: {}",
                slow_score_tick_result,
                total_score,
                total_count
            );
        } else {
            slow_score_tick_result = self.factors[factor as usize].tick();
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

    pub fn new(
        health_controller: &HealthController,
        cfg: RaftstoreReporterConfig,
    ) -> Self {
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

    pub fn record_network_duration(
        &mut self,
        id: u64,
        duration: HashMap<u64, Duration>,
    ) {
        self.slow_score.record_network(id, duration);
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
        // Record a fairly great value when timeout
        self.slow_trend.slow_cause.record(500_000, Instant::now());

        // healthy: The health status of the current store.
        // all_ticks_finished: The last tick of all factors is finished.
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
        if !all_ticks_finished && factor != InspectFactor::Network {
            // If the last tick is not finished, it means that the current store might
            // be busy on handling requests or delayed on I/O operations. And only when
            // the current store is not busy, it should record the last_tick as a timeout.
            if !store_maybe_busy && !factor_tick_finished {
                self.slow_score.get_mut(factor).record_timeout();
            }
        }

        let slow_score_tick_result = self.slow_score.tick(factor);
        if slow_score_tick_result.updated_score.is_some() && !slow_score_tick_result.has_new_record
        {
            self.set_is_healthy(false);
        }

        // Publish the slow score to health controller
        if slow_score_tick_result.updated_score.is_some() {
            match factor {
                InspectFactor::RaftDisk | InspectFactor::KvDisk => {
                    // Publish slow score to health controller
                    self.health_controller_inner
                        .update_raftstore_slow_score(self.slow_score.get_disk_score());
                }
                InspectFactor::Network => {
                    // self.health_controller_inner
                    //     .update_network_slow_score(self.slow_score.get_network_score());
                }
            }
        }

        slow_score_tick_result
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
        // Currently, when `network ratio == 1`, it summarizes all factors by `sum`
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
