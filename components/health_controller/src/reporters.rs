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
use security::SecurityManager;
use grpcio::{Environment, ChannelBuilder, Channel};
use pd_client::PdClient;
use grpcio_health::{proto::HealthClient};
use tikv_util::warn;

use tikv_util::info;

use crate::{
    HealthController, HealthControllerInner, RaftstoreDuration, UnifiedDuration,
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

    // client_mgr: Arc<TikvClientMgr>,
    inspect_network_interval: Duration,
}

impl UnifiedSlowScore {
    pub fn new(cfg: &RaftstoreReporterConfig, client_mgr: Arc<Mutex<TikvClientMgr>>) -> Self {
        let mut unified_slow_score = UnifiedSlowScore {
            factors: Vec::new(),
            network_factors: Arc::new(Mutex::new(HashMap::new())),
            // client_mgr: client_mgr.clone(),
            inspect_network_interval: cfg.inspect_network_interval,
        };
        // The first factor is for Raft Disk I/O.
        unified_slow_score.factors.push(SlowScore::new(
            cfg.inspect_interval, // timeout
            cfg.inspect_interval, // inspect interval
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        ));
        // The second factor is for KvDB Disk I/O.
        unified_slow_score.factors.push(SlowScore::new(
            cfg.inspect_kvdb_interval, // timeout
            cfg.inspect_kvdb_interval, // inspect interval
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        ));

        let network_factors = unified_slow_score.network_factors.clone();
        let inspect_network_interval = unified_slow_score.inspect_network_interval;
        std::thread::spawn(move || {
            loop {
                let health_clients = client_mgr.lock().unwrap().get_health_clients();
                for store_id in health_clients.keys() {
                    let mut network_factors = network_factors.lock().unwrap();
                    if !network_factors.contains_key(store_id) {
                        network_factors.insert(*store_id, SlowScore::new(
                            NETWORK_TIMEOUT_THRESHOLD,
                            inspect_network_interval,
                            NETWORK_TIMEOUT_RATIO_THRESHOLD,
                            NETWORK_ROUND_TICKS,
                            NETWORK_RECOVERY_INTERVALS,
                        ));
                    }
                }
                network_factors.lock().unwrap().retain(|store_id, _| health_clients.contains_key(store_id));
                std::thread::sleep(Duration::from_secs(60));
            }
        });


        unified_slow_score
    }

    // pub fn insert_network_factor(&mut self, store_id: u64) {
    //     if self.network_factors.contains_key(&store_id) {
    //         return;
    //     }
    //     let slow_score = SlowScore::new(
    //         NETWORK_TIMEOUT_THRESHOLD,
    //         self.inspect_network_interval,
    //         NETWORK_TIMEOUT_RATIO_THRESHOLD,
    //         NETWORK_ROUND_TICKS,
    //         NETWORK_RECOVERY_INTERVALS,
    //     );
    //     self.network_factors.insert(store_id, slow_score);
    // }

    // pub fn remove_network_factor(&mut self, store_id: u64) {
    //     self.network_factors.remove(&store_id);
    // }

    #[inline]
    pub fn record_disk(
        &mut self,
        id: u64,
        factor: InspectFactor,
        duration: &UnifiedDuration,
        not_busy: bool,
    ) {
        // For disk factors, we care about the raftstore duration.
        let dur = duration.raftstore_duration.delays_on_disk_io(false);   
        self.factors[factor as usize].record(id, dur, not_busy);
    }

    pub fn record_network(
        &mut self,
        id: u64,
        duration: &UnifiedDuration,
    ) {
        info!(
            "recording slow score: id: {}, store_id: {}, input_duration: {:?}",
            id,
            duration.store_id,
            duration.network_duration
        );
        self.network_factors.lock().unwrap().entry(duration.store_id).or_insert_with(|| {
            SlowScore::new(
                NETWORK_TIMEOUT_THRESHOLD,
                self.inspect_network_interval,
                NETWORK_TIMEOUT_RATIO_THRESHOLD,
                NETWORK_ROUND_TICKS,
                NETWORK_RECOVERY_INTERVALS,
            )
        }).record(id, duration.network_duration.unwrap_or_default(), true);
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
    pub fn get_disk_score(&self) -> f64 {
        (self.factors[InspectFactor::RaftDisk as usize].get() + 
            self.factors[InspectFactor::KvDisk as usize].get()) / 2.0
    }

    pub fn get_network_score(&self) -> HashMap<u64, u64> {
        self.network_factors.lock().unwrap()
            .iter()
            .map(|(k, v)| (*k, v.get() as u64)) 
            .collect()
    }

    pub fn last_tick_finished(&self, factor: InspectFactor) -> bool {
        if factor == InspectFactor::Network {
            // For network factor, we need to check all network factors.
            self.network_factors.lock().unwrap()
                .values()
                .all(|f| f.last_tick_finished())
        } else {
            // For other factors, we just check the specific factor.
            self.factors[factor as usize].last_tick_finished()
        }
    }

    pub fn record_timeout(&mut self, factor: InspectFactor) {
        if factor == InspectFactor::Network {
            // For network factor, we need to tick all network factors.
            let mut network_factors = self.network_factors.lock().unwrap();
            for (_, factor) in network_factors.iter_mut() {
                factor.record_timeout();
            }
        } else {
            // For other factors, we just record the specific factor.
            self.factors[factor as usize].record_timeout();
        }
    }

    pub fn tick(&mut self, factor: InspectFactor) -> SlowScoreTickResult {
        let mut slow_score_tick_result = SlowScoreTickResult::default();
        if factor == InspectFactor::Network {
            // For network factor, we need to tick all network factors and return the avg result.
            slow_score_tick_result.has_new_record = false;
            let mut network_factors = self.network_factors.lock().unwrap();
            let mut total_score = 0.0;
            let mut total_count = 0;
            for (_, factor) in network_factors.iter_mut() {
                let factor_tick_result = factor.tick();
                if factor_tick_result.updated_score.is_some() {
                    slow_score_tick_result.has_new_record = true;
                    total_score += factor_tick_result.updated_score.unwrap_or(0.0);
                    total_count += 1;
                }
                slow_score_tick_result.tick_id = factor_tick_result.tick_id.max(slow_score_tick_result.tick_id);
            }
            if total_count > 0 {
                slow_score_tick_result.updated_score = Some(total_score / total_count as f64);
            }
            slow_score_tick_result.should_force_report_slow_store = network_factors
                .values()
                .any(|factor| factor.get().ge(&100.));
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
    disk_score_reached_100: bool,
    network_score_reached_100: bool,
}

impl RaftstoreReporter {
    const MODULE_NAME: &'static str = "raftstore";

    pub fn new(
        health_controller: &HealthController,
        client_mgr: Arc<Mutex<TikvClientMgr>>,
        cfg: RaftstoreReporterConfig,
    ) -> Self {
        Self {
            health_controller_inner: health_controller.inner.clone(),
            slow_score: UnifiedSlowScore::new(&cfg, client_mgr),
            slow_trend: SlowTrendStatistics::new(cfg),
            is_healthy: true,
            disk_score_reached_100: false,
            network_score_reached_100: false,
        }
    }

    pub fn get_disk_slow_score(&mut self) -> f64 {
        if self.disk_score_reached_100 {
            self.disk_score_reached_100 = false;
            100.0
        } else {
            self.slow_score.get_disk_score()
        }
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
        duration: UnifiedDuration,
        store_not_busy: bool,
    ) {
        // Fine-tuned, `SlowScore` only takes the I/O jitters on the disk into account.
        
        self.slow_trend.record(&duration.raftstore_duration);

        match factor {
            InspectFactor::RaftDisk | InspectFactor::KvDisk => {
                self.slow_score
                    .record_disk(id, factor, &duration, store_not_busy);
                // Publish slow score to health controller
                self.health_controller_inner
                    .update_raftstore_slow_score(self.slow_score.get_disk_score());
            }
            InspectFactor::Network => {
                self.slow_score
                    .record_network(id, &duration);
                // self.health_controller_inner
                //     .update_network_slow_score(self.slow_score.get_network_score());
            }
        }
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
        // last_ticks_finished: The last tick of factor is finished.
        let (healthy, last_ticks_finished) = (
            self.is_healthy(),
            self.slow_score.last_tick_finished(factor),
        );
        // The health status is recovered to serving as long as any tick
        // does not timeout.
        if !healthy && last_ticks_finished {
            self.set_is_healthy(true);
        }
        if !last_ticks_finished {
            // If the tick is not finished, it means that the current store might
            // be busy on handling requests or delayed on I/O operations. And only when
            // the current store is not busy, it should record the last_tick as a timeout.
            if factor == InspectFactor::Network || !store_maybe_busy {
                info!(
                    "raftstore reporter tick: factor {:?} not finished, store_maybe_busy: {}",
                    factor, store_maybe_busy
                );
                self.slow_score.record_timeout(factor);
            }
        }

        let slow_score_tick_result = self.slow_score.tick(factor);
        if slow_score_tick_result.updated_score.is_some() && !slow_score_tick_result.has_new_record
        {
            self.set_is_healthy(false);
        }

        if slow_score_tick_result.should_force_report_slow_store {
            match factor {
                InspectFactor::RaftDisk | InspectFactor::KvDisk => {
                    self.disk_score_reached_100 = true
                }
                InspectFactor::Network => self.network_score_reached_100 = true,
            }
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

#[derive(Clone)]
pub struct TikvClientMgr {
    store_id: u64,
    pd_client: Arc<dyn PdClient>,
    // store_address -> client
    channels: Arc<Mutex<HashMap<u64, Channel>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl TikvClientMgr {
    pub fn new(
        store_id: u64,
        pd_client: Arc<dyn PdClient>,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let mgr = TikvClientMgr {
            store_id,
            pd_client: pd_client.clone(),
            channels: Arc::new(Mutex::new(HashMap::default())),
            env: env.clone(),
            security_mgr: security_mgr.clone(),
        };

        let mgr_clone = mgr.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                loop {
                    mgr_clone.update();
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            });
        });
        mgr
    }

    fn update(&self) {
        let stores = match self.pd_client.get_all_stores(true) {
            Ok(stores) => stores,
            Err(e) => {
                warn!("failed to get stores from PD"; "err" => ?e);
                return;
            }
        };

        let mut channels = self.channels.lock().unwrap();
        let existing_stores: std::collections::HashSet<_> = channels.keys().cloned().collect();

        for store in stores {
            let addr = store.get_address().to_string();
            let store_id = store.get_id();
            if !existing_stores.contains(&store_id) && store_id != self.store_id {
                let channel = self.security_mgr.connect(
                    ChannelBuilder::new(self.env.clone()),
                    &addr,
                );
                channels.insert(store_id, channel);
            }
        }
    }

    pub fn get_health_clients(&self) -> HashMap<u64, HealthClient> {
        let mut health_clients = HashMap::default();

        for (store_id, channel) in self.channels.lock().unwrap().iter() {
            let client = HealthClient::new(
                channel.clone(),
            );
            health_clients.insert(*store_id, client);
        }
        health_clients
    }
}