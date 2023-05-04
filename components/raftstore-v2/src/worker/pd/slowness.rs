// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};

use engine_traits::{KvEngine, RaftEngine};
use kvproto::pdpb;
use pd_client::PdClient;
use raftstore::store::{metrics::*, util::RaftstoreDuration, Config};
use tikv_util::trend::{RequestPerSecRecorder, Trend};

use super::Runner;

/// The types cause this node slow on processing.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SlowCauseType {
    DiskIo = 0,
    NetworkIo = 1,
    // ...
}

impl SlowCauseType {
    pub fn as_str(&self) -> &str {
        match *self {
            SlowCauseType::DiskIo => "disk-io",
            SlowCauseType::NetworkIo => "network-io",
        }
    }
}

pub struct SlownessStatistics {
    slow_causes: [Trend; 2],
    slow_result: Trend,
    slow_result_recorder: RequestPerSecRecorder,
    last_tick_id: u64,
    last_tick_finished: bool,
}

impl SlownessStatistics {
    #[inline]
    pub fn new(cfg: &Config) -> Self {
        let (io_cause_str, net_cause_str) = (
            SlowCauseType::DiskIo.as_str(),
            SlowCauseType::NetworkIo.as_str(),
        );
        Self {
            slow_causes: [
                // Disk IO jitters detective
                Trend::new(
                    // Disable SpikeFilter for now
                    Duration::from_secs(0),
                    STORE_SLOW_TREND_MISC_GAUGE_VEC
                        .with_label_values(&[io_cause_str, "spike_filter_value"]),
                    STORE_SLOW_TREND_MISC_GAUGE_VEC
                        .with_label_values(&[io_cause_str, "spike_filter_count"]),
                    Duration::from_secs(180),
                    Duration::from_secs(30),
                    Duration::from_secs(120),
                    Duration::from_secs(600),
                    1,
                    tikv_util::time::duration_to_us(Duration::from_micros(500)),
                    STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                        .with_label_values(&[io_cause_str, "L1"]),
                    STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                        .with_label_values(&[io_cause_str, "L2"]),
                    cfg.slow_trend_unsensitive_cause,
                ),
                // Network jitters detective
                Trend::new(
                    // Disable SpikeFilter for now
                    Duration::from_secs(0),
                    STORE_SLOW_TREND_MISC_GAUGE_VEC
                        .with_label_values(&[net_cause_str, "spike_filter_value"]),
                    STORE_SLOW_TREND_MISC_GAUGE_VEC
                        .with_label_values(&[net_cause_str, "spike_filter_count"]),
                    Duration::from_secs(180),
                    Duration::from_secs(30),
                    Duration::from_secs(120),
                    Duration::from_secs(600),
                    1,
                    tikv_util::time::duration_to_us(Duration::from_micros(500)),
                    STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                        .with_label_values(&[net_cause_str, "L1"]),
                    STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                        .with_label_values(&[net_cause_str, "L2"]),
                    cfg.slow_trend_unsensitive_cause,
                ),
            ],
            slow_result: Trend::new(
                // Disable SpikeFilter for now
                Duration::from_secs(0),
                STORE_SLOW_TREND_RESULT_MISC_GAUGE_VEC.with_label_values(&["spike_filter_value"]),
                STORE_SLOW_TREND_RESULT_MISC_GAUGE_VEC.with_label_values(&["spike_filter_count"]),
                Duration::from_secs(120),
                Duration::from_secs(15),
                Duration::from_secs(60),
                Duration::from_secs(300),
                1,
                2000,
                STORE_SLOW_TREND_RESULT_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                    .with_label_values(&["L1"]),
                STORE_SLOW_TREND_RESULT_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                    .with_label_values(&["L2"]),
                cfg.slow_trend_unsensitive_result,
            ),
            slow_result_recorder: RequestPerSecRecorder::new(),
            last_tick_id: 0,
            last_tick_finished: true,
        }
    }

    #[inline]
    fn slow_cause(&self, t: SlowCauseType) -> &Trend {
        &self.slow_causes[t as usize]
    }

    #[inline]
    fn slow_cause_mut(&mut self, t: SlowCauseType) -> &mut Trend {
        &mut self.slow_causes[t as usize]
    }

    #[inline]
    fn slow_result(&self) -> &Trend {
        &self.slow_result
    }

    #[inline]
    fn slow_result_mut(&mut self) -> &mut Trend {
        &mut self.slow_result
    }

    #[inline]
    fn slow_result_recorder_mut(&mut self) -> &mut RequestPerSecRecorder {
        &mut self.slow_result_recorder
    }

    #[inline]
    fn get_cause_value(&self) -> f64 {
        let mut max = std::f64::MIN;
        // TODO: should be optimized, `max` maybe ignore some corner cases.
        for cause_type in [SlowCauseType::DiskIo, SlowCauseType::NetworkIo] {
            max = f64::max(max, self.slow_causes[cause_type as usize].l0_avg());
        }
        max
    }

    #[inline]
    fn get_cause_rate(&self) -> f64 {
        let mut max = std::f64::MIN;
        // TODO: should be optimized, `max` maybe ignore some corner cases.
        for cause_type in [SlowCauseType::DiskIo, SlowCauseType::NetworkIo] {
            max = f64::max(max, self.slow_causes[cause_type as usize].increasing_rate());
        }
        max
    }
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn handle_update_slowness_stats(&mut self, _tick: u64, duration: RaftstoreDuration) {
        self.slowness_stats.last_tick_finished = true;
        // Record IO latency on Disk.
        self.slowness_stats
            .slow_cause_mut(SlowCauseType::DiskIo)
            .record(
                tikv_util::time::duration_to_us(duration.store_wait_duration.unwrap()),
                Instant::now(),
            );
        // Record IO latency on network
        self.slowness_stats
            .slow_cause_mut(SlowCauseType::NetworkIo)
            .record(
                tikv_util::time::duration_to_us(duration.store_commit_duration.unwrap()),
                Instant::now(),
            );
    }

    pub fn handle_slowness_stats_timeout(&mut self) {
        // Record a fairly great value when timeout
        self.slowness_stats
            .slow_cause_mut(SlowCauseType::DiskIo)
            .record(500_000, Instant::now()); // 500ms for DiskIo
        self.slowness_stats
            .slow_cause_mut(SlowCauseType::NetworkIo)
            .record(50_000, Instant::now()); // 50ms for NetworkIo

        if !self.slowness_stats.last_tick_finished && self.is_store_heartbeat_delayed() {
            // If the last slowness tick already reached abnormal state and was delayed for
            // reporting by `store-heartbeat` to PD, we should report it here manually as
            // a FAKE `store-heartbeat`.
            self.handle_fake_store_heartbeat();
        }
        self.slowness_stats.last_tick_id += 1;
        self.slowness_stats.last_tick_finished = false;
    }

    pub fn update_slowness_in_store_stats(&mut self, stats: &mut pdpb::StoreStats, query_num: u64) {
        let mut slow_trend = pdpb::SlowTrend::default();
        // TODO: update the pb of SlowTrend to make it can detect slowness
        // from NetworkIo and DiskIo respectively, and react by the comprehensive
        // results.
        slow_trend.set_cause_rate(self.slowness_stats.get_cause_rate());
        slow_trend.set_cause_value(self.slowness_stats.get_cause_value());
        let total_query_num = self
            .slowness_stats
            .slow_result_recorder_mut()
            .record_and_get_current_rps(query_num, Instant::now());
        if let Some(total_query_num) = total_query_num {
            self.slowness_stats
                .slow_result_mut()
                .record(total_query_num as u64, Instant::now());
            slow_trend.set_result_value(self.slowness_stats.slow_result().l0_avg());
            let slow_trend_result_rate = self.slowness_stats.slow_result().increasing_rate();
            slow_trend.set_result_rate(slow_trend_result_rate);
            STORE_SLOW_TREND_RESULT_GAUGE.set(slow_trend_result_rate);
            STORE_SLOW_TREND_RESULT_VALUE_GAUGE.set(total_query_num);
        } else {
            // Just to mark the invalid range on the graphic
            STORE_SLOW_TREND_RESULT_VALUE_GAUGE.set(-100.0);
        }
        stats.set_slow_trend(slow_trend);
        self.flush_slowness_metrics();
    }

    fn flush_slowness_metrics(&mut self) {
        for t in [SlowCauseType::DiskIo, SlowCauseType::NetworkIo] {
            let label = [t.as_str()];
            let slow_cause = self.slowness_stats.slow_cause(t);
            STORE_SLOW_TREND_GAUGE
                .with_label_values(&label)
                .set(slow_cause.increasing_rate());
            STORE_SLOW_TREND_L0_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l0_avg());
            STORE_SLOW_TREND_L1_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l1_avg());
            STORE_SLOW_TREND_L2_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l2_avg());
            STORE_SLOW_TREND_L0_L1_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l0_l1_rate());
            STORE_SLOW_TREND_L1_L2_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l1_l2_rate());
            STORE_SLOW_TREND_L1_MARGIN_ERROR_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l1_margin_error_base());
            STORE_SLOW_TREND_L2_MARGIN_ERROR_GAUGE
                .with_label_values(&label)
                .set(slow_cause.l2_margin_error_base());
        }
        // Report results of all slow Trends.
        let slow_result = self.slowness_stats.slow_result();
        STORE_SLOW_TREND_RESULT_L0_GAUGE.set(slow_result.l0_avg());
        STORE_SLOW_TREND_RESULT_L1_GAUGE.set(slow_result.l1_avg());
        STORE_SLOW_TREND_RESULT_L2_GAUGE.set(slow_result.l2_avg());
        STORE_SLOW_TREND_RESULT_L0_L1_GAUGE.set(slow_result.l0_l1_rate());
        STORE_SLOW_TREND_RESULT_L1_L2_GAUGE.set(slow_result.l1_l2_rate());
        STORE_SLOW_TREND_RESULT_L1_MARGIN_ERROR_GAUGE.set(slow_result.l1_margin_error_base());
        STORE_SLOW_TREND_RESULT_L2_MARGIN_ERROR_GAUGE.set(slow_result.l2_margin_error_base());
    }
}
