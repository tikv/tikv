// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use health_controller::{
    trend::{RequestPerSecRecorder, Trend},
    types::RaftstoreDuration,
};
use kvproto::pdpb;
use pd_client::PdClient;
use raftstore::store::{metrics::*, Config};

use super::Runner;
pub struct SlownessStatistics {
    /// Detector to detect NetIo&DiskIo jitters.
    slow_cause: Trend,
    /// Reactor as an assistant detector to detect the QPS jitters.
    slow_result: Trend,
    slow_result_recorder: RequestPerSecRecorder,
    last_tick_finished: bool,
}

impl SlownessStatistics {
    #[inline]
    pub fn new(cfg: &Config) -> Self {
        Self {
            slow_cause: Trend::new(
                // Disable SpikeFilter for now
                Duration::from_secs(0),
                STORE_SLOW_TREND_MISC_GAUGE_VEC.with_label_values(&["spike_filter_value"]),
                STORE_SLOW_TREND_MISC_GAUGE_VEC.with_label_values(&["spike_filter_count"]),
                Duration::from_secs(180),
                Duration::from_secs(30),
                Duration::from_secs(120),
                Duration::from_secs(600),
                1,
                tikv_util::time::duration_to_us(Duration::from_micros(500)),
                STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC.with_label_values(&["L1"]),
                STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC.with_label_values(&["L2"]),
                cfg.slow_trend_unsensitive_cause,
            ),
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
            last_tick_finished: true,
        }
    }
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    /// Record slowness periodically.
    pub fn handle_update_slowness_stats(&mut self, _tick: u64, duration: RaftstoreDuration) {
        self.slowness_stats.last_tick_finished = true;
        // TODO: It's more appropriate to divide the factor into `Disk IO factor` and
        // `Net IO factor`.
        // Currently, to make the detection and judgement of Slowness of V2 compactible
        // to V1, it summarizes all factors by `sum` simplily, approved valid to common
        // cases when there exists IO jitters on Network or Disk.
        self.slowness_stats.slow_cause.record(
            tikv_util::time::duration_to_us(duration.sum()),
            Instant::now(),
        );
    }

    pub fn handle_slowness_stats_tick(&mut self) {
        let mock_slowness_last_tick_unfinished = || {
            fail_point!("mock_slowness_last_tick_unfinished", |_| { true });
            false
        };
        // Handle timeout if the last tick is not finished as expected.
        if mock_slowness_last_tick_unfinished() || !self.slowness_stats.last_tick_finished {
            // Record a sufficiently large interval to indicate potential write progress
            // hanging on I/O. We use the store heartbeat interval as the default value.
            self.slowness_stats.slow_cause.record(
                self.store_heartbeat_interval.as_micros() as u64,
                Instant::now(),
            );

            // If the last slowness tick already reached an abnormal state and was delayed
            // for reporting by `store-heartbeat` to PD, we should manually report it here
            // as a FAKE `store-heartbeat`. This ensures that the heartbeat to PD is not
            // lost. Normally, this case rarely happens in raftstore-v2.
            if self.is_store_heartbeat_delayed() {
                self.handle_fake_store_heartbeat();
            }
        } else {
            // The following code records a periodic "white noise", which helps mitigate any
            // minor fluctuations in disk I/O or network I/O latency. After
            // extensive e2e testing, a duration of "100ms" has been determined
            // to be the most suitable choice.
            self.slowness_stats
                .slow_cause
                .record(100_000, Instant::now()); // 100ms
        }
        // Move to the next tick.
        self.slowness_stats.last_tick_finished = false;
    }

    pub fn update_slowness_in_store_stats(&mut self, stats: &mut pdpb::StoreStats, query_num: u64) {
        let mut slow_trend = pdpb::SlowTrend::default();
        // TODO: update the parameters of SlowTrend to make it can detect slowness
        // in corner cases.
        slow_trend.set_cause_rate(self.slowness_stats.slow_cause.increasing_rate());
        slow_trend.set_cause_value(self.slowness_stats.slow_cause.l0_avg());
        let total_query_num = self
            .slowness_stats
            .slow_result_recorder
            .record_and_get_current_rps(query_num, Instant::now());
        if let Some(total_query_num) = total_query_num {
            self.slowness_stats
                .slow_result
                .record(total_query_num as u64, Instant::now());
            slow_trend.set_result_value(self.slowness_stats.slow_result.l0_avg());
            let slow_trend_result_rate = self.slowness_stats.slow_result.increasing_rate();
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
        // Report slowness of Trend.
        STORE_SLOW_TREND_GAUGE.set(self.slowness_stats.slow_cause.increasing_rate());
        STORE_SLOW_TREND_L0_GAUGE.set(self.slowness_stats.slow_cause.l0_avg());
        STORE_SLOW_TREND_L1_GAUGE.set(self.slowness_stats.slow_cause.l1_avg());
        STORE_SLOW_TREND_L2_GAUGE.set(self.slowness_stats.slow_cause.l2_avg());
        STORE_SLOW_TREND_L0_L1_GAUGE.set(self.slowness_stats.slow_cause.l0_l1_rate());
        STORE_SLOW_TREND_L1_L2_GAUGE.set(self.slowness_stats.slow_cause.l1_l2_rate());
        STORE_SLOW_TREND_L1_MARGIN_ERROR_GAUGE
            .set(self.slowness_stats.slow_cause.l1_margin_error_base());
        STORE_SLOW_TREND_L2_MARGIN_ERROR_GAUGE
            .set(self.slowness_stats.slow_cause.l2_margin_error_base());
        // Report result of Trend.
        STORE_SLOW_TREND_RESULT_L0_GAUGE.set(self.slowness_stats.slow_result.l0_avg());
        STORE_SLOW_TREND_RESULT_L1_GAUGE.set(self.slowness_stats.slow_result.l1_avg());
        STORE_SLOW_TREND_RESULT_L2_GAUGE.set(self.slowness_stats.slow_result.l2_avg());
        STORE_SLOW_TREND_RESULT_L0_L1_GAUGE.set(self.slowness_stats.slow_result.l0_l1_rate());
        STORE_SLOW_TREND_RESULT_L1_L2_GAUGE.set(self.slowness_stats.slow_result.l1_l2_rate());
        STORE_SLOW_TREND_RESULT_L1_MARGIN_ERROR_GAUGE
            .set(self.slowness_stats.slow_result.l1_margin_error_base());
        STORE_SLOW_TREND_RESULT_L2_MARGIN_ERROR_GAUGE
            .set(self.slowness_stats.slow_result.l2_margin_error_base());
    }
}
