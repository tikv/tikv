// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub mod trend;
pub mod types;

use std::{
    cmp,
    time::{Duration, Instant},
};

use ordered_float::OrderedFloat;
use prometheus::IntGauge;
use trend::{RequestPerSecRecorder, Trend};
pub use types::{LatencyInspector, RaftstoreDuration};

pub struct HealthController {}

// Slow score is a value that represents the speed of a store and ranges in [1,
// 100]. It is maintained in the AIMD way.
// If there are some inspecting requests timeout during a round, by default the
// score will be increased at most 1x when above 10% inspecting requests
// timeout. If there is not any timeout inspecting requests, the score will go
// back to 1 in at least 5min.
pub struct SlowScore {
    value: OrderedFloat<f64>,
    last_record_time: Instant,
    last_update_time: Instant,

    timeout_requests: usize,
    total_requests: usize,

    inspect_interval: Duration,
    // The maximal tolerated timeout ratio.
    ratio_thresh: OrderedFloat<f64>,
    // Minimal time that the score could be decreased from 100 to 1.
    min_ttr: Duration,

    // After how many ticks the value need to be updated.
    round_ticks: u64,
    // Identify every ticks.
    last_tick_id: u64,
    // If the last tick does not finished, it would be recorded as a timeout.
    last_tick_finished: bool,
}

impl SlowScore {
    pub fn new(inspect_interval: Duration) -> SlowScore {
        SlowScore {
            value: OrderedFloat(1.0),

            timeout_requests: 0,
            total_requests: 0,

            inspect_interval,
            ratio_thresh: OrderedFloat(0.1),
            min_ttr: Duration::from_secs(5 * 60),
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            round_ticks: 30,
            last_tick_id: 0,
            last_tick_finished: true,
        }
    }

    pub fn record(&mut self, id: u64, duration: Duration, not_busy: bool) {
        self.last_record_time = Instant::now();
        if id != self.last_tick_id {
            return;
        }
        self.last_tick_finished = true;
        self.total_requests += 1;
        if not_busy && duration >= self.inspect_interval {
            self.timeout_requests += 1;
        }
    }

    pub fn record_timeout(&mut self) {
        self.last_tick_finished = true;
        self.total_requests += 1;
        self.timeout_requests += 1;
    }

    pub fn update(&mut self) -> f64 {
        let elapsed = self.last_update_time.elapsed();
        self.update_impl(elapsed).into()
    }

    pub fn get(&self) -> f64 {
        self.value.into()
    }

    // Update the score in a AIMD way.
    fn update_impl(&mut self, elapsed: Duration) -> OrderedFloat<f64> {
        if self.timeout_requests == 0 {
            let desc = 100.0 * (elapsed.as_millis() as f64 / self.min_ttr.as_millis() as f64);
            if OrderedFloat(desc) > self.value - OrderedFloat(1.0) {
                self.value = 1.0.into();
            } else {
                self.value -= desc;
            }
        } else {
            let timeout_ratio = self.timeout_requests as f64 / self.total_requests as f64;
            let near_thresh =
                cmp::min(OrderedFloat(timeout_ratio), self.ratio_thresh) / self.ratio_thresh;
            let value = self.value * (OrderedFloat(1.0) + near_thresh);
            self.value = cmp::min(OrderedFloat(100.0), value);
        }

        self.total_requests = 0;
        self.timeout_requests = 0;
        self.last_update_time = Instant::now();
        self.value
    }

    pub fn should_force_report_slow_store(&self) -> bool {
        self.value >= OrderedFloat(100.0) && (self.last_tick_id % self.round_ticks == 0)
    }

    pub fn get_inspect_interval(&self) -> Duration {
        self.inspect_interval
    }

    pub fn last_tick_finished(&self) -> bool {
        self.last_tick_finished
    }

    pub fn tick(&mut self) -> SlowScoreTickResult {
        let id = self.last_tick_id + 1;
        self.last_tick_id += 1;
        self.last_tick_finished = false;

        let (updated_score, has_new_record) = if self.last_tick_id % self.round_ticks == 0 {
            // `last_update_time` is refreshed every round. If no update happens in a whole
            // round, we set the status to unknown.
            let has_new_record = self.last_record_time >= self.last_update_time;

            let slow_score = self.update();
            (Some(slow_score), has_new_record)
        } else {
            (None, false)
        };

        SlowScoreTickResult {
            tick_id: id,
            updated_score,
            has_new_record,
        }
    }
}

pub struct SlowScoreTickResult {
    pub tick_id: u64,
    // None if skipped in this tick
    pub updated_score: Option<f64>,
    pub has_new_record: bool,
}

pub struct SlowTrendConfig {
    pub unsensitive_cause: f64,
    pub unsensitive_result: f64,
    pub net_io_factor: f64,

    pub cause_spike_filter_value_gauge: IntGauge,
    pub cause_spike_filter_count_gauge: IntGauge,
    pub cause_l1_gap_gauges: IntGauge,
    pub cause_l2_gap_gauges: IntGauge,

    pub result_spike_filter_value_gauge: IntGauge,
    pub result_spike_filter_count_gauge: IntGauge,
    pub result_l1_gap_gauges: IntGauge,
    pub result_l2_gap_gauges: IntGauge,
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
    pub fn new(config: SlowTrendConfig) -> Self {
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
    pub fn record(&mut self, duration: RaftstoreDuration) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_score() {
        let mut slow_score = SlowScore::new(Duration::from_millis(500));
        slow_score.timeout_requests = 5;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(1.5),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 10;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(3.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 20;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(6.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 100;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(12.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 11;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(24.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 0;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(19.0),
            slow_score.update_impl(Duration::from_secs(15))
        );

        slow_score.timeout_requests = 0;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(1.0),
            slow_score.update_impl(Duration::from_secs(57))
        );
    }
}
