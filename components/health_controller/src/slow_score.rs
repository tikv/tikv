// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    time::{Duration, Instant},
};

use ordered_float::OrderedFloat;

const DEFAULT_UPDATE_ROUND_INTERVALS: Duration = Duration::from_secs(10);
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
            min_ttr: DEFAULT_UPDATE_ROUND_INTERVALS.mul_f64(30.0),
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            round_ticks: 30,
            last_tick_id: 0,
            last_tick_finished: true,
        }
    }

    // Only for kvdb.
    pub fn new_with_extra_config(inspect_interval: Duration, timeout_ratio: f64) -> SlowScore {
        SlowScore {
            value: OrderedFloat(1.0),

            timeout_requests: 0,
            total_requests: 0,

            inspect_interval,
            ratio_thresh: OrderedFloat(timeout_ratio),
            min_ttr: DEFAULT_UPDATE_ROUND_INTERVALS.mul_f64(30.0),
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            round_ticks: DEFAULT_UPDATE_ROUND_INTERVALS.div_duration_f64(inspect_interval) as u64,
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
        let should_force_report_slow_store = self.should_force_report_slow_store();

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
            should_force_report_slow_store,
        }
    }
}

pub struct SlowScoreTickResult {
    pub tick_id: u64,
    // None if skipped in this tick
    pub updated_score: Option<f64>,
    pub has_new_record: bool,
    pub should_force_report_slow_store: bool,
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
