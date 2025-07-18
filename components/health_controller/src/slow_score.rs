// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::HashSet,
    time::{Duration, Instant},
};

use ordered_float::{Float, OrderedFloat};

/// Interval for updating the slow score.
const UPDATE_INTERVALS: Duration = Duration::from_secs(10);
/// Disk recovery intervals for the disk slow score.
/// If the score has reached 100 and there is no timeout inspecting requests
/// during this interval, the score will go back to 1 after 5min.
pub const DISK_RECOVERY_INTERVALS: Duration = Duration::from_secs(60 * 5);
/// Network recovery intervals for the network slow score.
/// If the score has reached 100 and there is no timeout inspecting requests
/// during this interval, the score will go back to 1 after 30min.
pub const NETWORK_RECOVERY_INTERVALS: Duration = Duration::from_secs(60 * 30);
/// After every DISK_ROUND_TICKS, the disk's slow score will be updated.
pub const DISK_ROUND_TICKS: u64 = 30;
/// After every NETWORK_ROUND_TICKS, the network's slow score will be updated.
pub const NETWORK_ROUND_TICKS: u64 = 3;
/// DISK_TIMEOUT_RATIO_THRESHOLD is the maximal tolerated timeout ratio
/// for disk inspecting requests. If the timeout ratio is larger than this
/// threshold, the disk's slow score will be multiplied by 2.
pub const DISK_TIMEOUT_RATIO_THRESHOLD: f64 = 0.1;
/// NETWORK_TIMEOUT_RATIO_THRESHOLD is the maximal tolerated timeout ratio
/// for network inspecting requests. If the timeout ratio is larger than this
/// threshold, the network's slow score will be multiplied by 2.
pub const NETWORK_TIMEOUT_RATIO_THRESHOLD: f64 = 1.0;
/// The timeout threshold for network inspecting requests.
pub const NETWORK_TIMEOUT_THRESHOLD: Duration = Duration::from_secs(2);
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

    timeout_threshold: Duration,
    // The maximal tolerated timeout ratio.
    ratio_thresh: OrderedFloat<f64>,
    // Minimal time that the score could be decreased from 100 to 1.
    min_ttr: Duration,

    // After how many ticks the value need to be updated.
    round_ticks: u64,
    // Identify every ticks.
    last_tick_id: u64,
    // If the tick_id is less than `last_tick_id`-`min_timeout_ticks`, we view
    // this tick as timeout.
    min_timeout_ticks: u64,
    uncompleted_ticks: HashSet<u64>,
    max_running_ticks: u64,
}

impl SlowScore {
    pub fn new(
        timeout_threshold: Duration,
        inspect_interval: Duration,
        ratio_thresh: f64,
        round_ticks: u64,
        recovery_interval: Duration,
    ) -> SlowScore {
        let min_timeout_ticks = if inspect_interval.is_zero() {
            1  
        } else {
            timeout_threshold.div_duration_f64(inspect_interval).ceil() as u64
        };
        SlowScore {
            value: OrderedFloat(1.0),

            timeout_requests: 0,
            total_requests: 0,

            timeout_threshold,
            ratio_thresh: OrderedFloat(ratio_thresh),
            min_ttr: recovery_interval,
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            round_ticks,
            last_tick_id: 0,
            min_timeout_ticks,
            uncompleted_ticks: HashSet::new(),
            max_running_ticks: 3, // the worker thread count is 3.
        }
    }

    // Only for kvdb.
    pub fn new_with_extra_config(inspect_interval: Duration, timeout_ratio: f64) -> SlowScore {
        SlowScore {
            value: OrderedFloat(1.0),

            timeout_requests: 0,
            total_requests: 0,

            timeout_threshold: inspect_interval,
            ratio_thresh: OrderedFloat(timeout_ratio),
            min_ttr: DISK_RECOVERY_INTERVALS,
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            // The minimal round ticks is 1 for kvdb.
            round_ticks: cmp::max(
                UPDATE_INTERVALS.div_duration_f64(inspect_interval) as u64,
                1_u64,
            ),
            last_tick_id: 0,
            min_timeout_ticks: 1,
            uncompleted_ticks: HashSet::new(),
            max_running_ticks: 3,
        }
    }

    pub fn record(&mut self, id: u64, duration: Duration, not_busy: bool) {
        self.last_record_time = Instant::now();
        if id <= self.last_tick_id.saturating_sub(self.min_timeout_ticks) {
            return;
        }

        self.uncompleted_ticks.remove(&id);
        self.total_requests += 1;
        if not_busy && duration >= self.timeout_threshold {
            self.timeout_requests += 1;
        }
    }

    pub fn record_timeout(&mut self) {
        self.last_record_time = Instant::now();
        let threshold = self.last_tick_id.saturating_sub(self.min_timeout_ticks - 1);
        let timeout_requests = self
            .uncompleted_ticks
            .iter()
            .filter(|&&id| id <= threshold)
            .count();
        self.total_requests += timeout_requests;
        self.timeout_requests += timeout_requests;
        // After recording the timeout requests, we do not need to keep them.
        self.uncompleted_ticks.retain(|&id| id > threshold);
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

    // Check if the last timeout tick is finished. If a tick id is less than
    // `last_tick_id` - `min_timeout_ticks`, we view this tick as timeout.
    pub fn last_tick_finished(&self) -> bool {
        let threshold = self.last_tick_id.saturating_sub(self.min_timeout_ticks - 1);
        let exist_uncompleted_tick = self.uncompleted_ticks.iter().any(|&id| id <= threshold);

        !exist_uncompleted_tick
    }

    pub fn exceed_max_running_ticks(&self) -> bool {
        let threshold = self.last_tick_id.saturating_sub(self.max_running_ticks - 1);
        let exist_uncompleted_tick = self.uncompleted_ticks.iter().any(|&id| id <= threshold);

        exist_uncompleted_tick
    }

    pub fn tick(&mut self) -> SlowScoreTickResult {
        let should_force_report_slow_store = self.should_force_report_slow_store();

        let id = self.last_tick_id + 1;
        self.last_tick_id += 1;
        self.uncompleted_ticks.insert(self.last_tick_id);

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

#[derive(Default)]
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
        let mut slow_score = SlowScore::new(
            Duration::from_millis(500),
            Duration::from_millis(500),
            DISK_TIMEOUT_RATIO_THRESHOLD,
            DISK_ROUND_TICKS,
            DISK_RECOVERY_INTERVALS,
        );
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

    #[test]
    fn test_slow_score_extra() {
        let mut slow_score = SlowScore::new_with_extra_config(Duration::from_millis(1000), 0.6);
        slow_score.timeout_requests = 1;
        slow_score.total_requests = 10;
        let score = slow_score.update_impl(Duration::from_secs(10));
        assert!(score > OrderedFloat(1.16));
        assert!(score < OrderedFloat(1.17));

        slow_score.timeout_requests = 2;
        slow_score.total_requests = 10;
        let score = slow_score.update_impl(Duration::from_secs(10));
        assert!(score > OrderedFloat(1.5));
        assert!(score < OrderedFloat(1.6));

        slow_score.timeout_requests = 0;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(1.0),
            slow_score.update_impl(Duration::from_secs(57))
        );

        slow_score.timeout_requests = 3;
        slow_score.total_requests = 10;
        assert_eq!(
            OrderedFloat(1.5),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 6;
        slow_score.total_requests = 10;
        assert_eq!(
            OrderedFloat(3.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 10;
        slow_score.total_requests = 10;
        assert_eq!(
            OrderedFloat(6.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        // Test too large inspect interval.
        let slow_score = SlowScore::new_with_extra_config(Duration::from_secs(11), 0.1);
        assert_eq!(slow_score.round_ticks, 1);
    }

    #[test]
    fn test_slow_score_uncompleted_ticks() {
        let mut slow_score = SlowScore::new(
            Duration::from_millis(500),
            Duration::from_millis(100),
            DISK_TIMEOUT_RATIO_THRESHOLD,
            6,
            DISK_RECOVERY_INTERVALS,
        );

        // Record some uncompleted ticks.
        for i in 0..=slow_score.min_timeout_ticks {
            slow_score.uncompleted_ticks.insert(i);
        }
        slow_score.last_tick_id = 5;

        // Record a timeout and completed tick.
        slow_score.record(3, Duration::from_millis(600), true);
        assert!(!slow_score.uncompleted_ticks.contains(&3));
        assert_eq!(1, slow_score.timeout_requests);
        assert_eq!(1, slow_score.total_requests);
        assert!(!slow_score.last_tick_finished());

        // Record a non-timeout and completed tick.
        slow_score.record(2, Duration::from_millis(400), true);
        assert!(!slow_score.uncompleted_ticks.contains(&2));
        assert_eq!(1, slow_score.timeout_requests);
        assert_eq!(2, slow_score.total_requests);
        assert!(!slow_score.last_tick_finished());

        // A new tick is coming, the last_tick_id is false. So we should record
        // the timeout requests.
        slow_score.record_timeout();
        assert_eq!(2, slow_score.timeout_requests);
        assert_eq!(3, slow_score.total_requests);
        assert!(slow_score.last_tick_finished());
        assert!(!slow_score.uncompleted_ticks.contains(&0)); // The uncompleted ticks should be removed.

        let result = slow_score.tick();
        assert_eq!(6, result.tick_id);
        assert!(result.has_new_record);
        assert!(result.updated_score.is_some());
    }
}
