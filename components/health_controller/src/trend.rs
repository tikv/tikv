// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::vec_deque::VecDeque,
    time::{Duration, Instant},
};

use prometheus::IntGauge;
use tikv_util::info;

pub struct SampleValue {
    value: u64,
    time: Instant,
}

pub struct SampleWindow {
    sum: u64,
    values: VecDeque<SampleValue>,
    duration: Duration,
    overflow: bool,
}

impl SampleWindow {
    pub fn new(duration: Duration) -> Self {
        Self {
            sum: 0,
            values: VecDeque::new(),
            duration,
            overflow: false,
        }
    }

    #[inline]
    pub fn record(&mut self, value: u64, now: Instant) {
        self.values.push_back(SampleValue { value, time: now });
        self.sum = self.sum.saturating_add(value);
        while !self.values.is_empty()
            && now.duration_since(self.values.front().unwrap().time) > self.duration
        {
            let front = self.values.pop_front().unwrap();
            self.sum = self.sum.saturating_sub(front.value);
            self.overflow = true;
        }
    }

    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.overflow
    }

    #[inline]
    pub fn drain(&mut self) -> (VecDeque<SampleValue>, u64, bool) {
        let result = (
            self.values.drain(..).collect::<VecDeque<_>>(),
            self.sum,
            self.overflow,
        );
        self.sum = 0;
        self.overflow = false;
        result
    }

    #[inline]
    // TODO: better memory operating?
    pub fn move_from(&mut self, source: &mut Self) {
        (self.values, self.sum, self.overflow) = source.drain();
    }

    #[inline]
    pub fn valid(&self) -> bool {
        !self.values.is_empty()
    }

    #[inline]
    pub fn avg(&self) -> f64 {
        if !self.values.is_empty() {
            self.sum as f64 / self.values.len() as f64
        } else {
            0.0
        }
    }

    #[inline]
    pub fn std_ev(&self) -> f64 {
        if self.values.len() <= 1 {
            return 0.0;
        }
        let avg = self.avg();
        let mut delta_sq_sum = 0.0;
        for v in self.values.iter() {
            let delta = (v.value as f64) - avg;
            delta_sq_sum += delta * delta;
        }
        // We use `self.values.len()` rather than `self.values.len() - 1`
        f64::sqrt(delta_sq_sum / self.values.len() as f64)
    }

    #[inline]
    pub fn std_ev_ratio(&self) -> f64 {
        if self.values.len() <= 1 {
            0.0
        } else {
            self.std_ev() / self.avg()
        }
    }
}

pub struct SampleWindows {
    pub windows: Vec<SampleWindow>,
}

impl SampleWindows {
    pub fn new(windows_durations: Vec<Duration>) -> Self {
        let mut windows = vec![];
        for duration in windows_durations.iter() {
            windows.push(SampleWindow::new(*duration));
        }
        Self { windows }
    }

    #[inline]
    pub fn record(&mut self, value: u64, now: Instant) {
        self.windows
            .iter_mut()
            .for_each(|window| window.record(value, now))
    }

    #[inline]
    pub fn valid(&self) -> bool {
        for window in self.windows.iter() {
            if !window.valid() {
                return false;
            }
        }
        true
    }
}

// TODO: Generalize this module using SPOT(https://dl.acm.org/doi/10.1145/3097983.3098144)
//
// Without SPOT:
//   - Margin errors calculating is based on sampling
//   - `flip_margin_error_multiple` controls when to flip, hence control what to
//     sample
//   - `flip_margin_error_multiple` is a fixed value, can't fit all cases
//
// With SPOT:
//   - `enter_threshold_multiple` will be insteaded of by `risk`
//   - `risk` also a fixed value, but it's based on distribution, so it could
//     fits all
struct HistoryWindow {
    name: &'static str,
    window_duration: Duration,
    sample_interval_duration: Duration,
    current_window: SampleWindow,
    previous_window: SampleWindow,
    last_sampled_time: Instant,
    last_flipped_time: Instant,
    flipping_start_time: Option<Instant>,
    margin_error_base: f64,
    flip_margin_error_multiple: f64,
    gap_gauge: IntGauge,
}

impl HistoryWindow {
    pub fn new(
        name: &'static str,
        window_duration: Duration,
        sample_interval_duration: Duration,
        margin_error_base: f64,
        gap_gauge: IntGauge,
        flip_margin_error_multiple: f64,
    ) -> Self {
        let now = Instant::now();
        Self {
            name,
            window_duration,
            sample_interval_duration,
            current_window: SampleWindow::new(window_duration),
            previous_window: SampleWindow::new(window_duration),
            last_sampled_time: now,
            last_flipped_time: now,
            flipping_start_time: None,
            margin_error_base,
            gap_gauge,
            flip_margin_error_multiple,
        }
    }

    #[inline]
    pub fn record(&mut self, value: f64, now: Instant, increasing_rate: f64) {
        let gap_secs = if self.current_window.is_overflow() {
            now.saturating_duration_since(self.current_window.values.front().unwrap().time)
                .as_secs() as i64
        } else if self.previous_window.is_overflow() {
            now.saturating_duration_since(self.previous_window.values.front().unwrap().time)
                .as_secs() as i64
        } else {
            // Just to mark the invalid range on the graphic
            -100
        };
        self.gap_gauge.set(gap_secs);

        if now.duration_since(self.last_sampled_time) <= self.sample_interval_duration {
            return;
        }
        let should_skip = self.try_flip(value, now, increasing_rate);
        if should_skip {
            return;
        }
        self.current_window.record(value as u64, now);
        self.last_sampled_time = now;
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.current_window.is_overflow() || self.previous_window.is_overflow()
    }

    #[inline]
    pub fn margin_error(&self) -> f64 {
        let margin_error = if self.flipping_start_time.is_none() {
            if self.current_window.is_overflow() {
                self.current_window.std_ev()
            } else if self.previous_window.is_overflow() {
                // We use the previous margin error in the duration:
                //    - After flipping ends
                //    - Yet before current window is overflow
                self.previous_window.std_ev()
            } else {
                0.0
            }
        } else if self.previous_window.is_overflow() {
            self.previous_window.std_ev()
        } else {
            0.0
        };
        f64::max(margin_error, self.margin_error_base)
    }

    #[inline]
    // Return bool: shoud_skip_current_value
    fn try_flip(&mut self, value: f64, now: Instant, increasing_rate: f64) -> bool {
        if !self.current_window.is_overflow() {
            return false;
        }
        let current_avg = self.current_window.avg();
        let margin_error = self.margin_error();

        // The output margin_error multiple can up to `self.flip_margin_error_multiple +
        // 1`  without flipping (increasing_rate already minus a margin_error)
        let flip_margin_error = margin_error * self.flip_margin_error_multiple;
        let delta = f64::abs(value - current_avg);

        // Strict condition for exiting flipping (to do actual flipping)
        if self.flipping_start_time.is_some() {
            // Make sure not stuck at flipping phase by using `time_based_multiple`,
            // increase by time
            //   - Expectation of time_based_multiple: starts at 0.0, to `margin_error * 5%`
            //     at 4min, to 10% at 12min, to 20% at 28min
            //   - f64::abs() is for preventing crash in case the server time is adjusted
            let flipping_duration = now.duration_since(self.flipping_start_time.unwrap());
            let time_based_multiple =
                (f64::abs(flipping_duration.as_secs() as f64) / 240.0 + 1.0).log2() / 20.0;
            if f64::abs(increasing_rate) > margin_error * time_based_multiple {
                // Keep flipping, skip the huge-changing phase, wait for stable
                return true;
            } else {
                // The huge-changing phase ends, do flipping
                self.flip();
                self.flipping_start_time = None;
                self.last_flipped_time = now;
                info!(
                    "history window flipping: end";
                    "name" => self.name,
                    "delta" => delta,
                    "flip_margin_error" => margin_error,
                    "time_based_multiple" => time_based_multiple,
                    "increasing_rate" => increasing_rate,
                    "flipping_duration" => flipping_duration.as_secs(),
                );
                return false;
            }
        }

        // Loose condition for entering flipping
        if now.duration_since(self.last_flipped_time) > self.window_duration
            && delta > flip_margin_error
        {
            // Enter flipping phase, may last for a while
            self.flipping_start_time = Some(Instant::now());
            info!(
                "history window flipping: enter";
                "name" => self.name,
                "delta" => delta,
                "flip_margin_error" => flip_margin_error,
                "increasing_rate" => increasing_rate,
            );
        }
        false
    }

    #[inline]
    fn flip(&mut self) {
        self.previous_window.move_from(&mut self.current_window);
    }
}

// TODO: Generalize this filter using SPOT(https://dl.acm.org/doi/10.1145/3097983.3098144)
//   - `enter_threshold_multiple` is a fixed value, can't fit all cases
//   - Using SPOT, `enter_threshold_multiple` will be insteaded of by `risk`
//   - `risk` also a fixed value, but it's based on distribution, so it could
//     fits all
pub struct SpikeFilter {
    values: VecDeque<SampleValue>,
    duration: Duration,
    filter_value_gauge: IntGauge,
    filter_count_gauge: IntGauge,
    exit_threshold_avg_multiple: f64,
    exit_threshold_margin_error_multiple: f64,
    enter_threshold_multiple: f64,
}

impl SpikeFilter {
    pub fn new(
        duration: Duration,
        filter_value_gauge: IntGauge,
        filter_count_gauge: IntGauge,
        exit_threshold_avg_multiple: f64,
        exit_threshold_margin_error_multiple: f64,
        enter_threshold_multiple: f64,
    ) -> Self {
        assert!(enter_threshold_multiple > 1.0);
        Self {
            values: VecDeque::new(),
            duration,
            filter_value_gauge,
            filter_count_gauge,
            exit_threshold_avg_multiple,
            exit_threshold_margin_error_multiple,
            enter_threshold_multiple,
        }
    }

    #[inline]
    // TODO: better memory operating?
    pub fn record(
        &mut self,
        value: u64,
        now: Instant,
        history_avg: f64,
        history_margin_error: f64,
    ) -> Option<Vec<SampleValue>> {
        let exit_threshold = history_avg * self.exit_threshold_avg_multiple
            + history_margin_error * self.exit_threshold_margin_error_multiple;
        let enter_threshold = exit_threshold * self.enter_threshold_multiple;
        let curr = SampleValue { value, time: now };

        // Spike entering check
        if (value as f64) > enter_threshold {
            // Hold the very high values in the checking sequence
            self.values.push_back(curr);
            if now.duration_since(self.values.front().unwrap().time) > self.duration {
                // The checking sequence is too long to be a spike, dump all and exit checking
                let values: Vec<SampleValue> = self.values.drain(..).collect();
                return Some(values);
            }
            // The curr value is on hold, return None
            return None;
        }

        // Not in a spike, nothing happen
        if self.values.is_empty() {
            return Some(vec![curr]);
        }

        // In a spike

        // Spike ending check
        if (value as f64) < exit_threshold {
            if self.values.len() <= 2 {
                // The checking sequence is too short to be a spike, dump all and exit checking
                let mut values: Vec<SampleValue> = self.values.drain(..).collect();
                values.push(curr);
                return Some(values);
            }
            // The checking sequence is not long enough to be regular high, it's a spike,
            // discard all but return curr
            self.filter_value_gauge.set(self.avg() as i64);
            self.filter_count_gauge.inc();
            self.values.drain(..);
            return Some(vec![curr]);
        }

        // Hold curr value to this spike
        self.values.push_back(curr);
        None
    }

    #[inline]
    fn avg(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let mut sum: f64 = 0.0;
        for value in self.values.iter() {
            sum += value.value as f64;
        }
        sum / (self.values.len() as f64)
    }
}

// Responsibilities of each window:
//
// L0:
//     Eleminate very short time jitter,
//     Consider its avg value as a point in data flow
// L1:
//     `L0.avg/L1.avg` to trigger slow-event, not last long but high sensitive
//     Sensitive could be tuned by `L0.duration` and `L1.duration`
//     Include periodic fluctuations, so it's avg could be seen as baseline
// value     Its duration is also the no-detectable duration after TiKV starting
// L2:
//     `L1.avg/L2.avg` to trigger slow-event, last long but low sensitive
//     Sensitive could be tuned by `L1.duration` and `L2.duration`
//
// L* History:
//     Sample history values and calculate the margin error
//
// Spike Filter:
//     Erase very high and short time spike-values
//
pub struct Trend {
    sample_interval: usize,
    sample_sequence_id: usize,

    spike_filter: SpikeFilter,
    spike_filter_enabled: bool,

    data_flow: SampleWindows,

    l1_history: HistoryWindow,
    l2_history: HistoryWindow,

    // When SPOT is being used, these should be `risk multiple`
    l1_margin_error_multiple: f64,
    l2_margin_error_multiple: f64,

    curves_composer: CurvesComposer,
}

impl Trend {
    pub fn new(
        spike_filter_duration: Duration,
        spike_filter_value_gauge: IntGauge,
        spike_filter_count_gauge: IntGauge,
        history_duration: Duration,
        l0_duration: Duration,
        l1_duration: Duration,
        l2_duration: Duration,
        sample_interval: usize,
        tolerable_margin_error_value: u64,
        l1_gap_gauge: IntGauge,
        l2_gap_gauge: IntGauge,
        unsensitive_multiple: f64,
    ) -> Self {
        let margin_error_base = tolerable_margin_error_value as f64;
        Self {
            sample_interval,
            sample_sequence_id: 0,
            data_flow: SampleWindows::new(vec![l0_duration, l1_duration, l2_duration]),
            spike_filter_enabled: !spike_filter_duration.is_zero(),
            spike_filter: SpikeFilter::new(
                spike_filter_duration,
                spike_filter_value_gauge,
                spike_filter_count_gauge,
                1.0,
                5.0,
                2.0,
            ),
            l1_history: HistoryWindow::new(
                "L1",
                history_duration,
                Duration::from_secs(1),
                margin_error_base,
                l1_gap_gauge,
                3.0,
            ),
            l2_history: HistoryWindow::new(
                "L2",
                history_duration,
                Duration::from_secs(1),
                margin_error_base,
                l2_gap_gauge,
                2.0,
            ),
            l1_margin_error_multiple: 3.0 * unsensitive_multiple,
            l2_margin_error_multiple: 2.0 * unsensitive_multiple,
            curves_composer: CurvesComposer::new(l0_duration, l1_duration, l2_duration, 2.0),
        }
    }

    #[inline]
    pub fn record(&mut self, value: u64, now: Instant) {
        if !self.check_should_sample() {
            return;
        }
        if !self.spike_filter_enabled || !self.data_flow.windows[1].is_overflow() {
            self.record_unfiltered(value, now);
            return;
        }
        if let Some(filtered) =
            self.spike_filter
                .record(value, now, self.l1_avg(), self.l1_margin_error())
        {
            for sample in filtered.iter() {
                self.record_unfiltered(sample.value, sample.time)
            }
        }
    }

    #[inline]
    pub fn increasing_rate(&self) -> f64 {
        self.curves_composer
            .compose(self.l0_l1_rate(), self.l1_l2_rate())
    }

    #[inline]
    pub fn l0_avg(&self) -> f64 {
        self.data_flow.windows[0].avg()
    }

    #[inline]
    pub fn l1_avg(&self) -> f64 {
        self.data_flow.windows[1].avg()
    }

    #[inline]
    pub fn l2_avg(&self) -> f64 {
        self.data_flow.windows[2].avg()
    }

    #[inline]
    pub fn l1_margin_error_base(&self) -> f64 {
        self.l1_history.margin_error()
    }

    #[inline]
    pub fn l2_margin_error_base(&self) -> f64 {
        self.l2_history.margin_error()
    }

    #[inline]
    pub fn l0_l1_rate(&self) -> f64 {
        if !self.data_flow.windows[2].is_overflow() {
            return 0.0;
        }
        if !self.l1_history.valid() {
            return 0.0;
        }
        let l1_avg = self.l1_avg();
        Trend::la_lb_rate(self.l0_avg(), l1_avg, self.l1_margin_error())
    }

    #[inline]
    pub fn l1_l2_rate(&self) -> f64 {
        if !self.data_flow.windows[2].is_overflow() {
            return 0.0;
        }
        if !self.l2_history.valid() {
            return 0.0;
        }
        Trend::la_lb_rate(self.l1_avg(), self.l2_avg(), self.l2_margin_error())
    }

    #[inline]
    fn check_should_sample(&mut self) -> bool {
        if self.sample_interval <= 1 {
            return true;
        }
        let should = self.sample_sequence_id % self.sample_interval == 0;
        self.sample_sequence_id += 1;
        should
    }

    #[inline]
    fn record_unfiltered(&mut self, value: u64, now: Instant) {
        self.data_flow.record(value, now);
        // TODO: Reduce the `increasing_rate()` calculating count?
        let increasing_rate = self.increasing_rate();
        self.l1_history.record(self.l0_avg(), now, increasing_rate);
        self.l2_history.record(self.l1_avg(), now, increasing_rate);
    }

    #[inline]
    fn l1_margin_error(&self) -> f64 {
        self.l1_history.margin_error() * self.l1_margin_error_multiple
    }

    #[inline]
    fn l2_margin_error(&self) -> f64 {
        self.l2_history.margin_error() * self.l2_margin_error_multiple
    }

    #[inline]
    fn la_lb_rate(la_avg: f64, lb_avg: f64, margin_error: f64) -> f64 {
        if lb_avg < f64::EPSILON {
            return 0.0;
        }
        let mut increased = la_avg - lb_avg;
        if f64::abs(increased) < f64::EPSILON {
            return 0.0;
        }
        increased = if la_avg < lb_avg {
            if -increased > margin_error {
                -increased - margin_error
            } else {
                0.0
            }
        } else if increased > margin_error {
            increased - margin_error
        } else {
            0.0
        };
        let mut inc_sq = increased * increased;
        if la_avg < lb_avg {
            inc_sq = -inc_sq;
        };
        let res = la_avg * inc_sq / f64::sqrt(lb_avg);
        if la_avg >= lb_avg {
            f64::sqrt(res)
        } else {
            -f64::sqrt(-res)
        }
    }
}

struct CurvesComposer {
    l0_l1_vs_l1_l2: f64,
}

impl CurvesComposer {
    pub fn new(
        l0_duration: Duration,
        l1_duration: Duration,
        l2_duration: Duration,
        l1_l2_extra_weight: f64,
    ) -> Self {
        let l0_l1 = l0_duration.as_nanos() as f64 / l1_duration.as_nanos() as f64;
        let l1_l2 = l1_duration.as_nanos() as f64 / l2_duration.as_nanos() as f64;
        Self {
            l0_l1_vs_l1_l2: l1_l2_extra_weight * l0_l1 / l1_l2,
        }
    }

    #[inline]
    pub fn compose(&self, l0_l1_rate: f64, l1_l2_rate: f64) -> f64 {
        l0_l1_rate + l1_l2_rate * self.l0_l1_vs_l1_l2
    }
}

pub struct RequestPerSecRecorder {
    previous_ts: Instant,
    initialized: bool,
}

impl Default for RequestPerSecRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestPerSecRecorder {
    pub fn new() -> Self {
        Self {
            previous_ts: Instant::now(),
            initialized: false,
        }
    }

    #[inline]
    pub fn record_and_get_current_rps(
        &mut self,
        observed_request_count: u64,
        now: Instant,
    ) -> Option<f64> {
        if !self.initialized {
            self.initialized = true;
            self.previous_ts = now;
            None
        } else {
            self.initialized = true;
            let secs = now.saturating_duration_since(self.previous_ts).as_secs();
            self.previous_ts = now;
            if secs == 0 {
                None
            } else {
                Some(observed_request_count as f64 / secs as f64)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn test_sample_window() {
        let now = Instant::now();
        let mut window = SampleWindow::new(Duration::from_secs(4));
        assert_eq!(window.valid(), false);
        assert_eq!(window.avg(), 0.0);
        assert_eq!(window.std_ev_ratio(), 0.0);
        window.record(10, now);
        assert_eq!(window.valid(), true);
        assert_eq!(window.avg(), 10.0);
        assert_eq!(window.overflow, false);
        assert_eq!(window.std_ev_ratio(), 0.0);
        window.record(20, now + Duration::from_secs(1));
        assert_eq!(window.avg(), (10.0 + 20.0) / 2.0);
        assert_eq!(window.overflow, false);
        assert_eq!(window.std_ev_ratio(), 5.0 / 15.0);
        window.record(30, now + Duration::from_secs(2));
        assert_eq!(window.avg(), (10.0 + 20.0 + 30.0) / 3.0);
        assert_eq!(window.overflow, false);
        assert_eq!(window.std_ev_ratio(), f64::sqrt(200.0 / 3.0) / 20.0);
        window.record(40, now + Duration::from_secs(5));
        assert_eq!(window.avg(), (20.0 + 30.0 + 40.0) / 3.0);
        assert_eq!(window.overflow, true);
        assert_eq!(window.std_ev_ratio(), f64::sqrt(200.0 / 3.0) / 30.0);
    }
}
