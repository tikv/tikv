// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use num_traits::{AsPrimitive, FromPrimitive};

use crate::time::Instant;

#[derive(PartialEq, Debug)]
pub enum Trend {
    Increasing,
    Decreasing,
    NoTrend,
}

pub const SMOOTHER_STALE_RECORD_THRESHOLD: u64 = 300; // 5min
pub const SMOOTHER_TIME_RANGE_THRESHOLD: u64 = 60; // 1min

// Smoother is a sliding window used to provide steadier flow statistics.
pub struct Smoother<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    records: VecDeque<(T, Instant)>,
    total: T,
}

impl<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64> Default
    for Smoother<T, CAP, STALE_DUR, MIN_TIME_SPAN>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    fn default() -> Self {
        Self {
            records: VecDeque::with_capacity(CAP),
            total: Default::default(),
        }
    }
}

impl<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64>
    Smoother<T, CAP, STALE_DUR, MIN_TIME_SPAN>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    pub fn observe(&mut self, record: T) {
        self.observe_with_time(record, Instant::now_coarse());
    }

    pub fn observe_with_time(&mut self, record: T, time: Instant) {
        if self.records.len() == CAP {
            let v = self.records.pop_front().unwrap().0;
            self.total -= v;
        }

        self.total += record;

        self.records.push_back((record, time));
        self.remove_stale_records();
    }

    fn remove_stale_records(&mut self) {
        // make sure there are two records left at least
        while self.records.len() > 2 {
            if self.records.front().unwrap().1.saturating_elapsed_secs() > STALE_DUR as f64 {
                let v = self.records.pop_front().unwrap().0;
                self.total -= v;
            } else {
                break;
            }
        }
    }

    pub fn get_recent(&self) -> T {
        if self.records.is_empty() {
            return T::default();
        }
        self.records.back().unwrap().0
    }

    pub fn get_avg(&self) -> f64 {
        if self.records.is_empty() {
            return 0.0;
        }
        self.total.as_() / self.records.len() as f64
    }

    pub fn get_max(&self) -> T {
        if self.records.is_empty() {
            return T::default();
        }
        self.records
            .iter()
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
            .unwrap()
            .0
    }

    pub fn get_percentile_90(&mut self) -> T {
        if self.records.is_empty() {
            return FromPrimitive::from_u64(0).unwrap();
        }
        let mut v: Vec<_> = self.records.iter().collect();
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        v[((self.records.len() - 1) as f64 * 0.90) as usize].0
    }

    pub fn trend(&self) -> Trend {
        if self.records.len() <= 1 {
            return Trend::NoTrend;
        }

        // If the lastest record is too old, no trend
        if self.records.back().unwrap().1.saturating_elapsed_secs() > STALE_DUR as f64 {
            return Trend::NoTrend;
        }

        let (mut left, mut left_cnt) = (T::default(), 0);
        let (mut right, mut right_cnt) = (T::default(), 0);

        // The time span matters
        if MIN_TIME_SPAN > 0 {
            // If the records doesn't cover a enough time span, no trend
            let time_span = self.records.front().unwrap().1.saturating_elapsed_secs()
                - self.records.back().unwrap().1.saturating_elapsed_secs();
            if time_span < MIN_TIME_SPAN as f64 {
                return Trend::NoTrend;
            }

            // Split the record into left and right by the middle of time range
            for r in self.records.iter() {
                let elapsed_secs = r.1.saturating_elapsed_secs();
                if elapsed_secs > time_span / 2.0 {
                    left += r.0;
                    left_cnt += 1;
                } else {
                    right += r.0;
                    right_cnt += 1;
                }
            }
        } else {
            let half = self.records.len() / 2;
            for (i, r) in self.records.iter().enumerate() {
                if i < half {
                    left += r.0;
                    left_cnt += 1;
                } else {
                    right += r.0;
                    right_cnt += 1;
                }
            }
        }

        // Decide if there is a trend by the two averages.
        // Adding 2 here is to give a tolerance
        let (l_avg, r_avg) = (left.as_() / left_cnt as f64, right.as_() / right_cnt as f64);
        if r_avg > l_avg + 2.0 {
            return Trend::Increasing;
        }
        if l_avg > r_avg + 2.0 {
            return Trend::Decreasing;
        }

        Trend::NoTrend
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{ops::Sub, time::Duration};

    use crate::{
        smoother::{
            Smoother, Trend, SMOOTHER_STALE_RECORD_THRESHOLD, SMOOTHER_TIME_RANGE_THRESHOLD,
        },
        time::Instant,
    };

    #[test]
    fn test_smoother() {
        let mut smoother = Smoother::<u64, 5, SMOOTHER_STALE_RECORD_THRESHOLD, 0>::default();
        smoother.observe(1);
        smoother.observe(6);
        smoother.observe(2);
        smoother.observe(3);
        smoother.observe(4);
        smoother.observe(5);
        smoother.observe(0);

        assert!((smoother.get_avg() - 2.8).abs() < f64::EPSILON);
        assert_eq!(smoother.get_recent(), 0);
        assert_eq!(smoother.get_max(), 5);
        assert_eq!(smoother.get_percentile_90(), 4);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        let mut smoother = Smoother::<f64, 5, SMOOTHER_STALE_RECORD_THRESHOLD, 0>::default();
        smoother.observe(1.0);
        smoother.observe(6.0);
        smoother.observe(2.0);
        smoother.observe(3.0);
        smoother.observe(4.0);
        smoother.observe(5.0);
        smoother.observe(9.0);
        assert!((smoother.get_avg() - 4.6).abs() < f64::EPSILON);
        assert!((smoother.get_recent() - 9.0).abs() < f64::EPSILON);
        assert!((smoother.get_max() - 9.0).abs() < f64::EPSILON);
        assert!((smoother.get_percentile_90() - 5.0).abs() < f64::EPSILON);
        assert_eq!(smoother.trend(), Trend::Increasing);
    }

    #[test]
    fn test_smoother_trend() {
        // The time range is not enough
        let mut smoother = Smoother::<
            u64,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        let now = Instant::now_coarse();
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 1)),
        );
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 2)),
        );
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 3)),
        );
        smoother.observe_with_time(4, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(4, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(4, now);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        // Increasing trend, the left range contains 3 records, the right range contains
        // 1 records.
        let mut smoother = Smoother::<
            f64,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD)),
        );
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 1)),
        );
        smoother.observe_with_time(4.0, now);
        assert_eq!(smoother.trend(), Trend::Increasing);

        // Decreasing trend, the left range contains 1 records, the right range contains
        // 3 records.
        let mut smoother = Smoother::<
            f32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            4.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(1.0, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(2.0, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(1.0, now);
        assert_eq!(smoother.trend(), Trend::Decreasing);

        // No trend, the left range contains 1 records, the right range contains 3
        // records.
        let mut smoother = Smoother::<
            f32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(1.0, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(3.0, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(2.0, now);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        // No trend, because the latest record is too old
        let mut smoother = Smoother::<
            u32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_STALE_RECORD_THRESHOLD + 1)),
        );
        assert_eq!(smoother.trend(), Trend::NoTrend);
    }
}
