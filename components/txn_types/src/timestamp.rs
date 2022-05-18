// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use collections::HashSet;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct TimeStamp(u64);

const TSO_PHYSICAL_SHIFT_BITS: u64 = 18;

impl TimeStamp {
    /// Create a time stamp from physical and logical components.
    pub fn compose(physical: u64, logical: u64) -> TimeStamp {
        TimeStamp((physical << TSO_PHYSICAL_SHIFT_BITS) + logical)
    }

    pub const fn zero() -> TimeStamp {
        TimeStamp(0)
    }

    pub const fn max() -> TimeStamp {
        TimeStamp(u64::MAX)
    }

    pub const fn new(ts: u64) -> TimeStamp {
        TimeStamp(ts)
    }

    /// Extracts physical part of a timestamp, in milliseconds.
    pub fn physical(self) -> u64 {
        self.0 >> TSO_PHYSICAL_SHIFT_BITS
    }

    /// Extracts logical part of a timestamp.
    pub fn logical(self) -> u64 {
        self.0 & ((1 << TSO_PHYSICAL_SHIFT_BITS) - 1)
    }

    #[must_use]
    pub fn next(self) -> TimeStamp {
        assert!(self.0 < u64::MAX);
        TimeStamp(self.0 + 1)
    }

    #[must_use]
    pub fn prev(self) -> TimeStamp {
        assert!(self.0 > 0);
        TimeStamp(self.0 - 1)
    }

    pub fn incr(&mut self) -> &mut TimeStamp {
        assert!(self.0 < u64::MAX);
        self.0 += 1;
        self
    }

    pub fn decr(&mut self) -> &mut TimeStamp {
        assert!(self.0 > 0);
        self.0 -= 1;
        self
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn is_max(self) -> bool {
        self.0 == u64::MAX
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

impl From<u64> for TimeStamp {
    fn from(ts: u64) -> TimeStamp {
        TimeStamp(ts)
    }
}

impl From<&u64> for TimeStamp {
    fn from(ts: &u64) -> TimeStamp {
        TimeStamp(*ts)
    }
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl slog::Value for TimeStamp {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::Value::serialize(&self.0, record, key, serializer)
    }
}

const TS_SET_USE_VEC_LIMIT: usize = 8;

/// A hybrid immutable set for timestamps.
#[derive(Debug, Clone, PartialEq)]
pub enum TsSet {
    /// When the set is empty, avoid the useless cloning of Arc.
    Empty,
    /// `Vec` is suitable when the set is small or the set is barely used, and it doesn't worth
    /// converting a `Vec` into a `HashSet`.
    Vec(Arc<[TimeStamp]>),
    /// `Set` is suitable when there are many timestamps **and** it will be queried multiple times.
    Set(Arc<HashSet<TimeStamp>>),
}

impl Default for TsSet {
    #[inline]
    fn default() -> TsSet {
        TsSet::Empty
    }
}

impl TsSet {
    /// Create a `TsSet` from the given vec of timestamps. It will select the proper internal
    /// collection type according to the size.
    #[inline]
    pub fn new(ts: Vec<TimeStamp>) -> Self {
        if ts.is_empty() {
            TsSet::Empty
        } else if ts.len() <= TS_SET_USE_VEC_LIMIT {
            // If there are too few elements in `ts`, use Vec directly instead of making a Set.
            TsSet::Vec(ts.into())
        } else {
            TsSet::Set(Arc::new(ts.into_iter().collect()))
        }
    }

    pub fn from_u64s(ts: Vec<u64>) -> Self {
        // This conversion is safe because TimeStamp is a transparent wrapper over u64.
        Self::new(unsafe { tikv_util::memory::vec_transmute(ts) })
    }

    pub fn vec_from_u64s(ts: Vec<u64>) -> Self {
        // This conversion is safe because TimeStamp is a transparent wrapper over u64.
        Self::vec(unsafe { tikv_util::memory::vec_transmute(ts) })
    }

    /// Create a `TsSet` from the given vec of timestamps, but it will be forced to use `Vec` as the
    /// internal collection type. When it's sure that the set will be queried at most once, use this
    /// is better than `TsSet::new`, since both the querying on `Vec` and the conversion from `Vec`
    /// to `HashSet` is O(N).
    #[inline]
    pub fn vec(ts: Vec<TimeStamp>) -> Self {
        if ts.is_empty() {
            TsSet::Empty
        } else {
            TsSet::Vec(ts.into())
        }
    }

    /// Query whether the given timestamp is contained in the set.
    #[inline]
    pub fn contains(&self, ts: TimeStamp) -> bool {
        match self {
            TsSet::Empty => false,
            TsSet::Vec(vec) => vec.contains(&ts),
            TsSet::Set(set) => set.contains(&ts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Key;

    #[test]
    fn test_ts() {
        let physical = 1568700549751;
        let logical = 108;
        let ts = TimeStamp::compose(physical, logical);
        assert_eq!(ts, 411225436913926252.into());

        let extracted_physical = ts.physical();
        assert_eq!(extracted_physical, physical);

        let extracted_logical = ts.logical();
        assert_eq!(extracted_logical, logical);
    }

    #[test]
    fn test_split_ts() {
        let k = b"k";
        let ts = TimeStamp(123);
        assert!(Key::split_on_ts_for(k).is_err());
        let enc = Key::from_encoded_slice(k).append_ts(ts);
        let res = Key::split_on_ts_for(enc.as_encoded()).unwrap();
        assert_eq!(res, (k.as_ref(), ts));
    }

    #[test]
    fn test_ts_set() {
        let s = TsSet::new(vec![]);
        assert_eq!(s, TsSet::Empty);
        assert!(!s.contains(1.into()));

        let s = TsSet::vec(vec![]);
        assert_eq!(s, TsSet::Empty);

        let s = TsSet::from_u64s(vec![1, 2]);
        assert_eq!(s, TsSet::Vec(vec![1.into(), 2.into()].into()));
        assert!(s.contains(1.into()));
        assert!(s.contains(2.into()));
        assert!(!s.contains(3.into()));

        let s2 = TsSet::vec(vec![1.into(), 2.into()]);
        assert_eq!(s2, s);

        let big_ts_list: Vec<TimeStamp> =
            (0..=TS_SET_USE_VEC_LIMIT as u64).map(Into::into).collect();
        let s = TsSet::new(big_ts_list.clone());
        assert_eq!(
            s,
            TsSet::Set(Arc::new(big_ts_list.clone().into_iter().collect()))
        );
        assert!(s.contains(1.into()));
        assert!(s.contains((TS_SET_USE_VEC_LIMIT as u64).into()));
        assert!(!s.contains((TS_SET_USE_VEC_LIMIT as u64 + 1).into()));

        let s = TsSet::vec(big_ts_list.clone());
        assert_eq!(s, TsSet::Vec(big_ts_list.into()));
    }
}
