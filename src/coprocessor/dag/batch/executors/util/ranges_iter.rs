// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::KeyRange;

#[derive(PartialEq, Clone, Debug)]
pub enum IterStatus {
    /// All ranges are consumed.
    Drained,

    /// Last range is drained or this iteration is a fresh start so that caller should scan
    /// on a new range.
    NewNonPointRange(KeyRange),

    /// Similar to `NewNonPointRange`, but the new range is a point range, whose key will be placed
    /// in `start_key`. The content of other fields should be discarded.
    NewPointRange(KeyRange),

    /// Last non-point range is not drained and the caller should continue scanning without changing
    /// the scan range.
    Continue,
}

/// A trait that determines how to deal with possible point ranges.
pub trait PointRangePolicy: Send + Sync + 'static {
    fn is_point_range(&self, range: &KeyRange) -> bool;
}

/// A policy that respects point ranges.
///
/// Notice that this exists because currently we use prefix_next() result as end_key to mark a range
/// as point range. Once we have other clearer notation, e.g. a flag, we won't need these policy any
/// more.
pub struct PointRangeEnable;

/// A policy that conditional respects point ranges.
///
/// TODO: Maybe better to be `PointRangeDisable`.
pub struct PointRangeConditional(bool);

impl PointRangeConditional {
    pub fn new(enable: bool) -> Self {
        PointRangeConditional(enable)
    }
}

impl PointRangePolicy for PointRangeConditional {
    #[inline]
    fn is_point_range(&self, range: &KeyRange) -> bool {
        crate::coprocessor::util::is_point(range) && self.0
    }
}

impl PointRangePolicy for PointRangeEnable {
    #[inline]
    fn is_point_range(&self, range: &KeyRange) -> bool {
        crate::coprocessor::util::is_point(range)
    }
}

/// An iterator like structure that produces user key ranges.
///
/// For each `next()`, it produces one of the following:
/// - a new non-point range
/// - a new point range
/// - a flag indicating continuing last non-point range
/// - a flag indicating that all ranges are consumed
///
/// If a new non-point or point range is returned, caller can then scan unknown amount of key(s)
/// within this new range. The caller must inform the structure so that it will emit a new range
/// next time by calling `notify_drained()` after current non-point range is drained. Point range is
/// regarded as drained automatically after calling `next()`. Multiple `notify_drained()` without
/// `next()` will have no effect.
pub struct RangesIterator<T: PointRangePolicy> {
    /// Whether or not we are processing a valid range. If we are not processing a range, or there
    /// is no range any more, this field is `false`.
    in_range: bool,

    iter: std::vec::IntoIter<KeyRange>,

    policy: T,
}

impl<T: PointRangePolicy> RangesIterator<T> {
    #[inline]
    pub fn new(user_key_ranges: Vec<KeyRange>, policy: T) -> Self {
        Self {
            in_range: false,
            iter: user_key_ranges.into_iter(),
            policy,
        }
    }

    /// Continues iterating.
    #[inline]
    pub fn next(&mut self) -> IterStatus {
        if self.in_range {
            return IterStatus::Continue;
        }
        match self.iter.next() {
            None => IterStatus::Drained,
            Some(range) => {
                if self.policy.is_point_range(&range) {
                    // No need to set `in_range = true` because point range always drains.
                    IterStatus::NewPointRange(range)
                } else {
                    self.in_range = true;
                    IterStatus::NewNonPointRange(range)
                }
            }
        }
    }

    /// Notifies that current range is drained.
    #[inline]
    pub fn notify_drained(&mut self) {
        self.in_range = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic;

    static RANGE_INDEX: atomic::AtomicU64 = atomic::AtomicU64::new(1);

    fn new_point() -> KeyRange {
        use byteorder::{BigEndian, WriteBytesExt};

        let v = RANGE_INDEX.fetch_add(1, atomic::Ordering::SeqCst);
        let mut r = KeyRange::new();
        r.mut_start().write_u64::<BigEndian>(v).unwrap();
        // Enough to pass `util::is_point`.
        r.mut_end().write_u64::<BigEndian>(v + 1).unwrap();
        r
    }

    fn new_range() -> KeyRange {
        use byteorder::{BigEndian, WriteBytesExt};

        let v = RANGE_INDEX.fetch_add(2, atomic::Ordering::SeqCst);
        let mut r = KeyRange::new();
        r.mut_start().write_u64::<BigEndian>(v).unwrap();
        // Enough to not pass `util::is_point`.
        r.mut_end().write_u64::<BigEndian>(v + 2).unwrap();
        r
    }

    #[test]
    fn test_basic() {
        // Empty
        let mut c = RangesIterator::new(vec![], PointRangeEnable);
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);

        // Pure non-point range
        let ranges = vec![new_range(), new_range(), new_range()];
        let mut c = RangesIterator::new(ranges.clone(), PointRangeEnable);
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[1].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        c.notify_drained(); // multiple consumes will not take effect
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[2].clone()));
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);

        // Pure point range
        let ranges = vec![new_point(), new_point(), new_point()];
        let mut c = RangesIterator::new(ranges.clone(), PointRangeEnable);
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[1].clone()));
        c.notify_drained(); // no effect
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[2].clone()));
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);

        // Mixed
        let ranges = vec![new_point(), new_range(), new_point()];
        let mut c = RangesIterator::new(ranges.clone(), PointRangeEnable);
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[1].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[2].clone()));
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);

        // Mixed
        let ranges = vec![new_range(), new_point(), new_range(), new_range()];
        let mut c = RangesIterator::new(ranges.clone(), PointRangeEnable);
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewPointRange(ranges[1].clone()));
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[2].clone()));
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewNonPointRange(ranges[3].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }
}
