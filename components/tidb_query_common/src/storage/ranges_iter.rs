// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::range::Range;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum IterStatus {
    /// All ranges are consumed.
    Drained,

    /// Last range is drained or this iteration is a fresh start so that caller should scan
    /// on a new range.
    NewRange(Range),

    /// Last interval range is not drained and the caller should continue scanning without changing
    /// the scan range.
    Continue,
}

/// An iterator like structure that produces user key ranges.
///
/// For each `next()`, it produces one of the following:
/// - a new range
/// - a flag indicating continuing last interval range
/// - a flag indicating that all ranges are consumed
///
/// If a new range is returned, caller can then scan unknown amount of key(s) within this new range.
/// The caller must inform the structure so that it will emit a new range next time by calling
/// `notify_drained()` after current range is drained. Multiple `notify_drained()` without `next()`
/// will have no effect.
pub struct RangesIterator {
    /// Whether or not we are processing a valid range. If we are not processing a range, or there
    /// is no range any more, this field is `false`.
    in_range: bool,

    iter: std::vec::IntoIter<Range>,
}

impl RangesIterator {
    #[inline]
    pub fn new(user_key_ranges: Vec<Range>) -> Self {
        Self {
            in_range: false,
            iter: user_key_ranges.into_iter(),
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
                self.in_range = true;
                IterStatus::NewRange(range)
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
    use std::sync::atomic;

    use super::{super::range::IntervalRange, *};

    static RANGE_INDEX: atomic::AtomicU64 = atomic::AtomicU64::new(1);

    fn new_range() -> Range {
        use byteorder::{BigEndian, WriteBytesExt};

        let v = RANGE_INDEX.fetch_add(2, atomic::Ordering::SeqCst);
        let mut r = IntervalRange::from(("", ""));
        r.lower_inclusive.write_u64::<BigEndian>(v).unwrap();
        r.upper_exclusive.write_u64::<BigEndian>(v + 2).unwrap();
        Range::Interval(r)
    }

    #[test]
    fn test_basic() {
        // Empty
        let mut c = RangesIterator::new(vec![]);
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);

        // Non-empty
        let ranges = vec![new_range(), new_range(), new_range()];
        let mut c = RangesIterator::new(ranges.clone());
        assert_eq!(c.next(), IterStatus::NewRange(ranges[0].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewRange(ranges[1].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        c.notify_drained(); // multiple consumes will not take effect
        assert_eq!(c.next(), IterStatus::NewRange(ranges[2].clone()));
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }
}
