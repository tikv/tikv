// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::coprocessor::KeyRange;

#[derive(PartialEq, Clone, Debug)]
pub enum ConsumerResult {
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

/// An iterator like structure that produces user key ranges.
///
/// For each `next()`, it produces a new non-point range, or a new point range, or a flag indicating
/// continuing last non-point range, or a flag indicating that all ranges are consumed. If a new
/// non-point or point range is returned, caller will then scan unknown amount of key(s) within this
/// new range. If last non-point range is returned, caller will continue scanning. The caller must
/// inform the structure to emit a new range next time by calling `consume()` after current
/// non-point range is drained. Point range is considered as drained and will be consumed
/// automatically after retrieving. Multiple `consume()` without `next()` will only consume
/// corresponding range once.
pub struct RangesConsumer {
    in_range: bool,
    emit_point_range: bool,
    iter: std::vec::IntoIter<KeyRange>,
}

impl RangesConsumer {
    #[inline]
    pub fn new(user_key_ranges: Vec<KeyRange>, emit_point_range: bool) -> Self {
        Self {
            in_range: false,
            iter: user_key_ranges.into_iter(),
            emit_point_range,
        }
    }

    #[inline]
    pub fn next(&mut self) -> ConsumerResult {
        if self.in_range {
            return ConsumerResult::Continue;
        }
        match self.iter.next() {
            None => ConsumerResult::Drained,
            Some(range) => {
                if crate::coprocessor::util::is_point(&range) && self.emit_point_range {
                    self.in_range = false;
                    ConsumerResult::NewPointRange(range)
                } else {
                    self.in_range = true;
                    ConsumerResult::NewNonPointRange(range)
                }
            }
        }
    }

    #[inline]
    pub fn consume(&mut self) {
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

    // TODO: Test `RangesConsumer::new(..., false)`.

    #[test]
    fn test_basic() {
        // Empty
        let mut c = RangesConsumer::new(vec![], true);
        assert_eq!(c.next(), ConsumerResult::Drained);
        assert_eq!(c.next(), ConsumerResult::Drained);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);
        assert_eq!(c.next(), ConsumerResult::Drained);

        // Pure non-point range
        let ranges = vec![new_range(), new_range(), new_range()];
        let mut c = RangesConsumer::new(ranges.clone(), true);
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[0].clone())
        );
        assert_eq!(c.next(), ConsumerResult::Continue);
        assert_eq!(c.next(), ConsumerResult::Continue);
        c.consume();
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[1].clone())
        );
        assert_eq!(c.next(), ConsumerResult::Continue);
        assert_eq!(c.next(), ConsumerResult::Continue);
        c.consume();
        c.consume(); // multiple consumes will not take effect
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[2].clone())
        );
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);

        // Pure point range
        let ranges = vec![new_point(), new_point(), new_point()];
        let mut c = RangesConsumer::new(ranges.clone(), true);
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[0].clone()));
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[1].clone()));
        c.consume(); // no effect
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[2].clone()));
        assert_eq!(c.next(), ConsumerResult::Drained);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);

        // Mixed
        let ranges = vec![new_point(), new_range(), new_point()];
        let mut c = RangesConsumer::new(ranges.clone(), true);
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[0].clone()));
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[1].clone())
        );
        assert_eq!(c.next(), ConsumerResult::Continue);
        assert_eq!(c.next(), ConsumerResult::Continue);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[2].clone()));
        assert_eq!(c.next(), ConsumerResult::Drained);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);

        // Mixed
        let ranges = vec![new_range(), new_point(), new_range(), new_range()];
        let mut c = RangesConsumer::new(ranges.clone(), true);
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[0].clone())
        );
        assert_eq!(c.next(), ConsumerResult::Continue);
        assert_eq!(c.next(), ConsumerResult::Continue);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::NewPointRange(ranges[1].clone()));
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[2].clone())
        );
        c.consume();
        assert_eq!(
            c.next(),
            ConsumerResult::NewNonPointRange(ranges[3].clone())
        );
        assert_eq!(c.next(), ConsumerResult::Continue);
        assert_eq!(c.next(), ConsumerResult::Continue);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);
        c.consume();
        assert_eq!(c.next(), ConsumerResult::Drained);
    }
}
