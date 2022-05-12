// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, cmp::Reverse, collections::BinaryHeap};

use txn_types::TimeStamp;

pub struct Resolver {
    // BinaryHeap is max heap, so we reverse order to get a min heap.
    region_id: u64,
    lock_ts_heap: BinaryHeap<Reverse<TimeStamp>>,
    resolved_ts: TimeStamp,
}

impl Resolver {
    pub fn new(region_id: u64) -> Resolver {
        Resolver {
            region_id,
            lock_ts_heap: BinaryHeap::new(),
            resolved_ts: TimeStamp::zero(),
        }
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        self.resolved_ts
    }

    pub fn track_ts(&mut self, start_ts: TimeStamp) {
        debug!("track ts {}, region {}", start_ts, self.region_id);
        self.lock_ts_heap.push(Reverse(start_ts));
    }

    pub fn untrack_ts_before(&mut self, ts: TimeStamp) {
        let mut last_min_ts = None;
        debug!("untrack ts before {}, region {}", ts, self.region_id);
        loop {
            let min_ts = self.lock_ts_heap.peek();
            if min_ts.is_none() {
                break;
            }
            let min_ts = min_ts.unwrap();
            if min_ts.0 > ts {
                break;
            }
            last_min_ts = self.lock_ts_heap.pop();
        }
        if let Some(last_min_ts) = last_min_ts {
            self.resolved_ts = last_min_ts.0;
        }
    }

    /// Try to advance resolved ts.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        let min_tracked_ts = self.lock_ts_heap.peek();
        let mut min_start_ts = min_ts;
        if let Some(min_tracked_ts) = min_tracked_ts {
            min_start_ts = min_tracked_ts.0;
        }
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);

        self.resolved_ts = cmp::max(new_resolved_ts, self.resolved_ts);
        self.resolved_ts
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_resolver() {
        let test_cases = vec![
            (vec![1, 3, 4], 4, 5, 5),
            (vec![6, 8, 10], 8, 11, 10),
            (vec![11, 12, 13], 13, 12, 13),
        ];

        for (i, (tracked_ts, untracked_ts, min_ts, expected)) in test_cases.into_iter().enumerate()
        {
            let mut resolver = Resolver::new(1);
            for ts in tracked_ts {
                resolver.track_ts(ts.into());
            }
            resolver.untrack_ts_before(untracked_ts.into());
            let resolved_ts = resolver.resolve(min_ts.into());
            assert!(
                resolved_ts == expected.into(),
                "case {} failed to resolve ts, resolved_ts {}, expected {}",
                i,
                resolved_ts,
                expected
            );
        }
    }
}
