// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use txn_types::TimeStamp;

pub struct Resolver {
    // BinaryHeap is max heap, so we reverse order to get a min heap.
    lock_ts_heap: BinaryHeap<Reverse<TimeStamp>>,
    resolved_ts: TimeStamp,
}

impl Resolver {
    pub fn new(region_id: u64) -> Resolver {
        Resolver {
            lock_ts_heap: BinaryHeap::new(),
            resolved_ts: TimeStamp::zero(),
        }
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        self.resolved_ts
    }

    pub fn track_ts(&mut self, start_ts: TimeStamp) {
        self.lock_ts_heap.push(Reverse(start_ts));
    }

    pub fn untrack_ts_before(&mut self, ts: TimeStamp) {
        loop {
            let min_lock_ts = self.lock_ts_heap.peek();
            if min_lock_ts.is_none() {
                break;
            }
            let min_lock_ts = min_lock_ts.unwrap();
            if min_lock_ts.0 > ts {
                break;
            }
            self.lock_ts_heap.pop();
        }
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    /// Return None means the resolver is not initialized.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        let min_lock_ts = self.lock_ts_heap.peek();
        let mut min_start_ts = min_ts;
        if let Some(min_lock_ts) = min_lock_ts {
            min_start_ts = min_lock_ts.0;
        }
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);

        assert!(
            self.resolved_ts > new_resolved_ts,
            "resolved ts should be incremented, old resolved ts {}, new resolved ts {}",
            self.resolved_ts,
            new_resolved_ts
        );

        self.resolved_ts = new_resolved_ts;
        new_resolved_ts
    }
}

// TODO: unit test
