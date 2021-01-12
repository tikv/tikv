// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use crossbeam::atomic::AtomicCell;
use std::cmp;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use txn_types::{Key, TimeStamp};

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // start_ts -> locked keys.
    locks: BTreeMap<TimeStamp, HashSet<Key>>,
    // The timestamps that guarantees no more commit will happen before.
    resolved_ts: Arc<AtomicU64>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
}

impl Resolver {
    pub fn from_resolved_ts(region_id: u64, resolved_ts: Arc<AtomicU64>) -> Resolver {
        Resolver {
            region_id,
            resolved_ts,
            locks: BTreeMap::new(),
            min_ts: TimeStamp::zero(),
        }
    }

    pub fn new(region_id: u64) -> Resolver {
        Self::from_resolved_ts(region_id, Arc::new(AtomicU64::new(0)))
    }

    // pub fn resolved_ts(&self) -> Arc<AtomicCell<TimeStamp>> {
    //     self.resolved_ts.clone()
    // }

    pub fn locks(&self) -> &BTreeMap<TimeStamp, HashSet<Key>> {
        &self.locks
    }

    pub fn track_lock(&mut self, start_ts: TimeStamp, key: &Key) {
        debug!(
            "track lock {}@{}, region {}",
            log_wrappers::Value(key.as_encoded()),
            start_ts,
            self.region_id
        );
        self.locks.entry(start_ts).or_default().insert(key.clone());
    }

    pub fn untrack_lock(&mut self, start_ts: TimeStamp, commit_ts: Option<TimeStamp>, key: &Key) {
        debug!(
            "untrack lock {}@{}, commit@{}, region {}",
            log_wrappers::Value(key.as_encoded()),
            start_ts,
            commit_ts.clone().unwrap_or_else(TimeStamp::zero),
            self.region_id,
        );
        if let Some(commit_ts) = commit_ts {
            assert!(
                commit_ts > TimeStamp::from(self.resolved_ts.load(Ordering::Relaxed)),
                "{}@{}, commit@{} < {:?}, region {}",
                log_wrappers::Value(key.as_encoded()),
                start_ts,
                commit_ts,
                self.resolved_ts,
                self.region_id
            );
            assert!(
                commit_ts > self.min_ts,
                "{}@{}, commit@{} < {:?}, region {}",
                log_wrappers::Value(key.as_encoded()),
                start_ts,
                commit_ts,
                self.min_ts,
                self.region_id
            );
        }

        let entry = self.locks.get_mut(&start_ts);
        // It's possible that rollback happens on a not existing transaction.
        assert!(
            entry.is_some() || commit_ts.is_none(),
            "{}@{}, commit@{} is not tracked, region {}",
            log_wrappers::Value(key.as_encoded()),
            start_ts,
            commit_ts.unwrap_or_else(TimeStamp::zero),
            self.region_id
        );
        if let Some(locked_keys) = entry {
            assert!(
                locked_keys.remove(&key) || commit_ts.is_none(),
                "{}@{}, commit@{} is not tracked, region {}, {:?}",
                log_wrappers::Value(key.as_encoded()),
                start_ts,
                commit_ts.unwrap_or_else(TimeStamp::zero),
                self.region_id,
                locked_keys
            );
            if locked_keys.is_empty() {
                self.locks.remove(&start_ts);
            }
        }
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        // Find the min start ts.
        let min_lock = self.locks.keys().next().cloned();
        let has_lock = min_lock.is_some();
        let min_start_ts = min_lock.unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);

        // Resolved ts never decrease.
        let prev_ts = self
            .resolved_ts
            .fetch_max(new_resolved_ts.into_inner(), Ordering::Relaxed);

        if prev_ts < new_resolved_ts.into_inner() {
            debug!(
                "resolved ts by resolver";
                "region id" => self.region_id,
                "new resolved ts" => new_resolved_ts.into_inner()
            );
        }

        let new_min_ts = if has_lock {
            // If there are some lock, the min_ts must be smaller than
            // the min start ts, so it guarantees to be smaller than
            // any late arriving commit ts.
            new_resolved_ts // cmp::min(min_start_ts, min_ts)
        } else {
            min_ts
        };
        // Min ts never decrease.
        self.min_ts = cmp::max(self.min_ts, new_min_ts);

        cmp::max(TimeStamp::from(prev_ts), new_resolved_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use txn_types::Key;

    #[derive(Clone)]
    enum Event {
        Lock(u64, Key),
        Unlock(u64, Option<u64>, Key),
        // min_ts, expect
        Resolve(u64, u64),
    }

    #[test]
    fn test_resolve() {
        let cases = vec![
            vec![Event::Lock(1, Key::from_raw(b"a")), Event::Resolve(2, 1)],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(3, Key::from_raw(b"a")),
                Event::Unlock(3, Some(4), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Lock(1, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller start_ts.
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(1, Some(4), Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Unlock(1, None, Key::from_raw(b"a")),
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Resolve(4, 2),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(5, 5),
            ],
            // Rollback may contain a key that is not locked.
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, None, Key::from_raw(b"b")),
                Event::Unlock(1, None, Key::from_raw(b"a")),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new(1);
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => resolver.track_lock(start_ts.into(), &key),
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(start_ts.into(), commit_ts.map(Into::into), &key)
                    }
                    Event::Resolve(min_ts, expect) => {
                        assert_eq!(resolver.resolve(min_ts.into()), expect.into(), "case {}", i)
                    }
                }
            }
        }
    }
}
