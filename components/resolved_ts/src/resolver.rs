// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use crossbeam::atomic::AtomicCell;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use txn_types::{Key, TimeStamp};

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // key -> start_ts
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,
    // start_ts -> locked keys.
    lock_ts_heap: BTreeMap<TimeStamp, HashSet<Arc<[u8]>>>,
    // The timestamps that guarantees no more commit will happen before.
    resolved_ts: TimeStamp,
    // The global `resolved_ts` that may updated by multi threads
    global_resolved_ts: Arc<AtomicU64>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
}

impl Resolver {
    pub fn from_resolved_ts(region_id: u64, global_resolved_ts: Arc<AtomicU64>) -> Resolver {
        Resolver {
            region_id,
            resolved_ts: TimeStamp::zero(),
            global_resolved_ts,
            locks_by_key: HashMap::default(),
            lock_ts_heap: BTreeMap::new(),
            min_ts: TimeStamp::zero(),
        }
    }

    pub fn new(region_id: u64) -> Resolver {
        Self::from_resolved_ts(region_id, Arc::new(AtomicU64::new(0)))
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        self.resolved_ts
    }

    pub fn locks(&self) -> &BTreeMap<TimeStamp, HashSet<Arc<[u8]>>> {
        &self.lock_ts_heap
    }

    pub fn track_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>) {
        debug!(
            "track lock {}@{}, region {}",
            &log_wrappers::Value::key(&key),
            start_ts,
            self.region_id
        );
        let key: Arc<[u8]> = key.into_boxed_slice().into();
        self.locks_by_key.insert(key.clone(), start_ts);
        self.lock_ts_heap.entry(start_ts).or_default().insert(key);
    }

    pub fn untrack_lock(&mut self, commit_ts: Option<TimeStamp>, key: &[u8]) {
        if let Some(commit_ts) = commit_ts {
            assert!(
                commit_ts > self.resolved_ts,
                "{}@{:?}, commit@{} < {:?}, region {}",
                &log_wrappers::Value::key(key),
                self.locks_by_key.get(key),
                commit_ts,
                self.resolved_ts,
                self.region_id
            );
            assert!(
                commit_ts > self.min_ts,
                "{}@{:?}, commit@{} < {:?}, region {}",
                &log_wrappers::Value::key(key),
                self.locks_by_key.get(key),
                commit_ts,
                self.min_ts,
                self.region_id
            );
        }
        let start_ts = if let Some(start_ts) = self.locks_by_key.remove(key) {
            start_ts
        } else {
            info!("untrack a lock that was not tracked before"; "key" => &log_wrappers::Value::key(key));
            return;
        };
        debug!(
            "untrack lock {}@{}, region {}",
            &log_wrappers::Value::key(key),
            start_ts,
            self.region_id,
        );

        let entry = self.lock_ts_heap.get_mut(&start_ts);
        if let Some(locked_keys) = entry {
            locked_keys.remove(key);
            if locked_keys.is_empty() {
                self.lock_ts_heap.remove(&start_ts);
            }
        }
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        // Find the min start ts.
        let min_lock = self.lock_ts_heap.keys().next().cloned();
        let has_lock = min_lock.is_some();
        let min_start_ts = min_lock.unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);

        // Resolved ts never decrease.
        self.resolved_ts = cmp::max(self.resolved_ts, new_resolved_ts);

        // Update the global resolved ts
        let prev_ts = self
            .global_resolved_ts
            .fetch_max(self.resolved_ts.into_inner(), Ordering::Relaxed);
        if prev_ts < self.resolved_ts.into_inner() {
            debug!(
                "forward resolved ts by resolver";
                "region id" => self.region_id,
                "new resolved ts" => ?self.resolved_ts,
            );
        } else {
            info!(
                "faied to forward resolved ts by resolver";
                "region id" => self.region_id,
                "prev ts" => ?prev_ts,
                "new resolved ts" => ?self.resolved_ts,
                "min lock" => ?min_lock,
                "pd ts" => ?min_ts,
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

        self.resolved_ts
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
                    Event::Lock(start_ts, key) => resolver.track_lock(start_ts.into(), key.0),
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(commit_ts, &key.0)
                    }
                    Event::Resolve(min_ts, expect) => {
                        assert_eq!(resolver.resolve(min_ts.into()), expect.into(), "case {}", i)
                    }
                }
            }
        }
    }
}
