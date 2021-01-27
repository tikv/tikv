// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use collections::{HashMap, HashSet};
use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use txn_types::TimeStamp;

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // key -> start_ts
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,
    // start_ts -> locked keys.
    lock_ts_heap: BTreeMap<TimeStamp, HashSet<Arc<[u8]>>>,
    // The timestamps that guarantees no more commit will happen before.
    // None if the resolver is not initialized.
    resolved_ts: Option<TimeStamp>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
}

impl Resolver {
    pub fn new(region_id: u64) -> Resolver {
        Resolver {
            region_id,
            locks_by_key: HashMap::default(),
            lock_ts_heap: BTreeMap::new(),
            resolved_ts: None,
            min_ts: TimeStamp::zero(),
        }
    }

    pub fn init(&mut self) {
        self.resolved_ts = Some(TimeStamp::zero());
    }

    pub fn resolved_ts(&self) -> Option<TimeStamp> {
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

    pub fn untrack_lock(&mut self, key: &[u8]) {
        let start_ts = if let Some(start_ts) = self.locks_by_key.remove(key) {
            start_ts
        } else {
            debug!("untrack a lock that was not tracked before"; "key" => &log_wrappers::Value::key(key));
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
    /// Return None means the resolver is not initialized.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        let old_resolved_ts = self.resolved_ts?;

        // Find the min start ts.
        let min_lock = self.lock_ts_heap.keys().next().cloned();
        let has_lock = min_lock.is_some();
        let min_start_ts = min_lock.unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);
        // Resolved ts never decrease.
        self.resolved_ts = Some(cmp::max(old_resolved_ts, new_resolved_ts));

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
        // start_ts, key
        Lock(u64, Key),
        // key
        Unlock(Key),
        // min_ts, expect
        Resolve(u64, u64),
    }

    #[test]
    fn test_resolve() {
        let cases = vec![
            vec![Event::Lock(1, Key::from_raw(b"a")), Event::Resolve(2, 1)],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(3, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Lock(1, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller start_ts.
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Unlock(Key::from_raw(b"a")),
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Resolve(4, 2),
                Event::Unlock(Key::from_raw(b"a")),
                Event::Resolve(5, 5),
            ],
            // Rollback may contain a key that is not locked.
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(Key::from_raw(b"b")),
                Event::Unlock(Key::from_raw(b"a")),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new(1);
            resolver.init();
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => {
                        resolver.track_lock(start_ts.into(), key.into_raw().unwrap())
                    }
                    Event::Unlock(key) => resolver.untrack_lock(&key.into_raw().unwrap()),
                    Event::Resolve(min_ts, expect) => assert_eq!(
                        resolver.resolve(min_ts.into()).unwrap(),
                        expect.into(),
                        "case {}",
                        i
                    ),
                }
            }

            let mut resolver = Resolver::new(1);
            for e in case {
                match e {
                    Event::Lock(start_ts, key) => {
                        resolver.track_lock(start_ts.into(), key.into_raw().unwrap())
                    }
                    Event::Unlock(key) => resolver.untrack_lock(&key.into_raw().unwrap()),
                    Event::Resolve(min_ts, _) => {
                        assert_eq!(resolver.resolve(min_ts.into()), None, "case {}", i)
                    }
                }
            }
        }
    }
}
