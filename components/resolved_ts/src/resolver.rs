// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::BTreeMap, sync::Arc};

use collections::{HashMap, HashSet};
use raftstore::store::RegionReadProgress;
use tikv_util::{
    memory::{HeapSize, MemoryQuota},
    time::Instant,
};
use txn_types::TimeStamp;

use crate::metrics::RTS_RESOLVED_FAIL_ADVANCE_VEC;

const MAX_NUMBER_OF_LOCKS_IN_LOG: usize = 10;
const ON_DROP_WARN_HEAP_SIZE: usize = 64 * 1024 * 1024; // 64MB

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
    // The highest index `Resolver` had been tracked
    tracked_index: u64,
    // The region read progress used to utilize `resolved_ts` to serve stale read request
    read_progress: Option<Arc<RegionReadProgress>>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
    // Whether the `Resolver` is stopped
    stopped: bool,

    // The memory quota for the `Resolver` and its lock keys and timestamps.
    memory_quota: Arc<MemoryQuota>,
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let far_lock = self.lock_ts_heap.iter().next();
        let mut dt = f.debug_tuple("Resolver");
        dt.field(&format_args!("region={}", self.region_id));

        if let Some((ts, keys)) = far_lock {
            dt.field(&format_args!(
                "oldest_lock={:?}",
                keys.iter()
                    // We must use Display format here or the redact won't take effect.
                    .map(|k| format!("{}", log_wrappers::Value::key(k)))
                    .collect::<Vec<_>>()
            ));
            dt.field(&format_args!("oldest_lock_ts={:?}", ts));
        }

        dt.finish()
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        // Free memory quota used by locks_by_key.
        let mut bytes = 0;
        let num_locks = self.num_locks();
        for key in self.locks_by_key.keys() {
            bytes += key.heap_size();
        }
        if bytes > ON_DROP_WARN_HEAP_SIZE {
            warn!("drop huge resolver";
                "region_id" => self.region_id,
                "bytes" => bytes,
                "num_locks" => num_locks,
            );
        }
        self.memory_quota.free(bytes);
    }
}

impl Resolver {
    pub fn new(region_id: u64, memory_quota: Arc<MemoryQuota>) -> Resolver {
        Resolver::with_read_progress(region_id, None, memory_quota)
    }

    pub fn with_read_progress(
        region_id: u64,
        read_progress: Option<Arc<RegionReadProgress>>,
        memory_quota: Arc<MemoryQuota>,
    ) -> Resolver {
        Resolver {
            region_id,
            resolved_ts: TimeStamp::zero(),
            locks_by_key: HashMap::default(),
            lock_ts_heap: BTreeMap::new(),
            read_progress,
            tracked_index: 0,
            min_ts: TimeStamp::zero(),
            stopped: false,
            memory_quota,
        }
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        self.resolved_ts
    }

    pub fn tracked_index(&self) -> u64 {
        self.tracked_index
    }

    pub fn stopped(&self) -> bool {
        self.stopped
    }

    // Return an approximate heap memory usage in bytes.
    pub fn approximate_heap_bytes(&self) -> usize {
        if self.locks_by_key.is_empty() {
            return 0;
        }

        const SAMPLE_COUNT: usize = 32;
        let mut key_count = 0;
        let mut key_bytes = 0;
        for key in self.locks_by_key.keys() {
            key_count += 1;
            key_bytes += key.len();
            if key_count >= SAMPLE_COUNT {
                break;
            }
        }
        self.locks_by_key.len() * (key_bytes / key_count + std::mem::size_of::<TimeStamp>())
            + self.lock_ts_heap.len()
                * (std::mem::size_of::<TimeStamp>() + std::mem::size_of::<HashSet<Arc<[u8]>>>())
    }

    pub fn locks(&self) -> &BTreeMap<TimeStamp, HashSet<Arc<[u8]>>> {
        &self.lock_ts_heap
    }

    pub fn stop_tracking(&mut self) {
        // TODO: should we also clear the lock heap?
        self.stopped = true;
        self.read_progress.take();
    }

    pub fn update_tracked_index(&mut self, index: u64) {
        assert!(
            self.tracked_index <= index,
            "region {}, tracked_index: {}, incoming index: {}",
            self.region_id,
            self.tracked_index,
            index
        );
        self.tracked_index = index;
    }

    #[must_use]
    pub fn track_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>, index: Option<u64>) -> bool {
        if let Some(index) = index {
            self.update_tracked_index(index);
        }
        debug!(
            "track lock {}@{}, region {}",
            &log_wrappers::Value::key(&key),
            start_ts,
            self.region_id
        );
        let bytes = key.as_slice().heap_size() + std::mem::size_of::<TimeStamp>();
        if !self.memory_quota.alloc(bytes) {
            return false;
        }
        let key: Arc<[u8]> = key.into_boxed_slice().into();
        self.locks_by_key.insert(key.clone(), start_ts);
        self.lock_ts_heap.entry(start_ts).or_default().insert(key);
        true
    }

    pub fn untrack_lock(&mut self, key: &[u8], index: Option<u64>) {
        if let Some(index) = index {
            self.update_tracked_index(index);
        }
        let start_ts = if let Some(start_ts) = self.locks_by_key.remove(key) {
            let bytes = key.heap_size();
            self.memory_quota.free(bytes);
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
    pub fn resolve(&mut self, min_ts: TimeStamp, now: Option<Instant>) -> TimeStamp {
        // The `Resolver` is stopped, not need to advance, just return the current
        // `resolved_ts`
        if self.stopped {
            return self.resolved_ts;
        }
        // Find the min start ts.
        let min_lock = self.lock_ts_heap.keys().next().cloned();
        let has_lock = min_lock.is_some();
        let min_start_ts = min_lock.unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);
        if self.resolved_ts >= new_resolved_ts {
            let label = if has_lock { "has_lock" } else { "stale_ts" };
            RTS_RESOLVED_FAIL_ADVANCE_VEC
                .with_label_values(&[label])
                .inc();
        }

        // Resolved ts never decrease.
        self.resolved_ts = cmp::max(self.resolved_ts, new_resolved_ts);

        // Publish an `(apply index, safe ts)` item into the region read progress
        if let Some(rrp) = &self.read_progress {
            rrp.update_safe_ts_with_time(self.tracked_index, self.resolved_ts.into_inner(), now);
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

    pub(crate) fn log_locks(&self, min_start_ts: u64) {
        // log lock with the minimum start_ts >= min_start_ts
        if let Some((start_ts, keys)) = self
            .lock_ts_heap
            .range(TimeStamp::new(min_start_ts)..)
            .next()
        {
            let keys_for_log = keys
                .iter()
                .map(|key| log_wrappers::Value::key(key))
                .take(MAX_NUMBER_OF_LOCKS_IN_LOG)
                .collect::<Vec<_>>();
            info!(
                "locks with the minimum start_ts in resolver";
                "region_id" => self.region_id,
                "start_ts" => start_ts,
                "sampled_keys" => ?keys_for_log,
            );
        }
    }

    pub(crate) fn num_locks(&self) -> u64 {
        self.locks_by_key.len() as u64
    }

    pub(crate) fn num_transactions(&self) -> u64 {
        self.lock_ts_heap.len() as u64
    }

    pub(crate) fn read_progress(&self) -> Option<&Arc<RegionReadProgress>> {
        self.read_progress.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use txn_types::Key;

    use super::*;

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
            let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
            let mut resolver = Resolver::new(1, memory_quota);
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => {
                        assert!(resolver.track_lock(
                            start_ts.into(),
                            key.into_raw().unwrap(),
                            None
                        ));
                    }
                    Event::Unlock(key) => resolver.untrack_lock(&key.into_raw().unwrap(), None),
                    Event::Resolve(min_ts, expect) => {
                        assert_eq!(
                            resolver.resolve(min_ts.into(), None),
                            expect.into(),
                            "case {}",
                            i
                        )
                    }
                }
            }
        }
    }

    #[test]
    fn test_memory_quota() {
        let memory_quota = Arc::new(MemoryQuota::new(1024));
        let mut resolver = Resolver::new(1, memory_quota.clone());
        let mut key = vec![0; 77];
        let mut ts = TimeStamp::default();
        while resolver.track_lock(ts, key.clone(), None) {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
        }
        let remain = 1024 % key.len();
        assert_eq!(memory_quota.in_use(), 1024 - remain);

        let mut ts = TimeStamp::default();
        for _ in 0..5 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            resolver.untrack_lock(&key, None);
        }
        assert_eq!(memory_quota.in_use(), 1024 - 5 * key.len() - remain);
        drop(resolver);
        assert_eq!(memory_quota.in_use(), 0);
    }
}
