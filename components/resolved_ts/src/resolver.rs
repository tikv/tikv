// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::BTreeMap, sync::Arc, time::Duration};

use collections::{HashMap, HashSet};
use raftstore::store::RegionReadProgress;
use tikv_util::{
    memory::{HeapSize, MemoryQuota},
    time::Instant,
};
use txn_types::{Key, TimeStamp};

use crate::metrics::RTS_RESOLVED_FAIL_ADVANCE_VEC;

const MAX_NUMBER_OF_LOCKS_IN_LOG: usize = 10;
pub const ON_DROP_WARN_HEAP_SIZE: usize = 64 * 1024 * 1024; // 64MB

#[derive(Clone)]
pub enum TsSource {
    // A lock in LOCK CF
    Lock(Arc<[u8]>),
    // A memory lock in concurrency manager
    MemoryLock(Key),
    PdTso,
    // The following sources can also come from PD or memory lock, but we care more about sources
    // in resolved-ts.
    BackupStream,
    Cdc,
}

impl TsSource {
    pub fn label(&self) -> &str {
        match self {
            TsSource::Lock(_) => "lock",
            TsSource::MemoryLock(_) => "rts_cm_min_lock",
            TsSource::PdTso => "pd_tso",
            TsSource::BackupStream => "backup_stream",
            TsSource::Cdc => "cdc",
        }
    }

    pub fn key(&self) -> Option<Key> {
        match self {
            TsSource::Lock(k) => Some(Key::from_encoded_slice(k)),
            TsSource::MemoryLock(k) => Some(k.clone()),
            _ => None,
        }
    }
}

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // key -> start_ts
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,
    // start_ts -> locked keys.
    pub(crate) lock_ts_heap: BTreeMap<TimeStamp, HashSet<Arc<[u8]>>>,
    // The last shrink time.
    last_aggressive_shrink_time: Instant,
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
    // The last attempt of resolve(), used for diagnosis.
    pub(crate) last_attempt: Option<LastAttempt>,
}

#[derive(Clone)]
pub(crate) struct LastAttempt {
    success: bool,
    ts: TimeStamp,
    reason: TsSource,
}

impl slog::Value for LastAttempt {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(
            key,
            &format_args!(
                "{{ success={}, ts={}, reason={}, key={:?} }}",
                self.success,
                self.ts,
                self.reason.label(),
                self.reason.key(),
            ),
        )
    }
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let far_lock = self.oldest_transaction();
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
            bytes += self.lock_heap_size(key);
        }
        if bytes > ON_DROP_WARN_HEAP_SIZE {
            warn!("drop huge resolver";
                "region_id" => self.region_id,
                "bytes" => bytes,
                "num_locks" => num_locks,
                "memory_quota_in_use" => self.memory_quota.in_use(),
                "memory_quota_capacity" => self.memory_quota.capacity(),
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
            last_aggressive_shrink_time: Instant::now_coarse(),
            read_progress,
            tracked_index: 0,
            min_ts: TimeStamp::zero(),
            stopped: false,
            last_attempt: None,
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

    fn shrink_ratio(&mut self, ratio: usize, timestamp: Option<TimeStamp>) {
        // HashMap load factor is 87% approximately, leave some margin to avoid
        // frequent rehash.
        //
        // See https://github.com/rust-lang/hashbrown/blob/v0.14.0/src/raw/mod.rs#L208-L220
        const MIN_SHRINK_RATIO: usize = 2;
        if self.locks_by_key.capacity()
            > self.locks_by_key.len() * cmp::max(MIN_SHRINK_RATIO, ratio)
        {
            self.locks_by_key.shrink_to_fit();
        }
        if let Some(ts) = timestamp && let Some(lock_set) = self.lock_ts_heap.get_mut(&ts)
            && lock_set.capacity() > lock_set.len() * cmp::max(MIN_SHRINK_RATIO, ratio) {
            lock_set.shrink_to_fit();
        }
    }

    // Return an approximate heap memory usage in bytes.
    pub fn approximate_heap_bytes(&self) -> usize {
        // memory used by locks_by_key.
        let memory_quota_in_use = self.memory_quota.in_use();

        // memory used by lock_ts_heap.
        let memory_lock_ts_heap = self.lock_ts_heap.len()
            * (std::mem::size_of::<TimeStamp>() + std::mem::size_of::<HashSet<Arc<[u8]>>>())
            // memory used by HashSet<Arc<u8>>
            + self.locks_by_key.len() * std::mem::size_of::<Arc<[u8]>>();

        memory_quota_in_use + memory_lock_ts_heap
    }

    fn lock_heap_size(&self, key: &[u8]) -> usize {
        // A resolver has
        // * locks_by_key: HashMap<Arc<[u8]>, TimeStamp>
        // * lock_ts_heap: BTreeMap<TimeStamp, HashSet<Arc<[u8]>>>
        //
        // We only count memory used by locks_by_key. Because the majority of
        // memory is consumed by keys, locks_by_key and lock_ts_heap shares
        // the same Arc<[u8]>, so lock_ts_heap is negligible. Also, it's hard to
        // track accurate memory usage of lock_ts_heap as a timestamp may have
        // many keys.
        key.heap_size() + std::mem::size_of::<TimeStamp>()
    }

    #[must_use]
    pub fn track_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>, index: Option<u64>) -> bool {
        if let Some(index) = index {
            self.update_tracked_index(index);
        }
        let bytes = self.lock_heap_size(&key);
        debug!(
            "track lock {}@{}",
            &log_wrappers::Value::key(&key),
            start_ts;
            "region_id" => self.region_id,
            "memory_in_use" => self.memory_quota.in_use(),
            "memory_capacity" => self.memory_quota.capacity(),
            "key_heap_size" => bytes,
        );
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
            let bytes = self.lock_heap_size(key);
            self.memory_quota.free(bytes);
            start_ts
        } else {
            debug!("untrack a lock that was not tracked before";
                "key" => &log_wrappers::Value::key(key),
                "region_id" => self.region_id,
            );
            return;
        };
        debug!(
            "untrack lock {}@{}",
            &log_wrappers::Value::key(key),
            start_ts;
            "region_id" => self.region_id,
            "memory_in_use" => self.memory_quota.in_use(),
        );

        let mut shrink_ts = None;
        if let Some(locked_keys) = self.lock_ts_heap.get_mut(&start_ts) {
            // Only shrink large set, because committing a small transaction is
            // fast and shrink adds unnecessary overhead.
            const SHRINK_SET_CAPACITY: usize = 256;
            if locked_keys.capacity() > SHRINK_SET_CAPACITY {
                shrink_ts = Some(start_ts);
            }
            locked_keys.remove(key);
            if locked_keys.is_empty() {
                self.lock_ts_heap.remove(&start_ts);
            }
        }
        // Use a large ratio to amortize the cost of rehash.
        let shrink_ratio = 8;
        self.shrink_ratio(shrink_ratio, shrink_ts);
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    /// Return None means the resolver is not initialized.
    pub fn resolve(
        &mut self,
        min_ts: TimeStamp,
        now: Option<Instant>,
        source: TsSource,
    ) -> TimeStamp {
        // Use a small ratio to shrink the memory usage aggressively.
        const AGGRESSIVE_SHRINK_RATIO: usize = 2;
        const AGGRESSIVE_SHRINK_INTERVAL: Duration = Duration::from_secs(10);
        if self.last_aggressive_shrink_time.saturating_elapsed() > AGGRESSIVE_SHRINK_INTERVAL {
            self.shrink_ratio(AGGRESSIVE_SHRINK_RATIO, None);
            self.last_aggressive_shrink_time = Instant::now_coarse();
        }

        // The `Resolver` is stopped, not need to advance, just return the current
        // `resolved_ts`
        if self.stopped {
            return self.resolved_ts;
        }

        // Find the min start ts.
        let min_lock = self
            .oldest_transaction()
            .and_then(|(ts, locks)| locks.iter().next().map(|lock| (*ts, lock)));
        let has_lock = min_lock.is_some();
        let min_start_ts = min_lock.map(|(ts, _)| ts).unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);
        // reason is the min source of the new resolved ts.
        let reason = match (min_lock, min_ts) {
            (Some(lock), min_ts) if lock.0 < min_ts => TsSource::Lock(lock.1.clone()),
            (Some(_), _) => source,
            (None, _) => source,
        };

        if self.resolved_ts >= new_resolved_ts {
            RTS_RESOLVED_FAIL_ADVANCE_VEC
                .with_label_values(&[reason.label()])
                .inc();
            self.last_attempt = Some(LastAttempt {
                success: false,
                ts: new_resolved_ts,
                reason,
            });
        } else {
            self.last_attempt = Some(LastAttempt {
                success: true,
                ts: new_resolved_ts,
                reason,
            })
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
                "sampled keys" => ?keys_for_log,
            );
        }
    }

    pub(crate) fn num_locks(&self) -> u64 {
        self.locks_by_key.len() as u64
    }

    pub(crate) fn num_transactions(&self) -> u64 {
        self.lock_ts_heap.len() as u64
    }

    pub(crate) fn oldest_transaction(&self) -> Option<(&TimeStamp, &HashSet<Arc<[u8]>>)> {
        self.lock_ts_heap.iter().next()
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
                            resolver.resolve(min_ts.into(), None, TsSource::PdTso),
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
    fn test_untrack_lock_shrink_ratio() {
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let mut resolver = Resolver::new(1, memory_quota);
        let mut key = vec![0; 16];
        let mut ts = TimeStamp::default();
        for _ in 0..1000 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            let _ = resolver.track_lock(ts, key.clone(), None);
        }
        assert!(
            resolver.locks_by_key.capacity() >= 1000,
            "{}",
            resolver.locks_by_key.capacity()
        );

        let mut ts = TimeStamp::default();
        for _ in 0..901 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            resolver.untrack_lock(&key, None);
        }
        // shrink_to_fit may reserve some space in accordance with the resize
        // policy, but it is expected to be less than 500.
        assert!(
            resolver.locks_by_key.capacity() < 500,
            "{}, {}",
            resolver.locks_by_key.capacity(),
            resolver.locks_by_key.len(),
        );

        for _ in 0..99 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            resolver.untrack_lock(&key, None);
        }
        assert!(
            resolver.locks_by_key.capacity() < 100,
            "{}, {}",
            resolver.locks_by_key.capacity(),
            resolver.locks_by_key.len(),
        );

        // Trigger aggressive shrink.
        resolver.last_aggressive_shrink_time = Instant::now_coarse() - Duration::from_secs(600);
        resolver.resolve(TimeStamp::new(0), None, TsSource::PdTso);
        assert!(
            resolver.locks_by_key.capacity() == 0,
            "{}, {}",
            resolver.locks_by_key.capacity(),
            resolver.locks_by_key.len(),
        );
    }

    #[test]
    fn test_untrack_lock_set_shrink_ratio() {
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let mut resolver = Resolver::new(1, memory_quota);
        let mut key = vec![0; 16];
        let ts = TimeStamp::new(1);
        for i in 0..1000usize {
            key[0..8].copy_from_slice(&i.to_be_bytes());
            let _ = resolver.track_lock(ts, key.clone(), None);
        }
        assert!(
            resolver.lock_ts_heap[&ts].capacity() >= 1000,
            "{}",
            resolver.lock_ts_heap[&ts].capacity()
        );

        for i in 0..990usize {
            key[0..8].copy_from_slice(&i.to_be_bytes());
            resolver.untrack_lock(&key, None);
        }
        // shrink_to_fit may reserve some space in accordance with the resize
        // policy, but it is expected to be less than 100.
        assert!(
            resolver.lock_ts_heap[&ts].capacity() < 500,
            "{}, {}",
            resolver.lock_ts_heap[&ts].capacity(),
            resolver.lock_ts_heap[&ts].len(),
        );
    }

    #[test]
    fn test_memory_quota() {
        let memory_quota = Arc::new(MemoryQuota::new(1024));
        let mut resolver = Resolver::new(1, memory_quota.clone());
        let mut key = vec![0; 77];
        let lock_size = resolver.lock_heap_size(&key);
        let mut ts = TimeStamp::default();
        while resolver.track_lock(ts, key.clone(), None) {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
        }
        let remain = 1024 % lock_size;
        assert_eq!(memory_quota.in_use(), 1024 - remain);

        let mut ts = TimeStamp::default();
        for _ in 0..5 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            resolver.untrack_lock(&key, None);
        }
        assert_eq!(memory_quota.in_use(), 1024 - 5 * lock_size - remain);
        drop(resolver);
        assert_eq!(memory_quota.in_use(), 0);
    }
}
