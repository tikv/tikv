// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::BTreeMap, sync::Arc, time::Duration};

use collections::{HashMap, HashMapEntry};
use raftstore::store::RegionReadProgress;
use tikv::storage::txn::txn_status_cache::{TxnState, TxnStatusCache};
use tikv_util::{
    memory::{MemoryQuota, MemoryQuotaExceeded},
    time::Instant,
};
use txn_types::{Key, TimeStamp};

use crate::metrics::*;

pub const ON_DROP_WARN_HEAP_SIZE: usize = 64 * 1024 * 1024; // 64MB

#[derive(Clone)]
pub enum TsSource {
    // A lock in LOCK CF
    Lock(TxnLocks),
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
            TsSource::Lock(locks) => locks
                .sample_lock
                .as_ref()
                .map(|k| Key::from_encoded_slice(k)),
            TsSource::MemoryLock(k) => Some(k.clone()),
            _ => None,
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
pub struct TxnLocks {
    pub lock_count: usize,
    // A sample key in a transaction.
    pub sample_lock: Option<Arc<[u8]>>,
}

impl std::fmt::Debug for TxnLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnLocks")
            .field("lock_count", &self.lock_count)
            .field(
                "sample_lock",
                &self
                    .sample_lock
                    .as_ref()
                    .map(|k| log_wrappers::Value::key(k)),
            )
            .finish()
    }
}

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    region_id: u64,
    // key -> start_ts
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,
    // start_ts -> locked keys.
    lock_ts_heap: BTreeMap<TimeStamp, TxnLocks>,
    // the start_ts and lock samples of large transactions, which use a different tracking strategy
    // from normal transactions.
    large_txns: HashMap<TimeStamp, TxnLocks>,
    // each large transaction tracked by this resolver has a representative key tracked. So that
    // when the large transaction is rolled back, we can rely on this key to guarantee that
    // eventually there will be orphaned transactions.
    large_txn_key_representative: HashMap<Vec<u8>, TimeStamp>,
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
    last_attempt: Option<LastAttempt>,
    txn_status_cache: Arc<TxnStatusCache>,
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

        if let Some((ts, txn_locks)) = far_lock {
            dt.field(&format_args!(
                "oldest_lock_count={:?}",
                txn_locks.lock_count
            ));
            dt.field(&format_args!(
                "oldest_lock_sample={:?}",
                txn_locks.sample_lock
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
    pub fn new(
        region_id: u64,
        memory_quota: Arc<MemoryQuota>,
        txn_status_cache: Arc<TxnStatusCache>,
    ) -> Resolver {
        Resolver::with_read_progress(region_id, None, memory_quota, txn_status_cache)
    }

    #[cfg(test)]
    fn new_for_test(region_id: u64, memory_quota: Arc<MemoryQuota>) -> Resolver {
        Resolver::with_read_progress(
            region_id,
            None,
            memory_quota,
            Arc::new(TxnStatusCache::new_for_test()),
        )
    }

    pub fn with_read_progress(
        region_id: u64,
        read_progress: Option<Arc<RegionReadProgress>>,
        memory_quota: Arc<MemoryQuota>,
        txn_status_cache: Arc<TxnStatusCache>,
    ) -> Resolver {
        Resolver {
            region_id,
            resolved_ts: TimeStamp::zero(),
            locks_by_key: HashMap::default(),
            lock_ts_heap: BTreeMap::new(),
            large_txns: Default::default(),
            large_txn_key_representative: HashMap::<Vec<u8>, TimeStamp>::default(),
            last_aggressive_shrink_time: Instant::now_coarse(),
            read_progress,
            tracked_index: 0,
            min_ts: TimeStamp::zero(),
            stopped: false,
            memory_quota,
            last_attempt: None,
            txn_status_cache,
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

    pub fn locks(&self) -> &BTreeMap<TimeStamp, TxnLocks> {
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

    // Return an approximate heap memory usage in bytes.
    pub fn approximate_heap_bytes(&self) -> usize {
        if self.locks_by_key.is_empty() {
            return 0;
        }

        const SAMPLE_COUNT: usize = 8;
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
                * (std::mem::size_of::<TimeStamp>() + std::mem::size_of::<TxnLocks>())
            + self
                .large_txn_key_representative
                .keys()
            .map(|k| k.len() * 2 /* count for the key in TxnLocks */ + std::mem::size_of::<TimeStamp>())
                .sum::<usize>()
            + self.large_txns.len() * (std::mem::size_of::<TimeStamp>() + std::mem::size_of::<TxnLocks>())
    }

    fn lock_heap_size(&self, key: &[u8]) -> usize {
        // A resolver has
        // * locks_by_key: HashMap<Arc<[u8]>, TimeStamp>
        // * lock_ts_heap: BTreeMap<TimeStamp, TxnLocks>
        //
        // We only count memory used by locks_by_key. Because the majority of
        // memory is consumed by keys, locks_by_key and lock_ts_heap shares
        // the same Arc<[u8]>, so lock_ts_heap is negligible. Also, it's hard to
        // track accurate memory usage of lock_ts_heap as a timestamp may have
        // many keys.
        std::mem::size_of_val(key) + std::mem::size_of::<TimeStamp>()
    }

    fn shrink_ratio(&mut self, ratio: usize) {
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
    }

    pub fn track_lock(
        &mut self,
        start_ts: TimeStamp,
        key: Vec<u8>,
        index: Option<u64>,
        generation: u64, /* generation is used to identify whether the lock is a pipelined
                          * transaction's lock */
    ) -> Result<(), MemoryQuotaExceeded> {
        if let Some(index) = index {
            self.update_tracked_index(index);
        }
        debug!(
            "track lock {}@{}",
            &log_wrappers::Value::key(&key),
            start_ts;
            "region_id" => self.region_id,
            "memory_in_use" => self.memory_quota.in_use(),
            "memory_capacity" => self.memory_quota.capacity(),
            "generation" => generation,
        );
        if generation == 0 {
            self.track_normal_lock(start_ts, key)?;
        } else {
            self.track_large_txn_lock(start_ts, key)?;
        }
        Ok(())
    }

    fn track_normal_lock(
        &mut self,
        start_ts: TimeStamp,
        key: Vec<u8>,
    ) -> Result<(), MemoryQuotaExceeded> {
        let bytes = self.lock_heap_size(&key);
        self.memory_quota.alloc(bytes)?;
        let key: Arc<[u8]> = key.into_boxed_slice().into();
        match self.locks_by_key.entry(key) {
            HashMapEntry::Occupied(_) => {
                // Free memory quota because it's already in the map.
                self.memory_quota.free(bytes);
            }
            HashMapEntry::Vacant(entry) => {
                // Add lock count for the start ts.
                let txn_locks = self.lock_ts_heap.entry(start_ts).or_insert_with(|| {
                    let mut txn_locks = TxnLocks::default();
                    txn_locks.sample_lock = Some(entry.key().clone());
                    txn_locks
                });
                txn_locks.lock_count += 1;

                entry.insert(start_ts);
            }
        }
        Ok(())
    }

    pub fn untrack_lock(&mut self, key: &[u8], index: Option<u64>) {
        if let Some(index) = index {
            self.update_tracked_index(index);
        }
        if let Some(start_ts) = self.locks_by_key.remove(key) {
            let bytes = self.lock_heap_size(key);
            self.memory_quota.free(bytes);
            debug!(
                "untrack lock {}@{}",
                &log_wrappers::Value::key(key),
                start_ts;
                "region_id" => self.region_id,
                "memory_in_use" => self.memory_quota.in_use(),
            );
            if let Some(txn_locks) = self.lock_ts_heap.get_mut(&start_ts) {
                if txn_locks.lock_count > 0 {
                    txn_locks.lock_count -= 1;
                }
                if txn_locks.lock_count == 0 {
                    self.lock_ts_heap.remove(&start_ts);
                }
            };
            // Use a large ratio to amortize the cost of rehash.
            let shrink_ratio = 8;
            self.shrink_ratio(shrink_ratio);
        } else if let Some(start_ts) = self.large_txn_key_representative.remove(key) {
            let entry = self.large_txns.remove(&start_ts);
            debug_assert!(
                entry.is_some(),
                "large txn lock should be untracked only once"
            );
            debug!(
                "untrack lock {}@{}",
                &log_wrappers::Value::key(key),
                start_ts;
                "region_id" => self.region_id,
                "memory_in_use" => self.memory_quota.in_use(),
            );
        } else {
            debug!("untrack a lock whose key is not tracked, should be from a pipelined transaction";
                "key" => &log_wrappers::Value::key(key),
                "region_id" => self.region_id,
            );
        }
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
            self.shrink_ratio(AGGRESSIVE_SHRINK_RATIO);
            self.last_aggressive_shrink_time = Instant::now_coarse();
        }

        // The `Resolver` is stopped, not need to advance, just return the current
        // `resolved_ts`
        if self.stopped {
            return self.resolved_ts;
        }

        // Find the min start ts.
        let min_lock = self.oldest_transaction();
        let has_lock = min_lock.is_some();
        let min_txn_ts = min_lock.as_ref().map(|(ts, _)| *ts).unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_txn_ts, min_ts);
        // reason is the min source of the new resolved ts.
        let reason = match (min_lock, min_ts) {
            (Some((lock_ts, txn_locks)), min_ts) if lock_ts < min_ts => TsSource::Lock(txn_locks),
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

    /// Logs the txns with min start_ts or min_commit_ts. Search from
    /// `lower_bound`. Normal txns are logged with start_ts.
    /// Large txns are logged with min_commit_ts.
    pub(crate) fn log_locks(&self, lower_bound: u64) {
        self.log_min_lock(lower_bound.into());
        self.log_min_large_txn(lower_bound.into());
    }

    fn log_min_lock(&self, lower_bound: TimeStamp) {
        if let Some((start_ts, txn_locks)) = self.lock_ts_heap.range(lower_bound..).next() {
            info!(
                "non-large txn locks with the minimum start_ts in resolver";
                "search_lower_bound" => lower_bound,
                "start_ts" => start_ts,
                "txn_locks" => ?txn_locks,
                "region_id" => self.region_id,
            );
        }
    }

    fn log_min_large_txn(&self, lower_bound: TimeStamp) {
        let min_min_commit_ts_txn = self
            .large_txns
            .iter()
            .filter_map(|(&start_ts, _)| {
                self.lookup_min_commit_ts(start_ts)
                    .map(|min_commit_ts| (start_ts, min_commit_ts))
            })
            .filter(|(_, min_commit_ts)| *min_commit_ts >= lower_bound)
            .min_by_key(|(_, min_commit_ts)| *min_commit_ts);

        if let Some((start_ts, min_commit_ts)) = min_min_commit_ts_txn {
            info!(
                "large txn locks with the minimum min_commit_ts in resolver";
                "search_lower_bound" => lower_bound,
                "start_ts" => start_ts,
                "min_commit_ts" => min_commit_ts,
                "region_id" => self.region_id,
            );
        }
    }

    // Map a transaction's start_ts to a min_commit_ts.
    // When a large txn is committed or rolled back, return None.
    // When not found in cache, fallback to its start_ts as start_ts is also a valid
    // min_commit_ts
    fn lookup_min_commit_ts(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        match self.txn_status_cache.get(start_ts) {
            None => {
                info!("Large txn not found in cache"; "start_ts" => start_ts);
                Some(start_ts)
            }
            // TODO: optimization: whenever a large txn is committed or rolled back, we can stop
            // tracking this txn
            Some(TxnState::Ongoing { min_commit_ts }) => Some(min_commit_ts),
            Some(TxnState::Committed { .. }) | Some(TxnState::RolledBack) => None,
        }
    }

    // This may be inaccurate for large transactions. But it's just for monitoring
    // and diagnosis.
    // The inaccuracy comes from untracking large txn locks, because we do not know
    // the ts when untracking a lock.
    pub(crate) fn num_locks(&self) -> u64 {
        (self.locks_by_key.len()
            + self
                .large_txns
                .values()
                .map(|locks| locks.lock_count)
                .sum::<usize>()) as u64
    }

    pub(crate) fn num_transactions(&self) -> u64 {
        (self.lock_ts_heap.len() + self.large_txns.len()) as u64
    }

    pub(crate) fn read_progress(&self) -> Option<&Arc<RegionReadProgress>> {
        self.read_progress.as_ref()
    }

    // Return the transaction with the smallest min_commit_ts. When min_commit_ts
    // is unknown, use start_ts instead.
    pub(crate) fn oldest_transaction(&self) -> Option<(TimeStamp, TxnLocks)> {
        let oldest_normal_txn = self
            .lock_ts_heap
            .iter()
            .next()
            .map(|(ts, txn_locks)| (ts, txn_locks.clone()));

        let oldest_large_txn = self
            .large_txns
            .iter()
            .filter_map(|(start_ts, txn_locks)| {
                self.lookup_min_commit_ts(*start_ts)
                    .map(|ts| (ts, txn_locks.clone()))
            })
            .min_by_key(|(ts, _)| *ts);

        match (oldest_normal_txn, oldest_large_txn) {
            (Some((&ts1, txn_locks1)), Some((ts2, txn_locks2))) => {
                if ts1 < ts2 {
                    Some((ts1, txn_locks1))
                } else {
                    Some((ts2, txn_locks2))
                }
            }
            (Some((&ts, txn_locks)), None) => Some((ts, txn_locks)),
            (None, Some((ts, txn_locks))) => Some((ts, txn_locks)),
            (None, None) => None,
        }
    }

    pub(crate) fn take_last_attempt(&mut self) -> Option<LastAttempt> {
        self.last_attempt.take()
    }

    fn track_large_txn_lock(
        &mut self,
        start_ts: TimeStamp,
        key: Vec<u8>,
    ) -> Result<(), MemoryQuotaExceeded> {
        self.large_txns
            .entry(start_ts)
            .and_modify(|entry| entry.lock_count += 1)
            .or_insert_with(|| {
                self.large_txn_key_representative
                    .insert(key.clone(), start_ts);
                TxnLocks {
                    lock_count: 1,
                    sample_lock: Some(key.into_boxed_slice().into()),
                }
            });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

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
            let mut resolver = Resolver::new_for_test(1, memory_quota);
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => {
                        resolver
                            .track_lock(start_ts.into(), key.into_raw().unwrap(), None, 0)
                            .unwrap();
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
    fn test_memory_quota() {
        let memory_quota = Arc::new(MemoryQuota::new(1024));
        let mut resolver = Resolver::new_for_test(1, memory_quota.clone());
        let mut key = vec![0; 77];
        let lock_size = resolver.lock_heap_size(&key);
        let mut ts = TimeStamp::default();
        while resolver.track_lock(ts, key.clone(), None, 0).is_ok() {
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

    #[test]
    fn test_untrack_lock_shrink_ratio() {
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let mut resolver = Resolver::new_for_test(1, memory_quota);
        let mut key = vec![0; 16];
        let mut ts = TimeStamp::default();
        for _ in 0..1000 {
            ts.incr();
            key[0..8].copy_from_slice(&ts.into_inner().to_be_bytes());
            let _ = resolver.track_lock(ts, key.clone(), None, 0);
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
    fn test_idempotent_track_and_untrack_lock() {
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let mut resolver = Resolver::new_for_test(1, memory_quota);
        let mut key = vec![0; 16];

        // track_lock
        let mut ts = TimeStamp::default();
        for c in 0..10 {
            ts.incr();
            for k in 0..100u64 {
                key[0..8].copy_from_slice(&k.to_be_bytes());
                key[8..16].copy_from_slice(&ts.into_inner().to_be_bytes());
                let _ = resolver.track_lock(ts, key.clone(), None, 0);
            }
            let in_use1 = resolver.memory_quota.in_use();
            let key_count1 = resolver.locks_by_key.len();
            let txn_count1 = resolver.lock_ts_heap.len();
            let txn_lock_count1 = resolver.lock_ts_heap[&ts].lock_count;
            assert!(in_use1 > 0);
            assert_eq!(key_count1, (c + 1) * 100);
            assert_eq!(txn_count1, c + 1);

            // Put same keys again, resolver internal state must be idempotent.
            for k in 0..100u64 {
                key[0..8].copy_from_slice(&k.to_be_bytes());
                key[8..16].copy_from_slice(&ts.into_inner().to_be_bytes());
                let _ = resolver.track_lock(ts, key.clone(), None, 0);
            }
            let in_use2 = resolver.memory_quota.in_use();
            let key_count2 = resolver.locks_by_key.len();
            let txn_count2 = resolver.lock_ts_heap.len();
            let txn_lock_count2 = resolver.lock_ts_heap[&ts].lock_count;
            assert_eq!(in_use1, in_use2);
            assert_eq!(key_count1, key_count2);
            assert_eq!(txn_count1, txn_count2);
            assert_eq!(txn_lock_count1, txn_lock_count2);
        }
        assert_eq!(resolver.resolve(ts, None, TsSource::PdTso), 1.into());

        // untrack_lock
        let mut ts = TimeStamp::default();
        for _ in 0..10 {
            ts.incr();
            for k in 0..100u64 {
                key[0..8].copy_from_slice(&k.to_be_bytes());
                key[8..16].copy_from_slice(&ts.into_inner().to_be_bytes());
                resolver.untrack_lock(&key, None);
            }
            let in_use1 = resolver.memory_quota.in_use();
            let key_count1 = resolver.locks_by_key.len();
            let txn_count1 = resolver.lock_ts_heap.len();

            // Unlock same keys again, resolver internal state must be idempotent.
            for k in 0..100u64 {
                key[0..8].copy_from_slice(&k.to_be_bytes());
                key[8..16].copy_from_slice(&ts.into_inner().to_be_bytes());
                resolver.untrack_lock(&key, None);
            }
            let in_use2 = resolver.memory_quota.in_use();
            let key_count2 = resolver.locks_by_key.len();
            let txn_count2 = resolver.lock_ts_heap.len();
            assert_eq!(in_use1, in_use2);
            assert_eq!(key_count1, key_count2);
            assert_eq!(txn_count1, txn_count2);

            assert_eq!(resolver.resolve(ts, None, TsSource::PdTso), ts);
        }

        assert_eq!(resolver.memory_quota.in_use(), 0);
        assert_eq!(resolver.locks_by_key.len(), 0);
        assert_eq!(resolver.lock_ts_heap.len(), 0);
    }

    #[test]
    fn test_large_txn_tracking() {
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let txn_status_cache = Arc::new(TxnStatusCache::new(100));
        let mut resolver = Resolver::new(1, memory_quota, txn_status_cache.clone());
        let key1: Vec<u8> = vec![1, 2, 3, 4];
        let key2: Vec<u8> = vec![5, 6, 7, 8];
        let key3: Vec<u8> = vec![9, 10, 11, 12];

        // track 2 large txns, T1{key1}, T2{key2, key3}
        resolver
            .track_lock(1.into(), key1.clone(), None, 1)
            .unwrap();
        resolver
            .track_lock(2.into(), key2.clone(), None, 1)
            .unwrap();
        resolver.track_lock(2.into(), key3, None, 2).unwrap();
        assert_eq!(resolver.num_locks(), 3);
        assert_eq!(resolver.num_transactions(), 2);
        assert_eq!(resolver.locks_by_key.len(), 0);
        assert_eq!(resolver.large_txns.len(), 2);
        assert_eq!(resolver.large_txn_key_representative.len(), 2);
        assert_eq!(resolver.resolved_ts(), TimeStamp::zero());

        assert_eq!(resolver.resolve(20.into(), None, TsSource::PdTso), 1.into());

        txn_status_cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 10.into(),
            },
            SystemTime::now(),
        );
        txn_status_cache.upsert(
            2.into(),
            TxnState::Ongoing {
                min_commit_ts: 5.into(),
            },
            SystemTime::now(),
        );

        assert_eq!(resolver.resolve(20.into(), None, TsSource::PdTso), 5.into());
        let oldest_txn = resolver.oldest_transaction().unwrap();
        assert_eq!(oldest_txn.0, 5.into());
        assert_eq!(oldest_txn.1.lock_count, 2);
    }
}
