// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements a cache for the status of recent finished
//! transactions. When a transaction is committed or rolled back, we store the
//! information in the cache for a while. Later, in some cases, one can find
//! the transaction status without accessing the physical storage. This helps
//! to quickly find out the transaction status in some cases.
//!
//! > **Note:**
//! > * Currently, only committed transactions are cached. We may also cache
//! > rolled-back transactions in the future.
//! > * Currently, the cache is only used to filter unnecessary stale prewrite
//! > requests. We will also consider use the cache for other purposes in the
//! > future.
//!
//! ## Why we need this?
//!
//! ### For filtering out unwanted late-arrived stale prewrite requests
//!
//! This solves a problem which has a complicated background.
//!
//! There's such an optimization in pessimistic transactions when TiKV runs
//! accompanied with TiDB: non-unique index keys don't need to be pessimistic-
//! locked, and WRITE CF don't need to be checked either when prewriting. The
//! correctness in case there's any kinds of conflicts will be protected by
//! the corresponding row key, as the index key is never written without
//! writing the corresponding row key.
//!
//! However, it's later found to be problematic, especially with async commit
//! and 1PC, as the prewrite requests on these index keys lost its idempotency.
//! You can see [this issue](https://github.com/tikv/tikv/issues/11187) to see
//! how it causes problems, including those that affects transaction
//! correctness.
//!
//! The problem happens when the prewrite request to the same index key is
//! sent more than once. Our first solution is to add a `is_retry_request` flag
//! to the second (or even more) requests, which is sent due to retrying from
//! the client side. But it's still imperfect, considering that it's
//! theoretically possible that the original request arrives to TiKV later than
//! the retried one. In fact, we once observed this happens in an environment
//! where the network is terribly unstable.
//!
//! Our second solution, additional to the previous one, is to use this cache.
//! Each committed transaction should be guaranteed to be kept in the cache for
//! [a long-enough time](CACHE_ITEMS_REQUIRED_KEEP_TIME). When a prewrite
//! request is received, it should check the cache before executing. If it finds
//! its belonging transaction is already committed, it won't skip constraint
//! check in WRITE CF. Note that if the index key is already committed but the
//! transaction info is not cached, then a late-arrived prewrite request cannot
//! be protected by this mechanism. This means we shouldn't miss any cacheable
//! transactions, and it is the reason why committed transactions should be
//! cached for *a long-enough time*.
//!
//! Unfortunately, the solution is still imperfect. As it's already known, it
//! may still be problematic due to the following reasons:
//!
//! 1. We don't have mechanism to refuse requests that have
//! past more than [CACHE_ITEMS_REQUIRED_KEEP_TIME] since they were sent.
//! 2. To prevent the cache from consuming too much more memory than expected,
//! we have a limit to the capacity (though the limit is very large), and it's
//! configurable (so the cache can be disabled, see how the `capacity` parameter
//! of function [TxnStatusCache::new] is used) as a way to escape from potential
//! faults.
//! 3. The cache can't be synced across different TiKV instances.
//!
//! The third case above needs detailed explanation to be clarified. This is
//! an example of the problem:
//!
//! 1. Client try to send prewrite request to TiKV A, who has the leader of the
//! region containing a index key. The request is not received by TiKV and the
//! client retries.
//! 2. The leader is transferred to TiKV B, and the retries prewrite request
//! is sent to it and processed successfully.
//! 3. The transaction is committed on TiKV B, not being known by TiKV A.
//! 4. The leader transferred back to TiKV A.
//! 5. The original request arrives to TiKV A and being executed. As the
//! status of the transaction is not in the cache in TiKV A, the prewrite
//! request will be handled in normal way, skipping constraint checks.
//!
//! As of the time when this module is written, the above remaining cases have
//! not yet been handled, considering the extremely low possibility to happen
//! and high complexity to fix.
//!
//! The perfect and most elegant way to fix all of these problem is never to
//! skip constraint checks or never skipping pessimistic locks for index keys.
//! Or to say, totally remove the optimization mentioned above on index keys.
//! But for historical reason, this may lead to significant performance
//! regression in existing clusters.
//!
//! ### For read data locked by large transactions more efficiently
//!
//! * Note: the `TxnStatusCache` is designed prepared for this usage, but not
//! used yet for now.
//!
//! Consider the case that a very-large transaction locked a lot of keys after
//! prewriting, while many simple reads and writes executes frequently, thus
//! these simple transactions frequently meets the lock left by the large
//! transaction. It will be very inefficient for these small transactions to
//! come back to the client and start resolve lock procedure. Even if the client
//! side has the cache of that transaction, it still wastes an RTT.
//!
//! There would be more possibilities if we have such a cache in TiKV side: for
//! read requests, it can check the cache to know whether it can read from the
//! lock; and for write requests, if it finds the transaction of that lock is
//! already committed, it can merge together the resolve-lock-committing and the
//! write operation that the request needs to perform.
//!
//! ## Design Update-1: Dual-Cache System
//!
//! Goal: maximize all TiKV nodes' awareness of large pipelined transactions
//! during their lifetime, i.e. from their first writes to all locks being
//! committed. This is crucial to resolved-ts resolver to handle locks belonging
//! to large transactions.
//!
//! The txn_status_cache is then split into two independent parts.
//! 1. `normal_cache`: For most transactions, including committed and rolled
//!    back transactions.
//! 2. `large_txn_cache`: Specifically for ongoing large transactions.
//!
//! ### Key characteristics:
//!
//! - Large Transaction Identification: Large transactions are identified if
//!   their `start_ts` and `min_commit_ts` differ. Non-large transactions which
//!   request to be cached in the cache can also be treated as large
//!   transactions, as they imply their min_commit_ts are useful.
//!
//! - Prioritized Caching: The `large_txn_cache` has a higher priority when
//!   upserting and retrieving transaction status.
//!
//! This dual-cache design allows for more efficient handling of both normal and
//! large transactions, preventing either type from dominating the cache and
//! evicting information about transactions of the other type.
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossbeam::utils::CachePadded;
use parking_lot::Mutex;
use tikv_util::{
    lru,
    lru::{GetTailEntry, LruCache},
};
use txn_types::TimeStamp;

use crate::storage::metrics::*;

const TXN_STATUS_CACHE_SLOTS: usize = 128;

/// A cache item should be kept for at least this time.
/// Actually this should be guaranteed only for committed transactions. See
/// [this section](#
/// for-filtering-out-unwanted-late-arrived-stale-prewrite-requests) for details
/// about why this is needed.
const CACHE_ITEMS_REQUIRED_KEEP_TIME: Duration = Duration::from_secs(30);
const CACHE_ITEMS_REQUIRED_KEEP_TIME_FOR_LARGE_TXNS: Duration = Duration::from_secs(30);

pub struct CacheEntry {
    state: TxnState,
    /// The system timestamp in milliseconds when the entry is inserted to the
    /// cache.
    update_time: u64,
}

impl CacheEntry {
    pub(crate) fn commit_ts(&self) -> Option<TimeStamp> {
        match self.state {
            TxnState::Committed { commit_ts } => Some(commit_ts),
            _ => None,
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, Eq)]
pub enum TxnState {
    Ongoing { min_commit_ts: TimeStamp },
    Committed { commit_ts: TimeStamp },
    RolledBack,
}

/// Defines the policy to evict expired entries from the cache.
/// [`TxnStatusCache`] needs to keep entries for a while, so the common
/// policy that only limiting capacity is not proper to be used here.
struct TxnStatusCacheEvictPolicy {
    required_keep_time_millis: u64,
    #[cfg(test)]
    simulated_system_time: Option<Arc<AtomicU64>>,
}

impl TxnStatusCacheEvictPolicy {
    fn new(
        required_keep_time: Duration,
        #[allow(unused_variables)] simulated_system_time: Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            required_keep_time_millis: required_keep_time.as_millis() as u64,
            #[cfg(test)]
            simulated_system_time,
        }
    }

    #[inline]
    #[cfg(not(test))]
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    /// When used in tests, the system time can be simulated by controlling the
    /// field `simulated_system_time`.
    #[inline]
    #[cfg(test)]
    fn now(&self) -> SystemTime {
        // Always get the system time to simulate the latency.
        let now = SystemTime::now();
        if let Some(pseudo_system_time) = &self.simulated_system_time {
            UNIX_EPOCH
                + std::time::Duration::from_millis(
                    pseudo_system_time.load(std::sync::atomic::Ordering::Acquire),
                )
        } else {
            now
        }
    }
}

impl lru::EvictPolicy<TimeStamp, CacheEntry> for TxnStatusCacheEvictPolicy {
    fn should_evict(
        &self,
        current_size: usize,
        capacity: usize,
        get_tail_entry: &impl GetTailEntry<TimeStamp, CacheEntry>,
    ) -> bool {
        // See how much time has been elapsed since the tail entry is inserted.
        // If it's long enough, remove it.
        if let Some((_, v)) = get_tail_entry.get_tail_entry() {
            if self.now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                > self.required_keep_time_millis + v.update_time
            {
                return true;
            }
        }

        // If the capacity limit is exceeded, remove it.
        current_size > capacity
    }
}

type TxnStatusCacheSlot =
    LruCache<TimeStamp, CacheEntry, lru::CountTracker, TxnStatusCacheEvictPolicy>;

/// The cache for storing transaction status. It holds recent
/// `start_ts` -> `commit_ts` pairs for a while, which can be useful for quickly
/// but not strictly determining transaction status.
///
/// `TxnStatusCache` is divided into several slots
/// to make the lock more fine-grained. Each slot uses an [`LruCache`] as the
/// internal implementation, with customized evict policy. However, we do not
/// always adopt the LRU behavior. Some operation to an existing entry in the
/// cache won't promote it to the most-recent place.
///
/// Note that the `TxnStatusCache` updates metrics in some operations assuming
/// there's at most one instance of `TxnStatusCache` in a process.
pub struct TxnStatusCache {
    // default cache for committed txns
    normal_cache: Vec<CachePadded<Mutex<TxnStatusCacheSlot>>>,
    // for large txns, or any txn whose min_commit_ts needs to be cached
    // It is isolated from the normal cache to prevents large transactions from being evicted due
    // to normal transactions. This is how this module "prioritizes" large transactions.
    large_txn_cache: Vec<CachePadded<Mutex<TxnStatusCacheSlot>>>,
    is_enabled: bool,
}

unsafe impl Sync for TxnStatusCache {}

impl TxnStatusCache {
    fn new_impl(
        slots: usize,
        required_keep_time: Duration,
        large_txn_required_keep_time: Duration,
        capacity: usize,
        large_txn_capacity: usize,
        simulated_system_time: Option<Arc<AtomicU64>>,
    ) -> Self {
        if capacity == 0 {
            return Self {
                normal_cache: vec![],
                large_txn_cache: vec![],
                is_enabled: false,
            };
        }

        // The limit of the LruCache of each slot.
        let allowed_capacity_per_slot = capacity / slots;
        let capacity_per_slot_for_large_txns = large_txn_capacity / slots;
        // The total memory allocated initially by the LruCache's internal data
        // structure for all slots.

        let mut initial_allocated_capacity_total = 0;
        let res = Self {
            normal_cache: (0..slots)
                .map(|_| {
                    let cache = LruCache::new(
                        allowed_capacity_per_slot,
                        0,
                        lru::CountTracker::default(),
                        TxnStatusCacheEvictPolicy::new(
                            required_keep_time,
                            simulated_system_time.clone(),
                        ),
                    );
                    let allocated_capacity = cache.internal_allocated_capacity();
                    initial_allocated_capacity_total += allocated_capacity;
                    Mutex::new(cache).into()
                })
                .collect(),
            large_txn_cache: (0..slots)
                .map(|_| {
                    let cache = LruCache::new(
                        capacity_per_slot_for_large_txns,
                        0,
                        lru::CountTracker::default(),
                        TxnStatusCacheEvictPolicy::new(
                            large_txn_required_keep_time,
                            simulated_system_time.clone(),
                        ),
                    );
                    let allocated_capacity = cache.internal_allocated_capacity();
                    initial_allocated_capacity_total += allocated_capacity;
                    Mutex::new(cache).into()
                })
                .collect(),
            is_enabled: true,
        };
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .set(initial_allocated_capacity_total as i64);
        res
    }

    pub fn new(capacity: usize) -> Self {
        Self::with_slots_and_time_limit(
            TXN_STATUS_CACHE_SLOTS,
            CACHE_ITEMS_REQUIRED_KEEP_TIME,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_FOR_LARGE_TXNS,
            capacity,
        )
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        // 1M capacity should be enough for tests.
        Self::with_slots_and_time_limit(
            16,
            CACHE_ITEMS_REQUIRED_KEEP_TIME,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_FOR_LARGE_TXNS,
            1 << 20,
        )
    }

    pub fn with_slots_and_time_limit(
        slots: usize,
        required_keep_time: Duration,
        large_txn_required_keep_time: Duration,
        capacity: usize,
    ) -> Self {
        Self::new_impl(
            slots,
            required_keep_time,
            large_txn_required_keep_time,
            capacity,
            capacity,
            None,
        )
    }

    /// Create a `TxnStatusCache` instance for test purpose, with simulating
    /// system time enabled. This helps when testing functionalities that are
    /// related to system time.
    ///
    /// An `AtomicU64` will be returned. Store timestamps
    /// in milliseconds in it to control the time.
    #[cfg(test)]
    fn with_simulated_system_time(
        slots: usize,
        required_keep_time: Duration,
        capacity: usize,
    ) -> (Self, Arc<AtomicU64>) {
        let system_time = Arc::new(AtomicU64::new(0));
        let res = Self::new_impl(
            slots,
            required_keep_time,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_FOR_LARGE_TXNS,
            capacity,
            capacity,
            Some(system_time.clone()),
        );
        (res, system_time)
    }

    fn slot_index(&self, start_ts: TimeStamp) -> usize {
        fxhash::hash(&start_ts) % self.normal_cache.len()
    }

    /// Insert a transaction status into the cache, or update it. The current
    /// system time should be passed from outside to avoid getting system time
    /// repeatedly when multiple items are being inserted.
    pub fn upsert(&self, start_ts: TimeStamp, state: TxnState, now: SystemTime) {
        if !self.is_enabled {
            return;
        }

        let update_time = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let entry = CacheEntry { state, update_time };
        let slot_index = self.slot_index(start_ts);

        let previous_size;
        let after_size;
        let previous_allocated;
        let after_allocated;
        match &state {
            // large cache path
            TxnState::Ongoing { min_commit_ts } if *min_commit_ts > start_ts => {
                // The large_txn_cache takes precedence over the normal_cache in the retrieval
                // process.
                // This case implies that min_commit_ts would be useful, thus the entry is put
                // in the large cache.
                let mut large_txn_cache = self.large_txn_cache[slot_index].lock();
                previous_size = large_txn_cache.size();
                previous_allocated = large_txn_cache.internal_allocated_capacity();
                if let Some(existing_entry) = large_txn_cache.get_mut(&start_ts) {
                    // don't update committed or rolled back txns.
                    if let TxnState::Ongoing {
                        min_commit_ts: existing_min_commit_ts,
                    } = existing_entry.state
                    {
                        if *min_commit_ts > existing_min_commit_ts {
                            existing_entry.state = state;
                            existing_entry.update_time = update_time;
                        }
                    }
                } else {
                    large_txn_cache.insert(start_ts, entry);
                }
                after_size = large_txn_cache.size();
                after_allocated = large_txn_cache.internal_allocated_capacity();
            }
            // normal cache path
            _ => {
                let mut normal_cache = self.normal_cache[slot_index].lock();
                previous_size = normal_cache.size();
                previous_allocated = normal_cache.internal_allocated_capacity();
                if let Some(existing_entry) = normal_cache.get_mut(&start_ts) {
                    // don't update committed or rolled back txns.
                    if let TxnState::Ongoing { min_commit_ts } = existing_entry.state {
                        if let TxnState::Committed { commit_ts } = state {
                            assert!(min_commit_ts <= commit_ts);
                        }
                        existing_entry.state = state;
                        existing_entry.update_time = update_time;
                    }
                } else {
                    normal_cache.insert(start_ts, entry);
                }
                after_size = normal_cache.size();
                after_allocated = normal_cache.internal_allocated_capacity();

                let mut large_cache = self.large_txn_cache[slot_index].lock();
                if let Some(existing_entry) = large_cache.get_mut(&start_ts) {
                    // don't update committed or rolled back txns.
                    if let TxnState::Ongoing { min_commit_ts } = existing_entry.state {
                        if let TxnState::Committed { commit_ts } = state {
                            assert!(min_commit_ts <= commit_ts);
                        }
                        existing_entry.state = state;
                        existing_entry.update_time = update_time;
                    }
                }
            }
        }
        // Update statistics.
        // CAUTION: Assuming that only one TxnStatusCache instance is in a TiKV process.
        SCHED_TXN_STATUS_CACHE_SIZE
            .used
            .add(after_size as i64 - previous_size as i64);
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .add(after_allocated as i64 - previous_allocated as i64);
    }

    /// Insert a committed txn into the normal cache.
    pub fn insert_committed(&self, start_ts: TimeStamp, commit_ts: TimeStamp, now: SystemTime) {
        let state = TxnState::Committed { commit_ts };
        self.upsert(start_ts, state, now);
    }

    pub fn get(&self, start_ts: TimeStamp) -> Option<TxnState> {
        if !self.is_enabled {
            return None;
        }

        let slot_index = self.slot_index(start_ts);

        // large txn cache should be queried first, because it has a higher priority
        // when upserting. It's possible for a start_ts to exist in both caches.
        // The one in large_txn_cache is more up-to-date.
        let mut large_txn_cache = self.large_txn_cache[slot_index].lock();
        if let Some(entry) = large_txn_cache.get(&start_ts) {
            return Some(entry.state);
        }

        let mut normal_cache = self.normal_cache[slot_index].lock();
        if let Some(entry) = normal_cache.get(&start_ts) {
            return Some(entry.state);
        }

        None
    }

    /// Try to get the committed txn from the normal cache, without promoting
    /// the item (if exists) to the most recent place.
    /// If the txn is ongoing or rolled back, returns None.
    pub fn get_committed_no_promote(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        if !self.is_enabled {
            return None;
        }

        let slot = self.normal_cache[self.slot_index(start_ts)].lock();
        slot.get_no_promote(&start_ts)
            .and_then(|entry| entry.commit_ts())
    }

    /// Try to get the committed txn from the normal cache.
    /// If the txn is ongoing or rolled back, returns None.
    pub fn get_committed(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        if !self.is_enabled {
            return None;
        }

        let mut slot = self.normal_cache[self.slot_index(start_ts)].lock();
        slot.get(&start_ts).and_then(|entry| entry.commit_ts())
    }

    /// Removes a txn from the normal cache. We usually don't need to
    /// remove anything from the normal cache, but it's useful in tests
    /// to construct cache-miss cases.
    /// Return its commit_ts if it's committed; return None otherwise.
    #[cfg(test)]
    pub fn remove_normal(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        if !self.is_enabled {
            return None;
        }

        let res = {
            let mut slot = self.normal_cache[self.slot_index(start_ts)].lock();
            slot.remove(&start_ts).and_then(|e| e.commit_ts())
        };
        debug_assert!(self.get_committed_no_promote(start_ts).is_none());
        res
    }

    /// Remove a txn from the large-txn cache.
    /// This could happen when the large txn finishes its
    /// secondary commit phase, i.e. all locks have been cleared. There is no
    /// need to keep this txn in cache any longer.
    pub fn remove_large_txn(&self, start_ts: TimeStamp) -> Option<CacheEntry> {
        if !self.is_enabled {
            return None;
        }
        let mut slot = self.large_txn_cache[self.slot_index(start_ts)].lock();
        slot.remove(&start_ts)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant, SystemTime},
    };

    use rand::{prelude::SliceRandom, Rng};

    use super::*;

    fn bench_insert_impl(b: &mut test::Bencher, init_size: usize) {
        let (c, time) = TxnStatusCache::with_simulated_system_time(
            TXN_STATUS_CACHE_SLOTS,
            Duration::from_millis(init_size as u64),
            1 << 20,
        );
        let start_time = SystemTime::now();
        // Spread these items evenly in a specific time limit, so that every time
        // a new item is inserted, an item will be popped out.
        for i in 1..=init_size {
            c.insert_committed(
                (i as u64).into(),
                (i as u64 + 1).into(),
                start_time + Duration::from_millis(i as u64),
            );
        }
        let mut current_time_shift = (init_size + 1) as u64;
        b.iter(|| {
            let simulated_now = start_time + Duration::from_millis(current_time_shift);
            // Simulate the system time advancing.
            time.store(
                simulated_now
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                Ordering::Release,
            );
            c.insert_committed(
                current_time_shift.into(),
                (current_time_shift + 1).into(),
                simulated_now,
            );
            current_time_shift += 1;
        });
        test::black_box(&c);
    }

    fn bench_get_impl(b: &mut test::Bencher, init_size: usize) {
        let c = TxnStatusCache::with_slots_and_time_limit(
            TXN_STATUS_CACHE_SLOTS,
            CACHE_ITEMS_REQUIRED_KEEP_TIME,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_FOR_LARGE_TXNS,
            1 << 20,
        );
        let now = SystemTime::now();
        for i in 1..=init_size {
            c.insert_committed(
                (i as u64).into(),
                (i as u64 + 1).into(),
                now + Duration::from_millis(i as u64),
            );
        }
        let rand_range = if init_size == 0 { 10000 } else { init_size } as u64;
        b.iter(|| {
            let ts = rand::thread_rng().gen_range(0u64, rand_range);
            let res = c.get_committed_no_promote(ts.into());
            test::black_box(&res);
        })
    }

    #[bench]
    fn bench_insert_empty(b: &mut test::Bencher) {
        bench_insert_impl(b, 0);
    }

    #[bench]
    fn bench_insert_100000(b: &mut test::Bencher) {
        bench_insert_impl(b, 100000);
    }

    #[bench]
    fn bench_get_empty(b: &mut test::Bencher) {
        bench_get_impl(b, 0);
    }

    #[bench]
    fn bench_get_100000(b: &mut test::Bencher) {
        bench_get_impl(b, 100000);
    }

    /// A simple statistic tool for collecting a set of data and calculating the
    /// average, stddev, and percentiles (by using a linear histogram).
    /// Data is collected in u128, and results are given in f64.
    struct SimpleStatistics {
        sum: u128,
        sum_square: u128,
        count: usize,
        bucket_width: u128,
        buckets: Vec<usize>,
    }

    impl SimpleStatistics {
        fn new(bucket_width: u128) -> Self {
            Self {
                sum: 0,
                sum_square: 0,
                count: 0,
                bucket_width,
                buckets: vec![],
            }
        }

        /// Merge another instance into the current one
        fn add(&mut self, other: Self) {
            self.sum += other.sum;
            self.sum_square += other.sum_square;
            self.count += other.count;
            assert_eq!(self.bucket_width, other.bucket_width);
            if self.buckets.len() < other.buckets.len() {
                self.buckets.resize(other.buckets.len(), 0);
            }
            for (count, other_count) in self.buckets.iter_mut().zip(other.buckets.iter()) {
                *count += *other_count
            }
        }

        fn avg(&self) -> f64 {
            self.sum as f64 / (self.count as f64)
        }

        fn stddev(&self) -> f64 {
            let avg = self.avg();
            let sum_sqr_diff: f64 =
                (self.sum_square as f64) - (self.sum as f64 * avg * 2.0) + avg * self.count as f64;
            (sum_sqr_diff / (self.count - 1) as f64).sqrt()
        }

        /// Calculate the percentile value at specified position (should be in
        /// range [0, 1])
        fn percentile(&self, position: f64) -> f64 {
            let mut bucket = self.buckets.len();
            let mut prefix_sum = self.count;
            while bucket > 0 {
                bucket -= 1;
                prefix_sum -= self.buckets[bucket];
                let prefix_percentile = prefix_sum as f64 / self.count as f64;
                if prefix_percentile <= position {
                    assert_le!(prefix_sum as f64, position * self.count as f64);
                    assert_lt!(
                        position * self.count as f64,
                        (prefix_sum + self.buckets[bucket]) as f64
                    );
                    break;
                }
            }

            bucket as f64 * self.bucket_width as f64
                + (position * self.count as f64 - prefix_sum as f64) * self.bucket_width as f64
                    / self.buckets[bucket] as f64
        }

        fn observe(&mut self, value: u128) {
            self.sum += value;
            self.sum_square += value * value;
            self.count += 1;
            let bucket = (value / self.bucket_width) as usize;
            if self.buckets.len() <= bucket {
                self.buckets.resize(bucket + 1, 0);
            }
            self.buckets[bucket] += 1;
        }
    }

    fn bench_concurrent_impl<T>(
        name: &str,
        threads: usize,
        function: impl Fn(u64) -> T + Send + Sync + 'static,
    ) {
        let start_time = Instant::now();
        // Run the benchmark code repeatedly for 10 seconds.
        const TIME_LIMIT: Duration = Duration::from_secs(10);
        let iteration = Arc::new(AtomicU64::new(0));

        // Make the lifetime checker happy.
        let function = Arc::new(function);

        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let f = function.clone();
            let iteration = iteration.clone();
            let handle = std::thread::spawn(move || {
                let mut stats = SimpleStatistics::new(20);
                loop {
                    if start_time.elapsed() > TIME_LIMIT {
                        break;
                    }
                    let i = iteration.fetch_add(1, Ordering::SeqCst);
                    let iter_start_time = Instant::now();
                    test::black_box(f(i));
                    let duration = iter_start_time.elapsed();
                    stats.observe(duration.as_nanos());
                }
                stats
            });
            handles.push(handle);
        }

        let mut total_stats = SimpleStatistics::new(20);
        for h in handles {
            total_stats.add(h.join().unwrap());
        }

        println!(
            "benchmark {}: duration per iter: avg: {:?}, stddev: {:?}, percentile .99: {:?}, percentile .999: {:?}",
            name,
            Duration::from_nanos(total_stats.avg() as u64),
            Duration::from_nanos(total_stats.stddev() as u64),
            Duration::from_nanos(total_stats.percentile(0.99) as u64),
            Duration::from_nanos(total_stats.percentile(0.999) as u64),
        );
    }

    fn bench_txn_status_cache_concurrent_impl(
        threads: usize,
        init_size: usize,
        simulate_contention: bool,
        get_before_insert: bool,
    ) {
        let slots = if simulate_contention {
            1
        } else {
            TXN_STATUS_CACHE_SLOTS
        };
        let (c, time) = TxnStatusCache::with_simulated_system_time(
            slots,
            Duration::from_millis(init_size as u64),
            1 << 20,
        );
        let start_time = SystemTime::now();
        for i in 1..=init_size {
            c.insert_committed(
                (i as u64).into(),
                (i as u64 + 1).into(),
                start_time + Duration::from_millis(i as u64),
            );
        }

        let name = format!(
            "bench_concurrent_{}_{}_size{}{}",
            if get_before_insert {
                "get_and_insert"
            } else {
                "insert"
            },
            threads,
            init_size,
            if simulate_contention {
                "_contention"
            } else {
                ""
            },
        );

        bench_concurrent_impl(&name, threads, move |iter| {
            let time_shift = init_size as u64 + iter;
            let now = start_time + Duration::from_millis(time_shift);
            time.store(
                now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                Ordering::Release,
            );

            if get_before_insert {
                test::black_box(c.get_committed_no_promote(time_shift.into()));
            }
            c.insert_committed(time_shift.into(), (time_shift + 1).into(), now);
            test::black_box(&c);
        });
    }

    #[bench]
    #[ignore]
    fn bench_txn_status_cache_concurrent(_b: &mut test::Bencher) {
        // This case is implemented to run the concurrent benchmark in a handy way
        // just like running other normal benchmarks. However, it doesn't seem
        // to be possible to benchmark an operation in concurrent way by using
        // either the built-in bencher or criterion.
        // Here we test it in our own way without using the built-in bencher,
        // and output the result by stdout.
        // When you need to run this benchmark, comment out the `#[ignore]` and
        // add --nocapture in your benchmark command line to get the result.
        bench_txn_status_cache_concurrent_impl(16, 10000, false, false);
        bench_txn_status_cache_concurrent_impl(16, 10000, true, false);
        bench_txn_status_cache_concurrent_impl(16, 10000, false, true);
        bench_txn_status_cache_concurrent_impl(16, 10000, true, true);
        bench_txn_status_cache_concurrent_impl(64, 10000, false, false);
        bench_txn_status_cache_concurrent_impl(64, 10000, true, false);
        bench_txn_status_cache_concurrent_impl(64, 10000, false, true);
        bench_txn_status_cache_concurrent_impl(64, 10000, true, true);
    }

    #[test]
    fn test_insert_and_get() {
        let c = TxnStatusCache::new_for_test();
        assert!(c.get_committed_no_promote(1.into()).is_none());

        let now = SystemTime::now();

        c.insert_committed(1.into(), 2.into(), now);
        assert_eq!(c.get_committed_no_promote(1.into()).unwrap(), 2.into());
        c.insert_committed(3.into(), 4.into(), now);
        assert_eq!(c.get_committed_no_promote(3.into()).unwrap(), 4.into());

        let mut start_ts_list: Vec<_> = (1..100).step_by(2).map(TimeStamp::from).collect();
        start_ts_list.shuffle(&mut rand::thread_rng());
        for &start_ts in &start_ts_list {
            let commit_ts = start_ts.next();
            c.insert_committed(start_ts, commit_ts, now);
        }
        start_ts_list.shuffle(&mut rand::thread_rng());
        for &start_ts in &start_ts_list {
            let commit_ts = start_ts.next();
            assert_eq!(c.get_committed_no_promote(start_ts).unwrap(), commit_ts);
        }
    }

    #[test]
    fn test_evicting_expired() {
        let (c, time) =
            TxnStatusCache::with_simulated_system_time(1, Duration::from_millis(1000), 1000);
        let time_base = SystemTime::now();
        let set_time = |offset_millis: u64| {
            time.store(
                time_base.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 + offset_millis,
                Ordering::Release,
            )
        };
        let now = || UNIX_EPOCH + Duration::from_millis(time.load(Ordering::Acquire));

        set_time(0);
        assert_lt!(
            time_base.duration_since(now()).unwrap(),
            Duration::from_millis(1)
        );

        c.insert_committed(1.into(), 2.into(), now());
        set_time(1);
        c.insert_committed(3.into(), 4.into(), now());
        set_time(2);
        c.insert_committed(5.into(), 6.into(), now());
        // Size should be calculated by count.
        assert_eq!(c.normal_cache[0].lock().size(), 3);

        // All the 3 entries are kept
        assert_eq!(c.get_committed_no_promote(1.into()).unwrap(), 2.into());
        assert_eq!(c.get_committed_no_promote(3.into()).unwrap(), 4.into());
        assert_eq!(c.get_committed_no_promote(5.into()).unwrap(), 6.into());

        set_time(1001);
        c.insert_committed(7.into(), 8.into(), now());
        // Entry 1 will be popped out.
        assert!(c.get_committed_no_promote(1.into()).is_none());
        assert_eq!(c.get_committed_no_promote(3.into()).unwrap(), 4.into());
        assert_eq!(c.get_committed_no_promote(5.into()).unwrap(), 6.into());
        set_time(1004);
        c.insert_committed(9.into(), 10.into(), now());
        // It pops more than 1 entry if there are many expired items at the tail.
        // Entry 3 and 5 will be popped out.
        assert!(c.get_committed_no_promote(1.into()).is_none());
        assert!(c.get_committed_no_promote(3.into()).is_none());
        assert!(c.get_committed_no_promote(5.into()).is_none());
        assert_eq!(c.get_committed_no_promote(7.into()).unwrap(), 8.into());
        assert_eq!(c.get_committed_no_promote(9.into()).unwrap(), 10.into());

        // Now the cache's contents are:
        // 7@1001, 9@1004
        // Test `get` promotes an entry and entries are not in order on insert time.
        assert_eq!(c.get_committed(7.into()).unwrap(), 8.into());
        set_time(2003);
        c.insert_committed(11.into(), 12.into(), now());
        assert_eq!(c.get_committed_no_promote(7.into()).unwrap(), 8.into());
        assert_eq!(c.get_committed_no_promote(9.into()).unwrap(), 10.into());
        assert_eq!(c.get_committed_no_promote(11.into()).unwrap(), 12.into());

        set_time(2005);
        c.insert_committed(13.into(), 14.into(), now());
        assert!(c.get_committed_no_promote(7.into()).is_none());
        assert!(c.get_committed_no_promote(9.into()).is_none());
        assert_eq!(c.get_committed_no_promote(11.into()).unwrap(), 12.into());

        // Now the cache's contents are:
        // 11@2003, 13@2005
        set_time(3004);
        c.insert_committed(14.into(), 14.into(), now());
        assert!(c.get_committed_no_promote(11.into()).is_none());
        assert_eq!(c.get_committed_no_promote(13.into()).unwrap(), 14.into());

        set_time(3006);
        c.remove_normal(13.into());

        // Now the cache is empty.
        c.insert_committed(15.into(), 16.into(), now());
        set_time(3008);
        c.insert_committed(17.into(), 18.into(), now());
        set_time(4007);
        c.insert_committed(19.into(), 20.into(), now());
        // 15's insert time is not updated, and is at the tail of the LRU, so it should
        // be popped.
        assert!(c.get_committed_no_promote(15.into()).is_none());
        assert_eq!(c.get_committed_no_promote(17.into()).unwrap(), 18.into());

        // Now the cache's contents are:
        // 17@3008, 19@4007
        // Test system time being changed, which can lead to current time being less
        // than entries' insert time.
        set_time(2000);
        c.insert_committed(21.into(), 22.into(), now());
        assert_eq!(c.get_committed_no_promote(17.into()).unwrap(), 18.into());
        assert_eq!(c.get_committed_no_promote(19.into()).unwrap(), 20.into());
        assert_eq!(c.get_committed_no_promote(21.into()).unwrap(), 22.into());
        set_time(3500);
        c.insert_committed(23.into(), 24.into(), now());
        assert_eq!(c.get_committed_no_promote(21.into()).unwrap(), 22.into());
        assert_eq!(c.get_committed(17.into()).unwrap(), 18.into());
        assert_eq!(c.get_committed(19.into()).unwrap(), 20.into());
        assert_eq!(c.get_committed(23.into()).unwrap(), 24.into());
        // `get` promotes the entries, and entry 21 is put to the tail.
        c.insert_committed(24.into(), 24.into(), now());
        assert_eq!(c.get_committed_no_promote(17.into()).unwrap(), 18.into());
        assert_eq!(c.get_committed_no_promote(19.into()).unwrap(), 20.into());
        assert!(c.get_committed_no_promote(21.into()).is_none());
        assert_eq!(c.get_committed_no_promote(23.into()).unwrap(), 24.into());

        // Now the cache's contents are:
        // 17@3008, 19@4007, 23@3500
        // The time passed to `insert` may differ from the time fetched in
        // the `TxnStatusCacheEvictPolicy` as they are fetched at different time.
        set_time(4009);
        // Insert with time 4007, but check with time 4009
        c.insert_committed(25.into(), 26.into(), now() - Duration::from_millis(2));
        assert!(c.get_committed_no_promote(17.into()).is_none());
        assert_eq!(c.get_committed_no_promote(19.into()).unwrap(), 20.into());

        // The cache's contents:
        // 19@4007, 23@3500, 25@4007
        set_time(4010);
        c.insert_committed(27.into(), 28.into(), now());
        // The cache's contents:
        // 19@4007, 23@3500, 25@4007, 27@4010

        // It's also possible to check with a lower time considering that system time
        // may be changed. Insert with time 5018, but check with time 5008
        set_time(5008);
        c.insert_committed(29.into(), 30.into(), now() + Duration::from_millis(10));
        assert!(c.get_committed_no_promote(19.into()).is_none());
        assert!(c.get_committed_no_promote(23.into()).is_none());
        assert!(c.get_committed_no_promote(25.into()).is_none());
        assert_eq!(c.get_committed_no_promote(27.into()).unwrap(), 28.into());
        assert_eq!(c.get_committed_no_promote(29.into()).unwrap(), 30.into());

        // Now the cache's contents are:
        // 27@4010, 29@5018
        // Considering the case that system time is being changed, it's even
        // possible that the entry being inserted is already expired
        // comparing to the current time. It doesn't matter whether the
        // entry will be dropped immediately or not. We just ensure it won't
        // trigger more troubles.
        set_time(7000);
        c.insert_committed(31.into(), 32.into(), now() - Duration::from_millis(1001));
        assert!(c.get_committed_no_promote(27.into()).is_none());
        assert!(c.get_committed_no_promote(29.into()).is_none());
        assert!(c.get_committed_no_promote(31.into()).is_none());
        assert_eq!(c.normal_cache[0].lock().size(), 0);
    }

    #[test]
    fn test_setting_capacity() {
        let c = TxnStatusCache::new_impl(
            2,
            Duration::from_millis(1000),
            Duration::from_millis(1000),
            10,
            10,
            None,
        );
        assert!(c.is_enabled);
        assert_eq!(c.normal_cache.len(), 2);
        assert_eq!(c.normal_cache[0].lock().capacity(), 5);
        assert_eq!(c.normal_cache[1].lock().capacity(), 5);

        let c = TxnStatusCache::new_impl(
            2,
            Duration::from_millis(1000),
            Duration::from_millis(1000),
            0,
            0,
            None,
        );
        assert!(!c.is_enabled);
        assert_eq!(c.normal_cache.len(), 0);
        // All operations are noops and won't cause panic or return any incorrect
        // result.
        c.insert_committed(1.into(), 2.into(), SystemTime::now());
        assert!(c.get_committed_no_promote(1.into()).is_none());
        assert!(c.get_committed(1.into()).is_none());
    }

    #[test]
    fn test_evicting_by_capacity() {
        let (c, time) =
            TxnStatusCache::with_simulated_system_time(1, Duration::from_millis(1000), 5);
        let time_base = SystemTime::now();
        let set_time = |offset_millis: u64| {
            time.store(
                time_base.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 + offset_millis,
                Ordering::Release,
            )
        };
        let now = || UNIX_EPOCH + Duration::from_millis(time.load(Ordering::Acquire));

        set_time(0);
        c.insert_committed(1.into(), 2.into(), now());
        set_time(2);
        c.insert_committed(3.into(), 4.into(), now());
        set_time(4);
        c.insert_committed(5.into(), 6.into(), now());
        set_time(6);
        c.insert_committed(7.into(), 8.into(), now());

        // The cache can keep at most 5 entries.
        set_time(8);
        c.insert_committed(9.into(), 10.into(), now());
        // Entry 1 not evicted. 5 entries in the cache currently
        assert_eq!(c.normal_cache[0].lock().len(), 5);
        assert_eq!(c.get_committed_no_promote(1.into()).unwrap(), 2.into());
        set_time(10);
        c.insert_committed(11.into(), 12.into(), now());
        // Entry 1 evicted. Still 5 entries in the cache.
        assert_eq!(c.normal_cache[0].lock().len(), 5);
        assert!(c.get_committed_no_promote(1.into()).is_none());
        assert_eq!(c.get_committed_no_promote(3.into()).unwrap(), 4.into());

        // Nothing will be evicted after trying to insert an existing key.
        c.insert_committed(11.into(), 12.into(), now());
        assert_eq!(c.normal_cache[0].lock().len(), 5);
        assert_eq!(c.get_committed_no_promote(3.into()).unwrap(), 4.into());

        // Current contents (key@time):
        // 3@2, 5@4, 7@6. 9@8, 11@10
        // Evicting by time works as well.
        set_time(1005);
        c.insert_committed(13.into(), 14.into(), now());
        assert_eq!(c.normal_cache[0].lock().len(), 4);
        assert!(c.get_committed_no_promote(3.into()).is_none());
        assert!(c.get_committed_no_promote(5.into()).is_none());
        assert_eq!(c.get_committed_no_promote(7.into()).unwrap(), 8.into());

        // Reorder the entries by `get` to prepare for testing the next case.
        assert_eq!(c.get_committed(7.into()).unwrap(), 8.into());
        assert_eq!(c.get_committed(9.into()).unwrap(), 10.into());
        assert_eq!(c.get_committed(11.into()).unwrap(), 12.into());

        c.insert_committed(15.into(), 16.into(), now());
        // Current contents:
        // 13@1005, 7@6. 9@8, 11@10, 15@1005
        assert_eq!(c.normal_cache[0].lock().len(), 5);
        // Expired entries that are not the tail can be evicted after the tail
        // is evicted due to capacity exceeded.
        set_time(1011);
        c.insert_committed(17.into(), 18.into(), now());
        assert_eq!(c.normal_cache[0].lock().len(), 2);
        assert!(c.get_committed_no_promote(13.into()).is_none());
        assert!(c.get_committed_no_promote(7.into()).is_none());
        assert!(c.get_committed_no_promote(9.into()).is_none());
        assert!(c.get_committed_no_promote(11.into()).is_none());
        assert_eq!(c.get_committed(15.into()).unwrap(), 16.into());
        assert_eq!(c.get_committed(17.into()).unwrap(), 18.into());
    }

    fn setup_cache() -> TxnStatusCache {
        TxnStatusCache::with_slots_and_time_limit(
            16,
            Duration::from_secs(30),
            Duration::from_secs(60),
            1000,
        )
    }

    #[test]
    fn test_upsert_and_get_normal_txn() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 2.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 2.into()
            })
        );

        cache.upsert(
            1.into(),
            TxnState::Committed {
                commit_ts: 3.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Committed {
                commit_ts: 3.into()
            })
        );
    }

    #[test]
    fn test_upsert_and_get_large_txn() {
        let cache = setup_cache();
        let now = SystemTime::now();
        let large_ts = 10000.into();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: large_ts,
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: large_ts
            })
        );

        cache.upsert(
            1.into(),
            TxnState::Committed {
                commit_ts: large_ts,
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Committed {
                commit_ts: large_ts
            })
        );
    }

    #[test]
    fn test_update_ongoing_txn() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 2.into(),
            },
            now,
        );
        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 3.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 3.into()
            })
        );
    }

    #[test]
    fn test_update_committed_txn() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Committed {
                commit_ts: 2.into(),
            },
            now,
        );
        cache.upsert(1.into(), TxnState::RolledBack, now);
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Committed {
                commit_ts: 2.into()
            })
        );
    }

    #[test]
    fn test_normal_to_large_txn_transition() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 1.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 1.into()
            })
        );

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 2.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 2.into()
            })
        );
    }

    #[test]
    fn test_large_to_normal_txn_transition() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 10000.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 10000.into()
            })
        );

        cache.upsert(
            1.into(),
            TxnState::Committed {
                commit_ts: 10001.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Committed {
                commit_ts: 10001.into()
            })
        );
    }

    #[test]
    fn test_eviction_normal_txn() {
        let (cache, time) =
            TxnStatusCache::with_simulated_system_time(1, Duration::from_secs(30), 1);

        time.store(0, Ordering::SeqCst);
        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 2.into(),
            },
            SystemTime::now(),
        );

        time.store(31 * 1000, Ordering::SeqCst); // 31 seconds later
        cache.upsert(
            3.into(),
            TxnState::Ongoing {
                min_commit_ts: 4.into(),
            },
            SystemTime::now(),
        );
        assert_eq!(cache.get(1.into()), None);
        assert_eq!(
            cache.get(3.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 4.into()
            })
        );
    }

    #[test]
    fn test_eviction_large_txn() {
        let (cache, time) =
            TxnStatusCache::with_simulated_system_time(1, Duration::from_secs(60), 1);

        time.store(0, Ordering::SeqCst);
        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 10000.into(),
            },
            SystemTime::now(),
        );

        time.store(61 * 1000, Ordering::SeqCst); // 61 seconds later
        cache.upsert(
            2.into(),
            TxnState::Ongoing {
                min_commit_ts: 20000.into(),
            },
            SystemTime::now(),
        );
        assert_eq!(cache.get(1.into()), None);
        assert_eq!(
            cache.get(2.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 20000.into()
            })
        );
    }

    #[test]
    fn test_capacity_limit() {
        let cache = TxnStatusCache::with_slots_and_time_limit(
            1,
            Duration::from_secs(30),
            Duration::from_secs(60),
            5,
        );
        let now = SystemTime::now();

        for i in 1..=5 {
            cache.upsert(
                i.into(),
                TxnState::Ongoing {
                    min_commit_ts: (i + 1).into(),
                },
                now,
            );
        }

        cache.upsert(
            6.into(),
            TxnState::Ongoing {
                min_commit_ts: 7.into(),
            },
            now,
        );
        assert_eq!(cache.get(1.into()), None);
        assert_eq!(
            cache.get(6.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 7.into()
            })
        );
    }

    #[test]
    fn test_get_committed() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Committed {
                commit_ts: 2.into(),
            },
            now,
        );
        cache.upsert(
            3.into(),
            TxnState::Ongoing {
                min_commit_ts: 4.into(),
            },
            now,
        );

        assert_eq!(cache.get_committed(1.into()), Some(2.into()));
        assert_eq!(cache.get_committed(3.into()), None);
    }

    #[test]
    fn test_remove_large_txn() {
        let cache = setup_cache();
        let now = SystemTime::now();

        cache.upsert(
            1.into(),
            TxnState::Ongoing {
                min_commit_ts: 10000.into(),
            },
            now,
        );
        assert_eq!(
            cache.get(1.into()),
            Some(TxnState::Ongoing {
                min_commit_ts: 10000.into()
            })
        );

        let removed = cache.remove_large_txn(1.into());
        assert!(removed.is_some());
        assert_eq!(cache.get(1.into()), None);
    }
}
