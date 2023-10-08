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
//! [a long-enough time](CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS). When a prewrite
//! request is received, it should check the cache before executing. If it finds
//! its belonging transaction is already committed, it won't skip constraint
//! check in WRITE CF. Note that if the index key is already committed but the
//! transaction info is not cached, then a late-arrived prewrite request cannot
//! be protected by this mechanism. This means we shouldn't miss any cacheable
//! transactions, and it is the reason why committed transactions should be
//! cached for *a long-enough time*.
//!
//! Unfortunately, the solution is still imperfect. As it's already known, one
//! of the reasons is that we don't have mechanism to refuse requests that have
//! past more than [CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS] since they were sent.
//! The other reason is that the cache can't be synced across different TiKV
//! instances. Consider this case:
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
//! Consider the case that a cluster a very-large transaction locked a lot of
//! keys after prewritting, while many simple reads and writes executes
//! frequently, thus these simple transactions frequently meets the lock left
//! by the large transaction. It will be very inefficient for these small
//! transactions to come back to the client and start resolve lock procedure.
//! Even if the client side has the cache of that transaction, it still wastes
//! an RTT.
//!
//! There would be more possibilities if we have such a cache in TiKV side: for
//! read requests, it can check the cache to know whether it can read from the
//! lock; and for write requests, if it finds the transaction of that lock is
//! already committed, it can merge together the resolve-lock-committing and the
//! write operation that the request need to perform.

use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

use crossbeam::utils::CachePadded;
use parking_lot::Mutex;
use tikv_util::{
    lru,
    lru::{GetTailKv, LruCache},
};
use txn_types::TimeStamp;

use crate::storage::metrics::*;

const TXN_STATUS_CACHE_SLOTS: usize = 128;

/// An cache item should be kept for at least this time.
/// Actually this should be guaranteed only for committed transactions. See
/// [this section](#
/// for-filtering-out-unwanted-late-arrived-stale-prewrite-requests) for details
/// about why this is needed.
const CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS: u64 = 30000;

struct CacheEntry {
    commit_ts: TimeStamp,
    /// The system timestamp in milliseconds when the entry is inserted to the
    /// cache.
    insert_time: u64,
}

struct TxnStatusCacheEvictPolicy {
    limit_millis: u64,
    #[allow(dead_code)]
    simulated_system_time: Option<Arc<AtomicU64>>,
}

impl TxnStatusCacheEvictPolicy {
    #[inline]
    #[cfg(not(test))]
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    /// The
    #[inline]
    #[cfg(test)]
    fn now(&self) -> SystemTime {
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
        get_tail_kv: &impl GetTailKv<TimeStamp, CacheEntry>,
    ) -> bool {
        if current_size <= capacity {
            return false;
        }

        if let Some((_, v)) = get_tail_kv.get_tail_kv() {
            self.now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                > self.limit_millis + v.insert_time
        } else {
            true
        }
    }
}

struct TxnStatusCacheSlot {
    cache: LruCache<TimeStamp, CacheEntry, lru::CountTracker, TxnStatusCacheEvictPolicy>,
    last_check_size: usize,
    last_check_capacity: usize,
}

pub struct TxnStatusCache {
    slots: Vec<CachePadded<Mutex<TxnStatusCacheSlot>>>,
}

unsafe impl Sync for TxnStatusCache {}

impl TxnStatusCache {
    fn new_impl(
        slots: usize,
        limit_millis: u64,
        simulated_system_time: Option<Arc<AtomicU64>>,
    ) -> Self {
        let mut initial_capacity = 0;
        let res = Self {
            slots: (0..slots)
                .map(|_| {
                    let cache = LruCache::new(
                        16,
                        0,
                        lru::CountTracker::default(),
                        TxnStatusCacheEvictPolicy {
                            limit_millis,
                            simulated_system_time: simulated_system_time.clone(),
                        },
                    );
                    let capacity = cache.internal_mem_capacity();
                    initial_capacity += capacity;
                    Mutex::new(TxnStatusCacheSlot {
                        cache,
                        last_check_size: 0,
                        last_check_capacity: capacity,
                    })
                    .into()
                })
                .collect(),
        };
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .set(initial_capacity as i64);
        res
    }

    pub fn new() -> Self {
        Self::with_slots_and_time_limit(
            TXN_STATUS_CACHE_SLOTS,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS,
        )
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self::with_slots_and_time_limit(16, CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS)
    }

    pub fn with_slots_and_time_limit(slots: usize, limit_millis: u64) -> Self {
        Self::new_impl(slots, limit_millis, None)
    }

    #[cfg(test)]
    fn with_simulated_system_time(slots: usize, limit_millis: u64) -> (Self, Arc<AtomicU64>) {
        let system_time = Arc::new(AtomicU64::new(0));
        let res = Self::new_impl(slots, limit_millis, Some(system_time.clone()));
        (res, system_time)
    }

    fn slot_index(&self, start_ts: TimeStamp) -> usize {
        fxhash::hash(&start_ts) % self.slots.len()
    }

    pub fn insert(&self, start_ts: TimeStamp, commit_ts: TimeStamp, now: SystemTime) {
        let mut slot = self.slots[self.slot_index(start_ts)].lock();
        let insert_time = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        slot.cache.insert_if_not_exist(
            start_ts,
            CacheEntry {
                commit_ts,
                insert_time,
            },
        );
        let size = slot.cache.size();
        let capacity = slot.cache.internal_mem_capacity();
        // Update statistics.
        // CAUTION: Assuming that only one TxnStatusCache instance is in a TiKV process.
        SCHED_TXN_STATUS_CACHE_SIZE
            .used
            .add(size as i64 - slot.last_check_size as i64);
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .add(capacity as i64 - slot.last_check_capacity as i64);
        slot.last_check_size = size;
        slot.last_check_capacity = capacity;
    }

    pub fn get_no_promote(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        let slot = self.slots[self.slot_index(start_ts)].lock();
        slot.cache
            .get_no_promote(&start_ts)
            .map(|entry| entry.commit_ts)
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

    use rand::Rng;
    use test::Bencher;

    use super::*;

    fn bench_insert_impl(b: &mut test::Bencher, init_size: usize) {
        let (c, time) =
            TxnStatusCache::with_simulated_system_time(TXN_STATUS_CACHE_SLOTS, init_size as u64);
        let start_time = SystemTime::now();
        for i in 1..=init_size {
            c.insert(
                (i as u64).into(),
                (i as u64 + 1).into(),
                start_time + Duration::from_millis(i as u64),
            );
        }
        let mut current_time_shift = (init_size + 1) as u64;
        b.iter(|| {
            let now = start_time + Duration::from_millis(current_time_shift);
            time.store(
                now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                Ordering::Release,
            );
            c.insert(
                current_time_shift.into(),
                (current_time_shift + 1).into(),
                now,
            );
            current_time_shift += 1;
        });
        test::black_box(&c);
    }

    fn bench_get_impl(b: &mut test::Bencher, init_size: usize) {
        let c = TxnStatusCache::with_slots_and_time_limit(
            TXN_STATUS_CACHE_SLOTS,
            CACHE_ITEMS_REQUIRED_KEEP_TIME_MILLIS,
        );
        let now = SystemTime::now();
        for i in 1..=init_size {
            c.insert(
                (i as u64).into(),
                (i as u64 + 1).into(),
                now + Duration::from_millis(i as u64),
            );
        }
        let rand_range = if init_size == 0 { 10000 } else { init_size } as u64;
        b.iter(|| {
            let ts = rand::thread_rng().gen_range(0u64, rand_range);
            let res = c.get_no_promote(ts.into());
            test::black_box(&res);
        })
    }

    #[bench]
    fn bench_insert_empty(b: &mut test::Bencher) {
        // 120ns/iter
        bench_insert_impl(b, 0);
    }

    #[bench]
    fn bench_insert_100000(b: &mut test::Bencher) {
        // 132ns/iter
        bench_insert_impl(b, 100000);
    }

    #[bench]
    fn bench_get_empty(b: &mut test::Bencher) {
        // 25ns/iter
        bench_get_impl(b, 0);
    }

    #[bench]
    fn bench_get_100000(b: &mut test::Bencher) {
        // 35ns/iter
        bench_get_impl(b, 100000);
    }

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

        fn add(&mut self, other: &Self) {
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

        fn percentile(&self, percentile: f64) -> f64 {
            let mut bucket = self.buckets.len();
            let mut prefix_sum = self.count;
            while bucket > 0 {
                bucket -= 1;
                prefix_sum -= self.buckets[bucket];
                let prefix_percentile = prefix_sum as f64 / self.count as f64;
                if prefix_percentile <= percentile {
                    assert_le!(prefix_sum as f64, percentile * self.count as f64);
                    assert_lt!(
                        percentile * self.count as f64,
                        (prefix_sum + self.buckets[bucket]) as f64
                    );
                    break;
                }
            }

            bucket as f64 * self.bucket_width as f64
                + (percentile * self.count as f64 - prefix_sum as f64) * self.bucket_width as f64
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

    // As it seems neither rust's builtin test framework nor criterion supports
    // benchmarking concurrent operations, we use a test to simulate it, and
    // ignore it by default. Remember to pass --nocapture to get the output.
    fn bench_concurrent_impl<T>(
        name: &str,
        threads: usize,
        function: impl Fn(u64) -> T + Send + Sync + 'static,
    ) {
        let start_time = Instant::now();
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
            total_stats.add(&h.join().unwrap());
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
        let (c, time) = TxnStatusCache::with_simulated_system_time(slots, init_size as u64);
        let start_time = SystemTime::now();
        for i in 1..=init_size {
            c.insert(
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
                test::black_box(c.get_no_promote(time_shift.into()));
            }
            c.insert(time_shift.into(), (time_shift + 1).into(), now);
            test::black_box(&c);
        });
    }

    #[bench]
    // #[ignore]
    fn test_bench_txn_status_cache_concurrent(_b: &mut Bencher) {
        bench_txn_status_cache_concurrent_impl(16, 10000, false, false);
        bench_txn_status_cache_concurrent_impl(16, 10000, true, false);
        bench_txn_status_cache_concurrent_impl(16, 10000, false, true);
        bench_txn_status_cache_concurrent_impl(16, 10000, true, true);
        bench_txn_status_cache_concurrent_impl(64, 10000, false, false);
        bench_txn_status_cache_concurrent_impl(64, 10000, true, false);
        bench_txn_status_cache_concurrent_impl(64, 10000, false, true);
        bench_txn_status_cache_concurrent_impl(64, 10000, true, true);
    }
}
