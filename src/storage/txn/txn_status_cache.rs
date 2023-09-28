// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

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

use crate::storage::metrics::SCHED_TXN_STATUS_CACHE_SIZE;

const TXN_STATUS_CACHE_SLOTS: usize = 128;

pub const REQ_MAX_FLYING_TIME_MILLIS: u64 = 30000;

struct CacheEntry {
    commit_ts: TimeStamp,
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
        Self::with_slots_and_time_limit(TXN_STATUS_CACHE_SLOTS, REQ_MAX_FLYING_TIME_MILLIS)
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self::with_slots_and_time_limit(16, REQ_MAX_FLYING_TIME_MILLIS)
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

    pub fn get(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
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
            REQ_MAX_FLYING_TIME_MILLIS,
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
            let res = c.get(ts.into());
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
                test::black_box(c.get(time_shift.into()));
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
