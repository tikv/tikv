// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use crossbeam::utils::CachePadded;
use rand::Rng;

// An array of core-local values from RocksDB. Ideally the generic type, T, is
// cache aligned to prevent false sharing.
pub struct CoreLocalArray<T: Default> {
    data: Vec<T>,
    size_shift: usize,
}

impl<T: Default> Default for CoreLocalArray<T> {
    fn default() -> Self {
        let num_cores = std::thread::available_parallelism().unwrap().get();
        // find a power of two >= num_cpus and >= 8
        let mut size_shift = 3;
        while (1 << size_shift) < num_cores {
            size_shift += 1;
        }
        let mut data = Vec::with_capacity(1 << size_shift);
        for _ in 0..data.capacity() {
            data.push(T::default());
        }
        CoreLocalArray { data, size_shift }
    }
}

impl<T: Default> CoreLocalArray<T> {
    // returns pointer to the element corresponding to the core that the thread
    // currently runs on.
    pub fn access(&self) -> &T {
        self.access_element_and_index().0
    }

    // same as above, but also returns the core index, which the client can cache
    // to reduce how often core ID needs to be retrieved. Only do this if some
    // inaccuracy is tolerable, as the thread may migrate to a different core.
    fn access_element_and_index(&self) -> (&T, usize) {
        let core_id = physical_core_id();
        let idx = if core::intrinsics::unlikely(core_id < 0) {
            rand::thread_rng().gen_range(0..(1 << self.size_shift))
        } else {
            core_id as usize & ((1 << self.size_shift) - 1)
        };
        (self.access_at_core(idx), idx)
    }

    // returns pointer to element for the specified core index. This can be used,
    // e.g., for aggregation, or if the client caches core index.
    pub fn access_at_core(&self, idx: usize) -> &T {
        &self.data[idx]
    }
}

fn physical_core_id() -> i32 {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        unsafe {
            let cpu_id = libc::sched_getcpu();
            if cpu_id < 0 { -1 } else { cpu_id as i32 }
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
    {
        -1
    }
}

pub const ENGINE_TICKER_TYPES: &[Tickers] = &[Tickers::BytesRead, Tickers::IterBytesRead];

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum Tickers {
    BytesRead = 0,
    IterBytesRead,
    TickerEnumMax,
}

#[derive(Default)]
struct StatisticsData {
    tickers: [AtomicU64; Tickers::TickerEnumMax as usize],
}

impl StatisticsData {
    #[inline]
    fn record_ticker(&self, ticker_type: Tickers, count: u64) {
        self.tickers[ticker_type as usize].fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    fn get_ticker_count(&self, ticker_type: Tickers) -> u64 {
        self.tickers[ticker_type as usize].load(Ordering::Relaxed)
    }

    #[inline]
    fn exchange(&self, ticker_type: Tickers, count: u64) -> u64 {
        self.tickers[ticker_type as usize].swap(count, Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct Statistics {
    // Synchronizes anything that operates across other cores' local data,
    // such that operations like `get_and_reset_ticker_count` can be performed atomically.
    _aggregate_lock: Mutex<()>,
    per_core_stats: CoreLocalArray<CachePadded<StatisticsData>>,
}

impl Statistics {
    pub fn get_ticker_count(&self, ticker_type: Tickers) -> u64 {
        let _guard = self._aggregate_lock.lock().unwrap();
        self.get_ticker_count_locked(ticker_type)
    }

    fn get_ticker_count_locked(&self, ticker_type: Tickers) -> u64 {
        self.per_core_stats
            .data
            .iter()
            .fold(0, |acc, stats| acc + stats.get_ticker_count(ticker_type))
    }

    pub fn record_ticker(&self, ticker_type: Tickers, count: u64) {
        // todo(SpadeA): whether we need to have a switcher for disabling collections?
        self.per_core_stats
            .access()
            .record_ticker(ticker_type, count)
    }

    pub fn get_and_reset_ticker_count(&self, ticker_type: Tickers) -> u64 {
        let _guard = self._aggregate_lock.lock().unwrap();
        self.per_core_stats
            .data
            .iter()
            .fold(0, |acc, stats| acc + stats.exchange(ticker_type, 0))
    }
}

// LocalStatistics contain Statistics counters that will be aggregated per
// each iterator instance and then will be sent to the global statistics when
// the iterator is destroyed.
//
// The purpose of this approach is to avoid perf regression happening
// when multiple threads bump the atomic counters from a DBIter::Next().
#[derive(Default)]
pub(crate) struct LocalStatistics {
    // Map to Tickers::IterBytesRead
    pub(crate) bytes_read: u64,
}

#[cfg(test)]
pub mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{Statistics, Tickers};

    #[test]
    fn test_core_local() {
        let statistics = Arc::new(Statistics::default());
        std::thread::scope(|s| {
            for _ in 0..4 {
                let statistics_clone = statistics.clone();
                s.spawn(move || {
                    statistics_clone.record_ticker(Tickers::BytesRead, 100);
                    statistics_clone.record_ticker(Tickers::IterBytesRead, 200);
                    // sleep a while to make the next spwan use another core as much as possible
                    std::thread::sleep(Duration::from_millis(100));
                });
            }
        });
        let read_bytes = statistics.get_ticker_count(Tickers::BytesRead);
        assert_eq!(read_bytes, 400);
        assert_eq!(
            statistics.get_and_reset_ticker_count(Tickers::BytesRead),
            400
        );
        assert_eq!(statistics.get_ticker_count(Tickers::BytesRead), 0);

        let iter_bytes = statistics.get_ticker_count(Tickers::IterBytesRead);
        assert_eq!(iter_bytes, 800);
        assert_eq!(
            statistics.get_and_reset_ticker_count(Tickers::IterBytesRead),
            800
        );
        assert_eq!(statistics.get_ticker_count(Tickers::IterBytesRead), 0);
    }
}
