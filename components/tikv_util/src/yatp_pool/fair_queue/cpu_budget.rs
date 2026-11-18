// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

/// Default minimum wall time between CPU budget refills.
pub const DEFAULT_CPU_REFILL_INTERVAL: Duration = Duration::from_millis(10);
/// Caps saved credit at rate × CPU_BURST_WINDOW and bounds the refill interval.
pub const CPU_BURST_WINDOW: Duration = Duration::from_millis(100);

/// Shared CPU accounting, independent of the scheduler's state lock.
/// Refill, capping and consumption form one transaction on the shared balance.
pub(super) struct CpuBudget {
    bucket: Mutex<CpuTokenBucket>,
}

impl CpuBudget {
    pub(super) fn new(rate: f64, refill_interval: Duration) -> Self {
        Self {
            bucket: Mutex::new(CpuTokenBucket::new(rate, refill_interval)),
        }
    }

    pub(super) fn retry_at(&self) -> Option<Instant> {
        let mut bucket = self.bucket.lock().unwrap();
        bucket.retry_at(Instant::now())
    }

    pub(super) fn consume(&self, elapsed: Duration) {
        self.consume_with_clock(elapsed, Instant::now);
    }

    pub(super) fn consume_and_retry_at(&self, elapsed: Duration) -> Option<Instant> {
        self.consume_and_retry_at_with_clock(elapsed, Instant::now)
    }

    // Sample refill time only after acquiring the lock. Injectable clocks let
    // tests cover refill boundaries without depending on thread scheduling.
    fn consume_with_clock(&self, elapsed: Duration, now: impl FnOnce() -> Instant) {
        let mut bucket = self.bucket.lock().unwrap();
        bucket.consume(elapsed, now());
    }

    fn consume_and_retry_at_with_clock(
        &self,
        elapsed: Duration,
        mut now: impl FnMut() -> Instant,
    ) -> Option<Instant> {
        let mut bucket = self.bucket.lock().unwrap();
        bucket.consume(elapsed, now());
        // Keep the second refill and its fresh timestamp: crossing a refill
        // boundary after consuming can cap the balance differently from
        // refilling only once before consuming.
        bucket.retry_at(now())
    }

    #[cfg(test)]
    pub(super) fn balance_ns(&self) -> f64 {
        self.bucket.lock().unwrap().balance_ns
    }

    #[cfg(test)]
    pub(super) fn set_balance_ns(&self, balance_ns: f64) {
        self.bucket.lock().unwrap().balance_ns = balance_ns;
    }
}

struct CpuTokenBucket {
    // CPU nanoseconds available across all workers; negative values are debt
    // from work that has already executed.
    balance_ns: f64,
    // CPU-ns earned per wall-ns: the budgeted number of CPU cores, possibly
    // fractional. Each worker charges its actual thread CPU time.
    rate: f64,
    refill_interval: Duration,
    last_refill: Instant,
}

impl CpuTokenBucket {
    fn new(rate: f64, refill_interval: Duration) -> Self {
        assert!(!refill_interval.is_zero() && refill_interval <= CPU_BURST_WINDOW);
        Self {
            balance_ns: rate * CPU_BURST_WINDOW.as_nanos() as f64,
            rate,
            refill_interval,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        if elapsed >= self.refill_interval {
            self.balance_ns = (self.balance_ns + elapsed.as_nanos() as f64 * self.rate)
                .min(self.rate * CPU_BURST_WINDOW.as_nanos() as f64);
            self.last_refill = now;
        }
    }

    fn retry_at(&mut self, now: Instant) -> Option<Instant> {
        self.refill(now);
        // Retry at the next refill opportunity. A large debt may need several
        // retries, and a zero balance still cannot admit another poll.
        (self.balance_ns <= 0.0).then_some(self.last_refill + self.refill_interval)
    }

    fn consume(&mut self, elapsed: Duration, now: Instant) {
        self.refill(now);
        self.balance_ns -= elapsed.as_nanos() as f64;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier, TryLockError},
        thread,
    };

    use super::*;

    #[test]
    fn refill_debt_and_burst_bound() {
        let mut bucket = CpuTokenBucket::new(2.0, DEFAULT_CPU_REFILL_INTERVAL);
        let start = bucket.last_refill;
        assert_eq!(bucket.balance_ns, 200_000_000.0);
        assert_eq!(bucket.retry_at(start), None);

        bucket.balance_ns = -30_000_000.0;
        let first_refill = start + DEFAULT_CPU_REFILL_INTERVAL;
        assert_eq!(
            bucket.retry_at(first_refill - Duration::from_nanos(1)),
            Some(first_refill)
        );
        assert_eq!(bucket.balance_ns, -30_000_000.0);
        assert_eq!(bucket.last_refill, start);
        assert_eq!(
            bucket.retry_at(first_refill),
            Some(first_refill + DEFAULT_CPU_REFILL_INTERVAL)
        );
        assert_eq!(bucket.balance_ns, -10_000_000.0);
        assert_eq!(
            bucket.retry_at(first_refill + DEFAULT_CPU_REFILL_INTERVAL),
            None
        );
        assert_eq!(bucket.balance_ns, 10_000_000.0);

        bucket.refill(start + Duration::from_secs(100));
        assert_eq!(bucket.balance_ns, 200_000_000.0);
    }

    #[test]
    fn zero_balance_waits_and_same_timestamp_does_not_refill_twice() {
        for interval in [Duration::from_millis(1), DEFAULT_CPU_REFILL_INTERVAL] {
            let mut bucket = CpuTokenBucket::new(0.5, interval);
            let start = bucket.last_refill;
            bucket.consume(Duration::from_millis(50), start);
            let next = start + interval;
            assert_eq!(bucket.retry_at(next - Duration::from_nanos(1)), Some(next));
            assert_eq!(bucket.balance_ns, 0.0);

            assert_eq!(bucket.retry_at(next), None);
            let credit = interval / 2;
            assert_eq!(bucket.balance_ns, credit.as_nanos() as f64);
            bucket.consume(credit, next);
            assert_eq!(bucket.retry_at(next), Some(next + interval));
            assert_eq!(bucket.balance_ns, 0.0);
        }
    }

    #[test]
    fn consume_refills_before_charging_after_idle() {
        let budget = CpuBudget::new(2.0, DEFAULT_CPU_REFILL_INTERVAL);
        let start = budget.bucket.lock().unwrap().last_refill;
        budget.consume_with_clock(Duration::from_millis(50), || {
            assert!(matches!(
                budget.bucket.try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            start + Duration::from_secs(1)
        });
        // Idle refill caps at 200ms first; the charge must not be discarded by
        // capping an already-debited balance after the charge.
        assert_eq!(budget.bucket.lock().unwrap().balance_ns, 150_000_000.0);
    }

    #[test]
    fn consume_then_retry_keeps_both_refill_observations() {
        let budget = CpuBudget::new(1.0, DEFAULT_CPU_REFILL_INTERVAL);
        let start = budget.bucket.lock().unwrap().last_refill;
        let next = start + DEFAULT_CPU_REFILL_INTERVAL;
        let mut clock_calls = 0;
        let retry_at = budget.consume_and_retry_at_with_clock(Duration::from_millis(5), || {
            assert!(matches!(
                budget.bucket.try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            clock_calls += 1;
            match clock_calls {
                1 => next - Duration::from_nanos(1),
                2 => next,
                _ => panic!("unexpected refill clock sample"),
            }
        });
        assert_eq!(clock_calls, 2);
        assert_eq!(retry_at, None);
        let bucket = budget.bucket.lock().unwrap();
        // Consume sees no refill and leaves 95ms; retry crosses the boundary,
        // adds 10ms and caps at 100ms. A single refill would leave 95ms.
        assert_eq!(bucket.balance_ns, 100_000_000.0);
        assert_eq!(bucket.last_refill, next);
    }

    #[test]
    fn concurrent_charges_share_one_balance() {
        const WORKERS: usize = 8;
        const CHARGES: usize = 1_000;
        let elapsed = Duration::from_micros(50);
        let budget = Arc::new(CpuBudget::new(2.0, DEFAULT_CPU_REFILL_INTERVAL));
        let start = budget.bucket.lock().unwrap().last_refill;
        let barrier = Arc::new(Barrier::new(WORKERS));
        let handles: Vec<_> = (0..WORKERS)
            .map(|worker| {
                let budget = budget.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..CHARGES {
                        if worker % 2 == 0 {
                            budget.consume_with_clock(elapsed, || start);
                        } else {
                            let _ = budget.consume_and_retry_at_with_clock(elapsed, || start);
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let mut bucket = budget.bucket.lock().unwrap();
        let expected = 200_000_000.0 - (WORKERS * CHARGES) as f64 * elapsed.as_nanos() as f64;
        assert_eq!(bucket.balance_ns, expected);
        assert_eq!(
            bucket.retry_at(start),
            Some(start + DEFAULT_CPU_REFILL_INTERVAL)
        );
        assert_eq!(bucket.last_refill, start);
    }
}
