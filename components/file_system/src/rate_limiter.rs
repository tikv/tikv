// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::metrics::{tls_collect_rate_limiter_request_wait, RATE_LIMITER_MAX_BYTES_PER_SEC};
use super::{IOOp, IOPriority, IOType};

use std::str::FromStr;
use std::sync::{
    atomic::{AtomicI64, AtomicU32, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::EnumCount;
use tikv_util::time::Instant;

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(50);
const DEFAULT_REFILLS_PER_SEC: usize = (1.0 / DEFAULT_REFILL_PERIOD.as_secs_f32()) as usize;
const MAX_DURATION_PER_WAIT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum IORateLimitMode {
    WriteOnly,
    ReadOnly,
    AllIo,
}

impl IORateLimitMode {
    pub fn as_str(&self) -> &str {
        match *self {
            IORateLimitMode::WriteOnly => "write-only",
            IORateLimitMode::ReadOnly => "read-only",
            IORateLimitMode::AllIo => "all-io",
        }
    }

    #[inline]
    pub fn contains(&self, op: IOOp) -> bool {
        match *self {
            IORateLimitMode::WriteOnly => op == IOOp::Write,
            IORateLimitMode::ReadOnly => op == IOOp::Read,
            _ => true,
        }
    }
}

impl FromStr for IORateLimitMode {
    type Err = String;
    fn from_str(s: &str) -> Result<IORateLimitMode, String> {
        match s {
            "write-only" => Ok(IORateLimitMode::WriteOnly),
            "read-only" => Ok(IORateLimitMode::ReadOnly),
            "all-io" => Ok(IORateLimitMode::AllIo),
            s => Err(format!(
                "expect: write-only, read-only or all-io, got: {:?}",
                s
            )),
        }
    }
}

impl Serialize for IORateLimitMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IORateLimitMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected, Visitor};
        struct StrVistor;
        impl<'de> Visitor<'de> for StrVistor {
            type Value = IORateLimitMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a IO rate limit mode")
            }

            fn visit_str<E>(self, value: &str) -> Result<IORateLimitMode, E>
            where
                E: Error,
            {
                let p = match IORateLimitMode::from_str(&*value.trim().to_lowercase()) {
                    Ok(p) => p,
                    _ => {
                        return Err(E::invalid_value(
                            Unexpected::Other(&"invalid IO rate limit mode".to_string()),
                            &self,
                        ));
                    }
                };
                Ok(p)
            }
        }

        deserializer.deserialize_str(StrVistor)
    }
}

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct IORateLimiterStatistics {
    read_bytes: [CachePadded<AtomicUsize>; IOType::COUNT],
    write_bytes: [CachePadded<AtomicUsize>; IOType::COUNT],
}

impl IORateLimiterStatistics {
    pub fn new() -> Self {
        IORateLimiterStatistics {
            read_bytes: Default::default(),
            write_bytes: Default::default(),
        }
    }

    pub fn fetch(&self, io_type: IOType, io_op: IOOp) -> usize {
        let io_type_idx = io_type as usize;
        match io_op {
            IOOp::Read => self.read_bytes[io_type_idx].load(Ordering::Relaxed),
            IOOp::Write => self.write_bytes[io_type_idx].load(Ordering::Relaxed),
        }
    }

    pub fn record(&self, io_type: IOType, io_op: IOOp, bytes: usize) {
        let io_type_idx = io_type as usize;
        match io_op {
            IOOp::Read => {
                self.read_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
            }
            IOOp::Write => {
                self.write_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn reset(&self) {
        for i in 0..IOType::COUNT {
            self.read_bytes[i].store(0, Ordering::Relaxed);
            self.write_bytes[i].store(0, Ordering::Relaxed);
        }
    }
}

impl Default for IORateLimiterStatistics {
    fn default() -> Self {
        Self::new()
    }
}

/// Used to dynamically adjust the proportion of total budgets allocated for rate limited
/// IO. This is needed when global IOs are only partially rate limited, e.g. when mode is
/// IORateLimitMode::WriteOnly.
pub trait IOBudgetAdjustor: Send + Sync {
    fn adjust(&self, threshold: usize) -> usize;
}

/// Limit total IO flow below provided threshold by throttling lower-priority IOs.
/// Rate limit is disabled when total IO threshold is set to zero.
struct PriorityBasedIORateLimiter {
    // High-priority IOs are only limited when strict is true
    strict: bool,
    // Available bytes of current epoch. Could be negative.
    bytes_available: [CachePadded<AtomicI64>; IOPriority::COUNT],
    // Maximum bytes permitted during current epoch
    bytes_per_epoch: [CachePadded<AtomicUsize>; IOPriority::COUNT],
    protected: Mutex<PriorityBasedIORateLimiterProtected>,
}

struct PriorityBasedIORateLimiterProtected {
    next_refill_time: Instant,
    // Adjust low priority IO flow based on system backlog
    adjustor: Option<Arc<dyn IOBudgetAdjustor>>,
}

impl PriorityBasedIORateLimiterProtected {
    fn new() -> Self {
        PriorityBasedIORateLimiterProtected {
            next_refill_time: Instant::now_coarse() + DEFAULT_REFILL_PERIOD,
            adjustor: None,
        }
    }
}

impl PriorityBasedIORateLimiter {
    fn new(strict: bool) -> Self {
        PriorityBasedIORateLimiter {
            strict,
            bytes_available: Default::default(),
            bytes_per_epoch: Default::default(),
            protected: Mutex::new(PriorityBasedIORateLimiterProtected::new()),
        }
    }

    /// Dynamically changes the total IO flow threshold.
    fn set_bytes_per_sec(&self, bytes_per_sec: usize) {
        let new_rate = (bytes_per_sec as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as usize;
        self.bytes_per_epoch[IOPriority::High as usize].store(new_rate, Ordering::Relaxed);
        RATE_LIMITER_MAX_BYTES_PER_SEC
            .high
            .set(bytes_per_sec as i64);
        let now = Instant::now_coarse();
        let mut locked = self.protected.lock();
        if new_rate == 0 {
            self.bytes_per_epoch[IOPriority::Medium as usize].store(new_rate, Ordering::Relaxed);
            RATE_LIMITER_MAX_BYTES_PER_SEC
                .medium
                .set(bytes_per_sec as i64);
            self.bytes_per_epoch[IOPriority::Low as usize].store(new_rate, Ordering::Relaxed);
            RATE_LIMITER_MAX_BYTES_PER_SEC.low.set(bytes_per_sec as i64);
        } else {
            locked.next_refill_time = now;
            self.refill(&mut locked, now);
        }
    }

    fn set_low_priority_io_adjustor(&self, adjustor: Option<Arc<dyn IOBudgetAdjustor>>) {
        let mut locked = self.protected.lock();
        locked.adjustor = adjustor;
    }

    #[inline]
    fn request(&self, priority: IOPriority, amount: usize) -> usize {
        let (amount, wait) = self.request_imp(priority, amount);
        if let Some(wait) = wait {
            let mut to_wait = wait;
            loop {
                if to_wait > MAX_DURATION_PER_WAIT {
                    std::thread::sleep(MAX_DURATION_PER_WAIT);
                    if self.recheck(priority) {
                        break;
                    }
                    to_wait -= MAX_DURATION_PER_WAIT;
                } else {
                    std::thread::sleep(to_wait);
                    break;
                }
            }
            tls_collect_rate_limiter_request_wait(priority.as_str(), wait - to_wait);
        }
        amount
    }

    #[inline]
    async fn async_request(&self, priority: IOPriority, amount: usize) -> usize {
        let (amount, wait) = self.request_imp(priority, amount);
        if let Some(wait) = wait {
            let mut to_wait = wait;
            loop {
                if to_wait > MAX_DURATION_PER_WAIT {
                    tokio::time::sleep(MAX_DURATION_PER_WAIT).await;
                    if self.recheck(priority) {
                        break;
                    }
                    to_wait -= MAX_DURATION_PER_WAIT;
                } else {
                    tokio::time::sleep(wait).await;
                    break;
                }
            }
            tls_collect_rate_limiter_request_wait(priority.as_str(), wait - to_wait);
        }
        amount
    }

    #[cfg(test)]
    fn request_with_skewed_clock(&self, priority: IOPriority, amount: usize) -> usize {
        fn skewed_duration(d: Duration) -> Duration {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let subtraction: bool = rng.gen();
            let offset = std::cmp::min(Duration::from_millis(1), d / 100);
            if subtraction { d - offset } else { d + offset }
        }
        let (amount, wait) = self.request_imp(priority, amount);
        if let Some(wait) = wait {
            let mut to_wait = wait;
            loop {
                if to_wait > MAX_DURATION_PER_WAIT {
                    std::thread::sleep(skewed_duration(MAX_DURATION_PER_WAIT));
                    if self.recheck(priority) {
                        break;
                    }
                    to_wait -= MAX_DURATION_PER_WAIT;
                } else {
                    std::thread::sleep(skewed_duration(to_wait));
                    break;
                }
            }
            tls_collect_rate_limiter_request_wait(priority.as_str(), wait - to_wait);
        }
        amount
    }

    #[inline]
    fn recheck(&self, priority: IOPriority) -> bool {
        self.bytes_available[priority as usize].load(Ordering::Relaxed) >= 0
    }

    fn request_imp(&self, priority: IOPriority, amount: usize) -> (usize, Option<Duration>) {
        debug_assert!(amount > 0);
        let priority_idx = priority as usize;
        let cached_bytes_per_epoch = self.bytes_per_epoch[priority_idx].load(Ordering::Relaxed);
        // Flow control is disabled when limit is zero.
        if cached_bytes_per_epoch == 0 {
            return (amount, None);
        }
        let amount = std::cmp::min(amount, cached_bytes_per_epoch);
        let remains = self.bytes_available[priority_idx]
            .fetch_sub(amount as i64, Ordering::Relaxed)
            - amount as i64;
        // We prefer not to partially return only a portion of requested bytes.
        if remains >= 0 || !self.strict && priority == IOPriority::High {
            return (amount, None);
        }
        let remains = (-remains) as usize;
        // Wait for next refill.
        let mut wait = {
            let now = Instant::now_coarse();
            let mut locked = self.protected.lock();
            if locked.next_refill_time <= now {
                self.refill(&mut locked, now);
                Duration::default()
            } else {
                locked.next_refill_time - now
            }
        };
        // Wait for additional refills.
        // `(a-1)/b` is equivalent to `roundup(a.saturating_sub(b)/b)`.
        wait += DEFAULT_REFILL_PERIOD * ((remains - 1) / cached_bytes_per_epoch) as u32;
        (amount, Some(wait))
    }

    /// Updates and refills IO budgets for next epoch based on IO priority.
    /// This method is a no-op when high-priority IO rate limit equals zero.
    /// Here we provide best-effort priority control:
    /// 1) Limited IO budget is assigned to lower priority to ensure higher priority can at least
    ///    consume the same IO amount as the last few epochs without breaching global threshold.
    /// 2) Higher priority may temporarily use lower priority's IO budgets. When this happens,
    ///    total IO flow could exceed global threshold.
    /// 3) Highest priority IO alone must not exceed global threshold (in strict mode).
    fn refill(&self, locked: &mut PriorityBasedIORateLimiterProtected, now: Instant) {
        let mut budgets = self.bytes_per_epoch[IOPriority::High as usize].load(Ordering::Relaxed);
        if budgets == 0 {
            // It's possible that rate limit is toggled off in the meantime.
            return;
        }
        debug_assert!(now >= locked.next_refill_time);
        let overflow_epochs =
            (now - locked.next_refill_time).as_secs_f32() / DEFAULT_REFILL_PERIOD.as_secs_f32();
        locked.next_refill_time = now + DEFAULT_REFILL_PERIOD;

        debug_assert!(
            IOPriority::High as usize == IOPriority::Medium as usize + 1
                && IOPriority::Medium as usize == IOPriority::Low as usize + 1
        );
        for pri in &[IOPriority::High, IOPriority::Medium, IOPriority::Low] {
            let p = *pri as usize;

            if *pri != IOPriority::High {
                match *pri {
                    IOPriority::Medium => RATE_LIMITER_MAX_BYTES_PER_SEC
                        .medium
                        .set((budgets * DEFAULT_REFILLS_PER_SEC) as i64),
                    IOPriority::Low => {
                        // Only apply rate limit adjustments on low-priority IOs.
                        if let Some(adjustor) = &locked.adjustor {
                            budgets = adjustor.adjust(budgets);
                        }
                        RATE_LIMITER_MAX_BYTES_PER_SEC
                            .low
                            .set((budgets * DEFAULT_REFILLS_PER_SEC) as i64);
                    }
                    _ => unreachable!(),
                }
                self.bytes_per_epoch[p].store(budgets, Ordering::Relaxed);
            }
            let overflow_refill = (budgets as f32 * overflow_epochs) as i64;
            let r = self.bytes_available[p].fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |mut unused| {
                    // Compensate for missing epochs.
                    unused += overflow_refill;
                    if unused > 0 {
                        let res = budgets as i64;
                        budgets = unused as usize;
                        Some(res)
                    } else {
                        let res = unused + budgets as i64;
                        budgets = 1;
                        Some(res)
                    }
                },
            );
            debug_assert!(r.is_ok());
        }
    }

    #[cfg(test)]
    fn critical_section(&self, now: Instant) {
        let mut locked = self.protected.lock();
        locked.next_refill_time = now;
        self.refill(&mut locked, now);
    }

    #[cfg(test)]
    fn reset(&self) {
        for p in self.bytes_available.iter() {
            p.store(0, Ordering::Relaxed);
        }
    }
}

/// A high-performance IO rate limiter used for prioritized flow control.
/// An instance of `IORateLimiter` can be safely shared between threads.
pub struct IORateLimiter {
    mode: IORateLimitMode,
    priority_map: [CachePadded<AtomicU32>; IOType::COUNT],
    throughput_limiter: Arc<PriorityBasedIORateLimiter>,
    stats: Option<Arc<IORateLimiterStatistics>>,
}

impl IORateLimiter {
    pub fn new(mode: IORateLimitMode, strict: bool, enable_statistics: bool) -> Self {
        let priority_map: [CachePadded<AtomicU32>; IOType::COUNT] = Default::default();
        for t in priority_map.iter() {
            t.store(IOPriority::High as u32, Ordering::Relaxed);
        }
        IORateLimiter {
            mode,
            priority_map,
            throughput_limiter: Arc::new(PriorityBasedIORateLimiter::new(strict)),
            stats: if enable_statistics {
                Some(Arc::new(IORateLimiterStatistics::new()))
            } else {
                None
            },
        }
    }

    pub fn new_for_test() -> Self {
        IORateLimiter::new(
            IORateLimitMode::AllIo,
            true, /*strict*/
            true, /*enable_statistics*/
        )
    }

    pub fn statistics(&self) -> Option<Arc<IORateLimiterStatistics>> {
        self.stats.clone()
    }

    pub fn set_io_rate_limit(&self, rate: usize) {
        self.throughput_limiter.set_bytes_per_sec(rate);
    }

    pub fn set_io_priority(&self, io_type: IOType, io_priority: IOPriority) {
        self.priority_map[io_type as usize].store(io_priority as u32, Ordering::Relaxed);
    }

    pub fn set_low_priority_io_adjustor_if_needed(
        &self,
        adjustor: Option<Arc<dyn IOBudgetAdjustor>>,
    ) {
        if self.mode != IORateLimitMode::AllIo {
            self.throughput_limiter
                .set_low_priority_io_adjustor(adjustor);
        }
    }

    /// Requests for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, mut bytes: usize) -> usize {
        if self.mode.contains(io_op) {
            bytes = self.throughput_limiter.request(
                IOPriority::unsafe_from_u32(
                    self.priority_map[io_type as usize].load(Ordering::Relaxed),
                ),
                bytes,
            );
        }
        if let Some(stats) = &self.stats {
            stats.record(io_type, io_op, bytes);
        }
        bytes
    }

    /// Asynchronously requests for token for bytes and potentially update
    /// statistics. If this request can not be satisfied, the call is blocked.
    /// Granted token can be less than the requested bytes, but must be greater
    /// than zero.
    pub async fn async_request(&self, io_type: IOType, io_op: IOOp, mut bytes: usize) -> usize {
        if self.mode.contains(io_op) {
            bytes = self
                .throughput_limiter
                .async_request(
                    IOPriority::unsafe_from_u32(
                        self.priority_map[io_type as usize].load(Ordering::Relaxed),
                    ),
                    bytes,
                )
                .await;
        }
        if let Some(stats) = &self.stats {
            stats.record(io_type, io_op, bytes);
        }
        bytes
    }

    #[cfg(test)]
    fn request_with_skewed_clock(&self, io_type: IOType, io_op: IOOp, mut bytes: usize) -> usize {
        if self.mode.contains(io_op) {
            bytes = self.throughput_limiter.request_with_skewed_clock(
                IOPriority::unsafe_from_u32(
                    self.priority_map[io_type as usize].load(Ordering::Relaxed),
                ),
                bytes,
            );
        }
        if let Some(stats) = &self.stats {
            stats.record(io_type, io_op, bytes);
        }
        bytes
    }
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IORateLimiter>>> = Mutex::new(None);
}

// Do NOT use this method in test environment.
pub fn set_io_rate_limiter(limiter: Option<Arc<IORateLimiter>>) {
    *IO_RATE_LIMITER.lock() = limiter;
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    (*IO_RATE_LIMITER.lock()).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Barrier;

    macro_rules! approximate_eq {
        ($left:expr, $right:expr) => {
            assert_ge!(($left), ($right) * 0.95);
            assert_le!(($left), ($right) / 0.95);
        };
    }

    #[derive(Debug, Clone, Copy)]
    struct Request(IOType, IOOp, usize, Option<Duration>);

    struct BackgroundContext {
        limiter: Arc<IORateLimiter>,
        requests: Vec<Request>,
    }

    impl BackgroundContext {
        fn new(limiter: Arc<IORateLimiter>) -> Self {
            Self {
                limiter,
                requests: Vec::new(),
            }
        }
        fn add_background_jobs(&mut self, job_count: usize, request: Request) {
            for _ in 0..job_count {
                self.requests.push(request);
            }
        }
        fn run(&self, duration: Duration) -> Duration {
            let barrier = Arc::new(Barrier::new(self.requests.len()));
            let stop = Arc::new(AtomicBool::new(false));
            let mut threads: Vec<_> = self
                .requests
                .clone()
                .into_iter()
                .map(|r| {
                    let barrier = barrier.clone();
                    let limiter = self.limiter.clone();
                    let stop = stop.clone();
                    std::thread::spawn(move || {
                        let Request(io_type, op, len, interval) = r;
                        barrier.wait();
                        let now = Instant::now();
                        while !stop.load(Ordering::Relaxed) {
                            limiter.request_with_skewed_clock(io_type, op, len);
                            if let Some(interval) = interval {
                                std::thread::sleep(interval);
                            }
                        }
                        now.saturating_elapsed_secs()
                    })
                })
                .collect();
            std::thread::sleep(duration);
            stop.store(true, Ordering::Relaxed);
            let mut total = 0.0;
            for t in threads.drain(..) {
                total += t.join().unwrap();
            }
            Duration::from_secs_f64(total / self.requests.len() as f64)
        }
    }

    #[test]
    fn test_rate_limit_toggle() {
        let bytes_per_sec = 2000;
        let limiter = IORateLimiter::new_for_test();
        limiter.set_io_priority(IOType::Compaction, IOPriority::Low);
        let limiter = Arc::new(limiter);
        let stats = limiter.statistics().unwrap();
        // enable rate limit
        limiter.set_io_rate_limit(bytes_per_sec);
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            1, /*job_count*/
            Request(IOType::ForegroundWrite, IOOp::Write, 10, None),
        );
        ctx.add_background_jobs(
            1, /*job_count*/
            Request(IOType::Compaction, IOOp::Write, 10, None),
        );
        let duration = ctx.run(Duration::from_secs(1));
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
        // disable rate limit
        limiter.set_io_rate_limit(0);
        stats.reset();
        let duration = ctx.run(Duration::from_secs(1));
        assert!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64
                > bytes_per_sec as f64 * duration.as_secs_f64() * 4.0
        );
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write) as f64
                > bytes_per_sec as f64 * duration.as_secs_f64() * 4.0
        );
        // enable rate limit
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        let duration = ctx.run(Duration::from_secs(1));
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
    }

    fn verify_rate_limit(limiter: &Arc<IORateLimiter>, bytes_per_sec: usize, duration: Duration) {
        let stats = limiter.statistics().unwrap();
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        limiter.throughput_limiter.reset();
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            2, /*job_count*/
            Request(IOType::ForegroundWrite, IOOp::Write, 10, None),
        );
        let duration = ctx.run(duration);
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
    }

    #[test]
    fn test_rate_limit_dynamic_priority() {
        let bytes_per_sec = 2000;
        let limiter = Arc::new(IORateLimiter::new(
            IORateLimitMode::AllIo,
            false, /*strict*/
            true,  /*enable_statistics*/
        ));
        limiter.set_io_priority(IOType::ForegroundWrite, IOPriority::Medium);
        verify_rate_limit(&limiter, bytes_per_sec, Duration::from_secs(2));
        limiter.set_io_priority(IOType::ForegroundWrite, IOPriority::High);
        let stats = limiter.statistics().unwrap();
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            2, /*job_count*/
            Request(IOType::ForegroundWrite, IOOp::Write, 10, None),
        );
        let duration = ctx.run(Duration::from_secs(2));
        assert_gt!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64() * 1.5
        );
    }

    #[test]
    fn test_rate_limited_heavy_flow() {
        let low_bytes_per_sec = 2000;
        let high_bytes_per_sec = 10000;
        let limiter = Arc::new(IORateLimiter::new_for_test());
        verify_rate_limit(&limiter, low_bytes_per_sec, Duration::from_secs(2));
        verify_rate_limit(&limiter, high_bytes_per_sec, Duration::from_secs(2));
        verify_rate_limit(&limiter, low_bytes_per_sec, Duration::from_secs(2));
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let limiter = Arc::new(IORateLimiter::new_for_test());
        limiter.set_io_rate_limit(kbytes_per_sec * 1000);
        let stats = limiter.statistics().unwrap();
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            actual_kbytes_per_sec, /*job_count*/
            Request(
                IOType::Compaction,
                IOOp::Write,
                2,
                Some(Duration::from_millis(2)),
            ),
        );
        let duration = ctx.run(Duration::from_secs(2));
        approximate_eq!(
            stats.fetch(IOType::Compaction, IOOp::Write) as f64,
            actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0
        );
    }

    #[test]
    fn test_rate_limited_hybrid_flow() {
        let bytes_per_sec = 100000;
        let write_work = 50;
        let compaction_work = 80;
        let import_work = 50;
        let limiter = IORateLimiter::new_for_test();
        limiter.set_io_rate_limit(bytes_per_sec);
        limiter.set_io_priority(IOType::Compaction, IOPriority::Medium);
        limiter.set_io_priority(IOType::Import, IOPriority::Low);
        let stats = limiter.statistics().unwrap();
        let limiter = Arc::new(limiter);
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            1, /*job_count*/
            Request(
                IOType::ForegroundWrite,
                IOOp::Write,
                write_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        ctx.add_background_jobs(
            1, /*job_count*/
            Request(
                IOType::Compaction,
                IOOp::Write,
                compaction_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        ctx.add_background_jobs(
            1, /*job_count*/
            Request(
                IOType::Import,
                IOOp::Write,
                import_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        let duration = ctx.run(Duration::from_secs(2));
        let write_bytes = stats.fetch(IOType::ForegroundWrite, IOOp::Write);
        let compaction_bytes = stats.fetch(IOType::Compaction, IOOp::Write);
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write);
        let total_bytes = write_bytes + import_bytes + compaction_bytes;
        approximate_eq!((compaction_bytes + write_bytes) as f64, total_bytes as f64);
        approximate_eq!(
            total_bytes as f64,
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
    }

    #[bench]
    fn bench_critical_section(b: &mut test::Bencher) {
        let inner_limiter = PriorityBasedIORateLimiter::new(true /*strict*/);
        inner_limiter.set_bytes_per_sec(1024);
        let now = Instant::now_coarse();
        b.iter(|| {
            inner_limiter.critical_section(now);
        });
    }
}
