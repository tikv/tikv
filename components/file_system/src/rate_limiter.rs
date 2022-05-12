// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::EnumCount;
use tikv_util::time::Instant;

use super::{
    metrics::{tls_collect_rate_limiter_request_wait, RATE_LIMITER_MAX_BYTES_PER_SEC},
    IOOp, IOPriority, IOType,
};

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(50);
const DEFAULT_REFILLS_PER_SEC: usize = (1.0 / DEFAULT_REFILL_PERIOD.as_secs_f32()) as usize;
const MAX_WAIT_DURATION_PER_REQUEST: Duration = Duration::from_millis(500);

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
                            Unexpected::Other("invalid IO rate limit mode"),
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
    // Total bytes passed through during current epoch
    bytes_through: [CachePadded<AtomicUsize>; IOPriority::COUNT],
    // Maximum bytes permitted during current epoch
    bytes_per_epoch: [CachePadded<AtomicUsize>; IOPriority::COUNT],
    protected: Mutex<PriorityBasedIORateLimiterProtected>,
}

struct PriorityBasedIORateLimiterProtected {
    next_refill_time: Instant,
    // Bytes that can't be fulfilled in current epoch
    pending_bytes: [usize; IOPriority::COUNT],
    // Adjust low priority IO flow based on system backlog
    adjustor: Option<Arc<dyn IOBudgetAdjustor>>,
}

impl PriorityBasedIORateLimiterProtected {
    fn new() -> Self {
        PriorityBasedIORateLimiterProtected {
            next_refill_time: Instant::now_coarse() + DEFAULT_REFILL_PERIOD,
            pending_bytes: [0; IOPriority::COUNT],
            adjustor: None,
        }
    }
}

macro_rules! do_sleep {
    ($duration:expr, sync) => {
        std::thread::sleep($duration);
    };
    ($duration:expr, async) => {
        tokio::time::sleep($duration).await;
    };
    ($duration:expr, skewed_sync) => {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let subtraction: bool = rng.gen();
        let offset = std::cmp::min(Duration::from_millis(1), ($duration) / 100);
        std::thread::sleep(if subtraction {
            $duration - offset
        } else {
            $duration + offset
        });
    };
}

/// Actual implementation for requesting IOs from PriorityBasedIORateLimiter.
/// An attempt will first be recorded. If the attempted amount exceeds the available quotas of
/// current epoch, the requester will be queued (logically) and sleep until served.
/// Macro is necessary to de-dup codes used both in async/sync functions.
macro_rules! request_imp {
    ($limiter:ident, $priority:ident, $amount:ident, $mode:tt) => {{
        debug_assert!($amount > 0);
        let priority_idx = $priority as usize;
        let cached_bytes_per_epoch = $limiter.bytes_per_epoch[priority_idx].load(Ordering::Relaxed);
        // Flow control is disabled when limit is zero.
        if cached_bytes_per_epoch == 0 {
            return $amount;
        }
        let mut amount = std::cmp::min($amount, cached_bytes_per_epoch);
        let bytes_through =
            $limiter.bytes_through[priority_idx].fetch_add(amount, Ordering::Relaxed) + amount;
        // We prefer not to partially return only a portion of requested bytes.
        if bytes_through <= cached_bytes_per_epoch
            || !$limiter.strict && $priority == IOPriority::High
        {
            return amount;
        }
        let wait = {
            let now = Instant::now_coarse();
            let mut locked = $limiter.protected.lock();
            // The request is already partially fulfilled in current epoch when consumption
            // overflow bytes are smaller than requested amount.
            let remains = std::cmp::min(bytes_through - cached_bytes_per_epoch, amount);
            // When there is a recent refill, double check if bytes consumption has been reset.
            if now + DEFAULT_REFILL_PERIOD < locked.next_refill_time + Duration::from_millis(1)
                && $limiter.bytes_through[priority_idx].fetch_add(remains, Ordering::Relaxed)
                    + remains
                    <= cached_bytes_per_epoch
            {
                return amount;
            }
            // Enqueue itself by adding to pending_bytes, whose current value denotes a position
            // of logical queue to wait in.
            locked.pending_bytes[priority_idx] += remains;
            // Calculate wait duration by queue_len / served_per_epoch.
            let wait = if locked.next_refill_time <= now {
                $limiter.refill(&mut locked, now);
                // Bytes served by next epoch (and skipped epochs) during refill are subtracted
                // from pending_bytes, round up the rest.
                DEFAULT_REFILL_PERIOD
                    * ((locked.pending_bytes[priority_idx] + cached_bytes_per_epoch - 1)
                        / cached_bytes_per_epoch) as u32
            } else {
                // `(a-1)/b` is equivalent to `roundup(a.saturating_sub(b)/b)`.
                locked.next_refill_time - now
                    + DEFAULT_REFILL_PERIOD
                        * ((locked.pending_bytes[priority_idx] - 1) / cached_bytes_per_epoch) as u32
            };
            if wait > MAX_WAIT_DURATION_PER_REQUEST {
                // Long wait duration could freeze request thread not to react to latest budgets
                // adjustment. Exit early by returning partial quotas.
                amount = std::cmp::max(
                    (MAX_WAIT_DURATION_PER_REQUEST.as_secs_f32() * amount as f32
                        / wait.as_secs_f32()) as usize,
                    1,
                );
                // Subtracting redundant bytes requested before.
                // In this situation, the prolonged wait is caused by accumulated pending-bytes,
                // and bytes-through is always saturated.
                // `saturating_sub`: Better safe than sorry.
                locked.pending_bytes[priority_idx] = locked.pending_bytes[priority_idx]
                    .saturating_sub(remains.saturating_sub(amount));
                MAX_WAIT_DURATION_PER_REQUEST
            } else {
                wait
            }
        };
        tls_collect_rate_limiter_request_wait($priority.as_str(), wait);
        do_sleep!(wait, $mode);
        amount
    }};
}

impl PriorityBasedIORateLimiter {
    fn new(strict: bool) -> Self {
        PriorityBasedIORateLimiter {
            strict,
            bytes_through: Default::default(),
            bytes_per_epoch: Default::default(),
            protected: Mutex::new(PriorityBasedIORateLimiterProtected::new()),
        }
    }

    /// Dynamically changes the total IO flow threshold.
    fn set_bytes_per_sec(&self, bytes_per_sec: usize) {
        let now = (bytes_per_sec as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as usize;
        let before = self.bytes_per_epoch[IOPriority::High as usize].swap(now, Ordering::Relaxed);
        RATE_LIMITER_MAX_BYTES_PER_SEC
            .high
            .set(bytes_per_sec as i64);
        if now == 0 || before == 0 {
            // Toggle on or off rate limit.
            let _locked = self.protected.lock();
            self.bytes_per_epoch[IOPriority::Medium as usize].store(now, Ordering::Relaxed);
            RATE_LIMITER_MAX_BYTES_PER_SEC
                .medium
                .set(bytes_per_sec as i64);
            self.bytes_per_epoch[IOPriority::Low as usize].store(now, Ordering::Relaxed);
            RATE_LIMITER_MAX_BYTES_PER_SEC.low.set(bytes_per_sec as i64);
        }
    }

    fn set_low_priority_io_adjustor(&self, adjustor: Option<Arc<dyn IOBudgetAdjustor>>) {
        let mut locked = self.protected.lock();
        locked.adjustor = adjustor;
    }

    fn request(&self, priority: IOPriority, amount: usize) -> usize {
        request_imp!(self, priority, amount, sync)
    }

    async fn async_request(&self, priority: IOPriority, amount: usize) -> usize {
        request_imp!(self, priority, amount, async)
    }

    #[cfg(test)]
    fn request_with_skewed_clock(&self, priority: IOPriority, amount: usize) -> usize {
        request_imp!(self, priority, amount, skewed_sync)
    }

    /// Updates and refills IO budgets for next epoch based on IO priority.
    /// Here we provide best-effort priority control:
    /// 1) Limited IO budget is assigned to lower priority to ensure higher priority can at least
    ///    consume the same IO amount as the last few epochs without breaching global threshold.
    /// 2) Higher priority may temporarily use lower priority's IO budgets. When this happens,
    ///    total IO flow could exceed global threshold.
    /// 3) Highest priority IO alone must not exceed global threshold (in strict mode).
    fn refill(&self, locked: &mut PriorityBasedIORateLimiterProtected, now: Instant) {
        let mut total_budgets =
            self.bytes_per_epoch[IOPriority::High as usize].load(Ordering::Relaxed);
        if total_budgets == 0 {
            // It's possible that rate limit is toggled off in the meantime.
            return;
        }
        debug_assert!(now >= locked.next_refill_time);
        let skipped_epochs =
            (now - locked.next_refill_time).as_secs_f32() / DEFAULT_REFILL_PERIOD.as_secs_f32();
        locked.next_refill_time = now + DEFAULT_REFILL_PERIOD;

        debug_assert!(
            IOPriority::High as usize == IOPriority::Medium as usize + 1
                && IOPriority::Medium as usize == IOPriority::Low as usize + 1
        );
        let mut remaining_budgets = total_budgets;
        let mut used_budgets = 0;
        for pri in &[IOPriority::High, IOPriority::Medium] {
            let p = *pri as usize;
            // Skipped epochs can only serve pending requests rather that in-coming ones, catch up
            // by subtracting them from pending_bytes.
            let served_by_skipped_epochs = std::cmp::min(
                (remaining_budgets as f32 * skipped_epochs) as usize,
                locked.pending_bytes[p],
            );
            locked.pending_bytes[p] -= served_by_skipped_epochs;
            // Reserve some of new epoch's budgets to serve pending bytes.
            let to_serve_pending_bytes = std::cmp::min(locked.pending_bytes[p], remaining_budgets);
            locked.pending_bytes[p] -= to_serve_pending_bytes;
            // Update throughput estimation over recent epochs.
            let served_by_first_epoch = std::cmp::min(
                self.bytes_through[p].swap(to_serve_pending_bytes, Ordering::Relaxed),
                remaining_budgets,
            );
            used_budgets += ((served_by_first_epoch + served_by_skipped_epochs) as f32
                / (skipped_epochs + 1.0)) as usize;
            // Only apply rate limit adjustments on low-priority IOs.
            if *pri == IOPriority::Medium {
                if let Some(adjustor) = &locked.adjustor {
                    total_budgets = adjustor.adjust(total_budgets);
                }
            }
            remaining_budgets = if total_budgets > used_budgets {
                total_budgets - used_budgets
            } else {
                1 // A small positive value so not to disable flow control.
            };
            if *pri == IOPriority::High {
                RATE_LIMITER_MAX_BYTES_PER_SEC
                    .medium
                    .set((remaining_budgets * DEFAULT_REFILLS_PER_SEC) as i64);
            } else {
                RATE_LIMITER_MAX_BYTES_PER_SEC
                    .low
                    .set((remaining_budgets * DEFAULT_REFILLS_PER_SEC) as i64);
            }
            self.bytes_per_epoch[p - 1].store(remaining_budgets, Ordering::Relaxed);
        }
        let p = IOPriority::Low as usize;
        let to_serve_pending_bytes = std::cmp::min(locked.pending_bytes[p], remaining_budgets);
        locked.pending_bytes[p] -= to_serve_pending_bytes;
        self.bytes_through[p].store(to_serve_pending_bytes, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn critical_section(&self, now: Instant) {
        let mut locked = self.protected.lock();
        locked.next_refill_time = now;
        self.refill(&mut locked, now);
    }

    #[cfg(test)]
    fn reset(&self) {
        let mut locked = self.protected.lock();
        for p in &[IOPriority::High, IOPriority::Medium] {
            let p = *p as usize;
            locked.pending_bytes[p] = 0;
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
        for p in priority_map.iter() {
            p.store(IOPriority::High as u32, Ordering::Relaxed);
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
    use std::sync::atomic::AtomicBool;

    use super::*;

    macro_rules! approximate_eq {
        ($left:expr, $right:expr) => {
            assert!(($left) >= ($right) * 0.85);
            assert!(($right) >= ($left) * 0.85);
        };
    }

    struct BackgroundContext {
        threads: Vec<std::thread::JoinHandle<()>>,
        stop: Option<Arc<AtomicBool>>,
    }

    impl Drop for BackgroundContext {
        fn drop(&mut self) {
            if let Some(stop) = &self.stop {
                stop.store(true, Ordering::Relaxed);
            }
            for t in self.threads.drain(..) {
                t.join().unwrap();
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct Request(IOType, IOOp, usize);

    fn start_background_jobs(
        limiter: &Arc<IORateLimiter>,
        job_count: usize,
        request: Request,
        interval: Option<Duration>,
    ) -> BackgroundContext {
        let mut threads = vec![];
        let stop = Arc::new(AtomicBool::new(false));
        for _ in 0..job_count {
            let stop = stop.clone();
            let limiter = limiter.clone();
            let t = std::thread::spawn(move || {
                let Request(io_type, op, len) = request;
                while !stop.load(Ordering::Relaxed) {
                    limiter.request_with_skewed_clock(io_type, op, len);
                    if let Some(interval) = interval {
                        std::thread::sleep(interval);
                    }
                }
            });
            threads.push(t);
        }
        BackgroundContext {
            threads,
            stop: Some(stop),
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
        let t0 = Instant::now();
        let _write_context = start_background_jobs(
            &limiter,
            1, /*job_count*/
            Request(IOType::ForegroundWrite, IOOp::Write, 10),
            None, /*interval*/
        );
        let _compaction_context = start_background_jobs(
            &limiter,
            1, /*job_count*/
            Request(IOType::Compaction, IOOp::Write, 10),
            None, /*interval*/
        );
        std::thread::sleep(Duration::from_secs(1));
        let t1 = Instant::now();
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * (t1 - t0).as_secs_f64()
        );
        // disable rate limit
        limiter.set_io_rate_limit(0);
        stats.reset();
        std::thread::sleep(Duration::from_secs(1));
        let t2 = Instant::now();
        assert!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64
                > bytes_per_sec as f64 * (t2 - t1).as_secs_f64() * 4.0
        );
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write) as f64
                > bytes_per_sec as f64 * (t2 - t1).as_secs_f64() * 4.0
        );
        // enable rate limit
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        std::thread::sleep(Duration::from_secs(1));
        let t3 = Instant::now();
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * (t3 - t2).as_secs_f64()
        );
    }

    fn verify_rate_limit(limiter: &Arc<IORateLimiter>, bytes_per_sec: usize, duration: Duration) {
        let stats = limiter.statistics().unwrap();
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        limiter.throughput_limiter.reset();
        let actual_duration = {
            let begin = Instant::now();
            {
                let _context = start_background_jobs(
                    limiter,
                    2, /*job_count*/
                    Request(IOType::ForegroundWrite, IOOp::Write, 10),
                    None, /*interval*/
                );
                std::thread::sleep(duration);
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        approximate_eq!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64,
            bytes_per_sec as f64 * actual_duration.as_secs_f64()
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
        stats.reset();
        let duration = {
            let begin = Instant::now();
            {
                let _context = start_background_jobs(
                    &limiter,
                    2, /*job_count*/
                    Request(IOType::ForegroundWrite, IOOp::Write, 10),
                    None, /*interval*/
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        assert!(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write) as f64
                > bytes_per_sec as f64 * duration.as_secs_f64() * 1.5
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
        let duration = {
            let begin = Instant::now();
            {
                // each thread request at most 1000 bytes per second
                let _context = start_background_jobs(
                    &limiter,
                    actual_kbytes_per_sec, /*job_count*/
                    Request(IOType::Compaction, IOOp::Write, 1),
                    Some(Duration::from_millis(1)),
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
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
        let begin = Instant::now();
        {
            let _write = start_background_jobs(
                &limiter,
                1, /*job_count*/
                Request(
                    IOType::ForegroundWrite,
                    IOOp::Write,
                    write_work * bytes_per_sec / 100 / 1000,
                ),
                Some(Duration::from_millis(1)),
            );
            let _compaction = start_background_jobs(
                &limiter,
                1, /*job_count*/
                Request(
                    IOType::Compaction,
                    IOOp::Write,
                    compaction_work * bytes_per_sec / 100 / 1000,
                ),
                Some(Duration::from_millis(1)),
            );
            let _import = start_background_jobs(
                &limiter,
                1, /*job_count*/
                Request(
                    IOType::Import,
                    IOOp::Write,
                    import_work * bytes_per_sec / 100 / 1000,
                ),
                Some(Duration::from_millis(1)),
            );
            std::thread::sleep(Duration::from_secs(2));
        }
        let end = Instant::now();
        let duration = end.duration_since(begin);
        let write_bytes = stats.fetch(IOType::ForegroundWrite, IOOp::Write);
        approximate_eq!(
            write_bytes as f64,
            (write_work * bytes_per_sec / 100) as f64 * duration.as_secs_f64()
        );
        let compaction_bytes = stats.fetch(IOType::Compaction, IOOp::Write);
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write);
        let total_bytes = write_bytes + import_bytes + compaction_bytes;
        approximate_eq!((compaction_bytes + write_bytes) as f64, total_bytes as f64);
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
