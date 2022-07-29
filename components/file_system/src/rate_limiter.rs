// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicI64, AtomicU32, AtomicUsize, Ordering},
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
    io_stats,
    metrics::{tls_collect_rate_limiter_request_wait, RATE_LIMITER_MAX_BYTES_PER_SEC},
    IoOp, IoPriority, IoType,
};

/// Theoretically a smaller refill period increases CPU overhead while reduces
/// busty IOs. In practice the value of this parameter is of little importance.
pub(crate) const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(100);
pub(crate) const DEFAULT_REFILLS_PER_SEC: usize =
    (1.0 / DEFAULT_REFILL_PERIOD.as_secs_f32()) as usize;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum IoRateLimitMode {
    WriteOnly,
    ReadOnly,
    AllIo,
}

impl IoRateLimitMode {
    pub fn as_str(&self) -> &str {
        match *self {
            IoRateLimitMode::WriteOnly => "write-only",
            IoRateLimitMode::ReadOnly => "read-only",
            IoRateLimitMode::AllIo => "all-io",
        }
    }

    #[inline]
    pub fn contains(&self, op: IoOp) -> bool {
        match *self {
            IoRateLimitMode::WriteOnly => op == IoOp::Write,
            IoRateLimitMode::ReadOnly => op == IoOp::Read,
            _ => true,
        }
    }
}

impl FromStr for IoRateLimitMode {
    type Err = String;
    fn from_str(s: &str) -> Result<IoRateLimitMode, String> {
        match s {
            "write-only" => Ok(IoRateLimitMode::WriteOnly),
            "read-only" => Ok(IoRateLimitMode::ReadOnly),
            "all-io" => Ok(IoRateLimitMode::AllIo),
            s => Err(format!(
                "expect: write-only, read-only or all-io, got: {:?}",
                s
            )),
        }
    }
}

impl Serialize for IoRateLimitMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IoRateLimitMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Unexpected, Visitor};
        struct StrVistor;
        impl<'de> Visitor<'de> for StrVistor {
            type Value = IoRateLimitMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a I/O rate limit mode")
            }

            fn visit_str<E>(self, value: &str) -> Result<IoRateLimitMode, E>
            where
                E: Error,
            {
                let p = match IoRateLimitMode::from_str(&*value.trim().to_lowercase()) {
                    Ok(p) => p,
                    _ => {
                        return Err(E::invalid_value(
                            Unexpected::Other("invalid I/O rate limit mode"),
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
#[derive(Debug, Default)]
pub struct IoRateLimiterStatistics {
    read_bytes: [CachePadded<AtomicUsize>; IoType::COUNT],
    write_bytes: [CachePadded<AtomicUsize>; IoType::COUNT],
}

impl IoRateLimiterStatistics {
    pub fn fetch(&self, io_type: IoType, io_op: IoOp) -> usize {
        let io_type_idx = io_type as usize;
        match io_op {
            IoOp::Read => self.read_bytes[io_type_idx].load(Ordering::Relaxed),
            IoOp::Write => self.write_bytes[io_type_idx].load(Ordering::Relaxed),
        }
    }

    pub fn record(&self, io_type: IoType, io_op: IoOp, bytes: usize) {
        let io_type_idx = io_type as usize;
        match io_op {
            IoOp::Read => {
                self.read_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
            }
            IoOp::Write => {
                self.write_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn reset(&self) {
        for i in 0..IoType::COUNT {
            self.read_bytes[i].store(0, Ordering::Relaxed);
            self.write_bytes[i].store(0, Ordering::Relaxed);
        }
    }
}

/// A high-performance I/O rate limiter used for prioritized flow control.
/// Limit total I/O flow below provided threshold by throttling lower-priority
/// I/Os. Rate limiting is disabled when total I/O threshold is set to zero.
/// Can be safely shared between threads.
pub struct IoRateLimiter {
    /// When set to false, high-priority I/Os will not be throttled.
    strict: bool,
    mode: IoRateLimitMode,
    priority_map: [CachePadded<AtomicU32>; IoType::COUNT],
    batch_buffered_reads: Option<usize>,

    /// Available virtual I/O quotas of current epoch. Could be negative.
    /// Negative value represents unfulfilled I/O requests.
    bytes_available: [CachePadded<AtomicI64>; IoPriority::COUNT],
    /// Maximum physical I/O bytes of all I/O types permitted during current
    /// epoch.
    bytes_per_epoch: CachePadded<AtomicUsize>,

    protected: Mutex<IoRateLimiterInner>,

    stats: Option<Arc<IoRateLimiterStatistics>>,
}

struct IoRateLimiterInner {
    next_refill_time: Instant,
}

impl IoRateLimiterInner {
    fn new() -> Self {
        IoRateLimiterInner {
            next_refill_time: Instant::now_coarse() + DEFAULT_REFILL_PERIOD,
        }
    }
}

/// Macro that can unfold to both async or sync code.
macro_rules! do_sleep {
    ($duration:expr,sync) => {
        std::thread::sleep($duration);
    };
    ($duration:expr,async) => {
        let wait =
            tikv_util::timer::GLOBAL_TIMER_HANDLE.delay(std::time::Instant::now() + $duration);
        let _ = futures::compat::Compat01As03::new(wait).await;
    };
    ($duration:expr,skewed_sync) => {
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

/// Macro that can unfold to both async or sync code.
macro_rules! request_physical_imp {
    ($self:ident, $priority:ident, $bytes:expr, $mode:tt) => {{
        let priority_idx = $priority as usize;
        let cached_bytes_per_epoch = $self.bytes_per_epoch.load(Ordering::Relaxed);
        // Flow control is disabled when limit is zero.
        if cached_bytes_per_epoch > 0 {
            let bytes = $bytes;
            let remains = $self.bytes_available[priority_idx]
                .fetch_sub(bytes as i64, Ordering::Relaxed)
                - bytes as i64;
            if remains < 0 && ($priority != IoPriority::High || $self.strict) {
                let mut total_wait = Duration::default();
                loop {
                    let now = Instant::now_coarse();
                    let wait = {
                        let mut locked = $self.protected.lock();
                        if now + DEFAULT_REFILL_PERIOD / 16 >= locked.next_refill_time {
                            $self.refill(&mut locked, now);
                        }
                        if $self.bytes_available[priority_idx].load(Ordering::Relaxed) >= 0 {
                            break;
                        }
                        locked.next_refill_time - now
                    };
                    do_sleep!(wait, $mode);
                    total_wait += wait;
                }
                if !total_wait.is_zero() {
                    tls_collect_rate_limiter_request_wait($priority.as_str(), total_wait);
                }
            }
        }
    }};
}

impl IoRateLimiter {
    pub fn new(
        mode: IoRateLimitMode,
        strict: bool,
        batch_buffered_reads: Option<usize>,
        enable_statistics: bool,
    ) -> Self {
        let priority_map: [CachePadded<AtomicU32>; IoType::COUNT] = Default::default();
        for p in priority_map.iter() {
            p.store(IoPriority::High as u32, Ordering::Relaxed);
        }
        IoRateLimiter {
            strict,
            mode,
            priority_map,
            batch_buffered_reads,
            bytes_available: Default::default(),
            bytes_per_epoch: Default::default(),
            protected: Mutex::new(IoRateLimiterInner::new()),
            stats: if enable_statistics {
                Some(Arc::new(IoRateLimiterStatistics::default()))
            } else {
                None
            },
        }
    }

    pub fn new_for_test() -> Self {
        IoRateLimiter::new(
            IoRateLimitMode::AllIo,
            true, // strict
            None, // batch_buffered_reads
            true, // enable_statistics
        )
    }

    pub fn statistics(&self) -> Option<Arc<IoRateLimiterStatistics>> {
        self.stats.clone()
    }

    /// Dynamically changes the total I/O flow threshold.
    pub fn set_io_rate_limit(&self, bytes_per_sec: usize) {
        let new_rate = (bytes_per_sec as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as usize;
        let mut locked = self.protected.lock();
        self.reset_rate(&mut locked, new_rate);
        RATE_LIMITER_MAX_BYTES_PER_SEC
            .high
            .set(bytes_per_sec as i64);
        RATE_LIMITER_MAX_BYTES_PER_SEC
            .medium
            .set(bytes_per_sec as i64);
        RATE_LIMITER_MAX_BYTES_PER_SEC.low.set(bytes_per_sec as i64);
    }

    pub fn with_io_rate_limit<F>(&self, f: F)
    where
        F: FnOnce(usize) -> Option<usize>,
    {
        let mut locked = self.protected.lock();
        let old_bytes_per_sec = (self.bytes_per_epoch.load(Ordering::Relaxed) as f64
            / DEFAULT_REFILL_PERIOD.as_secs_f64()) as usize;
        if let Some(bytes_per_sec) = f(old_bytes_per_sec) {
            let new_rate = (bytes_per_sec as f64 * DEFAULT_REFILL_PERIOD.as_secs_f64()) as usize;
            self.reset_rate(&mut locked, new_rate);
            RATE_LIMITER_MAX_BYTES_PER_SEC
                .high
                .set(bytes_per_sec as i64);
            RATE_LIMITER_MAX_BYTES_PER_SEC
                .medium
                .set(bytes_per_sec as i64);
            RATE_LIMITER_MAX_BYTES_PER_SEC.low.set(bytes_per_sec as i64);
        }
    }

    pub fn set_io_priority(&self, io_type: IoType, io_priority: IoPriority) {
        let mut locked = self.protected.lock();
        self.priority_map[io_type as usize].store(io_priority as u32, Ordering::Relaxed);
        let rate = self.bytes_per_epoch.load(Ordering::Relaxed);
        self.reset_rate(&mut locked, rate);
    }

    fn reset_rate(&self, locked: &mut IoRateLimiterInner, rate: usize) {
        // Trigger a refill immediately. This avoids cold-start effect during
        // unit testing.
        locked.next_refill_time = Instant::now_coarse();
        self.bytes_per_epoch.store(rate, Ordering::Relaxed);
        for p in 0..IoPriority::COUNT {
            self.bytes_available[p].store(0_i64, Ordering::Relaxed);
        }
    }

    /// Requests for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_op: IoOp, bytes: usize) -> usize {
        let mut ctx = io_stats::get_io_context();
        if ctx.defer_mode {
            // Bypass all limits in defer mode.
            return bytes;
        }
        if self.mode.contains(io_op) {
            let priority = IoPriority::unsafe_from_u32(
                self.priority_map[ctx.io_type as usize].load(Ordering::Relaxed),
            );
            if io_op == IoOp::Write || self.batch_buffered_reads.is_none() {
                request_physical_imp!(self, priority, bytes, sync);
            } else {
                let batch = self.batch_buffered_reads.unwrap();
                ctx.outstanding_read_bytes += bytes;
                if ctx.outstanding_read_bytes >= batch {
                    let true_bytes = io_stats::fetch_thread_io_bytes().read;
                    request_physical_imp!(
                        self,
                        priority,
                        std::cmp::min(
                            true_bytes - ctx.total_read_bytes,
                            // Just in case something went wrong with OS stats.
                            ctx.outstanding_read_bytes * 2,
                        ),
                        sync
                    );
                    ctx.outstanding_read_bytes = 0;
                    ctx.total_read_bytes = true_bytes;
                }
            }
            io_stats::set_io_context(ctx);
        }
        if let Some(stats) = &self.stats {
            stats.record(ctx.io_type, io_op, bytes);
        }
        bytes
    }

    /// Asynchronously requests for token for bytes and potentially update
    /// statistics. If this request can not be satisfied, the call is blocked.
    /// Granted token can be less than the requested bytes, but must be greater
    /// than zero.
    /// The coroutine can potentially be scheduled away during the call. Thread
    /// local I/O context will not be accessed in it. All requested bytes are
    /// treated as physical I/Os without further scrutiny.
    pub async fn async_request(&self, io_type: IoType, bytes: usize) -> usize {
        let priority = IoPriority::unsafe_from_u32(
            self.priority_map[io_type as usize].load(Ordering::Relaxed),
        );
        request_physical_imp!(self, priority, bytes, async);
        bytes
    }

    #[cfg(test)]
    fn request_for_test(&self, io_type: IoType, io_op: IoOp, bytes: usize) -> usize {
        if self.mode.contains(io_op) {
            let priority = IoPriority::unsafe_from_u32(
                self.priority_map[io_type as usize].load(Ordering::Relaxed),
            );
            request_physical_imp!(self, priority, bytes, sync);
        }
        if let Some(stats) = &self.stats {
            stats.record(io_type, io_op, bytes);
        }
        bytes
    }

    /// Updates and refills I/O budgets for next epoch based on I/O priority.
    /// This method is a no-op when high-priority I/O rate limit equals zero.
    /// Here we provide best-effort priority control:
    /// 1) Limited I/O budget is assigned to lower priority to ensure higher
    /// priority can at least    consume the same I/O amount as the last few
    /// epochs without breaching global threshold. 2) Higher priority may
    /// temporarily use lower priority's I/O budgets. When this happens,
    ///    total I/O flow could exceed global threshold.
    /// 3) Highest priority I/O alone must not exceed global threshold (in
    /// strict mode).
    fn refill(&self, locked: &mut IoRateLimiterInner, now: Instant) {
        let mut budgets = self.bytes_per_epoch.load(Ordering::Relaxed);
        if budgets == 0 {
            // Rate limit is toggled off in the meantime.
            return;
        }
        let new_refill_time = now + DEFAULT_REFILL_PERIOD;
        let elapsed_epochs = (new_refill_time - locked.next_refill_time).as_secs_f32()
            / DEFAULT_REFILL_PERIOD.as_secs_f32();
        locked.next_refill_time = new_refill_time;

        for pri in &[IoPriority::High, IoPriority::Medium, IoPriority::Low] {
            let p = *pri as usize;

            match *pri {
                IoPriority::Medium => RATE_LIMITER_MAX_BYTES_PER_SEC
                    .medium
                    .set((budgets * DEFAULT_REFILLS_PER_SEC) as i64),
                IoPriority::Low => {
                    RATE_LIMITER_MAX_BYTES_PER_SEC
                        .low
                        .set((budgets * DEFAULT_REFILLS_PER_SEC) as i64);
                }
                _ => {}
            }

            let old_available = self.bytes_available[p]
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |available| {
                    Some(std::cmp::min(available + budgets as i64, budgets as i64))
                })
                .unwrap();
            let average_usage = ((budgets as f32 - std::cmp::max(old_available, 0) as f32)
                / elapsed_epochs) as usize;
            budgets = budgets.saturating_sub(average_usage);
        }
    }

    #[cfg(test)]
    fn critical_section(&self, now: Instant) {
        let mut locked = self.protected.lock();
        locked.next_refill_time = now;
        self.refill(&mut locked, now);
    }
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IoRateLimiter>>> = Mutex::new(None);
}

// Do NOT use this method in test environment.
pub fn set_io_rate_limiter(limiter: Option<Arc<IoRateLimiter>>) {
    *IO_RATE_LIMITER.lock() = limiter;
}

pub fn get_io_rate_limiter() -> Option<Arc<IoRateLimiter>> {
    (*IO_RATE_LIMITER.lock()).clone()
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;

    macro_rules! approximate_eq {
        ($left:expr, $right:expr) => {
            assert_ge!(($left) as f64, ($right) as f64 * 0.95);
            assert_le!(($left) as f64, ($right) as f64 / 0.95);
        };
    }

    #[derive(Debug, Clone, Copy)]
    struct Request(IoType, IoOp, usize, Option<Duration>);

    struct BackgroundContext {
        limiter: Arc<IoRateLimiter>,
        requests: Vec<Request>,
    }

    impl BackgroundContext {
        fn new(limiter: Arc<IoRateLimiter>) -> Self {
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
        fn run(&self, duration: Duration) {
            let barrier = Arc::new(Barrier::new(self.requests.len() + 1));
            let mut threads: Vec<_> = self
                .requests
                .clone()
                .into_iter()
                .map(|r| {
                    let barrier = barrier.clone();
                    let limiter = self.limiter.clone();
                    std::thread::spawn(move || {
                        let Request(io_type, op, len, interval) = r;
                        barrier.wait();
                        let start = Instant::now_coarse();
                        while start.saturating_elapsed() < duration {
                            limiter.request_for_test(io_type, op, len);
                            if let Some(interval) = interval {
                                std::thread::sleep(interval);
                            }
                        }
                    })
                })
                .collect();
            barrier.wait();
            std::thread::sleep(duration + Duration::from_millis(10));
            for t in threads.drain(..) {
                t.join().unwrap();
            }
        }
    }

    #[test]
    fn test_rate_limit_toggle() {
        let bytes_per_sec = 2000;
        let limiter = IoRateLimiter::new_for_test();
        limiter.set_io_priority(IoType::Compaction, IoPriority::Low);
        let limiter = Arc::new(limiter);
        let stats = limiter.statistics().unwrap();
        // enable rate limit
        limiter.set_io_rate_limit(bytes_per_sec);
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            1, // job_count
            Request(IoType::ForegroundWrite, IoOp::Write, 10, None),
        );
        ctx.add_background_jobs(
            1, // job_count
            Request(IoType::Compaction, IoOp::Write, 10, None),
        );
        let duration = Duration::from_secs(2);
        ctx.run(duration);
        approximate_eq!(
            stats.fetch(IoType::ForegroundWrite, IoOp::Write),
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
        // disable rate limit
        limiter.set_io_rate_limit(0);
        stats.reset();
        ctx.run(duration);
        assert_gt!(
            stats.fetch(IoType::ForegroundWrite, IoOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64() * 4.0
        );
        assert_gt!(
            stats.fetch(IoType::Compaction, IoOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64() * 4.0
        );
        // enable rate limit
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        ctx.run(duration);
        approximate_eq!(
            stats.fetch(IoType::ForegroundWrite, IoOp::Write),
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
    }

    fn verify_rate_limit(limiter: &Arc<IoRateLimiter>, bytes_per_sec: usize, duration: Duration) {
        let stats = limiter.statistics().unwrap();
        limiter.set_io_rate_limit(bytes_per_sec);
        stats.reset();
        let mut ctx = BackgroundContext::new(limiter.clone());
        ctx.add_background_jobs(
            2, // job_count
            Request(IoType::ForegroundWrite, IoOp::Write, 10, None),
        );
        ctx.run(duration);
        approximate_eq!(
            stats.fetch(IoType::ForegroundWrite, IoOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64()
        );
    }

    #[test]
    fn test_rate_limit_dynamic_priority() {
        let bytes_per_sec = 2000;
        let limiter = Arc::new(IoRateLimiter::new(
            IoRateLimitMode::AllIo,
            false, // strict
            None,  // batch_buffered_reads
            true,  // enable_statistics
        ));
        limiter.set_io_priority(IoType::ForegroundWrite, IoPriority::Medium);
        verify_rate_limit(&limiter, bytes_per_sec, Duration::from_secs(2));
        limiter.set_io_priority(IoType::ForegroundWrite, IoPriority::High);
        let stats = limiter.statistics().unwrap();
        let mut ctx = BackgroundContext::new(limiter);
        ctx.add_background_jobs(
            2, // job_count
            Request(IoType::ForegroundWrite, IoOp::Write, 10, None),
        );
        let duration = Duration::from_secs(2);
        ctx.run(duration);
        assert_gt!(
            stats.fetch(IoType::ForegroundWrite, IoOp::Write) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64() * 4.0
        );
    }

    #[test]
    fn test_rate_limited_heavy_flow() {
        let low_bytes_per_sec = 2000;
        let high_bytes_per_sec = 10000;
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        verify_rate_limit(&limiter, low_bytes_per_sec, Duration::from_secs(2));
        verify_rate_limit(&limiter, high_bytes_per_sec, Duration::from_secs(2));
        verify_rate_limit(&limiter, low_bytes_per_sec, Duration::from_secs(2));
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        limiter.set_io_rate_limit(kbytes_per_sec * 1000);
        let stats = limiter.statistics().unwrap();
        let mut ctx = BackgroundContext::new(limiter);
        ctx.add_background_jobs(
            actual_kbytes_per_sec, // job_count
            Request(
                IoType::Compaction,
                IoOp::Write,
                2,
                Some(Duration::from_millis(2)),
            ),
        );
        let duration = Duration::from_secs(2);
        ctx.run(duration);
        approximate_eq!(
            stats.fetch(IoType::Compaction, IoOp::Write),
            actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0
        );
    }

    #[test]
    fn test_rate_limited_hybrid_flow() {
        let bytes_per_sec = 100000;
        let write_work = 50;
        let compaction_work = 80;
        let import_work = 50;
        let limiter = IoRateLimiter::new_for_test();
        limiter.set_io_rate_limit(bytes_per_sec);
        limiter.set_io_priority(IoType::Compaction, IoPriority::Medium);
        limiter.set_io_priority(IoType::Import, IoPriority::Low);
        let stats = limiter.statistics().unwrap();
        let limiter = Arc::new(limiter);
        let mut ctx = BackgroundContext::new(limiter);
        ctx.add_background_jobs(
            1, // job_count
            Request(
                IoType::ForegroundWrite,
                IoOp::Write,
                write_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        ctx.add_background_jobs(
            1, // job_count
            Request(
                IoType::Compaction,
                IoOp::Write,
                compaction_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        ctx.add_background_jobs(
            1, // job_count
            Request(
                IoType::Import,
                IoOp::Write,
                import_work * bytes_per_sec / 100 / 1000 * 2,
                Some(Duration::from_millis(2)),
            ),
        );
        let duration = Duration::from_secs(2);
        ctx.run(duration);
        let write_bytes = stats.fetch(IoType::ForegroundWrite, IoOp::Write);
        let compaction_bytes = stats.fetch(IoType::Compaction, IoOp::Write);
        let import_bytes = stats.fetch(IoType::Import, IoOp::Write);
        let total_bytes = write_bytes + compaction_bytes + import_bytes;
        approximate_eq!(write_bytes, total_bytes * write_work / 100);
        approximate_eq!(compaction_bytes + write_bytes, total_bytes);
        // println!("{}, {}, {}, {}", write_bytes, compaction_bytes, import_bytes,
        // bytes_per_sec * 2);
        approximate_eq!(total_bytes, bytes_per_sec as f64 * duration.as_secs_f64());
    }

    #[bench]
    fn bench_critical_section(b: &mut test::Bencher) {
        let inner_limiter = IoRateLimiter::new(IoRateLimitMode::AllIo, true, None, false);
        inner_limiter.set_io_rate_limit(1024);
        let now = Instant::now_coarse();
        b.iter(|| {
            inner_limiter.critical_section(now);
        });
    }
}
