// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOMeasure, IOOp, IOPriority, IOType};

use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, RwLock};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, Sender},
    Arc,
};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tikv_util::time::Instant;

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct IORateLimiterStatistics {
    read_bytes: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
    write_bytes: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
    read_ios: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
    write_ios: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
}

impl IORateLimiterStatistics {
    pub fn new() -> Self {
        IORateLimiterStatistics {
            read_bytes: Default::default(),
            write_bytes: Default::default(),
            read_ios: Default::default(),
            write_ios: Default::default(),
        }
    }

    pub fn fetch(&self, io_type: IOType, io_op: IOOp, feature: IOMeasure) -> usize {
        let io_type_idx = io_type as usize;
        match (io_op, feature) {
            (IOOp::Read, IOMeasure::Bytes) => self.read_bytes[io_type_idx].load(Ordering::Relaxed),
            (IOOp::Write, IOMeasure::Bytes) => {
                self.write_bytes[io_type_idx].load(Ordering::Relaxed)
            }
            (IOOp::Read, IOMeasure::Iops) => self.read_ios[io_type_idx].load(Ordering::Relaxed),
            (IOOp::Write, IOMeasure::Iops) => self.write_ios[io_type_idx].load(Ordering::Relaxed),
        }
    }

    pub fn record(&self, io_type: IOType, io_op: IOOp, bytes: usize) {
        let io_type_idx = io_type as usize;
        match io_op {
            IOOp::Read => {
                self.read_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
                self.read_ios[io_type_idx].fetch_add(1, Ordering::Relaxed);
            }
            IOOp::Write => {
                self.write_bytes[io_type_idx].fetch_add(bytes, Ordering::Relaxed);
                self.write_ios[io_type_idx].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn reset(&self) {
        for i in 0..IOType::VARIANT_COUNT {
            self.read_bytes[i].store(0, Ordering::Relaxed);
            self.write_bytes[i].store(0, Ordering::Relaxed);
            self.read_ios[i].store(0, Ordering::Relaxed);
            self.write_ios[i].store(0, Ordering::Relaxed);
        }
    }
}

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(200);

#[inline]
fn calculate_ios_per_refill(ios_per_sec: usize, refill_period: Duration) -> usize {
    (ios_per_sec as f64 * refill_period.as_secs_f64()) as usize
}

/// Limit total IO flow below provided threshold by throttling low-priority IOs.
/// Rate limit is disabled when total IO threshold is set to zero.
#[derive(Debug)]
struct RawIORateLimiter {
    refill_period: Duration,
    ios_through: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT],
    ios_per_sec: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT],
    pending_ios: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT],
    protected: RwLock<RawIORateLimiterProtected>,
}

#[derive(Debug)]
struct RawIORateLimiterProtected {
    last_refill_time: Instant,
}

impl RawIORateLimiterProtected {
    pub fn new() -> Self {
        RawIORateLimiterProtected {
            last_refill_time: Instant::now_coarse(),
        }
    }
}

macro_rules! sleep_impl {
    ($duration:expr, "sync") => {
        std::thread::sleep($duration)
    };
    ($duration:expr, "async") => {
        tokio::time::delay_for($duration).await
    };
    ($duration:expr, "non-blocking") => {
        return 0
    };
}

macro_rules! request_impl {
    ($self:expr, $priority:expr, $amount:expr, $mode:tt) => {{
        let priority_idx = $priority as usize;
        loop {
            let cached_ios_per_refill = $self.ios_per_sec[priority_idx].load(Ordering::Relaxed);
            if cached_ios_per_refill == 0 {
                return $amount;
            }
            let amount = std::cmp::min($amount, cached_ios_per_refill);
            let ios_through =
                $self.ios_through[priority_idx].fetch_add(amount, Ordering::AcqRel) + amount;
            if ios_through <= cached_ios_per_refill {
                return amount;
            }
            if let Some(locked) = $self.protected.try_read() {
                let now = Instant::now_coarse();
                if locked.last_refill_time + Duration::from_millis(1) >= now {
                    continue;
                }
                let pending =
                    $self.pending_ios[priority_idx].fetch_add(amount, Ordering::Relaxed) + amount;
                let since_last_refill = now.duration_since(locked.last_refill_time);
                drop(locked);
                if $self.refill_period > since_last_refill {
                    sleep_impl!($self.refill_period - since_last_refill, $mode);
                }
                if pending <= cached_ios_per_refill {
                    return amount;
                }
            } else {
                // spin a while for the concurrent refill to complete
                std::thread::yield_now();
                while $self.protected.try_read().is_none() {
                    std::thread::yield_now();
                }
            }
        }
    }};
}

impl RawIORateLimiter {
    fn new(refill_period: Duration, _ios_per_sec: usize) -> Self {
        assert!(IOPriority::High as usize == IOPriority::VARIANT_COUNT - 1);
        let ios_per_sec: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT] = Default::default();
        let _ios_per_sec = calculate_ios_per_refill(_ios_per_sec, refill_period);
        for i in 0..IOPriority::VARIANT_COUNT {
            ios_per_sec[i].store(_ios_per_sec, Ordering::Relaxed);
        }
        RawIORateLimiter {
            refill_period,
            ios_through: Default::default(),
            ios_per_sec,
            pending_ios: Default::default(),
            protected: RwLock::new(RawIORateLimiterProtected::new()),
        }
    }

    /// Dynamically changes the total IO flow threshold, effective after at most `refill_period`.
    #[allow(dead_code)]
    fn set_ios_per_sec(&self, ios_per_sec: usize) {
        let now = calculate_ios_per_refill(ios_per_sec, self.refill_period);
        let before = self.ios_per_sec[IOPriority::High as usize].swap(now, Ordering::Relaxed);
        if before == 0 || now == 0 {
            // toggle on/off rate limit.
            // we hold this lock so a concurrent refill can't negate our effort.
            let _locked = self.protected.write();
            for i in 0..IOPriority::VARIANT_COUNT - 1 {
                self.ios_per_sec[i].store(now, Ordering::Relaxed);
            }
        }
    }

    fn request(&self, priority: IOPriority, amount: usize) -> usize {
        request_impl!(self, priority, amount, "sync")
    }

    async fn async_request(&self, priority: IOPriority, amount: usize) -> usize {
        request_impl!(self, priority, amount, "async")
    }

    fn refill(&self) {
        let mut locked = self.protected.write();

        let mut limit = self.ios_per_sec[IOPriority::High as usize].load(Ordering::Relaxed);
        if limit == 0 {
            return;
        }
        let now = Instant::now_coarse();
        let mut ios_through = self.ios_through[IOPriority::High as usize].swap(
            self.pending_ios[IOPriority::High as usize].swap(0, Ordering::Relaxed),
            Ordering::Release,
        );
        for p in &[IOPriority::Medium, IOPriority::Low] {
            let pi = *p as usize;
            limit = if limit > ios_through {
                limit - ios_through
            } else {
                1 // a small positive value
            };
            self.ios_per_sec[pi].store(limit, Ordering::Relaxed);
            ios_through = self.ios_through[pi].swap(
                self.pending_ios[pi].swap(0, Ordering::Relaxed),
                Ordering::Release,
            );
        }

        locked.last_refill_time = now;
    }
}

#[derive(Debug)]
struct Refiller {
    sender: Option<Mutex<Sender<()>>>,
    handle: Option<JoinHandle<()>>,
}

impl Refiller {
    pub fn new() -> Self {
        Refiller {
            sender: None,
            handle: None,
        }
    }

    pub fn must_start(&mut self, limiter: Arc<RawIORateLimiter>) {
        let (tx, rx) = mpsc::channel();
        let h = ThreadBuilder::new()
            .name("refiller".to_owned())
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    rx.recv_timeout(DEFAULT_REFILL_PERIOD)
                {
                    limiter.refill();
                }
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap();
        self.handle = Some(h);
        self.sender = Some(Mutex::new(tx));
    }
}

/// An instance of `IORateLimiter` should be safely shared between threads.
#[derive(Debug)]
pub struct IORateLimiter {
    priority_map: [IOPriority; IOType::VARIANT_COUNT],
    bytes: Option<Arc<RawIORateLimiter>>,
    ios: Option<RawIORateLimiter>,
    enable_statistics: CachePadded<AtomicBool>,
    stats: Arc<IORateLimiterStatistics>,
    refiller: Refiller,
}

impl IORateLimiter {
    pub fn new() -> IORateLimiter {
        let limiter = Arc::new(RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, 0));
        let mut refiller = Refiller::new();
        refiller.must_start(limiter.clone());
        IORateLimiter {
            priority_map: [IOPriority::High; IOType::VARIANT_COUNT],
            bytes: Some(limiter),
            ios: None,
            enable_statistics: CachePadded::new(AtomicBool::new(true)),
            stats: Arc::new(IORateLimiterStatistics::new()),
            refiller,
        }
    }

    pub fn enable_statistics(&self, enable: bool) {
        self.enable_statistics.store(enable, Ordering::Relaxed);
    }

    pub fn set_io_priority(&mut self, io_type: IOType, io_priority: IOPriority) {
        self.priority_map[io_type as usize] = io_priority;
    }

    pub fn statistics(&self) -> Arc<IORateLimiterStatistics> {
        self.stats.clone()
    }

    #[allow(dead_code)]
    pub fn set_io_rate_limit(&self, measure: IOMeasure, rate: usize) {
        match measure {
            IOMeasure::Bytes => {
                if let Some(bytes) = &self.bytes {
                    bytes.set_ios_per_sec(rate);
                }
            }
            IOMeasure::Iops => {
                if let Some(ios) = &self.ios {
                    ios.set_ios_per_sec(rate);
                }
            }
        }
    }

    /// Requests for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, mut bytes: usize) -> usize {
        if io_op == IOOp::Write || io_type == IOType::Export {
            let priority = self.priority_map[io_type as usize];
            if let Some(ios) = &self.ios {
                ios.request(priority, 1);
            }
            bytes = if let Some(b) = &self.bytes {
                b.request(priority, bytes)
            } else {
                bytes
            };
        }
        if self.enable_statistics.load(Ordering::Relaxed) {
            self.stats.record(io_type, io_op, bytes);
        }
        bytes
    }

    /// Asynchronously requests for token for bytes and potentially update
    /// statistics. If this request can not be satisfied, the call is blocked.
    /// Granted token can be less than the requested bytes, but must be greater
    /// than zero.
    pub async fn async_request(&self, io_type: IOType, io_op: IOOp, mut bytes: usize) -> usize {
        if io_op == IOOp::Write || io_type == IOType::Export {
            let priority = self.priority_map[io_type as usize];
            if let Some(ios) = &self.ios {
                ios.async_request(priority, 1).await;
            }
            bytes = if let Some(b) = &self.bytes {
                b.async_request(priority, bytes).await
            } else {
                bytes
            };
        }
        if self.enable_statistics.load(Ordering::Relaxed) {
            self.stats.record(io_type, io_op, bytes);
        }
        bytes
    }
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IORateLimiter>>> = Mutex::new(None);
}

// Do NOT use this method in multi-threaded test environment.
pub fn set_io_rate_limiter(limiter: Option<Arc<IORateLimiter>>) {
    *IO_RATE_LIMITER.lock() = limiter;
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    if let Some(ref limiter) = *IO_RATE_LIMITER.lock() {
        Some(limiter.clone())
    } else {
        None
    }
}

// Set a global rate limit that can be used to trace IOs.
pub struct WithIORateLimit {
    previous_io_rate_limiter: Option<Arc<IORateLimiter>>,
}

impl WithIORateLimit {
    pub fn new() -> (Self, Arc<IORateLimiterStatistics>) {
        let previous_io_rate_limiter = get_io_rate_limiter();
        let limiter = Arc::new(IORateLimiter::new());
        limiter.enable_statistics(true);
        let stats = limiter.statistics();
        set_io_rate_limiter(Some(limiter));
        (
            WithIORateLimit {
                previous_io_rate_limiter,
            },
            stats,
        )
    }
}

impl Drop for WithIORateLimit {
    fn drop(&mut self) {
        set_io_rate_limiter(self.previous_io_rate_limiter.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    fn approximate_eq(left: f64, right: f64) {
        assert!(left >= right * 0.9);
        assert!(left <= right * 1.1);
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

    #[derive(Clone, Copy)]
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
                    limiter.request(io_type, op, len);
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

    fn verify_rate_limit(limiter: &Arc<IORateLimiter>, bytes_per_sec: usize) {
        let stats = limiter.statistics();
        stats.reset();
        limiter.set_io_rate_limit(IOMeasure::Bytes, bytes_per_sec);
        let duration = {
            let begin = Instant::now();
            {
                let _context = start_background_jobs(
                    limiter,
                    10, /*job_count*/
                    Request(IOType::ForegroundWrite, IOOp::Write, 10),
                    None, /*interval*/
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        approximate_eq(
            stats.fetch(IOType::ForegroundWrite, IOOp::Write, IOMeasure::Bytes) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64(),
        );
    }

    #[test]
    fn test_rate_limited_heavy_flow() {
        let low_bytes_per_sec = 2000;
        let high_bytes_per_sec = 10000;
        let limiter = Arc::new(IORateLimiter::new());
        limiter.enable_statistics(true);
        verify_rate_limit(&limiter, low_bytes_per_sec);
        verify_rate_limit(&limiter, high_bytes_per_sec);
        verify_rate_limit(&limiter, low_bytes_per_sec);
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let limiter = Arc::new(IORateLimiter::new());
        limiter.enable_statistics(true);
        limiter.set_io_rate_limit(IOMeasure::Bytes, kbytes_per_sec * 1000);
        let stats = limiter.statistics();
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
        approximate_eq(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes) as f64,
            actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0,
        );
    }

    #[test]
    fn test_rate_limited_hybrid_flow() {
        let bytes_per_sec = 100000;
        let write_work = 50;
        let compaction_work = 60;
        let import_work = 10;
        let mut limiter = IORateLimiter::new();
        limiter.enable_statistics(true);
        limiter.set_io_rate_limit(IOMeasure::Bytes, bytes_per_sec);
        limiter.set_io_priority(IOType::Compaction, IOPriority::Medium);
        limiter.set_io_priority(IOType::Import, IOPriority::Low);
        let stats = limiter.statistics();
        let limiter = Arc::new(limiter);
        let duration = {
            let begin = Instant::now();
            {
                let _write = start_background_jobs(
                    &limiter,
                    2, /*job_count*/
                    Request(
                        IOType::ForegroundWrite,
                        IOOp::Write,
                        write_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _compaction = start_background_jobs(
                    &limiter,
                    2, /*job_count*/
                    Request(
                        IOType::Compaction,
                        IOOp::Write,
                        compaction_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _import = start_background_jobs(
                    &limiter,
                    2, /*job_count*/
                    Request(
                        IOType::Import,
                        IOOp::Write,
                        import_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        let write_bytes = stats.fetch(IOType::ForegroundWrite, IOOp::Write, IOMeasure::Bytes);
        approximate_eq(
            write_bytes as f64,
            (write_work * bytes_per_sec / 100) as f64 * duration.as_secs_f64(),
        );
        let compaction_bytes = stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes);
        approximate_eq(
            compaction_bytes as f64,
            ((100 - write_work) * bytes_per_sec / 100) as f64 * duration.as_secs_f64(),
        );
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write, IOMeasure::Bytes);
        let total_bytes = write_bytes + import_bytes + compaction_bytes;
        approximate_eq(
            total_bytes as f64,
            bytes_per_sec as f64 * duration.as_secs_f64(),
        );
    }
}
