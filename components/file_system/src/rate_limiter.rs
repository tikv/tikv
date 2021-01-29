// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{condvar::Condvar, IOMeasure, IOOp, IOPriority, IOType};

use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tikv_util::time::Instant;

/// Used to calibrate actual IO throughput.
/// A naive implementation would be deducing disk IO based on empirical ratio
/// with respect to hardware environment.
pub trait BytesCalibrator: Send {
    /// Calibrate estimation of throughput of current epoch. This methods can
    /// be called several times before a reset.
    /// Returned value must be no larger than `before_calibration`.
    fn calibrate(&mut self, before_calibration: usize) -> usize;

    fn reset(&mut self);
}

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

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(10);

#[inline]
fn calculate_ios_per_refill(ios_per_sec: usize, refill_period: Duration) -> usize {
    (ios_per_sec as f64 * refill_period.as_secs_f64()) as usize
}

struct RawIORateLimiterProtected {
    last_refill_time: Instant,
    calibrator: Option<Box<dyn BytesCalibrator>>,
}

/// Limit total IO flow below provided threshold by throttling low-priority IOs.
/// Rate limit is disabled when total IO threshold is set to zero.
struct RawIORateLimiter {
    refill_period: Duration,
    priority_ios_through: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT],
    priority_ios_per_sec: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT],
    condv: Condvar,
    protected: Mutex<RawIORateLimiterProtected>,
}

macro_rules! condv_wait_impl {
    ($condv:expr, $lock:expr, $guard:expr, $timeout:expr, true) => {
        $condv.wait_timeout($guard, $timeout)
    };
    ($condv:expr, $lock:expr, $guard:expr, $timeout:expr, false) => {
        $condv.async_wait_timeout(&$lock, $guard, $timeout).await
    };
}

macro_rules! request_impl {
    ($self:expr, $priority:expr, $amount:expr, $sync:tt) => {{
        let priority_idx = $priority as usize;
        loop {
            let cached_ios_per_refill =
                $self.priority_ios_per_sec[priority_idx].load(Ordering::Relaxed);
            if cached_ios_per_refill == 0 {
                return $amount;
            }
            let amount = std::cmp::min($amount, cached_ios_per_refill);
            let tmp = $self.priority_ios_through[priority_idx].fetch_add(amount, Ordering::AcqRel);
            if amount + tmp <= cached_ios_per_refill {
                return amount;
            }
            let mut locked = $self.protected.lock();
            // TODO: calibration
            let now = Instant::now_coarse();
            assert!(now >= locked.last_refill_time);
            let since_last_refill = now.duration_since(locked.last_refill_time);
            if since_last_refill < $self.refill_period {
                let cached_last_refill_time = locked.last_refill_time;
                // use a slightly larger timeout so that they can react to notification
                // and preserve enqueue order
                let (mut locked, timed_out) = condv_wait_impl!(
                    $self.condv,
                    &$self.protected,
                    locked,
                    $self.refill_period.mul_f32(1.1) - since_last_refill,
                    $sync
                );
                if timed_out && locked.last_refill_time == cached_last_refill_time {
                    $self.refill(&mut locked, Instant::now_coarse());
                }
            } else {
                $self.refill(&mut locked, now);
            }
        }
    }};
}

impl RawIORateLimiter {
    pub fn new(refill_period: Duration, ios_per_sec: usize) -> Self {
        assert!(IOPriority::High as usize == IOPriority::VARIANT_COUNT - 1);
        let priority_ios_per_sec: [CachePadded<AtomicUsize>; IOPriority::VARIANT_COUNT] =
            Default::default();
        let ios_per_sec = calculate_ios_per_refill(ios_per_sec, refill_period);
        for i in 0..IOPriority::VARIANT_COUNT {
            priority_ios_per_sec[i].store(ios_per_sec, Ordering::Relaxed);
        }
        RawIORateLimiter {
            refill_period,
            priority_ios_through: Default::default(),
            priority_ios_per_sec,
            condv: Condvar::new(),
            protected: Mutex::new(RawIORateLimiterProtected {
                last_refill_time: Instant::now_coarse(),
                calibrator: None,
            }),
        }
    }

    #[allow(dead_code)]
    pub fn set_calibrator(&mut self, calibrator: Box<dyn BytesCalibrator>) {
        self.protected.get_mut().calibrator = Some(calibrator);
    }

    /// Dynamically changes the total IO flow threshold, effective after at most `refill_period`.
    #[allow(dead_code)]
    pub fn set_ios_per_sec(&self, ios_per_sec: usize) {
        // we hold this lock so a concurrent refill can't negate our attempt.
        let _locked = self.protected.lock();
        let before = self.priority_ios_per_sec[IOPriority::High as usize].load(Ordering::Relaxed);
        let now = calculate_ios_per_refill(ios_per_sec, self.refill_period);
        self.priority_ios_per_sec[IOPriority::High as usize].store(now, Ordering::Relaxed);
        if before == 0 || now == 0 {
            // corner case for disabling/enabling rate limit.
            for i in 0..IOPriority::VARIANT_COUNT - 1 {
                self.priority_ios_per_sec[i].store(now, Ordering::Relaxed);
            }
        }
    }

    pub fn request(&self, priority: IOPriority, amount: usize) -> usize {
        request_impl!(self, priority, amount, true /*sync*/)
    }

    pub async fn async_request(&self, priority: IOPriority, amount: usize) -> usize {
        request_impl!(self, priority, amount, false /*sync*/)
    }

    #[inline]
    fn refill(&self, locked: &mut MutexGuard<RawIORateLimiterProtected>, now: Instant) {
        let mut cached_priority_ios_through = [0; IOPriority::VARIANT_COUNT];
        for i in 0..IOPriority::VARIANT_COUNT {
            cached_priority_ios_through[i] = std::cmp::min(
                self.priority_ios_through[i].load(Ordering::Relaxed),
                self.priority_ios_per_sec[i].load(Ordering::Relaxed),
            );
        }
        // start from high priority
        let mut limit =
            self.priority_ios_per_sec[IOPriority::High as usize].load(Ordering::Relaxed);
        self.priority_ios_through[IOPriority::High as usize].store(0, Ordering::Release);
        let mut higher_priority_ios_through =
            cached_priority_ios_through[IOPriority::High as usize];
        for i in (0..IOPriority::VARIANT_COUNT - 1).rev() {
            limit = if limit > higher_priority_ios_through {
                limit - higher_priority_ios_through
            } else if limit == 0 {
                0 // 0 means disabled
            } else {
                10 // a small positive value
            };
            self.priority_ios_per_sec[i].store(limit, Ordering::Relaxed);
            higher_priority_ios_through += cached_priority_ios_through[i];
            // finally reset the consumption
            self.priority_ios_through[i].store(0, Ordering::Release);
        }

        if let Some(calibrator) = &mut locked.calibrator {
            calibrator.reset();
        }
        locked.last_refill_time = now;
        self.condv.notify_all(locked);
    }

    #[cfg(test)]
    pub fn is_drained(&self, priority: IOPriority) -> bool {
        self.priority_ios_through[priority as usize].load(Ordering::Acquire)
            >= self.priority_ios_per_sec[priority as usize].load(Ordering::Acquire)
    }
}

/// An instance of `IORateLimiter` should be safely shared between threads.
pub struct IORateLimiter {
    priority_map: [IOPriority; IOType::VARIANT_COUNT],
    bytes: Option<RawIORateLimiter>,
    ios: Option<RawIORateLimiter>,
    enable_statistics: CachePadded<AtomicBool>,
    stats: Arc<IORateLimiterStatistics>,
}

impl IORateLimiter {
    pub fn new() -> IORateLimiter {
        IORateLimiter {
            priority_map: [IOPriority::High; IOType::VARIANT_COUNT],
            bytes: Some(RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, 0)),
            ios: None,
            enable_statistics: CachePadded::new(AtomicBool::new(true)),
            stats: Arc::new(IORateLimiterStatistics::new()),
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
        if io_op == IOOp::Write || io_type == IOType::Other {
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
        if io_op == IOOp::Write || io_type == IOType::Other {
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

pub struct WithIORateLimiter {
    previous_io_rate_limiter: Option<Arc<IORateLimiter>>,
}

impl WithIORateLimiter {
    pub fn new(limiter: Option<Arc<IORateLimiter>>) -> Self {
        let previous_io_rate_limiter = get_io_rate_limiter();
        set_io_rate_limiter(limiter);
        WithIORateLimiter {
            previous_io_rate_limiter,
        }
    }
}

impl Drop for WithIORateLimiter {
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

    fn approximate_eq_2(left: f64, right: f64, margin: f64) {
        assert!(left >= right * (1.0 - margin));
        assert!(left <= right * (1.0 + margin));
    }

    #[test]
    fn test_basic_rate_limit() {
        let refills_in_one_sec = 10;
        let refill_period = Duration::from_millis(1000 / refills_in_one_sec as u64);
        let limiter = RawIORateLimiter::new(refill_period, 2 * refills_in_one_sec);

        assert_eq!(limiter.request(IOPriority::High, 1), 1);
        assert_eq!(limiter.request(IOPriority::High, 10), 2);
        limiter.set_ios_per_sec(10 * refills_in_one_sec);
        assert_eq!(limiter.request(IOPriority::High, 10), 10);
        limiter.set_ios_per_sec(100 * refills_in_one_sec);

        let limiter = Arc::new(limiter);
        let mut threads = vec![];
        assert_eq!(limiter.request(IOPriority::High, 1000), 100);
        let begin = Instant::now();
        for _ in 0..50 {
            let limiter = limiter.clone();
            let t = std::thread::spawn(move || {
                assert_eq!(limiter.request(IOPriority::High, 1), 1);
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        let end = Instant::now();
        assert!(end.duration_since(begin) >= refill_period);
        assert!(end.duration_since(begin) < refill_period * 2);
    }

    #[test]
    fn test_dynamic_relax_limit() {
        let refills_in_one_sec = 10;
        let refill_period = Duration::from_millis(1000 / refills_in_one_sec as u64);
        let limiter = Arc::new(RawIORateLimiter::new(refill_period, 2 * refills_in_one_sec));

        let (limiter1, limiter2) = (limiter.clone(), limiter.clone());

        fn inner(limiter: Arc<RawIORateLimiter>) {
            assert!(limiter.is_drained(IOPriority::High));
            assert_eq!(limiter.request(IOPriority::High, 100), 100);
        }
        assert_eq!(limiter.request(IOPriority::High, 100), 2);
        // only one thread do the refill after timeout
        let t1 = std::thread::spawn(move || {
            inner(limiter1);
        });
        let t2 = std::thread::spawn(move || {
            inner(limiter2);
        });
        let begin = Instant::now();
        // make sure limiters start waiting
        std::thread::sleep(Duration::from_millis(10));
        limiter.set_ios_per_sec(10000 * refills_in_one_sec);
        t1.join().unwrap();
        t2.join().unwrap();
        let end = Instant::now();
        approximate_eq(
            end.duration_since(begin).as_secs_f64(),
            refill_period.as_secs_f64() * 1.1,
        );
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
        limiter: Arc<IORateLimiter>,
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

    #[test]
    fn test_rate_limited_heavy_flow() {
        let bytes_per_sec = 10000;
        let limiter = Arc::new(IORateLimiter::new());
        limiter.enable_statistics(true);
        limiter.set_io_rate_limit(IOMeasure::Bytes, bytes_per_sec);
        let stats = limiter.statistics();
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
                    limiter,
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
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::ForegroundWrite,
                        IOOp::Write,
                        write_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _compaction = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::Compaction,
                        IOOp::Write,
                        compaction_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _import = start_background_jobs(
                    limiter,
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
        approximate_eq_2(
            compaction_bytes as f64,
            ((100 - write_work) * bytes_per_sec / 100) as f64 * duration.as_secs_f64(),
            0.2,
        );
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write, IOMeasure::Bytes);
        let total_bytes = write_bytes + import_bytes + compaction_bytes;
        approximate_eq(
            total_bytes as f64,
            bytes_per_sec as f64 * duration.as_secs_f64(),
        );
    }
}
