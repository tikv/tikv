// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{condvar::Condvar, IOMeasure, IOOp, IOType};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tikv_util::time::Instant;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum IOPriority {
    Low,
    High,
}

fn get_priority(io_type: IOType) -> IOPriority {
    match io_type {
        IOType::Compaction => IOPriority::Low,
        _ => IOPriority::High,
    }
}

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

/// Record statistics of different IO stream.
#[derive(Debug)]
struct RawIOVec {
    inner: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
}

impl RawIOVec {
    fn new() -> Self {
        let r = RawIOVec {
            inner: Default::default(),
        };
        for i in 0..IOType::VARIANT_COUNT {
            r.inner[i].store(0, Ordering::Relaxed);
        }
        r
    }

    fn store(&self, i: usize, amount: usize) {
        self.inner[i].store(amount, Ordering::Relaxed);
    }

    fn fetch_add(&self, i: usize, amount: usize) -> usize {
        self.inner[i].fetch_add(amount, Ordering::Relaxed)
    }

    fn fetch_sub(&self, i: usize, amount: usize) -> usize {
        self.inner[i].fetch_sub(amount, Ordering::Relaxed)
    }

    fn load(&self, i: usize) -> usize {
        self.inner[i].load(Ordering::Relaxed)
    }

    fn reset(&self) {
        for i in 0..IOType::VARIANT_COUNT {
            self.inner[i].store(0, Ordering::Relaxed);
        }
    }
}

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(10);

#[inline]
fn calculate_bytes_per_refill(bytes_per_sec: usize, refill_period: Duration) -> usize {
    (bytes_per_sec as f64 * refill_period.as_secs_f64()) as usize
}

struct RawIORateLimiterProtected {
    last_refill_time: Instant,
    usage_estimation: [usize; IOType::VARIANT_COUNT],
    calibrator: Option<Box<dyn BytesCalibrator>>,
}

// TODO: add builder
struct RawIORateLimiter {
    // configurations
    refill_period: Duration,
    bytes_per_refill: AtomicUsize,
    percentage: [usize; IOType::VARIANT_COUNT],
    // states
    private_bytes_per_refill: RawIOVec,
    consumed: RawIOVec,
    stats: Arc<RawIOVec>,
    condv: Condvar,
    protected: Mutex<RawIORateLimiterProtected>,
}

impl RawIORateLimiter {
    pub fn new(refill_period: Duration, bytes_per_sec: usize) -> Self {
        assert!(IOType::Other as usize == 0);
        RawIORateLimiter {
            refill_period,
            bytes_per_refill: AtomicUsize::new(calculate_bytes_per_refill(
                bytes_per_sec,
                refill_period,
            )),
            percentage: [0; IOType::VARIANT_COUNT],
            private_bytes_per_refill: RawIOVec::new(),
            consumed: RawIOVec::new(),
            stats: Arc::new(RawIOVec::new()),
            condv: Condvar::new(),
            protected: Mutex::new(RawIORateLimiterProtected {
                last_refill_time: Instant::now_coarse(),
                usage_estimation: [0; IOType::VARIANT_COUNT],
                calibrator: None,
            }),
        }
    }

    pub fn set_percentage(&mut self, io_type: IOType, percentage: usize) {
        self.percentage[io_type as usize] = percentage;
    }

    #[allow(dead_code)]
    pub fn set_calibrator(&mut self, calibrator: Box<dyn BytesCalibrator>) {
        self.protected.get_mut().calibrator = Some(calibrator);
    }

    pub fn finalize(&mut self) -> Arc<RawIOVec> {
        let bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        for i in 0..IOType::VARIANT_COUNT {
            self.protected.get_mut().usage_estimation[i] = self.percentage[i];
            self.private_bytes_per_refill
                .store(i, bytes_per_refill * self.percentage[i] / 100);
        }
        self.stats.clone()
    }

    pub fn set_bytes_per_sec(&self, bytes_per_sec: usize) {
        self.bytes_per_refill.store(
            calculate_bytes_per_refill(bytes_per_sec, self.refill_period),
            Ordering::Relaxed,
        );
    }

    pub fn request(&self, io_type: IOType, amount: usize) -> usize {
        let io_type_idx = io_type as usize;
        let percentage = self.percentage[io_type_idx];
        if percentage == 0 {
            // TODO
            return amount;
        }
        loop {
            let cached_bytes_per_refill = self.private_bytes_per_refill.load(io_type_idx);
            if cached_bytes_per_refill == 0 {
                return amount;
            }
            let amount = std::cmp::min(cached_bytes_per_refill, amount);
            let cached_consumed = self.consumed.fetch_add(io_type_idx, amount);
            if cached_consumed + amount > cached_bytes_per_refill {
                let mut locked = self.protected.lock();
                // TODO: calibration
                let now = Instant::now_coarse();
                assert!(now >= locked.last_refill_time);
                let since_last_refill = now.duration_since(locked.last_refill_time);
                if since_last_refill < self.refill_period {
                    let cached_last_refill_time = locked.last_refill_time;
                    // use a slightly larger timeout so that they can react to notification
                    // and preserve priority information
                    let (mut locked, timed_out) = self
                        .condv
                        .wait_timeout(locked, self.refill_period.mul_f32(1.1) - since_last_refill);
                    if timed_out && locked.last_refill_time == cached_last_refill_time {
                        self.refill(&mut locked, Instant::now_coarse());
                    }
                } else {
                    self.refill(&mut locked, now);
                }
            } else {
                return amount;
            }
        }
    }

    pub async fn async_request(&self, io_type: IOType, amount: usize) -> usize {
        let io_type_idx = io_type as usize;
        let percentage = self.percentage[io_type_idx];
        if percentage == 0 {
            // TODO
            return amount;
        }
        loop {
            let cached_bytes_per_refill = self.private_bytes_per_refill.load(io_type_idx);
            if cached_bytes_per_refill == 0 {
                return amount;
            }
            let amount = std::cmp::min(cached_bytes_per_refill, amount);
            let cached_consumed = self.consumed.fetch_add(io_type_idx, amount);
            if cached_consumed + amount > cached_bytes_per_refill {
                let mut locked = self.protected.lock();
                // TODO: calibration
                let now = Instant::now_coarse();
                assert!(now >= locked.last_refill_time);
                let since_last_refill = now.duration_since(locked.last_refill_time);
                if since_last_refill < self.refill_period {
                    let cached_last_refill_time = locked.last_refill_time;
                    // use a slightly larger timeout so that they can react to notification
                    // and preserve priority information
                    let (mut locked, timed_out) = self
                        .condv
                        .async_wait_timeout(
                            &self.protected,
                            locked,
                            self.refill_period.mul_f32(1.1) - since_last_refill,
                        )
                        .await;
                    if timed_out && locked.last_refill_time == cached_last_refill_time {
                        self.refill(&mut locked, Instant::now_coarse());
                    }
                } else {
                    self.refill(&mut locked, now);
                }
            } else {
                return amount;
            }
        }
    }

    #[inline]
    fn refill(&self, locked: &mut RawIORateLimiterProtected, now: Instant) {
        // 1. update estimation
        let cached_bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        let mut sum_usage = 0;
        for i in 0..IOType::VARIANT_COUNT {
            if self.percentage[i] == 0 {
                continue;
            }
            let private = self.private_bytes_per_refill.load(i);
            let consumed = std::cmp::min(self.consumed.inner[i].load(Ordering::Relaxed), private);
            // exponential average
            locked.usage_estimation[i] =
                (locked.usage_estimation[i] + consumed * 100 / private) / 2;
            sum_usage += locked.usage_estimation[i];
            self.stats.fetch_add(i, consumed);
            self.consumed.inner[i].store(0, Ordering::Relaxed);
        }
        // 2. assign shares for next epoch based on normalized estimation
        for i in 0..IOType::VARIANT_COUNT {
            if self.percentage[i] > 0 {
                self.private_bytes_per_refill.store(
                    i,
                    locked.usage_estimation[i] * cached_bytes_per_refill / sum_usage,
                );
            }
        }
        // 2. state reset
        if let Some(calibrator) = &mut locked.calibrator {
            calibrator.reset();
        }
        locked.last_refill_time = now;
    }

    #[cfg(test)]
    pub fn is_drained(&self, io_type: IOType) -> bool {
        self.consumed.load(io_type as usize) >= self.private_bytes_per_refill.load(io_type as usize)
    }
}

pub struct IOStats {
    read_bytes: Option<Arc<RawIOVec>>,
    write_bytes: Option<Arc<RawIOVec>>,
    read_ios: Option<Arc<RawIOVec>>,
    write_ios: Option<Arc<RawIOVec>>,
}

impl IOStats {
    fn new() -> Self {
        IOStats {
            read_bytes: None,
            write_bytes: None,
            read_ios: None,
            write_ios: None,
        }
    }

    fn set_stats(&mut self, stats: Arc<RawIOVec>, io_op: IOOp, feature: IOMeasure) {
        match (io_op, feature) {
            (IOOp::Read, IOMeasure::Bytes) => self.read_bytes = Some(stats),
            (IOOp::Write, IOMeasure::Bytes) => self.write_bytes = Some(stats),
            (IOOp::Read, IOMeasure::Iops) => self.read_ios = Some(stats),
            (IOOp::Write, IOMeasure::Iops) => self.write_ios = Some(stats),
        }
    }

    pub fn fetch(&self, io_type: IOType, io_op: IOOp, feature: IOMeasure) -> usize {
        match (io_op, feature) {
            (IOOp::Read, IOMeasure::Bytes) => {
                if let Some(bytes) = &self.read_bytes {
                    bytes.load(io_type as usize)
                } else {
                    0
                }
            }
            (IOOp::Write, IOMeasure::Bytes) => {
                if let Some(bytes) = &self.write_bytes {
                    bytes.load(io_type as usize)
                } else {
                    0
                }
            }
            (IOOp::Read, IOMeasure::Iops) => {
                if let Some(ios) = &self.read_ios {
                    ios.load(io_type as usize)
                } else {
                    0
                }
            }
            (IOOp::Write, IOMeasure::Iops) => {
                if let Some(ios) = &self.write_ios {
                    ios.load(io_type as usize)
                } else {
                    0
                }
            }
        }
    }
}

/// An instance of `IORateLimiter` should be safely shared between threads.
pub struct IORateLimiter {
    bytes: Option<RawIORateLimiter>,
    write_bytes: Option<RawIORateLimiter>,
    read_bytes: Option<RawIORateLimiter>,
    ios: Option<RawIORateLimiter>,
    write_ios: Option<RawIORateLimiter>,
    read_ios: Option<RawIORateLimiter>,
    stats: Arc<IOStats>,
}

impl IORateLimiter {
    pub fn new() -> IORateLimiter {
        IORateLimiter {
            bytes: None,
            write_bytes: None,
            read_bytes: None,
            ios: None,
            write_ios: None,
            read_ios: None,
            stats: Arc::new(IOStats::new()),
        }
    }

    #[allow(dead_code)]
    pub fn set_bytes_per_sec(&self, io_op: Option<IOOp>, bytes_per_sec: usize) {
        match io_op {
            Some(IOOp::Write) => {
                if let Some(bytes) = &self.write_bytes {
                    bytes.set_bytes_per_sec(bytes_per_sec);
                }
            }
            Some(IOOp::Read) => {
                if let Some(bytes) = &self.read_bytes {
                    bytes.set_bytes_per_sec(bytes_per_sec);
                }
            }
            None => {
                if let Some(bytes) = &self.bytes {
                    bytes.set_bytes_per_sec(bytes_per_sec);
                }
            }
        }
    }

    /// Requests for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        if let Some(ios) = &self.ios {
            ios.request(io_type, 1);
        }
        let bytes = if let Some(b) = &self.bytes {
            b.request(io_type, bytes)
        } else {
            bytes
        };
        match io_op {
            IOOp::Read => {
                if let Some(ios) = &self.read_ios {
                    ios.request(io_type, 1);
                }
                if let Some(b) = &self.read_bytes {
                    b.request(io_type, bytes)
                } else {
                    bytes
                }
            }
            IOOp::Write => {
                if let Some(ios) = &self.write_ios {
                    ios.request(io_type, 1);
                }
                if let Some(b) = &self.write_bytes {
                    b.request(io_type, bytes)
                } else {
                    bytes
                }
            }
        }
    }

    /// Asynchronously requests for token for bytes and potentially update
    /// statistics. If this request can not be satisfied, the call is blocked.
    /// Granted token can be less than the requested bytes, but must be greater
    /// than zero.
    pub async fn async_request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        if let Some(ios) = &self.ios {
            ios.async_request(io_type, 1).await;
        }
        let bytes = if let Some(b) = &self.bytes {
            b.async_request(io_type, bytes).await
        } else {
            bytes
        };
        match io_op {
            IOOp::Read => {
                if let Some(ios) = &self.read_ios {
                    ios.async_request(io_type, 1).await;
                }
                if let Some(b) = &self.read_bytes {
                    b.async_request(io_type, bytes).await
                } else {
                    bytes
                }
            }
            IOOp::Write => {
                if let Some(ios) = &self.write_ios {
                    ios.async_request(io_type, 1).await;
                }
                if let Some(b) = &self.write_bytes {
                    b.async_request(io_type, bytes).await
                } else {
                    bytes
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn test_rate_limit() {
        let refills_in_one_sec = 10;
        let refill_period = Duration::from_millis(1000 / refills_in_one_sec as u64);
        let mut limiter = RawIORateLimiter::new(refill_period, 2 * refills_in_one_sec);
        limiter.set_percentage(IOType::Compaction, 100);
        limiter.finalize();

        assert_eq!(limiter.request(IOType::Compaction, 1), 1);
        assert_eq!(limiter.request(IOType::Compaction, 10), 2);
        limiter.set_bytes_per_sec(10 * refills_in_one_sec);
        assert_eq!(limiter.request(IOType::Compaction, 10), 10);
        limiter.set_bytes_per_sec(100 * refills_in_one_sec);

        let limiter = Arc::new(limiter);
        let mut threads = vec![];
        assert_eq!(limiter.request(IOType::Compaction, 1000), 100);
        let begin = Instant::now();
        for _ in 0..50 {
            let limiter = limiter.clone();
            let t = std::thread::spawn(move || {
                assert_eq!(limiter.request(IOType::Compaction, 1), 1);
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
        let mut limiter = RawIORateLimiter::new(refill_period, 2 * refills_in_one_sec);
        limiter.set_percentage(IOType::Compaction, 100);
        limiter.finalize();
        let limiter = Arc::new(limiter);

        let (limiter1, limiter2) = (limiter.clone(), limiter.clone());

        fn inner(limiter: Arc<RawIORateLimiter>) {
            assert!(limiter.is_drained(IOType::Compaction));
            assert_eq!(limiter.request(IOType::Compaction, 100), 100);
        }
        assert_eq!(limiter.request(IOType::Compaction, 100), 2);
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
        limiter.set_bytes_per_sec(10000 * refills_in_one_sec);
        t1.join().unwrap();
        t2.join().unwrap();
        let end = Instant::now();
        assert!(end.duration_since(begin).as_secs_f64() > refill_period.as_secs_f64() * 0.9);
        assert!(end.duration_since(begin).as_secs_f64() < refill_period.as_secs_f64() * 1.1 * 1.1);
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
    struct Request(IOType, usize);

    fn start_background_jobs(
        limiter: Arc<RawIORateLimiter>,
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
                let Request(io_type, len) = request;
                while !stop.load(Ordering::Relaxed) {
                    limiter.request(io_type, len);
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
        let mut limiter = RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, bytes_per_sec);
        limiter.set_percentage(IOType::Compaction, 100);
        let raw_stats = limiter.finalize();
        let limiter = Arc::new(limiter);
        let stats = IOStats {
            read_bytes: None,
            write_bytes: Some(raw_stats),
            read_ios: None,
            write_ios: None,
        };
        let duration = {
            let begin = Instant::now();
            {
                let _context = start_background_jobs(
                    limiter,
                    10, /*job_count*/
                    Request(IOType::Compaction, 10),
                    None, /*interval*/
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes)
                <= (bytes_per_sec as f64 * duration.as_secs_f64() * 1.1) as usize
        );
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes)
                >= (bytes_per_sec as f64 * duration.as_secs_f64() * 0.9) as usize
        );
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let mut limiter = RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, kbytes_per_sec * 1000);
        limiter.set_percentage(IOType::Compaction, 100);
        let raw_stats = limiter.finalize();
        let limiter = Arc::new(limiter);
        let stats = IOStats {
            read_bytes: None,
            write_bytes: Some(raw_stats),
            read_ios: None,
            write_ios: None,
        };
        let duration = {
            let begin = Instant::now();
            {
                // each thread request at most 1000 bytes per second
                let _context = start_background_jobs(
                    limiter,
                    actual_kbytes_per_sec, /*job_count*/
                    Request(IOType::Compaction, 1),
                    Some(Duration::from_millis(1)),
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes)
                <= (kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0) as usize
        );
        assert!(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes)
                >= (actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0 * 0.9) as usize
        );
    }

    #[test]
    fn test_rate_limited_hybrid_flow() {
        let bytes_per_sec = 100000;
        let (write_percent, write_pressure) = (30, 30);
        let (import_percent, import_pressure) = (30, 10);
        let (flush_percent, flush_perssure) = (20, 50);
        let (compaction_percent, compaction_pressure) = (20, 50);
        let mut limiter = RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, bytes_per_sec);
        limiter.set_percentage(IOType::ForegroundWrite, write_percent);
        limiter.set_percentage(IOType::Import, import_percent);
        limiter.set_percentage(IOType::Flush, flush_percent);
        limiter.set_percentage(IOType::Compaction, compaction_percent);
        let raw_stats = limiter.finalize();
        let limiter = Arc::new(limiter);
        let stats = IOStats {
            read_bytes: None,
            write_bytes: Some(raw_stats),
            read_ios: None,
            write_ios: None,
        };
        let duration = {
            let begin = Instant::now();
            {
                let _write = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::ForegroundWrite,
                        write_pressure * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _import = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::Import,
                        import_pressure * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _flush = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::Flush,
                        flush_perssure * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _compaction = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(
                        IOType::Compaction,
                        compaction_pressure * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        let write_bytes = stats.fetch(IOType::ForegroundWrite, IOOp::Write, IOMeasure::Bytes);
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write, IOMeasure::Bytes);
        let flush_bytes = stats.fetch(IOType::Flush, IOOp::Write, IOMeasure::Bytes);
        let compaction_bytes = stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes);
        let total_bytes = write_bytes + import_bytes + flush_bytes + compaction_bytes;
        assert!(
            write_bytes
                > ((write_pressure * bytes_per_sec / 100) as f64 * duration.as_secs_f64() * 0.9)
                    as usize
        );
        assert!(
            import_bytes
                > ((import_pressure * bytes_per_sec / 100) as f64 * duration.as_secs_f64() * 0.9)
                    as usize
        );
        assert!(total_bytes > (bytes_per_sec as f64 * duration.as_secs_f64() * 0.9) as usize);
        assert!(total_bytes < (bytes_per_sec as f64 * duration.as_secs_f64() * 1.1) as usize);
    }
}
