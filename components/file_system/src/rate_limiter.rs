// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{condvar::Condvar, IOMeasure, IOOp, IOType};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    fn load(&self, i: usize) -> usize {
        self.inner[i].load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    fn reset(&self) {
        for i in 0..IOType::VARIANT_COUNT {
            self.inner[i].store(0, Ordering::Relaxed);
        }
    }
}

const DEFAULT_REFILL_PERIOD: Duration = Duration::from_millis(10);
const MIN_IO_RATE_BYTES_LIMIT_FOR_TYPE: usize = 1 * 1024 * 1024;

#[inline]
fn calculate_bytes_per_refill(bytes_per_sec: usize, refill_period: Duration) -> usize {
    (bytes_per_sec as f64 * refill_period.as_secs_f64()) as usize
}

struct RawIORateLimiterProtected {
    last_refill_time: Instant,
    calibrator: Option<Box<dyn BytesCalibrator>>,
}

/// Limit total IO flow below provided threshold by applying backpressure to low-priority
/// IO types proportional to their `backpressure-weight`. Rate limit is disabled when
/// total IO threshold is set to zero.
/// TODO: add builder
struct RawIORateLimiter {
    refill_period: Duration,
    backpressure_weight: [usize; IOType::VARIANT_COUNT],
    total_bytes_per_refill: CachePadded<AtomicUsize>,
    low_priority_bytes_per_refill: RawIOVec,
    high_priority_aggregated_consumed: CachePadded<AtomicUsize>,
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
            backpressure_weight: [0; IOType::VARIANT_COUNT],
            total_bytes_per_refill: CachePadded::new(AtomicUsize::new(calculate_bytes_per_refill(
                bytes_per_sec,
                refill_period,
            ))),
            low_priority_bytes_per_refill: RawIOVec::new(),
            high_priority_aggregated_consumed: CachePadded::new(AtomicUsize::new(0)),
            consumed: RawIOVec::new(),
            stats: Arc::new(RawIOVec::new()),
            condv: Condvar::new(),
            protected: Mutex::new(RawIORateLimiterProtected {
                last_refill_time: Instant::now_coarse(),
                calibrator: None,
            }),
        }
    }

    #[allow(dead_code)]
    pub fn set_backpressure_weight(&mut self, io_type: IOType, weight: usize) {
        self.backpressure_weight[io_type as usize] = weight;
    }

    #[allow(dead_code)]
    pub fn set_calibrator(&mut self, calibrator: Box<dyn BytesCalibrator>) {
        self.protected.get_mut().calibrator = Some(calibrator);
    }

    #[allow(dead_code)]
    pub fn finalize(&mut self) -> Arc<RawIOVec> {
        // sanitize backpressure weight to percentage
        let total_weight: usize = self.backpressure_weight.iter().sum();
        if total_weight > 0 && total_weight != 100 {
            self.backpressure_weight.iter_mut().for_each(|w| {
                *w = *w * 100 / total_weight;
            });
        }
        let bytes_per_refill = self.total_bytes_per_refill.load(Ordering::Relaxed);
        if bytes_per_refill > 0 {
            for i in 0..IOType::VARIANT_COUNT {
                if self.backpressure_weight[i] > 0 {
                    self.low_priority_bytes_per_refill
                        .store(i, bytes_per_refill);
                }
            }
        }
        self.stats.clone()
    }

    /// Dynamically changes the total IO flow threshold, effective after at most `refill_period`.
    #[allow(dead_code)]
    pub fn set_bytes_per_sec(&self, bytes_per_sec: usize) {
        self.total_bytes_per_refill.store(
            calculate_bytes_per_refill(bytes_per_sec, self.refill_period),
            Ordering::Relaxed,
        );
    }

    pub fn request(&self, io_type: IOType, amount: usize) -> usize {
        let io_type_idx = io_type as usize;
        let backpressure_weight = self.backpressure_weight[io_type_idx];
        loop {
            let cached_bytes_per_refill = self.total_bytes_per_refill.load(Ordering::Relaxed);
            if cached_bytes_per_refill == 0 {
                return amount;
            }
            if backpressure_weight == 0 {
                let amount = std::cmp::min(cached_bytes_per_refill, amount);
                if self
                    .high_priority_aggregated_consumed
                    .fetch_add(amount, Ordering::Relaxed)
                    + amount
                    <= cached_bytes_per_refill
                {
                    // only for bookkeeping
                    self.consumed.fetch_add(io_type_idx, amount);
                    return amount;
                }
            } else {
                let cached_bytes_per_refill = self.low_priority_bytes_per_refill.load(io_type_idx);
                let amount = std::cmp::min(cached_bytes_per_refill, amount);
                if self.consumed.fetch_add(io_type_idx, amount) + amount <= cached_bytes_per_refill
                {
                    return amount;
                }
            }
            let mut locked = self.protected.lock();
            // TODO: calibration
            let now = Instant::now_coarse();
            assert!(now >= locked.last_refill_time);
            let since_last_refill = now.duration_since(locked.last_refill_time);
            if since_last_refill < self.refill_period {
                let cached_last_refill_time = locked.last_refill_time;
                // use a slightly larger timeout so that they can react to notification
                // and preserve enqueue order
                let (mut locked, timed_out) = self
                    .condv
                    .wait_timeout(locked, self.refill_period.mul_f32(1.1) - since_last_refill);
                if timed_out && locked.last_refill_time == cached_last_refill_time {
                    self.refill(&mut locked, Instant::now_coarse());
                }
            } else {
                self.refill(&mut locked, now);
            }
        }
    }

    #[inline]
    fn refill(&self, locked: &mut RawIORateLimiterProtected, now: Instant) {
        let cached_bytes_per_refill = self.total_bytes_per_refill.load(Ordering::Relaxed);
        // consumptions are capped below corresponding thresholds
        let mut total_consumed = std::cmp::min(
            cached_bytes_per_refill,
            self.high_priority_aggregated_consumed
                .load(Ordering::Relaxed),
        );
        for i in 0..IOType::VARIANT_COUNT {
            let mut consumed = self.consumed.load(i);
            self.consumed.store(i, 0);
            if self.backpressure_weight[i] > 0 {
                consumed = std::cmp::min(self.low_priority_bytes_per_refill.load(i), consumed);
                total_consumed += consumed;
            }
            self.stats.fetch_add(i, consumed);
        }
        // calculate backpressure
        let backpressure = cached_bytes_per_refill as i64 - total_consumed as i64;
        for i in 0..IOType::VARIANT_COUNT {
            if self.backpressure_weight[i] > 0 {
                let updated = std::cmp::min(
                    MIN_IO_RATE_BYTES_LIMIT_FOR_TYPE,
                    (self.low_priority_bytes_per_refill.load(i) as i64
                        + backpressure * self.backpressure_weight[i] as i64 / 100)
                        .abs() as usize,
                );
                self.low_priority_bytes_per_refill.store(i, updated);
            }
        }

        self.high_priority_aggregated_consumed
            .store(0, Ordering::Relaxed);
        if let Some(calibrator) = &mut locked.calibrator {
            calibrator.reset();
        }
        locked.last_refill_time = now;
        self.condv.notify_all();
    }

    #[cfg(test)]
    pub fn is_drained(&self, io_type: IOType) -> bool {
        if self.backpressure_weight[io_type as usize] > 0 {
            self.consumed.load(io_type as usize)
                >= self.low_priority_bytes_per_refill.load(io_type as usize)
        } else {
            self.high_priority_aggregated_consumed
                .load(Ordering::Relaxed)
                >= self.total_bytes_per_refill.load(Ordering::Relaxed)
        }
    }
}

pub struct IOStats {
    read_bytes: Option<Arc<RawIOVec>>,
    write_bytes: Option<Arc<RawIOVec>>,
    read_ios: Option<Arc<RawIOVec>>,
    write_ios: Option<Arc<RawIOVec>>,
}

impl IOStats {
    pub fn new() -> Self {
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

    fn approximate_eq(left: f64, right: f64) {
        assert!(left > right * 0.9);
        assert!(left < right * 1.1);
    }

    fn approximate_eq_2(left: f64, right: f64, margin: f64) {
        assert!(left > right * (1.0 - margin));
        assert!(left < right * (1.0 + margin));
    }

    #[test]
    fn test_basic_rate_limit() {
        let refills_in_one_sec = 10;
        let refill_period = Duration::from_millis(1000 / refills_in_one_sec as u64);
        let mut limiter = RawIORateLimiter::new(refill_period, 2 * refills_in_one_sec);
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
        approximate_eq(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes) as f64,
            bytes_per_sec as f64 * duration.as_secs_f64(),
        );
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let mut limiter = RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, kbytes_per_sec * 1000);
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
        approximate_eq(
            stats.fetch(IOType::Compaction, IOOp::Write, IOMeasure::Bytes) as f64,
            actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0,
        );
    }

    #[test]
    fn test_rate_limited_hybrid_flow() {
        let bytes_per_sec = 100000;
        let write_work = 60;
        let (import_weight, import_work) = (30, 50);
        let (flush_weight, flush_work) = (10, 50);
        let mut limiter = RawIORateLimiter::new(DEFAULT_REFILL_PERIOD, bytes_per_sec);
        limiter.set_backpressure_weight(IOType::Import, import_weight);
        limiter.set_backpressure_weight(IOType::Flush, flush_weight);
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
                        write_work * bytes_per_sec / 100 / 1000 / 2,
                    ),
                    Some(Duration::from_millis(1)),
                );
                let _import = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(IOType::Import, import_work * bytes_per_sec / 100 / 1000 / 2),
                    Some(Duration::from_millis(1)),
                );
                let _flush = start_background_jobs(
                    limiter.clone(),
                    2, /*job_count*/
                    Request(IOType::Flush, flush_work * bytes_per_sec / 100 / 1000 / 2),
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
        let import_bytes = stats.fetch(IOType::Import, IOOp::Write, IOMeasure::Bytes);
        let flush_bytes = stats.fetch(IOType::Flush, IOOp::Write, IOMeasure::Bytes);
        let total_bytes = write_bytes + import_bytes + flush_bytes;
        approximate_eq(
            total_bytes as f64,
            bytes_per_sec as f64 * duration.as_secs_f64(),
        );
        let unfulfiled_import = 1.0
            - import_bytes as f64
                / ((import_work * bytes_per_sec / 100) as f64 * duration.as_secs_f64());
        let unfulfiled_flush = 1.0
            - flush_bytes as f64
                / ((flush_work * bytes_per_sec / 100) as f64 * duration.as_secs_f64());
        approximate_eq_2(
            unfulfiled_import * flush_weight as f64,
            unfulfiled_flush * import_weight as f64,
            0.2,
        );
    }
}
