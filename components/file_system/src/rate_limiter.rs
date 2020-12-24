// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{condvar::Condvar, IOOp, IOType};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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

/// Used to calibrate actual bytes through since last reset.
/// TODO: implement iosnoop-based calibrator
#[derive(Debug)]
struct BytesCalibrator {
    io_type: IOType,
    io_op: IOOp,
}

impl BytesCalibrator {
    #[allow(dead_code)]
    pub fn new(io_type: IOType, io_op: IOOp) -> Self {
        BytesCalibrator { io_type, io_op }
    }

    pub fn calibrate(&self) -> usize {
        0
    }

    pub fn reset(&self) {}
}

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct BytesRecorder {
    read: [AtomicUsize; IOType::VARIANT_COUNT],
    write: [AtomicUsize; IOType::VARIANT_COUNT],
}

impl BytesRecorder {
    pub fn new() -> Self {
        BytesRecorder {
            read: Default::default(),
            write: Default::default(),
        }
    }

    pub fn add(&self, io_type: IOType, io_op: IOOp, len: usize) {
        match io_op {
            IOOp::Read => {
                self.read[io_type as usize].fetch_add(len, Ordering::Relaxed);
            }
            IOOp::Write => {
                self.write[io_type as usize].fetch_add(len, Ordering::Relaxed);
            }
        }
    }

    pub fn fetch(&self, io_type: IOType, io_op: IOOp) -> usize {
        match io_op {
            IOOp::Read => self.read[io_type as usize].load(Ordering::Relaxed),
            IOOp::Write => self.write[io_type as usize].load(Ordering::Relaxed),
        }
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        for i in self.read.iter() {
            i.store(0, Ordering::Relaxed);
        }
        for i in self.write.iter() {
            i.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
struct PerTypeIORateLimiter {
    bytes_per_refill: AtomicUsize,
    consumed: AtomicUsize,
    refill_period: Duration,
    last_refill_time: Mutex<Instant>,
    condv: Condvar,
    calibrator: Option<BytesCalibrator>,
}

#[inline]
fn calculate_bytes_per_refill(bytes_per_sec: usize, refill_period: Duration) -> usize {
    (bytes_per_sec as f64 * refill_period.as_secs_f64()) as usize
}

impl PerTypeIORateLimiter {
    /// Create a new rate limiter. Rate limiting is disabled when `bytes_per_sec` is zero.
    pub fn new(bytes_per_sec: usize, refill_period: Duration) -> PerTypeIORateLimiter {
        PerTypeIORateLimiter {
            bytes_per_refill: AtomicUsize::new(calculate_bytes_per_refill(
                bytes_per_sec,
                refill_period,
            )),
            consumed: AtomicUsize::new(0),
            refill_period,
            last_refill_time: Mutex::new(Instant::now()),
            condv: Condvar::new(),
            calibrator: None,
        }
    }

    pub fn set_bytes_per_sec(&self, bytes_per_sec: usize) {
        self.bytes_per_refill.store(
            calculate_bytes_per_refill(bytes_per_sec, self.refill_period),
            Ordering::Relaxed,
        );
    }

    #[allow(dead_code)]
    pub fn set_calibrator(&mut self, calibrator: BytesCalibrator) {
        self.calibrator = Some(calibrator);
    }

    #[inline]
    fn request_fast(&self, bytes_per_refill: usize, bytes: usize) -> Option<usize> {
        if self.consumed.load(Ordering::Relaxed) < bytes_per_refill {
            // Consumed bytes are allowed to be larger than the actual bytes
            // through when quotas are drained.
            let before = self.consumed.fetch_add(bytes, Ordering::Relaxed);
            if before < bytes_per_refill {
                return Some(std::cmp::min(bytes_per_refill - before, bytes));
            }
        }
        None
    }

    #[inline]
    fn refill_and_request(&self, bytes: usize) -> usize {
        let token = std::cmp::min(self.bytes_per_refill.load(Ordering::Relaxed), bytes);
        self.consumed.store(token, Ordering::Relaxed);
        if let Some(calibrator) = &self.calibrator {
            calibrator.reset();
        }
        self.condv.notify_all();
        token
    }

    pub fn request(&self, bytes: usize, priority: IOPriority) -> usize {
        let cached_bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        if cached_bytes_per_refill == 0 {
            return bytes;
        }
        if priority == IOPriority::High {
            self.consumed.fetch_add(bytes, Ordering::Relaxed);
            // Unlimited requestor don't do refills themselves, therefore the first
            // limited IO in long period will take penalty for a mandotory refill.
            return bytes;
        }
        loop {
            if let Some(bytes) = self.request_fast(cached_bytes_per_refill, bytes) {
                break bytes;
            }
            let mut last_refill_time = self.last_refill_time.lock().unwrap();
            // double check if bytes have been refilled by others
            if self.consumed.load(Ordering::Relaxed) < cached_bytes_per_refill {
                continue;
            }
            if let Some(calibrator) = &self.calibrator {
                let calibrated = calibrator.calibrate();
                self.consumed.store(calibrated, Ordering::Relaxed);
                if calibrated < cached_bytes_per_refill {
                    continue;
                }
            }
            let now = Instant::now();
            if now > *last_refill_time {
                let since_last_refill = now.duration_since(*last_refill_time);
                if since_last_refill >= self.refill_period {
                    *last_refill_time = now;
                    break self.refill_and_request(bytes);
                } else {
                    let cached_last_refill_time = *last_refill_time;
                    let (mut last_refill_time, timed_out) = self
                        .condv
                        .wait_timeout(last_refill_time, self.refill_period - since_last_refill);
                    let now = Instant::now();
                    if timed_out && *last_refill_time == cached_last_refill_time {
                        // timeout, do the refill myself
                        *last_refill_time = now;
                        break self.refill_and_request(bytes);
                    }
                }
            }
        }
    }

    pub async fn async_request(&self, bytes: usize, priority: IOPriority) -> usize {
        let cached_bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        if cached_bytes_per_refill == 0 {
            return bytes;
        }
        if priority == IOPriority::High {
            self.consumed.fetch_add(bytes, Ordering::Relaxed);
            return bytes;
        }
        loop {
            if let Some(bytes) = self.request_fast(cached_bytes_per_refill, bytes) {
                break bytes;
            }
            let mut last_refill_time = self.last_refill_time.lock().unwrap();
            if self.consumed.load(Ordering::Relaxed) < cached_bytes_per_refill {
                continue;
            }
            if let Some(calibrator) = &self.calibrator {
                let calibrated = calibrator.calibrate();
                self.consumed.store(calibrated, Ordering::Relaxed);
                if calibrated < cached_bytes_per_refill {
                    continue;
                }
            }
            let now = Instant::now();
            if now > *last_refill_time {
                let since_last_refill = now.duration_since(*last_refill_time);
                if since_last_refill >= self.refill_period {
                    *last_refill_time = now;
                    break self.refill_and_request(bytes);
                } else {
                    let cached_last_refill_time = *last_refill_time;
                    let (mut last_refill_time, timed_out) = self
                        .condv
                        .async_wait_timeout(
                            &self.last_refill_time,
                            last_refill_time,
                            self.refill_period - since_last_refill,
                        )
                        .await;
                    let now = Instant::now();
                    if timed_out && *last_refill_time == cached_last_refill_time {
                        *last_refill_time = now;
                        break self.refill_and_request(bytes);
                    }
                }
            }
        }
    }
}

impl Default for PerTypeIORateLimiter {
    fn default() -> PerTypeIORateLimiter {
        PerTypeIORateLimiter::new(0, Duration::from_millis(10))
    }
}

/// An instance of `IORateLimiter` should be safely shared between threads.
#[derive(Debug)]
pub struct IORateLimiter {
    // IOType::Other slot is used to store type-less limiter
    write_limiters: [PerTypeIORateLimiter; IOType::VARIANT_COUNT],
    read_limiters: [PerTypeIORateLimiter; IOType::VARIANT_COUNT],
    total_limiters: [PerTypeIORateLimiter; IOType::VARIANT_COUNT],
    recorder: Arc<BytesRecorder>,
}

impl IORateLimiter {
    // TODO: pass in rate limiting options
    pub fn new(bytes_per_sec: usize, recorder: Arc<BytesRecorder>) -> IORateLimiter {
        let limiter = IORateLimiter {
            write_limiters: Default::default(),
            read_limiters: Default::default(),
            total_limiters: Default::default(),
            recorder,
        };
        if bytes_per_sec != 0 {
            for l in limiter.write_limiters.iter() {
                l.set_bytes_per_sec(bytes_per_sec);
            }
            for l in limiter.read_limiters.iter() {
                l.set_bytes_per_sec(bytes_per_sec);
            }
            for l in limiter.total_limiters.iter() {
                l.set_bytes_per_sec(bytes_per_sec);
            }
        }
        limiter
    }

    /// Requests for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let prio = get_priority(io_type);
        let mut bytes = self.total_limiters[IOType::Other as usize].request(bytes, prio);
        if io_type != IOType::Other {
            bytes = self.total_limiters[io_type as usize].request(bytes, prio);
        }
        match io_op {
            IOOp::Write => {
                bytes = self.write_limiters[IOType::Other as usize].request(bytes, prio);
                if io_type != IOType::Other {
                    bytes = self.write_limiters[io_type as usize].request(bytes, prio);
                }
            }
            IOOp::Read => {
                bytes = self.read_limiters[IOType::Other as usize].request(bytes, prio);
                if io_type != IOType::Other {
                    bytes = self.read_limiters[io_type as usize].request(bytes, prio);
                }
            }
        }
        self.recorder.add(io_type, io_op, bytes);
        bytes
    }

    /// Asynchronously requests for token for bytes and potentially update
    /// statistics. If this request can not be satisfied, the call is blocked.
    /// Granted token can be less than the requested bytes, but must be greater
    /// than zero.
    pub async fn async_request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let prio = get_priority(io_type);
        let mut bytes = self.total_limiters[IOType::Other as usize]
            .async_request(bytes, prio)
            .await;
        if io_type != IOType::Other {
            bytes = self.total_limiters[io_type as usize]
                .async_request(bytes, prio)
                .await;
        }
        match io_op {
            IOOp::Write => {
                bytes = self.write_limiters[IOType::Other as usize]
                    .async_request(bytes, prio)
                    .await;
                if io_type != IOType::Other {
                    bytes = self.write_limiters[io_type as usize]
                        .async_request(bytes, prio)
                        .await
                }
            }
            IOOp::Read => {
                bytes = self.read_limiters[IOType::Other as usize]
                    .async_request(bytes, prio)
                    .await;
                if io_type != IOType::Other {
                    bytes = self.read_limiters[io_type as usize]
                        .async_request(bytes, prio)
                        .await
                }
            }
        }
        self.recorder.add(io_type, io_op, bytes);
        bytes
    }
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IORateLimiter>>> = Mutex::new(None);
}

pub fn set_io_rate_limiter(limiter: IORateLimiter) {
    *IO_RATE_LIMITER.lock().unwrap() = Some(Arc::new(limiter));
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    if let Some(ref limiter) = *IO_RATE_LIMITER.lock().unwrap() {
        Some(limiter.clone())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use test::Bencher;

    #[test]
    fn test_rate_limit() {
        let refills_in_one_sec = 100;
        let refill_period = Duration::from_millis(1000 / refills_in_one_sec as u64);

        let limiter = PerTypeIORateLimiter::new(2 * refills_in_one_sec, refill_period);
        assert_eq!(limiter.request(1, IOPriority::Low), 1);
        assert_eq!(limiter.request(10, IOPriority::Low), 1);
        assert_eq!(limiter.request(10, IOPriority::Low), 2);
        limiter.set_bytes_per_sec(10 * refills_in_one_sec);
        assert_eq!(limiter.request(10, IOPriority::Low), 10 - 2);
        limiter.set_bytes_per_sec(100 * refills_in_one_sec);

        let limiter = Arc::new(limiter);
        let mut threads = vec![];
        assert_eq!(limiter.request(1000, IOPriority::Low), 100 - 2 - 10);
        assert_eq!(limiter.request(1000, IOPriority::Low), 100);
        let begin = Instant::now();
        for _ in 0..50 {
            let limiter = limiter.clone();
            let t = std::thread::spawn(move || {
                assert_eq!(limiter.request(1, IOPriority::Low), 1);
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        let end = Instant::now();
        assert!(end.duration_since(begin) > refill_period);
        assert!(end.duration_since(begin) < refill_period * 2);
        assert_eq!(limiter.request(1000, IOPriority::Low), 50);
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

    fn start_background_jobs(
        job_count: usize,
        io_type: IOType,
        io_op: IOOp,
        request: usize,
    ) -> BackgroundContext {
        let mut threads = vec![];
        let stop = Arc::new(AtomicBool::new(false));
        for _ in 0..job_count {
            let stop = stop.clone();
            let limiter = get_io_rate_limiter().unwrap();
            let t = std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    limiter.request(io_type, io_op, request);
                }
            });
            threads.push(t);
        }
        BackgroundContext {
            threads,
            stop: Some(stop),
        }
    }

    fn start_background_jobs_counted(
        job_count: usize,
        io_type: IOType,
        io_op: IOOp,
        request: usize,
        count: usize,
        interval: Duration,
    ) -> BackgroundContext {
        let mut threads = vec![];
        for _ in 0..job_count {
            let limiter = get_io_rate_limiter().unwrap();
            let t = std::thread::spawn(move || {
                let mut requested = 0;
                while requested < count {
                    limiter.request(io_type, io_op, request);
                    std::thread::sleep(interval);
                    requested += 1;
                }
            });
            threads.push(t);
        }
        BackgroundContext {
            threads,
            stop: None,
        }
    }

    #[test]
    fn test_rate_limited_heavy_flow() {
        let bytes_per_sec = 10000;
        let recorder = Arc::new(BytesRecorder::new());
        set_io_rate_limiter(IORateLimiter::new(bytes_per_sec, recorder.clone()));
        let duration = {
            let begin = Instant::now();
            {
                let _context = start_background_jobs(10, IOType::Compaction, IOOp::Write, 10);
                std::thread::sleep(Duration::from_secs(2));
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        assert!(
            recorder.fetch(IOType::Compaction, IOOp::Write)
                <= (bytes_per_sec as f64 * duration.as_secs_f64()) as usize
        );
        assert!(
            recorder.fetch(IOType::Compaction, IOOp::Write)
                >= (bytes_per_sec as f64 * duration.as_secs_f64() * 0.95) as usize
        );
    }

    #[test]
    fn test_rate_limited_light_flow() {
        let kbytes_per_sec = 3;
        let actual_kbytes_per_sec = 2;
        let recorder = Arc::new(BytesRecorder::new());
        set_io_rate_limiter(IORateLimiter::new(kbytes_per_sec * 1000, recorder.clone()));
        let duration = {
            let begin = Instant::now();
            {
                // each thread request at most 1000 bytes per second, elapsed around 2 seconds
                let _context = start_background_jobs_counted(
                    actual_kbytes_per_sec,
                    IOType::Compaction,
                    IOOp::Write,
                    1,
                    2 * 1000,
                    Duration::from_millis(1),
                );
            }
            let end = Instant::now();
            end.duration_since(begin)
        };
        assert!(
            recorder.fetch(IOType::Compaction, IOOp::Write)
                <= (kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0) as usize
        );
        assert!(
            recorder.fetch(IOType::Compaction, IOOp::Write)
                >= (actual_kbytes_per_sec as f64 * duration.as_secs_f64() * 1000.0 * 0.9) as usize
        );
    }

    #[bench]
    fn bench_acquire_limiter(b: &mut Bencher) {
        set_io_rate_limiter(IORateLimiter::new(0, Arc::new(BytesRecorder::new())));
        b.iter(|| {
            let _ = get_io_rate_limiter().unwrap();
        });
    }

    #[bench]
    fn bench_noop_limiter(b: &mut Bencher) {
        set_io_rate_limiter(IORateLimiter::new(0, Arc::new(BytesRecorder::new())));
        let _context = start_background_jobs(3, IOType::Write, IOOp::Write, 10);
        let limiter = get_io_rate_limiter().unwrap();
        b.iter(|| {
            limiter.request(IOType::Write, IOOp::Write, 10);
        });
    }

    #[bench]
    fn bench_not_limited(b: &mut Bencher) {
        set_io_rate_limiter(IORateLimiter::new(10000, Arc::new(BytesRecorder::new())));
        let _context = start_background_jobs(3, IOType::Write, IOOp::Write, 10);
        let limiter = get_io_rate_limiter().unwrap();
        b.iter(|| {
            limiter.request(IOType::Write, IOOp::Write, 10);
        });
    }

    #[bench]
    fn bench_limited_fast(b: &mut Bencher) {
        set_io_rate_limiter(IORateLimiter::new(
            usize::max_value(),
            Arc::new(BytesRecorder::new()),
        ));
        let _context = start_background_jobs(3, IOType::Compaction, IOOp::Write, 1);
        let limiter = get_io_rate_limiter().unwrap();
        b.iter(|| {
            limiter.request(IOType::Compaction, IOOp::Write, 1);
        });
    }
}
