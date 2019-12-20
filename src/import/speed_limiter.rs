// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Speed limit enforcement.
//!
//! We need to restrict the maximum upload speed to avoid saturating the network
//! bandwidth, which caused tikv-importer unable to contact PD.
//!
//! This is implemented using the Token Bucket algorithm based on [1], but
//! allowing bursting.
//!
//! [1]: https://medium.com/smyte/rate-limiter-df3408325846

use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;

use tikv_util::time::{duration_to_sec, Instant};

/// A `Clock` controls the passing of time.
pub trait Clock {
    /// Type to represent a point of time.
    type Instant: Copy;

    /// Returns the current time instant. It should be monotonically increasing
    /// but high precision is not required.
    fn now(&self) -> Self::Instant;

    /// Computes the duration elapsed between two time instants. The first
    /// instant must be later than the second one.
    fn sub(later: Self::Instant, earlier: Self::Instant) -> Duration;

    /// Sleeps the current thread for the given duration.
    fn sleep(&self, dur: Duration);
}

/// The `StandardClock` obtains time using the physical clock.
pub struct StandardClock;

impl Clock for StandardClock {
    type Instant = Instant;

    fn now(&self) -> Instant {
        Instant::now_coarse()
    }

    fn sub(later: Instant, earlier: Instant) -> Duration {
        later.duration_since(earlier)
    }

    fn sleep(&self, dur: Duration) {
        sleep(dur);
    }
}

/// The type that enforces a speed limit across multiple threads.
pub struct SpeedLimiter<C: Clock = StandardClock> {
    // The maximum speed limit, unit = Byte/s.
    speed_limit: f64,
    // The last update time and the remaining amount of bytes can be sent during
    // this second.
    value: Mutex<(C::Instant, f64)>,
    // The clock used for time calculation.
    clock: C,
}

const LOG_THRESHOLD: Duration = Duration::from_secs(1);

/// `SpeedLimiter` is a type to control the upper limit speed of an action
/// cooperatively.
impl<C: Clock> SpeedLimiter<C> {
    /// Creates a speed limiter.
    pub fn new(speed_limit: f64, clock: C) -> Self {
        Self {
            speed_limit,
            value: Mutex::new((clock.now(), speed_limit)),
            clock,
        }
    }

    /// Takes a block of the given size (in bytes) from the speed limiter. If
    /// the block is taken too quickly, this method will sleep until the speed
    /// limit is below the specified value.
    ///
    /// This method will block the entire thread if the speed limit is violated,
    /// and thus should not be invoked when implementing an async fn.
    pub fn take(&self, tag: &str, byte_size: u64) {
        let size = byte_size as f64;

        loop {
            let mut lock = self.value.lock().unwrap();
            let (last_update, mut value) = *lock;
            let now = self.clock.now();
            let elapsed = duration_to_sec(C::sub(now, last_update));
            value = self.speed_limit.min(value + elapsed * self.speed_limit);
            if value >= size || value >= self.speed_limit {
                // The second condition allows take() to proceed on a completely
                // filled bucket, i.e. allows sending a 1 GB file on a 512 MB/s
                // limit. The bucket value will temporarily drop to -512 MB,
                // which will be refilled after 2 seconds as expected.
                *lock = (now, value - size);
                return;
            }

            // Reaching here means the bucket's value is not high enough to send
            // all bytes. We sleep for the time needed to completely refill the
            // bucket.
            drop(lock);

            let sleep_seconds = 1.0 - value / self.speed_limit;
            let sleep_dur = Duration::from_secs_f64(sleep_seconds);
            let should_log = sleep_dur > LOG_THRESHOLD;
            if should_log {
                // Don't bother with short waits, avoiding flooding the log with
                // useless information.
                info!(
                    "speed limited begin";
                    "tag" => %tag,
                    "value" => %value,
                    "size" => %byte_size,
                    "going to wait" => ?sleep_dur,
                );
            }
            self.clock.sleep(sleep_dur);
            if should_log {
                info!(
                    "speed limited end";
                    "tag" => %tag,
                    "value" => %value,
                    "size" => %byte_size,
                    "takes" => ?sleep_dur,
                );
            }
        }
    }
}

// This is a metronome-based test case inspired by Java's [MultithreadedTC]
// framework.
//
// [MultithreadedTC]: https://www.cs.umd.edu/projects/PL/multithreadedtc/
#[cfg(test)]
mod metronome {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;
    use std::sync::Arc;
    use std::time::Duration;

    use crossbeam::atomic::AtomicCell;
    use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
    use crossbeam::thread::{scope, Scope};

    struct Sleep {
        end: Duration,
        wakeup_send: Sender<()>,
    }

    // Implement Ord for Sleep with the *earliest* end time as the maximum
    // in order to fit in the BinaryHeap.
    impl PartialEq for Sleep {
        fn eq(&self, other: &Self) -> bool {
            self.end == other.end
        }
    }

    impl PartialOrd for Sleep {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            other.end.partial_cmp(&self.end)
        }
    }

    impl Eq for Sleep {}

    impl Ord for Sleep {
        fn cmp(&self, other: &Self) -> Ordering {
            other.end.cmp(&self.end)
        }
    }

    enum ThreadState {
        Sleep(Sleep),
        Ended,
    }

    pub struct Metronome {
        now: Arc<AtomicCell<Duration>>,
        tick_recv: Receiver<ThreadState>,
    }

    /// If any threading operation exceeds this value, the test has probably
    /// dead-locked and should be considered a failure.
    const TIMEOUT: Duration = Duration::from_millis(500);

    impl Metronome {
        pub fn new() -> (Self, Sleeper) {
            let (tick_send, tick_recv) = unbounded();
            let now = Arc::new(AtomicCell::new(Duration::from_secs(0)));
            (
                Metronome {
                    now: Arc::clone(&now),
                    tick_recv,
                },
                Sleeper { now, tick_send },
            )
        }

        pub fn run(&self, mut threads: usize) {
            let mut sleepers = BinaryHeap::with_capacity(threads);
            for state in self.tick_recv.iter() {
                match state {
                    ThreadState::Ended => {
                        threads -= 1;
                    }
                    ThreadState::Sleep(call) => {
                        sleepers.push(call);
                    }
                }
                if sleepers.len() >= threads {
                    // reaching here means all threads are blocked, so we can
                    // guarantee no one will be waiting for time earlier than
                    // those inside the `sleepers` binary heap.
                    //
                    // we could then pop the earliest item, and jump the time to
                    // that point.
                    if let Some(call) = sleepers.pop() {
                        self.now.store(call.end);
                        call.wakeup_send.send_timeout((), TIMEOUT).unwrap();
                    } else {
                        // sleepers is empty meaning all threads are completed.
                        // just quit the loop.
                        return;
                    }
                }
            }
        }
    }

    pub struct Sleeper {
        now: Arc<AtomicCell<Duration>>,
        tick_send: Sender<ThreadState>,
    }

    impl Sleeper {
        pub fn now(&self) -> Duration {
            self.now.load()
        }

        pub fn sleep(&self, d: Duration) {
            let (wakeup_send, wakeup_recv) = bounded(0);
            let end = self.now.load() + d;
            self.tick_send
                .send_timeout(ThreadState::Sleep(Sleep { end, wakeup_send }), TIMEOUT)
                .unwrap();
            wakeup_recv.recv_timeout(TIMEOUT).unwrap();
        }

        pub fn spawn<'env, F>(&'env self, sc: &Scope<'env>, f: F)
        where
            F: FnOnce() + Send + 'env,
        {
            sc.spawn(move |_| {
                let _thread_ender = ThreadEnder {
                    tick_send: &self.tick_send,
                };
                f();
            });
        }
    }

    struct ThreadEnder<'env> {
        tick_send: &'env Sender<ThreadState>,
    }

    impl Drop for ThreadEnder<'_> {
        fn drop(&mut self) {
            self.tick_send
                .send_timeout(ThreadState::Ended, TIMEOUT)
                .unwrap();
        }
    }

    #[test]
    fn test_metronome() {
        let (metronome, sleeper) = Metronome::new();

        scope(|sc| {
            sc.spawn(|_| metronome.run(2));
            sleeper.spawn(sc, || {
                assert_eq!(sleeper.now(), Duration::from_secs(0));
                sleeper.sleep(Duration::from_secs(200));
                assert_eq!(sleeper.now(), Duration::from_secs(200));
                sleeper.sleep(Duration::from_secs(300));
                assert_eq!(sleeper.now(), Duration::from_secs(500));
            });
            sleeper.spawn(sc, || {
                assert_eq!(sleeper.now(), Duration::from_secs(0));
                sleeper.sleep(Duration::from_secs(150));
                assert_eq!(sleeper.now(), Duration::from_secs(150));
                sleeper.sleep(Duration::from_secs(250));
                assert_eq!(sleeper.now(), Duration::from_secs(400));
            });
        })
        .unwrap();

        assert_eq!(sleeper.now(), Duration::from_secs(500));
    }

    #[test]
    fn test_panic_not_deadlocking() {
        // TODO: The TiKV CI doesn't support #[should_panic], it interprets
        // *all* stack traces as failure even if the test is expected to fail,
        // so we need insert a custom panic hook to silence the stack trace.
        // Since set_hook requires a non-panicking thread we need to wrap the
        // actual test case inside `catch_unwind`.
        use std::panic;

        let original_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));

        let res = panic::catch_unwind(|| {
            let (metronome, sleeper) = Metronome::new();

            scope(|sc| {
                sc.spawn(|_| metronome.run(1));
                sleeper.spawn(sc, || panic!("expected failure"));
            })
            .unwrap();
        });

        panic::set_hook(original_hook);

        assert!(res.is_err());
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::scope;

    use super::metronome::{Metronome, Sleeper};
    use super::*;

    impl Clock for Sleeper {
        type Instant = Duration;
        fn now(&self) -> Duration {
            self.now()
        }
        fn sub(later: Duration, earlier: Duration) -> Duration {
            later - earlier
        }
        fn sleep(&self, dur: Duration) {
            self.sleep(dur);
        }
    }

    #[test]
    fn test_under_limit_single_thread() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        scope(|sc| {
            sc.spawn(|_| metronome.run(1));
            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 50);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
                speed_limit.take("", 51);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
                speed_limit.take("", 52);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
                speed_limit.take("", 53);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
                speed_limit.take("", 54);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
                speed_limit.take("", 55);
                assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
            });
        })
        .unwrap();

        assert_eq!(speed_limit.clock.now(), Duration::from_secs(0));
    }

    #[test]
    fn test_over_limit_single_thread() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        scope(|sc| {
            sc.spawn(|_| metronome.run(1));
            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 200);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(0));
                speed_limit.take("", 201);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(0));

                // 783_203_125 ns = (200+201)/512 seconds
                speed_limit.take("", 202);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(783_203_125));
                speed_limit.take("", 203);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(783_203_125));

                // 1_574_218_750 ns = (200+201+202+203)/512 seconds
                speed_limit.take("", 204);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_750));
                speed_limit.take("", 205);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_750));
            });
        })
        .unwrap();

        assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_750));
    }

    #[test]
    fn test_over_limit_multi_thread() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        // Scheduling of the items (total up to 512 per second):
        //
        // [ 200 ] -> [ 202 ] -> [ 204 ]
        // [ 201 ] -> [ 203 ] -> [ 205 ]

        scope(|sc| {
            sc.spawn(|_| metronome.run(2));

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 200);
                // these 1ns sleeps are inserted as synchronization point to
                // enforce thread ordering by flushing the sleeper binary heap,
                // so that it won't run like 200 -> 202 -> 201 -> 203 which
                // ruins the point of this new test.
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1));

                speed_limit.take("", 202);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(783_203_126));

                speed_limit.take("", 204);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_751));
            });

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 201);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1));

                speed_limit.take("", 203);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(783_203_126));

                speed_limit.take("", 205);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_751));
            });
        })
        .unwrap();

        assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_574_218_751));
    }

    #[test]
    fn test_over_limit_multi_thread_2() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        // Scheduling of the items (total up to 512 per second):
        //
        // [ 300 ] -> [ 301 ] -> [ 302 ] -> [ 303 ] -> [ 304 ]
        // [ 100 ] -> [ 102 ] -> [ 104 ]
        // [ 101 ]    [ 103 ]

        scope(|sc| {
            sc.spawn(|_| metronome.run(2));

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 300);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1));

                speed_limit.take("", 301);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(978_515_626));

                speed_limit.take("", 302);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_966_796_876));

                speed_limit.take("", 303);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(2_759_765_625));

                speed_limit.take("", 304);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(3_351_562_500));
            });

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 100);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(0));

                speed_limit.take("", 101);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1));

                speed_limit.take("", 102);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(978_515_625));

                speed_limit.take("", 103);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(978_515_626));

                speed_limit.take("", 104);
                speed_limit.clock.sleep(Duration::from_nanos(1));
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(1_966_796_876));
            });
        })
        .unwrap();

        assert_eq!(speed_limit.clock.now(), Duration::from_nanos(3_351_562_500));
    }

    #[test]
    fn test_hiatus() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        // ensure the speed limiter won't forget to enforce until a long pause
        // i.e. we're observing the _maximum_ speed, not the _average_ speed.
        scope(|sc| {
            sc.spawn(|_| metronome.run(1));

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 400);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(0));

                speed_limit.take("", 401);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(781_250_000));

                speed_limit.clock.sleep(Duration::from_secs(10));
                assert_eq!(
                    speed_limit.clock.now(),
                    Duration::from_nanos(10_781_250_000)
                );

                speed_limit.take("", 402);
                assert_eq!(
                    speed_limit.clock.now(),
                    Duration::from_nanos(10_781_250_000)
                );

                speed_limit.take("", 403);
                assert_eq!(
                    speed_limit.clock.now(),
                    Duration::from_nanos(11_566_406_250)
                );
            });
        })
        .unwrap();

        assert_eq!(
            speed_limit.clock.now(),
            Duration::from_nanos(11_566_406_250)
        );
    }

    #[test]
    fn test_burst() {
        let (metronome, sleeper) = Metronome::new();
        let speed_limit = SpeedLimiter::<Sleeper>::new(512.0, sleeper);

        // ensure we could still send something much higher than the speed limit
        scope(|sc| {
            sc.spawn(|_| metronome.run(1));

            speed_limit.clock.spawn(sc, || {
                speed_limit.take("", 5000);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(0));

                speed_limit.take("", 5001);
                assert_eq!(speed_limit.clock.now(), Duration::from_nanos(9_765_625_000));

                speed_limit.take("", 5002);
                assert_eq!(
                    speed_limit.clock.now(),
                    Duration::from_nanos(19_533_203_125)
                );
            });
        })
        .unwrap();

        assert_eq!(
            speed_limit.clock.now(),
            Duration::from_nanos(19_533_203_125)
        );
    }
}
