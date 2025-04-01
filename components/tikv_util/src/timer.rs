// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ord, Ordering, Reverse},
    collections::BinaryHeap,
    sync::{mpsc, Arc, Mutex},
    thread::Builder,
    time::Duration,
};

use lazy_static::lazy_static;
use time::Timespec;
use tokio_executor::park::ParkThread;
use tokio_timer::{
    self,
    clock::{Clock, Now},
    timer::Handle,
    Delay,
};

use crate::{
    sys::thread::StdThreadBuildWrapper,
    time::{monotonic_raw_now, Instant},
};

pub struct Timer<T> {
    pending: BinaryHeap<Reverse<TimeoutTask<T>>>,
}

impl<T> Timer<T> {
    pub fn new(capacity: usize) -> Self {
        Timer {
            pending: BinaryHeap::with_capacity(capacity),
        }
    }

    /// Adds a periodic task into the `Timer`.
    pub fn add_task(&mut self, timeout: Duration, task: T) {
        let task = TimeoutTask {
            next_tick: Instant::now() + timeout,
            task,
        };
        self.pending.push(Reverse(task));
    }

    /// Gets the next `timeout` from the timer.
    pub fn next_timeout(&mut self) -> Option<Instant> {
        self.pending.peek().map(|task| task.0.next_tick)
    }

    /// Pops a `TimeoutTask` from the `Timer`, which should be ticked before
    /// `instant`. Returns `None` if no tasks should be ticked any more.
    ///
    /// The normal use case is keeping `pop_task_before` until get `None` in
    /// order to retrieve all available events.
    pub fn pop_task_before(&mut self, instant: Instant) -> Option<T> {
        if self
            .pending
            .peek()
            .map_or(false, |t| t.0.next_tick <= instant)
        {
            return self.pending.pop().map(|t| t.0.task);
        }
        None
    }
}

#[derive(Debug)]
struct TimeoutTask<T> {
    next_tick: Instant,
    task: T,
}

impl<T> PartialEq for TimeoutTask<T> {
    fn eq(&self, other: &TimeoutTask<T>) -> bool {
        self.next_tick == other.next_tick
    }
}

impl<T> Eq for TimeoutTask<T> {}

impl<T> PartialOrd for TimeoutTask<T> {
    fn partial_cmp(&self, other: &TimeoutTask<T>) -> Option<Ordering> {
        self.next_tick.partial_cmp(&other.next_tick)
    }
}

impl<T> Ord for TimeoutTask<T> {
    fn cmp(&self, other: &TimeoutTask<T>) -> Ordering {
        // TimeoutTask.next_tick must have same type of instants.
        self.partial_cmp(other).unwrap()
    }
}

struct SystemClock;

impl Now for SystemClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

lazy_static! {
    pub static ref GLOBAL_TIMER_HANDLE: Handle = start_timer_thread("timer", SystemClock);
}

/// A struct that marks the *zero* time.
///
/// A *zero* time can be any time, as what it represents is `Instant`,
/// which is Opaque.
struct TimeZero {
    /// An arbitrary time used as the zero time.
    ///
    /// Note that `zero` doesn't have to be related to `steady_time_point`, as
    /// what's observed here is elapsed time instead of time point.
    zero: std::time::Instant,
    /// A base time point.
    ///
    /// The source of time point should grow steady.
    steady_time_point: Timespec,
}

/// A clock that produces time in a steady speed.
///
/// Time produced by the clock is not affected by clock jump or time adjustment.
/// Internally it uses CLOCK_MONOTONIC_RAW to get a steady time source.
///
/// `Instant`s produced by this clock can't be compared or used to calculate
/// elapse unless they are produced using the same zero time.
#[derive(Clone)]
pub struct SteadyClock {
    zero: Arc<TimeZero>,
}

lazy_static! {
    static ref STEADY_CLOCK: SteadyClock = SteadyClock {
        zero: Arc::new(TimeZero {
            zero: std::time::Instant::now(),
            steady_time_point: monotonic_raw_now(),
        }),
    };
}

impl Default for SteadyClock {
    #[inline]
    fn default() -> SteadyClock {
        STEADY_CLOCK.clone()
    }
}

impl Now for SteadyClock {
    #[inline]
    fn now(&self) -> std::time::Instant {
        let n = monotonic_raw_now();
        let dur = Instant::elapsed_duration(n, self.zero.steady_time_point);
        self.zero.zero + dur
    }
}

/// A timer that creates steady delays.
///
/// Delay created by this timer will not be affected by time adjustment.
#[derive(Clone)]
pub struct SteadyTimer {
    clock: SteadyClock,
    handle: Handle,
}

impl SteadyTimer {
    /// Creates a delay future that will be notified after the given duration.
    pub fn delay(&self, dur: Duration) -> Delay {
        self.handle.delay(self.clock.now() + dur)
    }
}

lazy_static! {
    static ref GLOBAL_STEADY_TIMER: SteadyTimer = start_global_steady_timer();
}

impl Default for SteadyTimer {
    #[inline]
    fn default() -> SteadyTimer {
        GLOBAL_STEADY_TIMER.clone()
    }
}

fn start_global_steady_timer() -> SteadyTimer {
    let clock = SteadyClock::default();
    let handle = start_timer_thread("steady-timer", clock.clone());
    SteadyTimer { clock, handle }
}

/// A clock that ratchets forward.
///
/// It is used to workaround a panic[^1] in tokio-timer when the clock goes
/// backward, which is possible in some environments, e.g., aliyun ECS g7.
///
/// [^1]: https://github.com/tokio-rs/tokio/pull/515
struct RatchetClock<T: Now> {
    // Although the RatchetClock has to be thread-safe (Now requires Sync+Send),
    // the [`tokio_timer::timer::Clock`] that wraps it can only be accessed by
    // the timer thread. Therefore, performance penalty of using a Mutex is negligible.
    state: Mutex<RatchetClockState<T>>,
}

struct RatchetClockState<T> {
    start: std::time::Instant,
    clock: T,

    // The last recorded time in milliseconds.
    //
    // Using u64 is sufficient here because tokio-timer operates with millisecond
    // precision, and `u64::MAX` milliseconds represent an extremely large time span
    // (several million years).
    last_now_ms: u64,
}

impl<T: Now> Now for RatchetClock<T> {
    fn now(&self) -> std::time::Instant {
        let mut state = self.state.lock().unwrap();
        let now = state.clock.now();
        let now_ms = now.saturating_duration_since(state.start).as_millis() as u64;

        if now_ms > state.last_now_ms {
            state.last_now_ms = now_ms;
            state.start + Duration::from_millis(now_ms)
        } else {
            state.start + Duration::from_millis(state.last_now_ms)
        }
    }
}

impl<T: Now> RatchetClock<T> {
    fn new(clock: T) -> Self {
        RatchetClock {
            state: Mutex::new(RatchetClockState {
                clock,
                start: std::time::Instant::now(),
                last_now_ms: 0,
            }),
        }
    }
}

/// Start a timer thread with specific thread name and clock source.
fn start_timer_thread<T: Now>(name: &str, clock: T) -> Handle {
    let (tx, rx) = mpsc::channel();
    let props = crate::thread_group::current_properties();
    let ratchet_clock = RatchetClock::new(clock);
    let clock = Clock::new_with_now(ratchet_clock);
    Builder::new()
        .name(thd_name!(name))
        .spawn_wrapper(move || {
            crate::thread_group::set_properties(props);

            let mut timer = tokio_timer::Timer::new_with_now(ParkThread::new(), clock);
            tx.send(timer.handle()).unwrap();
            loop {
                timer.turn(None).unwrap();
            }
        })
        .unwrap();
    rx.recv().unwrap()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    use futures::{compat::Future01CompatExt, executor::block_on};

    use super::*;

    #[derive(Debug, PartialEq, Copy, Clone)]
    enum Task {
        A,
        B,
        C,
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(20), Task::A);
        timer.add_task(Duration::from_millis(150), Task::C);
        timer.add_task(Duration::from_millis(100), Task::B);
        assert_eq!(timer.pending.len(), 3);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::A);
        assert_eq!(timer.pop_task_before(tick_time), None);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::B);
        assert_eq!(timer.pop_task_before(tick_time), None);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(timer.pop_task_before(tick_time).unwrap(), Task::C);
        assert_eq!(timer.pop_task_before(tick_time), None);
    }

    #[test]
    fn test_global_timer() {
        let handle = super::GLOBAL_TIMER_HANDLE.clone();
        let delay =
            handle.delay(::std::time::Instant::now() + std::time::Duration::from_millis(100));
        let timer = Instant::now();
        block_on(delay.compat()).unwrap();
        assert!(timer.saturating_elapsed() >= Duration::from_millis(100));
    }

    #[test]
    fn test_global_steady_timer() {
        let t = SteadyTimer::default();
        let start = t.clock.now();
        let delay = t.delay(Duration::from_millis(100));
        block_on(delay.compat()).unwrap();
        let end = t.clock.now();
        let elapsed = end.duration_since(start);
        assert!(
            elapsed >= Duration::from_millis(100),
            "{:?} {:?} {:?}",
            start,
            end,
            elapsed
        );
    }

    #[test]
    fn test_ratchet_clock() {
        struct BadClock {
            backward: AtomicBool,
        }
        impl Now for BadClock {
            fn now(&self) -> std::time::Instant {
                let now = std::time::Instant::now();
                if self.backward.load(AtomicOrdering::Relaxed) {
                    self.backward.store(false, AtomicOrdering::Relaxed);
                    now - Duration::from_millis(10)
                } else {
                    self.backward.store(true, AtomicOrdering::Relaxed);
                    now
                }
            }
        }
        let clock = BadClock {
            backward: AtomicBool::new(false),
        };
        let handle = start_timer_thread("timer", clock);

        for i in 0..100 {
            let deadline = if i % 2 == 0 {
                std::time::Instant::now() + Duration::from_millis(i)
            } else {
                std::time::Instant::now() - Duration::from_millis(i)
            };
            let delay = handle.delay(deadline);
            block_on(delay.compat()).unwrap();
        }
    }
}
