// Copyright 2016 PingCAP, Inc.
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

use std::time::{SystemTime, Duration, Instant};
use std::thread::{self, JoinHandle, Builder};
use std::sync::mpsc::{self, Sender};

/// Convert Duration to milliseconds.
#[inline]
pub fn duration_to_ms(d: Duration) -> u64 {
    let nanos = d.subsec_nanos() as u64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

/// Convert Duration to seconds.
#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = d.subsec_nanos() as f64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

/// Convert Duration to nanoseconds.
#[inline]
pub fn duration_to_nanos(d: Duration) -> u64 {
    let nanos = d.subsec_nanos() as u64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000_000_000 + nanos
}

pub struct SlowTimer {
    slow_time: Duration,
    t: Instant,
}

impl SlowTimer {
    pub fn new() -> SlowTimer {
        SlowTimer::default()
    }

    pub fn from(slow_time: Duration) -> SlowTimer {
        SlowTimer {
            slow_time: slow_time,
            t: Instant::now(),
        }
    }

    pub fn from_secs(secs: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_secs(secs))
    }

    pub fn from_millis(millis: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_millis(millis))
    }

    pub fn elapsed(&self) -> Duration {
        self.t.elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.elapsed() >= self.slow_time
    }
}

const DEFAULT_SLOW_SECS: u64 = 1;

impl Default for SlowTimer {
    fn default() -> SlowTimer {
        SlowTimer::from_secs(DEFAULT_SLOW_SECS)
    }
}

const DEFAULT_WAIT_MS: u64 = 100;

pub struct Monitor {
    tx: Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl Monitor {
    pub fn new<D, N>(on_jumped: D, now: N) -> Monitor
        where D: Fn() + Send + 'static,
              N: Fn() -> SystemTime + Send + 'static
    {
        let (tx, rx) = mpsc::channel();
        let h = Builder::new()
            .name(thd_name!("time-monitor-worker"))
            .spawn(move || {
                while let Err(_) = rx.try_recv() {
                    let before = now();
                    thread::sleep(Duration::from_millis(DEFAULT_WAIT_MS));

                    let after = now();
                    if let Err(e) = after.duration_since(before) {
                        error!("system time jumped back, {:?} -> {:?}, err {:?}",
                               before,
                               after,
                               e);
                        on_jumped()
                    }
                }
            })
            .unwrap();

        Monitor {
            tx: tx,
            handle: Some(h),
        }
    }
}

impl Default for Monitor {
    fn default() -> Monitor {
        Monitor::new(|| {}, SystemTime::now)
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Err(e) = self.tx.send(true) {
            error!("send quit message for time monitor worker failed {:?}", e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed {:?}", e);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, Duration};
    use std::thread;
    use std::ops::Sub;
    use std::f64;
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_time_monitor() {
        let jumped = Arc::new(AtomicBool::new(false));
        let triggered = AtomicBool::new(false);
        let now = move || {
            if !triggered.load(Ordering::SeqCst) {
                triggered.store(true, Ordering::SeqCst);
                SystemTime::now()
            } else {
                SystemTime::now().sub(Duration::from_secs(2))
            }
        };

        let jumped2 = jumped.clone();
        let on_jumped = move || {
            jumped2.store(true, Ordering::SeqCst);
        };

        let _m = Monitor::new(on_jumped, now);
        thread::sleep(Duration::from_secs(1));

        assert_eq!(jumped.load(Ordering::SeqCst), true);
    }

    #[test]
    fn test_duration_to() {
        let tbl = vec![0, 100, 1_000, 5_000, 9999, 1_000_000, 1_000_000_000];
        for ms in tbl {
            let d = Duration::from_millis(ms);
            assert_eq!(ms, duration_to_ms(d));
            let exp_sec = ms as f64 / 1000.0;
            let act_sec = duration_to_sec(d);
            assert!((act_sec - exp_sec).abs() < f64::EPSILON);
            assert_eq!(ms * 1_000_000, duration_to_nanos(d));
        }
    }
}
