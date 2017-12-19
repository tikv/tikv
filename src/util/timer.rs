// Copyright 2017 PingCAP, Inc.
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

use std::cmp::Ordering;
use std::time::{Duration, Instant};
use std::collections::BinaryHeap;

/// A empty task for keeping compatible.
pub struct EmptyTask;

pub struct TimeoutTask<T> {
    next_tick: Instant,
    timeout: Duration,
    task: T,
}

impl<T> TimeoutTask<T> {
    pub fn into_inner(self) -> T {
        self.task
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl<T> AsRef<T> for TimeoutTask<T> {
    fn as_ref(&self) -> &T {
        &self.task
    }
}

impl<T> AsMut<T> for TimeoutTask<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.task
    }
}

impl<T> PartialEq for TimeoutTask<T> {
    fn eq(&self, other: &TimeoutTask<T>) -> bool {
        if self.next_tick == other.next_tick {
            return true;
        }
        false
    }
}

impl<T> Eq for TimeoutTask<T> {}

impl<T> PartialOrd for TimeoutTask<T> {
    fn partial_cmp(&self, other: &TimeoutTask<T>) -> Option<Ordering> {
        Some(if self.next_tick > other.next_tick {
            Ordering::Less
        } else if self.next_tick < other.next_tick {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }
}

impl<T> Ord for TimeoutTask<T> {
    fn cmp(&self, other: &TimeoutTask<T>) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct Timer<T> {
    now: Instant,
    pending: BinaryHeap<TimeoutTask<T>>,
}

impl<T> Timer<T> {
    pub fn new(capacity: usize) -> Self {
        Timer {
            now: Instant::now(),
            pending: BinaryHeap::with_capacity(capacity),
        }
    }

    /// Add a periodic task into the `Timer`. The `timeout` will
    /// be aligned up to `tick` of the `Timer` if need.
    pub fn add_task(&mut self, timeout: Duration, task: T) {
        let task = TimeoutTask {
            next_tick: Instant::now() + timeout,
            timeout: timeout,
            task: task,
        };
        self.pending.push(task);
    }

    /// Get the next `timeout` from the timer, and fill `delivered`
    /// with tasks will be triggered after `timeout` elapsed.
    /// If there is no pending tasks, `delivered` won't be changed.
    pub fn next_timeout(&mut self, delivered: &mut Vec<TimeoutTask<T>>) -> Option<Duration> {
        if let Some(timeout_task) = self.pending.pop() {
            let tick_time = timeout_task.next_tick;
            delivered.push(timeout_task);
            while let Some(timeout_task) = self.pending.pop() {
                if timeout_task.next_tick > tick_time {
                    self.pending.push(timeout_task);
                    break;
                }
                delivered.push(timeout_task);
            }
            self.now = Instant::now();
            if self.now > tick_time {
                return Some(Duration::default());
            }
            return Some(tick_time.duration_since(self.now));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, Sender};
    use util::worker::{Builder as WorkerBuilder, Runnable};

    #[derive(Debug, PartialEq, Eq, Copy, Clone)]
    enum Task {
        A,
        B,
        C,
    }

    struct Runner {
        ch: Sender<&'static str>,
    }

    impl Runnable<&'static str> for Runner {
        fn run(&mut self, msg: &'static str) {
            self.ch.send(msg).unwrap();
        }
        fn shutdown(&mut self) {
            self.ch.send("").unwrap();
        }
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new(10);
        let mut events = Vec::new();
        timer.add_task(Duration::from_millis(20), Task::A);
        timer.add_task(Duration::from_millis(150), Task::C);
        timer.add_task(Duration::from_millis(100), Task::B);
        assert_eq!(timer.pending.len(), 3);

        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_ref(), &Task::A);

        events.clear();
        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_ref(), &Task::B);

        for event in events.drain(..) {
            timer.add_task(event.timeout(), event.into_inner());
        }

        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_ref(), &Task::B);

        events.clear();
        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_ref(), &Task::C);
    }

    #[test]
    fn test_worker_with_timer() {
        let mut worker = WorkerBuilder::new("test-worker-with-timer").create();
        for _ in 0..10 {
            worker.schedule("normal msg").unwrap();
        }

        let (tx, rx) = mpsc::channel();
        let runner = Runner { ch: tx.clone() };

        let mut counter = 0;
        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(200), Task::A);
        timer.add_task(Duration::from_millis(300), Task::B);

        worker
            .start_with_timer(runner, Some(timer), move |ref mut timer, timeout_task| {
                match *timeout_task.as_ref() {
                    Task::A => tx.send("task a").unwrap(),
                    Task::B => tx.send("task b").unwrap(),
                    _ => unreachable!(),
                };
                counter += 1;
                if counter <= 2 {
                    timer.add_task(timeout_task.timeout(), timeout_task.into_inner());
                }
            })
            .unwrap();

        for i in 0..14 {
            let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            if i == 1 || i == 3 || i == 5 {
                assert_eq!(msg, "task a");
            } else if i == 7 {
                assert_eq!(msg, "task b");
            } else {
                assert_eq!(msg, "normal msg");
            }
        }
        worker.stop().unwrap().join().unwrap();
    }
}
