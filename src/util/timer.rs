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

use std::cmp::{max, Ordering};
use std::time::Duration;
use util::time::Instant;
use std::collections::BinaryHeap;

struct TimeoutTask<T> {
    next_tick: Instant,
    timeout: Duration,
    task: T,
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
    tick: Duration,
    pending: BinaryHeap<TimeoutTask<T>>,
    delivered: Vec<TimeoutTask<T>>,
}

impl<T> Timer<T> {
    fn align_instant(&self, ins: Instant) -> Instant {
        let mut next = self.now + self.tick;
        loop {
            if next < ins {
                next += self.tick;
                continue;
            }
            break next;
        }
    }

    /// Create a new timer with a specified `tick` which means
    /// the min timeout when the `Timer` calls `next_timeout`.
    pub fn new(tick: Duration) -> Self {
        Timer {
            now: Instant::now_coarse(),
            tick: tick,
            pending: BinaryHeap::new(),
            delivered: Vec::new(),
        }
    }

    /// Add a periodic task into the `Timer`. The `timeout` will
    /// be aligned up to `tick` of the `Timer` if need.
    pub fn add_task(&mut self, mut timeout: Duration, task: T) {
        timeout = max(timeout, self.tick);
        let task = TimeoutTask {
            next_tick: self.now + timeout,
            timeout: timeout,
            task: task,
        };
        self.pending.push(task);
    }

    /// Get the next `timeout` from the timer. After it's called and
    /// the `timeout` elapsed, we should call `run_delivered_tasks` to
    /// consume triggered tasks.
    pub fn next_timeout(&mut self) -> Option<Duration> {
        assert!(self.delivered.is_empty());
        if let Some(timeout_task) = self.pending.pop() {
            let next_tick = self.align_instant(timeout_task.next_tick);
            self.delivered.push(timeout_task);
            while let Some(timeout_task) = self.pending.pop() {
                if timeout_task.next_tick > next_tick {
                    break;
                }
                self.delivered.push(timeout_task);
            }
            return Some(next_tick - Instant::now_coarse());
        }
        None
    }

    /// Run all delivered tasks. if the `run` returns true,
    /// the task will be put into the `Timer` again.
    pub fn run_delivered_tasks<F: FnMut(&mut T) -> bool>(&mut self, run: &mut F) {
        for mut timeout_task in self.delivered.drain(..) {
            if run(&mut timeout_task.task) {
                timeout_task.next_tick += timeout_task.timeout;
                self.pending.push(timeout_task);
            }
        }
    }
}
