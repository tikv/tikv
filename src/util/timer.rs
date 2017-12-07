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

struct Timeout<T> {
    tick: Instant,
    timeout: Duration,
    task: T,
}

impl<T> PartialEq for Timeout<T> {
    fn eq(&self, other: &Timeout<T>) -> bool {
        if self.tick == other.tick {
            return true;
        }
        false
    }
}

impl<T> Eq for Timeout<T> {}

impl<T> PartialOrd for Timeout<T> {
    fn partial_cmp(&self, other: &Timeout<T>) -> Option<Ordering> {
        Some(if self.tick > other.tick {
            Ordering::Less
        } else if self.tick < other.tick {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }
}

impl<T> Ord for Timeout<T> {
    fn cmp(&self, other: &Timeout<T>) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct Timer<T: Copy> {
    timers: BinaryHeap<Timeout<T>>,
}

impl<T: Copy> Timer<T> {
    pub fn new() -> Self {
        Timer {
            timers: BinaryHeap::new(),
        }
    }

    pub fn add_timeout(&mut self, timeout: Duration, task: T) {
        let timeout = Timeout {
            tick: Instant::now() + timeout,
            timeout: timeout,
            task: task,
        };
        self.timers.push(timeout);
    }

    pub fn next_timeout<F>(&mut self, mut keep_tick: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        let now = Instant::now();
        let could_pop = match self.timers.peek() {
            Some(t) => t.tick <= now,
            None => return None,
        };
        if could_pop {
            let mut timeout = self.timers.pop().unwrap();
            let task = timeout.task;
            if keep_tick(&task) {
                while timeout.tick <= now {
                    timeout.tick += timeout.timeout;
                }
                self.timers.push(timeout);
            }
            return Some(task);
        }
        None
    }
}
