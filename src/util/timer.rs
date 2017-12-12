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
use std::time::Duration;
use util::time::Instant;
use std::collections::BinaryHeap;

pub struct TimeoutTask<T> {
    next_tick: Instant,
    timeout: Duration,
    task: T,
}

impl<T> TimeoutTask<T> {
    pub fn restore(self, timer: &mut Timer<T>) {
        timer.pending.push(self);
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
    tick: Duration,
    pending: BinaryHeap<TimeoutTask<T>>,
}

impl<T> Timer<T> {
    fn align_up_instant(&self, ins: Instant) -> Instant {
        let mut next = self.now + self.tick;
        let now = Instant::now_coarse();
        loop {
            if next < ins || next < now {
                next += self.tick;
                continue;
            }
            break next;
        }
    }

    fn deliver(&self, mut task: TimeoutTask<T>, buffer: &mut Vec<TimeoutTask<T>>) {
        task.next_tick = self.align_up_instant(task.next_tick + task.timeout);
        buffer.push(task);
    }

    /// Create a new timer with a specified `tick` which means
    /// the min timeout when the `Timer` calls `next_timeout`.
    pub fn new(tick: Duration) -> Self {
        Timer {
            now: Instant::now_coarse(),
            tick: tick,
            pending: BinaryHeap::new(),
        }
    }

    /// Add a periodic task into the `Timer`. The `timeout` will
    /// be aligned up to `tick` of the `Timer` if need.
    pub fn add_task(&mut self, timeout: Duration, task: T) {
        let task = TimeoutTask {
            next_tick: self.now + timeout,
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
            let next_tick = self.align_up_instant(timeout_task.next_tick);
            self.deliver(timeout_task, delivered);
            while let Some(timeout_task) = self.pending.pop() {
                if timeout_task.next_tick > next_tick {
                    self.pending.push(timeout_task);
                    break;
                }
                self.deliver(timeout_task, delivered);
            }
            self.now = next_tick;
            // TODO: Should use checked sub.
            return Some(next_tick - Instant::now_coarse());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Task {
        A,
        B,
        C,
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new(Duration::from_millis(100));
        timer.add_task(Duration::from_millis(20), Task::A);
        timer.add_task(Duration::from_millis(150), Task::C);
        timer.add_task(Duration::from_millis(100), Task::B);
        assert_eq!(timer.pending.len(), 3);

        let mut events = Vec::new();
        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].as_ref(), &Task::A);
        assert_eq!(events[1].as_ref(), &Task::B);
        for event in events {
            event.restore(&mut timer);
        }

        let mut events = Vec::new();
        assert!(timer.next_timeout(&mut events).is_some());
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].as_ref(), &Task::A);
        assert_eq!(events[1].as_ref(), &Task::C);
        assert_eq!(events[2].as_ref(), &Task::B);
    }
}
