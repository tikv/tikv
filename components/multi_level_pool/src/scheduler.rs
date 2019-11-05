// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::park::Parker;
use crate::task::ArcTask;

use crossbeam::deque::Injector;
use crossbeam::queue::ArrayQueue;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    injectors: [Injector<ArcTask>; 3],
    level_elapsed: [AtomicU64; 3],
    sleepers: ArrayQueue<Parker>,
}

impl Scheduler {
    pub fn new(sleeper_capacity: usize) -> Self {
        let inner = SchedulerInner {
            injectors: [Injector::new(), Injector::new(), Injector::new()],
            level_elapsed: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
            sleepers: ArrayQueue::new(sleeper_capacity),
        };
        Scheduler(Arc::new(inner))
    }

    pub fn add_task(&self, task: ArcTask) {
        let stats = &task.0.task_stats;
        let elapsed = stats.elapsed.load(Ordering::SeqCst);
        let level = match elapsed {
            0..=4_999 => 0,
            5_000..=299_999 => 1,
            _ => 2,
        };
        self.0.injectors[level].push(task);
        if let Ok(parker) = self.0.sleepers.pop() {
            parker.unpark();
        }
    }

    pub fn add_level_elapsed(&self, level: usize, elapsed: u64) {
        self.0.level_elapsed[level].fetch_add(elapsed, Ordering::SeqCst);
    }

    pub fn injector(&self, level: usize) -> &Injector<ArcTask> {
        &self.0.injectors[level]
    }

    pub fn sleepers(&self) -> &ArrayQueue<Parker> {
        &self.0.sleepers
    }
}
