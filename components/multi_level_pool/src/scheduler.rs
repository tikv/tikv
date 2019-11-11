// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::park::Parker;
use crate::task::ArcTask;

use crossbeam::deque::Injector;
use crossbeam::queue::ArrayQueue;
use prometheus::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// `Scheduler` is responsible for adding new tasks into the thread pool injectors.
#[derive(Clone)]
pub struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    injectors: [Injector<ArcTask>; 3],
    level_elapsed: [IntCounter; 3],
    sleepers: ArrayQueue<Parker>,
}

impl Scheduler {
    pub fn new(sleeper_capacity: usize, level_elapsed: [IntCounter; 3]) -> Self {
        let inner = SchedulerInner {
            injectors: [Injector::new(), Injector::new(), Injector::new()],
            level_elapsed,
            sleepers: ArrayQueue::new(sleeper_capacity),
        };
        Scheduler(Arc::new(inner))
    }

    pub fn add_task(&self, task: ArcTask) {
        let level = task.0.fixed_level.unwrap_or_else(|| {
            let stats = &task.0.task_stats;
            let elapsed = stats.elapsed.load(Ordering::SeqCst);
            match elapsed {
                0..=999 => 0,
                1_000..=99_999 => 1,
                _ => 2,
            }
        });
        self.0.injectors[level].push(task);
        if let Ok(parker) = self.0.sleepers.pop() {
            parker.unpark();
        }
    }

    pub fn add_level_elapsed(&self, level: usize, elapsed: u64) {
        self.0.level_elapsed[level].inc_by(elapsed as i64);
    }

    pub fn get_level_elapsed(&self, level: usize) -> u64 {
        self.0.level_elapsed[level].get() as u64
    }

    pub fn injector(&self, level: usize) -> &Injector<ArcTask> {
        &self.0.injectors[level]
    }

    pub fn sleepers(&self) -> &ArrayQueue<Parker> {
        &self.0.sleepers
    }
}
