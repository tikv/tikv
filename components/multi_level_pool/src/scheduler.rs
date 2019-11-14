// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crate::park::Parker;
use crate::task::ArcTask;
use crate::LEVEL_COUNT;

use crossbeam::deque::Injector;
use crossbeam::queue::ArrayQueue;
use init_with::InitWith;
use prometheus::*;

use std::sync::Arc;

/// `Scheduler` is responsible for adding new tasks into the thread pool injectors.
#[derive(Clone)]
pub struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    injectors: [Injector<ArcTask>; LEVEL_COUNT],
    level_elapsed: [IntCounter; LEVEL_COUNT],
    level_poll_times: [IntCounter; LEVEL_COUNT],
    sleepers: ArrayQueue<Parker>,
}

impl Scheduler {
    pub fn new(name: &str, sleeper_capacity: usize) -> Self {
        let inner = SchedulerInner {
            injectors: <[Injector<ArcTask>; LEVEL_COUNT]>::init_with(|| Injector::new()),
            level_elapsed: <[IntCounter; LEVEL_COUNT]>::init_with_indices(|idx| {
                MULTI_LEVEL_POOL_LEVEL_ELAPSED.with_label_values(&[name, &format!("{}", idx)])
            }),
            level_poll_times: <[IntCounter; LEVEL_COUNT]>::init_with_indices(|idx| {
                MULTI_LEVEL_POOL_LEVEL_POLL_TIMES.with_label_values(&[name, &format!("{}", idx)])
            }),
            sleepers: ArrayQueue::new(sleeper_capacity),
        };
        Scheduler(Arc::new(inner))
    }

    pub fn add_task(&self, task: ArcTask) {
        let level = task.fixed_level().unwrap_or_else(|| {
            let elapsed_micros = task.elapsed_micros();
            // TODO: support other level settings
            match elapsed_micros {
                0..=999 => 0,
                1_000..=99_999 => 1,
                _ => 2,
            }
        });
        task.set_level(level);
        self.0.injectors[level].push(task);
        if let Ok(parker) = self.0.sleepers.pop() {
            parker.unpark();
        }
    }

    pub fn add_level_elapsed(&self, level: usize, elapsed: i64) {
        self.0.level_elapsed[level].inc_by(elapsed);
        self.0.level_poll_times[level].inc();
    }

    pub fn level_elapsed(&self) -> &[IntCounter; LEVEL_COUNT] {
        &self.0.level_elapsed
    }

    pub fn injectors(&self) -> &[Injector<ArcTask>; LEVEL_COUNT] {
        &self.0.injectors
    }

    pub fn sleepers(&self) -> &ArrayQueue<Parker> {
        &self.0.sleepers
    }
}
