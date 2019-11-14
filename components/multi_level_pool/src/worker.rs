use crate::metrics::*;
use crate::park::Parker;
use crate::scheduler::Scheduler;
use crate::task::ArcTask;
use crate::LEVEL_COUNT;

use crossbeam::deque::{Steal, Stealer, Worker as LocalQueue};
use crossbeam::sync::WaitGroup;
use init_with::InitWith;
use prometheus::*;
use rand::prelude::*;
use tokio_timer::timer::Handle;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// A worker thread for running `Future`s.
pub struct Worker {
    pub local: LocalQueue<ArcTask>,
    pub stealers: Vec<Stealer<ArcTask>>,
    pub scheduler: Scheduler,
    pub timer_handle: Handle,
    pub timer_wg: Option<WaitGroup>,
    pub parker: Parker,
    pub proportions: Proportions,
    pub task_source_count: TaskSourceCount,
    pub after_start: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Worker {
    pub fn start(self, thread_builder: thread::Builder) {
        thread_builder
            .spawn(move || self.start_impl())
            .expect("start worker thread error");
    }

    fn start_impl(mut self) {
        (self.after_start)();
        let mut rng = thread_rng();
        let _guard = tokio_timer::set_default(&self.timer_handle);
        self.timer_wg.take().expect("timer wg missing").wait();
        let mut step = 0;
        loop {
            if let Some(task) = self.find_task(&mut rng, &self.task_source_count) {
                unsafe {
                    task.poll();
                }
            } else {
                match step {
                    0..=2 => step += 1,
                    _ => {
                        self.parker.park(self.scheduler.sleepers());
                        step = 0;
                    }
                }
            }
        }
    }

    fn find_task(
        &self,
        rng: &mut ThreadRng,
        task_source_count: &TaskSourceCount,
    ) -> Option<ArcTask> {
        if let Some(task) = self.local.pop() {
            task_source_count.inc_local();
            return Some(task);
        }

        // Local is empty, steal from injector
        let level = self.proportions.get_level(rng.next_u32());
        loop {
            match self.scheduler.injectors()[level].steal_batch_and_pop(&self.local) {
                Steal::Success(task) => {
                    task_source_count.inc_injector(level);
                    return Some(task);
                }
                Steal::Retry => continue,
                _ => break,
            }
        }

        // Fail to steal from expected injector, steal from other workers
        if !self.stealers.is_empty() {
            // Steal starting from a random one
            let stealers = self
                .stealers
                .iter()
                .skip(rng.gen_range(0, self.stealers.len()))
                .chain(&self.stealers)
                .take(self.stealers.len());
            for stealer in stealers {
                loop {
                    match stealer.steal_batch_and_pop(&self.local) {
                        Steal::Success(task) => {
                            task_source_count.inc_steal();
                            return Some(task);
                        }
                        Steal::Retry => continue,
                        _ => break,
                    }
                }
            }
        }

        // Try to steal from all injectors.
        // It's not in batch to avoid get too many tasks from unexpected level.
        for (level, injector) in self.scheduler.injectors().iter().enumerate() {
            loop {
                match injector.steal() {
                    Steal::Success(task) => {
                        task_source_count.inc_injector(level);
                        return Some(task);
                    }
                    Steal::Retry => continue,
                    _ => break,
                }
            }
        }

        // Fail to find a task
        None
    }
}

const MAX_LEVEL0_PROPORTION: i64 = std::u32::MAX as i64;

// 1/2
const MIN_LEVEL0_PROPORTION: i64 = 1 << 31;

// 31/32
const DEFAULT_LEVEL0_PROPORTION: i64 = (1 << 27) * ((1 << 5) - 1);

const PROPORTIONS_COUNT: usize = LEVEL_COUNT - 1;

#[derive(Clone)]
pub struct Proportions([IntGauge; PROPORTIONS_COUNT]);

impl Proportions {
    pub fn new(name: &str) -> Proportions {
        let proportions = <[IntGauge; PROPORTIONS_COUNT]>::init_with_indices(|idx| {
            MULTI_LEVEL_POOL_PROPORTIONS.with_label_values(&[name, &format!("{}", idx)])
        });
        // TODO: support other level settings
        proportions[0].set(i64::from(DEFAULT_LEVEL0_PROPORTION));
        proportions[1].set(i64::from(DEFAULT_LEVEL0_PROPORTION / 8 + (1 << 29) * 7));
        Proportions(proportions)
    }

    fn get_level(&self, rand_val: u32) -> usize {
        for level in 0..PROPORTIONS_COUNT {
            if rand_val < self.0[level].get() as u32 {
                return level;
            }
        }
        LEVEL_COUNT - 1
    }
}

// Update proportions interval
const UPDATE_PROPORTIONS_INTERVAL: Duration = Duration::from_secs(1);

pub async fn update_proportions(scheduler: Scheduler, proportions: Proportions) {
    let mut last_level_elapsed = [0i64; LEVEL_COUNT];
    loop {
        tokio_timer::delay_for(UPDATE_PROPORTIONS_INTERVAL).await;
        let new_level_elapsed =
            <[i64; LEVEL_COUNT]>::init_with_indices(|level| scheduler.level_elapsed()[level].get());
        let level_elapsed_diff = <[i64; LEVEL_COUNT]>::init_with_indices(|level| {
            new_level_elapsed[level] - last_level_elapsed[level]
        });
        let total: i64 = level_elapsed_diff.iter().sum();
        let level0_percentage = level_elapsed_diff[0] as f64 / total as f64;

        let old_proportions =
            <[i64; PROPORTIONS_COUNT]>::init_with_indices(|idx| proportions.0[idx].get());
        let mut new_proportions = old_proportions;

        // TODO: support other level settings

        // TODO: Support setting level0 percentage. Currently 80% by default.
        if level0_percentage < 0.75 {
            new_proportions[0] = (old_proportions[0] + MAX_LEVEL0_PROPORTION) / 2;
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 29) * 7;
            proportions.0[1].set(new_proportions[1]);
            proportions.0[0].set(new_proportions[0]);
        } else if level0_percentage > 0.85 {
            new_proportions[0] = (old_proportions[0] + MIN_LEVEL0_PROPORTION) / 2;
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 29) * 7;
            proportions.0[0].set(new_proportions[0]);
            proportions.0[1].set(new_proportions[1]);
        }
        last_level_elapsed = new_level_elapsed;
    }
}
