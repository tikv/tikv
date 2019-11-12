use crate::park::Parker;
use crate::scheduler::Scheduler;
use crate::task::ArcTask;

use crossbeam::deque::{Steal, Stealer, Worker as LocalQueue};
use crossbeam::sync::WaitGroup;
use prometheus::*;
use rand::prelude::*;
use tokio_timer::timer::Handle;

use std::sync::atomic::Ordering;
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
    pub after_start: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Worker {
    pub fn start(
        self,
        thread_builder: thread::Builder,
        level_run: [IntCounter; 3],
        level_stolen: [IntCounter; 7],
    ) {
        thread_builder
            .spawn(move || self.start_impl(level_run, level_stolen))
            .expect("start worker thread error");
    }

    fn start_impl(mut self, level_run: [IntCounter; 3], level_stolen: [IntCounter; 7]) {
        (self.after_start)();
        let mut rng = thread_rng();
        let _guard = tokio_timer::set_default(&self.timer_handle);
        self.timer_wg.take().expect("timer wg missing").wait();
        loop {
            let task = self.find_task(&mut rng, &level_stolen);
            let level = task.0.level.load(Ordering::SeqCst);
            level_run[level].inc();
            unsafe {
                task.poll();
            }
        }
    }

    fn find_task(&self, rng: &mut ThreadRng, level_stolen: &[IntCounter; 7]) -> ArcTask {
        if let Some(task) = self.local.pop() {
            level_stolen[5].inc();
            return task;
        }
        // TODO: experimenting now
        loop {
            // Local is empty, steal from injector
            let level = self.proportions.get_level(rng.next_u32());
            let mut step = 0;
            loop {
                match self
                    .scheduler
                    .injector(level)
                    .steal_batch_and_pop(&self.local)
                {
                    Steal::Success(task) => {
                        level_stolen[level].inc();
                        return task;
                    }
                    Steal::Retry => {
                        level_stolen[6].inc();
                        continue;
                    }
                    _ => match step {
                        0..=2 => {
                            std::thread::yield_now();
                            step += 1;
                        }
                        _ => {
                            break;
                        }
                    },
                }
            }
            // Fail to steal from expected injector, steal from other workers
            if !self.stealers.is_empty() {
                'outer: loop {
                    let i = rng.gen::<usize>() % self.stealers.len();
                    for j in 0..self.stealers.len() {
                        let idx = (i + j) % self.stealers.len();
                        match self.stealers[idx].steal_batch_and_pop(&self.local) {
                            Steal::Success(task) => {
                                level_stolen[3].inc();
                                return task;
                            }
                            Steal::Retry => {
                                level_stolen[6].inc();
                                break;
                            }
                            _ => break 'outer,
                        }
                    }
                }
            }
            // steal from other injectors
            for j in 0..3 {
                let idx = (level + j) % 3;
                match self.scheduler.injector(idx).steal() {
                    Steal::Success(task) => {
                        level_stolen[idx].inc();
                        return task;
                    }
                    _ => {}
                }
            }
            // Fail to find a task, sleep
            level_stolen[4].inc();
            self.parker.park(self.scheduler.sleepers());
        }
    }
}

const MAX_LEVEL0_PROPORTION: u32 = std::u32::MAX;

// 1/2
const MIN_LEVEL0_PROPORTION: u32 = 1 << 31;

// 31/32
const DEFAULT_LEVEL0_PROPORTION: u32 = (1 << 27) * ((1 << 5) - 1);

#[derive(Clone)]
pub struct Proportions([IntGauge; 2]);

impl Proportions {
    pub fn init(level_proportions: [IntGauge; 2]) -> Proportions {
        level_proportions[0].set(DEFAULT_LEVEL0_PROPORTION as i64);
        level_proportions[1].set((DEFAULT_LEVEL0_PROPORTION / 8 + (1 << 29) * 7) as i64);
        Proportions(level_proportions)
    }

    fn get_level(&self, rand_val: u32) -> usize {
        let level = if rand_val < self.0[0].get() as u32 {
            0
        } else if rand_val < self.0[1].get() as u32 {
            1
        } else {
            2
        };
        level
    }
}

// Update proportions interval
const UPDATE_PROPORTIONS_INTERVAL: Duration = Duration::from_secs(1);

pub async fn update_proportions(scheduler: Scheduler, proportions: Proportions) {
    let mut last_level_elapsed = [0u64; 3];
    loop {
        tokio_timer::delay_for(UPDATE_PROPORTIONS_INTERVAL).await;
        let new_level_elapsed = [
            scheduler.get_level_elapsed(0),
            scheduler.get_level_elapsed(1),
            scheduler.get_level_elapsed(2),
        ];
        let level_elapsed_diff = [
            new_level_elapsed[0] - last_level_elapsed[0],
            new_level_elapsed[1] - last_level_elapsed[1],
            new_level_elapsed[2] - last_level_elapsed[2],
        ];
        let total = (level_elapsed_diff[0] + level_elapsed_diff[1] + level_elapsed_diff[2]) as f64;
        let level0_percentage = level_elapsed_diff[0] as f64 / total;
        let old_proportions = [proportions.0[0].get() as u32, proportions.0[1].get() as u32];
        let mut new_proportions = old_proportions;
        if level0_percentage < 0.75 {
            new_proportions[0] = u32::min(
                ((u64::from(old_proportions[0]) + (1 << 32)) / 2) as u32,
                MAX_LEVEL0_PROPORTION,
            );
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 29) * 7;
            proportions.0[1].set(new_proportions[1] as i64);
            proportions.0[0].set(new_proportions[0] as i64);
        } else if level0_percentage > 0.85 {
            new_proportions[0] = u32::max(
                ((u64::from(old_proportions[0]) + (1 << 31)) / 2) as u32,
                MIN_LEVEL0_PROPORTION,
            );
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 29) * 7;
            proportions.0[0].set(new_proportions[0] as i64);
            proportions.0[1].set(new_proportions[1] as i64);
        }
        last_level_elapsed = new_level_elapsed;
    }
}
