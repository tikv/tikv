use crate::park::Parker;
use crate::scheduler::Scheduler;
use crate::task::ArcTask;

use crossbeam::deque::{Steal, Stealer, Worker as LocalQueue};
use crossbeam::sync::WaitGroup;
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
        let mut step = 0;
        let _guard = tokio_timer::set_default(&self.timer_handle);
        self.timer_wg.take().expect("timer wg missing").wait();
        loop {
            if let Some(task) = self.find_task(&mut rng) {
                step = 0;
                unsafe {
                    task.poll();
                }
            } else {
                match step {
                    0..=2 => {
                        std::thread::yield_now();
                        step += 1;
                    }
                    _ => {
                        self.parker.park(self.scheduler.sleepers());
                        step = 0;
                    }
                }
            }
        }
    }

    fn find_task(&self, rng: &mut ThreadRng) -> Option<ArcTask> {
        if let Some(task) = self.local.pop() {
            return Some(task);
        }
        let mut retry = true;
        while retry {
            retry = false;
            // Local is empty, steal from injector
            let i = self.proportions.get_level(rng.next_u32());
            for j in 0..3 {
                let idx = (i + j) % 3;
                match self
                    .scheduler
                    .injector(idx)
                    .steal_batch_and_pop(&self.local)
                {
                    Steal::Success(task) => {
                        return Some(task);
                    }
                    Steal::Retry => retry = true,
                    _ => {}
                }
            }
            // Fail to steal from injectors, steal from others
            if !self.stealers.is_empty() {
                let i = rng.gen::<usize>() % self.stealers.len();
                for j in 0..self.stealers.len() {
                    let idx = (i + j) % self.stealers.len();
                    match self.stealers[idx].steal_batch_and_pop(&self.local) {
                        Steal::Success(task) => {
                            return Some(task);
                        }
                        Steal::Retry => retry = true,
                        _ => {}
                    }
                }
            }
        }
        None
    }
}

// 127/128
const MAX_LEVEL0_PROPORTION: u32 = (1 << 25) * ((1 << 7) - 1);

// 1/2
const MIN_LEVEL0_PROPORTION: u32 = 1 << 16;

// 31/32
const DEFAULT_LEVEL0_PROPORTION: u32 = (1 << 27) * ((1 << 5) - 1);

#[derive(Clone)]
pub struct Proportions([IntGauge; 2]);

impl Proportions {
    pub fn init(level_proportions: [IntGauge; 2]) -> Proportions {
        level_proportions[0].set(DEFAULT_LEVEL0_PROPORTION as i64);
        level_proportions[1].set((DEFAULT_LEVEL0_PROPORTION / 8 + (1 << 24) * 7) as i64);
        Proportions(level_proportions)
    }

    fn get_level(&self, rand_val: u32) -> usize {
        if rand_val < self.0[0].get() as u32 {
            0
        } else if rand_val < self.0[1].get() as u32 {
            1
        } else {
            2
        }
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
            new_proportions[0] = u32::max(
                ((u64::from(old_proportions[0]) + (1 << 32)) / 2) as u32,
                MAX_LEVEL0_PROPORTION,
            );
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 24) * 7;
            proportions.0[1].set(new_proportions[1] as i64);
            proportions.0[0].set(new_proportions[0] as i64);
        } else if level0_percentage > 0.85 {
            new_proportions[0] = u32::max(
                ((u64::from(old_proportions[0]) + (1 << 31)) / 2) as u32,
                MIN_LEVEL0_PROPORTION,
            );
            // level 1 : level 2 = 7 : 1
            new_proportions[1] = new_proportions[0] / 8 + (1 << 24) * 7;
            proportions.0[0].set(new_proportions[0] as i64);
            proportions.0[1].set(new_proportions[1] as i64);
        }
        last_level_elapsed = new_level_elapsed;
    }
}
