use crate::park::Parker;
use crate::scheduler::Scheduler;
use crate::task::ArcTask;

use crossbeam::deque::{Steal, Stealer, Worker as LocalQueue};
use rand::prelude::*;

use std::sync::Arc;
use std::thread;

pub struct Worker {
    pub local: LocalQueue<ArcTask>,
    pub stealers: Vec<Stealer<ArcTask>>,
    pub scheduler: Scheduler,
    pub parker: Parker,
    pub after_start: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Worker {
    pub fn start(self, thread_builder: thread::Builder) {
        thread_builder
            .spawn(move || self.start_impl())
            .expect("start worker thread error");
    }

    fn start_impl(self) {
        (self.after_start)();
        let mut rng = thread_rng();
        let mut step = 0;
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
            let i = match rng.next_u32() {
                0..=4160749567 => 0,
                4160749568..=4261412863 => 1,
                _ => 2,
            };
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
        None
    }
}
