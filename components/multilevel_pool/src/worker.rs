use crate::park::Parker;
use crate::stats::{StatsMap, TaskStats};
use crate::task::{ArcTask, Task};

use crossbeam::deque::{Injector, Steal, Stealer, Worker as LocalQueue};
use prometheus::{IntCounter, IntGauge};
use rand::prelude::*;

use std::sync::Arc;
use std::thread;

pub struct Worker {
    pub local: LocalQueue<ArcTask>,
    pub stealers: Vec<Stealer<ArcTask>>,
    pub injectors: Arc<[Injector<ArcTask>]>,
    pub after_start: Arc<dyn Fn() + Send + Sync + 'static>,
    pub parker: Parker,
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
                poll_task(task);
            } else {
                match step {
                    0..=2 => {
                        std::thread::yield_now();
                        step += 1;
                    }
                    _ => {
                        self.parker.park();
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
            let i = rng.gen::<usize>() % 3;
            for j in 0..3 {
                let idx = (i + j) % 3;
                match self.injectors[idx].steal_batch_and_pop(&self.local) {
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

fn poll_task(task: ArcTask) {
    unsafe { task.poll() }
}
