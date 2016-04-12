/// Worker contains all workers that do the expensive job in background.


use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle, Builder};
use std::collections::VecDeque;
use std::time::Duration;
use std::io;
use std::fmt::Display;
use std::time::Instant;

use util::HandyRwLock;

const TASK_PEEK_INTERVAL_SECS: u64 = 100;

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    name: String,
    queue: Arc<RwLock<VecDeque<T>>>,
    handle: Option<JoinHandle<()>>,
    stopped: Arc<RwLock<bool>>,
}

impl<T: Display + Send + Sync + 'static> Worker<T> {
    pub fn new(name: String) -> Worker<T> {
        Worker {
            name: name,
            queue: Arc::new(RwLock::new(VecDeque::new())),
            handle: None,
            stopped: Arc::new(RwLock::new(false)),
        }
    }

    pub fn start<R: Runnable<T> + Send + 'static>(&mut self, mut runner: R) -> io::Result<()> {
        info!("starting working thread: {}", self.name);
        let stopped = self.stopped.clone();
        let queue = self.queue.clone();

        let res = Builder::new().name(self.name.clone()).spawn(move || {
            loop {
                let t = queue.wl().pop_front();
                if t.is_none() {
                    // TODO find or implement a concurrent deque instead.
                    thread::sleep(Duration::from_millis(TASK_PEEK_INTERVAL_SECS));
                    if *stopped.rl() {
                        break;
                    }
                    continue;
                }
                let t = t.unwrap();
                let task_str = format!("{}", t);
                let timer = Instant::now();
                runner.run(t);
                info!("task {} takes {:?} to finish.", task_str, timer.elapsed());
            }
        });
        let h = try!(res);
        self.handle = Some(h);
        Ok(())
    }

    pub fn schedule<Iter: IntoIterator<Item = T>>(&self, tasks: Iter) {
        self.queue.wl().extend(tasks);
        debug!("after scheduling we have {} pending tasks",
               self.queue.rl().len());
    }

    pub fn is_busy(&self) -> bool {
        !self.queue.rl().is_empty()
    }

    pub fn stop(&mut self) -> thread::Result<()> {
        *self.stopped.wl() = true;
        if let Some(h) = self.handle.take() {
            try!(h.join());
        }
        Ok(())
    }
}

mod snap;
mod split_check;

pub use self::snap::{Task as SnapTask, Runner as SnapRunner};
pub use self::split_check::{Task as SplitCheckTask, Runner as SplitCheckRunner};
