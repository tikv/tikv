// Copyright 2016 PingCAP, Inc.
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


/// Worker contains all workers that do the expensive job in background.

mod metrics;
mod future;

use std::sync::{Arc, Mutex};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::io;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SendError, Sender, TryRecvError};
use std::error::Error;
use std::time::Duration;

use util::Either;
use util::time::{Instant, SlowTimer};
use self::metrics::*;

pub use self::future::Runnable as FutureRunnable;
pub use self::future::Scheduler as FutureScheduler;
pub use self::future::Worker as FutureWorker;

const NAP_SECS: u64 = 1;
const DEFAULT_TASKS_PER_TICK: usize = 10000;

pub struct Stopped<T>(pub T);

impl<T> Display for Stopped<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> Debug for Stopped<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> From<Stopped<T>> for Box<Error + Sync + Send + 'static> {
    fn from(_: Stopped<T>) -> Box<Error + Sync + Send + 'static> {
        box_err!("channel has been closed")
    }
}

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

pub trait BatchRunnable<T: Display> {
    /// Run a batch of tasks.
    ///
    /// Please note that ts will be clear after invoking this method.
    fn run_batch(&mut self, ts: &mut Vec<T>);
    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

impl<T: Display, R: Runnable<T>> BatchRunnable<T> for R {
    fn run_batch(&mut self, ts: &mut Vec<T>) {
        for t in ts.drain(..) {
            let task_str = format!("{}", t);
            let timer = SlowTimer::new();
            self.run(t);
            slow_log!(timer, "handle task {}", task_str);
        }
    }

    fn on_tick(&mut self) {
        Runnable::on_tick(self)
    }

    fn shutdown(&mut self) {
        Runnable::shutdown(self)
    }
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T> {
    name: Arc<String>,
    counter: Arc<AtomicUsize>,
    sender: Sender<Option<T>>,
}

impl<T: Display> Scheduler<T> {
    fn new<S: Into<String>>(
        name: S,
        counter: AtomicUsize,
        sender: Sender<Option<T>>,
    ) -> Scheduler<T> {
        Scheduler {
            name: Arc::new(name.into()),
            counter: Arc::new(counter),
            sender: sender,
        }
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        debug!("scheduling task {}", task);
        if let Err(SendError(Some(t))) = self.sender.send(Some(task)) {
            return Err(Stopped(t));
        }
        self.counter.fetch_add(1, Ordering::SeqCst);
        WORKER_PENDING_TASK_VEC
            .with_label_values(&[&self.name])
            .inc();
        Ok(())
    }

    /// Check if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::SeqCst) > 0
    }
}

impl<T: Display> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            name: self.name.clone(),
            counter: self.counter.clone(),
            sender: self.sender.clone(),
        }
    }
}

/// Create a scheduler that can't be scheduled any task.
///
/// Useful for test purpose.
#[cfg(test)]
pub fn dummy_scheduler<T: Display>() -> Scheduler<T> {
    let (tx, _) = mpsc::channel();
    Scheduler::new("dummy scheduler", AtomicUsize::new(0), tx)
}

#[derive(Copy, Clone)]
pub struct Builder<S: Into<String>> {
    name: S,
    batch_size: usize,
    tick_timeout: Duration,
    tasks_per_tick: usize,
}

impl<S: Into<String>> Builder<S> {
    pub fn new(name: S) -> Self {
        Builder {
            name: name,
            batch_size: 1,
            tick_timeout: Duration::from_secs(NAP_SECS),
            tasks_per_tick: DEFAULT_TASKS_PER_TICK,
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn tick_timeout(mut self, tick_dur: Duration) -> Self {
        self.tick_timeout = tick_dur;
        self
    }

    pub fn tasks_per_tick(mut self, tasks: usize) -> Self {
        self.tasks_per_tick = tasks;
        self
    }

    pub fn create<T: Display>(self) -> Worker<T> {
        let (tx, rx) = mpsc::channel::<Option<T>>();
        Worker {
            scheduler: Scheduler::new(self.name, AtomicUsize::new(0), tx),
            receiver: Mutex::new(Some(rx)),
            handle: None,
            batch_size: self.batch_size,
            tick_timeout: self.tick_timeout,
            tasks_per_tick: self.tasks_per_tick,
        }
    }
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    scheduler: Scheduler<T>,
    receiver: Mutex<Option<Receiver<Option<T>>>>,
    handle: Option<JoinHandle<()>>,
    batch_size: usize,
    tick_timeout: Duration,
    tasks_per_tick: usize,
}

fn poll<R, T>(
    mut runner: R,
    rx: Receiver<Option<T>>,
    counter: Arc<AtomicUsize>,
    batch_size: usize,
    tick_timeout: Duration,
    tasks_per_tick: usize,
) where
    R: BatchRunnable<T> + Send + 'static,
    T: Display + Send + 'static,
{
    let name = thread::current().name().unwrap().to_owned();
    let mut buffer = Vec::with_capacity(batch_size);
    let mut task_counter = 0;
    let mut keep_going = true;
    let (mut timeout, mut start) = (None, Instant::now_coarse());
    while keep_going {
        keep_going = fill_task_batch(&rx, &mut buffer, batch_size, timeout);
        if !buffer.is_empty() {
            counter.fetch_sub(buffer.len(), Ordering::SeqCst);
            WORKER_PENDING_TASK_VEC
                .with_label_values(&[&name])
                .sub(buffer.len() as f64);
            WORKER_HANDLED_TASK_VEC
                .with_label_values(&[&name])
                .inc_by(buffer.len() as f64)
                .unwrap();

            task_counter += buffer.len();
            runner.run_batch(&mut buffer);
            buffer.clear();
        }

        timeout = timeout.map_or(Some(tick_timeout), |dur| {
            let new_start = Instant::now_coarse();
            let elapsed = new_start.duration_since(start);
            start = new_start;
            dur.checked_sub(elapsed)
        });

        if timeout.is_none() || task_counter >= tasks_per_tick || !keep_going {
            timeout = None;
            runner.on_tick();
            task_counter = 0;
        }
    }
    runner.shutdown();
}

// Fill buffer with next task batch comes from `rx`.
fn fill_task_batch<T>(
    rx: &Receiver<Option<T>>,
    buffer: &mut Vec<T>,
    batch_size: usize,
    timeout: Option<Duration>,
) -> bool {
    let next_task = match timeout {
        Some(dur) => rx.recv_timeout(dur).map_err(Either::Left),
        None => rx.recv().map_err(Either::Right),
    };
    match next_task {
        Ok(Some(task)) => {
            buffer.push(task);
            while buffer.len() < batch_size {
                match rx.try_recv() {
                    Ok(Some(t)) => buffer.push(t),
                    Err(TryRecvError::Empty) => break,
                    Ok(None) | Err(_) => return false,
                }
            }
            true
        }
        Err(Either::Left(RecvTimeoutError::Timeout)) => true,
        _ => false,
    }
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Create a worker.
    pub fn new<S: Into<String>>(name: S) -> Worker<T> {
        Builder::new(name).create()
    }

    /// Start the worker.
    pub fn start<R>(&mut self, runner: R) -> Result<(), io::Error>
    where
        R: BatchRunnable<T> + Send + 'static,
    {
        let mut receiver = self.receiver.lock().unwrap();
        info!("starting working thread: {}", self.scheduler.name);
        if receiver.is_none() {
            warn!("worker {} has been started.", self.scheduler.name);
            return Ok(());
        }

        let rx = receiver.take().unwrap();
        let counter = self.scheduler.counter.clone();
        let batch_size = self.batch_size;
        let tick_dur = self.tick_timeout;
        let tasks_per_tick = self.tasks_per_tick;
        let h = ThreadBuilder::new()
            .name(thd_name!(self.scheduler.name.as_ref()))
            .spawn(move || {
                poll(runner, rx, counter, batch_size, tick_dur, tasks_per_tick)
            })?;
        self.handle = Some(h);
        Ok(())
    }

    /// Get a scheduler to schedule task.
    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        self.scheduler.schedule(task)
    }

    /// Check if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.handle.is_none() || self.scheduler.is_busy()
    }

    pub fn name(&self) -> &str {
        self.scheduler.name.as_str()
    }

    /// Stop the worker thread.
    pub fn stop(&mut self) -> Option<thread::JoinHandle<()>> {
        // close sender explicitly so the background thread will exit.
        info!("stoping {}", self.scheduler.name);
        if self.handle.is_none() {
            return None;
        }
        if let Err(e) = self.scheduler.sender.send(None) {
            warn!("failed to stop worker thread: {:?}", e);
        }
        self.handle.take()
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::sync::*;
    use std::sync::mpsc::*;
    use std::time::Duration;

    use super::*;

    struct StepRunner {
        ch: Sender<u64>,
    }

    impl Runnable<u64> for StepRunner {
        fn run(&mut self, step: u64) {
            self.ch.send(step).unwrap();
            thread::sleep(Duration::from_millis(step));
        }

        fn shutdown(&mut self) {
            self.ch.send(0).unwrap();
        }
    }

    struct BatchRunner {
        ch: Sender<Vec<u64>>,
    }

    impl BatchRunnable<u64> for BatchRunner {
        fn run_batch(&mut self, ms: &mut Vec<u64>) {
            self.ch.send(ms.to_vec()).unwrap();
        }

        fn shutdown(&mut self) {
            self.ch.send(vec![]).unwrap();
        }
    }

    struct TickRunner {
        ch: Sender<&'static str>,
    }

    impl Runnable<&'static str> for TickRunner {
        fn run(&mut self, msg: &'static str) {
            self.ch.send(msg).unwrap();
        }
        fn on_tick(&mut self) {
            self.ch.send("tick msg").unwrap();
        }
    }

    #[test]
    fn test_worker() {
        let mut worker = Worker::new("test-worker");
        let (tx, rx) = mpsc::channel();
        worker.start(StepRunner { ch: tx }).unwrap();
        assert!(!worker.is_busy());
        worker.schedule(60).unwrap();
        worker.schedule(40).unwrap();
        worker.schedule(50).unwrap();
        assert!(worker.is_busy());
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 60);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 40);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 50);
        assert!(!worker.is_busy());
        worker.stop().unwrap().join().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
        // when shutdown, StepRunner should send back a 0.
        assert_eq!(0, rx.recv().unwrap());
    }

    #[test]
    fn test_threaded() {
        let mut worker = Worker::new("test-worker-threaded");
        let (tx, rx) = mpsc::channel();
        worker.start(StepRunner { ch: tx }).unwrap();
        let scheduler = worker.scheduler();
        thread::spawn(move || {
            scheduler.schedule(90).unwrap();
            scheduler.schedule(110).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 90);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 110);
        worker.stop().unwrap().join().unwrap();
        assert_eq!(0, rx.recv().unwrap());
    }

    #[test]
    fn test_batch() {
        let mut worker = Builder::new("test-worker-batch").batch_size(10).create();
        let (tx, rx) = mpsc::channel();
        worker.start(BatchRunner { ch: tx }).unwrap();
        for _ in 0..20 {
            worker.schedule(50).unwrap();
        }
        worker.stop().unwrap().join().unwrap();
        let mut sum = 0;
        loop {
            let v = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            // when runner is shutdown, it will send back an empty vector.
            if v.is_empty() {
                break;
            }
            sum += v.into_iter().fold(0, |a, b| a + b);
        }
        assert_eq!(sum, 50 * 20);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn test_autowired_batch() {
        let mut worker = Builder::new("test-worker-batch").batch_size(10).create();
        let (tx, rx) = mpsc::channel();
        worker.start(StepRunner { ch: tx }).unwrap();
        for _ in 0..20 {
            worker.schedule(50).unwrap();
        }
        worker.stop().unwrap().join().unwrap();
        for _ in 0..20 {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        assert_eq!(rx.recv().unwrap(), 0);
    }

    #[test]
    fn test_on_tick() {
        let mut worker = Builder::new("test-worker-tick")
            .batch_size(4)
            .tick_timeout(Duration::from_secs(3))
            .tasks_per_tick(5)
            .create();
        for _ in 0..20 {
            worker.schedule("normal msg").unwrap();
        }
        thread::sleep(Duration::from_secs(3));

        let (tx, rx) = mpsc::channel();
        worker.start(TickRunner { ch: tx }).unwrap();

        // Because we produce "normal msg" quickly, so we won't meet timeout.
        // And because `tasks_per_tick` is 5, `batch_size` is 4, so we will
        // get a "tick msg" after we finish 2 batches.
        //
        // So the stream in rx should be: ^^^^^^^^o^^^^^^^^o^^^^.
        // '^' means normal message but 'o' means tick message.
        for i in 0..22 {
            let msg = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if i != 8 && i != 17 {
                assert_eq!(msg, "normal msg");
            } else {
                assert_eq!(msg, "tick msg");
            }
        }

        // tick message from `timeout`.
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), "tick msg");

        // tick message from `not keep_going`.
        worker.stop().unwrap().join().unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), "tick msg");
        assert!(rx.recv().is_err());
    }
}
