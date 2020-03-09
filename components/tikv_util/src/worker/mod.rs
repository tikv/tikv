// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/*!

`Worker` provides a mechanism to run tasks asynchronously (i.e. in the background) with some
additional features, for example, ticks.

A worker contains:

- A runner (which should implement the `Runnable` trait): to run tasks one by one or in batch.
- A scheduler: to send tasks to the runner, returns immediately.

Briefly speaking, this is a mpsc (multiple-producer-single-consumer) model.

*/

mod future;
mod metrics;

use crossbeam::channel::{RecvTimeoutError, TryRecvError, TrySendError};
use prometheus::IntGauge;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use std::{io, usize};

use self::metrics::*;
use crate::mpsc::{self, Receiver, Sender};
use crate::time::Instant;
use crate::timer::Timer;

pub use self::future::dummy_scheduler as dummy_future_scheduler;
pub use self::future::Runnable as FutureRunnable;
pub use self::future::Scheduler as FutureScheduler;
pub use self::future::{Stopped, Worker as FutureWorker};

#[derive(Eq, PartialEq)]
pub enum ScheduleError<T> {
    Stopped(T),
    Full(T),
}

impl<T> ScheduleError<T> {
    pub fn into_inner(self) -> T {
        match self {
            ScheduleError::Stopped(t) | ScheduleError::Full(t) => t,
        }
    }
}

impl<T> Error for ScheduleError<T> {
    fn description(&self) -> &str {
        match *self {
            ScheduleError::Stopped(_) => "channel has been closed",
            ScheduleError::Full(_) => "channel is full",
        }
    }
}

impl<T> Display for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl<T> Debug for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

pub trait Runnable<T: Display> {
    /// Runs a task.
    fn run(&mut self, _: T) {
        unimplemented!()
    }

    /// Runs a batch of tasks.
    ///
    /// Please note that ts will be clear after invoking this method.
    fn run_batch(&mut self, ts: &mut Vec<T>) {
        for t in ts.drain(..) {
            let task_str = format!("{}", t);
            let timer = Instant::now_coarse();
            self.run(t);
            slow_log!(timer.elapsed(), "handle task {}", task_str);
        }
    }

    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

pub trait RunnableWithTimer<T: Display, U>: Runnable<T> {
    fn on_timeout(&mut self, _: &mut Timer<U>, _: U);
}

struct DefaultRunnerWithTimer<R>(R);

impl<T: Display, R: Runnable<T>> Runnable<T> for DefaultRunnerWithTimer<R> {
    fn run(&mut self, t: T) {
        self.0.run(t)
    }
    fn run_batch(&mut self, ts: &mut Vec<T>) {
        self.0.run_batch(ts)
    }
    fn on_tick(&mut self) {
        self.0.on_tick()
    }
    fn shutdown(&mut self) {
        self.0.shutdown()
    }
}

impl<T: Display, R: Runnable<T>> RunnableWithTimer<T, ()> for DefaultRunnerWithTimer<R> {
    fn on_timeout(&mut self, _: &mut Timer<()>, _: ()) {}
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T> {
    name: Arc<String>,
    counter: Arc<AtomicUsize>,
    sender: Sender<Option<T>>,
    metrics_pending_task_count: IntGauge,
}

impl<T: Display> Scheduler<T> {
    fn new<S>(name: S, counter: AtomicUsize, sender: Sender<Option<T>>) -> Scheduler<T>
    where
        S: Into<String>,
    {
        let name = name.into();
        Scheduler {
            metrics_pending_task_count: WORKER_PENDING_TASK_VEC.with_label_values(&[&name]),
            name: Arc::new(name),
            counter: Arc::new(counter),
            sender,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped or number pending tasks exceeds capacity, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        debug!("scheduling task {}", task);
        if let Err(e) = self.sender.try_send(Some(task)) {
            match e {
                TrySendError::Disconnected(Some(t)) => return Err(ScheduleError::Stopped(t)),
                TrySendError::Full(Some(t)) => return Err(ScheduleError::Full(t)),
                _ => unreachable!(),
            }
        }
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.metrics_pending_task_count.inc();
        Ok(())
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::SeqCst) > 0
    }
}

impl<T> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            name: Arc::clone(&self.name),
            counter: Arc::clone(&self.counter),
            sender: self.sender.clone(),
            metrics_pending_task_count: self.metrics_pending_task_count.clone(),
        }
    }
}

/// Creates a scheduler that can't schedule any task.
///
/// Useful for test purpose.
pub fn dummy_scheduler<T: Display>() -> (Scheduler<T>, Receiver<Option<T>>) {
    let (tx, rx) = mpsc::unbounded::<Option<T>>();
    (
        Scheduler::new("dummy scheduler", AtomicUsize::new(0), tx),
        rx,
    )
}

#[derive(Copy, Clone)]
pub struct Builder<S: Into<String>> {
    name: S,
    batch_size: usize,
    pending_capacity: usize,
}

impl<S: Into<String>> Builder<S> {
    pub fn new(name: S) -> Self {
        Builder {
            name,
            batch_size: 1,
            pending_capacity: usize::MAX,
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Pending tasks won't exceed `pending_capacity`.
    pub fn pending_capacity(mut self, pending_capacity: usize) -> Self {
        self.pending_capacity = pending_capacity;
        self
    }

    pub fn create<T: Display>(self) -> Worker<T> {
        let (tx, rx) = if self.pending_capacity == usize::MAX {
            mpsc::unbounded::<Option<T>>()
        } else {
            mpsc::bounded::<Option<T>>(self.pending_capacity)
        };

        Worker {
            scheduler: Scheduler::new(self.name, AtomicUsize::new(0), tx),
            receiver: Mutex::new(Some(rx)),
            handle: None,
            batch_size: self.batch_size,
        }
    }
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    scheduler: Scheduler<T>,
    receiver: Mutex<Option<Receiver<Option<T>>>>,
    handle: Option<JoinHandle<()>>,
    batch_size: usize,
}

fn poll<R, T, U>(
    mut runner: R,
    rx: Receiver<Option<T>>,
    counter: Arc<AtomicUsize>,
    batch_size: usize,
    mut timer: Timer<U>,
) where
    R: RunnableWithTimer<T, U> + Send + 'static,
    T: Display + Send + 'static,
    U: Send + 'static,
{
    let current_thread = thread::current();
    let name = current_thread.name().unwrap();
    let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[name]);
    let metrics_handled_task_count = WORKER_HANDLED_TASK_VEC.with_label_values(&[name]);

    let mut batch = Vec::with_capacity(batch_size);
    let mut keep_going = true;
    let mut tick_time = None;
    while keep_going {
        tick_time = tick_time.or_else(|| timer.next_timeout());
        let timeout = tick_time.map(|t| t.checked_sub(Instant::now()).unwrap_or_default());

        keep_going = fill_task_batch(&rx, &mut batch, batch_size, timeout);
        if !batch.is_empty() {
            // batch will be cleared after `run_batch`, so we need to store its length
            // before `run_batch`.
            let batch_len = batch.len();
            runner.run_batch(&mut batch);
            counter.fetch_sub(batch_len, Ordering::SeqCst);
            metrics_pending_task_count.sub(batch_len as i64);
            metrics_handled_task_count.inc_by(batch_len as i64);
            batch.clear();
        }

        if tick_time.is_some() {
            let now = Instant::now();
            while let Some(task) = timer.pop_task_before(now) {
                runner.on_timeout(&mut timer, task);
                tick_time = None;
            }
        }
        runner.on_tick();
    }
    runner.shutdown();
}

/// Fills buffer with next task batch coming from `rx`.
fn fill_task_batch<T>(
    rx: &Receiver<Option<T>>,
    buffer: &mut Vec<T>,
    batch_size: usize,
    timeout: Option<Duration>,
) -> bool {
    let head_task = match timeout {
        Some(dur) => match rx.recv_timeout(dur) {
            Err(RecvTimeoutError::Timeout) => return true,
            Err(RecvTimeoutError::Disconnected) | Ok(None) => return false,
            Ok(Some(task)) => task,
        },
        None => match rx.recv() {
            Err(_) | Ok(None) => return false,
            Ok(Some(task)) => task,
        },
    };
    buffer.push(head_task);
    while buffer.len() < batch_size {
        match rx.try_recv() {
            Ok(Some(t)) => buffer.push(t),
            Err(TryRecvError::Empty) => return true,
            Err(_) | Ok(None) => return false,
        }
    }
    true
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Creates a worker.
    pub fn new<S: Into<String>>(name: S) -> Worker<T> {
        Builder::new(name).create()
    }

    /// Starts the worker.
    pub fn start<R: Runnable<T> + Send + 'static>(&mut self, runner: R) -> Result<(), io::Error> {
        let runner = DefaultRunnerWithTimer(runner);
        let timer: Timer<()> = Timer::new(0);
        self.start_with_timer(runner, timer)
    }

    pub fn start_with_timer<R, U>(&mut self, runner: R, timer: Timer<U>) -> Result<(), io::Error>
    where
        R: RunnableWithTimer<T, U> + Send + 'static,
        U: Send + 'static,
    {
        let mut receiver = self.receiver.lock().unwrap();
        info!("starting working thread"; "worker" => &self.scheduler.name);
        if receiver.is_none() {
            warn!("worker has been started"; "worker" => &self.scheduler.name);
            return Ok(());
        }

        let rx = receiver.take().unwrap();
        let counter = Arc::clone(&self.scheduler.counter);
        let batch_size = self.batch_size;
        let h = ThreadBuilder::new()
            .name(thd_name!(self.scheduler.name.as_ref()))
            .spawn(move || poll(runner, rx, counter, batch_size, timer))?;
        self.handle = Some(h);
        Ok(())
    }

    /// Gets a scheduler to schedule the task.
    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        self.scheduler.schedule(task)
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.handle.is_none() || self.scheduler.is_busy()
    }

    pub fn name(&self) -> &str {
        self.scheduler.name.as_str()
    }

    /// Stops the worker thread.
    pub fn stop(&mut self) -> Option<thread::JoinHandle<()>> {
        // Closes sender explicitly so the background thread will exit.
        info!("stoping worker"; "worker" => &self.scheduler.name);
        let handle = self.handle.take()?;
        if let Err(e) = self.scheduler.sender.send(None) {
            warn!("failed to stop worker thread"; "err" => ?e);
        }
        Some(handle)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    struct StepRunner {
        ch: mpsc::Sender<u64>,
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
        ch: mpsc::Sender<Vec<u64>>,
    }

    impl Runnable<u64> for BatchRunner {
        fn run_batch(&mut self, ms: &mut Vec<u64>) {
            self.ch.send(ms.to_vec()).unwrap();
        }

        fn shutdown(&mut self) {
            self.ch.send(vec![]).unwrap();
        }
    }

    struct TickRunner {
        ch: mpsc::Sender<&'static str>,
    }

    impl Runnable<&'static str> for TickRunner {
        fn run(&mut self, msg: &'static str) {
            self.ch.send(msg).unwrap();
        }
        fn on_tick(&mut self) {
            self.ch.send("tick msg").unwrap();
        }
        fn shutdown(&mut self) {
            self.ch.send("").unwrap();
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
        // task is handled before we update the busy status, so that we need some sleep.
        thread::sleep(Duration::from_millis(100));
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
            let result: u64 = v.into_iter().sum();
            sum += result;
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
        let mut worker = Builder::new("test-worker-tick").batch_size(4).create();
        for _ in 0..10 {
            worker.schedule("normal msg").unwrap();
        }
        let (tx, rx) = mpsc::channel();
        worker.start(TickRunner { ch: tx }).unwrap();
        for i in 0..13 {
            let msg = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if i != 4 && i != 9 && i != 12 {
                assert_eq!(msg, "normal msg");
            } else {
                assert_eq!(msg, "tick msg");
            }
        }
        worker.stop().unwrap().join().unwrap();
    }

    #[test]
    fn test_pending_capacity() {
        let mut worker = Builder::new("test-worker-busy")
            .batch_size(4)
            .pending_capacity(3)
            .create();
        let scheduler = worker.scheduler();

        for i in 0..3 {
            scheduler.schedule(i).unwrap();
        }
        assert_eq!(scheduler.schedule(3).unwrap_err(), ScheduleError::Full(3));

        let (tx, rx) = mpsc::channel();
        worker.start(BatchRunner { ch: tx }).unwrap();
        assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());

        worker.stop().unwrap().join().unwrap();
        drop(rx);
    }
}
