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

use crossbeam::channel::TrySendError;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;

use crate::mpsc::{self, Receiver, Sender};
use crate::timer::GLOBAL_TIMER_HANDLE;

pub use self::future::dummy_scheduler as dummy_future_scheduler;
pub use self::future::Runnable as FutureRunnable;
pub use self::future::Scheduler as FutureScheduler;
pub use self::future::{Stopped, Worker as FutureWorker};
pub use crate::future::poll_future_notify;
use crate::futures::Future;

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

impl<T> Error for ScheduleError<T> {}

impl<T> Display for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let msg = match *self {
            ScheduleError::Stopped(_) => "channel has been closed",
            ScheduleError::Full(_) => "channel is full",
        };
        write!(f, "{}", msg)
    }
}

impl<T> Debug for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

pub trait Runnable<T: Display> {
    fn register_scheduler(&mut self, _: Scheduler<T>) {}

    /// Runs a task.
    fn run(&mut self, _: T) {
        unimplemented!()
    }

    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

pub trait SharedRunnable: Send {
    fn handle(&mut self);
}

pub struct SharedRunner<T: Display + Send + 'static, R: Runnable<T> + 'static> {
    runner: R,
    receiver: Receiver<T>,
    counter: Arc<AtomicUsize>,
}

impl<T, R> SharedRunnable for SharedRunner<T, R>
where
    T: Display + Send + 'static,
    R: Runnable<T> + Send + 'static,
{
    fn handle(&mut self) {
        self.runner.on_tick();
        while let Ok(t) = self.receiver.try_recv() {
            self.runner.run(t);
            self.counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// SharedScheduler provides interface to schedule task to underlying workers with one pool
pub struct Scheduler<T> {
    counter: Arc<AtomicUsize>,
    sender: Sender<T>,
    pool_notify: Sender<WorkerMsg>,
    runner_id: usize,
}

impl<T: Display + Send + 'static> Scheduler<T> {
    pub fn fake() -> Scheduler<T> {
        let (tx, _) = mpsc::unbounded();
        let (pool_notify, _) = mpsc::unbounded();
        let counter = Arc::new(AtomicUsize::new(0));
        Scheduler {
            sender: tx,
            pool_notify,
            runner_id: 0,
            counter,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped or number pending tasks exceeds capacity, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        debug!("scheduling task {}", task);
        if let Err(e) = self.sender.try_send(task) {
            match e {
                TrySendError::Disconnected(t) => return Err(ScheduleError::Stopped(t)),
                TrySendError::Full(t) => return Err(ScheduleError::Full(t)),
            }
        }
        if let Err(e) = self.pool_notify.try_send(WorkerMsg::Task(self.runner_id)) {
            match e {
                TrySendError::Disconnected(_) => (),
                TrySendError::Full(_) => panic!("notif pool is full"),
            }
        }
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn notify(&self) {
        let _ = self.pool_notify.try_send(WorkerMsg::Task(self.runner_id));
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::Relaxed) > 0
    }

    pub fn notify_timeout(&self, wait: Duration) {
        let notify = self.pool_notify.clone();
        let runner_id = self.runner_id;
        let delay = GLOBAL_TIMER_HANDLE
            .delay(std::time::Instant::now() + wait)
            .map_err(|_| panic!("register time task error"))
            .inspect(move |_| {
                let _ = notify.send(WorkerMsg::Task(runner_id));
            });
        poll_future_notify(delay);
    }

    pub fn register_timeout(&self, task: T, wait: Duration) {
        debug!("scheduling task {}", task);
        let sender = self.sender.clone();
        let notify = self.pool_notify.clone();
        let runner_id = self.runner_id;
        let delay = GLOBAL_TIMER_HANDLE
            .delay(std::time::Instant::now() + wait)
            .map_err(|_| panic!("register time task error"))
            .inspect(move |_| {
                if let Err(e) = sender.try_send(task) {
                    match e {
                        TrySendError::Disconnected(_) => (),
                        _ => panic!("send delay scheduler message error"),
                    }
                }
                let _ = notify.send(WorkerMsg::Task(runner_id));
            });
        poll_future_notify(delay);
    }
}

impl<T> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            counter: Arc::clone(&self.counter),
            sender: self.sender.clone(),
            pool_notify: self.pool_notify.clone(),
            runner_id: self.runner_id,
        }
    }
}

pub enum WorkerMsg {
    Task(usize),
    Stop,
}

#[derive(Clone)]
pub struct Worker {
    sender: Sender<WorkerMsg>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    runners: Arc<Mutex<Vec<Box<dyn SharedRunnable>>>>,
}

impl Worker {
    pub fn new(name: &str) -> Worker {
        let (tx, rx) = mpsc::unbounded();
        let runners = Arc::new(Mutex::new(Vec::default()));
        let runners2 = runners.clone();
        let h = ThreadBuilder::new()
            .name(thd_name!(name))
            .spawn(move || poll_multiple_runner(rx, runners2))
            .unwrap();
        Worker {
            handle: Arc::new(Mutex::new(Some(h))),
            sender: tx,
            runners,
        }
    }

    pub fn start<T, R>(&self, runner: R) -> Scheduler<T>
    where
        T: Display + Send + 'static,
        R: Runnable<T> + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded();
        let mut runners = self.runners.lock().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut r = SharedRunner {
            runner,
            receiver: rx,
            counter: counter.clone(),
        };
        let idx = runners.len();
        let scheduler = Scheduler {
            sender: tx,
            pool_notify: self.sender.clone(),
            runner_id: idx,
            counter,
        };
        r.runner.register_scheduler(scheduler.clone());
        runners.push(Box::new(r));
        scheduler
    }

    pub fn stop(&self) -> Option<JoinHandle<()>> {
        self.sender.send(WorkerMsg::Stop).unwrap();
        self.handle.lock().unwrap().take()
    }
}

fn poll_multiple_runner(
    rx: Receiver<WorkerMsg>,
    all_runners: Arc<Mutex<Vec<Box<dyn SharedRunnable>>>>,
) {
    loop {
        match rx.recv() {
            Ok(WorkerMsg::Task(rid)) => {
                let mut runners_guard = all_runners.lock().unwrap();
                if rid < runners_guard.len() {
                    runners_guard[rid].handle();
                }
            }
            Ok(WorkerMsg::Stop) | Err(_) => {
                break;
            }
        }
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
