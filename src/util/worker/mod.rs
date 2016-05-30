/// Worker contains all workers that do the expensive job in background.


use std::sync::Arc;
use std::thread::{self, JoinHandle, Builder};
use std::io;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender, Receiver};
use std::result;

use util::SlowTimer;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Stopped
        IoError(e: io::Error) {
            from()
            display("{}", e)
        }
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(_: mpsc::SendError<T>) -> Error {
        // do we need to return the failed data
        Error::Stopped
    }
}

pub type Result<T> = result::Result<T, Error>;

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
}

pub trait BatchRunnable<T: Display> {
    /// run a batch of tasks.
    ///
    /// Please note that ts will be clear after invoking this method.
    fn run_batch(&mut self, ts: &mut Vec<T>);
}

impl<T: Display, R: Runnable<T>> BatchRunnable<T> for R {
    fn run_batch(&mut self, ts: &mut Vec<T>) {
        for t in ts.drain(..) {
            let task_str = format!("{}", t);
            let timer = SlowTimer::new();
            self.run(t);
            slow_log!(timer,
                      "task {} takes {:?} to finish.",
                      task_str,
                      timer.elapsed());
        }
    }
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T> {
    counter: Arc<AtomicUsize>,
    sender: Sender<Option<T>>,
}

impl<T: Display> Scheduler<T> {
    fn new(counter: AtomicUsize, sender: Sender<Option<T>>) -> Scheduler<T> {
        Scheduler {
            counter: Arc::new(counter),
            sender: sender,
        }
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<()> {
        debug!("scheduling task {}", task);
        try!(self.sender.send(Some(task)));
        self.counter.fetch_add(1, Ordering::SeqCst);
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
            counter: self.counter.clone(),
            sender: self.sender.clone(),
        }
    }
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    name: String,
    scheduler: Scheduler<T>,
    receiver: Option<Receiver<Option<T>>>,
    handle: Option<JoinHandle<()>>,
}

fn poll<R, T>(mut runner: R, rx: Receiver<Option<T>>, counter: Arc<AtomicUsize>, batch_size: usize)
    where R: BatchRunnable<T> + Send + 'static,
          T: Display + Send + 'static
{
    let mut keep_going = true;
    let mut buffer = Vec::with_capacity(batch_size);
    while keep_going {
        let t = rx.recv();
        match t {
            Ok(Some(t)) => buffer.push(t),
            _ => return,
        }
        while buffer.len() < batch_size {
            match rx.try_recv() {
                Ok(None) => {
                    keep_going = false;
                    break;
                }
                Ok(Some(t)) => buffer.push(t),
                _ => break,
            }
        }
        counter.fetch_sub(buffer.len(), Ordering::SeqCst);
        runner.run_batch(&mut buffer);
        buffer.clear();
    }
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Create a worker.
    pub fn new<S: Into<String>>(name: S) -> Worker<T> {
        let (tx, rx) = mpsc::channel();
        Worker {
            name: name.into(),
            scheduler: Scheduler::new(AtomicUsize::new(0), tx),
            receiver: Some(rx),
            handle: None,
        }
    }

    /// Start the worker.
    pub fn start<R: Runnable<T> + Send + 'static>(&mut self, runner: R) -> Result<()> {
        self.start_batch(runner, 1)
    }

    pub fn start_batch<R>(&mut self, runner: R, batch_size: usize) -> Result<()>
        where R: BatchRunnable<T> + Send + 'static
    {
        info!("starting working thread: {}", self.name);
        if self.receiver.is_none() {
            warn!("worker {} has been started.", self.name);
            return Ok(());
        }

        let rx = self.receiver.take().unwrap();
        let counter = self.scheduler.counter.clone();
        let h = try!(Builder::new()
            .name(self.name.clone())
            .spawn(move || poll(runner, rx, counter, batch_size)));
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
    pub fn schedule(&self, task: T) -> Result<()> {
        self.scheduler.schedule(task)
    }

    /// Check if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.handle.is_none() || self.scheduler.is_busy()
    }

    /// Stop the worker thread.
    pub fn stop(&mut self) -> thread::Result<()> {
        let handler = match self.handle.take() {
            Some(h) => h,
            None => return Ok(()),
        };

        // close sender explicitly so the background thread will exit.
        info!("stoping {}", self.name);
        if let Err(e) = self.scheduler.sender.send(None) {
            warn!("failed to stop worker thread: {:?}", e);
        }
        handler.join()
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::sync::Arc;
    use std::sync::atomic::*;
    use std::cmp;
    use std::time::Duration;

    use super::*;

    struct CountRunner {
        count: Arc<AtomicUsize>,
    }

    impl Runnable<u64> for CountRunner {
        fn run(&mut self, step: u64) {
            self.count.fetch_add(step as usize, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
        }
    }

    struct BatchRunner {
        count: Arc<AtomicUsize>,
    }

    impl BatchRunnable<u64> for BatchRunner {
        fn run_batch(&mut self, ms: &mut Vec<u64>) {
            let total = ms.iter().fold(0, |l, &r| l + r);
            self.count.fetch_add(total as usize, Ordering::SeqCst);
            let max_sleep = ms.iter().fold(0, |l, &r| cmp::max(l, r));
            thread::sleep(Duration::from_millis(max_sleep));
        }
    }

    #[test]
    fn test_worker() {
        let mut worker = Worker::new("test-worker");
        let count = Arc::new(AtomicUsize::new(0));
        worker.start(CountRunner { count: count.clone() }).unwrap();
        assert!(!worker.is_busy());
        worker.schedule(50).unwrap();
        worker.schedule(50).unwrap();
        worker.schedule(50).unwrap();
        assert!(worker.is_busy());
        for _ in 0..100 {
            if !worker.is_busy() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!worker.is_busy());
        assert_eq!(count.load(Ordering::SeqCst), 150);
        worker.stop().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
    }

    #[test]
    fn test_threaded() {
        let mut worker = Worker::new("test-worker-threaded");
        let count = Arc::new(AtomicUsize::new(0));
        worker.start(CountRunner { count: count.clone() }).unwrap();
        let scheduler = worker.scheduler();
        thread::spawn(move || {
            scheduler.schedule(100).unwrap();
            scheduler.schedule(100).unwrap();
        });
        for _ in 1..1000 {
            if worker.is_busy() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        worker.stop().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 200);
    }

    #[test]
    fn test_batch() {
        let mut worker = Worker::new("test-worker-batch");
        let count = Arc::new(AtomicUsize::new(0));
        worker.start_batch(BatchRunner { count: count.clone() }, 10).unwrap();
        for _ in 0..20 {
            worker.schedule(50).unwrap();
        }
        worker.stop().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 20 * 50);
    }
}
