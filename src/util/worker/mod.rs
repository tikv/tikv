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
    fn run_batch(&mut self, ts: Vec<T>);
}

impl<T: Display, R: Runnable<T>> BatchRunnable<T> for R {
    fn run_batch(&mut self, ts: Vec<T>) {
        for t in ts {
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

/// Scheduler provide interface to schedule task to underlying workers.
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
    batch_size: usize,
    scheduler: Option<Scheduler<T>>,
    receiver: Option<Receiver<Option<T>>>,
    handle: Option<JoinHandle<()>>,
}

fn poll<R, T>(mut runner: R, rx: Receiver<Option<T>>, counter: Arc<AtomicUsize>, batch_size: usize)
    where R: BatchRunnable<T> + Send + 'static,
          T: Display + Send + 'static
{
    let mut keep_going = true;
    while keep_going {
        let mut buffer = vec![];
        let t = rx.recv();
        match t {
            Ok(Some(t)) => buffer.push(t),
            _ => return,
        }
        while let Ok(opt) = rx.try_recv() {
            match opt {
                None => {
                    keep_going = false;
                    break;
                }
                Some(t) => buffer.push(t),
            }
            if buffer.len() == batch_size {
                break;
            }
        }
        buffer.shrink_to_fit();
        counter.fetch_sub(buffer.len(), Ordering::SeqCst);
        runner.run_batch(buffer);
    }
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Create a worker.
    pub fn new<S: Into<String>>(name: S, batch_size: usize) -> Worker<T> {
        let (tx, rx) = mpsc::channel();
        Worker {
            name: name.into(),
            batch_size: batch_size,
            scheduler: Some(Scheduler::new(AtomicUsize::new(0), tx)),
            receiver: Some(rx),
            handle: None,
        }
    }

    /// Start the worker.
    pub fn start<R: BatchRunnable<T> + Send + 'static>(&mut self, runner: R) -> Result<()> {
        info!("starting working thread: {}", self.name);
        if self.receiver.is_none() {
            warn!("worker {} has been started.", self.name);
            return Ok(());
        }

        let rx = self.receiver.take().unwrap();
        let counter = self.scheduler.as_ref().unwrap().counter.clone();
        let batch_size = self.batch_size;
        let res = Builder::new()
            .name(self.name.clone())
            .spawn(move || poll(runner, rx, counter, batch_size));
        let h = try!(res);
        self.handle = Some(h);
        Ok(())
    }

    /// Get a scheduler to schedule task.
    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.as_ref().unwrap().clone()
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<()> {
        self.scheduler.as_ref().ok_or(Error::Stopped).and_then(|s| s.schedule(task))
    }

    /// Check if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.scheduler.as_ref().map_or(true, |s| s.is_busy())
    }

    /// Stop the worker thread.
    pub fn stop(&mut self) -> thread::Result<()> {
        let scheduler = match self.scheduler.take() {
            None => return Ok(()),
            Some(s) => s,
        };
        // close sender explicitly so the background thread will exit.
        info!("stoping {}", self.name);
        if let Err(e) = scheduler.sender.send(None) {
            warn!("failed to stop worker thread: {:?}", e);
        }
        if let Some(h) = self.handle.take() {
            try!(h.join());
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::cmp;
    use std::time::{Instant, Duration};

    use super::*;

    struct SleepRunner;

    impl Runnable<u64> for SleepRunner {
        fn run(&mut self, ms: u64) {
            thread::sleep(Duration::from_millis(ms));
        }
    }

    struct BatchRunner;

    impl BatchRunnable<u64> for BatchRunner {
        fn run_batch(&mut self, ms: Vec<u64>) {
            let max_sleep = ms.iter().fold(0, |l, &r| cmp::max(l, r));
            thread::sleep(Duration::from_millis(max_sleep));
        }
    }

    #[test]
    fn test_worker() {
        let mut worker = Worker::new("test-worker".to_owned(), 1);
        worker.start(SleepRunner).unwrap();
        assert!(!worker.is_busy());
        let timer = Instant::now();
        worker.schedule(50).unwrap();
        worker.schedule(50).unwrap();
        assert!(worker.is_busy());
        thread::sleep(Duration::from_millis(60));
        assert!(!worker.is_busy());
        worker.stop().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
        assert!(timer.elapsed() >= Duration::from_millis(100));
    }

    #[test]
    fn test_threaded() {
        let mut worker = Worker::new("test-worker-threaded".to_owned(), 1);
        worker.start(SleepRunner).unwrap();
        let scheduler = worker.scheduler();
        let timer = Instant::now();
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
        assert!(timer.elapsed() >= Duration::from_millis(200));
    }

    #[test]
    fn test_batch() {
        let mut worker = Worker::new("test-worker-batch".to_owned(), 10);
        worker.start(BatchRunner).unwrap();
        let timer = Instant::now();
        for _ in 0..10 {
            worker.schedule(50).unwrap();
        }
        worker.stop().unwrap();
        assert!(timer.elapsed() < Duration::from_millis(50 * 10));
    }
}
