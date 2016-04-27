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

pub type Result<T> = result::Result<T, Error>;

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    name: String,
    counter: Arc<AtomicUsize>,
    sender: Option<Sender<T>>,
    receiver: Option<Receiver<T>>,
    handle: Option<JoinHandle<()>>,
}

impl<T: Display + Send + 'static> Worker<T> {
    pub fn new(name: String) -> Worker<T> {
        let (tx, rx) = mpsc::channel();
        Worker {
            name: name,
            counter: Arc::new(AtomicUsize::new(0)),
            sender: Some(tx),
            receiver: Some(rx),
            handle: None,
        }
    }

    pub fn start<R: Runnable<T> + Send + 'static>(&mut self, mut runner: R) -> Result<()> {
        info!("starting working thread: {}", self.name);
        if self.receiver.is_none() {
            warn!("worker {} has been started.", self.name);
            return Ok(());
        }

        let rx = self.receiver.take().unwrap();
        let counter = self.counter.clone();
        let res = Builder::new().name(self.name.clone()).spawn(move || {
            loop {
                let t = rx.recv();
                if t.is_err() {
                    // no more msg will be sent.
                    return;
                }
                counter.fetch_sub(1, Ordering::SeqCst);
                let t = t.unwrap();
                let task_str = format!("{}", t);
                let timer = SlowTimer::new();
                runner.run(t);
                slow_log!(timer,
                          "task {} takes {:?} to finish.",
                          task_str,
                          timer.elapsed());
            }
        });
        let h = try!(res);
        self.handle = Some(h);
        Ok(())
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<()> {
        debug!("scheduling task {}", task);
        if let Some(tx) = self.sender.as_ref() {
            // receiver will be closed only when we close the sender first.
            tx.send(task).unwrap();
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        } else {
            Err(Error::Stopped)
        }
    }

    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::SeqCst) > 0
    }

    pub fn stop(&mut self) -> thread::Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }
        // close sender explicitly so the background thread will exit.
        info!("stoping {}", self.name);
        drop(self.sender.take().unwrap());
        if let Some(h) = self.handle.take() {
            try!(h.join());
        }
        Ok(())
    }
}
