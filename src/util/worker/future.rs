// Copyright 2017 PingCAP, Inc.
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

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle, Builder};
use std::io;
use std::fmt::Display;

use futures::Stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use tokio_core::reactor::{Core, Handle};

use super::Stopped;

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T, handle: &Handle);
    fn shutdown(&mut self) {}
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T> {
    name: Arc<String>,
    sender: UnboundedSender<Option<T>>,
}

impl<T: Display> Scheduler<T> {
    fn new<S: Into<String>>(name: S, sender: UnboundedSender<Option<T>>) -> Scheduler<T> {
        Scheduler {
            name: Arc::new(name.into()),
            sender: sender,
        }
    }

    /// Schedule a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        debug!("scheduling task {}", task);
        if let Err(err) = self.sender.send(Some(task)) {
            return Err(Stopped(err.into_inner().unwrap()));
        }
        Ok(())
    }
}

impl<T: Display> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            name: self.name.clone(),
            sender: self.sender.clone(),
        }
    }
}

/// A worker that can schedule time consuming tasks.
pub struct Worker<T: Display> {
    scheduler: Scheduler<T>,
    receiver: Mutex<Option<UnboundedReceiver<Option<T>>>>,
    handle: Option<JoinHandle<()>>,
}

// TODO: add metrics.
fn poll<R, T>(mut runner: R, rx: UnboundedReceiver<Option<T>>)
    where R: Runnable<T> + Send + 'static,
          T: Display + Send + 'static
{
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    {
        let f = rx.take_while(|t| Ok(t.is_some())).for_each(|t| {
            runner.run(t.unwrap(), &handle);
            Ok(())
        });
        if core.run(f).is_err() {
            error!("worker occurs an error");
        }
    }
    runner.shutdown();
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Create a worker.
    pub fn new<S: Into<String>>(name: S) -> Worker<T> {
        let (tx, rx) = unbounded();
        Worker {
            scheduler: Scheduler::new(name, tx),
            receiver: Mutex::new(Some(rx)),
            handle: None,
        }
    }

    /// Start the worker.
    pub fn start<R>(&mut self, runner: R) -> Result<(), io::Error>
        where R: Runnable<T> + Send + 'static
    {
        let mut receiver = self.receiver.lock().unwrap();
        info!("starting working thread: {}", self.scheduler.name);
        if receiver.is_none() {
            warn!("worker {} has been started.", self.scheduler.name);
            return Ok(());
        }

        let rx = receiver.take().unwrap();
        let h = try!(Builder::new()
            .name(thd_name!(self.scheduler.name.as_ref()))
            .spawn(move || poll(runner, rx)));

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
        self.handle.is_none()
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
    use std::sync::mpsc;
    use std::sync::mpsc::*;
    use std::time::Duration;
    use std::time::Instant;

    use futures::Future;
    use tokio_timer::Timer;
    use tokio_core::reactor::Handle;

    use super::*;

    struct StepRunner {
        ch: Sender<u64>,
    }

    impl Runnable<u64> for StepRunner {
        fn run(&mut self, step: u64, handle: &Handle) {
            self.ch.send(step).unwrap();
            let timer = Timer::default();
            let f = timer.sleep(Duration::from_millis(step)).map_err(|_| ());
            handle.spawn(f);
        }

        fn shutdown(&mut self) {
            self.ch.send(0).unwrap();
        }
    }

    #[test]
    fn test_future_worker() {
        let mut worker = Worker::new("test-async-worker");
        let (tx, rx) = mpsc::channel();
        worker.start(StepRunner { ch: tx }).unwrap();
        assert!(!worker.is_busy());
        // The default the tick size of tokio_timer is 100ms.
        let start = Instant::now();
        worker.schedule(500).unwrap();
        worker.schedule(1000).unwrap();
        worker.schedule(1500).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 500);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1000);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1500);
        // above three tasks are executed concurrently, should be less then 2s.
        assert!(start.elapsed() < Duration::from_secs(2));
        worker.stop().unwrap().join().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
        // when shutdown, StepRunner should send back a 0.
        assert_eq!(0, rx.recv().unwrap());
    }
}