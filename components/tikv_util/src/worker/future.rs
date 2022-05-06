// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    io,
    sync::{Arc, Mutex},
    thread::{self, Builder, JoinHandle},
};

use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    stream::StreamExt,
};
use prometheus::IntGauge;
use tokio::task::LocalSet;

use super::metrics::*;

pub struct Stopped<T>(pub T);

impl<T> Display for Stopped<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> Debug for Stopped<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "channel has been closed")
    }
}

impl<T> From<Stopped<T>> for Box<dyn Error + Sync + Send + 'static> {
    fn from(_: Stopped<T>) -> Box<dyn Error + Sync + Send + 'static> {
        box_err!("channel has been closed")
    }
}

pub trait Runnable<T: Display> {
    fn run(&mut self, t: T);
    fn shutdown(&mut self) {}
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T> {
    name: Arc<str>,
    sender: UnboundedSender<Option<T>>,
    metrics_pending_task_count: IntGauge,
}

pub fn dummy_scheduler<T: Display>() -> Scheduler<T> {
    let (tx, _) = unbounded();
    Scheduler::new("dummy future scheduler".to_owned(), tx)
}

impl<T: Display> Scheduler<T> {
    pub fn new<S: Into<Arc<str>>>(name: S, sender: UnboundedSender<Option<T>>) -> Scheduler<T> {
        let name = name.into();
        Scheduler {
            metrics_pending_task_count: WORKER_PENDING_TASK_VEC.with_label_values(&[&name]),
            name,
            sender,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        debug!("scheduling task {}", task);
        if let Err(err) = self.sender.unbounded_send(Some(task)) {
            return Err(Stopped(err.into_inner().unwrap()));
        }
        self.metrics_pending_task_count.inc();
        Ok(())
    }
}

impl<T: Display> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            name: Arc::clone(&self.name),
            sender: self.sender.clone(),
            metrics_pending_task_count: self.metrics_pending_task_count.clone(),
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
fn poll<R, T>(mut runner: R, mut rx: UnboundedReceiver<Option<T>>)
where
    R: Runnable<T> + Send + 'static,
    T: Display + Send + 'static,
{
    tikv_alloc::add_thread_memory_accessor();
    let current_thread = thread::current();
    let name = current_thread.name().unwrap();
    let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[name]);
    let metrics_handled_task_count = WORKER_HANDLED_TASK_VEC.with_label_values(&[name]);

    let handle = LocalSet::new();
    {
        let task = async {
            while let Some(msg) = rx.next().await {
                if let Some(t) = msg {
                    runner.run(t);
                    metrics_pending_task_count.dec();
                    metrics_handled_task_count.inc();
                } else {
                    break;
                }
            }
        };
        // `UnboundedReceiver` never returns an error.
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(handle.run_until(task));
    }
    runner.shutdown();
    tikv_alloc::remove_thread_memory_accessor();
}

impl<T: Display + Send + 'static> Worker<T> {
    /// Creates a worker.
    pub fn new<S: Into<Arc<str>>>(name: S) -> Worker<T> {
        let (tx, rx) = unbounded();
        Worker {
            scheduler: Scheduler::new(name, tx),
            receiver: Mutex::new(Some(rx)),
            handle: None,
        }
    }

    /// Starts the worker.
    pub fn start<R>(&mut self, runner: R) -> Result<(), io::Error>
    where
        R: Runnable<T> + Send + 'static,
    {
        let mut receiver = self.receiver.lock().unwrap();
        info!("starting working thread"; "worker" => &self.scheduler.name);
        if receiver.is_none() {
            warn!("worker has been started"; "worker" => &self.scheduler.name);
            return Ok(());
        }

        let rx = receiver.take().unwrap();
        let props = crate::thread_group::current_properties();
        let h = Builder::new()
            .name(thd_name!(self.scheduler.name.as_ref()))
            .spawn(move || {
                crate::thread_group::set_properties(props);
                poll(runner, rx)
            })?;

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
    pub fn schedule(&self, task: T) -> Result<(), Stopped<T>> {
        self.scheduler.schedule(task)
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.handle.is_none()
    }

    pub fn name(&self) -> &str {
        &self.scheduler.name
    }

    /// Stops the worker thread.
    pub fn stop(&mut self) -> Option<thread::JoinHandle<()>> {
        // close sender explicitly so the background thread will exit.
        info!("stoping worker"; "worker" => &self.scheduler.name);
        let handle = self.handle.take()?;
        if let Err(e) = self.scheduler.sender.unbounded_send(None) {
            warn!("failed to stop worker thread"; "err" => ?e);
        }

        Some(handle)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{self, Sender},
        time::Duration,
    };

    use futures::compat::Future01CompatExt;
    use tokio::task::spawn_local;
    use tokio_timer::timer;

    use super::*;
    use crate::{time::Instant, timer::GLOBAL_TIMER_HANDLE};

    struct StepRunner {
        timer: timer::Handle,
        ch: Sender<u64>,
    }

    impl Runnable<u64> for StepRunner {
        fn run(&mut self, step: u64) {
            self.ch.send(step).unwrap();
            let f = self
                .timer
                .delay(std::time::Instant::now() + Duration::from_millis(step))
                .compat();
            spawn_local(f);
        }

        fn shutdown(&mut self) {
            self.ch.send(0).unwrap();
        }
    }

    #[test]
    fn test_future_worker() {
        let mut worker = Worker::new("test-async-worker");
        let (tx, rx) = mpsc::channel();
        worker
            .start(StepRunner {
                timer: GLOBAL_TIMER_HANDLE.clone(),
                ch: tx,
            })
            .unwrap();
        assert!(!worker.is_busy());
        // The default tick size of tokio_timer is 100ms.
        let start = Instant::now();
        worker.schedule(500).unwrap();
        worker.schedule(1000).unwrap();
        worker.schedule(1500).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 500);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1000);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1500);
        // above three tasks are executed concurrently, should be less then 2s.
        assert!(start.saturating_elapsed() < Duration::from_secs(2));
        worker.stop().unwrap().join().unwrap();
        // now worker can't handle any task
        assert!(worker.is_busy());
        // when shutdown, StepRunner should send back a 0.
        assert_eq!(0, rx.recv().unwrap());
    }

    #[test]
    fn test_block_on_inside_worker() {
        struct BlockingRunner;

        impl Runnable<bool> for BlockingRunner {
            fn run(&mut self, _: bool) {
                futures::executor::block_on(async {});
            }
        }

        let mut worker = Worker::new("test-block-on-worker");
        worker.start(BlockingRunner).unwrap();
        worker.schedule(true).unwrap();
        worker.schedule(false).unwrap();
        worker.stop().unwrap().join().unwrap();
    }
}
