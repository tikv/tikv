// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::IntGauge;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::compat::Future01CompatExt;
use futures::future::FutureExt;
use futures::stream::StreamExt;

use super::metrics::*;
use crate::future::poll_future_notify;
use crate::timer::GLOBAL_TIMER_HANDLE;
use crate::yatp_pool::{DefaultTicker, YatpPoolBuilder};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use yatp::{Remote, ThreadPool};

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

pub trait Runnable: Send {
    type Task: Display + Send + 'static;

    /// Runs a task.
    fn run(&mut self, _: Self::Task) {
        unimplemented!()
    }
    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

pub trait RunnableWithTimer: Runnable {
    fn on_timeout(&mut self);
    fn get_interval(&self) -> Duration;
}

enum Msg<T: Display + Send> {
    Task(T),
    Stop,
    Timeout,
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T: Display + Send> {
    counter: Arc<AtomicUsize>,
    sender: UnboundedSender<Msg<T>>,
    pending_capacity: usize,
    metrics_pending_task_count: IntGauge,
}

impl<T: Display + Send> Scheduler<T> {
    fn new(
        sender: UnboundedSender<Msg<T>>,
        counter: Arc<AtomicUsize>,
        pending_capacity: usize,
        metrics_pending_task_count: IntGauge,
    ) -> Scheduler<T> {
        Scheduler {
            counter,
            sender,
            pending_capacity,
            metrics_pending_task_count,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped or number pending tasks exceeds capacity, an error will return.
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        debug!("scheduling task {}", task);
        if self.counter.load(Ordering::Acquire) >= self.pending_capacity {
            return Err(ScheduleError::Full(task));
        }
        if let Err(e) = self.sender.unbounded_send(Msg::Task(task)) {
            if let Msg::Task(t) = e.into_inner() {
                return Err(ScheduleError::Stopped(t));
            }
        }
        self.metrics_pending_task_count.inc();
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::SeqCst) > 0
    }

    pub fn stop(&self) {
        let _ = self.sender.unbounded_send(Msg::Stop);
    }
}

impl<T: Display + Send> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            counter: Arc::clone(&self.counter),
            sender: self.sender.clone(),
            pending_capacity: self.pending_capacity,
            metrics_pending_task_count: self.metrics_pending_task_count.clone(),
        }
    }
}

pub struct LazyWorker<T: Display + Send + 'static> {
    scheduler: Scheduler<T>,
    worker: Worker,
    receiver: Option<UnboundedReceiver<Msg<T>>>,
    metrics_pending_task_count: IntGauge,
}

impl<T: Display + Send + 'static> LazyWorker<T> {
    pub fn start<R: 'static + Runnable<Task = T>>(&mut self, runner: R) -> bool {
        if let Some(receiver) = self.receiver.take() {
            self.worker
                .start_impl(runner, receiver, self.metrics_pending_task_count.clone());
            return true;
        }
        false
    }

    pub fn start_with_timer<R: 'static + RunnableWithTimer<Task = T>>(
        &mut self,
        runner: R,
    ) -> bool {
        if let Some(receiver) = self.receiver.take() {
            self.worker.start_with_timer_impl(
                runner,
                self.scheduler.sender.clone(),
                receiver,
                self.metrics_pending_task_count.clone(),
            );
            return true;
        }
        false
    }

    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }

    pub fn stop(&mut self) {
        self.scheduler.stop();
        self.worker.stop();
    }
}

#[derive(Copy, Clone)]
pub struct Builder<S: Into<String>> {
    name: S,
    thread_count: usize,
    pending_capacity: usize,
}

impl<S: Into<String>> Builder<S> {
    pub fn new(name: S) -> Self {
        Builder {
            name,
            thread_count: 1,
            pending_capacity: usize::MAX,
        }
    }

    /// Pending tasks won't exceed `pending_capacity`.
    pub fn pending_capacity(mut self, pending_capacity: usize) -> Self {
        self.pending_capacity = pending_capacity;
        self
    }

    pub fn thread_count(mut self, thread_count: usize) -> Self {
        self.thread_count = thread_count;
        self
    }

    pub fn create(self) -> Worker {
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(self.name)
            .thread_count(self.thread_count, self.thread_count)
            .build_single_level_pool();
        let remote = pool.remote().clone();
        let pool = Arc::new(Mutex::new(pool));
        Worker {
            remote,
            stop: Arc::new(AtomicBool::new(false)),
            pool,
            counter: Arc::new(AtomicUsize::new(0)),
            pending_capacity: self.pending_capacity,
        }
    }
}

/// A worker that can schedule time consuming tasks.
#[derive(Clone)]
pub struct Worker {
    pool: Arc<Mutex<ThreadPool<yatp::task::future::TaskCell>>>,
    remote: Remote<yatp::task::future::TaskCell>,
    pending_capacity: usize,
    counter: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl Worker {
    pub fn new<S: Into<String>>(name: S) -> Worker {
        Builder::new(name).create()
    }
    pub fn start<R: Runnable + 'static, S: Into<String>>(
        &self,
        name: S,
        runner: R,
    ) -> Scheduler<R::Task> {
        let (tx, rx) = unbounded();
        let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[&name.into()]);
        self.start_impl(runner, rx, metrics_pending_task_count.clone());
        Scheduler::new(
            tx,
            self.counter.clone(),
            self.pending_capacity,
            metrics_pending_task_count,
        )
    }

    pub fn start_with_timer<R: RunnableWithTimer + 'static, S: Into<String>>(
        &self,
        name: S,
        runner: R,
    ) -> Scheduler<R::Task> {
        let (tx, rx) = unbounded();
        let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[&name.into()]);
        self.start_with_timer_impl(runner, tx.clone(), rx, metrics_pending_task_count.clone());
        Scheduler::new(
            tx,
            self.counter.clone(),
            self.pending_capacity,
            metrics_pending_task_count,
        )
    }

    fn delay_notify<T: Display + Send + 'static>(tx: UnboundedSender<Msg<T>>, timeout: Duration) {
        let now = Instant::now();
        let f = GLOBAL_TIMER_HANDLE
            .delay(now + timeout)
            .compat()
            .map(move |_| {
                let _ = tx.unbounded_send(Msg::<T>::Timeout);
            });
        poll_future_notify(f);
    }

    pub fn lazy_build<T: Display + Send + 'static, S: Into<String>>(
        &self,
        name: S,
    ) -> LazyWorker<T> {
        let (rx, receiver) = unbounded();
        let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[&name.into()]);
        LazyWorker {
            receiver: Some(receiver),
            worker: self.clone(),
            scheduler: Scheduler::new(
                rx,
                self.counter.clone(),
                self.pending_capacity,
                metrics_pending_task_count.clone(),
            ),
            metrics_pending_task_count,
        }
    }

    /// Stops the worker thread.
    pub fn stop(&self) {
        self.pool.lock().unwrap().shutdown();
    }

    fn start_impl<R: Runnable + 'static>(
        &self,
        mut runner: R,
        mut receiver: UnboundedReceiver<Msg<R::Task>>,
        metrics_pending_task_count: IntGauge,
    ) {
        let counter = self.counter.clone();
        self.remote.spawn(async move {
            while let Some(msg) = receiver.next().await {
                match msg {
                    Msg::Task(task) => {
                        runner.run(task);
                        counter.fetch_sub(1, Ordering::Relaxed);
                        metrics_pending_task_count.dec();
                    }
                    Msg::Timeout => (),
                    Msg::Stop => break,
                }
            }
            runner.shutdown();
        });
    }

    fn start_with_timer_impl<R>(
        &self,
        mut runner: R,
        tx: UnboundedSender<Msg<R::Task>>,
        mut receiver: UnboundedReceiver<Msg<R::Task>>,
        metrics_pending_task_count: IntGauge,
    ) where
        R: RunnableWithTimer + 'static,
    {
        let counter = self.counter.clone();
        let timeout = runner.get_interval();
        Self::delay_notify(tx.clone(), timeout);
        self.remote.spawn(async move {
            while let Some(msg) = receiver.next().await {
                match msg {
                    Msg::Task(task) => {
                        runner.run(task);
                        counter.fetch_sub(1, Ordering::Relaxed);
                        metrics_pending_task_count.dec();
                    }
                    Msg::Timeout => {
                        runner.on_timeout();
                        Self::delay_notify(tx.clone(), timeout);
                    }
                    Msg::Stop => break,
                }
            }
            runner.shutdown();
        });
    }
}
