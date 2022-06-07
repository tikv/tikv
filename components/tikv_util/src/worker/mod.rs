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
mod pool;

pub use pool::{
    dummy_scheduler, Builder, LazyWorker, ReceiverWrapper, Runnable, RunnableWithTimer,
    ScheduleError, Scheduler, Worker,
};

pub use self::future::{
    dummy_scheduler as dummy_future_scheduler, Runnable as FutureRunnable,
    Scheduler as FutureScheduler, Stopped, Worker as FutureWorker,
};

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use super::*;

    struct StepRunner {
        ch: mpsc::Sender<u64>,
    }

    impl Runnable for StepRunner {
        type Task = u64;

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

    impl Runnable for BatchRunner {
        type Task = u64;

        fn run(&mut self, ms: u64) {
            self.ch.send(vec![ms]).unwrap();
        }

        fn shutdown(&mut self) {
            let _ = self.ch.send(vec![]);
        }
    }

    struct TickRunner {
        ch: mpsc::Sender<&'static str>,
    }

    impl Runnable for TickRunner {
        type Task = &'static str;

        fn run(&mut self, msg: &'static str) {
            self.ch.send(msg).unwrap();
        }
        fn shutdown(&mut self) {
            self.ch.send("").unwrap();
        }
    }

    #[test]
    fn test_worker() {
        let worker = Worker::new("test-worker");
        let (tx, rx) = mpsc::channel();
        let scheduler = worker.start("test-worker", StepRunner { ch: tx });
        assert!(!worker.is_busy());
        scheduler.schedule(60).unwrap();
        scheduler.schedule(40).unwrap();
        scheduler.schedule(50).unwrap();
        assert!(worker.is_busy());
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 60);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 40);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 50);
        // task is handled before we update the busy status, so that we need some sleep.
        thread::sleep(Duration::from_millis(100));
        assert!(!worker.is_busy());
        drop(scheduler);
        worker.stop();
        // now worker can't handle any task
        assert!(worker.is_busy());
        drop(worker);
        // when shutdown, StepRunner should send back a 0.
        assert_eq!(0, rx.recv().unwrap());
    }

    #[test]
    fn test_threaded() {
        let worker = Worker::new("test-worker-threaded");
        let (tx, rx) = mpsc::channel();
        let scheduler = worker.start("test-worker", StepRunner { ch: tx });
        thread::spawn(move || {
            scheduler.schedule(90).unwrap();
            scheduler.schedule(110).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 90);
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 110);
        worker.stop();
        assert_eq!(0, rx.recv().unwrap());
    }

    #[test]
    fn test_pending_capacity() {
        let worker = Builder::new("test-worker-busy")
            .pending_capacity(3)
            .create();
        let mut lazy_worker = worker.lazy_build("test-busy");
        let scheduler = lazy_worker.scheduler();

        for i in 0..3 {
            scheduler.schedule(i).unwrap();
        }
        assert_eq!(scheduler.schedule(3).unwrap_err(), ScheduleError::Full(3));

        let (tx, rx) = mpsc::channel();
        lazy_worker.start(BatchRunner { ch: tx });
        assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());

        worker.stop();
        drop(rx);
    }
}
