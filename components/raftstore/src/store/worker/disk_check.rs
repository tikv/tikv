// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, Sender};
use health_controller::types::LatencyInspector;
use tikv_util::{
    time::Instant,
    warn,
    worker::{Runnable, Worker},
};

#[derive(Debug)]
pub enum Task {
    InspectLatency { inspector: LatencyInspector },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::InspectLatency { .. } => write!(f, "InspectLatency"),
        }
    }
}

#[derive(Clone)]
/// A simple inspector to measure the latency of disk IO.
///
/// This is used to measure the latency of disk IO, which is used to determine
/// the health status of the TiKV server.
/// The inspector writes a file to the disk and measures the time it takes to
/// complete the write operation.
pub struct Runner {
    target: PathBuf,
    notifier: Sender<Task>,
    receiver: Receiver<Task>,
    bg_worker: Option<Worker>,
}

impl Runner {
    /// The filename to write to the disk to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FILENAME: &'static str = ".disk_latency_inspector.tmp";
    /// The content to write to the file to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FLUSH_STR: &'static [u8] = b"inspect disk io latency";

    #[inline]
    fn build(target: PathBuf) -> Self {
        // The disk check mechanism only cares about the latency of the most
        // recent request; older requests become stale and irrelevant. To avoid
        // unnecessary accumulation of multiple requests, we set a small
        // `capacity` for the disk check worker.
        let (notifier, receiver) = bounded(3);
        Self {
            target,
            notifier,
            receiver,
            bg_worker: None,
        }
    }

    #[inline]
    pub fn new(inspect_dir: PathBuf) -> Self {
        Self::build(inspect_dir.join(Self::DISK_IO_LATENCY_INSPECT_FILENAME))
    }

    #[inline]
    /// Only for test.
    /// Generate a dummy Runner.
    pub fn dummy() -> Self {
        Self::build(PathBuf::from("./").join(Self::DISK_IO_LATENCY_INSPECT_FILENAME))
    }

    #[inline]
    pub fn bind_background_worker(&mut self, bg_worker: Worker) {
        self.bg_worker = Some(bg_worker);
    }

    fn inspect(&self) -> Option<Duration> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.target)
            .ok()?;

        let start = Instant::now();
        // Ignore the error
        file.write_all(Self::DISK_IO_LATENCY_INSPECT_FLUSH_STR)
            .ok()?;
        file.sync_all().ok()?;
        Some(start.saturating_elapsed())
    }

    fn execute(&self) {
        if let Ok(task) = self.receiver.try_recv() {
            match task {
                Task::InspectLatency { mut inspector } => {
                    if let Some(latency) = self.inspect() {
                        inspector.record_apply_process(latency);
                        inspector.finish();
                    } else {
                        warn!("failed to inspect disk io latency");
                    }
                }
            }
        }
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        // Send the task to the limited capacity channel.
        if let Err(e) = self.notifier.try_send(task) {
            warn!("failed to send task to disk check bg_worker: {:?}", e);
        } else {
            let runner = self.clone();
            if let Some(bg_worker) = self.bg_worker.as_ref() {
                bg_worker.spawn_async_task(async move {
                    runner.execute();
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tikv_util::worker::Builder;

    use super::*;

    #[test]
    fn test_disk_check_runner() {
        let background_worker = Builder::new("disk-check-worker")
            .pending_capacity(256)
            .create();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let mut runner = Runner::dummy();
        runner.bind_background_worker(background_worker);
        // Validate the disk check runner.
        {
            let tx_1 = tx.clone();
            let inspector = LatencyInspector::new(
                1,
                Box::new(move |_, duration| {
                    let dur = duration.sum();
                    tx_1.send(dur).unwrap();
                }),
            );
            runner.run(Task::InspectLatency { inspector });
            let latency = rx.recv().unwrap();
            assert!(latency > Duration::from_secs(0));
        }
        // Invalid bg_worker and out of capacity
        {
            runner.bg_worker = None;
            for i in 2..=10 {
                let tx_2 = tx.clone();
                let inspector = LatencyInspector::new(
                    i as u64,
                    Box::new(move |_, duration| {
                        let dur = duration.sum();
                        tx_2.send(dur).unwrap();
                    }),
                );
                runner.run(Task::InspectLatency { inspector });
                // rx.recv().unwrap_err(); // the inspector should not receive any latency
                rx.recv_timeout(Duration::from_secs(1)).unwrap_err();
            }
        }
    }
}
