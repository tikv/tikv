// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use health_controller::types::LatencyInspector;
use tikv_util::{
    time::Instant,
    worker::{Builder as WorkerBuilder, Runnable, Worker},
};

#[inline]
pub fn init_disk_check_worker() -> Worker {
    // The disk check mechanism only cares about the latency of the most
    // recent request; older requests become stale and irrelevant. To avoid
    // unnecessary accumulation of multiple requests, we set a small
    // `pending_capacity` for the disk check worker.
    WorkerBuilder::new("disk-check-worker")
        .pending_capacity(3)
        .create()
}

/// A simple inspector to measure the latency of disk IO.
///
/// This is used to measure the latency of disk IO, which is used to determine
/// the health status of the TiKV server.
/// The inspector writes a file to the disk and measures the time it takes to
/// complete the write operation.
pub struct Runner {
    target: PathBuf,
}

impl Runner {
    /// The filename to write to the disk to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FILENAME: &'static str = ".disk_latency_inspector.tmp";
    /// The content to write to the file to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FLUSH_STR: &'static [u8] = b"inspect disk io latency";

    #[inline]
    pub fn new(inspect_dir: PathBuf) -> Self {
        Self {
            target: inspect_dir.join(Self::DISK_IO_LATENCY_INSPECT_FILENAME),
        }
    }

    /// Only for test.
    /// Generate a dummy Runner.
    pub fn dummy() -> Self {
        Self {
            target: PathBuf::new(),
        }
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
}

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

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::InspectLatency { mut inspector } => {
                if let Some(latency) = self.inspect() {
                    inspector.record_apply_process(latency);
                    inspector.finish();
                }
            }
        }
    }
}
