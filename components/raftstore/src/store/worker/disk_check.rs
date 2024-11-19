// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use health_controller::types::LatencyInspector;
use tikv_util::{time::Instant, worker::Runnable};

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

pub struct Runner {
    data_dir: PathBuf,
}

impl Runner {
    pub fn new(data_dir: PathBuf) -> Runner {
        Runner { data_dir }
    }

    fn perform_disk_test(&self) -> Option<Duration> {
        let test_file = self.data_dir.join("tikv_disk_test.tmp");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&test_file)
            .ok()?;

        let data = vec![0u8; 1024]; // 1KB data
        let start = Instant::now();
        file.write_all(&data).ok()?;
        file.sync_all().ok()?;
        let duration = start.saturating_elapsed();
        Some(duration)
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::InspectLatency { mut inspector } => {
                if let Some(latency) = self.perform_disk_test() {
                    inspector.record_disk_health_check(latency);
                    inspector.finish();
                }
            }
        }
    }
}
