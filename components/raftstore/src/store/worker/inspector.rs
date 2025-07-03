// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    io::Write,
    path::PathBuf,
    time::Duration,
    cmp,
};

use std::sync::Arc;
use crossbeam::channel::{Receiver, Sender, bounded};
use health_controller::types::LatencyInspector;
use tikv_util::{
    time::Instant,
    warn,
    worker::{Runnable, Worker},
};
use pd_client::health::HealthClient;
use grpcio_health::proto::HealthCheckResponse;
use grpcio_health::ServingStatus::Serving;
use grpcio::{Error, RpcStatusCode};

fn is_network_error(err: &pd_client::Error) -> bool {
    warn!("[test] checking network error"; "err" => ?err);
    match err {
        pd_client::Error::Grpc(Error::RpcFailure(status)) => 
            status.code() == RpcStatusCode::DEADLINE_EXCEEDED,
            // leader restart will return unavailable, message: "failed to connect to all addresses"
        _ => false,
    }
}

#[derive(Debug)]
pub enum Task {
    DiskLatency { inspector: LatencyInspector },
    NetworkLatency { inspector: LatencyInspector },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::DiskLatency { .. } => write!(f, "DiskLatency"),
            Task::NetworkLatency { .. } => write!(f, "NetworkLatency"),
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

    health_client: Arc<dyn HealthClient + Send + Sync>,
}

impl Runner {
    /// The filename to write to the disk to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FILENAME: &'static str = ".disk_latency_inspector.tmp";
    /// The content to write to the file to measure the latency.
    const DISK_IO_LATENCY_INSPECT_FLUSH_STR: &'static [u8] = b"inspect disk io latency";
    /// If duration is greater than 1s, it will be considered as a timeout.
    const NETWORK_TIMEOUT: Duration = Duration::from_secs(2);
    /// If duration is less than 1s, it will be considered as a normal network.
    const NETWORK_NOT_TIMEOUT: Duration = Duration::from_millis(500);

    #[inline]
    fn build(target: PathBuf, health_client: Arc<dyn HealthClient + Send + Sync>) -> Self {
        // The disk check mechanism only cares about the latency of the most
        // recent request; older requests become stale and irrelevant. To avoid
        // unnecessary accumulation of multiple requests, we set a small
        // `capacity` for the disk check worker.
        let (notifier, receiver) = bounded(20);
        Self {
            target,
            notifier,
            receiver,
            bg_worker: None,
            health_client,
        }
    }

    #[inline]
    pub fn new(inspect_dir: PathBuf, health_client: Arc<dyn HealthClient + Send + Sync>) -> Self {
        Self::build(inspect_dir.join(Self::DISK_IO_LATENCY_INSPECT_FILENAME), health_client)
    }

    #[inline]
    /// Only for test.
    /// Generate a dummy Runner.
    pub fn dummy() -> Self {
        struct DummyHealthClient;
        impl HealthClient for DummyHealthClient {
            fn check(&self) -> Result<HealthCheckResponse, pd_client::Error> {
                Ok(HealthCheckResponse::default())
            }
        }
        Self::build(
            PathBuf::from("./").join(Self::DISK_IO_LATENCY_INSPECT_FILENAME), 
            Arc::new(DummyHealthClient),
        )
    }

    #[inline]
    pub fn bind_background_worker(&mut self, bg_worker: Worker) {
        self.bg_worker = Some(bg_worker);
    }

    fn inspect_disk(&self) -> Option<Duration> {
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

    fn inspect_network(&self) -> Option<Duration> {
        let start = Instant::now();
        match self.health_client.check() {
            Ok(resp) => {
                if resp.status != Serving {
                    warn!("pd server is not serving."; "status" => ?resp.status);
                    // Non-network problem, we do not consider it as a timeout
                    return Some(cmp::max(start.saturating_elapsed(), Self::NETWORK_NOT_TIMEOUT));
                }
            },
            Err(e) => {
                if is_network_error(&e) {
                    warn!("network error when checking pd health"; "err" => ?e);
                    return Some(Self::NETWORK_TIMEOUT);
                }
                warn!("unexpected error when checking pd health"; "err" => ?e);
                // Non-network problem, we do not consider it as a timeout
                return Some(cmp::max(start.saturating_elapsed(), Self::NETWORK_NOT_TIMEOUT));
            }
        };
        Some(start.saturating_elapsed())
    }

    fn execute(&self) {
        if let Ok(task) = self.receiver.try_recv() {
            match task {
                Task::DiskLatency { mut inspector } => {
                    if let Some(latency) = self.inspect_disk() {
                        inspector.record_apply_process(latency);
                        inspector.finish();
                    } else {
                        warn!("failed to inspect disk io latency");
                    }
                },
                Task::NetworkLatency { mut inspector } => {
                    // For network latency, we don't have a specific implementation here.
                    if let Some(latency) = self.inspect_network() {
                        inspector.record_network_io_duration(latency);
                        inspector.finish();
                    } else {
                        warn!("failed to inspect network io latency");
                    }
                },
            }
        }
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        // Send the task to the limited capacity channel.
        if let Err(e) = self.notifier.try_send(task) {
            warn!("failed to send task to inspector bg_worker: {:?}", e);
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

impl Drop for Runner {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.target) {
            warn!("remove disk latency inspector file failed"; "err" => ?e);
        }
    }
}

#[cfg(test)]
mod tests {
    use tikv_util::worker::Builder;

    use super::*;

    #[test]
    fn test_inspector_runner() {
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
                    let dur = duration.raftstore_duration.sum();
                    tx_1.send(dur).unwrap();
                }),
            );
            runner.run(Task::DiskLatency { inspector });
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
                        let dur = duration.raftstore_duration.sum();
                        tx_2.send(dur).unwrap();
                    }),
                );
                runner.run(Task::DiskLatency { inspector });
                rx.recv_timeout(Duration::from_secs(1)).unwrap_err();
            }
        }
    }
}
