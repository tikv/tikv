// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    fmt::{self, Display, Formatter},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use tokio::sync::Mutex;
use futures::future::join_all;

use crossbeam::channel::{Receiver, Sender, TrySendError, bounded};
use grpcio::{Error, RpcStatusCode};
use grpcio_health::{ServingStatus::Serving, proto::Health, proto::HealthClient, proto::HealthCheckRequest};
use health_controller::types::LatencyInspector;
use tikv_client::TikvClientsMgr;
use tikv_util::{
    time::Instant,
    warn,
    worker::{Runnable, Worker},
};

fn is_network_error(err: &grpcio::Error) -> bool {
    warn!("[test] checking network error"; "err" => ?err);
    match err {
        Error::RpcFailure(status) => {
            status.code() == RpcStatusCode::DEADLINE_EXCEEDED
        }
        // leader restart will return unavailable, message: "failed to connect to all addresses"
        _ => false,
    }
}

#[derive(Debug)]
pub enum Task {
    DiskLatency {
        inspector: LatencyInspector,
    },
    NetworkLatency {
        inspector: LatencyInspector,
        start: Instant,
    },
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
    disk_runner: InnerRunner,
    network_runner: InnerRunner,

    target: PathBuf,
    tikv_clients_mgr: Arc<Mutex<TikvClientsMgr>>,
}

#[derive(Clone)]
struct InnerRunner {
    notifier: Sender<Task>,
    receiver: Receiver<Task>,
    bg_worker: Option<Worker>,
}

impl InnerRunner {
    fn build(cap: usize) -> Self {
        let (notifier, receiver) = bounded(cap);
        Self {
            notifier,
            receiver,
            bg_worker: None,
        }
    }

    #[inline]
    pub fn bind_background_worker(&mut self, bg_worker: Worker) {
        self.bg_worker = Some(bg_worker);
    }
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
    fn build(target: PathBuf, tikv_clients_mgr: Arc<Mutex<TikvClientsMgr>>) -> Self {
        // The disk check mechanism only cares about the latency of the most
        // recent request; older requests become stale and irrelevant. To avoid
        // unnecessary accumulation of multiple requests, we set a small
        // `capacity` for the disk check worker.
        let disk_runner = InnerRunner::build(3);
        let network_runner = InnerRunner::build(100);
        Self {
            disk_runner,
            network_runner,
            target,
            tikv_clients_mgr,
        }
    }

    #[inline]
    pub fn new(inspect_dir: PathBuf, tikv_clients_mgr: Arc<Mutex<TikvClientsMgr>>) -> Self {
        Self::build(
            inspect_dir.join(Self::DISK_IO_LATENCY_INSPECT_FILENAME),
            tikv_clients_mgr,
        )
    }

    #[inline]
    /// Only for test.
    /// Generate a dummy Runner.
    pub fn dummy() -> Self {
        Self::build(
            PathBuf::from("./").join(Self::DISK_IO_LATENCY_INSPECT_FILENAME),
            Arc::new(Mutex::new(TikvClientsMgr::default())),
        )
    }

    #[inline]
    pub fn bind_background_worker(&mut self, bg_worker: Worker) {
        self.disk_runner.bind_background_worker(bg_worker.clone());
        self.network_runner
            .bind_background_worker(bg_worker.clone());
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

    async fn inspect_network(&self, start: Instant) -> Option<Duration> {
        let health_clients = match self
            .tikv_clients_mgr
            .lock()
            .await
            .get_health_clients(Duration::from_secs(1))
            .await
        {
            Ok(clients) => clients,
            Err(e) => {
                warn!("failed to get health clients"; "err" => ?e);
                return None;
            }
        };
        let check_health = |client: HealthClient| async move {
            match client.check(&HealthCheckRequest::default()) {
                Ok(resp) => {
                    if resp.status != Serving {
                        warn!("pd server is not serving."; "status" => ?resp.status);
                        // Non-network problem, we do not consider it as a timeout
                        return Some(cmp::max(
                            start.saturating_elapsed(),
                            Self::NETWORK_NOT_TIMEOUT,
                        ));
                    }
                }
                Err(e) => {
                    if is_network_error(&e) {
                        warn!("network error when checking pd health"; "err" => ?e);
                        return Some(Self::NETWORK_TIMEOUT);
                    }
                    warn!("unexpected error when checking pd health"; "err" => ?e);
                    // Non-network problem, we do not consider it as a timeout
                    return Some(cmp::max(
                        start.saturating_elapsed(),
                        Self::NETWORK_NOT_TIMEOUT,
                    ));
                }
            };
            Some(start.saturating_elapsed())
        };
        let handles = health_clients
            .iter()
            .map(|client| check_health(client.clone()));
        let results = join_all(handles).await;

        results
            .iter()
            .flatten()
            .copied()
            .reduce(|a, b| a + b)
            .and_then(|sum| {
                let count = results.iter().flatten().count() as u32;
                if count > 0 {
                    Some(sum / count)
                } else {
                    None
                }
            })
    }

    fn execute_disk(&self) {
        if let Ok(task) = self.disk_runner.receiver.try_recv() {
            match task {
                Task::DiskLatency { mut inspector } => {
                    if let Some(latency) = self.inspect_disk() {
                        inspector.record_apply_process(latency);
                        inspector.finish();
                    } else {
                        warn!("failed to inspect disk io latency");
                    }
                }
                _ => {}
            }
        }
    }

    async fn execute_network(&self) {
        if let Ok(task) = self.network_runner.receiver.try_recv() {
            match task {
                Task::NetworkLatency {
                    mut inspector,
                    start,
                } => {
                    if let Some(latency) = self.inspect_network(start).await {
                        inspector.record_network_io_duration(latency);
                        inspector.finish();
                    } else {
                        warn!("failed to inspect network io latency");
                    }
                }
                _ => {}
            }
        }
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::DiskLatency { inspector: _ } => {
                // Send the task to the limited capacity channel.
                if let Err(e) = self.disk_runner.notifier.try_send(task) {
                    warn!("failed to send task to inspector bg_worker: {:?}", e);
                } else {
                    let runner = self.clone();
                    if let Some(bg_worker) = self.disk_runner.bg_worker.as_ref() {
                        bg_worker.spawn_async_task(async move {
                            runner.execute_disk();
                        });
                    }
                }
            }
            Task::NetworkLatency {
                inspector: _,
                start: _,
            } => {
                if let Err(e) = self
                    .network_runner
                    .notifier
                    .try_send(task)
                {
                    let e = match e {
                        TrySendError::Full(back_task) => {
                            // Try to make space and resend once
                            let _ = self.network_runner.receiver.try_recv();
                            if let Err(e2) = self
                                .network_runner
                                .notifier
                                .try_send(back_task)
                            {
                                e2
                            } else {
                                // Successfully sent after making space, so return early
                                return;
                            }
                        }
                        other => other,
                    };
                    warn!("failed to send task to inspector bg_worker: {:?}", e);
                } else {
                    let runner = self.clone();
                    if let Some(bg_worker) = self.network_runner.bg_worker.as_ref() {
                        bg_worker.spawn_async_task(async move {
                            runner.execute_network().await;
                        });
                    }
                }
            }
        };
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
            runner.disk_runner.bg_worker = None;
            runner.network_runner.bg_worker = None;
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
