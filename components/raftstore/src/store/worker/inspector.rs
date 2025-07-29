// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter}, io::Write, path::PathBuf, sync::Arc, time::Duration, thread
};
use collections::HashMap;

use pd_client::{Config as PdConfig, PdClient, RpcClient};
use security::SecurityConfig;

use crossbeam::channel::{Receiver, Sender, TrySendError, bounded};
use grpcio::{Error, RpcStatusCode, Environment, ChannelBuilder};
use grpcio_health::{proto::{HealthCheckRequest, HealthClient}, ServingStatus::Serving};
use health_controller::{
    reporters::TikvClientMgr,
    types::LatencyInspector
};
use tikv_util::{
    time::Instant,
    warn, 
    info,
    worker::{Runnable, Worker},
};

use std::sync::Mutex;

use security::SecurityManager;


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
    tikv_client_mgr: Arc<Mutex<TikvClientMgr>>,
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
    // fn build(target: PathBuf, pd_client: Arc<RpcClient, Global>) -> Self {
    //     // The disk check mechanism only cares about the latency of the most
    //     // recent request; older requests become stale and irrelevant. To avoid
    //     // unnecessary accumulation of multiple requests, we set a small
    //     // `capacity` for the disk check worker.
    //     let disk_runner = InnerRunner::build(3);
    //     let network_runner = InnerRunner::build(100);
    //     Self {
    //         disk_runner,
    //         network_runner,
    //         target,
    //         pd_client,
    //         health_clients: None,
    //     }
    // }
    fn build(
        target: PathBuf,
        tikv_client_mgr: Arc<Mutex<TikvClientMgr>>,
    ) -> Self {
        let runner = Runner {
            disk_runner: InnerRunner::build(3),
            network_runner: InnerRunner::build(100),
            target,
            tikv_client_mgr,
        };
        
        runner
    }

    #[inline]
    pub fn new(
        inspect_dir: PathBuf,
        tikv_client_mgr: Arc<Mutex<TikvClientMgr>>,
    ) -> Self {
        Self::build(
            inspect_dir.join(Self::DISK_IO_LATENCY_INSPECT_FILENAME),
            tikv_client_mgr,
        )
    }

    #[inline]
    /// Only for test.
    /// Generate a dummy Runner.
    // pub fn dummy() -> Self {
    //     Self::build(
    //         PathBuf::from("./").join(Self::DISK_IO_LATENCY_INSPECT_FILENAME),
    //     )
    // }

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

    // fn inspect_network(&self, start: Instant) -> Option<Duration> {
    //     let clients = self.health_clients.lock().unwrap();
    //     for (store_id, client) in clients.iter() {
    //         match client.check(&HealthCheckRequest::new()) {
    //             Ok(resp) => {
    //                 if resp.status != Serving {
    //                     warn!("store is not serving"; "store" => store_id);
    //                     return Some(Self::NETWORK_NOT_TIMEOUT);
    //                 }
    //             }
    //             Err(e) => {
    //                 if is_network_error(&e) {
    //                     warn!("network error from"; "store" => store_id);
    //                     return Some(Self::NETWORK_TIMEOUT);
    //                 }
    //                 warn!("non-network error from"; "store" => store_id, "err" => ?e);
    //                 return Some(Self::NETWORK_NOT_TIMEOUT);
    //             }
    //         }
    //     }
    //     Some(start.saturating_elapsed())
    // }
    async fn inspect_network(
        &self,
        start: Instant,
        inspector: Arc<Mutex<LatencyInspector>>,
    ) {
        let mut handles = Vec::new();
        for (store_id, client) in self.tikv_client_mgr.lock().unwrap().get_health_clients() {
            let inspector = inspector.clone();
            let client = client.clone();

            let handle = thread::spawn(move || {
                info!("start health check"; "inspect_id" => inspector.lock().unwrap().id, "store_id" => store_id);
                let result = client.check(&HealthCheckRequest::new());
                info!("end health check"; "inspect_id" => inspector.lock().unwrap().id, "store_id" => store_id);
                let dur = match result {
                    Ok(_) => {
                        start.saturating_elapsed()
                    }
                    Err(e) if is_network_error(&e) => {
                        warn!("network error from"; "store" => store_id);
                        Self::NETWORK_TIMEOUT
                    }
                    Err(e) => {
                        warn!("non-network error from"; "store" => store_id, "err" => ?e);
                        Self::NETWORK_NOT_TIMEOUT
                    }
                };
            
                let mut guard = inspector.lock().unwrap();
                guard.record_network_io_duration(store_id, dur);
                guard.finish();
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
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
        // info!("execute_network called");
        if let Ok(task) = self.network_runner.receiver.try_recv() {
            match task {
                Task::NetworkLatency {
                    inspector,
                    start,
                } => {
                    let inspector = Arc::new(Mutex::new(inspector));
                    let this = self.clone();

                    this.inspect_network(start, inspector.clone()).await;
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
                let mut task_opt = Some(task);
                if let Err(e) = self
                    .network_runner
                    .notifier
                    .try_send(task_opt.take().unwrap())
                {
                    let e = match e {
                        TrySendError::Full(task) => {
                            // Try to make space and resend once
                            let _ = self.network_runner.receiver.try_recv();
                            if let Err(e2) = self
                                .network_runner
                                .notifier
                                .try_send(task)
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
            // Task::NetworkLatency { .. } => {
            //     let mut retry_task = Some(task);
            
            //     // take task out of Option for first try
            //     let first_try = retry_task.take().unwrap();
            //     match self.network_runner.notifier.try_send(first_try) {
            //         Ok(_) => {
            //             info!("successfully sent task to inspector network_runner");
            //             if let Some(bg_worker) = self.network_runner.bg_worker.as_ref() {
            //                 info!("spawning network inspector task");
            //                 let runner = self.clone();
            //                 bg_worker.spawn_async_task(async move {
            //                     runner.execute_network();
            //                 });
            //             } else {
            //                 warn!("network_runner.bg_worker is None!");
            //             }
            //         }
            //         Err(TrySendError::Full(_)) => {
            //             // make room and retry
            //             let _ = self.network_runner.receiver.try_recv();
            
            //             if let Some(task) = retry_task {
            //                 if let Err(e) = self.network_runner.notifier.try_send(task) {
            //                     warn!("failed to resend task to inspector bg_worker: {:?}", e);
            //                 } else if let Some(bg_worker) = self.network_runner.bg_worker.as_ref() {
            //                     let runner = self.clone();
            //                     bg_worker.spawn_async_task(async move {
            //                         runner.execute_network().await;
            //                     });
            //                 }
            //             }
            //         }
            //         Err(e) => {
            //             warn!("failed to send task to inspector bg_worker: {:?}", e);
            //         }
            //     }
            // }
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

    use std::time::Duration;

    use tikv_util::time::Instant;

    use super::Runner;

    #[test]
    fn test_update_health_clients_with_tidb_cluster() {
        let runner = Runner::dummy();

        runner.update_health_clients();

        {
            let clients = runner.health_clients.lock().unwrap();
            assert!(
                !clients.is_empty(),
                "No health clients found, maybe TiKV not registered in PD?"
            );
            for (addr, _) in clients.iter() {
                println!("Registered health client for store: {}", addr);
            }
        }

        let start = Instant::now();
        let dur = runner.inspect_network(start);
        assert!(
            dur.is_some(),
            "Network inspection should return latency duration"
        );
        println!("Measured network latency: {:?}", dur.unwrap());
    }
}
