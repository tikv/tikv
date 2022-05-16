// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::*;

use futures::{channel::mpsc, FutureExt, SinkExt, StreamExt, TryFutureExt};
use grpcio::{self, *};
use kvproto::brpb::*;
use tikv_util::{error, info, worker::*};

use super::Task;

/// Service handles the RPC messages for the `Backup` service.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
}

impl Service {
    /// Create a new backup service.
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        Service { scheduler }
    }
}

impl Backup for Service {
    fn backup(
        &mut self,
        ctx: RpcContext<'_>,
        req: BackupRequest,
        mut sink: ServerStreamingSink<BackupResponse>,
    ) {
        let mut cancel = None;
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        if let Err(status) = match Task::new(req, tx) {
            Ok((task, c)) => {
                cancel = Some(c);
                self.scheduler.schedule(task).map_err(|e| {
                    RpcStatus::with_message(RpcStatusCode::INVALID_ARGUMENT, format!("{:?}", e))
                })
            }
            Err(e) => Err(RpcStatus::with_message(
                RpcStatusCode::UNKNOWN,
                format!("{:?}", e),
            )),
        } {
            error!("backup task initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("backup failed to send error"; "error" => ?e)),
            );
            return;
        };

        let send_task = async move {
            let mut s = rx.map(|resp| Ok((resp, WriteFlags::default())));
            sink.send_all(&mut s).await?;
            sink.close().await?;
            Ok(())
        }
        .map(|res: Result<()>| {
            match res {
                Ok(_) => {
                    info!("backup closed");
                }
                Err(e) => {
                    if let Some(c) = cancel {
                        // Cancel the running task.
                        c.store(true, Ordering::SeqCst);
                    }
                    error!("backup canceled"; "error" => ?e);
                }
            }
        });

        ctx.spawn(send_task);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use external_storage_export::make_local_backend;
    use tikv::storage::txn::tests::{must_commit, must_prewrite_put};
    use tikv_util::worker::{dummy_scheduler, ReceiverWrapper};
    use txn_types::TimeStamp;

    use super::*;
    use crate::endpoint::tests::*;

    fn new_rpc_suite() -> (Server, BackupClient, ReceiverWrapper<Task>) {
        let env = Arc::new(EnvBuilder::new().build());
        let (scheduler, rx) = dummy_scheduler();
        let backup_service = super::Service::new(scheduler);
        let builder =
            ServerBuilder::new(env.clone()).register_service(create_backup(backup_service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = BackupClient::new(channel);
        (server, client, rx)
    }

    #[test]
    fn test_client_stop() {
        let (_server, client, mut rx) = new_rpc_suite();

        let (tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();
        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"2".to_vec(), 1),
            (b"2".to_vec(), b"5".to_vec(), 2),
        ]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        for i in 0..5 {
            let start = alloc_ts();
            let key = format!("{}", i);
            must_prewrite_put(
                &engine,
                key.as_bytes(),
                key.as_bytes(),
                key.as_bytes(),
                start,
            );
            let commit = alloc_ts();
            must_commit(&engine, key.as_bytes(), start, commit);
        }

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(now.to_string())));

        let stream = client.backup(&req).unwrap();
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Drop stream without start receiving will cause cancel error.
        drop(stream);
        // A stopped remote must not cause panic.
        endpoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(alloc_ts().to_string())));
        let mut stream = client.backup(&req).unwrap();
        // Drop steam once it received something.
        client.spawn(async move {
            let _ = stream.next().await;
        });
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // A stopped remote must not cause panic.
        endpoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(alloc_ts().to_string())));
        let stream = client.backup(&req).unwrap();
        let task = rx.recv().unwrap();
        // Drop stream without start receiving will cause cancel error.
        drop(stream);
        // Wait util the task is canceled in map_err.
        loop {
            std::thread::sleep(Duration::from_millis(100));
            if task.resp.unbounded_send(Default::default()).is_err() {
                break;
            }
        }
        // The task should be canceled.
        assert!(task.has_canceled());
        // A stopped remote must not cause panic.
        endpoint.handle_backup_task(task);
    }
}
