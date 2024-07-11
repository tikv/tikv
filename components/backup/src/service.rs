// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::*, Arc, Mutex};

use futures::{
    channel::mpsc::{self, UnboundedReceiver},
    stream::{self, FusedStream},
    FutureExt, SinkExt, Stream, StreamExt, TryFutureExt,
};
use futures_util::stream::AbortHandle;
use kvproto::{backup_grpc::backup_server::Backup, brpb::*};
use raftstore::store::snapshot_backup::SnapshotBrHandle;
use tikv_util::{error, info, warn, worker::*};
use tonic::codegen::BoxStream;

use super::Task;
use crate::disk_snap::{self, StreamHandleLoop};

/// Service handles the RPC messages for the `Backup` service.
#[derive(Clone)]
pub struct Service<H: SnapshotBrHandle> {
    scheduler: Scheduler<Task>,
    snap_br_env: disk_snap::Env<H>,
    abort_last_req: Arc<Mutex<Option<AbortHandle>>>,
}

impl<H> Service<H>
where
    H: SnapshotBrHandle,
{
    /// Create a new backup service.
    pub fn new(scheduler: Scheduler<Task>, env: disk_snap::Env<H>) -> Self {
        Service {
            scheduler,
            snap_br_env: env,
            abort_last_req: Arc::default(),
        }
    }
}

struct CancelableReceiver<T> {
    receiver: UnboundedReceiver<T>,
    cancel: Arc<AtomicBool>,
}

impl<T> CancelableReceiver<T> {
    fn new(receiver: UnboundedReceiver<T>, cancel: Arc<AtomicBool>) -> Self {
        Self { receiver, cancel }
    }
}

impl<T: Send + Sync + 'static> Stream for CancelableReceiver<T> {
    type Item = tonic::Result<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.get_mut().receiver)
            .poll_next(cx)
            .map(|i| i.map(|r| Ok(r)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.receiver.size_hint()
    }
}

impl<T> Drop for CancelableReceiver<T> {
    fn drop(&mut self) {
        if !self.receiver.is_terminated() {
            self.cancel.store(true, Ordering::Release);
        }
    }
}

#[tonic::async_trait]
impl<H> Backup for Service<H>
where
    H: SnapshotBrHandle + 'static,
{
    type backupStream = BoxStream<BackupResponse>;
    async fn backup(
        &self,
        request: tonic::Request<BackupRequest>,
    ) -> std::result::Result<tonic::Response<BoxStream<BackupResponse>>, tonic::Status> {
        let mut cancel = None;
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        if let Err(status) = match Task::new(request.into_inner(), tx) {
            Ok((task, c)) => {
                cancel = Some(c);
                self.scheduler
                    .schedule(task)
                    .map_err(|e| tonic::Status::invalid_argument(format!("{:?}", e)))
            }
            Err(e) => Err(tonic::Status::unknown(format!("{:?}", e))),
        } {
            error!("backup task initiate failed"; "error" => ?status);
            return Err(status);
        };

        Ok(tonic::Response::new(
            Box::pin(CancelableReceiver::new(rx, cancel.unwrap())) as _,
        ))
    }

    type CheckPendingAdminOpStream = BoxStream<CheckAdminResponse>;
    /// CheckPendingAdminOp used for snapshot backup. before we start snapshot
    /// for a TiKV. we need stop all schedule first and make sure all
    /// in-flight schedule has finished. this rpc check all pending conf
    /// change for leader.
    async fn check_pending_admin_op(
        &self,
        request: tonic::Request<CheckAdminRequest>,
    ) -> std::result::Result<tonic::Response<BoxStream<CheckAdminResponse>>, tonic::Status> {
        let handle = self.snap_br_env.handle.clone();
        let _peer = tikv_util::get_grpc_peer_addr(&request);
        let (tx, rx) = mpsc::unbounded();
        if let Err(err) = handle.broadcast_check_pending_admin(tx) {
            return Err(tonic::Status::internal(format!("{err}")));
        }

        Ok(tonic::Response::new(Box::pin(rx) as _))
    }

    /// Server streaming response type for the PrepareSnapshotBackup method.
    type PrepareSnapshotBackupStream = BoxStream<PrepareSnapshotBackupResponse>;

    /// PrepareSnapshotBackup is an advanced version of preparing snapshot
    /// backup. Check the defination of `PrepareSnapshotBackupRequest` for
    /// more.
    async fn prepare_snapshot_backup(
        &self,
        request: tonic::Request<tonic::Streaming<PrepareSnapshotBackupRequest>>,
    ) -> std::result::Result<tonic::Response<BoxStream<PrepareSnapshotBackupResponse>>, tonic::Status>
    {
        let (l, new_cancel) = StreamHandleLoop::new(self.snap_br_env.clone());
        let peer = tikv_util::get_grpc_peer_addr(&request);
        // Note: should we disconnect here once there are more than one stream...?
        // Generally once two streams enter here, one may exit
        info!("A new prepare snapshot backup stream created!";
            "peer" => ?peer,
            "stream_count" => %self.snap_br_env.active_stream(),
        );
        let abort_last_req = self.abort_last_req.clone();
        let (tx, rx) = mpsc::unbounded();
        self.snap_br_env.get_async_runtime().spawn(async move {
            {
                let mut lock = abort_last_req.lock().unwrap();
                if let Some(cancel) = &*lock {
                    cancel.abort();
                }
                *lock = Some(new_cancel);
            }
            let res = l.run(request.into_inner(), tx.into()).await;
            info!("stream closed; probably everything is done or a problem cannot be retried happens"; 
                "result" => ?res, "peer" => ?peer);
        });
        Ok(tonic::Response::new(Box::pin(rx) as _))
    }
    /// prepare is used for file-copy backup. before we start the backup for a
    /// TiKV. we need invoke this function to generate the SST files map. or
    /// we get nothing to backup.
    async fn prepare(
        &self,
        request: tonic::Request<PrepareRequest>,
    ) -> tonic::Result<tonic::Response<PrepareResponse>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
    /// cleanup used for file-copy backup. after we finish the backup for a
    /// TiKV. we need clean some internel state. e.g. checkpoint, SST File
    /// maps
    async fn cleanup(
        &self,
        request: tonic::Request<CleanupRequest>,
    ) -> tonic::Result<tonic::Response<CleanupResponse>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use external_storage::make_local_backend;
    use kvproto::backup_grpc::{backup_client::BackupClient, backup_server::BackupServer};
    use tikv::storage::txn::tests::{must_commit, must_prewrite_put};
    use tikv_util::worker::{dummy_scheduler, ReceiverWrapper};
    use tokio::{net::TcpListener, runtime::Runtime};
    use tonic::transport::Channel;
    use txn_types::TimeStamp;

    use super::*;
    use crate::{disk_snap::Env, endpoint::tests::*};

    #[derive(Clone)]
    struct PanicHandle;

    impl SnapshotBrHandle for PanicHandle {
        fn send_wait_apply(
            &self,
            _region: u64,
            _req: raftstore::store::snapshot_backup::SnapshotBrWaitApplyRequest,
        ) -> raftstore::Result<()> {
            panic!("this case shouldn't call this!")
        }

        fn broadcast_wait_apply(
            &self,
            _req: raftstore::store::snapshot_backup::SnapshotBrWaitApplyRequest,
        ) -> raftstore::Result<()> {
            panic!("this case shouldn't call this!")
        }

        fn broadcast_check_pending_admin(
            &self,
            _tx: mpsc::UnboundedSender<tonic::Result<CheckAdminResponse>>,
        ) -> raftstore::Result<()> {
            panic!("this case shouldn't call this!")
        }
    }

    fn new_rpc_suite() -> (Runtime, BackupClient<Channel>, ReceiverWrapper<Task>) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (scheduler, rx) = dummy_scheduler();
        let backup_service =
            super::Service::new(scheduler, Env::new(PanicHandle, Default::default(), None));

        let builder =
            tonic::transport::Server::builder().add_service(BackupServer::new(backup_service));
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let sock_addr = listener.local_addr().unwrap();
        // start service
        runtime.spawn(builder.serve_with_incoming(listener));

        let addr = format!("http://127.0.0.1:{}", sock_addr.port());
        let channel_builder = tonic::transport::Channel::from_shared(addr).unwrap();
        let channel = runtime.block_on(channel_builder.connect()).unwrap();
        let client = BackupClient::new(channel);

        (runtime, client, rx)
    }

    #[test]
    fn test_client_stop() {
        let (runtime, mut client, mut rx) = new_rpc_suite();

        let (tmp, endpoint) = new_endpoint();
        let mut engine = endpoint.engine.clone();
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
                &mut engine,
                key.as_bytes(),
                key.as_bytes(),
                key.as_bytes(),
                start,
            );
            let commit = alloc_ts();
            must_commit(&mut engine, key.as_bytes(), start, commit);
        }

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(now.to_string())));

        let stream = runtime.block_on(client.backup(req)).unwrap().into_inner();
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Drop stream without start receiving will cause cancel error.
        drop(stream);
        // A stopped remote must not cause panic.
        endpoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(alloc_ts().to_string())));
        let mut stream = runtime.block_on(client.backup(req)).unwrap().into_inner();
        // Drop steam once it received something.
        runtime.spawn(async move {
            let _ = stream.next().await;
        });
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // A stopped remote must not cause panic.
        endpoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(alloc_ts().to_string())));
        let stream = runtime.block_on(client.backup(req)).unwrap().into_inner();
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
