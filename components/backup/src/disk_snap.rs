//! This module contains things about disk snapshot.

use std::{future::Future, sync::Arc, task::Poll, time::Duration};

use futures::future;
use futures_util::{
    future::{BoxFuture, FutureExt},
    sink::{Sink, SinkExt},
    stream::StreamExt,
};
use grpcio::{RpcStatus, WriteFlags};
use kvproto::{
    brpb::{
        PrepareSnapshotBackupEventType as PEvnT, PrepareSnapshotBackupRequest as PReq,
        PrepareSnapshotBackupRequestType as PReqT, PrepareSnapshotBackupResponse as PResp,
    },
    errorpb,
    metapb::Region,
};
use raftstore::store::{
    snapshot_backup::{
        RejectIngestAndAdmin, SnapshotBrHandle, SnapshotBrWaitApplyRequest, UnimplementedHandle,
    },
    SignificantRouter, SnapshotBrWaitApplySyncer,
};
use tikv_util::warn;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Interval, MissedTickBehavior},
};
use tokio_stream::Stream;

type Result<T> = std::result::Result<T, Error>;

const TICK_INTERVAL: Duration = Duration::from_secs(100);
const SERVER_SIDE_LEASE_IN_SEC: u64 = 300;

enum Error {
    NotInitialized,
    LeaseExpired,
    WaitApplyAborted,
    RaftStore(raftstore::Error),
}

enum HandleErr {
    AbortStream(RpcStatus),
    SendErrResp(errorpb::Error),
}

pub struct ResultSink(grpcio::DuplexSink<PResp>);

impl From<grpcio::DuplexSink<PResp>> for ResultSink {
    fn from(value: grpcio::DuplexSink<PResp>) -> Self {
        Self(value)
    }
}

impl ResultSink {
    async fn send(
        mut self,
        result: Result<PResp>,
        error_extra_info: impl FnOnce(&mut PResp),
    ) -> grpcio::Result<Self> {
        match result {
            Ok(item) => self.0.send((item, WriteFlags::default())).await?,
            Err(err) => match err.how_to_handle() {
                HandleErr::AbortStream(status) => {
                    self.0.fail(status.clone()).await?;
                    return Err(grpcio::Error::RpcFinished(Some(status)));
                }
                HandleErr::SendErrResp(err) => {
                    let mut resp = PResp::new();
                    error_extra_info(&mut resp);
                    resp.set_error(err);
                    self.0.send((resp, WriteFlags::default())).await?;
                }
            },
        }
        Ok(self)
    }
}

impl Error {
    fn how_to_handle(self) -> HandleErr {
        match self {
            Error::NotInitialized => HandleErr::AbortStream(RpcStatus::with_message(
                grpcio::RpcStatusCode::UNAVAILABLE,
                "coprocessor not initialized".to_owned(),
            )),
            Error::RaftStore(r) => HandleErr::AbortStream(RpcStatus::with_message(
                grpcio::RpcStatusCode::INTERNAL,
                format!("encountered unexpected raftstore error {r:?}"),
            )),
            Error::WaitApplyAborted => {
                HandleErr::SendErrResp({
                    let mut err = errorpb::Error::new();
                    err.set_message("wait apply has been aborted, perhaps epoch not match or leadership changed".to_owned());
                    err
                })
            }
            Error::LeaseExpired => HandleErr::AbortStream(RpcStatus::with_message(
                grpcio::RpcStatusCode::FAILED_PRECONDITION,
                format!(
                    "the lease has expired, you may not send `wait_apply` because it is no meaning"
                ),
            )),
        }
    }
}

pub struct Env<SR: SnapshotBrHandle> {
    handle: Arc<SR>,
    rejector: Arc<RejectIngestAndAdmin>,
}

impl<SR: SnapshotBrHandle> Clone for Env<SR> {
    fn clone(&self) -> Self {
        Self {
            handle: Arc::clone(&self.handle),
            rejector: Arc::clone(&self.rejector),
        }
    }
}

impl Env<UnimplementedHandle> {
    pub fn unimplemented() -> Self {
        Self {
            handle: Arc::new(UnimplementedHandle),
            rejector: Default::default(),
        }
    }
}

impl<SR: SnapshotBrHandle> Env<SR> {
    pub fn with_rejector(handle: SR, rejector: Arc<RejectIngestAndAdmin>) -> Self {
        Self {
            handle: Arc::new(handle),
            rejector,
        }
    }

    fn check_initialized(&self) -> Result<()> {
        if !self.rejector.connected() {
            return Err(Error::NotInitialized);
        }
        Ok(())
    }

    fn check_rejected(&self) -> Result<()> {
        self.check_initialized()?;
        if self.rejector.allowed() {
            return Err(Error::LeaseExpired);
        }
        Ok(())
    }

    fn update_lease(&self, lease_dur: Duration) -> Result<PResp> {
        self.check_initialized()?;
        let mut event = PResp::new();
        event.set_ty(PEvnT::UpdateLeaseResult);
        event.set_last_lease_is_vaild(self.rejector.heartbeat(lease_dur));
        Ok(event)
    }
}

pub struct StreamHandleLoop<SR: SnapshotBrHandle + 'static> {
    pending_regions: Vec<BoxFuture<'static, (Region, Result<()>)>>,
    env: Env<SR>,
}

enum StreamHandleEvent {
    Req(PReq),
    WaitApplyDone(Region, Result<()>),
    ConnectionGone(Option<grpcio::Error>),
}

impl<SR: SnapshotBrHandle + 'static> StreamHandleLoop<SR> {
    pub fn new(env: Env<SR>) -> Self {
        Self {
            env,
            pending_regions: vec![],
        }
    }

    fn async_wait_apply(&mut self, region: &Region) -> BoxFuture<'static, (Region, Result<()>)> {
        if let Err(err) = self.env.check_rejected() {
            return Box::pin(future::ready((region.clone(), Err(err))));
        }

        let (tx, rx) = oneshot::channel();
        let syncer = SnapshotBrWaitApplySyncer::new(region.id, tx);
        let handle = Arc::clone(&self.env.handle);
        let region = region.clone();
        let epoch = region.get_region_epoch().clone();
        let id = region.get_id();
        let send_res = handle
            .send_wait_apply(id, SnapshotBrWaitApplyRequest::strict(syncer, epoch))
            .map_err(Error::RaftStore);
        Box::pin(
            async move {
                send_res?;
                rx.await.map_err(|_| Error::WaitApplyAborted).map(|_| ())
            }
            .map(move |res| (region, res)),
        )
    }

    async fn next_event(
        &mut self,
        input: &mut (impl Stream<Item = grpcio::Result<PReq>> + Unpin),
    ) -> StreamHandleEvent {
        let wait_applies = future::poll_fn(|cx| {
            let selected =
                self.pending_regions
                    .iter_mut()
                    .enumerate()
                    .find_map(|(i, fut)| match fut.poll_unpin(cx) {
                        Poll::Ready(r) => Some((i, r)),
                        Poll::Pending => None,
                    });
            match selected {
                Some((i, region)) => {
                    self.pending_regions.swap_remove(i);
                    region.into()
                }
                None => Poll::Pending,
            }
        });

        tokio::select! {
            wres = wait_applies => {
                StreamHandleEvent::WaitApplyDone(wres.0, wres.1)
            }
            req = input.next() => {
                match req {
                    Some(Ok(req)) => StreamHandleEvent::Req(req),
                    Some(Err(err)) => StreamHandleEvent::ConnectionGone(Some(err)),
                    None => StreamHandleEvent::ConnectionGone(None)
                }
            }
        }
    }

    pub async fn run(
        mut self,
        mut input: impl Stream<Item = grpcio::Result<PReq>> + Unpin,
        mut sink: ResultSink,
    ) {
        loop {
            match self.next_event(&mut input).await {
                StreamHandleEvent::Req(req) => match req.get_ty() {
                    PReqT::UpdateLease => {
                        let lease_dur = Duration::from_secs(req.get_lease_in_seconds());
                        let feed_res = sink
                            .send(self.env.update_lease(lease_dur), |resp| {
                                resp.set_ty(PEvnT::UpdateLeaseResult);
                            })
                            .await;
                        match feed_res {
                            Ok(new_sink) => sink = new_sink,
                            Err(err) => {
                                warn!("stream closed; perhaps a problem cannot be retried happens"; "reason" => ?err);
                                return;
                            }
                        }
                    }
                    PReqT::WaitApply => {
                        let regions = req.get_regions();
                        for region in regions {
                            let res = self.async_wait_apply(region);
                            self.pending_regions.push(res);
                        }
                    }
                },
                StreamHandleEvent::WaitApplyDone(region, res) => {
                    let resp = res.map(|_| {
                        let mut resp = PResp::new();
                        resp.set_region(region.clone());
                        resp.set_ty(PEvnT::WaitApplyDone);
                        resp
                    });
                    let feed_res = sink
                        .send(resp, |resp| {
                            resp.set_ty(PEvnT::WaitApplyDone);
                            resp.set_region(region);
                        })
                        .await;
                    match feed_res {
                        Ok(new_sink) => sink = new_sink,
                        Err(err) => {
                            warn!("stream closed; perhaps a problem cannot be retried happens"; "reason" => ?err);
                            return;
                        }
                    }
                }
                StreamHandleEvent::ConnectionGone(err) => {
                    warn!("the client has gone, aborting loop"; "err" => ?err);
                    return;
                }
            }
        }
    }
}
