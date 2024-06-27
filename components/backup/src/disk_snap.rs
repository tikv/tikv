// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This module contains things about disk snapshot.

use std::{
    future::Pending,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};

use futures::{
    channel::mpsc::UnboundedSender,
    future::{self, TryFutureExt},
};
use futures_util::{
    future::{BoxFuture, FutureExt},
    sink::SinkExt,
    stream::{AbortHandle, Abortable, StreamExt},
};
use kvproto::{
    brpb::{
        PrepareSnapshotBackupEventType as PEvnT, PrepareSnapshotBackupRequest as PReq,
        PrepareSnapshotBackupRequestType as PReqT, PrepareSnapshotBackupResponse as PResp,
    },
    errorpb::{self, StaleCommand},
    metapb::Region,
};
use raftstore::store::{
    snapshot_backup::{
        AbortReason, PrepareDiskSnapObserver, SnapshotBrHandle, SnapshotBrWaitApplyRequest,
    },
    SnapshotBrWaitApplySyncer,
};
use tikv_util::{sys::thread::ThreadBuildWrapper, warn, Either};
use tokio::{
    runtime::{Handle, Runtime},
    sync::oneshot,
};
use tokio_stream::Stream;

const DEFAULT_RT_THREADS: usize = 2;

type Result<T> = std::result::Result<T, Error>;

enum Error {
    Uninitialized,
    LeaseExpired,
    /// Wait apply has been aborted.
    /// When the `reason` is `None`, implies the request itself has been
    /// canceled (seldom) due to message lost or something.
    WaitApplyAborted(Option<AbortReason>),
    RaftStore(raftstore::Error),
}

enum HandleErr {
    AbortStream(tonic::Status),
    SendErrResp(errorpb::Error),
}

pub struct ResultSink(UnboundedSender<tonic::Result<PResp>>);

impl From<UnboundedSender<tonic::Result<PResp>>> for ResultSink {
    fn from(value: UnboundedSender<tonic::Result<PResp>>) -> Self {
        Self(value)
    }
}

impl ResultSink {
    async fn send(
        &mut self,
        result: Result<PResp>,
        error_extra_info: impl FnOnce(&mut PResp),
    ) -> tonic::Result<()> {
        let res = match result {
            // Note: should we batch here?
            Ok(item) => Ok(item),
            Err(err) => match err.into() {
                HandleErr::AbortStream(status) => Err(status),
                HandleErr::SendErrResp(err) => {
                    let mut resp = PResp::default();
                    error_extra_info(&mut resp);
                    resp.set_error(err);
                    Ok(resp)
                }
            },
        };
        self.0
            .send(res)
            .map_err(|e| tonic::Status::aborted(format!("send failed: {:?}", e)))
            .await
    }
}

impl From<Error> for HandleErr {
    fn from(value: Error) -> Self {
        match value {
            Error::Uninitialized => {
                HandleErr::AbortStream(tonic::Status::unavailable("coprocessor not initialized"))
            }
            Error::RaftStore(r) => HandleErr::SendErrResp(errorpb::Error::from(r)),
            Error::WaitApplyAborted(reason) => HandleErr::SendErrResp({
                let mut err = errorpb::Error::default();
                err.set_message(format!("wait apply has been aborted, perhaps epoch not match or leadership changed, note = {:?}", reason));
                match reason {
                    Some(AbortReason::EpochNotMatch(enm)) => err.set_epoch_not_match(enm),
                    Some(AbortReason::StaleCommand { .. }) => {
                        err.set_stale_command(StaleCommand::default())
                    }
                    _ => {}
                }
                err
            }),
            Error::LeaseExpired => HandleErr::AbortStream(tonic::Status::failed_precondition(
                "the lease has expired, you may not send `wait_apply` because it is no meaning",
            )),
        }
    }
}

#[derive(Clone)]
pub struct Env<SR: SnapshotBrHandle> {
    pub(crate) handle: SR,
    rejector: Arc<PrepareDiskSnapObserver>,
    active_stream: Arc<AtomicU64>,
    // Left: a shared tokio runtime.
    // Right: a hosted runtime(usually for test cases).
    runtime: Either<Handle, Arc<Runtime>>,
}

impl<SR: SnapshotBrHandle> Env<SR> {
    pub fn new(
        handle: SR,
        rejector: Arc<PrepareDiskSnapObserver>,
        runtime: Option<Handle>,
    ) -> Self {
        let runtime = match runtime {
            None => Either::Right(Self::default_runtime()),
            Some(rt) => Either::Left(rt),
        };
        Self {
            handle,
            rejector,
            active_stream: Arc::new(AtomicU64::new(0)),
            runtime,
        }
    }

    pub fn active_stream(&self) -> u64 {
        self.active_stream.load(Ordering::SeqCst)
    }

    pub fn get_async_runtime(&self) -> &Handle {
        match &self.runtime {
            Either::Left(h) => h,
            Either::Right(rt) => rt.handle(),
        }
    }

    fn check_initialized(&self) -> Result<()> {
        if !self.rejector.initialized() {
            return Err(Error::Uninitialized);
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
        let mut event = PResp::default();
        event.set_ty(PEvnT::UpdateLeaseResult);
        event.set_last_lease_is_valid(self.rejector.update_lease(lease_dur));
        Ok(event)
    }

    fn reset(&self) -> PResp {
        let rejected = !self.rejector.allowed();
        self.rejector.reset();
        let mut event = PResp::default();
        event.set_ty(PEvnT::UpdateLeaseResult);
        event.set_last_lease_is_valid(rejected);
        event
    }

    fn default_runtime() -> Arc<Runtime> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DEFAULT_RT_THREADS)
            .enable_all()
            .with_sys_hooks()
            .thread_name("snap_br_backup_prepare")
            .build()
            .unwrap();
        Arc::new(rt)
    }
}

pub struct StreamHandleLoop<SR: SnapshotBrHandle + 'static> {
    pending_regions: Vec<BoxFuture<'static, (Region, Result<()>)>>,
    env: Env<SR>,
    aborted: Abortable<Pending<()>>,
}

impl<SR: SnapshotBrHandle + 'static> Drop for StreamHandleLoop<SR> {
    fn drop(&mut self) {
        self.env.active_stream.fetch_sub(1, Ordering::SeqCst);
    }
}

enum StreamHandleEvent {
    Req(PReq),
    WaitApplyDone(Region, Result<()>),
    ConnectionGone(Option<tonic::Status>),
    Abort,
}

impl<SR: SnapshotBrHandle + 'static> StreamHandleLoop<SR> {
    pub fn new(env: Env<SR>) -> (Self, AbortHandle) {
        let (aborted, handle) = futures_util::future::abortable(std::future::pending());
        env.active_stream.fetch_add(1, Ordering::SeqCst);
        let this = Self {
            env,
            aborted,
            pending_regions: vec![],
        };
        (this, handle)
    }

    fn async_wait_apply(&mut self, region: &Region) -> BoxFuture<'static, (Region, Result<()>)> {
        if let Err(err) = self.env.check_rejected() {
            return Box::pin(future::ready((region.clone(), Err(err))));
        }

        let (tx, rx) = oneshot::channel();
        let syncer = SnapshotBrWaitApplySyncer::new(region.id, tx);
        let handle = self.env.handle.clone();
        let region = region.clone();
        let epoch = region.get_region_epoch().clone();
        let id = region.get_id();
        let send_res = handle
            .send_wait_apply(id, SnapshotBrWaitApplyRequest::strict(syncer, epoch))
            .map_err(Error::RaftStore);
        Box::pin(
            async move {
                send_res?;
                rx.await
                    .map_err(|_| Error::WaitApplyAborted(None))
                    .and_then(|report| match report.aborted {
                        Some(reason) => Err(Error::WaitApplyAborted(Some(reason))),
                        None => Ok(()),
                    })
            }
            .map(move |res| (region, res)),
        )
    }

    async fn next_event(
        &mut self,
        input: &mut (impl Stream<Item = tonic::Result<PReq>> + Unpin),
    ) -> StreamHandleEvent {
        let pending_regions = &mut self.pending_regions;
        let wait_applies = future::poll_fn(|cx| {
            let selected = pending_regions.iter_mut().enumerate().find_map(|(i, fut)| {
                match fut.poll_unpin(cx) {
                    Poll::Ready(r) => Some((i, r)),
                    Poll::Pending => None,
                }
            });
            match selected {
                Some((i, region)) => {
                    // We have polled the future (and make sure it has ready) before, it is
                    // safe to drop this future directly.
                    let _ = pending_regions.swap_remove(i);
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
            _ = &mut self.aborted => {
                StreamHandleEvent::Abort
            }
        }
    }

    pub async fn run(
        mut self,
        mut input: impl Stream<Item = tonic::Result<PReq>> + Unpin,
        mut sink: ResultSink,
    ) -> tonic::Result<()> {
        loop {
            match self.next_event(&mut input).await {
                StreamHandleEvent::Req(req) => match req.get_ty() {
                    PReqT::UpdateLease => {
                        let lease_dur = Duration::from_secs(req.get_lease_in_seconds());
                        let result = self.env.update_lease(lease_dur);
                        sink.send(result, |resp| {
                            resp.set_ty(PEvnT::UpdateLeaseResult);
                        })
                        .await?;
                    }
                    PReqT::WaitApply => {
                        let regions = req.get_regions();
                        for region in regions {
                            let res = self.async_wait_apply(region);
                            self.pending_regions.push(res);
                        }
                    }
                    PReqT::Finish => {
                        sink.send(Ok(self.env.reset()), |_| {}).await?;
                        return Ok(());
                    }
                },
                StreamHandleEvent::WaitApplyDone(region, res) => {
                    let resp = res.map(|_| {
                        let mut resp = PResp::default();
                        resp.set_region(region.clone());
                        resp.set_ty(PEvnT::WaitApplyDone);
                        resp
                    });
                    sink.send(resp, |resp| {
                        resp.set_ty(PEvnT::WaitApplyDone);
                        resp.set_region(region);
                    })
                    .await?;
                }
                StreamHandleEvent::ConnectionGone(err) => {
                    warn!("the client has gone, aborting loop"; "err" => ?err);
                    return match err {
                        None => Ok(()),
                        Some(err) => Err(err),
                    };
                }
                StreamHandleEvent::Abort => {
                    warn!("Aborted disk snapshot prepare loop by the server.");
                    let err = tonic::Status::cancelled("the loop has been aborted by server");
                    let _ = sink.0.send(Err(err.clone())).await;
                    return Err(err);
                }
            }
        }
    }
}
