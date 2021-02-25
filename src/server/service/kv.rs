// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use tikv_util::time::{duration_to_sec, Instant};

use super::batch::ReqBatcher;
use crate::coprocessor::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::server::load_statistics::ThreadLoad;
use crate::server::metrics::*;
use crate::server::snap::Task as SnapTask;
use crate::server::Error;
use crate::server::Result as ServerResult;
use crate::storage::{
    errors::{
        extract_committed, extract_key_error, extract_key_errors, extract_kv_pairs,
        extract_region_error, map_kv_pairs,
    },
    kv::Engine,
    lock_manager::LockManager,
    SecondaryLocksStatus, Storage, TxnStatus,
};
use engine_rocks::RocksEngine;
use futures::compat::Future01CompatExt;
use futures::future::{self, Future, FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult,
    RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, *};
use kvproto::kvrpcpb::*;
use kvproto::mpp::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use kvproto::raft_serverpb::*;
use kvproto::tikvpb::*;
use raftstore::router::RaftStoreRouter;
use raftstore::store::{Callback, CasualMessage, StoreMsg};
use raftstore::{DiscardReason, Error as RaftStoreError};
use tikv_util::future::{paired_future_callback, poll_future_notify};
use tikv_util::mpsc::batch::{unbounded, BatchCollector, BatchReceiver, Sender};
use tikv_util::worker::Scheduler;
use txn_types::{self, Key};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
pub struct Service<T: RaftStoreRouter<RocksEngine> + 'static, E: Engine, L: LockManager> {
    /// Used to handle requests related to GC.
    gc_worker: GcWorker<E, T>,
    // For handling KV requests.
    storage: Storage<E, L>,
    // For handling coprocessor requests.
    cop: Endpoint<E>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,

    enable_req_batch: bool,

    grpc_thread_load: Arc<ThreadLoad>,

    readpool_normal_thread_load: Arc<ThreadLoad>,
}

impl<
        T: RaftStoreRouter<RocksEngine> + Clone + 'static,
        E: Engine + Clone,
        L: LockManager + Clone,
    > Clone for Service<T, E, L>
{
    fn clone(&self) -> Self {
        Service {
            gc_worker: self.gc_worker.clone(),
            storage: self.storage.clone(),
            cop: self.cop.clone(),
            ch: self.ch.clone(),
            snap_scheduler: self.snap_scheduler.clone(),
            enable_req_batch: self.enable_req_batch,
            grpc_thread_load: self.grpc_thread_load.clone(),
            readpool_normal_thread_load: self.readpool_normal_thread_load.clone(),
        }
    }
}

impl<T: RaftStoreRouter<RocksEngine> + 'static, E: Engine, L: LockManager> Service<T, E, L> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(
        storage: Storage<E, L>,
        gc_worker: GcWorker<E, T>,
        cop: Endpoint<E>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        grpc_thread_load: Arc<ThreadLoad>,
        readpool_normal_thread_load: Arc<ThreadLoad>,
        enable_req_batch: bool,
    ) -> Self {
        Service {
            gc_worker,
            storage,
            cop,
            ch,
            snap_scheduler,
            grpc_thread_load,
            readpool_normal_thread_load,
            enable_req_batch,
        }
    }
}

macro_rules! handle_request {
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident) => {
        fn $fn_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            let begin_instant = Instant::now_coarse();

            let resp = $future_name(&self.storage, req);
            let task = async move {
                let resp = resp.await?;
                sink.success(resp).await?;
                GRPC_MSG_HISTOGRAM_STATIC
                    .$fn_name
                    .observe(duration_to_sec(begin_instant.elapsed()));
                ServerResult::Ok(())
            }
            .map_err(|e| {
                debug!("kv rpc failed";
                    "request" => stringify!($fn_name),
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.$fn_name.inc();
            })
            .map(|_|());

            ctx.spawn(task);
        }
    }
}

impl<T: RaftStoreRouter<RocksEngine> + 'static, E: Engine, L: LockManager> Tikv
    for Service<T, E, L>
{
    handle_request!(kv_get, future_get, GetRequest, GetResponse);
    handle_request!(kv_scan, future_scan, ScanRequest, ScanResponse);
    handle_request!(
        kv_prewrite,
        future_prewrite,
        PrewriteRequest,
        PrewriteResponse
    );
    handle_request!(
        kv_pessimistic_lock,
        future_acquire_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    handle_request!(
        kv_pessimistic_rollback,
        future_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    handle_request!(kv_commit, future_commit, CommitRequest, CommitResponse);
    handle_request!(kv_cleanup, future_cleanup, CleanupRequest, CleanupResponse);
    handle_request!(
        kv_batch_get,
        future_batch_get,
        BatchGetRequest,
        BatchGetResponse
    );
    handle_request!(
        kv_batch_rollback,
        future_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    handle_request!(
        kv_txn_heart_beat,
        future_txn_heart_beat,
        TxnHeartBeatRequest,
        TxnHeartBeatResponse
    );
    handle_request!(
        kv_check_txn_status,
        future_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    handle_request!(
        kv_check_secondary_locks,
        future_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    handle_request!(
        kv_scan_lock,
        future_scan_lock,
        ScanLockRequest,
        ScanLockResponse
    );
    handle_request!(
        kv_resolve_lock,
        future_resolve_lock,
        ResolveLockRequest,
        ResolveLockResponse
    );
    handle_request!(
        kv_delete_range,
        future_delete_range,
        DeleteRangeRequest,
        DeleteRangeResponse
    );
    handle_request!(
        mvcc_get_by_key,
        future_mvcc_get_by_key,
        MvccGetByKeyRequest,
        MvccGetByKeyResponse
    );
    handle_request!(
        mvcc_get_by_start_ts,
        future_mvcc_get_by_start_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    handle_request!(raw_get, future_raw_get, RawGetRequest, RawGetResponse);
    handle_request!(
        raw_batch_get,
        future_raw_batch_get,
        RawBatchGetRequest,
        RawBatchGetResponse
    );
    handle_request!(raw_scan, future_raw_scan, RawScanRequest, RawScanResponse);
    handle_request!(
        raw_batch_scan,
        future_raw_batch_scan,
        RawBatchScanRequest,
        RawBatchScanResponse
    );
    handle_request!(raw_put, future_raw_put, RawPutRequest, RawPutResponse);
    handle_request!(
        raw_batch_put,
        future_raw_batch_put,
        RawBatchPutRequest,
        RawBatchPutResponse
    );
    handle_request!(
        raw_delete,
        future_raw_delete,
        RawDeleteRequest,
        RawDeleteResponse
    );
    handle_request!(
        raw_batch_delete,
        future_raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    handle_request!(
        raw_delete_range,
        future_raw_delete_range,
        RawDeleteRangeRequest,
        RawDeleteRangeResponse
    );

    fn kv_import(&mut self, _: RpcContext<'_>, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_gc(&mut self, ctx: RpcContext<'_>, _: GcRequest, sink: UnarySink<GcResponse>) {
        let e = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
        ctx.spawn(
            sink.fail(e)
                .unwrap_or_else(|e| error!("kv rpc failed"; "err" => ?e)),
        );
    }

    fn coprocessor(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        let begin_instant = Instant::now_coarse();
        let future = future_cop(&self.cop, Some(ctx.peer()), req);
        let task = async move {
            let resp = future.await?;
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .coprocessor
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "coprocessor",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn register_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegisterLockObserverRequest,
        sink: UnarySink<RegisterLockObserverResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.start_collecting(req.get_max_ts().into(), cb);

        let task = async move {
            // Here except for the receiving error of `futures::channel::oneshot`,
            // other errors will be returned as the successful response of rpc.
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = RegisterLockObserverResponse::default();
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .register_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "register_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.register_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn check_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: CheckLockObserverRequest,
        sink: UnarySink<CheckLockObserverResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self
            .gc_worker
            .get_collected_locks(req.get_max_ts().into(), cb);

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = CheckLockObserverResponse::default();
            match res {
                Ok((locks, is_clean)) => {
                    resp.set_is_clean(is_clean);
                    resp.set_locks(locks.into());
                }
                Err(e) => resp.set_error(format!("{}", e)),
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .check_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "check_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.check_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn remove_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RemoveLockObserverRequest,
        sink: UnarySink<RemoveLockObserverResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.stop_collecting(req.get_max_ts().into(), cb);

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = RemoveLockObserverResponse::default();
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .remove_lock_observer
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "remove_lock_observer",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.remove_lock_observer.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn physical_scan_lock(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: PhysicalScanLockRequest,
        sink: UnarySink<PhysicalScanLockResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.physical_scan_lock(
            req.take_context(),
            req.get_max_ts().into(),
            Key::from_raw(req.get_start_key()),
            req.get_limit() as _,
            cb,
        );

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = PhysicalScanLockResponse::default();
            match res {
                Ok(locks) => resp.set_locks(locks.into()),
                Err(e) => resp.set_error(format!("{}", e)),
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .physical_scan_lock
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "physical_scan_lock",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.physical_scan_lock.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn unsafe_destroy_range(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: UnsafeDestroyRangeRequest,
        sink: UnarySink<UnsafeDestroyRangeResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        // DestroyRange is a very dangerous operation. We don't allow passing MIN_KEY as start, or
        // MAX_KEY as end here.
        assert!(!req.get_start_key().is_empty());
        assert!(!req.get_end_key().is_empty());

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.unsafe_destroy_range(
            req.take_context(),
            Key::from_raw(&req.take_start_key()),
            Key::from_raw(&req.take_end_key()),
            cb,
        );

        let task = async move {
            let res = match res {
                Err(e) => Err(e),
                Ok(_) => f.await?,
            };
            let mut resp = UnsafeDestroyRangeResponse::default();
            // Region error is impossible here.
            if let Err(e) = res {
                resp.set_error(format!("{}", e));
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .unsafe_destroy_range
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "unsafe_destroy_range",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.unsafe_destroy_range.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn coprocessor_stream(
        &mut self,
        ctx: RpcContext<'_>,
        req: Request,
        mut sink: ServerStreamingSink<Response>,
    ) {
        let begin_instant = Instant::now_coarse();

        let mut stream = self
            .cop
            .parse_and_handle_stream_request(req, Some(ctx.peer()))
            .map(|resp| {
                GrpcResult::<(Response, WriteFlags)>::Ok((
                    resp,
                    WriteFlags::default().buffer_hint(true),
                ))
            });
        let future = async move {
            match sink.send_all(&mut stream).await.map_err(Error::from) {
                Ok(_) => {
                    GRPC_MSG_HISTOGRAM_STATIC
                        .coprocessor_stream
                        .observe(duration_to_sec(begin_instant.elapsed()));
                    let _ = sink.close().await;
                }
                Err(e) => {
                    debug!("kv rpc failed";
                        "request" => "coprocessor_stream",
                        "err" => ?e
                    );
                    GRPC_MSG_FAIL_COUNTER.coprocessor_stream.inc();
                }
            }
        };

        ctx.spawn(future);
    }

    fn raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |msg| {
                RAFT_MESSAGE_RECV_COUNTER.inc();
                let ret = ch.send_raft_msg(msg).map_err(Error::from);
                future::ready(ret)
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::raft send response fail"; "err" => ?e))
                .await;
        });
    }

    fn batch_raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchRaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        info!("batch_raft RPC is called, new gRPC stream established");
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |mut msgs| {
                let len = msgs.get_msgs().len();
                RAFT_MESSAGE_RECV_COUNTER.inc_by(len as i64);
                RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);
                for msg in msgs.take_msgs().into_iter() {
                    if let Err(e) = ch.send_raft_msg(msg) {
                        return future::err(Error::from(e));
                    }
                }
                future::ok(())
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::batch_raft send response fail"; "err" => ?e))
                .await;
        });
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        let task = SnapTask::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::new(RpcStatusCode::RESOURCE_EXHAUSTED, Some(err_msg));
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let region_id = req.get_context().get_region_id();
        let (cb, f) = paired_future_callback();
        let mut split_keys = if !req.get_split_key().is_empty() {
            vec![Key::from_raw(req.get_split_key()).into_encoded()]
        } else {
            req.take_split_keys()
                .into_iter()
                .map(|x| Key::from_raw(&x).into_encoded())
                .collect()
        };
        split_keys.sort();
        let req = CasualMessage::SplitRegion {
            region_epoch: req.take_context().take_region_epoch(),
            split_keys,
            callback: Callback::write(cb),
            source: ctx.peer().into(),
        };

        if let Err(e) = self.ch.send_casual_msg(region_id, req) {
            // Retrun region error instead a gRPC error.
            let mut resp = SplitRegionResponse::default();
            resp.set_region_error(raftstore_error_to_region_error(e, region_id));
            ctx.spawn(
                async move {
                    sink.success(resp).await?;
                    ServerResult::Ok(())
                }
                .map_err(|_| ())
                .map(|_| ()),
            );
            return;
        }

        let task = async move {
            let mut res = f.await?;
            let mut resp = SplitRegionResponse::default();
            if res.response.get_header().has_error() {
                resp.set_region_error(res.response.mut_header().take_error());
            } else {
                let admin_resp = res.response.mut_admin_response();
                let regions: Vec<_> = admin_resp.mut_splits().take_regions().into();
                if regions.len() < 2 {
                    error!(
                        "invalid split response";
                        "region_id" => region_id,
                        "resp" => ?admin_resp
                    );
                    resp.mut_region_error().set_message(format!(
                        "Internal Error: invalid response: {:?}",
                        admin_resp
                    ));
                } else {
                    if regions.len() == 2 {
                        resp.set_left(regions[0].clone());
                        resp.set_right(regions[1].clone());
                    }
                    resp.set_regions(regions.into());
                }
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .split_region
                .observe(duration_to_sec(begin_instant.elapsed()));
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "split_region",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.split_region.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn read_index(
        &mut self,
        ctx: RpcContext<'_>,
        req: ReadIndexRequest,
        sink: UnarySink<ReadIndexResponse>,
    ) {
        let begin_instant = Instant::now_coarse();

        let region_id = req.get_context().get_region_id();
        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        let mut inner_req = RaftRequest::default();
        inner_req.set_cmd_type(CmdType::ReadIndex);
        inner_req.mut_read_index().set_start_ts(req.get_start_ts());
        for r in req.get_ranges() {
            let mut range = kvproto::kvrpcpb::KeyRange::default();
            range.set_start_key(Key::from_raw(r.get_start_key()).into_encoded());
            range.set_end_key(Key::from_raw(r.get_end_key()).into_encoded());
            inner_req.mut_read_index().mut_key_ranges().push(range);
        }
        header.set_region_id(req.get_context().get_region_id());
        header.set_peer(req.get_context().get_peer().clone());
        header.set_region_epoch(req.get_context().get_region_epoch().clone());
        if req.get_context().get_term() != 0 {
            header.set_term(req.get_context().get_term());
        }
        header.set_sync_log(req.get_context().get_sync_log());
        header.set_read_quorum(true);
        cmd.set_header(header);
        cmd.set_requests(vec![inner_req].into());

        let (cb, f) = paired_future_callback();

        // We must deal with all requests which acquire read-quorum in raftstore-thread,
        // so just send it as an command.
        if let Err(e) = self.ch.send_command(cmd, Callback::Read(cb)) {
            // Retrun region error instead a gRPC error.
            let mut resp = ReadIndexResponse::default();
            resp.set_region_error(raftstore_error_to_region_error(e, region_id));
            ctx.spawn(
                async move {
                    sink.success(resp).await?;
                    ServerResult::Ok(())
                }
                .map_err(|_| ())
                .map(|_| ()),
            );
            return;
        }

        let task = async move {
            let mut res = f.await?;
            let mut resp = ReadIndexResponse::default();
            if res.response.get_header().has_error() {
                resp.set_region_error(res.response.mut_header().take_error());
            } else {
                let mut raft_resps = res.response.take_responses();
                if raft_resps.len() != 1 {
                    error!(
                        "invalid read index response";
                        "region_id" => region_id,
                        "response" => ?raft_resps
                    );
                    resp.mut_region_error().set_message(format!(
                        "Internal Error: invalid response: {:?}",
                        raft_resps
                    ));
                } else {
                    let mut read_index_resp = raft_resps[0].take_read_index();
                    if read_index_resp.has_locked() {
                        resp.set_locked(read_index_resp.take_locked());
                    } else {
                        resp.set_read_index(read_index_resp.get_read_index());
                    }
                }
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .read_index
                .observe(begin_instant.elapsed_secs());
            ServerResult::Ok(())
        }
        .map_err(|e| {
            debug!("kv rpc failed";
                "request" => "read_index",
                "err" => ?e
            );
            GRPC_MSG_FAIL_COUNTER.read_index.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn batch_commands(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchCommandsRequest>,
        mut sink: DuplexSink<BatchCommandsResponse>,
    ) {
        let (tx, rx) = unbounded(GRPC_MSG_NOTIFY_SIZE);

        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let storage = self.storage.clone();
        let cop = self.cop.clone();
        let enable_req_batch = self.enable_req_batch;
        let request_handler = stream.try_for_each(move |mut req| {
            let request_ids = req.take_request_ids();
            let requests: Vec<_> = req.take_requests().into();
            let mut batcher = if enable_req_batch && requests.len() > 2 {
                Some(ReqBatcher::new())
            } else {
                None
            };
            GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
            for (id, req) in request_ids.into_iter().zip(requests) {
                handle_batch_commands_request(&mut batcher, &storage, &cop, &peer, id, req, &tx);
            }
            if let Some(mut batch) = batcher {
                batch.commit(&storage, &tx);
                storage.release_snapshot();
            }
            future::ok(())
        });
        ctx.spawn(request_handler.unwrap_or_else(|e| error!("batch_commands error"; "err" => %e)));

        let thread_load = Arc::clone(&self.grpc_thread_load);
        let response_retriever = BatchReceiver::new(
            rx,
            GRPC_MSG_MAX_BATCH_SIZE,
            BatchCommandsResponse::default,
            BatchRespCollector,
        );

        let mut response_retriever = response_retriever
            .inspect(|r| GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64))
            .map(move |mut r| {
                r.set_transport_layer_load(thread_load.load() as u64);
                GrpcResult::<(BatchCommandsResponse, WriteFlags)>::Ok((
                    r,
                    WriteFlags::default().buffer_hint(false),
                ))
            });

        let send_task = async move {
            sink.send_all(&mut response_retriever).await?;
            sink.close().await?;
            Ok(())
        }
        .map_err(|e: grpcio::Error| {
            debug!("kv rpc failed";
                "request" => "batch_commands",
                "err" => ?e
            );
        })
        .map(|_| ());

        ctx.spawn(send_task);
    }

    fn ver_get(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerGetRequest,
        _sink: UnarySink<VerGetResponse>,
    ) {
        unimplemented!()
    }

    fn ver_batch_get(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerBatchGetRequest,
        _sink: UnarySink<VerBatchGetResponse>,
    ) {
        unimplemented!()
    }

    fn ver_mut(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerMutRequest,
        _sink: UnarySink<VerMutResponse>,
    ) {
        unimplemented!()
    }

    fn ver_batch_mut(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerBatchMutRequest,
        _sink: UnarySink<VerBatchMutResponse>,
    ) {
        unimplemented!()
    }

    fn ver_scan(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerScanRequest,
        _sink: UnarySink<VerScanResponse>,
    ) {
        unimplemented!()
    }

    fn ver_delete_range(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: VerDeleteRangeRequest,
        _sink: UnarySink<VerDeleteRangeResponse>,
    ) {
        unimplemented!()
    }

    fn batch_coprocessor(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: BatchRequest,
        _sink: ServerStreamingSink<BatchResponse>,
    ) {
        unimplemented!()
    }

    fn dispatch_mpp_task(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: DispatchTaskRequest,
        _sink: UnarySink<DispatchTaskResponse>,
    ) {
        unimplemented!()
    }

    fn cancel_mpp_task(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: CancelTaskRequest,
        _sink: UnarySink<CancelTaskResponse>,
    ) {
        unimplemented!()
    }

    fn establish_mpp_connection(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: EstablishMppConnectionRequest,
        _sink: ServerStreamingSink<MppDataPacket>,
    ) {
        unimplemented!()
    }

    fn raw_get_key_ttl(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        _: kvproto::kvrpcpb::RawGetKeyTtlRequest,
        sink: grpcio::UnarySink<kvproto::kvrpcpb::RawGetKeyTtlResponse>,
    ) {
        ctx.spawn(
            sink.fail(RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None))
                .unwrap_or_else(|_| ()),
        );
    }

    fn check_leader(
        &mut self,
        ctx: RpcContext<'_>,
        mut request: CheckLeaderRequest,
        sink: UnarySink<CheckLeaderResponse>,
    ) {
        let ts = request.get_ts();
        let leaders = request.take_regions().into();
        let (cb, resp) = paired_future_callback();
        let ch = self.ch.clone();
        let task = async move {
            ch.send_store_msg(StoreMsg::CheckLeader { leaders, cb })?;
            let regions = resp.await?;
            let mut resp = CheckLeaderResponse::default();
            resp.set_ts(ts);
            resp.set_regions(regions);
            sink.success(resp).await?;
            ServerResult::Ok(())
        }
        .map_err(|e| {
            warn!("cdc call CheckLeader failed"; "err" => ?e);
        })
        .map(|_| ());

        ctx.spawn(task);
    }
}

fn response_batch_commands_request<F>(
    id: u64,
    resp: F,
    tx: Sender<(u64, batch_commands_response::Response)>,
    begin_instant: Instant,
    label_enum: GrpcTypeKind,
) where
    F: Future<Output = Result<batch_commands_response::Response, ()>> + Send + 'static,
{
    let task = async move {
        if let Ok(resp) = resp.await {
            if tx.send_and_notify((id, resp)).is_err() {
                error!("KvService response batch commands fail");
            } else {
                GRPC_MSG_HISTOGRAM_STATIC
                    .get(label_enum)
                    .observe(begin_instant.elapsed_secs());
            }
        }
    };
    poll_future_notify(task);
}

fn handle_batch_commands_request<E: Engine, L: LockManager>(
    batcher: &mut Option<ReqBatcher>,
    storage: &Storage<E, L>,
    cop: &Endpoint<E>,
    peer: &str,
    id: u64,
    req: batch_commands_request::Request,
    tx: &Sender<(u64, batch_commands_response::Response)>,
) {
    // To simplify code and make the logic more clear.
    macro_rules! oneof {
        ($p:path) => {
            |resp| batch_commands_response::Response {
                cmd: Some($p(resp)),
                ..Default::default()
            }
        };
    }

    macro_rules! handle_cmd {
        ($($cmd: ident, $future_fn: ident ( $($arg: expr),* ), $metric_name: ident;)*) => {
            match req.cmd {
                None => {
                    // For some invalid requests.
                    let begin_instant = Instant::now();
                    let resp = future::ok(batch_commands_response::Response::default());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::invalid);
                },
                Some(batch_commands_request::request::Cmd::Get(req)) => {
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.maybe_commit(storage, tx);
                        req_batch.can_batch_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let resp = future_get(storage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::Get))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::kv_get);
                    }
                },
                Some(batch_commands_request::request::Cmd::RawGet(req)) => {
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.maybe_commit(storage, tx);
                        req_batch.can_batch_raw_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_raw_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let resp = future_raw_get(storage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::RawGet))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::raw_get);
                    }
                },
                $(Some(batch_commands_request::request::Cmd::$cmd(req)) => {
                    let begin_instant = Instant::now();
                    let resp = $future_fn($($arg,)* req)
                        .map_ok(oneof!(batch_commands_response::response::Cmd::$cmd))
                        .map_err(|_| GRPC_MSG_FAIL_COUNTER.$metric_name.inc());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::$metric_name);
                })*
                Some(batch_commands_request::request::Cmd::Import(_)) => unimplemented!(),
            }
        }
    }

    handle_cmd! {
        Scan, future_scan(storage), kv_scan;
        Prewrite, future_prewrite(storage), kv_prewrite;
        Commit, future_commit(storage), kv_commit;
        Cleanup, future_cleanup(storage), kv_cleanup;
        BatchGet, future_batch_get(storage), kv_batch_get;
        BatchRollback, future_batch_rollback(storage), kv_batch_rollback;
        TxnHeartBeat, future_txn_heart_beat(storage), kv_txn_heart_beat;
        CheckTxnStatus, future_check_txn_status(storage), kv_check_txn_status;
        CheckSecondaryLocks, future_check_secondary_locks(storage), kv_check_secondary_locks;
        ScanLock, future_scan_lock(storage), kv_scan_lock;
        ResolveLock, future_resolve_lock(storage), kv_resolve_lock;
        Gc, future_gc(), kv_gc;
        DeleteRange, future_delete_range(storage), kv_delete_range;
        RawBatchGet, future_raw_batch_get(storage), raw_batch_get;
        RawPut, future_raw_put(storage), raw_put;
        RawBatchPut, future_raw_batch_put(storage), raw_batch_put;
        RawDelete, future_raw_delete(storage), raw_delete;
        RawBatchDelete, future_raw_batch_delete(storage), raw_batch_delete;
        RawScan, future_raw_scan(storage), raw_scan;
        RawDeleteRange, future_raw_delete_range(storage), raw_delete_range;
        RawBatchScan, future_raw_batch_scan(storage), raw_batch_scan;
        VerGet, future_ver_get(storage), ver_get;
        VerBatchGet, future_ver_batch_get(storage), ver_batch_get;
        VerMut, future_ver_mut(storage), ver_mut;
        VerBatchMut, future_ver_batch_mut(storage), ver_batch_mut;
        VerScan, future_ver_scan(storage), ver_scan;
        VerDeleteRange, future_ver_delete_range(storage), ver_delete_range;
        Coprocessor, future_cop(cop, Some(peer.to_string())), coprocessor;
        PessimisticLock, future_acquire_pessimistic_lock(storage), kv_pessimistic_lock;
        PessimisticRollback, future_pessimistic_rollback(storage), kv_pessimistic_rollback;
        Empty, future_handle_empty(), invalid;
    }
}

async fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> ServerResult<BatchCommandsEmptyResponse> {
    let mut res = BatchCommandsEmptyResponse::default();
    res.set_test_id(req.get_test_id());
    // `BatchCommandsWaker` processes futures in notify. If delay_time is too small, notify
    // can be called immediately, so the future is polled recursively and lead to deadlock.
    if req.get_delay_time() < 10 {
        Ok(res)
    } else {
        let _ = tikv_util::timer::GLOBAL_TIMER_HANDLE
            .delay(
                std::time::Instant::now() + std::time::Duration::from_millis(req.get_delay_time()),
            )
            .compat()
            .await;
        Ok(res)
    }
}

fn future_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: GetRequest,
) -> impl Future<Output = ServerResult<GetResponse>> {
    let v = storage.get(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_version().into(),
    );

    async move {
        let v = v.await;
        let mut resp = GetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((val, statistics, perf_statistics_delta)) => {
                    let scan_detail_v2 = resp.mut_exec_details_v2().mut_scan_detail_v2();
                    statistics.write_scan_detail(scan_detail_v2);
                    perf_statistics_delta.write_scan_detail(scan_detail_v2);
                    match val {
                        Some(val) => resp.set_value(val),
                        None => resp.set_not_found(true),
                    }
                }
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        Ok(resp)
    }
}

fn future_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: ScanRequest,
) -> impl Future<Output = ServerResult<ScanResponse>> {
    let end_key = Key::from_raw_maybe_unbounded(req.get_end_key());
    let v = storage.scan(
        req.take_context(),
        Key::from_raw(req.get_start_key()),
        end_key,
        req.get_limit() as usize,
        req.get_sample_step() as usize,
        req.get_version().into(),
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = ScanResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(kv_res) => {
                    resp.set_pairs(map_kv_pairs(kv_res).into());
                }
                Err(e) => {
                    let key_error = extract_key_error(&e);
                    resp.set_error(key_error.clone());
                    // Set key_error in the first kv_pair for backward compatibility.
                    let mut pair = KvPair::default();
                    pair.set_error(key_error);
                    resp.mut_pairs().push(pair);
                }
            }
        }
        Ok(resp)
    }
}

fn future_batch_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: BatchGetRequest,
) -> impl Future<Output = ServerResult<BatchGetResponse>> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let v = storage.batch_get(req.take_context(), keys, req.get_version().into());

    async move {
        let v = v.await;
        let mut resp = BatchGetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((kv_res, statistics, perf_statistics_delta)) => {
                    let pairs = map_kv_pairs(kv_res);
                    let scan_detail_v2 = resp.mut_exec_details_v2().mut_scan_detail_v2();
                    statistics.write_scan_detail(scan_detail_v2);
                    perf_statistics_delta.write_scan_detail(scan_detail_v2);
                    resp.set_pairs(pairs.into());
                }
                Err(e) => {
                    let key_error = extract_key_error(&e);
                    resp.set_error(key_error.clone());
                    // Set key_error in the first kv_pair for backward compatibility.
                    let mut pair = KvPair::default();
                    pair.set_error(key_error);
                    resp.mut_pairs().push(pair);
                }
            }
        }
        Ok(resp)
    }
}

fn future_scan_lock<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: ScanLockRequest,
) -> impl Future<Output = ServerResult<ScanLockResponse>> {
    let start_key = Key::from_raw_maybe_unbounded(req.get_start_key());
    let end_key = Key::from_raw_maybe_unbounded(req.get_end_key());

    let v = storage.scan_lock(
        req.take_context(),
        req.get_max_version().into(),
        start_key,
        end_key,
        req.get_limit() as usize,
    );

    async move {
        let v = v.await;
        let mut resp = ScanLockResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(locks) => resp.set_locks(locks.into()),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        Ok(resp)
    }
}

async fn future_gc(_: GcRequest) -> ServerResult<GcResponse> {
    Err(Error::Grpc(GrpcError::RpcFailure(RpcStatus::new(
        RpcStatusCode::UNIMPLEMENTED,
        None,
    ))))
}

fn future_delete_range<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: DeleteRangeRequest,
) -> impl Future<Output = ServerResult<DeleteRangeResponse>> {
    let (cb, f) = paired_future_callback();
    let res = storage.delete_range(
        req.take_context(),
        Key::from_raw(req.get_start_key()),
        Key::from_raw(req.get_end_key()),
        req.get_notify_only(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = DeleteRangeResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawGetRequest,
) -> impl Future<Output = ServerResult<RawGetResponse>> {
    let v = storage.raw_get(req.take_context(), req.take_cf(), req.take_key());

    async move {
        let v = v.await;
        let mut resp = RawGetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(Some(val)) => resp.set_value(val),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        Ok(resp)
    }
}

fn future_raw_batch_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchGetRequest,
) -> impl Future<Output = ServerResult<RawBatchGetResponse>> {
    let keys = req.take_keys().into();
    let v = storage.raw_batch_get(req.take_context(), req.take_cf(), keys);

    async move {
        let v = v.await;
        let mut resp = RawBatchGetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_pairs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_put<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawPutRequest,
) -> impl Future<Output = ServerResult<RawPutResponse>> {
    let (cb, f) = paired_future_callback();
    let res = storage.raw_put(
        req.take_context(),
        req.take_cf(),
        req.take_key(),
        req.take_value(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawPutResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_batch_put<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchPutRequest,
) -> impl Future<Output = ServerResult<RawBatchPutResponse>> {
    let cf = req.take_cf();
    let pairs = req
        .take_pairs()
        .into_iter()
        .map(|mut x| (x.take_key(), x.take_value()))
        .collect();

    let (cb, f) = paired_future_callback();
    let res = storage.raw_batch_put(req.take_context(), cf, pairs, cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawBatchPutResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_delete<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRequest,
) -> impl Future<Output = ServerResult<RawDeleteResponse>> {
    let (cb, f) = paired_future_callback();
    let res = storage.raw_delete(req.take_context(), req.take_cf(), req.take_key(), cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawDeleteResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_batch_delete<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Output = ServerResult<RawBatchDeleteResponse>> {
    let cf = req.take_cf();
    let keys = req.take_keys().into();
    let (cb, f) = paired_future_callback();
    let res = storage.raw_batch_delete(req.take_context(), cf, keys, cb);

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawBatchDeleteResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

fn future_raw_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawScanRequest,
) -> impl Future<Output = ServerResult<RawScanResponse>> {
    let end_key = if req.get_end_key().is_empty() {
        None
    } else {
        Some(req.take_end_key())
    };
    let v = storage.raw_scan(
        req.take_context(),
        req.take_cf(),
        req.take_start_key(),
        end_key,
        req.get_limit() as usize,
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = RawScanResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_kvs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_batch_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchScanRequest,
) -> impl Future<Output = ServerResult<RawBatchScanResponse>> {
    let v = storage.raw_batch_scan(
        req.take_context(),
        req.take_cf(),
        req.take_ranges().into(),
        req.get_each_limit() as usize,
        req.get_key_only(),
        req.get_reverse(),
    );

    async move {
        let v = v.await;
        let mut resp = RawBatchScanResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_kvs(extract_kv_pairs(v).into());
        }
        Ok(resp)
    }
}

fn future_raw_delete_range<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRangeRequest,
) -> impl Future<Output = ServerResult<RawDeleteRangeResponse>> {
    let (cb, f) = paired_future_callback();
    let res = storage.raw_delete_range(
        req.take_context(),
        req.take_cf(),
        req.take_start_key(),
        req.take_end_key(),
        cb,
    );

    async move {
        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await?,
        };
        let mut resp = RawDeleteRangeResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        Ok(resp)
    }
}

// unimplemented
fn future_ver_get<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerGetRequest,
) -> impl Future<Output = ServerResult<VerGetResponse>> {
    let resp = VerGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_get<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerBatchGetRequest,
) -> impl Future<Output = ServerResult<VerBatchGetResponse>> {
    let resp = VerBatchGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_mut<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerMutRequest,
) -> impl Future<Output = ServerResult<VerMutResponse>> {
    let resp = VerMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_mut<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerBatchMutRequest,
) -> impl Future<Output = ServerResult<VerBatchMutResponse>> {
    let resp = VerBatchMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_scan<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerScanRequest,
) -> impl Future<Output = ServerResult<VerScanResponse>> {
    let resp = VerScanResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_delete_range<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerDeleteRangeRequest,
) -> impl Future<Output = ServerResult<VerDeleteRangeResponse>> {
    let resp = VerDeleteRangeResponse::default();
    future::ok(resp)
}

fn future_cop<E: Engine>(
    cop: &Endpoint<E>,
    peer: Option<String>,
    req: Request,
) -> impl Future<Output = ServerResult<Response>> {
    let ret = cop.parse_and_handle_unary_request(req, peer);
    async move { Ok(ret.await) }
}

macro_rules! txn_command_future {
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) $prelude: stmt; ($v: ident, $resp: ident) { $else_branch: expr }) => {
        fn $fn_name<E: Engine, L: LockManager>(
            storage: &Storage<E, L>,
            $req: $req_ty,
        ) -> impl Future<Output = ServerResult<$resp_ty>> {
            $prelude
            let (cb, f) = paired_future_callback();
            let res = storage.sched_txn_command($req.into(), cb);

            async move {
                let $v = match res {
                    Err(e) => Err(e),
                    Ok(_) => f.await?,
                };
                let mut $resp = $resp_ty::default();
                if let Some(err) = extract_region_error(&$v) {
                    $resp.set_region_error(err);
                } else {
                    $else_branch;
                }
                Ok($resp)
            }
        }
    };
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($v: ident, $resp: ident) { $else_branch: expr }) => {
        txn_command_future!($fn_name, $req_ty, $resp_ty, (req) {}; ($v, $resp) { $else_branch });
    };
}

txn_command_future!(future_prewrite, PrewriteRequest, PrewriteResponse, (v, resp) {{
    if let Ok(v) = &v {
        resp.set_min_commit_ts(v.min_commit_ts.into_inner());
        resp.set_one_pc_commit_ts(v.one_pc_commit_ts.into_inner());
    }
    resp.set_errors(extract_key_errors(v.map(|v| v.locks)).into());
}});
txn_command_future!(future_acquire_pessimistic_lock, PessimisticLockRequest, PessimisticLockResponse, (v, resp) {
    match v {
        Ok(Ok(res)) => {
            let (values, not_founds) = res.into_values_and_not_founds();
            resp.set_values(values.into());
            resp.set_not_founds(not_founds);
        },
        Err(e) | Ok(Err(e)) => resp.set_errors(vec![extract_key_error(&e)].into()),
    }
});
txn_command_future!(future_pessimistic_rollback, PessimisticRollbackRequest, PessimisticRollbackResponse, (v, resp) {
    resp.set_errors(extract_key_errors(v).into())
});
txn_command_future!(future_batch_rollback, BatchRollbackRequest, BatchRollbackResponse, (v, resp) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    }
});
txn_command_future!(future_resolve_lock, ResolveLockRequest, ResolveLockResponse, (v, resp) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    }
});
txn_command_future!(future_commit, CommitRequest, CommitResponse, (v, resp) {
    match v {
        Ok(TxnStatus::Committed { commit_ts }) => {
            resp.set_commit_version(commit_ts.into_inner())
        }
        Ok(_) => unreachable!(),
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_cleanup, CleanupRequest, CleanupResponse, (v, resp) {
    if let Err(e) = v {
        if let Some(ts) = extract_committed(&e) {
            resp.set_commit_version(ts.into_inner());
        } else {
            resp.set_error(extract_key_error(&e));
        }
    }
});
txn_command_future!(future_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse, (v, resp) {
    match v {
        Ok(txn_status) => {
            if let TxnStatus::Uncommitted { lock, .. } = txn_status {
                resp.set_lock_ttl(lock.ttl);
            } else {
                unreachable!();
            }
        }
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_check_txn_status, CheckTxnStatusRequest, CheckTxnStatusResponse,
    (v, resp) {
        match v {
            Ok(txn_status) => match txn_status {
                TxnStatus::RolledBack => resp.set_action(Action::NoAction),
                TxnStatus::TtlExpire => resp.set_action(Action::TtlExpireRollback),
                TxnStatus::LockNotExist => resp.set_action(Action::LockNotExistRollback),
                TxnStatus::Committed { commit_ts } => {
                    resp.set_commit_version(commit_ts.into_inner())
                }
                TxnStatus::Uncommitted { lock, min_commit_ts_pushed } => {
                    if min_commit_ts_pushed {
                        resp.set_action(Action::MinCommitTsPushed);
                    }
                    resp.set_lock_ttl(lock.ttl);
                    let primary = lock.primary.clone();
                    resp.set_lock_info(lock.into_lock_info(primary));
                }
                TxnStatus::PessimisticRollBack => resp.set_action(Action::TtlExpirePessimisticRollback),
                TxnStatus::LockNotExistDoNothing => resp.set_action(Action::LockNotExistDoNothing),
            },
            Err(e) => resp.set_error(extract_key_error(&e)),
        }
});
txn_command_future!(future_check_secondary_locks, CheckSecondaryLocksRequest, CheckSecondaryLocksResponse, (status, resp) {
    match status {
        Ok(SecondaryLocksStatus::Locked(locks)) => {
            resp.set_locks(locks.into());
        },
        Ok(SecondaryLocksStatus::Committed(ts)) => {
            resp.set_commit_ts(ts.into_inner());
        },
        Ok(SecondaryLocksStatus::RolledBack) => {},
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_mvcc_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse, (v, resp) {
    match v {
        Ok(mvcc) => resp.set_info(mvcc.into_proto()),
        Err(e) => resp.set_error(format!("{}", e)),
    }
});
txn_command_future!(future_mvcc_get_by_start_ts, MvccGetByStartTsRequest, MvccGetByStartTsResponse, (v, resp) {
    match v {
        Ok(Some((k, vv))) => {
            resp.set_key(k.into_raw().unwrap());
            resp.set_info(vv.into_proto());
        }
        Ok(None) => {
            resp.set_info(Default::default());
        }
        Err(e) => resp.set_error(format!("{}", e)),
    }
});

#[cfg(feature = "protobuf-codec")]
pub mod batch_commands_response {
    pub type Response = kvproto::tikvpb::BatchCommandsResponseResponse;

    pub mod response {
        pub type Cmd = kvproto::tikvpb::BatchCommandsResponse_Response_oneof_cmd;
    }
}

#[cfg(feature = "protobuf-codec")]
pub mod batch_commands_request {
    pub type Request = kvproto::tikvpb::BatchCommandsRequestRequest;

    pub mod request {
        pub type Cmd = kvproto::tikvpb::BatchCommandsRequest_Request_oneof_cmd;
    }
}

#[cfg(feature = "prost-codec")]
pub use kvproto::tikvpb::batch_commands_request;
#[cfg(feature = "prost-codec")]
pub use kvproto::tikvpb::batch_commands_response;

struct BatchRespCollector;
impl BatchCollector<BatchCommandsResponse, (u64, batch_commands_response::Response)>
    for BatchRespCollector
{
    fn collect(
        &mut self,
        v: &mut BatchCommandsResponse,
        e: (u64, batch_commands_response::Response),
    ) -> Option<(u64, batch_commands_response::Response)> {
        v.mut_request_ids().push(e.0);
        v.mut_responses().push(e.1);
        None
    }
}

fn raftstore_error_to_region_error(e: RaftStoreError, region_id: u64) -> RegionError {
    if let RaftStoreError::Transport(DiscardReason::Disconnected) = e {
        // `From::from(RaftStoreError) -> RegionError` treats `Disconnected` as `Other`.
        let mut region_error = RegionError::default();
        let region_not_found = RegionNotFound {
            region_id,
            ..Default::default()
        };
        region_error.set_region_not_found(region_not_found);
        return region_error;
    }
    e.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use std::thread;

    #[test]
    fn test_poll_future_notify_with_slow_source() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();

        thread::Builder::new()
            .name("source".to_owned())
            .spawn(move || {
                block_on(signal_rx).unwrap();
                tx.send(100).unwrap();
            })
            .unwrap();

        let (tx1, rx1) = oneshot::channel::<usize>();
        let task = async move {
            let i = rx.await.unwrap();
            assert_eq!(thread::current().name(), Some("source"));
            tx1.send(i + 100).unwrap();
        };
        poll_future_notify(task);
        signal_tx.send(()).unwrap();
        assert_eq!(block_on(rx1).unwrap(), 200);
    }

    #[test]
    fn test_poll_future_notify_with_slow_poller() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();
        thread::Builder::new()
            .name("source".to_owned())
            .spawn(move || {
                tx.send(100).unwrap();
                signal_tx.send(()).unwrap();
            })
            .unwrap();

        let (tx1, rx1) = oneshot::channel::<usize>();
        block_on(signal_rx).unwrap();
        let task = async move {
            let i = rx.await.unwrap();
            assert_ne!(thread::current().name(), Some("source"));
            tx1.send(i + 100).unwrap();
        };
        poll_future_notify(task);
        assert_eq!(block_on(rx1).unwrap(), 200);
    }
}
