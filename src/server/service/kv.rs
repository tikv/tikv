// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tikv_util::time::{duration_to_sec, Instant};

use crate::coprocessor::Endpoint;
use crate::server::gc_worker::GcWorker;
use crate::server::load_statistics::ThreadLoad;
use crate::server::metrics::*;
use crate::server::service::batch::ReqBatcher;
use crate::server::snap::Task as SnapTask;
use crate::server::Error;
use crate::storage::{
    errors::{
        extract_committed, extract_key_error, extract_key_errors, extract_kv_pairs,
        extract_region_error,
    },
    kv::Engine,
    lock_manager::LockManager,
    PointGetCommand, Storage, TxnStatus,
};
use engine_rocks::RocksEngine;
use futures::executor::{self, Notify, Spawn};
use futures::future::Either;
use futures::{future, Async, Future, Sink, Stream};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus,
    RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::coprocessor::*;
use kvproto::kvrpcpb::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use kvproto::raft_serverpb::*;
use kvproto::tikvpb::*;
use raftstore::router::RaftStoreRouter;
use raftstore::store::{Callback, CasualMessage};
use tikv_util::future::{paired_future_callback, AndThenWith};
use tikv_util::mpsc::batch::{unbounded, BatchCollector, BatchReceiver, Sender};
use tikv_util::security::{check_common_name, SecurityManager};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Scheduler;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use txn_types::{self, Key};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
#[derive(Clone)]
pub struct Service<T: RaftStoreRouter<RocksEngine> + 'static, E: Engine, L: LockManager> {
    /// Used to handle requests related to GC.
    gc_worker: GcWorker<E>,
    // For handling KV requests.
    storage: Storage<E, L>,
    // For handling coprocessor requests.
    cop: Endpoint<E>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,

    enable_req_batch: bool,

    req_batch_wait_duration: Option<Duration>,

    timer_pool: Arc<Mutex<ThreadPool>>,

    grpc_thread_load: Arc<ThreadLoad>,

    readpool_normal_thread_load: Arc<ThreadLoad>,

    security_mgr: Arc<SecurityManager>,
}

impl<T: RaftStoreRouter<RocksEngine> + 'static, E: Engine, L: LockManager> Service<T, E, L> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(
        storage: Storage<E, L>,
        gc_worker: GcWorker<E>,
        cop: Endpoint<E>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        grpc_thread_load: Arc<ThreadLoad>,
        readpool_normal_thread_load: Arc<ThreadLoad>,
        enable_req_batch: bool,
        req_batch_wait_duration: Option<Duration>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let timer_pool = Arc::new(Mutex::new(
            ThreadPoolBuilder::new()
                .pool_size(1)
                .name_prefix("req_batch_timer_guard")
                .build(),
        ));
        Service {
            gc_worker,
            storage,
            cop,
            ch,
            snap_scheduler,
            grpc_thread_load,
            readpool_normal_thread_load,
            timer_pool,
            enable_req_batch,
            req_batch_wait_duration,
            security_mgr,
        }
    }

    fn send_fail_status<M>(
        &self,
        ctx: RpcContext<'_>,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

macro_rules! handle_request {
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident) => {
        fn $fn_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
                return;
            }
            let begin_instant = Instant::now_coarse();
            let future = $future_name(&self.storage, req)
                .and_then(|res| sink.success(res).map_err(Error::from))
                .map(move |_| GRPC_MSG_HISTOGRAM_STATIC.$fn_name.observe(duration_to_sec(begin_instant.elapsed())))
                .map_err(move |e| {
                    debug!("kv rpc failed";
                        "request" => stringify!($fn_name),
                        "err" => ?e
                    );
                    GRPC_MSG_FAIL_COUNTER.$fn_name.inc();
                });

            ctx.spawn(future);
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

    fn kv_gc(&mut self, ctx: RpcContext<'_>, req: GcRequest, sink: UnarySink<GcResponse>) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();
        let future = future_gc(&self.gc_worker, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .kv_gc
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_gc",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_gc.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();
        let future = future_cop(&self.cop, Some(ctx.peer()), req)
            .and_then(|resp| sink.success(resp).map_err(Error::from))
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .coprocessor
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "coprocessor",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
            });

        ctx.spawn(future);
    }

    fn register_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegisterLockObserverRequest,
        sink: UnarySink<RegisterLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.start_collecting(req.get_max_ts().into(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = RegisterLockObserverResponse::default();
                if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .register_lock_observer
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "register_lock_observer",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.register_lock_observer.inc();
            });

        ctx.spawn(future);
    }

    fn check_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: CheckLockObserverRequest,
        sink: UnarySink<CheckLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self
            .gc_worker
            .get_collected_locks(req.get_max_ts().into(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = CheckLockObserverResponse::default();
                match v {
                    Ok((locks, is_clean)) => {
                        resp.set_is_clean(is_clean);
                        resp.set_locks(locks.into());
                    }
                    Err(e) => resp.set_error(format!("{}", e)),
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .check_lock_observer
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "check_lock_observer",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.check_lock_observer.inc();
            });

        ctx.spawn(future);
    }

    fn remove_lock_observer(
        &mut self,
        ctx: RpcContext<'_>,
        req: RemoveLockObserverRequest,
        sink: UnarySink<RemoveLockObserverResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.stop_collecting(req.get_max_ts().into(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = RemoveLockObserverResponse::default();
                if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .remove_lock_observer
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "remove_lock_observer",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.remove_lock_observer.inc();
            });

        ctx.spawn(future);
    }

    fn physical_scan_lock(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: PhysicalScanLockRequest,
        sink: UnarySink<PhysicalScanLockResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let (cb, f) = paired_future_callback();
        let res = self.gc_worker.physical_scan_lock(
            req.take_context(),
            req.get_max_ts().into(),
            Key::from_raw(req.get_start_key()),
            req.get_limit() as _,
            cb,
        );

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = PhysicalScanLockResponse::default();
                match v {
                    Ok(locks) => resp.set_locks(locks.into()),
                    Err(e) => resp.set_error(format!("{}", e)),
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .physical_scan_lock
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "physical_scan_lock",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.physical_scan_lock.inc();
            });

        ctx.spawn(future);
    }

    fn unsafe_destroy_range(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: UnsafeDestroyRangeRequest,
        sink: UnarySink<UnsafeDestroyRangeResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
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

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = UnsafeDestroyRangeResponse::default();
                // Region error is impossible here.
                if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .unsafe_destroy_range
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "unsafe_destroy_range",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.unsafe_destroy_range.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor_stream(
        &mut self,
        ctx: RpcContext<'_>,
        req: Request,
        sink: ServerStreamingSink<Response>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let stream = self
            .cop
            .parse_and_handle_stream_request(req, Some(ctx.peer()))
            .map(|resp| (resp, WriteFlags::default().buffer_hint(true)))
            .map_err(|e| {
                let code = RpcStatusCode::UNKNOWN;
                let msg = Some(format!("{:?}", e));
                GrpcError::RpcFailure(RpcStatus::new(code, msg))
            });
        let future = sink
            .send_all(stream)
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .coprocessor_stream
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(Error::from)
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "coprocessor_stream",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.coprocessor_stream.inc();
            });

        ctx.spawn(future);
    }

    fn raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let ch = self.ch.clone();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |msg| {
                    RAFT_MESSAGE_RECV_COUNTER.inc();
                    ch.send_raft_msg(msg).map_err(Error::from)
                })
                .then(|res| {
                    let status = match res {
                        Err(e) => {
                            let msg = format!("{:?}", e);
                            error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                            RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                        }
                        Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
                    };
                    sink.fail(status)
                        .map_err(|e| error!("KvService::raft send response fail"; "err" => ?e))
                }),
        );
    }

    fn batch_raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchRaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        info!("batch_raft RPC is called, new gRPC stream established");
        let ch = self.ch.clone();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |mut msgs| {
                    let len = msgs.get_msgs().len();
                    RAFT_MESSAGE_RECV_COUNTER.inc_by(len as i64);
                    RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);
                    for msg in msgs.take_msgs().into_iter() {
                        if let Err(e) = ch.send_raft_msg(msg) {
                            return Err(Error::from(e));
                        }
                    }
                    Ok(())
                })
                .then(|res| {
                    let status = match res {
                        Err(e) => {
                            let msg = format!("{:?}", e);
                            error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                            RpcStatus::new(RpcStatusCode::UNKNOWN, Some(msg))
                        }
                        Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN, None),
                    };
                    sink.fail(status).map_err(
                        |e| error!("KvService::batch_raft send response fail"; "err" => ?e),
                    )
                }),
        )
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let task = SnapTask::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::new(RpcStatusCode::RESOURCE_EXHAUSTED, Some(err_msg));
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }

    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let region_id = req.get_context().get_region_id();
        let (cb, future) = paired_future_callback();
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
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.casual_send(region_id, req) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::RESOURCE_EXHAUSTED);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(move |mut v| {
                let mut resp = SplitRegionResponse::default();
                if v.response.get_header().has_error() {
                    resp.set_region_error(v.response.mut_header().take_error());
                } else {
                    let admin_resp = v.response.mut_admin_response();
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
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .split_region
                    .observe(duration_to_sec(begin_instant.elapsed()))
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "split_region",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.split_region.inc();
            });

        ctx.spawn(future);
    }

    fn read_index(
        &mut self,
        ctx: RpcContext<'_>,
        req: ReadIndexRequest,
        sink: UnarySink<ReadIndexResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let begin_instant = Instant::now_coarse();

        let region_id = req.get_context().get_region_id();
        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        let mut inner_req = RaftRequest::default();
        inner_req.set_cmd_type(CmdType::ReadIndex);
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

        let (cb, future) = paired_future_callback();

        if let Err(e) = self.ch.send_command(cmd, Callback::Read(cb)) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::RESOURCE_EXHAUSTED);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(move |mut v| {
                let mut resp = ReadIndexResponse::default();
                if v.response.get_header().has_error() {
                    resp.set_region_error(v.response.mut_header().take_error());
                } else {
                    let raft_resps = v.response.get_responses();
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
                        let read_index = raft_resps[0].get_read_index().get_read_index();
                        resp.set_read_index(read_index);
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(move |_| {
                GRPC_MSG_HISTOGRAM_STATIC
                    .read_index
                    .observe(begin_instant.elapsed_secs())
            })
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "read_index",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.read_index.inc();
            });

        ctx.spawn(future);
    }

    fn batch_commands(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchCommandsRequest>,
        sink: DuplexSink<BatchCommandsResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let (tx, rx) = unbounded(GRPC_MSG_NOTIFY_SIZE);

        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let storage = self.storage.clone();
        let cop = self.cop.clone();
        let gc_worker = self.gc_worker.clone();
        if self.enable_req_batch {
            let stopped = Arc::new(AtomicBool::new(false));
            let req_batcher = ReqBatcher::new(
                tx.clone(),
                self.req_batch_wait_duration,
                Arc::clone(&self.readpool_normal_thread_load),
            );
            let req_batcher = Arc::new(Mutex::new(req_batcher));
            if let Some(duration) = self.req_batch_wait_duration {
                let storage = storage.clone();
                let req_batcher = req_batcher.clone();
                let req_batcher2 = req_batcher.clone();
                let stopped = Arc::clone(&stopped);
                let start = std::time::Instant::now();
                let timer = GLOBAL_TIMER_HANDLE.clone();
                self.timer_pool.lock().unwrap().spawn(
                    timer
                        .interval(start, duration)
                        .take_while(move |_| {
                            // only stop timer when no more incoming and old batch is submitted.
                            future::ok(
                                !stopped.load(Ordering::Relaxed)
                                    || !req_batcher2.lock().unwrap().is_empty(),
                            )
                        })
                        .for_each(move |_| {
                            req_batcher.lock().unwrap().should_submit(&storage);
                            Ok(())
                        })
                        .map_err(|e| error!("batch_commands timer errored"; "err" => ?e)),
                );
            }
            let request_handler = stream.for_each(move |mut req| {
                let request_ids = req.take_request_ids();
                let requests: Vec<_> = req.take_requests().into();
                GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
                for (id, mut req) in request_ids.into_iter().zip(requests) {
                    if !req_batcher.lock().unwrap().filter(id, &mut req) {
                        handle_batch_commands_request(
                            &storage,
                            &gc_worker,
                            &cop,
                            &peer,
                            id,
                            req,
                            tx.clone(),
                        );
                    }
                }
                req_batcher.lock().unwrap().maybe_submit(&storage);
                future::ok(())
            });
            ctx.spawn(
                request_handler
                    .map_err(|e| error!("batch_commands error"; "err" => %e))
                    .and_then(move |_| {
                        // signal timer guard to stop polling
                        stopped.store(true, Ordering::Relaxed);
                        Ok(())
                    }),
            );
        } else {
            let request_handler = stream.for_each(move |mut req| {
                let request_ids = req.take_request_ids();
                let requests: Vec<_> = req.take_requests().into();
                GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
                for (id, req) in request_ids.into_iter().zip(requests) {
                    handle_batch_commands_request(
                        &storage,
                        &gc_worker,
                        &cop,
                        &peer,
                        id,
                        req,
                        tx.clone(),
                    );
                }
                future::ok(())
            });
            ctx.spawn(request_handler.map_err(|e| error!("batch_commands error"; "err" => %e)));
        };

        let thread_load = Arc::clone(&self.grpc_thread_load);
        let response_retriever = BatchReceiver::new(
            rx,
            GRPC_MSG_MAX_BATCH_SIZE,
            BatchCommandsResponse::default,
            BatchRespCollector,
        );

        let response_retriever = response_retriever
            .inspect(|r| GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64))
            .map(move |mut r| {
                r.set_transport_layer_load(thread_load.load() as u64);
                (r, WriteFlags::default().buffer_hint(false))
            })
            .map_err(|e| {
                let msg = Some(format!("{:?}", e));
                GrpcError::RpcFailure(RpcStatus::new(RpcStatusCode::UNKNOWN, msg))
            });

        ctx.spawn(sink.send_all(response_retriever).map(|_| ()).map_err(|e| {
            debug!("kv rpc failed";
                "request" => "batch_commands",
                "err" => ?e
            );
        }));
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
}

fn response_batch_commands_request<F>(
    id: u64,
    resp: F,
    tx: Sender<(u64, batch_commands_response::Response)>,
    begin_instant: Instant,
    label_enum: GrpcTypeKind,
) where
    F: Future<Item = batch_commands_response::Response, Error = ()> + Send + 'static,
{
    let f = resp.and_then(move |resp| {
        if tx.send_and_notify((id, resp)).is_err() {
            error!("KvService response batch commands fail");
            return Err(());
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .get(label_enum)
            .observe(begin_instant.elapsed_secs());
        Ok(())
    });
    poll_future_notify(f);
}

// BatchCommandsNotify is used to make business pool notifiy completion queues directly.
struct BatchCommandsNotify<F>(Arc<Mutex<Option<Spawn<F>>>>);
impl<F> Clone for BatchCommandsNotify<F> {
    fn clone(&self) -> BatchCommandsNotify<F> {
        BatchCommandsNotify(Arc::clone(&self.0))
    }
}
impl<F> Notify for BatchCommandsNotify<F>
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn notify(&self, id: usize) {
        let n = Arc::new(self.clone());
        let mut s = self.0.lock().unwrap();
        match s.as_mut().map(|spawn| spawn.poll_future_notify(&n, id)) {
            Some(Ok(Async::NotReady)) | None => {}
            _ => *s = None,
        };
    }
}

pub fn poll_future_notify<F: Future<Item = (), Error = ()> + Send + 'static>(f: F) {
    let spawn = Arc::new(Mutex::new(Some(executor::spawn(f))));
    let notify = BatchCommandsNotify(spawn);
    notify.notify(0);
}

fn handle_batch_commands_request<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    gc_worker: &GcWorker<E>,
    cop: &Endpoint<E>,
    peer: &str,
    id: u64,
    req: batch_commands_request::Request,
    tx: Sender<(u64, batch_commands_response::Response)>,
) {
    // To simplify code and make the logic more clear.
    macro_rules! oneof {
        ($p:path) => {
            |resp| {
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some($p(resp));
                res
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
                    response_batch_commands_request(id, resp, tx, begin_instant, GrpcTypeKind::invalid);
                }
                $(Some(batch_commands_request::request::Cmd::$cmd(req)) => {
                    let begin_instant = Instant::now();
                    let resp = $future_fn($($arg,)* req)
                        .map(oneof!(batch_commands_response::response::Cmd::$cmd))
                        .map_err(|_| GRPC_MSG_FAIL_COUNTER.$metric_name.inc());
                    response_batch_commands_request(id, resp, tx, begin_instant, GrpcTypeKind::$metric_name);
                })*
                Some(batch_commands_request::request::Cmd::Import(_)) => unimplemented!(),
            }
        }
    }

    handle_cmd! {
        Get, future_get(storage), kv_get;
        Scan, future_scan(storage), kv_scan;
        Prewrite, future_prewrite(storage), kv_prewrite;
        Commit, future_commit(storage), kv_commit;
        Cleanup, future_cleanup(storage), kv_cleanup;
        BatchGet, future_batch_get(storage), kv_batch_get;
        BatchRollback, future_batch_rollback(storage), kv_batch_rollback;
        TxnHeartBeat, future_txn_heart_beat(storage), kv_txn_heart_beat;
        CheckTxnStatus, future_check_txn_status(storage), kv_check_txn_status;
        ScanLock, future_scan_lock(storage), kv_scan_lock;
        ResolveLock, future_resolve_lock(storage), kv_resolve_lock;
        Gc, future_gc(gc_worker), kv_gc;
        DeleteRange, future_delete_range(storage), kv_delete_range;
        RawGet, future_raw_get(storage), raw_get;
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

fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> impl Future<Item = BatchCommandsEmptyResponse, Error = Error> {
    let mut res = BatchCommandsEmptyResponse::default();
    res.set_test_id(req.get_test_id());
    // `BatchCommandsNotify` processes futures in notify. If delay_time is too small, notify
    // can be called immediately, so the future is polled recursively and lead to deadlock.
    if req.get_delay_time() < 10 {
        Either::A(future::result(Ok(res)))
    } else {
        Either::B(
            tikv_util::timer::GLOBAL_TIMER_HANDLE
                .delay(
                    std::time::Instant::now()
                        + std::time::Duration::from_millis(req.get_delay_time()),
                )
                .map(move |_| res)
                .map_err(|_| unreachable!()),
        )
    }
}

fn future_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: GetRequest,
) -> impl Future<Item = GetResponse, Error = Error> {
    storage
        .get(
            req.take_context(),
            Key::from_raw(req.get_key()),
            req.get_version().into(),
        )
        .then(|v| {
            let mut resp = GetResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                match v {
                    Ok(Some(val)) => resp.set_value(val),
                    Ok(None) => resp.set_not_found(true),
                    Err(e) => resp.set_error(extract_key_error(&e)),
                }
            }
            Ok(resp)
        })
}

pub fn future_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    requests: Vec<u64>,
    commands: Vec<PointGetCommand>,
) -> impl Future<Item = (), Error = ()> {
    let begin_instant = Instant::now_coarse();

    storage.batch_get_command(commands).then(move |v| {
        match v {
            Ok(v) => {
                if requests.len() != v.len() {
                    error!("KvService batch response size mismatch");
                }
                for (req, v) in requests.into_iter().zip(v.into_iter()) {
                    let mut resp = GetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok(Some(val)) => resp.set_value(val),
                            Ok(None) => resp.set_not_found(true),
                            Err(e) => resp.set_error(extract_key_error(&e)),
                        }
                    }
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                    if tx.send_and_notify((req, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = GetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(extract_key_error(&e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .kv_batch_get_command
            .observe(begin_instant.elapsed_secs());
        Ok(())
    })
}

fn future_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: ScanRequest,
) -> impl Future<Item = ScanResponse, Error = Error> {
    let end_key = if req.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_raw(req.get_end_key()))
    };

    storage
        .scan(
            req.take_context(),
            Key::from_raw(req.get_start_key()),
            end_key,
            req.get_limit() as usize,
            req.get_version().into(),
            req.get_key_only(),
            req.get_reverse(),
        )
        .then(|v| {
            let mut resp = ScanResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_pairs(extract_kv_pairs(v).into());
            }
            Ok(resp)
        })
}

fn future_batch_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: BatchGetRequest,
) -> impl Future<Item = BatchGetResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    storage
        .batch_get(req.take_context(), keys, req.get_version().into())
        .then(|v| {
            let mut resp = BatchGetResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_pairs(extract_kv_pairs(v).into());
            }
            Ok(resp)
        })
}

fn future_gc<E: Engine>(
    gc_worker: &GcWorker<E>,
    mut req: GcRequest,
) -> impl Future<Item = GcResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = gc_worker.gc(req.take_context(), req.get_safe_point().into(), cb);

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = GcResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(extract_key_error(&e));
        }
        resp
    })
}

fn future_delete_range<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: DeleteRangeRequest,
) -> impl Future<Item = DeleteRangeResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.delete_range(
        req.take_context(),
        Key::from_raw(req.get_start_key()),
        Key::from_raw(req.get_end_key()),
        req.get_notify_only(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = DeleteRangeResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

fn future_raw_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawGetRequest,
) -> impl Future<Item = RawGetResponse, Error = Error> {
    storage
        .raw_get(req.take_context(), req.take_cf(), req.take_key())
        .then(|v| {
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
        })
}

pub fn future_raw_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    requests: Vec<u64>,
    cf: String,
    commands: Vec<PointGetCommand>,
) -> impl Future<Item = (), Error = ()> {
    let begin_instant = Instant::now_coarse();
    storage.raw_batch_get_command(cf, commands).then(move |v| {
        match v {
            Ok(v) => {
                if requests.len() != v.len() {
                    error!("KvService batch response size mismatch");
                }
                for (req, v) in requests.into_iter().zip(v.into_iter()) {
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
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                    if tx.send_and_notify((req, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = RawGetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(format!("{}", e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .raw_batch_get_command
            .observe(duration_to_sec(begin_instant.elapsed()));
        Ok(())
    })
}

fn future_raw_batch_get<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchGetRequest,
) -> impl Future<Item = RawBatchGetResponse, Error = Error> {
    let keys = req.take_keys().into();
    storage
        .raw_batch_get(req.take_context(), req.take_cf(), keys)
        .then(|v| {
            let mut resp = RawBatchGetResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_pairs(extract_kv_pairs(v).into());
            }
            Ok(resp)
        })
}

fn future_raw_put<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawPutRequest,
) -> impl Future<Item = RawPutResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let res = storage.raw_put(
        req.take_context(),
        req.take_cf(),
        req.take_key(),
        req.take_value(),
        cb,
    );

    AndThenWith::new(res, future.map_err(Error::from)).map(|v| {
        let mut resp = RawPutResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

fn future_raw_batch_put<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchPutRequest,
) -> impl Future<Item = RawBatchPutResponse, Error = Error> {
    let cf = req.take_cf();
    let pairs = req
        .take_pairs()
        .into_iter()
        .map(|mut x| (x.take_key(), x.take_value()))
        .collect();

    let (cb, f) = paired_future_callback();
    let res = storage.raw_batch_put(req.take_context(), cf, pairs, cb);

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = RawBatchPutResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

fn future_raw_delete<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRequest,
) -> impl Future<Item = RawDeleteResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.raw_delete(req.take_context(), req.take_cf(), req.take_key(), cb);

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = RawDeleteResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

fn future_raw_batch_delete<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Item = RawBatchDeleteResponse, Error = Error> {
    let cf = req.take_cf();
    let keys = req.take_keys().into();
    let (cb, f) = paired_future_callback();
    let res = storage.raw_batch_delete(req.take_context(), cf, keys, cb);

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = RawBatchDeleteResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

fn future_raw_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawScanRequest,
) -> impl Future<Item = RawScanResponse, Error = Error> {
    let end_key = if req.get_end_key().is_empty() {
        None
    } else {
        Some(req.take_end_key())
    };
    storage
        .raw_scan(
            req.take_context(),
            req.take_cf(),
            req.take_start_key(),
            end_key,
            req.get_limit() as usize,
            req.get_key_only(),
            req.get_reverse(),
        )
        .then(|v| {
            let mut resp = RawScanResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_kvs(extract_kv_pairs(v).into());
            }
            Ok(resp)
        })
}

fn future_raw_batch_scan<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawBatchScanRequest,
) -> impl Future<Item = RawBatchScanResponse, Error = Error> {
    storage
        .raw_batch_scan(
            req.take_context(),
            req.take_cf(),
            req.take_ranges().into(),
            req.get_each_limit() as usize,
            req.get_key_only(),
            req.get_reverse(),
        )
        .then(|v| {
            let mut resp = RawBatchScanResponse::default();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_kvs(extract_kv_pairs(v).into());
            }
            Ok(resp)
        })
}

fn future_raw_delete_range<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRangeRequest,
) -> impl Future<Item = RawDeleteRangeResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.raw_delete_range(
        req.take_context(),
        req.take_cf(),
        req.take_start_key(),
        req.take_end_key(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = RawDeleteRangeResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(format!("{}", e));
        }
        resp
    })
}

// unimplemented
fn future_ver_get<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerGetRequest,
) -> impl Future<Item = VerGetResponse, Error = Error> {
    let resp = VerGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_get<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerBatchGetRequest,
) -> impl Future<Item = VerBatchGetResponse, Error = Error> {
    let resp = VerBatchGetResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_mut<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerMutRequest,
) -> impl Future<Item = VerMutResponse, Error = Error> {
    let resp = VerMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_batch_mut<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerBatchMutRequest,
) -> impl Future<Item = VerBatchMutResponse, Error = Error> {
    let resp = VerBatchMutResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_scan<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerScanRequest,
) -> impl Future<Item = VerScanResponse, Error = Error> {
    let resp = VerScanResponse::default();
    future::ok(resp)
}

// unimplemented
fn future_ver_delete_range<E: Engine, L: LockManager>(
    _storage: &Storage<E, L>,
    mut _req: VerDeleteRangeRequest,
) -> impl Future<Item = VerDeleteRangeResponse, Error = Error> {
    let resp = VerDeleteRangeResponse::default();
    future::ok(resp)
}

fn future_cop<E: Engine>(
    cop: &Endpoint<E>,
    peer: Option<String>,
    req: Request,
) -> impl Future<Item = Response, Error = Error> {
    cop.parse_and_handle_unary_request(req, peer)
        .map_err(|_| unreachable!())
}

macro_rules! txn_command_future {
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) $prelude: stmt; ($v: ident, $resp: ident) { $else_branch: expr }) => {
        fn $fn_name<E: Engine, L: LockManager>(
            storage: &Storage<E, L>,
            $req: $req_ty,
        ) -> impl Future<Item = $resp_ty, Error = Error> {
            $prelude
            let (cb, f) = paired_future_callback();
            let res = storage.sched_txn_command($req.into(), cb);

            AndThenWith::new(res, f.map_err(Error::from)).map(move |$v| {
                let mut $resp = $resp_ty::default();
                if let Some(err) = extract_region_error(&$v) {
                    $resp.set_region_error(err);
                } else {
                    $else_branch;
                }
                $resp
            })
        }
    };
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($v: ident, $resp: ident) { $else_branch: expr }) => {
        txn_command_future!($fn_name, $req_ty, $resp_ty, (req) {}; ($v, $resp) { $else_branch });
    };
}

txn_command_future!(future_prewrite, PrewriteRequest, PrewriteResponse, (v, resp) {
    resp.set_errors(extract_key_errors(v).into())
});
txn_command_future!(future_acquire_pessimistic_lock, PessimisticLockRequest, PessimisticLockResponse, (v, resp) {
    match v {
        Ok(Ok(res)) => resp.set_values(res.into_vec().into()),
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
            if let TxnStatus::Uncommitted { lock_ttl, .. } = txn_status {
                resp.set_lock_ttl(lock_ttl);
            } else {
                unreachable!();
            }
        }
        Err(e) => resp.set_error(extract_key_error(&e)),
    }
});
txn_command_future!(future_check_txn_status, CheckTxnStatusRequest, CheckTxnStatusResponse,
    (req) let caller_start_ts = req.get_caller_start_ts().into();
    (v, resp) {
        match v {
            Ok(txn_status) => match txn_status {
                TxnStatus::RolledBack => resp.set_action(Action::NoAction),
                TxnStatus::TtlExpire => resp.set_action(Action::TtlExpireRollback),
                TxnStatus::LockNotExist => resp.set_action(Action::LockNotExistRollback),
                TxnStatus::Committed { commit_ts } => {
                    resp.set_commit_version(commit_ts.into_inner())
                }
                TxnStatus::Uncommitted {
                    lock_ttl,
                    min_commit_ts,
                } => {
                    resp.set_lock_ttl(lock_ttl);
                    // If the caller_start_ts is max, it's a point get in the autocommit transaction.
                    // Even though the min_commit_ts is not pushed, the point get can ingore the lock
                    // next time because it's not committed. So we pretend it has been pushed.
                    if min_commit_ts > caller_start_ts || caller_start_ts.is_max() {
                        resp.set_action(Action::MinCommitTsPushed);
                    }
                }
            },
            Err(e) => resp.set_error(extract_key_error(&e)),
        }
});
txn_command_future!(future_scan_lock, ScanLockRequest, ScanLockResponse, (v, resp) {
    match v {
        Ok(locks) => resp.set_locks(locks.into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tokio_sync::oneshot;

    #[test]
    fn test_poll_future_notify_with_slow_source() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();

        thread::Builder::new()
            .name("source".to_owned())
            .spawn(move || {
                signal_rx.wait().unwrap();
                tx.send(100).unwrap();
            })
            .unwrap();

        let (tx1, rx1) = oneshot::channel::<usize>();
        poll_future_notify(
            rx.map(move |i| {
                assert_eq!(thread::current().name(), Some("source"));
                tx1.send(i + 100).unwrap();
            })
            .map_err(|_| ()),
        );
        signal_tx.send(()).unwrap();
        assert_eq!(rx1.wait().unwrap(), 200);
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
        signal_rx.wait().unwrap();
        poll_future_notify(
            rx.map(move |i| {
                assert_ne!(thread::current().name(), Some("source"));
                tx1.send(i + 100).unwrap();
            })
            .map_err(|_| ()),
        );
        assert_eq!(rx1.wait().unwrap(), 200);
    }
}
