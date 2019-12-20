// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::{self, FromIterator};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::coprocessor::Endpoint;
use crate::raftstore::router::RaftStoreRouter;
use crate::raftstore::store::{Callback, CasualMessage};
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
    lock_manager::{LockManager, WaitTimeout},
    txn::PointGetCommand,
    Storage, TxnStatus,
};
use futures::executor::{self, Notify, Spawn};
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
use prometheus::HistogramTimer;
use tikv_util::collections::HashMap;
use tikv_util::future::{paired_future_callback, AndThenWith};
use tikv_util::mpsc::batch::{unbounded, BatchReceiver, Sender};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Scheduler;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use txn_types::{self, Key, TimeStamp};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static, E: Engine, L: LockManager> {
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
}

impl<T: RaftStoreRouter + 'static, E: Engine, L: LockManager> Service<T, E, L> {
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

impl<T: RaftStoreRouter + 'static, E: Engine, L: LockManager> Tikv for Service<T, E, L> {
    fn kv_get(&mut self, ctx: RpcContext<'_>, req: GetRequest, sink: UnarySink<GetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_get.start_coarse_timer();
        let future = future_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_get",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan(&mut self, ctx: RpcContext<'_>, req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan.start_coarse_timer();
        let future = future_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_scan",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_scan.inc();
            });

        ctx.spawn(future);
    }

    fn kv_prewrite(
        &mut self,
        ctx: RpcContext<'_>,
        req: PrewriteRequest,
        sink: UnarySink<PrewriteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_prewrite.start_coarse_timer();
        let future = future_prewrite(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_prewrite",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc();
            });

        ctx.spawn(future);
    }

    fn kv_pessimistic_lock(
        &mut self,
        ctx: RpcContext<'_>,
        req: PessimisticLockRequest,
        sink: UnarySink<PessimisticLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_pessimistic_lock
            .start_coarse_timer();
        let future = future_acquire_pessimistic_lock(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_pessimistic_lock",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_pessimistic_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_pessimistic_rollback(
        &mut self,
        ctx: RpcContext<'_>,
        req: PessimisticRollbackRequest,
        sink: UnarySink<PessimisticRollbackResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_pessimistic_rollback
            .start_coarse_timer();
        let future = future_pessimistic_rollback(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_pessimistic_rollback",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_pessimistic_rollback.inc();
            });

        ctx.spawn(future);
    }

    fn kv_commit(
        &mut self,
        ctx: RpcContext<'_>,
        req: CommitRequest,
        sink: UnarySink<CommitResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_commit.start_coarse_timer();

        let future = future_commit(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_commit",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_commit.inc();
            });

        ctx.spawn(future);
    }

    fn kv_import(&mut self, _: RpcContext<'_>, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_cleanup(
        &mut self,
        ctx: RpcContext<'_>,
        req: CleanupRequest,
        sink: UnarySink<CleanupResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_cleanup.start_coarse_timer();
        let future = future_cleanup(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_cleanup",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_get(
        &mut self,
        ctx: RpcContext<'_>,
        req: BatchGetRequest,
        sink: UnarySink<BatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_batch_get.start_coarse_timer();
        let future = future_batch_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_batch_get",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_rollback(
        &mut self,
        ctx: RpcContext<'_>,
        req: BatchRollbackRequest,
        sink: UnarySink<BatchRollbackResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_batch_rollback
            .start_coarse_timer();
        let future = future_batch_rollback(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_batch_rollback",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc();
            });

        ctx.spawn(future);
    }

    fn kv_txn_heart_beat(
        &mut self,
        ctx: RpcContext<'_>,
        req: TxnHeartBeatRequest,
        sink: UnarySink<TxnHeartBeatResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_txn_heart_beat
            .start_coarse_timer();
        let future = future_txn_heart_beat(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_txn_heart_beat",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_txn_heart_beat.inc();
            });

        ctx.spawn(future);
    }

    fn kv_check_txn_status(
        &mut self,
        ctx: RpcContext<'_>,
        req: CheckTxnStatusRequest,
        sink: UnarySink<CheckTxnStatusResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_check_txn_status
            .start_coarse_timer();
        let future = future_check_txn_status(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_check_txn_status",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_check_txn_status.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan_lock(
        &mut self,
        ctx: RpcContext<'_>,
        req: ScanLockRequest,
        sink: UnarySink<ScanLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan_lock.start_coarse_timer();
        let future = future_scan_lock(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_scan_lock",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_resolve_lock(
        &mut self,
        ctx: RpcContext<'_>,
        req: ResolveLockRequest,
        sink: UnarySink<ResolveLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_resolve_lock.start_coarse_timer();
        let future = future_resolve_lock(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_resolve_lock",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_gc(&mut self, ctx: RpcContext<'_>, req: GcRequest, sink: UnarySink<GcResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_gc.start_coarse_timer();
        let future = future_gc(&self.gc_worker, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_gc",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_gc.inc();
            });

        ctx.spawn(future);
    }

    fn kv_delete_range(
        &mut self,
        ctx: RpcContext<'_>,
        req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_delete_range.start_coarse_timer();
        let future = future_delete_range(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "kv_delete_range",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn raw_get(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawGetRequest,
        sink: UnarySink<RawGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_get.start_coarse_timer();
        let future = future_raw_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_get",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_get(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawBatchGetRequest,
        sink: UnarySink<RawBatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_get.start_coarse_timer();

        let future = future_raw_batch_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_batch_get",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_scan(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawScanRequest,
        sink: UnarySink<RawScanResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_scan.start_coarse_timer();

        let future = future_raw_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_scan",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_scan(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawBatchScanRequest,
        sink: UnarySink<RawBatchScanResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_scan.start_coarse_timer();

        let future = future_raw_batch_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_batch_scan",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_batch_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_put(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawPutRequest,
        sink: UnarySink<RawPutResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_put.start_coarse_timer();
        let future = future_raw_put(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_put",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_put(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawBatchPutRequest,
        sink: UnarySink<RawBatchPutResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_put.start_coarse_timer();

        let future = future_raw_batch_put(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_batch_put",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_batch_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawDeleteRequest,
        sink: UnarySink<RawDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete.start_coarse_timer();
        let future = future_raw_delete(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_delete",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_delete(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawBatchDeleteRequest,
        sink: UnarySink<RawBatchDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_delete.start_coarse_timer();

        let future = future_raw_batch_delete(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_batch_delete",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_batch_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete_range(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawDeleteRangeRequest,
        sink: UnarySink<RawDeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete_range.start_coarse_timer();

        let future = future_raw_delete_range(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "raw_delete_range",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.raw_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn unsafe_destroy_range(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: UnsafeDestroyRangeRequest,
        sink: UnarySink<UnsafeDestroyRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .unsafe_destroy_range
            .start_coarse_timer();

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
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "unsafe_destroy_range",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.unsafe_destroy_range.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.coprocessor.start_coarse_timer();
        let future = future_cop(&self.cop, req, Some(ctx.peer()))
            .and_then(|resp| sink.success(resp).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "coprocessor",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor_stream(
        &mut self,
        ctx: RpcContext<'_>,
        req: Request,
        sink: ServerStreamingSink<Response>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .coprocessor_stream
            .start_coarse_timer();

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
            .map(|_| timer.observe_duration())
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

    fn mvcc_get_by_key(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: MvccGetByKeyRequest,
        sink: UnarySink<MvccGetByKeyResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.mvcc_get_by_key.start_coarse_timer();

        let key = Key::from_raw(req.get_key());
        let (cb, f) = paired_future_callback();
        let res = self
            .storage
            .mvcc_by_key(req.take_context(), key.clone(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = MvccGetByKeyResponse::default();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(mvcc) => {
                            resp.set_info(mvcc.into_proto());
                        }
                        Err(e) => resp.set_error(format!("{}", e)),
                    };
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "mvcc_get_by_key",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.mvcc_get_by_key.inc();
            });

        ctx.spawn(future);
    }

    fn mvcc_get_by_start_ts(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: MvccGetByStartTsRequest,
        sink: UnarySink<MvccGetByStartTsResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .mvcc_get_by_start_ts
            .start_coarse_timer();

        let (cb, f) = paired_future_callback();
        let res = self
            .storage
            .mvcc_by_start_ts(req.take_context(), req.get_start_ts().into(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = MvccGetByStartTsResponse::default();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
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
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("kv rpc failed";
                    "request" => "mvcc_get_by_start_ts",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.mvcc_get_by_start_ts.inc();
            });
        ctx.spawn(future);
    }

    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.split_region.start_coarse_timer();

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
            .map(|_| timer.observe_duration())
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
        let timer = GRPC_MSG_HISTOGRAM_VEC.read_index.start_coarse_timer();

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
            .map(|_| timer.observe_duration())
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
                let start = Instant::now();
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
            |batch_resp, (id, resp)| {
                batch_resp.mut_request_ids().push(id);
                batch_resp.mut_responses().push(resp);
            },
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
}

fn response_batch_commands_request<F>(
    id: u64,
    resp: F,
    tx: Sender<(u64, batch_commands_response::Response)>,
    timer: HistogramTimer,
) where
    F: Future<Item = batch_commands_response::Response, Error = ()> + Send + 'static,
{
    let f = resp.and_then(move |resp| {
        if tx.send_and_notify((id, resp)).is_err() {
            error!("KvService response batch commands fail");
            return Err(());
        }
        timer.observe_duration();
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

    match req.cmd {
        None => {
            // For some invalid requests.
            let timer = GRPC_MSG_HISTOGRAM_VEC.invalid.start_coarse_timer();
            let resp = future::ok(batch_commands_response::Response::default());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Get(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_get.start_coarse_timer();
            let resp = future_get(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::Get))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Scan(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan.start_coarse_timer();
            let resp = future_scan(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::Scan))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Prewrite(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_prewrite.start_coarse_timer();
            let resp = future_prewrite(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::Prewrite))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Commit(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_commit.start_coarse_timer();
            let resp = future_commit(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::Commit))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_commit.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Import(_)) => unimplemented!(),
        Some(batch_commands_request::request::Cmd::Cleanup(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_cleanup.start_coarse_timer();
            let resp = future_cleanup(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::Cleanup))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::BatchGet(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_batch_get.start_coarse_timer();
            let resp = future_batch_get(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::BatchGet))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::BatchRollback(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC
                .kv_batch_rollback
                .start_coarse_timer();
            let resp = future_batch_rollback(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::BatchRollback
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::TxnHeartBeat(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC
                .kv_txn_heart_beat
                .start_coarse_timer();
            let resp = future_txn_heart_beat(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::TxnHeartBeat))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_txn_heart_beat.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::CheckTxnStatus(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC
                .kv_check_txn_status
                .start_coarse_timer();
            let resp = future_check_txn_status(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::CheckTxnStatus
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_check_txn_status.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::ScanLock(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan_lock.start_coarse_timer();
            let resp = future_scan_lock(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::ScanLock))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::ResolveLock(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_resolve_lock.start_coarse_timer();
            let resp = future_resolve_lock(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::ResolveLock))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Gc(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_gc.start_coarse_timer();
            let resp = future_gc(&gc_worker, req)
                .map(oneof!(batch_commands_response::response::Cmd::Gc))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_gc.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::DeleteRange(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.kv_delete_range.start_coarse_timer();
            let resp = future_delete_range(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::DeleteRange))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawGet(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_get.start_coarse_timer();
            let resp = future_raw_get(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawGet))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_get.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawBatchGet(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_get.start_coarse_timer();
            let resp = future_raw_batch_get(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawBatchGet))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_get.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawPut(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_put.start_coarse_timer();
            let resp = future_raw_put(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawPut))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_put.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawBatchPut(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_put.start_coarse_timer();
            let resp = future_raw_batch_put(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawBatchPut))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_put.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawDelete(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete.start_coarse_timer();
            let resp = future_raw_delete(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawDelete))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_delete.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawBatchDelete(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_delete.start_coarse_timer();
            let resp = future_raw_batch_delete(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::RawBatchDelete
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_delete.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawScan(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_scan.start_coarse_timer();
            let resp = future_raw_scan(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawScan))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_scan.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawDeleteRange(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete_range.start_coarse_timer();
            let resp = future_raw_delete_range(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::RawDeleteRange
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_delete_range.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::RawBatchScan(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_scan.start_coarse_timer();
            let resp = future_raw_batch_scan(&storage, req)
                .map(oneof!(batch_commands_response::response::Cmd::RawBatchScan))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_scan.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Coprocessor(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.coprocessor.start_coarse_timer();
            let resp = future_cop(&cop, req, Some(peer.to_string()))
                .map(oneof!(batch_commands_response::response::Cmd::Coprocessor))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.coprocessor.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::PessimisticLock(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC
                .kv_pessimistic_lock
                .start_coarse_timer();
            let resp = future_acquire_pessimistic_lock(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::PessimisticLock
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_pessimistic_lock.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::PessimisticRollback(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC
                .kv_pessimistic_rollback
                .start_coarse_timer();
            let resp = future_pessimistic_rollback(&storage, req)
                .map(oneof!(
                    batch_commands_response::response::Cmd::PessimisticRollback
                ))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_pessimistic_rollback.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
        Some(batch_commands_request::request::Cmd::Empty(req)) => {
            let timer = GRPC_MSG_HISTOGRAM_VEC.invalid.start_coarse_timer();
            let resp = future_handle_empty(req)
                .map(oneof!(batch_commands_response::response::Cmd::Empty))
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.invalid.inc());
            response_batch_commands_request(id, resp, tx, timer);
        }
    }
}

fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> impl Future<Item = BatchCommandsEmptyResponse, Error = Error> {
    tikv_util::timer::GLOBAL_TIMER_HANDLE
        .delay(std::time::Instant::now() + std::time::Duration::from_millis(req.get_delay_time()))
        .map(move |_| {
            let mut res = BatchCommandsEmptyResponse::default();
            res.set_test_id(req.get_test_id());
            res
        })
        .map_err(|_| unreachable!())
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
    let timer = GRPC_MSG_HISTOGRAM_VEC
        .kv_batch_get_command
        .start_coarse_timer();
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
        timer.observe_duration();
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

fn future_prewrite<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: PrewriteRequest,
) -> impl Future<Item = PrewriteResponse, Error = Error> {
    let for_update_ts = req.get_for_update_ts();
    let (cb, f) = paired_future_callback();
    let res = if for_update_ts == 0 {
        storage.prewrite(
            req.take_context(),
            req.take_mutations().into_iter().map(Into::into).collect(),
            req.take_primary_lock(),
            req.get_start_version().into(),
            req.get_lock_ttl(),
            req.get_skip_constraint_check(),
            req.get_txn_size(),
            req.get_min_commit_ts().into(),
            cb,
        )
    } else {
        let is_pessimistic_lock = req.take_is_pessimistic_lock();
        let mutations = req
            .take_mutations()
            .into_iter()
            .map(Into::into)
            .zip(is_pessimistic_lock.into_iter())
            .collect();
        storage.prewrite_pessimistic(
            req.take_context(),
            mutations,
            req.take_primary_lock(),
            req.get_start_version().into(),
            req.get_lock_ttl(),
            for_update_ts.into(),
            req.get_txn_size(),
            req.get_min_commit_ts().into(),
            cb,
        )
    };

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = PrewriteResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_errors(extract_key_errors(v).into());
        }
        resp
    })
}

fn future_acquire_pessimistic_lock<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: PessimisticLockRequest,
) -> impl Future<Item = PessimisticLockResponse, Error = Error> {
    let keys = req
        .take_mutations()
        .into_iter()
        .map(|x| match x.get_op() {
            Op::PessimisticLock => (
                Key::from_raw(x.get_key()),
                x.get_assertion() == Assertion::NotExist,
            ),
            _ => panic!("mismatch Op in pessimistic lock mutations"),
        })
        .collect();

    let (cb, f) = paired_future_callback();
    let res = storage.acquire_pessimistic_lock(
        req.take_context(),
        keys,
        req.take_primary_lock(),
        req.get_start_version().into(),
        req.get_lock_ttl(),
        req.get_is_first_lock(),
        req.get_for_update_ts().into(),
        WaitTimeout::from_encoded(req.get_wait_timeout()),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = PessimisticLockResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_errors(extract_key_errors(v).into());
        }
        resp
    })
}

fn future_pessimistic_rollback<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: PessimisticRollbackRequest,
) -> impl Future<Item = PessimisticRollbackResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let (cb, f) = paired_future_callback();
    let res = storage.pessimistic_rollback(
        req.take_context(),
        keys,
        req.get_start_version().into(),
        req.get_for_update_ts().into(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = PessimisticRollbackResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_errors(extract_key_errors(v).into());
        }
        resp
    })
}

fn future_commit<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: CommitRequest,
) -> impl Future<Item = CommitResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let (cb, f) = paired_future_callback();
    let res = storage.commit(
        req.take_context(),
        keys,
        req.get_start_version().into(),
        req.get_commit_version().into(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = CommitResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(TxnStatus::Committed { commit_ts }) => {
                    resp.set_commit_version(commit_ts.into_inner())
                }
                Ok(_) => unreachable!(),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        resp
    })
}

fn future_cleanup<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: CleanupRequest,
) -> impl Future<Item = CleanupResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.cleanup(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_start_version().into(),
        req.get_current_ts().into(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = CleanupResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            if let Some(ts) = extract_committed(&e) {
                resp.set_commit_version(ts.into_inner());
            } else {
                resp.set_error(extract_key_error(&e));
            }
        }
        resp
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

fn future_batch_rollback<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: BatchRollbackRequest,
) -> impl Future<Item = BatchRollbackResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

    let (cb, f) = paired_future_callback();
    let res = storage.rollback(req.take_context(), keys, req.get_start_version().into(), cb);

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = BatchRollbackResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(extract_key_error(&e));
        }
        resp
    })
}

fn future_txn_heart_beat<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: TxnHeartBeatRequest,
) -> impl Future<Item = TxnHeartBeatResponse, Error = Error> {
    let primary_key = Key::from_raw(req.get_primary_lock());

    let (cb, f) = paired_future_callback();
    let res = storage.txn_heart_beat(
        req.take_context(),
        primary_key,
        req.get_start_version().into(),
        req.get_advise_lock_ttl(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = TxnHeartBeatResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
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
        }
        resp
    })
}

fn future_check_txn_status<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: CheckTxnStatusRequest,
) -> impl Future<Item = CheckTxnStatusResponse, Error = Error> {
    let primary_key = Key::from_raw(req.get_primary_key());

    let (cb, f) = paired_future_callback();
    let res = storage.check_txn_status(
        req.take_context(),
        primary_key,
        req.get_lock_ts().into(),
        req.get_caller_start_ts().into(),
        req.get_current_ts().into(),
        req.get_rollback_if_not_exist(),
        cb,
    );

    let caller_start_ts = req.get_caller_start_ts().into();
    AndThenWith::new(res, f.map_err(Error::from)).map(move |v| {
        let mut resp = CheckTxnStatusResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
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
                        if min_commit_ts > caller_start_ts {
                            resp.set_action(Action::MinCommitTsPushed);
                        }
                    }
                },
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        resp
    })
}

fn future_scan_lock<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: ScanLockRequest,
) -> impl Future<Item = ScanLockResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.scan_locks(
        req.take_context(),
        req.get_max_version().into(),
        req.take_start_key(),
        req.get_limit() as usize,
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = ScanLockResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(locks) => resp.set_locks(locks.into()),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        resp
    })
}

fn future_resolve_lock<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    mut req: ResolveLockRequest,
) -> impl Future<Item = ResolveLockResponse, Error = Error> {
    let resolve_keys: Vec<Key> = req
        .get_keys()
        .iter()
        .map(|key| Key::from_raw(key))
        .collect();
    let txn_status = if req.get_start_version() > 0 {
        HashMap::from_iter(iter::once((
            req.get_start_version().into(),
            req.get_commit_version().into(),
        )))
    } else {
        HashMap::from_iter(
            req.take_txn_infos()
                .into_iter()
                .map(|info| (info.txn.into(), info.status.into())),
        )
    };

    let (cb, f) = paired_future_callback();
    let res = if !resolve_keys.is_empty() {
        let start_ts: TimeStamp = req.get_start_version().into();
        assert!(!start_ts.is_zero());
        let commit_ts = req.get_commit_version().into();
        storage.resolve_lock_lite(req.take_context(), start_ts, commit_ts, resolve_keys, cb)
    } else {
        storage.resolve_lock(req.take_context(), txn_status, cb)
    };

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = ResolveLockResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(extract_key_error(&e));
        }
        resp
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
    let timer = GRPC_MSG_HISTOGRAM_VEC
        .raw_batch_get_command
        .start_coarse_timer();
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
        timer.observe_duration();
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

fn future_cop<E: Engine>(
    cop: &Endpoint<E>,
    req: Request,
    peer: Option<String>,
) -> impl Future<Item = Response, Error = Error> {
    cop.parse_and_handle_unary_request(req, peer)
        .map_err(|_| unreachable!())
}

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
