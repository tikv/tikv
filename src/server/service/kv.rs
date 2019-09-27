// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::{self, FromIterator};
use std::sync::{Arc, Mutex};

use crate::coprocessor::Endpoint;
use crate::raftstore::store::{Callback, CasualMessage};
use crate::server::load_statistics::ThreadLoad;
use crate::server::metrics::*;
use crate::server::snap::Task as SnapTask;
use crate::server::transport::RaftStoreRouter;
use crate::server::Error;
use crate::storage::kv::Error as EngineError;
use crate::storage::lock_manager::LockMgr;
use crate::storage::mvcc::{Error as MvccError, LockType, Write as MvccWrite, WriteType};
use crate::storage::txn::Error as TxnError;
use crate::storage::{self, Engine, Key, Mutation, Options, Storage, Value};
use futures::executor::{self, Notify, Spawn};
use futures::{future, Async, Future, Sink, Stream};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus,
    RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};
use kvproto::kvrpcpb::{self, *};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use kvproto::raft_serverpb::*;
use kvproto::tikvpb::*;
use prometheus::HistogramTimer;
use tikv_util::collections::HashMap;
use tikv_util::future::{paired_future_callback, AndThenWith};
use tikv_util::mpsc::batch::{unbounded, BatchReceiver, Sender};
use tikv_util::worker::Scheduler;

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";
const GC_WORKER_IS_BUSY: &str = "gc worker is busy";

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static, E: Engine, L: LockMgr> {
    // For handling KV requests.
    storage: Storage<E, L>,
    // For handling coprocessor requests.
    cop: Endpoint<E>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,

    thread_load: Arc<ThreadLoad>,
}

impl<T: RaftStoreRouter + 'static, E: Engine, L: LockMgr> Service<T, E, L> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(
        storage: Storage<E, L>,
        cop: Endpoint<E>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        thread_load: Arc<ThreadLoad>,
    ) -> Self {
        Service {
            storage,
            cop,
            ch,
            snap_scheduler,
            thread_load,
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

impl<T: RaftStoreRouter + 'static, E: Engine, L: LockMgr> Tikv for Service<T, E, L> {
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
        _ctx: RpcContext<'_>,
        _req: CheckTxnStatusRequest,
        _sink: UnarySink<CheckTxnStatusResponse>,
    ) {
        unimplemented!();
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
        let future = future_gc(&self.storage, req)
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
        let res = self.storage.async_unsafe_destroy_range(
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
            .async_mvcc_by_key(req.take_context(), key.clone(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = MvccGetByKeyResponse::default();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(mvcc) => {
                            resp.set_info(extract_mvcc_info(mvcc));
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
            .async_mvcc_by_start_ts(req.take_context(), req.get_start_ts(), cb);

        let future = AndThenWith::new(res, f.map_err(Error::from))
            .and_then(|v| {
                let mut resp = MvccGetByStartTsResponse::default();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(Some((k, vv))) => {
                            resp.set_key(k.into_raw().unwrap());
                            resp.set_info(extract_mvcc_info(vv));
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

        let request_handler = stream.for_each(move |mut req| {
            let request_ids = req.take_request_ids();
            let requests: Vec<_> = req.take_requests().into();
            GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
            for (id, req) in request_ids.into_iter().zip(requests) {
                handle_batch_commands_request(&storage, &cop, &peer, id, req, tx.clone());
            }
            future::ok::<_, _>(())
        });

        ctx.spawn(request_handler.map_err(|e| error!("batch commands error"; "err" => %e)));

        let thread_load = Arc::clone(&self.thread_load);
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

fn poll_future_notify<F: Future<Item = (), Error = ()> + Send + 'static>(f: F) {
    let spawn = Arc::new(Mutex::new(Some(executor::spawn(f))));
    let notify = BatchCommandsNotify(spawn);
    notify.notify(0);
}

fn handle_batch_commands_request<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
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
        Some(batch_commands_request::request::Cmd::CheckTxnStatus(_)) => unimplemented!(),
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
            let resp = future_gc(&storage, req)
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

fn future_get<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: GetRequest,
) -> impl Future<Item = GetResponse, Error = Error> {
    storage
        .async_get(
            req.take_context(),
            Key::from_raw(req.get_key()),
            req.get_version(),
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

fn future_scan<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: ScanRequest,
) -> impl Future<Item = ScanResponse, Error = Error> {
    let end_key = if req.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_raw(req.get_end_key()))
    };

    let mut options = Options::default();
    options.key_only = req.get_key_only();
    options.reverse_scan = req.get_reverse();

    storage
        .async_scan(
            req.take_context(),
            Key::from_raw(req.get_start_key()),
            end_key,
            req.get_limit() as usize,
            req.get_version(),
            options,
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

fn future_prewrite<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: PrewriteRequest,
) -> impl Future<Item = PrewriteResponse, Error = Error> {
    let mutations = req
        .take_mutations()
        .into_iter()
        .map(|mut x| match x.get_op() {
            Op::Put => Mutation::Put((Key::from_raw(x.get_key()), x.take_value())),
            Op::Del => Mutation::Delete(Key::from_raw(x.get_key())),
            Op::Lock => Mutation::Lock(Key::from_raw(x.get_key())),
            Op::Insert => Mutation::Insert((Key::from_raw(x.get_key()), x.take_value())),
            _ => panic!("mismatch Op in prewrite mutations"),
        })
        .collect();
    let mut options = Options::default();
    options.lock_ttl = req.get_lock_ttl();
    options.skip_constraint_check = req.get_skip_constraint_check();
    options.for_update_ts = req.get_for_update_ts();
    options.is_pessimistic_lock = req.take_is_pessimistic_lock();
    options.txn_size = req.get_txn_size();

    let (cb, f) = paired_future_callback();
    let res = storage.async_prewrite(
        req.take_context(),
        mutations,
        req.take_primary_lock(),
        req.get_start_version(),
        options,
        cb,
    );

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

fn future_acquire_pessimistic_lock<E: Engine, L: LockMgr>(
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
    let mut options = Options::default();
    options.lock_ttl = req.get_lock_ttl();
    options.is_first_lock = req.get_is_first_lock();
    options.for_update_ts = req.get_for_update_ts();

    let (cb, f) = paired_future_callback();
    let res = storage.async_acquire_pessimistic_lock(
        req.take_context(),
        keys,
        req.take_primary_lock(),
        req.get_start_version(),
        options,
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

fn future_pessimistic_rollback<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: PessimisticRollbackRequest,
) -> impl Future<Item = PessimisticRollbackResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let (cb, f) = paired_future_callback();
    let res = storage.async_pessimistic_rollback(
        req.take_context(),
        keys,
        req.get_start_version(),
        req.get_for_update_ts(),
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

fn future_commit<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: CommitRequest,
) -> impl Future<Item = CommitResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let (cb, f) = paired_future_callback();
    let res = storage.async_commit(
        req.take_context(),
        keys,
        req.get_start_version(),
        req.get_commit_version(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = CommitResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            resp.set_error(extract_key_error(&e));
        }
        resp
    })
}

fn future_cleanup<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: CleanupRequest,
) -> impl Future<Item = CleanupResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_cleanup(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_start_version(),
        req.get_current_ts(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = CleanupResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else if let Err(e) = v {
            if let Some(ts) = extract_committed(&e) {
                resp.set_commit_version(ts);
            } else {
                resp.set_error(extract_key_error(&e));
            }
        }
        resp
    })
}

fn future_batch_get<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: BatchGetRequest,
) -> impl Future<Item = BatchGetResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    storage
        .async_batch_get(req.take_context(), keys, req.get_version())
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

fn future_batch_rollback<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: BatchRollbackRequest,
) -> impl Future<Item = BatchRollbackResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

    let (cb, f) = paired_future_callback();
    let res = storage.async_rollback(req.take_context(), keys, req.get_start_version(), cb);

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

fn future_txn_heart_beat<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: TxnHeartBeatRequest,
) -> impl Future<Item = TxnHeartBeatResponse, Error = Error> {
    let primary_key = Key::from_raw(req.get_primary_lock());

    let (cb, f) = paired_future_callback();
    let res = storage.async_txn_heart_beat(
        req.take_context(),
        primary_key,
        req.get_start_version(),
        req.get_advise_lock_ttl(),
        cb,
    );

    AndThenWith::new(res, f.map_err(Error::from)).map(|v| {
        let mut resp = TxnHeartBeatResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((ttl, _)) => resp.set_lock_ttl(ttl),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        resp
    })
}

fn future_scan_lock<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: ScanLockRequest,
) -> impl Future<Item = ScanLockResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_scan_locks(
        req.take_context(),
        req.get_max_version(),
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

fn future_resolve_lock<E: Engine, L: LockMgr>(
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
            req.get_start_version(),
            req.get_commit_version(),
        )))
    } else {
        HashMap::from_iter(
            req.take_txn_infos()
                .into_iter()
                .map(|info| (info.txn, info.status)),
        )
    };

    let (cb, f) = paired_future_callback();
    let res = if !resolve_keys.is_empty() {
        let start_ts = req.get_start_version();
        assert!(start_ts > 0);
        let commit_ts = req.get_commit_version();
        storage.async_resolve_lock_lite(req.take_context(), start_ts, commit_ts, resolve_keys, cb)
    } else {
        storage.async_resolve_lock(req.take_context(), txn_status, cb)
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

fn future_gc<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: GcRequest,
) -> impl Future<Item = GcResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_gc(req.take_context(), req.get_safe_point(), cb);

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

fn future_delete_range<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: DeleteRangeRequest,
) -> impl Future<Item = DeleteRangeResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_delete_range(
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

fn future_raw_get<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawGetRequest,
) -> impl Future<Item = RawGetResponse, Error = Error> {
    storage
        .async_raw_get(req.take_context(), req.take_cf(), req.take_key())
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

fn future_raw_batch_get<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawBatchGetRequest,
) -> impl Future<Item = RawBatchGetResponse, Error = Error> {
    let keys = req.take_keys().into();
    storage
        .async_raw_batch_get(req.take_context(), req.take_cf(), keys)
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

fn future_raw_put<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawPutRequest,
) -> impl Future<Item = RawPutResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let res = storage.async_raw_put(
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

fn future_raw_batch_put<E: Engine, L: LockMgr>(
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
    let res = storage.async_raw_batch_put(req.take_context(), cf, pairs, cb);

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

fn future_raw_delete<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRequest,
) -> impl Future<Item = RawDeleteResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_raw_delete(req.take_context(), req.take_cf(), req.take_key(), cb);

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

fn future_raw_batch_delete<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Item = RawBatchDeleteResponse, Error = Error> {
    let cf = req.take_cf();
    let keys = req.take_keys().into();
    let (cb, f) = paired_future_callback();
    let res = storage.async_raw_batch_delete(req.take_context(), cf, keys, cb);

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

fn future_raw_scan<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawScanRequest,
) -> impl Future<Item = RawScanResponse, Error = Error> {
    let end_key = if req.get_end_key().is_empty() {
        None
    } else {
        Some(req.take_end_key())
    };
    storage
        .async_raw_scan(
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

fn future_raw_batch_scan<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawBatchScanRequest,
) -> impl Future<Item = RawBatchScanResponse, Error = Error> {
    storage
        .async_raw_batch_scan(
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

fn future_raw_delete_range<E: Engine, L: LockMgr>(
    storage: &Storage<E, L>,
    mut req: RawDeleteRangeRequest,
) -> impl Future<Item = RawDeleteRangeResponse, Error = Error> {
    let (cb, f) = paired_future_callback();
    let res = storage.async_raw_delete_range(
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

fn extract_region_error<T>(res: &storage::Result<T>) -> Option<RegionError> {
    use crate::storage::Error;
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error::Engine(EngineError::Request(ref e)))
        | Err(Error::Txn(TxnError::Engine(EngineError::Request(ref e))))
        | Err(Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(ref e))))) => {
            Some(e.to_owned())
        }
        Err(Error::SchedTooBusy) => {
            let mut err = RegionError::default();
            let mut server_is_busy_err = ServerIsBusy::default();
            server_is_busy_err.set_reason(SCHEDULER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error::GCWorkerTooBusy) => {
            let mut err = RegionError::default();
            let mut server_is_busy_err = ServerIsBusy::default();
            server_is_busy_err.set_reason(GC_WORKER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error::Closed) => {
            // TiKV is closing, return an RegionError to tell the client that this region is unavailable
            // temporarily, the client should retry the request in other TiKVs.
            let mut err = RegionError::default();
            err.set_message("TiKV is Closing".to_string());
            Some(err)
        }
        _ => None,
    }
}

fn extract_committed(err: &storage::Error) -> Option<u64> {
    match *err {
        storage::Error::Txn(TxnError::Mvcc(MvccError::Committed { commit_ts })) => Some(commit_ts),
        _ => None,
    }
}

fn extract_key_error(err: &storage::Error) -> KeyError {
    let mut key_error = KeyError::default();
    match err {
        storage::Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(info))) => {
            key_error.set_locked(info.clone());
        }
        // failed in prewrite or pessimistic lock
        storage::Error::Txn(TxnError::Mvcc(MvccError::WriteConflict {
            start_ts,
            conflict_start_ts,
            conflict_commit_ts,
            key,
            primary,
            ..
        })) => {
            let mut write_conflict = WriteConflict::default();
            write_conflict.set_start_ts(*start_ts);
            write_conflict.set_conflict_ts(*conflict_start_ts);
            write_conflict.set_conflict_commit_ts(*conflict_commit_ts);
            write_conflict.set_key(key.to_owned());
            write_conflict.set_primary(primary.to_owned());
            key_error.set_conflict(write_conflict);
            // for compatibility with older versions.
            key_error.set_retryable(format!("{:?}", err));
        }
        storage::Error::Txn(TxnError::Mvcc(MvccError::AlreadyExist { key })) => {
            let mut exist = AlreadyExist::default();
            exist.set_key(key.clone());
            key_error.set_already_exist(exist);
        }
        // failed in commit
        storage::Error::Txn(TxnError::Mvcc(MvccError::TxnLockNotFound { .. })) => {
            warn!("txn conflicts"; "err" => ?err);
            key_error.set_retryable(format!("{:?}", err));
        }
        storage::Error::Txn(TxnError::Mvcc(MvccError::Deadlock {
            lock_ts,
            lock_key,
            deadlock_key_hash,
            ..
        })) => {
            warn!("txn deadlocks"; "err" => ?err);
            let mut deadlock = Deadlock::default();
            deadlock.set_lock_ts(*lock_ts);
            deadlock.set_lock_key(lock_key.to_owned());
            deadlock.set_deadlock_key_hash(*deadlock_key_hash);
            key_error.set_deadlock(deadlock);
        }
        _ => {
            error!("txn aborts"; "err" => ?err);
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

fn extract_kv_pairs(res: storage::Result<Vec<storage::Result<storage::KvPair>>>) -> Vec<KvPair> {
    match res {
        Ok(res) => res
            .into_iter()
            .map(|r| match r {
                Ok((key, value)) => {
                    let mut pair = KvPair::default();
                    pair.set_key(key);
                    pair.set_value(value);
                    pair
                }
                Err(e) => {
                    let mut pair = KvPair::default();
                    pair.set_error(extract_key_error(&e));
                    pair
                }
            })
            .collect(),
        Err(e) => {
            let mut pair = KvPair::default();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

fn extract_mvcc_info(mvcc: storage::MvccInfo) -> MvccInfo {
    let mut mvcc_info = MvccInfo::default();
    if let Some(lock) = mvcc.lock {
        let mut lock_info = MvccLock::default();
        let op = match lock.lock_type {
            LockType::Put => Op::Put,
            LockType::Delete => Op::Del,
            LockType::Lock => Op::Lock,
            LockType::Pessimistic => Op::PessimisticLock,
        };
        lock_info.set_type(op);
        lock_info.set_start_ts(lock.ts);
        lock_info.set_primary(lock.primary);
        lock_info.set_short_value(lock.short_value.unwrap_or_default());
        mvcc_info.set_lock(lock_info);
    }
    let vv = extract_2pc_values(mvcc.values);
    let vw = extract_2pc_writes(mvcc.writes);
    mvcc_info.set_writes(vw.into());
    mvcc_info.set_values(vv.into());
    mvcc_info
}

fn extract_2pc_values(res: Vec<(u64, Value)>) -> Vec<MvccValue> {
    res.into_iter()
        .map(|(start_ts, value)| {
            let mut value_info = MvccValue::default();
            value_info.set_start_ts(start_ts);
            value_info.set_value(value);
            value_info
        })
        .collect()
}

fn extract_2pc_writes(res: Vec<(u64, MvccWrite)>) -> Vec<kvrpcpb::MvccWrite> {
    res.into_iter()
        .map(|(commit_ts, write)| {
            let mut write_info = kvrpcpb::MvccWrite::default();
            let op = match write.write_type {
                WriteType::Put => Op::Put,
                WriteType::Delete => Op::Del,
                WriteType::Lock => Op::Lock,
                WriteType::Rollback => Op::Rollback,
            };
            write_info.set_type(op);
            write_info.set_start_ts(write.start_ts);
            write_info.set_commit_ts(commit_ts);
            write_info.set_short_value(write.short_value.unwrap_or_default());
            write_info
        })
        .collect()
}

fn extract_key_errors(res: storage::Result<Vec<storage::Result<()>>>) -> Vec<KeyError> {
    match res {
        Ok(res) => res
            .into_iter()
            .filter_map(|x| match x {
                Err(e) => Some(extract_key_error(&e)),
                Ok(_) => None,
            })
            .collect(),
        Err(e) => vec![extract_key_error(&e)],
    }
}

mod batch_commands_response {
    pub type Response = kvproto::tikvpb::BatchCommandsResponseResponse;

    pub mod response {
        pub type Cmd = kvproto::tikvpb::BatchCommandsResponse_Response_oneof_cmd;
    }
}

mod batch_commands_request {
    pub type Request = kvproto::tikvpb::BatchCommandsRequestRequest;

    pub mod request {
        pub type Cmd = kvproto::tikvpb::BatchCommandsRequest_Request_oneof_cmd;
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures::sync::oneshot;

    use super::*;
    use crate::storage;
    use crate::storage::mvcc::Error as MvccError;
    use crate::storage::txn::Error as TxnError;

    #[test]
    fn test_extract_key_error_write_conflict() {
        let start_ts = 110;
        let conflict_start_ts = 108;
        let conflict_commit_ts = 109;
        let key = b"key".to_vec();
        let primary = b"primary".to_vec();
        let case = storage::Error::from(TxnError::from(MvccError::WriteConflict {
            start_ts,
            conflict_start_ts,
            conflict_commit_ts,
            key: key.clone(),
            primary: primary.clone(),
        }));
        let mut expect = KeyError::default();
        let mut write_conflict = WriteConflict::default();
        write_conflict.set_start_ts(start_ts);
        write_conflict.set_conflict_ts(conflict_start_ts);
        write_conflict.set_conflict_commit_ts(conflict_commit_ts);
        write_conflict.set_key(key);
        write_conflict.set_primary(primary);
        expect.set_conflict(write_conflict);
        expect.set_retryable(format!("{:?}", case));

        let got = extract_key_error(&case);
        assert_eq!(got, expect);
    }

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
