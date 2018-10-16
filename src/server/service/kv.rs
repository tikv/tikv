// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
use std::iter::{self, FromIterator};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;

use futures::{future, Async, Future, Poll, Sink, Stream};
use grpc::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus,
    RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};
use kvproto::kvrpcpb;
use kvproto::kvrpcpb::*;
use kvproto::raft_serverpb::*;
use kvproto::tikvpb::*;
use kvproto::tikvpb_grpc;
use protobuf::RepeatedField;
use tokio::runtime::{Runtime, TaskExecutor};

use coprocessor::Endpoint;
use raftstore::store::{Callback, Msg as StoreMessage};
use server::metrics::*;
use server::snap::Task as SnapTask;
use server::transport::RaftStoreRouter;
use server::Error;
use storage::engine::Error as EngineError;
use storage::mvcc::{Error as MvccError, LockType, Write as MvccWrite, WriteType};
use storage::txn::Error as TxnError;
use storage::{self, Engine, Key, Mutation, Options, Storage, Value};
use util::collections::HashMap;
use util::future::{paired_future_callback, AndThenWith};
use util::mpsc::{unbounded, Receiver, Sender};
use util::worker::Scheduler;

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";
const GC_WORKER_IS_BUSY: &str = "gc worker is busy";

#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static, E: Engine> {
    // For handling KV requests.
    storage: Storage<E>,
    // For handling coprocessor requests.
    cop: Endpoint<E>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,

    // A helper thread for super batch. It's used to collect responses for batch_commands
    // interface.
    helper_runtime: Arc<Runtime>,
    in_heavy_load: Arc<AtomicBool>,
}

impl<T: RaftStoreRouter + 'static, E: Engine> Service<T, E> {
    pub fn new(
        storage: Storage<E>,
        cop: Endpoint<E>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        helper_runtime: Arc<Runtime>,
        in_heavy_load: Arc<AtomicBool>,
    ) -> Self {
        Service {
            storage,
            cop,
            ch,
            snap_scheduler,
            helper_runtime,
            in_heavy_load,
        }
    }

    fn send_fail_status<M>(
        &mut self,
        ctx: RpcContext,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

impl<T: RaftStoreRouter + 'static, E: Engine> tikvpb_grpc::Tikv for Service<T, E> {
    fn kv_get(&mut self, ctx: RpcContext, req: GetRequest, sink: UnarySink<GetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_get.start_coarse_timer();
        let future = future_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_get", e);
                GRPC_MSG_FAIL_COUNTER.kv_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan.start_coarse_timer();
        let future = future_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_scan", e);
                GRPC_MSG_FAIL_COUNTER.kv_scan.inc();
            });

        ctx.spawn(future);
    }

    fn kv_prewrite(
        &mut self,
        ctx: RpcContext,
        req: PrewriteRequest,
        sink: UnarySink<PrewriteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_prewrite.start_coarse_timer();
        let future = future_prewrite(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_prewrite", e);
                GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc();
            });

        ctx.spawn(future);
    }

    fn kv_commit(&mut self, ctx: RpcContext, req: CommitRequest, sink: UnarySink<CommitResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_commit.start_coarse_timer();

        let future = future_commit(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_commit", e);
                GRPC_MSG_FAIL_COUNTER.kv_commit.inc();
            });

        ctx.spawn(future);
    }

    fn kv_import(&mut self, _: RpcContext, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_cleanup(
        &mut self,
        ctx: RpcContext,
        req: CleanupRequest,
        sink: UnarySink<CleanupResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_cleanup.start_coarse_timer();
        let future = future_cleanup(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_cleanup", e);
                GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_get(
        &mut self,
        ctx: RpcContext,
        req: BatchGetRequest,
        sink: UnarySink<BatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_batch_get.start_coarse_timer();
        let future = future_batch_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_batch_get", e);
                GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_rollback(
        &mut self,
        ctx: RpcContext,
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
                debug!("{} failed: {:?}", "kv_batch_rollback", e);
                GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan_lock(
        &mut self,
        ctx: RpcContext,
        req: ScanLockRequest,
        sink: UnarySink<ScanLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan_lock.start_coarse_timer();
        let future = future_scan_lock(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_scan_lock", e);
                GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_resolve_lock(
        &mut self,
        ctx: RpcContext,
        req: ResolveLockRequest,
        sink: UnarySink<ResolveLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_resolve_lock.start_coarse_timer();
        let future = future_resolve_lock(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_resolve_lock", e);
                GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_gc(&mut self, ctx: RpcContext, req: GCRequest, sink: UnarySink<GCResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_gc.start_coarse_timer();
        let future = future_gc(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_gc", e);
                GRPC_MSG_FAIL_COUNTER.kv_gc.inc();
            });

        ctx.spawn(future);
    }

    // WARNING: Currently this API may leave some dirty keys in TiKV. Be careful using this API.
    fn kv_delete_range(
        &mut self,
        ctx: RpcContext,
        req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_delete_range.start_coarse_timer();
        let future = future_delete_range(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_delete_range", e);
                GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn raw_get(&mut self, ctx: RpcContext, req: RawGetRequest, sink: UnarySink<RawGetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_get.start_coarse_timer();
        let future = future_raw_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_get", e);
                GRPC_MSG_FAIL_COUNTER.raw_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_get(
        &mut self,
        ctx: RpcContext,
        req: RawBatchGetRequest,
        sink: UnarySink<RawBatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_get.start_coarse_timer();

        let future = future_raw_batch_get(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_get", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_scan(&mut self, ctx: RpcContext, req: RawScanRequest, sink: UnarySink<RawScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_scan.start_coarse_timer();

        let future = future_raw_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_scan", e);
                GRPC_MSG_FAIL_COUNTER.raw_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_scan(
        &mut self,
        ctx: RpcContext,
        req: RawBatchScanRequest,
        sink: UnarySink<RawBatchScanResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_scan.start_coarse_timer();

        let future = future_raw_batch_scan(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_scan", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_put(&mut self, ctx: RpcContext, req: RawPutRequest, sink: UnarySink<RawPutResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_put.start_coarse_timer();
        let future = future_raw_put(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_put", e);
                GRPC_MSG_FAIL_COUNTER.raw_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_put(
        &mut self,
        ctx: RpcContext,
        req: RawBatchPutRequest,
        sink: UnarySink<RawBatchPutResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_put.start_coarse_timer();

        let future = future_raw_batch_put(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_put", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete(
        &mut self,
        ctx: RpcContext,
        req: RawDeleteRequest,
        sink: UnarySink<RawDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete.start_coarse_timer();
        let future = future_raw_delete(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_delete", e);
                GRPC_MSG_FAIL_COUNTER.raw_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_delete(
        &mut self,
        ctx: RpcContext,
        req: RawBatchDeleteRequest,
        sink: UnarySink<RawBatchDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_delete.start_coarse_timer();

        let future = future_raw_batch_delete(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_delete", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete_range(
        &mut self,
        ctx: RpcContext,
        req: RawDeleteRangeRequest,
        sink: UnarySink<RawDeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete_range.start_coarse_timer();

        let future = future_raw_delete_range(&self.storage, req)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_delete_range", e);
                GRPC_MSG_FAIL_COUNTER.raw_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn unsafe_destroy_range(
        &mut self,
        ctx: RpcContext,
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
                let mut resp = UnsafeDestroyRangeResponse::new();
                // Region error is impossible here.
                if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                sink.success(resp).map_err(Error::from)
            })
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "unsafe_destroy_range", e);
                GRPC_MSG_FAIL_COUNTER.unsafe_destroy_range.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor(&mut self, ctx: RpcContext, req: Request, sink: UnarySink<Response>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.coprocessor.start_coarse_timer();
        let future = future_cop(&self.cop, req, Some(ctx.peer()))
            .map_err(Error::from)
            .and_then(|resp| sink.success(resp).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "coprocessor", e);
                GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor_stream(
        &mut self,
        ctx: RpcContext,
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
                let code = RpcStatusCode::Unknown;
                let msg = Some(format!("{:?}", e));
                GrpcError::RpcFailure(RpcStatus::new(code, msg))
            });
        let future = sink
            .send_all(stream)
            .map(|_| timer.observe_duration())
            .map_err(Error::from)
            .map_err(move |e| {
                debug!("{} failed: {:?}", "coprocessor_stream", e);
                GRPC_MSG_FAIL_COUNTER.coprocessor_stream.inc();
            });

        ctx.spawn(future);
    }

    fn raft(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<RaftMessage>,
        _: ClientStreamingSink<Done>,
    ) {
        let ch = self.ch.clone();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |msg| {
                    RAFT_MESSAGE_RECV_COUNTER.inc();
                    future::result(ch.send_raft_msg(msg)).map_err(Error::from)
                })
                .map_err(|e| error!("send raft msg to raft store fail: {}", e))
                .then(|_| future::ok::<_, ()>(())),
        );
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        let task = SnapTask::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::new(RpcStatusCode::ResourceExhausted, None);
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }

    fn mvcc_get_by_key(
        &mut self,
        ctx: RpcContext,
        mut req: MvccGetByKeyRequest,
        sink: UnarySink<MvccGetByKeyResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.mvcc_get_by_key.start_coarse_timer();

        let storage = self.storage.clone();

        let key = Key::from_raw(req.get_key());
        let (cb, future) = paired_future_callback();
        let res = storage.async_mvcc_by_key(req.take_context(), key.clone(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = MvccGetByKeyResponse::new();
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
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "mvcc_get_by_key", e);
                GRPC_MSG_FAIL_COUNTER.mvcc_get_by_key.inc();
            });

        ctx.spawn(future);
    }

    fn mvcc_get_by_start_ts(
        &mut self,
        ctx: RpcContext,
        mut req: MvccGetByStartTsRequest,
        sink: UnarySink<MvccGetByStartTsResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .mvcc_get_by_start_ts
            .start_coarse_timer();

        let storage = self.storage.clone();

        let (cb, future) = paired_future_callback();

        let res = storage.async_mvcc_by_start_ts(req.take_context(), req.get_start_ts(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = MvccGetByStartTsResponse::new();
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
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "mvcc_get_by_start_ts", e);
                GRPC_MSG_FAIL_COUNTER.mvcc_get_by_start_ts.inc();
            });
        ctx.spawn(future);
    }

    fn split_region(
        &mut self,
        ctx: RpcContext,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.split_region.start_coarse_timer();

        let region_id = req.get_context().get_region_id();
        let (cb, future) = paired_future_callback();
        let req = StoreMessage::SplitRegion {
            region_id,
            region_epoch: req.take_context().take_region_epoch(),
            split_keys: vec![Key::from_raw(req.get_split_key()).into_encoded()],
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.try_send(req) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(move |mut v| {
                let mut resp = SplitRegionResponse::new();
                if v.response.get_header().has_error() {
                    resp.set_region_error(v.response.mut_header().take_error());
                } else {
                    let admin_resp = v.response.mut_admin_response();
                    if admin_resp.get_splits().get_regions().len() != 2 {
                        error!(
                            "[region {}] invalid split response: {:?}",
                            region_id, admin_resp
                        );
                        resp.mut_region_error().set_message(format!(
                            "Internal Error: invalid response: {:?}",
                            admin_resp
                        ));
                    } else {
                        let mut regions = admin_resp.mut_splits().take_regions().into_vec();
                        let mut d = regions.drain(..);
                        resp.set_left(d.next().unwrap());
                        resp.set_right(d.next().unwrap());
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "split_region", e);
                GRPC_MSG_FAIL_COUNTER.split_region.inc();
            });

        ctx.spawn(future);
    }

    fn batch_commands(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<BatchCommandsRequest>,
        sink: DuplexSink<BatchCommandsResponse>,
    ) {
        let (tx, rx) = unbounded();
        let executor = self.helper_runtime.executor();

        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let storage = self.storage.clone();
        let cop = self.cop.clone();

        let request_handler = stream.for_each(move |mut req| {
            let request_ids = req.take_request_ids().into_iter();
            let requests = req.take_requests().into_vec();
            GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
            for (id, req) in request_ids
                .zip(requests)
                .filter_map(|(i, req)| req.cmd.map(|r| (i, r)))
            {
                handle_batch_commands_request(
                    &storage,
                    &cop,
                    peer.clone(),
                    &executor,
                    id,
                    req,
                    tx.clone(),
                );
            }
            future::ok::<_, _>(())
        });

        ctx.spawn(
            request_handler.map_err(|e| error!("error when receiving super-batch requests: {}", e)),
        );

        let response_retriever = BatchCommandsRetriever::new(rx, Arc::clone(&self.in_heavy_load))
            .inspect(|r| GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64))
            .map(|r| (r, WriteFlags::default().buffer_hint(false)))
            .map_err(|e| {
                let code = RpcStatusCode::Unknown;
                let msg = Some(format!("{:?}", e));
                GrpcError::RpcFailure(RpcStatus::new(code, msg))
            });

        ctx.spawn(
            sink.send_all(response_retriever)
                .map(|_| ())
                .map_err(|e| debug!("{} failed: {:?}", "coprocessor_stream", e)),
        );
    }
}

fn response_batch_commands_request<F>(
    executor: &TaskExecutor,
    id: u64,
    resp: F,
    tx: Sender<(u64, BatchCommandsResponse_Response)>,
) where
    F: Future<Item = BatchCommandsResponse_Response_oneof_cmd, Error = ()> + Send + 'static,
{
    let f = resp.and_then(move |resp| {
        let mut res = BatchCommandsResponse_Response::new();
        res.cmd = Some(resp);
        tx.send((id, res)).unwrap();
        future::ok::<_, ()>(())
    });
    executor.spawn(f);
}

fn handle_batch_commands_request<E: Engine>(
    storage: &Storage<E>,
    cop: &Endpoint<E>,
    peer: String,
    executor: &TaskExecutor,
    id: u64,
    req: BatchCommandsRequest_Request_oneof_cmd,
    tx: Sender<(u64, BatchCommandsResponse_Response)>,
) {
    match req {
        BatchCommandsRequest_Request_oneof_cmd::Get(req) => {
            let resp = future_get(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::Get)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::Scan(req) => {
            let resp = future_scan(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::Scan)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::Prewrite(req) => {
            let resp = future_prewrite(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::Prewrite)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::Commit(req) => {
            let resp = future_commit(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::Commit)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_commit.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::Import(_) => {
            panic!("unimplemented");
        }
        BatchCommandsRequest_Request_oneof_cmd::Cleanup(req) => {
            let resp = future_cleanup(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::Cleanup)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::BatchGet(req) => {
            let resp = future_batch_get(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::BatchGet)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::BatchRollback(req) => {
            let resp = future_batch_rollback(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::BatchRollback)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::ScanLock(req) => {
            let resp = future_scan_lock(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::ScanLock)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::ResolveLock(req) => {
            let resp = future_resolve_lock(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::ResolveLock)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::GC(req) => {
            let resp = future_gc(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::GC)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_gc.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::DeleteRange(req) => {
            let resp = future_delete_range(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::DeleteRange)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawGet(req) => {
            let resp = future_raw_get(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawGet)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_get.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawBatchGet(req) => {
            let resp = future_raw_batch_get(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawBatchGet)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_get.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawPut(req) => {
            let resp = future_raw_put(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawPut)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_put.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawBatchPut(req) => {
            let resp = future_raw_batch_put(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawBatchPut)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_put.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawDelete(req) => {
            let resp = future_raw_delete(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawDelete)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_delete.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawBatchDelete(req) => {
            let resp = future_raw_batch_delete(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawBatchDelete)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_delete.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawScan(req) => {
            let resp = future_raw_scan(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawScan)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_scan.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawDeleteRange(req) => {
            let resp = future_raw_delete_range(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawDeleteRange)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_delete_range.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::RawBatchScan(req) => {
            let resp = future_raw_batch_scan(&storage, req)
                .map(BatchCommandsResponse_Response_oneof_cmd::RawBatchScan)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_batch_scan.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
        BatchCommandsRequest_Request_oneof_cmd::Coprocessor(req) => {
            let resp = future_cop(&cop, req, Some(peer))
                .map(BatchCommandsResponse_Response_oneof_cmd::Coprocessor)
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.coprocessor.inc());
            response_batch_commands_request(executor, id, resp, tx);
        }
    }
}

// TODO: wait 2ms to avoid little batch.
struct BatchCommandsRetriever {
    receiver: Receiver<(u64, BatchCommandsResponse_Response)>,
    in_heavy_load: Arc<AtomicBool>,
    current_resp: BatchCommandsResponse,
}

impl BatchCommandsRetriever {
    fn new(
        rx: Receiver<(u64, BatchCommandsResponse_Response)>,
        in_heavy_load: Arc<AtomicBool>,
    ) -> Self {
        BatchCommandsRetriever {
            receiver: rx,
            in_heavy_load,
            current_resp: BatchCommandsResponse::default(),
        }
    }
}

impl Stream for BatchCommandsRetriever {
    type Item = BatchCommandsResponse;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.receiver.poll()? {
            Async::Ready(Some((id, resp))) => {
                self.current_resp.mut_request_ids().push(id);
                self.current_resp.mut_responses().push(resp);
            }
            Async::Ready(None) => return Ok(Async::Ready(None)),
            Async::NotReady => return Ok(Async::NotReady),
        }

        loop {
            match self.receiver.try_recv() {
                Ok((id, resp)) => {
                    self.current_resp.mut_request_ids().push(id);
                    self.current_resp.mut_responses().push(resp);
                    if self.current_resp.get_responses().len() < 1000 {
                        continue;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    warn!("super batch channel is closed, it's unexpected");
                }
                _ => {}
            }
            break;
        }

        if self.current_resp.get_responses().is_empty() {
            return Ok(Async::NotReady);
        }

        let mut resp = mem::replace(&mut self.current_resp, BatchCommandsResponse::default());
        resp.set_in_heavy_load(self.in_heavy_load.load(Ordering::SeqCst));
        Ok(Async::Ready(Some(resp)))
    }
}

fn future_get<E: Engine>(
    storage: &Storage<E>,
    mut req: GetRequest,
) -> impl Future<Item = GetResponse, Error = Error> {
    let get_result = storage.async_get(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_version(),
    );

    get_result.then(|v| {
        let mut resp = GetResponse::new();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(Some(val)) => resp.set_value(val),
                Ok(None) => (),
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        Ok(resp)
    })
}

fn future_scan<E: Engine>(
    storage: &Storage<E>,
    mut req: ScanRequest,
) -> impl Future<Item = ScanResponse, Error = Error> {
    let mut options = Options::default();
    options.key_only = req.get_key_only();
    options.reverse_scan = req.get_reverse();
    let scan_result = storage.async_scan(
        req.take_context(),
        Key::from_raw(req.get_start_key()),
        req.get_limit() as usize,
        req.get_version(),
        options,
    );

    scan_result.then(|v| {
        let mut resp = ScanResponse::new();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
        }
        Ok(resp)
    })
}

fn future_prewrite<E: Engine>(
    storage: &Storage<E>,
    mut req: PrewriteRequest,
) -> impl Future<Item = PrewriteResponse, Error = Error> {
    let mutations = req
        .take_mutations()
        .into_iter()
        .map(|mut x| match x.get_op() {
            Op::Put => Mutation::Put((Key::from_raw(x.get_key()), x.take_value())),
            Op::Del => Mutation::Delete(Key::from_raw(x.get_key())),
            Op::Lock => Mutation::Lock(Key::from_raw(x.get_key())),
            _ => panic!("mismatch Op in prewrite mutations"),
        })
        .collect();
    let mut options = Options::default();
    options.lock_ttl = req.get_lock_ttl();
    options.skip_constraint_check = req.get_skip_constraint_check();

    let (cb, future) = paired_future_callback();
    let prewrite_result = future::result(storage.async_prewrite(
        req.take_context(),
        mutations,
        req.take_primary_lock(),
        req.get_start_version(),
        options,
        cb,
    )).map_err(Error::from);

    prewrite_result.and_then(|_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = PrewriteResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_errors(RepeatedField::from_vec(extract_key_errors(v)));
            }
            resp
        })
    })
}

fn future_commit<E: Engine>(
    storage: &Storage<E>,
    mut req: CommitRequest,
) -> impl Future<Item = CommitResponse, Error = Error> {
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let (cb, future) = paired_future_callback();
    let commit_result = future::result(storage.async_commit(
        req.take_context(),
        keys,
        req.get_start_version(),
        req.get_commit_version(),
        cb,
    )).map_err(Error::from);

    commit_result.and_then(|_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = CommitResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(extract_key_error(&e));
            }
            resp
        })
    })
}

fn future_cleanup<E: Engine>(
    storage: &Storage<E>,
    mut req: CleanupRequest,
) -> impl Future<Item = CleanupResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let cleanup_result = future::result(storage.async_cleanup(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_start_version(),
        cb,
    )).map_err(Error::from);

    cleanup_result.and_then(|_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = CleanupResponse::new();
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
    })
}

fn future_batch_get<E: Engine>(
    storage: &Storage<E>,
    mut req: BatchGetRequest,
) -> impl Future<Item = BatchGetResponse, Error = Error> {
    let keys = req
        .get_keys()
        .into_iter()
        .map(|x| Key::from_raw(x))
        .collect();

    let get_result = storage.async_batch_get(req.take_context(), keys, req.get_version());
    get_result.then(|v| {
        let mut resp = BatchGetResponse::new();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
        }
        Ok(resp)
    })
}

fn future_batch_rollback<E: Engine>(
    storage: &Storage<E>,
    mut req: BatchRollbackRequest,
) -> impl Future<Item = BatchRollbackResponse, Error = Error> {
    let keys = req
        .get_keys()
        .into_iter()
        .map(|x| Key::from_raw(x))
        .collect();

    let (cb, future) = paired_future_callback();
    let rollback_result = future::result(storage.async_rollback(
        req.take_context(),
        keys,
        req.get_start_version(),
        cb,
    )).map_err(Error::from);

    rollback_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = BatchRollbackResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(extract_key_error(&e));
            }
            resp
        })
    })
}

fn future_scan_lock<E: Engine>(
    storage: &Storage<E>,
    mut req: ScanLockRequest,
) -> impl Future<Item = ScanLockResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let scan_result = future::result(storage.async_scan_locks(
        req.take_context(),
        req.get_max_version(),
        req.take_start_key(),
        req.get_limit() as usize,
        cb,
    )).map_err(Error::from);

    scan_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = ScanLockResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                match v {
                    Ok(locks) => resp.set_locks(RepeatedField::from_vec(locks)),
                    Err(e) => resp.set_error(extract_key_error(&e)),
                }
            }
            resp
        })
    })
}

fn future_resolve_lock<E: Engine>(
    storage: &Storage<E>,
    mut req: ResolveLockRequest,
) -> impl Future<Item = ResolveLockResponse, Error = Error> {
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

    let (cb, future) = paired_future_callback();
    let resolve_result =
        future::result(storage.async_resolve_lock(req.take_context(), txn_status, cb))
            .map_err(Error::from);

    resolve_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = ResolveLockResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(extract_key_error(&e));
            }
            resp
        })
    })
}
fn future_gc<E: Engine>(
    storage: &Storage<E>,
    mut req: GCRequest,
) -> impl Future<Item = GCResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let gc_result = future::result(storage.async_gc(req.take_context(), req.get_safe_point(), cb))
        .map_err(Error::from);

    gc_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = GCResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(extract_key_error(&e));
            }
            resp
        })
    })
}

fn future_delete_range<E: Engine>(
    storage: &Storage<E>,
    mut req: DeleteRangeRequest,
) -> impl Future<Item = DeleteRangeResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let delete_result = future::result(storage.async_delete_range(
        req.take_context(),
        Key::from_raw(req.get_start_key()),
        Key::from_raw(req.get_end_key()),
        cb,
    )).map_err(Error::from);

    delete_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = DeleteRangeResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
    })
}

fn future_raw_get<E: Engine>(
    storage: &Storage<E>,
    mut req: RawGetRequest,
) -> impl Future<Item = RawGetResponse, Error = Error> {
    storage
        .async_raw_get(req.take_context(), req.take_cf(), req.take_key())
        .then(|v| {
            let mut resp = RawGetResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                match v {
                    Ok(Some(val)) => resp.set_value(val),
                    Ok(None) => {}
                    Err(e) => resp.set_error(format!("{}", e)),
                }
            }
            Ok(resp)
        })
}

fn future_raw_batch_get<E: Engine>(
    storage: &Storage<E>,
    mut req: RawBatchGetRequest,
) -> impl Future<Item = RawBatchGetResponse, Error = Error> {
    let keys = req.take_keys().into_vec();
    storage
        .async_raw_batch_get(req.take_context(), req.take_cf(), keys)
        .then(|v| {
            let mut resp = RawBatchGetResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
            }
            Ok(resp)
        })
}

fn future_raw_put<E: Engine>(
    storage: &Storage<E>,
    mut req: RawPutRequest,
) -> impl Future<Item = RawPutResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let put_result = future::result(storage.async_raw_put(
        req.take_context(),
        req.take_cf(),
        req.take_key(),
        req.take_value(),
        cb,
    )).map_err(Error::from);

    put_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = RawPutResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
    })
}

fn future_raw_batch_put<E: Engine>(
    storage: &Storage<E>,
    mut req: RawBatchPutRequest,
) -> impl Future<Item = RawBatchPutResponse, Error = Error> {
    let cf = req.take_cf();
    let pairs = req
        .take_pairs()
        .into_iter()
        .map(|mut x| (x.take_key(), x.take_value()))
        .collect();

    let (cb, future) = paired_future_callback();
    let put_result = future::result(storage.async_raw_batch_put(req.take_context(), cf, pairs, cb))
        .map_err(Error::from);

    put_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = RawBatchPutResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
    })
}

fn future_raw_delete<E: Engine>(
    storage: &Storage<E>,
    mut req: RawDeleteRequest,
) -> impl Future<Item = RawDeleteResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let delete_result = future::result(storage.async_raw_delete(
        req.take_context(),
        req.take_cf(),
        req.take_key(),
        cb,
    )).map_err(Error::from);

    delete_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = RawDeleteResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
    })
}

fn future_raw_batch_delete<E: Engine>(
    storage: &Storage<E>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Item = RawBatchDeleteResponse, Error = Error> {
    let cf = req.take_cf();
    let keys = req.take_keys().into_vec();
    let (cb, future) = paired_future_callback();
    let delete_result =
        future::result(storage.async_raw_batch_delete(req.take_context(), cf, keys, cb))
            .map_err(Error::from);

    delete_result.and_then(move |_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = RawBatchDeleteResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
    })
}

fn future_raw_scan<E: Engine>(
    storage: &Storage<E>,
    mut req: RawScanRequest,
) -> impl Future<Item = RawScanResponse, Error = Error> {
    storage
        .async_raw_scan(
            req.take_context(),
            req.take_cf(),
            req.take_start_key(),
            req.get_limit() as usize,
            req.get_key_only(),
        )
        .then(|v| {
            let mut resp = RawScanResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_kvs(RepeatedField::from_vec(extract_kv_pairs(v)));
            }
            Ok(resp)
        })
}

fn future_raw_batch_scan<E: Engine>(
    storage: &Storage<E>,
    mut req: RawBatchScanRequest,
) -> impl Future<Item = RawBatchScanResponse, Error = Error> {
    storage
        .async_raw_batch_scan(
            req.take_context(),
            req.take_cf(),
            req.take_ranges().into_vec(),
            req.get_each_limit() as usize,
            req.get_key_only(),
        )
        .then(|v| {
            let mut resp = RawBatchScanResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else {
                resp.set_kvs(RepeatedField::from_vec(extract_kv_pairs(v)));
            }
            Ok(resp)
        })
}

fn future_raw_delete_range<E: Engine>(
    storage: &Storage<E>,
    mut req: RawDeleteRangeRequest,
) -> impl Future<Item = RawDeleteRangeResponse, Error = Error> {
    let (cb, future) = paired_future_callback();
    let delete_result = future::result(storage.async_raw_delete_range(
        req.take_context(),
        req.take_cf(),
        req.take_start_key(),
        req.take_end_key(),
        cb,
    )).map_err(Error::from);

    delete_result.and_then(|_| {
        future.map_err(Error::from).map(|v| {
            let mut resp = RawDeleteRangeResponse::new();
            if let Some(err) = extract_region_error(&v) {
                resp.set_region_error(err);
            } else if let Err(e) = v {
                resp.set_error(format!("{}", e));
            }
            resp
        })
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
    use storage::Error;
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error::Engine(EngineError::Request(ref e)))
        | Err(Error::Txn(TxnError::Engine(EngineError::Request(ref e))))
        | Err(Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(ref e))))) => {
            Some(e.to_owned())
        }
        Err(Error::SchedTooBusy) => {
            let mut err = RegionError::new();
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(SCHEDULER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error::GCWorkerTooBusy) => {
            let mut err = RegionError::new();
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(GC_WORKER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
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
    let mut key_error = KeyError::new();
    match *err {
        storage::Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked {
            ref key,
            ref primary,
            ts,
            ttl,
        })) => {
            let mut lock_info = LockInfo::new();
            lock_info.set_key(key.to_owned());
            lock_info.set_primary_lock(primary.to_owned());
            lock_info.set_lock_version(ts);
            lock_info.set_lock_ttl(ttl);
            key_error.set_locked(lock_info);
        }
        // failed in prewrite
        storage::Error::Txn(TxnError::Mvcc(MvccError::WriteConflict {
            start_ts,
            conflict_ts,
            ref key,
            ref primary,
        })) => {
            let mut write_conflict = WriteConflict::new();
            write_conflict.set_start_ts(start_ts);
            write_conflict.set_conflict_ts(conflict_ts);
            write_conflict.set_key(key.to_owned());
            write_conflict.set_primary(primary.to_owned());
            key_error.set_conflict(write_conflict);
            // for compatibility with older versions.
            key_error.set_retryable(format!("{:?}", err));
        }
        // failed in commit
        storage::Error::Txn(TxnError::Mvcc(MvccError::TxnLockNotFound { .. })) => {
            warn!("txn conflicts: {:?}", err);
            key_error.set_retryable(format!("{:?}", err));
        }
        _ => {
            error!("txn aborts: {:?}", err);
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
                    let mut pair = KvPair::new();
                    pair.set_key(key);
                    pair.set_value(value);
                    pair
                }
                Err(e) => {
                    let mut pair = KvPair::new();
                    pair.set_error(extract_key_error(&e));
                    pair
                }
            })
            .collect(),
        Err(e) => {
            let mut pair = KvPair::new();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

fn extract_mvcc_info(mvcc: storage::MvccInfo) -> MvccInfo {
    let mut mvcc_info = MvccInfo::new();
    if let Some(lock) = mvcc.lock {
        let mut lock_info = MvccLock::new();
        let op = match lock.lock_type {
            LockType::Put => Op::Put,
            LockType::Delete => Op::Del,
            LockType::Lock => Op::Lock,
        };
        lock_info.set_field_type(op);
        lock_info.set_start_ts(lock.ts);
        lock_info.set_primary(lock.primary);
        lock_info.set_short_value(lock.short_value.unwrap_or_default());
        mvcc_info.set_lock(lock_info);
    }
    let vv = extract_2pc_values(mvcc.values);
    let vw = extract_2pc_writes(mvcc.writes);
    mvcc_info.set_writes(RepeatedField::from_vec(vw));
    mvcc_info.set_values(RepeatedField::from_vec(vv));
    mvcc_info
}

fn extract_2pc_values(res: Vec<(u64, Value)>) -> Vec<MvccValue> {
    res.into_iter()
        .map(|(start_ts, value)| {
            let mut value_info = MvccValue::new();
            value_info.set_start_ts(start_ts);
            value_info.set_value(value);
            value_info
        })
        .collect()
}

fn extract_2pc_writes(res: Vec<(u64, MvccWrite)>) -> Vec<kvrpcpb::MvccWrite> {
    res.into_iter()
        .map(|(commit_ts, write)| {
            let mut write_info = kvrpcpb::MvccWrite::new();
            let op = match write.write_type {
                WriteType::Put => Op::Put,
                WriteType::Delete => Op::Del,
                WriteType::Lock => Op::Lock,
                WriteType::Rollback => Op::Rollback,
            };
            write_info.set_field_type(op);
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage;
    use storage::mvcc::Error as MvccError;
    use storage::txn::Error as TxnError;

    #[test]
    fn test_extract_key_error_write_conflict() {
        let start_ts = 110;
        let conflict_ts = 108;
        let key = b"key".to_vec();
        let primary = b"primary".to_vec();
        let case = storage::Error::from(TxnError::from(MvccError::WriteConflict {
            start_ts,
            conflict_ts,
            key: key.clone(),
            primary: primary.clone(),
        }));
        let mut expect = KeyError::new();
        let mut write_conflict = WriteConflict::new();
        write_conflict.set_start_ts(start_ts);
        write_conflict.set_conflict_ts(conflict_ts);
        write_conflict.set_key(key);
        write_conflict.set_primary(primary);
        expect.set_conflict(write_conflict);
        expect.set_retryable(format!("{:?}", case));

        let got = extract_key_error(&case);
        assert_eq!(got, expect);
    }

}
