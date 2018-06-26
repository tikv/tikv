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

use futures::sync::{mpsc as futures_mpsc, oneshot};
use futures::{future, stream, Future, Sink, Stream};
use grpc::{
    ClientStreamingSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus, RpcStatusCode,
    ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};
use kvproto::kvrpcpb;
use kvproto::kvrpcpb::*;
use kvproto::raft_serverpb::*;
use kvproto::tikvpb_grpc;
use protobuf::RepeatedField;
use std::iter::{self, FromIterator};

use coprocessor::local_metrics::BasicLocalMetrics;
use coprocessor::{err_resp, EndPointTask, RequestTask};
use raftstore::store::{Callback, Msg as StoreMessage};
use server::metrics::*;
use server::snap::Task as SnapTask;
use server::transport::RaftStoreRouter;
use server::{Error, OnResponse};
use storage::engine::Error as EngineError;
use storage::mvcc::{Error as MvccError, LockType, Write as MvccWrite, WriteType};
use storage::txn::Error as TxnError;
use storage::{self, Key, Mutation, Options, Storage, Value};
use util::collections::HashMap;
use util::future::paired_future_callback;
use util::worker::Scheduler;

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";

#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static> {
    // For handling KV requests.
    storage: Storage,
    // For handling coprocessor requests.
    end_point_scheduler: Scheduler<EndPointTask>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,
    recursion_limit: u32,
    stream_channel_size: usize,
}

impl<T: RaftStoreRouter + 'static> Service<T> {
    pub fn new(
        storage: Storage,
        end_point_scheduler: Scheduler<EndPointTask>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        recursion_limit: u32,
        stream_channel_size: usize,
    ) -> Service<T> {
        Service {
            storage,
            end_point_scheduler,
            ch,
            snap_scheduler,
            recursion_limit,
            stream_channel_size,
        }
    }

    fn send_fail_status<M>(
        &self,
        ctx: RpcContext,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }

    fn send_fail_status_to_stream<M>(
        &self,
        ctx: RpcContext,
        sink: ServerStreamingSink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

impl<T: RaftStoreRouter + 'static> tikvpb_grpc::Tikv for Service<T> {
    fn kv_get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_get.start_coarse_timer();

        let future = self
            .storage
            .async_get(
                req.take_context(),
                Key::from_raw(req.get_key()),
                req.get_version(),
            )
            .then(|v| {
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_get", e);
                GRPC_MSG_FAIL_COUNTER.kv_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan(&self, ctx: RpcContext, mut req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan.start_coarse_timer();

        let mut options = Options::default();
        options.key_only = req.get_key_only();
        options.reverse_scan = req.get_reverse();

        let future = self
            .storage
            .async_scan(
                req.take_context(),
                Key::from_raw(req.get_start_key()),
                req.get_limit() as usize,
                req.get_version(),
                options,
            )
            .then(|v| {
                let mut resp = ScanResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
                }
                Ok(resp)
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_scan", e);
                GRPC_MSG_FAIL_COUNTER.kv_scan.inc();
            });

        ctx.spawn(future);
    }

    fn kv_prewrite(
        &self,
        ctx: RpcContext,
        mut req: PrewriteRequest,
        sink: UnarySink<PrewriteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_prewrite.start_coarse_timer();

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
        let res = self.storage.async_prewrite(
            req.take_context(),
            mutations,
            req.take_primary_lock(),
            req.get_start_version(),
            options,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = PrewriteResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_errors(RepeatedField::from_vec(extract_key_errors(v)));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_prewrite", e);
                GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc();
            });

        ctx.spawn(future);
    }

    fn kv_commit(&self, ctx: RpcContext, mut req: CommitRequest, sink: UnarySink<CommitResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_commit.start_coarse_timer();

        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_commit(
            req.take_context(),
            keys,
            req.get_start_version(),
            req.get_commit_version(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = CommitResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_commit", e);
                GRPC_MSG_FAIL_COUNTER.kv_commit.inc();
            });

        ctx.spawn(future);
    }

    fn kv_import(&self, _: RpcContext, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_cleanup(
        &self,
        ctx: RpcContext,
        mut req: CleanupRequest,
        sink: UnarySink<CleanupResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_cleanup.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_cleanup(
            req.take_context(),
            Key::from_raw(req.get_key()),
            req.get_start_version(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_cleanup", e);
                GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_get(
        &self,
        ctx: RpcContext,
        mut req: BatchGetRequest,
        sink: UnarySink<BatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_batch_get.start_coarse_timer();

        let keys = req
            .get_keys()
            .into_iter()
            .map(|x| Key::from_raw(x))
            .collect();

        let future = self
            .storage
            .async_batch_get(req.take_context(), keys, req.get_version())
            .then(|v| {
                let mut resp = BatchGetResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
                }
                Ok(resp)
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_batch_get", e);
                GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_rollback(
        &self,
        ctx: RpcContext,
        mut req: BatchRollbackRequest,
        sink: UnarySink<BatchRollbackResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .kv_batch_rollback
            .start_coarse_timer();

        let keys = req
            .get_keys()
            .into_iter()
            .map(|x| Key::from_raw(x))
            .collect();

        let (cb, future) = paired_future_callback();
        let res =
            self.storage
                .async_rollback(req.take_context(), keys, req.get_start_version(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = BatchRollbackResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_batch_rollback", e);
                GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan_lock(
        &self,
        ctx: RpcContext,
        mut req: ScanLockRequest,
        sink: UnarySink<ScanLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_scan_lock.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_scan_lock(
            req.take_context(),
            req.get_max_version(),
            req.take_start_key(),
            req.get_limit() as usize,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_scan_lock", e);
                GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_resolve_lock(
        &self,
        ctx: RpcContext,
        mut req: ResolveLockRequest,
        sink: UnarySink<ResolveLockResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_resolve_lock.start_coarse_timer();

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
        let res = self
            .storage
            .async_resolve_lock(req.take_context(), txn_status, cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = ResolveLockResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_resolve_lock", e);
                GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc();
            });

        ctx.spawn(future);
    }

    fn kv_gc(&self, ctx: RpcContext, mut req: GCRequest, sink: UnarySink<GCResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_gc.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self
            .storage
            .async_gc(req.take_context(), req.get_safe_point(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = GCResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
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
        &self,
        ctx: RpcContext,
        mut req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.kv_delete_range.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_delete_range(
            req.take_context(),
            Key::from_raw(req.get_start_key()),
            Key::from_raw(req.get_end_key()),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = DeleteRangeResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "kv_delete_range", e);
                GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn raw_get(&self, ctx: RpcContext, mut req: RawGetRequest, sink: UnarySink<RawGetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_get.start_coarse_timer();

        let future = self
            .storage
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_get", e);
                GRPC_MSG_FAIL_COUNTER.raw_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_get(
        &self,
        ctx: RpcContext,
        mut req: RawBatchGetRequest,
        sink: UnarySink<RawBatchGetResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_get.start_coarse_timer();

        let keys = req.take_keys().into_vec();
        let future = self
            .storage
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_get", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_get.inc();
            });

        ctx.spawn(future);
    }

    fn raw_scan(&self, ctx: RpcContext, mut req: RawScanRequest, sink: UnarySink<RawScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_scan.start_coarse_timer();

        let future = self
            .storage
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_scan", e);
                GRPC_MSG_FAIL_COUNTER.raw_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_scan(
        &self,
        ctx: RpcContext,
        mut req: RawBatchScanRequest,
        sink: UnarySink<RawBatchScanResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_scan.start_coarse_timer();

        let future = self
            .storage
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
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_scan", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_scan.inc();
            });

        ctx.spawn(future);
    }

    fn raw_put(&self, ctx: RpcContext, mut req: RawPutRequest, sink: UnarySink<RawPutResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_put.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_raw_put(
            req.take_context(),
            req.take_cf(),
            req.take_key(),
            req.take_value(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawPutResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_put", e);
                GRPC_MSG_FAIL_COUNTER.raw_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_put(
        &self,
        ctx: RpcContext,
        mut req: RawBatchPutRequest,
        sink: UnarySink<RawBatchPutResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_put.start_coarse_timer();

        let pairs = req
            .take_pairs()
            .into_iter()
            .map(|mut x| (x.take_key(), x.take_value()))
            .collect();
        let (cb, future) = paired_future_callback();
        let res = self
            .storage
            .async_raw_batch_put(req.take_context(), req.take_cf(), pairs, cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawBatchPutResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_put", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_put.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete(
        &self,
        ctx: RpcContext,
        mut req: RawDeleteRequest,
        sink: UnarySink<RawDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res =
            self.storage
                .async_raw_delete(req.take_context(), req.take_cf(), req.take_key(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawDeleteResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_delete", e);
                GRPC_MSG_FAIL_COUNTER.raw_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_batch_delete(
        &self,
        ctx: RpcContext,
        mut req: RawBatchDeleteRequest,
        sink: UnarySink<RawBatchDeleteResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_batch_delete.start_coarse_timer();

        let keys = req.take_keys().into_vec();
        let (cb, future) = paired_future_callback();
        let res = self
            .storage
            .async_raw_batch_delete(req.take_context(), req.take_cf(), keys, cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawBatchDeleteResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_batch_delete", e);
                GRPC_MSG_FAIL_COUNTER.raw_batch_delete.inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete_range(
        &self,
        ctx: RpcContext,
        mut req: RawDeleteRangeRequest,
        sink: UnarySink<RawDeleteRangeResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.raw_delete_range.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let res = self.storage.async_raw_delete_range(
            req.take_context(),
            req.take_cf(),
            req.take_start_key(),
            req.take_end_key(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawDeleteRangeResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "raw_delete_range", e);
                GRPC_MSG_FAIL_COUNTER.raw_delete_range.inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor(&self, ctx: RpcContext, req: Request, sink: UnarySink<Response>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.coprocessor.start_coarse_timer();

        let (tx, rx) = oneshot::channel();
        let on_resp = OnResponse::Unary(tx);
        let req_task = match RequestTask::new(ctx.peer(), req, on_resp, self.recursion_limit) {
            Ok(req_task) => req_task,
            Err(e) => {
                let mut metrics = BasicLocalMetrics::default();
                let future = sink
                    .success(err_resp(e, &mut metrics))
                    .map(|_| timer.observe_duration())
                    .map_err(move |e| {
                        debug!("{} failed: {:?}", "coprocessor", e);
                        GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
                    });
                return ctx.spawn(future);
            }
        };

        let task = EndPointTask::Request(req_task);
        if let Err(e) = self.end_point_scheduler.schedule(task) {
            let error = Error::from(e);
            let code = RpcStatusCode::ResourceExhausted;
            return self.send_fail_status(ctx, sink, error, code);
        }

        let future = rx
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
        &self,
        ctx: RpcContext,
        req: Request,
        sink: ServerStreamingSink<Response>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .coprocessor_stream
            .start_coarse_timer();

        let (tx, rx) = futures_mpsc::channel(self.stream_channel_size);
        let on_resp = OnResponse::Streaming(tx);
        let req_task = match RequestTask::new(ctx.peer(), req, on_resp, self.recursion_limit) {
            Ok(req_task) => req_task,
            Err(e) => {
                let mut metrics = BasicLocalMetrics::default();
                let stream = stream::once::<_, GrpcError>(Ok(err_resp(e, &mut metrics)))
                    .map(|resp| (resp, WriteFlags::default()));
                let future = sink
                    .send_all(stream)
                    .map(|_| timer.observe_duration())
                    .map_err(move |e| {
                        debug!("{} failed: {:?}", "coprocessor_stream", e);
                        GRPC_MSG_FAIL_COUNTER.coprocessor_stream.inc();
                    });
                return ctx.spawn(future);
            }
        };

        let task = EndPointTask::Request(req_task);
        if let Err(e) = self.end_point_scheduler.schedule(task) {
            let error = Error::from(e);
            let code = RpcStatusCode::ResourceExhausted;
            return self.send_fail_status_to_stream(ctx, sink, error, code);
        }

        let stream = rx
            .map(|resp| (resp, WriteFlags::default().buffer_hint(true)))
            .map_err(|e| {
                let code = RpcStatusCode::Unknown;
                let msg = Some(format!("{:?}", e));
                GrpcError::RpcFailure(RpcStatus::new(code, msg))
            });

        let future = sink
            .send_all(stream)
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", "coprocessor_stream", e);
                GRPC_MSG_FAIL_COUNTER.coprocessor_stream.inc();
            });

        ctx.spawn(future);
    }

    fn raft(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
                            resp.set_key(k.raw().unwrap());
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
        &self,
        ctx: RpcContext,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.split_region.start_coarse_timer();

        let (cb, future) = paired_future_callback();
        let req = StoreMessage::SplitRegion {
            region_id: req.get_context().get_region_id(),
            region_epoch: req.take_context().take_region_epoch(),
            split_key: Key::from_raw(req.get_split_key()).encoded().clone(),
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.try_send(req) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|mut v| {
                let mut resp = SplitRegionResponse::new();
                if v.response.get_header().has_error() {
                    resp.set_region_error(v.response.mut_header().take_error());
                } else {
                    let admin_resp = v.response.mut_admin_response();
                    let split_resp = admin_resp.mut_split();
                    resp.set_left(split_resp.take_left());
                    resp.set_right(split_resp.take_right());
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
