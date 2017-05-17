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

use std::boxed::FnBox;
use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use mio::Token;
use grpc::{RpcContext, UnarySink, ClientStreamingSink, RequestStream, RpcStatus, RpcStatusCode};
use futures::{future, Future, Stream};
use futures::sync::oneshot;
use futures_cpupool::CpuPool;
use protobuf::RepeatedField;
use kvproto::tikvpb_grpc;
use kvproto::raft_serverpb::*;
use kvproto::kvrpcpb::*;
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};

use util::worker::Scheduler;
use util::buf::PipeBuffer;
use storage::{self, Storage, Key, Options, Mutation};
use storage::txn::Error as TxnError;
use storage::mvcc::Error as MvccError;
use storage::engine::Error as EngineError;
use super::transport::RaftStoreRouter;
use super::coprocessor::{RequestTask, EndPointTask};
use super::snap::Task as SnapTask;
use super::metrics::*;
use super::Error;

#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static> {
    pool: CpuPool,
    // For handling KV requests.
    storage: Storage,
    // For handling coprocessor requests.
    end_point_scheduler: Scheduler<EndPointTask>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,
    token: Arc<AtomicUsize>, // TODO: remove it.
}

impl<T: RaftStoreRouter + 'static> Service<T> {
    pub fn new(pool: CpuPool,
               storage: Storage,
               end_point_scheduler: Scheduler<EndPointTask>,
               ch: T,
               snap_scheduler: Scheduler<SnapTask>)
               -> Service<T> {
        Service {
            pool: pool,
            storage: storage,
            end_point_scheduler: end_point_scheduler,
            ch: ch,
            snap_scheduler: snap_scheduler,
            token: Arc::new(AtomicUsize::new(1)),
        }
    }
}

fn make_callback<T: Debug + Send + 'static>() -> (Box<FnBox(T) + Send>, oneshot::Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let callback = move |resp| {
        tx.send(resp).unwrap();
    };
    (box callback, rx)
}

impl<T: RaftStoreRouter + 'static> tikvpb_grpc::Tikv for Service<T> {
    fn kv_get(&self, _: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_get"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_get(req.take_context(),
                                            Key::from_raw(req.get_key()),
                                            req.get_version(),
                                            cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
                    .map(|v| {
                        let mut res = GetResponse::new();
                        if let Some(err) = extract_region_error(&v) {
                            res.set_region_error(err);
                        } else {
                            match v {
                                Ok(Some(val)) => res.set_value(val),
                                Ok(None) => res.set_value(vec![]),
                                Err(e) => res.set_error(extract_key_error(&e)),
                            }
                        }
                        res
                    })
                    .and_then(|res| sink.success(res).map_err(Error::from))
                    .map(|_| timer.observe_duration())
                    .map_err(|e| error!("kv_get failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_scan(&self, _: RpcContext, mut req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_scan"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let mut options = Options::default();
                options.key_only = req.get_key_only();

                let (cb, future) = make_callback();
                let res = storage.async_scan(req.take_context(),
                                             Key::from_raw(req.get_start_key()),
                                             req.get_limit() as usize,
                                             req.get_version(),
                                             options,
                                             cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
                    .map(|v| {
                        let mut resp = ScanResponse::new();
                        if let Some(err) = extract_region_error(&v) {
                            resp.set_region_error(err);
                        } else {
                            resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
                        }
                        resp
                    })
                    .and_then(|res| sink.success(res).map_err(Error::from))
                    .map(|_| timer.observe_duration())
                    .map_err(|e| error!("kv_scan failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_prewrite(&self,
                   _: RpcContext,
                   mut req: PrewriteRequest,
                   sink: UnarySink<PrewriteResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_prewrite"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let mutations = req.take_mutations()
                    .into_iter()
                    .map(|mut x| {
                        match x.get_op() {
                            Op::Put => Mutation::Put((Key::from_raw(x.get_key()), x.take_value())),
                            Op::Del => Mutation::Delete(Key::from_raw(x.get_key())),
                            Op::Lock => Mutation::Lock(Key::from_raw(x.get_key())),
                        }
                    })
                    .collect();
                let mut options = Options::default();
                options.lock_ttl = req.get_lock_ttl();
                options.skip_constraint_check = req.get_skip_constraint_check();

                let (cb, future) = make_callback();
                let res = storage.async_prewrite(req.take_context(),
                                                 mutations,
                                                 req.take_primary_lock(),
                                                 req.get_start_version(),
                                                 options,
                                                 cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_prewrite failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_commit(&self, _: RpcContext, mut req: CommitRequest, sink: UnarySink<CommitResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_commit"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

                let (cb, future) = make_callback();
                let res = storage.async_commit(req.take_context(),
                                               keys,
                                               req.get_start_version(),
                                               req.get_commit_version(),
                                               cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_commit failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_cleanup(&self,
                  _: RpcContext,
                  mut req: CleanupRequest,
                  sink: UnarySink<CleanupResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_cleanup"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_cleanup(req.take_context(),
                                                Key::from_raw(req.get_key()),
                                                req.get_start_version(),
                                                cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_cleanup failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_batch_get(&self,
                    _: RpcContext,
                    mut req: BatchGetRequest,
                    sink: UnarySink<BatchGetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_batch_get"]).start_timer();
        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let keys = req.get_keys().into_iter().map(|x| Key::from_raw(x)).collect();

                let (cb, future) = make_callback();
                let res = storage.async_batch_get(req.take_context(), keys, req.get_version(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
                    .map(|v| {
                        let mut resp = BatchGetResponse::new();
                        if let Some(err) = extract_region_error(&v) {
                            resp.set_region_error(err);
                        } else {
                            resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)))
                        }
                        resp
                    })
                    .and_then(|res| sink.success(res).map_err(Error::from))
                    .map(|_| timer.observe_duration())
                    .map_err(|e| error!("kv_batch_get failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_batch_rollback(&self,
                         _: RpcContext,
                         mut req: BatchRollbackRequest,
                         sink: UnarySink<BatchRollbackResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_batch_rollback"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let keys = req.get_keys().into_iter().map(|x| Key::from_raw(x)).collect();

                let (cb, future) = make_callback();
                let res =
                    storage.async_rollback(req.take_context(), keys, req.get_start_version(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_batch_rollback failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_scan_lock(&self,
                    _: RpcContext,
                    mut req: ScanLockRequest,
                    sink: UnarySink<ScanLockResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_scan_lock"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_scan_lock(req.take_context(), req.get_max_version(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_scan_lock failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_resolve_lock(&self,
                       _: RpcContext,
                       mut req: ResolveLockRequest,
                       sink: UnarySink<ResolveLockResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_resolve_lock"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let commit_ts = match req.get_commit_version() {
                    0 => None,
                    x => Some(x),
                };

                let (cb, future) = make_callback();
                let res =
                    storage.async_resolve_lock(req.take_context(),
                                               req.get_start_version(),
                                               commit_ts,
                                               cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_resolve_lock failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn kv_gc(&self, _: RpcContext, mut req: GCRequest, sink: UnarySink<GCResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["kv_gc"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_gc(req.take_context(), req.get_safe_point(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("kv_gc failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn raw_get(&self, _: RpcContext, mut req: RawGetRequest, sink: UnarySink<RawGetResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["raw_get"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_raw_get(req.take_context(), req.take_key(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
                    .map(|v| {
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
                        resp
                    })
                    .and_then(|res| sink.success(res).map_err(Error::from))
                    .map(|_| timer.observe_duration())
                    .map_err(|e| error!("raw_get failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn raw_put(&self, _: RpcContext, mut req: RawPutRequest, sink: UnarySink<RawPutResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["raw_put"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res =
                    storage.async_raw_put(req.take_context(), req.take_key(), req.take_value(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("raw_put failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn raw_delete(&self,
                  _: RpcContext,
                  mut req: RawDeleteRequest,
                  sink: UnarySink<RawDeleteResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["raw_delete"]).start_timer();

        let storage = self.storage.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res = storage.async_raw_delete(req.take_context(), req.take_key(), cb);
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
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
                    .map_err(|e| error!("raw_delete failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn coprocessor(&self, _: RpcContext, req: Request, sink: UnarySink<Response>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.with_label_values(&["coprocessor"]).start_timer();

        let end_point_scheduler = self.end_point_scheduler.clone();
        self.pool
            .spawn_fn(move || {
                let (cb, future) = make_callback();
                let res =
                    end_point_scheduler.schedule(EndPointTask::Request(RequestTask::new(req, cb)));
                if let Err(e) = res {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted,
                                                Some(format!("{}", e)));
                    return sink.fail(status).map_err(|e| error!("kv_get failed: {:?}", e)).boxed();
                }
                future.map_err(Error::from)
                    .and_then(|res| sink.success(res).map_err(Error::from))
                    .map(|_| timer.observe_duration())
                    .map_err(|e| error!("coprocessor failed: {:?}", e))
                    .boxed()
            })
            .forget();
    }

    fn raft(&self,
            _: RpcContext,
            stream: RequestStream<RaftMessage>,
            _: ClientStreamingSink<Done>) {
        let ch = self.ch.clone();
        self.pool
            .spawn(stream.map_err(Error::from)
                .for_each(move |msg| future::result(ch.send_raft_msg(msg)).map_err(Error::from))
                .then(|_| future::ok::<_, ()>(())))
            .forget();
    }

    fn snapshot(&self,
                _: RpcContext,
                stream: RequestStream<SnapshotChunk>,
                sink: ClientStreamingSink<Done>) {
        let token = Token(self.token.fetch_add(1, Ordering::SeqCst));
        let sched = self.snap_scheduler.clone();
        let sched2 = sched.clone();
        self.pool
            .spawn(stream.map_err(Error::from)
                .for_each(move |mut chunk| {
                    let res = if chunk.has_message() {
                        sched.schedule(SnapTask::Register(token, chunk.take_message()))
                            .map_err(Error::from)
                    } else if !chunk.get_data().is_empty() {
                        // TODO: Remove PipeBuffer or take good use of it.
                        let mut b = PipeBuffer::new(chunk.get_data().len());
                        b.write_all(chunk.get_data()).unwrap();
                        sched.schedule(SnapTask::Write(token, b)).map_err(Error::from)
                    } else {
                        Err(box_err!("empty chunk"))
                    };
                    future::result(res)
                })
                .then(move |res| {
                    let res = match res {
                        Ok(_) => sched2.schedule(SnapTask::Close(token)),
                        Err(e) => {
                            error!("receive snapshot err: {}", e);
                            sched2.schedule(SnapTask::Discard(token))
                        }
                    };
                    future::result(res.map_err(Error::from))
                })
                .and_then(|_| sink.success(Done::new()).map_err(Error::from))
                .then(|_| future::ok::<_, ()>(())))
            .forget();
    }
}

fn extract_region_error<T>(res: &storage::Result<T>) -> Option<RegionError> {
    use storage::Error;
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error::Engine(EngineError::Request(ref e))) |
        Err(Error::Txn(TxnError::Engine(EngineError::Request(ref e)))) |
        Err(Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(ref e))))) => {
            Some(e.to_owned())
        }
        Err(Error::SchedTooBusy) => {
            let mut err = RegionError::new();
            err.set_server_is_busy(ServerIsBusy::new());
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
        storage::Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked { ref key,
                                                                    ref primary,
                                                                    ts,
                                                                    ttl })) => {
            let mut lock_info = LockInfo::new();
            lock_info.set_key(key.to_owned());
            lock_info.set_primary_lock(primary.to_owned());
            lock_info.set_lock_version(ts);
            lock_info.set_lock_ttl(ttl);
            key_error.set_locked(lock_info);
        }
        storage::Error::Txn(TxnError::Mvcc(MvccError::WriteConflict)) |
        storage::Error::Txn(TxnError::Mvcc(MvccError::TxnLockNotFound)) => {
            debug!("txn conflicts: {}", err);
            key_error.set_retryable(format!("{:?}", err));
        }
        _ => {
            error!("txn aborts: {}", err);
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

fn extract_kv_pairs(res: storage::Result<Vec<storage::Result<storage::KvPair>>>) -> Vec<KvPair> {
    match res {
        Ok(res) => {
            res.into_iter()
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
                .collect()
        }
        Err(e) => {
            let mut pair = KvPair::new();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

fn extract_key_errors(res: storage::Result<Vec<storage::Result<()>>>) -> Vec<KeyError> {
    match res {
        Ok(res) => {
            res.into_iter()
                .filter_map(|x| match x {
                    Err(e) => Some(extract_key_error(&e)),
                    Ok(_) => None,
                })
                .collect()
        }
        Err(e) => vec![extract_key_error(&e)],
    }
}
