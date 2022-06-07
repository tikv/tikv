// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use api_version::KvFormat;
use futures::{
    compat::Future01CompatExt,
    future::{self, Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult,
    RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use kvproto::{
    coprocessor::*,
    kvrpcpb::*,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest},
    raft_serverpb::*,
    tikvpb::*,
};
use rfstore::{
    router::RaftStoreRouter,
    store::{Callback, CasualMessage},
    Error as RaftStoreError,
};
use tikv::{
    coprocessor::Endpoint,
    coprocessor_v2, forward_duplex, forward_unary,
    server::{load_statistics::ThreadLoadPool, metrics::*, Proxy},
    storage::{
        errors::{
            extract_committed, extract_key_error, extract_key_errors, extract_region_error,
            map_kv_pairs,
        },
        kv::Engine,
        lock_manager::LockManager,
        SecondaryLocksStatus, Storage, TxnStatus,
    },
};
use tikv_util::{
    future::{paired_future_callback, poll_future_notify},
    mpsc::batch::{unbounded, BatchCollector, BatchReceiver, Sender},
    time::{duration_to_ms, duration_to_sec, Instant},
};
use txn_types::{self, Key};

use super::batch::{BatcherBuilder, ReqBatcher};
use crate::server::{Error, Result as ServerResult};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
pub struct Service<T: RaftStoreRouter, L: LockManager, F: KvFormat> {
    store_id: u64,
    // For handling KV requests.
    storage: Storage<RaftKv, L, F>,
    // For handling coprocessor requests.
    copr: Endpoint<RaftKv>,
    // For handling corprocessor v2 requests.
    copr_v2: coprocessor_v2::Endpoint,
    // For handling raft messages.
    ch: T,

    enable_req_batch: bool,

    grpc_thread_load: Arc<ThreadLoadPool>,

    proxy: Proxy,
}

impl<T: RaftStoreRouter + Clone + 'static, L: LockManager + Clone, F: KvFormat + Clone> Clone
    for Service<T, L, F>
{
    fn clone(&self) -> Self {
        Service {
            store_id: self.store_id,
            storage: self.storage.clone(),
            copr: self.copr.clone(),
            copr_v2: self.copr_v2.clone(),
            ch: self.ch.clone(),
            enable_req_batch: self.enable_req_batch,
            grpc_thread_load: self.grpc_thread_load.clone(),
            proxy: self.proxy.clone(),
        }
    }
}

impl<T: RaftStoreRouter + 'static, L: LockManager, F: KvFormat> Service<T, L, F> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(
        store_id: u64,
        storage: Storage<RaftKv, L, F>,
        copr: Endpoint<RaftKv>,
        copr_v2: coprocessor_v2::Endpoint,
        ch: T,
        grpc_thread_load: Arc<ThreadLoadPool>,
        enable_req_batch: bool,
        proxy: Proxy,
    ) -> Self {
        Service {
            store_id,
            storage,
            copr,
            copr_v2,
            ch,
            enable_req_batch,
            grpc_thread_load,
            proxy,
        }
    }
}

macro_rules! handle_request {
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident) => {
        fn $fn_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            forward_unary!(self.proxy, $fn_name, ctx, req, sink);
            let begin_instant = Instant::now_coarse();

            let resp = $future_name(&self.storage, req);
            let task = async move {
                let resp = resp.await?;
                sink.success(resp).await?;
                GRPC_MSG_HISTOGRAM_STATIC
                    .$fn_name
                    .observe(duration_to_sec(begin_instant.saturating_elapsed()));
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

impl<T: RaftStoreRouter + 'static, L: LockManager, F: KvFormat> Tikv for Service<T, L, F> {
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

    fn kv_import(&mut self, _: RpcContext<'_>, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_gc(&mut self, ctx: RpcContext<'_>, _: GcRequest, sink: UnarySink<GcResponse>) {
        let e = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED);
        ctx.spawn(
            sink.fail(e)
                .unwrap_or_else(|e| error!("kv rpc failed"; "err" => ?e)),
        );
    }

    fn coprocessor(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        forward_unary!(self.proxy, coprocessor, ctx, req, sink);
        let begin_instant = Instant::now_coarse();
        let future = future_copr(&self.copr, Some(ctx.peer()), req);
        let task = async move {
            let resp = future.await?.consume();
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .coprocessor
                .observe(duration_to_sec(begin_instant.saturating_elapsed()));
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

    fn unsafe_destroy_range(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: UnsafeDestroyRangeRequest,
        sink: UnarySink<UnsafeDestroyRangeResponse>,
    ) {
        let begin_instant = Instant::now_coarse();
        let start = req.take_start_key();
        let end = req.take_end_key();
        // DestroyRange is a very dangerous operation. We don't allow passing MIN_KEY as start, or
        // MAX_KEY as end here.
        if start.is_empty()
            || end.is_empty()
            // All unsafe destroy range issued by TiDB are prefix range.
            // To simplify the logic, we only handle prefix delete range.
            || !tidb_query_common::util::is_prefix_next(&start, &end)
        {
            ctx.spawn(
                sink.fail(RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT))
                    .unwrap_or_else(|e| error!("kv rpc failed"; "err" => ?e)),
            );
            return;
        }
        let prefix = start.clone();
        let (callback, future) = paired_future_callback();
        self.ch.send_store_msg(StoreMsg::GetRegionsInRange {
            start,
            end,
            callback,
        });
        let ch = self.ch.clone();
        let kv = self.storage.get_engine().kv_engine();
        let task = async move {
            let mut resp = UnsafeDestroyRangeResponse::default();
            let regions = future.await?;
            let mut region_futures = vec![];
            for region in &regions {
                if let Some(snap) = kv.get_snap_access(region.id()) {
                    if !snap.has_data_in_prefix(&prefix) {
                        continue;
                    }
                }
                let (cb, fu) = paired_future_callback();
                let callback = Callback::write(Box::new(move |_| {
                    cb(());
                }));
                region_futures.push(fu);
                ch.send_casual_msg(
                    region.id(),
                    CasualMessage::DeletePrefix {
                        region_version: region.ver(),
                        prefix: prefix.clone(),
                        callback,
                    },
                );
            }
            let _ = futures::future::join_all(region_futures).await;
            // Wait and check if all regions have applied delete prefix.
            let mut regions_applied = false;
            for i in 1..=5 {
                let mut applied = true;
                for region in &regions {
                    if let Some(snap) = kv.get_snap_access(region.id()) {
                        if snap.has_data_in_prefix(&prefix) {
                            applied = false;
                            break;
                        }
                    }
                }
                if !applied {
                    let deadline = std::time::Instant::now() + Duration::from_secs(i);
                    let _ = GLOBAL_TIMER_HANDLE.delay(deadline).compat().await.is_ok();
                    continue;
                }
                regions_applied = true;
                break;
            }
            if regions_applied {
                // At last, make sure all shards exists and not split.
                for region in &regions {
                    let res = kv.get_shard_with_ver(region.id(), region.ver());
                    if res.is_err() {
                        resp.set_error(format!(
                            "region {} changed during delete range",
                            region.id()
                        ));
                        break;
                    }
                }
            } else {
                resp.set_error("some region failed to delete range".to_string());
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .unsafe_destroy_range
                .observe(duration_to_sec(begin_instant.saturating_elapsed()));
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
            .copr
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
                        .observe(duration_to_sec(begin_instant.saturating_elapsed()));
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
        let store_id = self.store_id;
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |msg| {
                RAFT_MESSAGE_RECV_COUNTER.inc();
                let to_store_id = msg.get_to_peer().get_store_id();
                if to_store_id != store_id {
                    future::err(Error::from(RaftStoreError::StoreNotMatch {
                        to_store_id,
                        my_store_id: store_id,
                    }))
                } else {
                    ch.send_raft_msg(msg);
                    future::ready(Ok(()))
                }
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN),
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
        let store_id = self.store_id;
        let ch = self.ch.clone();
        ctx.spawn(async move {
            let res = stream.map_err(Error::from).try_for_each(move |mut msgs| {
                let len = msgs.get_msgs().len();
                RAFT_MESSAGE_RECV_COUNTER.inc_by(len as u64);
                RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);
                for msg in msgs.take_msgs().into_iter() {
                    let to_store_id = msg.get_to_peer().get_store_id();
                    if to_store_id != store_id {
                        return future::err(Error::from(RaftStoreError::StoreNotMatch {
                            to_store_id,
                            my_store_id: store_id,
                        }));
                    }
                    ch.send_raft_msg(msg);
                }
                future::ok(())
            });
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::batch_raft send response fail"; "err" => ?e))
                .await;
        });
    }

    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        forward_unary!(self.proxy, split_region, ctx, req, sink);
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

        self.ch.send_casual_msg(region_id, req);

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
                .observe(duration_to_sec(begin_instant.saturating_elapsed()));
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
        forward_unary!(self.proxy, read_index, ctx, req, sink);
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
        self.ch.send_command(cmd, Callback::Read(cb));

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
                .observe(begin_instant.saturating_elapsed_secs());
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
        forward_duplex!(self.proxy, batch_commands, ctx, stream, sink);
        let (tx, rx) = unbounded(GRPC_MSG_NOTIFY_SIZE);

        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let storage = self.storage.clone();
        let copr = self.copr.clone();
        let copr_v2 = self.copr_v2.clone();
        let pool_size = storage.get_normal_pool_size();
        let batch_builder = BatcherBuilder::new(self.enable_req_batch, pool_size);
        let request_handler = stream.try_for_each(move |mut req| {
            let request_ids = req.take_request_ids();
            let requests: Vec<_> = req.take_requests().into();
            let queue = storage.get_readpool_queue_per_worker();
            let mut batcher = batch_builder.build(queue, request_ids.len());
            GRPC_REQ_BATCH_COMMANDS_SIZE.observe(requests.len() as f64);
            for (id, req) in request_ids.into_iter().zip(requests) {
                handle_batch_commands_request(
                    &mut batcher,
                    &storage,
                    &copr,
                    &copr_v2,
                    &peer,
                    id,
                    req,
                    &tx,
                );
                if let Some(batch) = batcher.as_mut() {
                    batch.maybe_commit(&storage, &tx);
                }
            }
            if let Some(batch) = batcher {
                batch.commit(&storage, &tx);
            }
            future::ok(())
        });
        ctx.spawn(request_handler.unwrap_or_else(|e| error!("batch_commands error"; "err" => %e)));

        let grpc_thread_load = Arc::clone(&self.grpc_thread_load);
        let response_retriever = BatchReceiver::new(
            rx,
            GRPC_MSG_MAX_BATCH_SIZE,
            MeasuredBatchResponse::default,
            BatchRespCollector,
        );

        let mut response_retriever = response_retriever.map(move |item| {
            for measure in item.measures {
                let GrpcRequestDuration { label, begin } = measure;
                GRPC_MSG_HISTOGRAM_STATIC
                    .get(label)
                    .observe(begin.saturating_elapsed_secs());
            }

            let mut r = item.batch_resp;
            GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64);
            r.set_transport_layer_load(grpc_thread_load.total_load() as u64);
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

    fn batch_coprocessor(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: BatchRequest,
        _sink: ServerStreamingSink<BatchResponse>,
    ) {
        unimplemented!()
    }

    fn check_leader(
        &mut self,
        _ctx: RpcContext<'_>,
        _request: CheckLeaderRequest,
        _sink: UnarySink<CheckLeaderResponse>,
    ) {
        unimplemented!()
    }

    fn get_store_safe_ts(
        &mut self,
        ctx: RpcContext<'_>,
        _request: StoreSafeTsRequest,
        sink: UnarySink<StoreSafeTsResponse>,
    ) {
        // TODO(x) implement it.
        let task = async move {
            let response = StoreSafeTsResponse::default();
            sink.success(response).await?;
            ServerResult::Ok(())
        }
        .map_err(|e| {
            warn!("call get_store_safe_ts failed"; "err" => ?e);
        })
        .map(|_| ());
        ctx.spawn(task);
    }

    fn get_lock_wait_info(
        &mut self,
        ctx: RpcContext<'_>,
        _request: GetLockWaitInfoRequest,
        sink: UnarySink<GetLockWaitInfoResponse>,
    ) {
        let (cb, f) = paired_future_callback();
        self.storage.dump_wait_for_entries(cb);
        let task = async move {
            let res = f.await?;
            let mut response = GetLockWaitInfoResponse::default();
            response.set_entries(RepeatedField::from_vec(res));
            sink.success(response).await?;
            ServerResult::Ok(())
        }
        .map_err(|e| {
            warn!("call dump_wait_for_entries failed"; "err" => ?e);
        })
        .map(|_| ());
        ctx.spawn(task);
    }
}

fn response_batch_commands_request<F, T>(
    id: u64,
    resp: F,
    tx: Sender<MeasuredSingleResponse>,
    begin: Instant,
    label: GrpcTypeKind,
) where
    MemoryTraceGuard<batch_commands_response::Response>: From<T>,
    F: Future<Output = Result<T, ()>> + Send + 'static,
{
    let task = async move {
        if let Ok(resp) = resp.await {
            let measure = GrpcRequestDuration { begin, label };
            let task = MeasuredSingleResponse::new(id, resp, measure);
            if let Err(e) = tx.send_and_notify(task) {
                error!("KvService response batch commands fail"; "err" => ?e);
            }
        }
    };
    poll_future_notify(task);
}

fn handle_batch_commands_request<L: LockManager, F: KvFormat>(
    batcher: &mut Option<ReqBatcher>,
    storage: &Storage<RaftKv, L, F>,
    copr: &Endpoint<RaftKv>,
    _copr_v2: &coprocessor_v2::Endpoint,
    peer: &str,
    id: u64,
    req: batch_commands_request::Request,
    tx: &Sender<MeasuredSingleResponse>,
) {
    match req.cmd {
        None => {
            let begin_instant = Instant::now();
            let resp = future::ok(batch_commands_response::Response::default());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::invalid,
            );
        }
        Some(batch_commands_request::request::Cmd::Get(req)) => {
            if batcher
                .as_mut()
                .map_or(false, |req_batch| req_batch.can_batch_get(&req))
            {
                batcher.as_mut().unwrap().add_get_request(req, id);
            } else {
                let begin_instant = Instant::now();
                let resp = future_get(storage, req)
                    .map_ok(|resp| batch_commands_response::Response {
                        cmd: Some(batch_commands_response::response::Cmd::Get(resp)),
                        ..Default::default()
                    })
                    .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
                response_batch_commands_request(
                    id,
                    resp,
                    tx.clone(),
                    begin_instant,
                    GrpcTypeKind::kv_get,
                );
            }
        }
        Some(batch_commands_request::request::Cmd::Coprocessor(req)) => {
            let begin_instant = Instant::now();
            let resp = future_copr(copr, Some(peer.to_string()), req)
                .map_ok(|resp| {
                    resp.map(|resp| batch_commands_response::Response {
                        cmd: Some(batch_commands_response::response::Cmd::Coprocessor(resp)),
                        ..Default::default()
                    })
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.coprocessor.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::coprocessor,
            );
        }
        Some(batch_commands_request::request::Cmd::Scan(req)) => {
            let begin_instant = Instant::now();
            let resp = future_scan(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Scan(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_scan,
            );
        }
        Some(batch_commands_request::request::Cmd::Prewrite(req)) => {
            let begin_instant = Instant::now();
            let resp = future_prewrite(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Prewrite(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_prewrite.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_prewrite,
            );
        }
        Some(batch_commands_request::request::Cmd::Commit(req)) => {
            let begin_instant = Instant::now();
            let resp = future_commit(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Commit(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_commit.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_commit,
            );
        }
        Some(batch_commands_request::request::Cmd::Cleanup(req)) => {
            let begin_instant = Instant::now();
            let resp = future_cleanup(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Cleanup(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_cleanup.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_cleanup,
            );
        }
        Some(batch_commands_request::request::Cmd::BatchGet(req)) => {
            let begin_instant = Instant::now();
            let resp = future_batch_get(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::BatchGet(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_get.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_batch_get,
            );
        }
        Some(batch_commands_request::request::Cmd::BatchRollback(req)) => {
            let begin_instant = Instant::now();
            let resp = future_batch_rollback(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::BatchRollback(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_batch_rollback.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_batch_rollback,
            );
        }
        Some(batch_commands_request::request::Cmd::TxnHeartBeat(req)) => {
            let begin_instant = Instant::now();
            let resp = future_txn_heart_beat(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::TxnHeartBeat(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_txn_heart_beat.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_txn_heart_beat,
            );
        }
        Some(batch_commands_request::request::Cmd::CheckTxnStatus(req)) => {
            let begin_instant = Instant::now();
            let resp = future_check_txn_status(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::CheckTxnStatus(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_check_txn_status.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_check_txn_status,
            );
        }
        Some(batch_commands_request::request::Cmd::CheckSecondaryLocks(req)) => {
            let begin_instant = Instant::now();
            let resp = future_check_secondary_locks(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::CheckSecondaryLocks(
                        resp,
                    )),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_check_secondary_locks.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_check_secondary_locks,
            );
        }
        Some(batch_commands_request::request::Cmd::ScanLock(req)) => {
            let begin_instant = Instant::now();
            let resp = future_scan_lock(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::ScanLock(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_scan_lock.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_scan_lock,
            );
        }
        Some(batch_commands_request::request::Cmd::ResolveLock(req)) => {
            let begin_instant = Instant::now();
            let resp = future_resolve_lock(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::ResolveLock(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_resolve_lock.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_resolve_lock,
            );
        }
        Some(batch_commands_request::request::Cmd::Gc(req)) => {
            let begin_instant = Instant::now();
            let resp = future_gc(req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Gc(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_gc.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_gc,
            );
        }
        Some(batch_commands_request::request::Cmd::DeleteRange(req)) => {
            let begin_instant = Instant::now();
            let resp = future_delete_range(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::DeleteRange(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_delete_range.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_delete_range,
            );
        }
        Some(batch_commands_request::request::Cmd::PessimisticLock(req)) => {
            let begin_instant = Instant::now();
            let resp = future_acquire_pessimistic_lock(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::PessimisticLock(
                        resp,
                    )),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_pessimistic_lock.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_pessimistic_lock,
            );
        }
        Some(batch_commands_request::request::Cmd::PessimisticRollback(req)) => {
            let begin_instant = Instant::now();
            let resp = future_pessimistic_rollback(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::PessimisticRollback(
                        resp,
                    )),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_pessimistic_rollback.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_pessimistic_rollback,
            );
        }
        Some(batch_commands_request::request::Cmd::Empty(req)) => {
            let begin_instant = Instant::now();
            let resp = future_handle_empty(req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Empty(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.invalid.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::invalid,
            );
        }
        Some(batch_commands_request::request::Cmd::Import(_)) => panic!("not implemented"),
        _ => panic!("not implemented"),
    }
}

async fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> ServerResult<BatchCommandsEmptyResponse> {
    let mut res = BatchCommandsEmptyResponse::default();
    res.set_test_id(req.get_test_id());
    // `BatchCommandsWaker` processes futures in notify. If delay_time is too small, notify
    // can be called immediately, so the future is polled recursively and lead to deadlock.
    if req.get_delay_time() >= 10 {
        let _ = tikv_util::timer::GLOBAL_TIMER_HANDLE
            .delay(
                std::time::Instant::now() + std::time::Duration::from_millis(req.get_delay_time()),
            )
            .compat()
            .await;
    }
    Ok(res)
}

fn future_get<L: LockManager, F: KvFormat>(
    storage: &Storage<RaftKv, L, F>,
    mut req: GetRequest,
) -> impl Future<Output = ServerResult<GetResponse>> {
    let start = Instant::now();
    let v = storage.get(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_version().into(),
    );

    async move {
        let v = v.await;
        let duration_ms = duration_to_ms(start.saturating_elapsed());
        let mut resp = GetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((val, stats)) => {
                    let exec_detail_v2 = resp.mut_exec_details_v2();
                    let scan_detail_v2 = exec_detail_v2.mut_scan_detail_v2();
                    stats.stats.write_scan_detail(scan_detail_v2);
                    stats.perf_stats.write_scan_detail(scan_detail_v2);
                    let time_detail = exec_detail_v2.mut_time_detail();
                    time_detail.set_kv_read_wall_time_ms(duration_ms as i64);
                    time_detail.set_wait_wall_time_ms(stats.latency_stats.wait_wall_time_ms as i64);
                    time_detail
                        .set_process_wall_time_ms(stats.latency_stats.process_wall_time_ms as i64);
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

fn future_scan<L: LockManager, F: KvFormat>(
    storage: &Storage<RaftKv, L, F>,
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

fn future_batch_get<L: LockManager, F: KvFormat>(
    storage: &Storage<RaftKv, L, F>,
    mut req: BatchGetRequest,
) -> impl Future<Output = ServerResult<BatchGetResponse>> {
    let start = Instant::now();
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let v = storage.batch_get(req.take_context(), keys, req.get_version().into());

    async move {
        let v = v.await;
        let duration_ms = duration_to_ms(start.saturating_elapsed());
        let mut resp = BatchGetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((kv_res, stats)) => {
                    let pairs = map_kv_pairs(kv_res);
                    let exec_detail_v2 = resp.mut_exec_details_v2();
                    let scan_detail_v2 = exec_detail_v2.mut_scan_detail_v2();
                    stats.stats.write_scan_detail(scan_detail_v2);
                    stats.perf_stats.write_scan_detail(scan_detail_v2);
                    let time_detail = exec_detail_v2.mut_time_detail();
                    time_detail.set_kv_read_wall_time_ms(duration_ms as i64);
                    time_detail.set_wait_wall_time_ms(stats.latency_stats.wait_wall_time_ms as i64);
                    time_detail
                        .set_process_wall_time_ms(stats.latency_stats.process_wall_time_ms as i64);
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

fn future_scan_lock<L: LockManager, F: KvFormat>(
    storage: &Storage<RaftKv, L, F>,
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
    ))))
}

fn future_delete_range<L: LockManager, F: KvFormat>(
    storage: &Storage<RaftKv, L, F>,
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

fn future_copr<E: Engine>(
    copr: &Endpoint<E>,
    peer: Option<String>,
    req: Request,
) -> impl Future<Output = ServerResult<MemoryTraceGuard<Response>>> {
    let ret = copr.parse_and_handle_unary_request(req, peer);
    async move { Ok(ret.await) }
}

macro_rules! txn_command_future {
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) $prelude: stmt; ($v: ident, $resp: ident) { $else_branch: expr }) => {
        fn $fn_name<E: Engine, L: LockManager, F: KvFormat>(
            storage: &Storage<E, L, F>,
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

pub mod batch_commands_response {
    pub type Response = kvproto::tikvpb::BatchCommandsResponseResponse;

    pub mod response {
        pub type Cmd = kvproto::tikvpb::BatchCommandsResponse_Response_oneof_cmd;
    }
}

pub mod batch_commands_request {
    pub type Request = kvproto::tikvpb::BatchCommandsRequestRequest;

    pub mod request {
        pub type Cmd = kvproto::tikvpb::BatchCommandsRequest_Request_oneof_cmd;
    }
}

use protobuf::RepeatedField;
use rfstore::store::StoreMsg;
use tikv::server::service::{GrpcRequestDuration, MeasuredBatchResponse, MeasuredSingleResponse};
use tikv_alloc::MemoryTraceGuard;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use crate::RaftKv;

struct BatchRespCollector;
impl BatchCollector<MeasuredBatchResponse, MeasuredSingleResponse> for BatchRespCollector {
    fn collect(
        &mut self,
        v: &mut MeasuredBatchResponse,
        mut e: MeasuredSingleResponse,
    ) -> Option<MeasuredSingleResponse> {
        v.batch_resp.mut_request_ids().push(e.id);
        v.batch_resp.mut_responses().push(e.resp.consume());
        v.measures.push(e.measure);
        None
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures::{channel::oneshot, executor::block_on};

    use super::*;

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
