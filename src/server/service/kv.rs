// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]: TiKV gRPC APIs implementation
use std::{mem, sync::Arc, time::Duration};

use api_version::KvFormat;
use fail::fail_point;
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
use kvproto::{coprocessor::*, kvrpcpb::*, mpp::*, raft_serverpb::*, tikvpb::*};
use protobuf::RepeatedField;
use raft::eraftpb::MessageType;
use raftstore::{
    store::{
        memory::{MEMTRACE_APPLYS, MEMTRACE_RAFT_ENTRIES, MEMTRACE_RAFT_MESSAGES},
        metrics::{MESSAGE_RECV_BY_STORE, RAFT_ENTRIES_CACHES_GAUGE},
        CheckLeaderTask,
    },
    Error as RaftStoreError, Result as RaftStoreResult,
};
use resource_control::ResourceGroupManager;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_kv::{RaftExtension, StageLatencyStats};
use tikv_util::{
    future::{paired_future_callback, poll_future_notify},
    mpsc::future::{unbounded, BatchReceiver, Sender, WakePolicy},
    sys::memory_usage_reaches_high_water,
    time::Instant,
    worker::Scheduler,
};
use tracker::{set_tls_tracker_token, RequestInfo, RequestType, Tracker, GLOBAL_TRACKERS};
use txn_types::{self, Key};

use super::batch::{BatcherBuilder, ReqBatcher};
use crate::{
    coprocessor::Endpoint,
    coprocessor_v2, forward_duplex, forward_unary, log_net_error,
    server::{
        gc_worker::GcWorker, load_statistics::ThreadLoadPool, metrics::*, snap::Task as SnapTask,
        Error, MetadataSourceStoreId, Proxy, Result as ServerResult,
    },
    storage::{
        self,
        errors::{
            extract_committed, extract_key_error, extract_key_errors, extract_kv_pairs,
            extract_region_error, extract_region_error_from_error, map_kv_pairs,
        },
        kv::Engine,
        lock_manager::LockManager,
        SecondaryLocksStatus, Storage, TxnStatus,
    },
};

const GRPC_MSG_MAX_BATCH_SIZE: usize = 128;
const GRPC_MSG_NOTIFY_SIZE: usize = 8;

/// Service handles the RPC messages for the `Tikv` service.
pub struct Service<E: Engine, L: LockManager, F: KvFormat> {
    store_id: u64,
    /// Used to handle requests related to GC.
    // TODO: make it Some after GC is supported for v2.
    gc_worker: GcWorker<E>,
    // For handling KV requests.
    storage: Storage<E, L, F>,
    // For handling coprocessor requests.
    copr: Endpoint<E>,
    // For handling corprocessor v2 requests.
    copr_v2: coprocessor_v2::Endpoint,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,
    // For handling `CheckLeader` request.
    check_leader_scheduler: Scheduler<CheckLeaderTask>,

    enable_req_batch: bool,

    grpc_thread_load: Arc<ThreadLoadPool>,

    proxy: Proxy,

    // Go `server::Config` to get more details.
    reject_messages_on_memory_ratio: f64,

    resource_manager: Option<Arc<ResourceGroupManager>>,
}

impl<E: Engine, L: LockManager, F: KvFormat> Drop for Service<E, L, F> {
    fn drop(&mut self) {
        self.check_leader_scheduler.stop();
    }
}

impl<E: Engine + Clone, L: LockManager + Clone, F: KvFormat> Clone for Service<E, L, F> {
    fn clone(&self) -> Self {
        Service {
            store_id: self.store_id,
            gc_worker: self.gc_worker.clone(),
            storage: self.storage.clone(),
            copr: self.copr.clone(),
            copr_v2: self.copr_v2.clone(),
            snap_scheduler: self.snap_scheduler.clone(),
            check_leader_scheduler: self.check_leader_scheduler.clone(),
            enable_req_batch: self.enable_req_batch,
            grpc_thread_load: self.grpc_thread_load.clone(),
            proxy: self.proxy.clone(),
            reject_messages_on_memory_ratio: self.reject_messages_on_memory_ratio,
            resource_manager: self.resource_manager.clone(),
        }
    }
}

impl<E: Engine, L: LockManager, F: KvFormat> Service<E, L, F> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(
        store_id: u64,
        storage: Storage<E, L, F>,
        gc_worker: GcWorker<E>,
        copr: Endpoint<E>,
        copr_v2: coprocessor_v2::Endpoint,
        snap_scheduler: Scheduler<SnapTask>,
        check_leader_scheduler: Scheduler<CheckLeaderTask>,
        grpc_thread_load: Arc<ThreadLoadPool>,
        enable_req_batch: bool,
        proxy: Proxy,
        reject_messages_on_memory_ratio: f64,
        resource_manager: Option<Arc<ResourceGroupManager>>,
    ) -> Self {
        Service {
            store_id,
            gc_worker,
            storage,
            copr,
            copr_v2,
            snap_scheduler,
            check_leader_scheduler,
            enable_req_batch,
            grpc_thread_load,
            proxy,
            reject_messages_on_memory_ratio,
            resource_manager,
        }
    }

    fn handle_raft_message(
        store_id: u64,
        ch: &E::RaftExtension,
        msg: RaftMessage,
        reject: bool,
    ) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if to_store_id != store_id {
            return Err(RaftStoreError::StoreNotMatch {
                to_store_id,
                my_store_id: store_id,
            });
        }
        if reject && msg.get_message().get_msg_type() == MessageType::MsgAppend {
            RAFT_APPEND_REJECTS.inc();
            let id = msg.get_region_id();
            let peer_id = msg.get_message().get_from();
            ch.report_reject_message(id, peer_id);
            return Ok(());
        }

        fail_point!("receive_raft_message_from_outside");
        ch.feed(msg, false);
        Ok(())
    }

    fn get_store_id_from_metadata(ctx: &RpcContext<'_>) -> Option<u64> {
        let metadata = ctx.request_headers();
        for i in 0..metadata.len() {
            let (key, value) = metadata.get(i).unwrap();
            if key == MetadataSourceStoreId::KEY {
                let store_id = MetadataSourceStoreId::parse(value);
                return Some(store_id);
            }
        }
        None
    }
}

macro_rules! handle_request {
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident) => {
        handle_request!($fn_name, $future_name, $req_ty, $resp_ty, no_time_detail);
    };
    ($fn_name: ident, $future_name: ident, $req_ty: ident, $resp_ty: ident, $time_detail: tt) => {
        fn $fn_name(&mut self, ctx: RpcContext<'_>, req: $req_ty, sink: UnarySink<$resp_ty>) {
            forward_unary!(self.proxy, $fn_name, ctx, req, sink);
            let begin_instant = Instant::now();

            let source = req.get_context().get_request_source().to_owned();
            let resource_control_ctx = req.get_context().get_resource_control_context();
            if let Some(resource_manager) = &self.resource_manager {
                resource_manager.consume_penalty(resource_control_ctx);
            }
            GRPC_RESOURCE_GROUP_COUNTER_VEC
                    .with_label_values(&[resource_control_ctx.get_resource_group_name()])
                    .inc();
            let resp = $future_name(&self.storage, req);
            let task = async move {
                let resp = resp.await?;
                let elapsed = begin_instant.saturating_elapsed();
                set_total_time!(resp, elapsed, $time_detail);
                sink.success(resp).await?;
                GRPC_MSG_HISTOGRAM_STATIC
                    .$fn_name
                    .observe(elapsed.as_secs_f64());
                record_request_source_metrics(source, elapsed);
                ServerResult::Ok(())
            }
            .map_err(|e| {
                log_net_error!(e, "kv rpc failed";
                    "request" => stringify!($fn_name)
                );
                GRPC_MSG_FAIL_COUNTER.$fn_name.inc();
            })
            .map(|_|());

            ctx.spawn(task);
        }
    }
}

macro_rules! set_total_time {
    ($resp:ident, $duration:expr,no_time_detail) => {};
    ($resp:ident, $duration:expr,has_time_detail) => {
        let mut $resp = $resp;
        $resp
            .mut_exec_details_v2()
            .mut_time_detail()
            .set_total_rpc_wall_time_ns($duration.as_nanos() as u64);
        $resp
            .mut_exec_details_v2()
            .mut_time_detail_v2()
            .set_total_rpc_wall_time_ns($duration.as_nanos() as u64);
    };
}

impl<E: Engine, L: LockManager, F: KvFormat> Tikv for Service<E, L, F> {
    handle_request!(kv_get, future_get, GetRequest, GetResponse, has_time_detail);
    handle_request!(kv_scan, future_scan, ScanRequest, ScanResponse);
    handle_request!(
        kv_prewrite,
        future_prewrite,
        PrewriteRequest,
        PrewriteResponse,
        has_time_detail
    );
    handle_request!(
        kv_pessimistic_lock,
        future_acquire_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse,
        has_time_detail
    );
    handle_request!(
        kv_pessimistic_rollback,
        future_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse,
        has_time_detail
    );
    handle_request!(
        kv_commit,
        future_commit,
        CommitRequest,
        CommitResponse,
        has_time_detail
    );
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
        BatchRollbackResponse,
        has_time_detail
    );
    handle_request!(
        kv_txn_heart_beat,
        future_txn_heart_beat,
        TxnHeartBeatRequest,
        TxnHeartBeatResponse,
        has_time_detail
    );
    handle_request!(
        kv_check_txn_status,
        future_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse,
        has_time_detail
    );
    handle_request!(
        kv_check_secondary_locks,
        future_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse,
        has_time_detail
    );
    handle_request!(
        kv_scan_lock,
        future_scan_lock,
        ScanLockRequest,
        ScanLockResponse,
        has_time_detail
    );
    handle_request!(
        kv_resolve_lock,
        future_resolve_lock,
        ResolveLockRequest,
        ResolveLockResponse,
        has_time_detail
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
    handle_request!(
        raw_get_key_ttl,
        future_raw_get_key_ttl,
        RawGetKeyTtlRequest,
        RawGetKeyTtlResponse
    );

    handle_request!(
        raw_compare_and_swap,
        future_raw_compare_and_swap,
        RawCasRequest,
        RawCasResponse
    );

    handle_request!(
        raw_checksum,
        future_raw_checksum,
        RawChecksumRequest,
        RawChecksumResponse
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

    fn kv_prepare_flashback_to_version(
        &mut self,
        ctx: RpcContext<'_>,
        req: PrepareFlashbackToVersionRequest,
        sink: UnarySink<PrepareFlashbackToVersionResponse>,
    ) {
        let begin_instant = Instant::now();

        let source = req.get_context().get_request_source().to_owned();
        let resp = future_prepare_flashback_to_version(self.storage.clone(), req);
        let task = async move {
            let resp = resp.await?;
            let elapsed = begin_instant.saturating_elapsed();
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .kv_prepare_flashback_to_version
                .observe(elapsed.as_secs_f64());
            record_request_source_metrics(source, elapsed);
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => stringify!($fn_name)
            );
            GRPC_MSG_FAIL_COUNTER.kv_prepare_flashback_to_version.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn kv_flashback_to_version(
        &mut self,
        ctx: RpcContext<'_>,
        req: FlashbackToVersionRequest,
        sink: UnarySink<FlashbackToVersionResponse>,
    ) {
        let begin_instant = Instant::now();

        let source = req.get_context().get_request_source().to_owned();
        let resp = future_flashback_to_version(self.storage.clone(), req);
        let task = async move {
            let resp = resp.await?;
            let elapsed = begin_instant.saturating_elapsed();
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .kv_flashback_to_version
                .observe(elapsed.as_secs_f64());
            record_request_source_metrics(source, elapsed);
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => stringify!($fn_name)
            );
            GRPC_MSG_FAIL_COUNTER.kv_flashback_to_version.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn coprocessor(&mut self, ctx: RpcContext<'_>, req: Request, sink: UnarySink<Response>) {
        forward_unary!(self.proxy, coprocessor, ctx, req, sink);
        let source = req.get_context().get_request_source().to_owned();
        let resource_control_ctx = req.get_context().get_resource_control_context();
        if let Some(resource_manager) = &self.resource_manager {
            resource_manager.consume_penalty(resource_control_ctx);
        }
        GRPC_RESOURCE_GROUP_COUNTER_VEC
            .with_label_values(&[resource_control_ctx.get_resource_group_name()])
            .inc();

        let begin_instant = Instant::now();
        let future = future_copr(&self.copr, Some(ctx.peer()), req);
        let task = async move {
            let resp = future.await?.consume();
            sink.success(resp).await?;
            let elapsed = begin_instant.saturating_elapsed();
            GRPC_MSG_HISTOGRAM_STATIC
                .coprocessor
                .observe(elapsed.as_secs_f64());
            record_request_source_metrics(source, elapsed);
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => "coprocessor"
            );
            GRPC_MSG_FAIL_COUNTER.coprocessor.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn raw_coprocessor(
        &mut self,
        ctx: RpcContext<'_>,
        req: RawCoprocessorRequest,
        sink: UnarySink<RawCoprocessorResponse>,
    ) {
        let source = req.get_context().get_request_source().to_owned();
        let resource_control_ctx = req.get_context().get_resource_control_context();
        if let Some(resource_manager) = &self.resource_manager {
            resource_manager.consume_penalty(resource_control_ctx);
        }
        GRPC_RESOURCE_GROUP_COUNTER_VEC
            .with_label_values(&[resource_control_ctx.get_resource_group_name()])
            .inc();

        let begin_instant = Instant::now();
        let future = future_raw_coprocessor(&self.copr_v2, &self.storage, req);
        let task = async move {
            let resp = future.await?;
            sink.success(resp).await?;
            let elapsed = begin_instant.saturating_elapsed();
            GRPC_MSG_HISTOGRAM_STATIC
                .raw_coprocessor
                .observe(elapsed.as_secs_f64());
            record_request_source_metrics(source, elapsed);
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => "coprocessor_v2"
            );
            GRPC_MSG_FAIL_COUNTER.raw_coprocessor.inc();
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
        let begin_instant = Instant::now();

        // DestroyRange is a very dangerous operation. We don't allow passing MIN_KEY as
        // start, or MAX_KEY as end here.
        assert!(!req.get_start_key().is_empty());
        assert!(!req.get_end_key().is_empty());

        let source = req.get_context().get_request_source().to_owned();
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
            let elapsed = begin_instant.saturating_elapsed();
            GRPC_MSG_HISTOGRAM_STATIC
                .unsafe_destroy_range
                .observe(elapsed.as_secs_f64());
            record_request_source_metrics(source, elapsed);
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => "unsafe_destroy_range"
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
        let begin_instant = Instant::now();
        let resource_control_ctx = req.get_context().get_resource_control_context();
        if let Some(resource_manager) = &self.resource_manager {
            resource_manager.consume_penalty(resource_control_ctx);
        }
        GRPC_RESOURCE_GROUP_COUNTER_VEC
            .with_label_values(&[resource_control_ctx.get_resource_group_name()])
            .inc();

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
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                    let _ = sink.close().await;
                }
                Err(e) => {
                    info!("kv rpc failed";
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
        let source_store_id = Self::get_store_id_from_metadata(&ctx);
        let message_received =
            source_store_id.map(|x| MESSAGE_RECV_BY_STORE.with_label_values(&[&format!("{}", x)]));
        info!(
            "raft RPC is called, new gRPC stream established";
            "source_store_id" => ?source_store_id,
        );

        let store_id = self.store_id;
        let ch = self.storage.get_engine().raft_extension();
        let reject_messages_on_memory_ratio = self.reject_messages_on_memory_ratio;

        let res = async move {
            let mut stream = stream.map_err(Error::from);
            while let Some(msg) = stream.try_next().await? {
                RAFT_MESSAGE_RECV_COUNTER.inc();
                let reject = needs_reject_raft_append(reject_messages_on_memory_ratio);
                if let Err(err @ RaftStoreError::StoreNotMatch { .. }) =
                    Self::handle_raft_message(store_id, &ch, msg, reject)
                {
                    // Return an error here will break the connection, only do that for
                    // `StoreNotMatch` to let tikv to resolve a correct address from PD
                    return Err(Error::from(err));
                }
                if let Some(ref counter) = message_received {
                    counter.inc();
                }
            }
            Ok::<(), Error>(())
        };

        ctx.spawn(async move {
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
        let source_store_id = Self::get_store_id_from_metadata(&ctx);
        let message_received =
            source_store_id.map(|x| MESSAGE_RECV_BY_STORE.with_label_values(&[&format!("{}", x)]));
        info!(
            "batch_raft RPC is called, new gRPC stream established";
            "source_store_id" => ?source_store_id,
        );

        let store_id = self.store_id;
        let ch = self.storage.get_engine().raft_extension();
        let reject_messages_on_memory_ratio = self.reject_messages_on_memory_ratio;

        let res = async move {
            let mut stream = stream.map_err(Error::from);
            while let Some(mut batch_msg) = stream.try_next().await? {
                let len = batch_msg.get_msgs().len();
                RAFT_MESSAGE_RECV_COUNTER.inc_by(len as u64);
                RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);
                let reject = needs_reject_raft_append(reject_messages_on_memory_ratio);
                for msg in batch_msg.take_msgs().into_iter() {
                    if let Err(err @ RaftStoreError::StoreNotMatch { .. }) =
                        Self::handle_raft_message(store_id, &ch, msg, reject)
                    {
                        // Return an error here will break the connection, only do that for
                        // `StoreNotMatch` to let tikv to resolve a correct address from PD
                        return Err(Error::from(err));
                    }
                }
                if let Some(ref counter) = message_received {
                    counter.inc_by(len as u64);
                }
            }
            Ok::<(), Error>(())
        };

        ctx.spawn(async move {
            let status = match res.await {
                Err(e) => {
                    fail_point!("on_batch_raft_stream_drop_by_err");
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
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, err_msg);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    fn tablet_snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<TabletSnapshotRequest>,
        sink: DuplexSink<TabletSnapshotResponse>,
    ) {
        let task = SnapTask::RecvTablet { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, err_msg);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    #[allow(clippy::collapsible_else_if)]
    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        forward_unary!(self.proxy, split_region, ctx, req, sink);
        let begin_instant = Instant::now();

        let region_id = req.get_context().get_region_id();
        let mut split_keys = if req.is_raw_kv {
            if !req.get_split_key().is_empty() {
                vec![F::encode_raw_key_owned(req.take_split_key(), None).into_encoded()]
            } else {
                req.take_split_keys()
                    .into_iter()
                    .map(|x| F::encode_raw_key_owned(x, None).into_encoded())
                    .collect()
            }
        } else {
            if !req.get_split_key().is_empty() {
                vec![Key::from_raw(req.get_split_key()).into_encoded()]
            } else {
                req.take_split_keys()
                    .into_iter()
                    .map(|x| Key::from_raw(&x).into_encoded())
                    .collect()
            }
        };
        split_keys.sort();
        let engine = self.storage.get_engine();
        let f = engine.raft_extension().split(
            region_id,
            req.take_context().take_region_epoch(),
            split_keys,
            ctx.peer(),
        );

        let task = async move {
            let res = f.await;
            let mut resp = SplitRegionResponse::default();
            match res {
                Ok(regions) => {
                    if regions.len() < 2 {
                        error!(
                            "invalid split response";
                            "region_id" => region_id,
                            "resp" => ?regions
                        );
                        resp.mut_region_error().set_message(format!(
                            "Internal Error: invalid response: {:?}",
                            regions
                        ));
                    } else {
                        if regions.len() == 2 {
                            resp.set_left(regions[0].clone());
                            resp.set_right(regions[1].clone());
                        }
                        resp.set_regions(regions.into());
                    }
                }
                Err(e) => {
                    let err: crate::storage::Result<()> = Err(e.into());
                    if let Some(err) = extract_region_error(&err) {
                        resp.set_region_error(err)
                    } else {
                        resp.mut_region_error()
                            .set_message(format!("failed to split: {:?}", err));
                    }
                }
            }
            sink.success(resp).await?;
            GRPC_MSG_HISTOGRAM_STATIC
                .split_region
                .observe(begin_instant.saturating_elapsed().as_secs_f64());
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => "split_region"
            );
            GRPC_MSG_FAIL_COUNTER.split_region.inc();
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

        let (tx, rx) = unbounded(WakePolicy::TillReach(GRPC_MSG_NOTIFY_SIZE));
        let ctx = Arc::new(ctx);
        let peer = ctx.peer();
        let storage = self.storage.clone();
        let copr = self.copr.clone();
        let copr_v2 = self.copr_v2.clone();
        let pool_size = storage.get_normal_pool_size();
        let batch_builder = BatcherBuilder::new(self.enable_req_batch, pool_size);
        let resource_manager = self.resource_manager.clone();
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
                    &resource_manager,
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
            collect_batch_resp,
        );

        let mut response_retriever = response_retriever.map(move |mut item| {
            handle_measures_for_batch_commands(&mut item);
            let mut r = item.batch_resp;
            GRPC_RESP_BATCH_COMMANDS_SIZE.observe(r.request_ids.len() as f64);
            // TODO: per thread load is more reasonable for batching.
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
            info!("kv rpc failed";
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

    fn check_leader(
        &mut self,
        ctx: RpcContext<'_>,
        mut request: CheckLeaderRequest,
        sink: UnarySink<CheckLeaderResponse>,
    ) {
        let begin_instant = Instant::now();
        let addr = ctx.peer();
        let ts = request.get_ts();
        let leaders = request.take_regions().into();
        let (cb, resp) = paired_future_callback();
        let check_leader_scheduler = self.check_leader_scheduler.clone();
        let task = async move {
            check_leader_scheduler
                .schedule(CheckLeaderTask::CheckLeader { leaders, cb })
                .map_err(|e| Error::Other(format!("{}", e).into()))?;
            let regions = resp.await?;
            let mut resp = CheckLeaderResponse::default();
            resp.set_ts(ts);
            resp.set_regions(regions);
            if let Err(e) = sink.success(resp).await {
                // CheckLeader has a built-in fast-success mechanism, so `RemoteStopped`
                // can be treated as a general situation.
                if let GrpcError::RemoteStopped = e {
                    return ServerResult::Ok(());
                }
                return Err(Error::from(e));
            }
            let elapsed = begin_instant.saturating_elapsed();
            GRPC_MSG_HISTOGRAM_STATIC
                .check_leader
                .observe(elapsed.as_secs_f64());
            ServerResult::Ok(())
        }
        .map_err(move |e| {
            // CheckLeader only needs quorum responses, remote may drops
            // requests early.
            info!("call CheckLeader failed"; "err" => ?e, "address" => addr);
        })
        .map(|_| ());

        ctx.spawn(task);
    }

    fn get_store_safe_ts(
        &mut self,
        ctx: RpcContext<'_>,
        mut request: StoreSafeTsRequest,
        sink: UnarySink<StoreSafeTsResponse>,
    ) {
        let key_range = request.take_key_range();
        let (cb, resp) = paired_future_callback();
        let check_leader_scheduler = self.check_leader_scheduler.clone();
        let task = async move {
            check_leader_scheduler
                .schedule(CheckLeaderTask::GetStoreTs { key_range, cb })
                .map_err(|e| Error::Other(format!("{}", e).into()))?;
            let store_safe_ts = resp.await?;
            let mut resp = StoreSafeTsResponse::default();
            resp.set_safe_ts(store_safe_ts);
            sink.success(resp).await?;
            ServerResult::Ok(())
        }
        .map_err(|e| {
            warn!("call GetStoreSafeTS failed"; "err" => ?e);
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
    source: String,
) where
    MemoryTraceGuard<batch_commands_response::Response>: From<T>,
    F: Future<Output = Result<T, ()>> + Send + 'static,
{
    let task = async move {
        if let Ok(resp) = resp.await {
            let measure = GrpcRequestDuration {
                begin,
                label,
                source,
            };
            let task = MeasuredSingleResponse::new(id, resp, measure);
            if let Err(e) = tx.send_with(task, WakePolicy::Immediately) {
                error!("KvService response batch commands fail"; "err" => ?e);
            }
        }
    };
    poll_future_notify(task);
}

fn handle_batch_commands_request<E: Engine, L: LockManager, F: KvFormat>(
    batcher: &mut Option<ReqBatcher>,
    storage: &Storage<E, L, F>,
    copr: &Endpoint<E>,
    copr_v2: &coprocessor_v2::Endpoint,
    peer: &str,
    id: u64,
    req: batch_commands_request::Request,
    tx: &Sender<MeasuredSingleResponse>,
    resource_manager: &Option<Arc<ResourceGroupManager>>,
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
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::invalid, String::default());
                },
                Some(batch_commands_request::request::Cmd::Get(req)) => {
                    let resource_control_ctx = req.get_context().get_resource_control_context();
                    if let Some(resource_manager) = resource_manager {
                        resource_manager.consume_penalty(resource_control_ctx);
                    }
                    GRPC_RESOURCE_GROUP_COUNTER_VEC
                        .with_label_values(&[resource_control_ctx.get_resource_group_name()])
                        .inc();
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.can_batch_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let source = req.get_context().get_request_source().to_owned();
                       let resp = future_get(storage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::Get))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::kv_get, source);
                    }
                },
                Some(batch_commands_request::request::Cmd::RawGet(req)) => {
                    let resource_control_ctx = req.get_context().get_resource_control_context();
                    if let Some(resource_manager) = resource_manager {
                        resource_manager.consume_penalty(resource_control_ctx);
                    }
                    GRPC_RESOURCE_GROUP_COUNTER_VEC
                    .with_label_values(&[resource_control_ctx.get_resource_group_name()])
                    .inc();
                    if batcher.as_mut().map_or(false, |req_batch| {
                        req_batch.can_batch_raw_get(&req)
                    }) {
                        batcher.as_mut().unwrap().add_raw_get_request(req, id);
                    } else {
                       let begin_instant = Instant::now();
                       let source = req.get_context().get_request_source().to_owned();
                       let resp = future_raw_get(storage, req)
                            .map_ok(oneof!(batch_commands_response::response::Cmd::RawGet))
                            .map_err(|_| GRPC_MSG_FAIL_COUNTER.raw_get.inc());
                        response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::raw_get, source);
                    }
                },
                Some(batch_commands_request::request::Cmd::Coprocessor(mut req)) => {
                    let resource_control_ctx = req.get_context().get_resource_control_context();
                    if let Some(resource_manager) = resource_manager {
                        resource_manager.consume_penalty(resource_control_ctx);
                    }
                    GRPC_RESOURCE_GROUP_COUNTER_VEC
                        .with_label_values(&[resource_control_ctx.get_resource_group_name()])
                        .inc();
                    let begin_instant = Instant::now();
                    let source = req.mut_context().take_request_source();
                    let resp = future_copr(copr, Some(peer.to_string()), req)
                        .map_ok(|resp| {
                            resp.map(oneof!(batch_commands_response::response::Cmd::Coprocessor))
                        })
                        .map_err(|_| GRPC_MSG_FAIL_COUNTER.coprocessor.inc());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::coprocessor, source);
                },
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
                        String::default(),
                    );
                }
                $(Some(batch_commands_request::request::Cmd::$cmd(mut req)) => {
                    let resource_control_ctx = req.get_context().get_resource_control_context();
                    if let Some(resource_manager) = resource_manager {
                        resource_manager.consume_penalty(resource_control_ctx);
                    }
                    GRPC_RESOURCE_GROUP_COUNTER_VEC
                        .with_label_values(&[resource_control_ctx.get_resource_group_name()])
                        .inc();
                    let begin_instant = Instant::now();
                    let source = req.mut_context().take_request_source();
                    let resp = $future_fn($($arg,)* req)
                        .map_ok(oneof!(batch_commands_response::response::Cmd::$cmd))
                        .map_err(|_| GRPC_MSG_FAIL_COUNTER.$metric_name.inc());
                    response_batch_commands_request(id, resp, tx.clone(), begin_instant, GrpcTypeKind::$metric_name, source);
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
        PrepareFlashbackToVersion, future_prepare_flashback_to_version(storage.clone()), kv_prepare_flashback_to_version;
        FlashbackToVersion, future_flashback_to_version(storage.clone()), kv_flashback_to_version;
        RawBatchGet, future_raw_batch_get(storage), raw_batch_get;
        RawPut, future_raw_put(storage), raw_put;
        RawBatchPut, future_raw_batch_put(storage), raw_batch_put;
        RawDelete, future_raw_delete(storage), raw_delete;
        RawBatchDelete, future_raw_batch_delete(storage), raw_batch_delete;
        RawScan, future_raw_scan(storage), raw_scan;
        RawDeleteRange, future_raw_delete_range(storage), raw_delete_range;
        RawBatchScan, future_raw_batch_scan(storage), raw_batch_scan;
        RawCoprocessor, future_raw_coprocessor(copr_v2, storage), coprocessor;
        PessimisticLock, future_acquire_pessimistic_lock(storage), kv_pessimistic_lock;
        PessimisticRollback, future_pessimistic_rollback(storage), kv_pessimistic_rollback;
    }
}

fn handle_measures_for_batch_commands(measures: &mut MeasuredBatchResponse) {
    use BatchCommandsResponse_Response_oneof_cmd::*;
    let now = Instant::now();
    for (resp, measure) in measures
        .batch_resp
        .mut_responses()
        .iter_mut()
        .zip(mem::take(&mut measures.measures))
    {
        let GrpcRequestDuration {
            label,
            begin,
            source,
        } = measure;
        let elapsed = now.saturating_duration_since(begin);
        GRPC_MSG_HISTOGRAM_STATIC
            .get(label)
            .observe(elapsed.as_secs_f64());
        record_request_source_metrics(source, elapsed);
        let exec_details = resp.cmd.as_mut().and_then(|cmd| match cmd {
            Get(resp) => Some(resp.mut_exec_details_v2()),
            Prewrite(resp) => Some(resp.mut_exec_details_v2()),
            Commit(resp) => Some(resp.mut_exec_details_v2()),
            BatchGet(resp) => Some(resp.mut_exec_details_v2()),
            ResolveLock(resp) => Some(resp.mut_exec_details_v2()),
            Coprocessor(resp) => Some(resp.mut_exec_details_v2()),
            PessimisticLock(resp) => Some(resp.mut_exec_details_v2()),
            CheckTxnStatus(resp) => Some(resp.mut_exec_details_v2()),
            TxnHeartBeat(resp) => Some(resp.mut_exec_details_v2()),
            CheckSecondaryLocks(resp) => Some(resp.mut_exec_details_v2()),
            _ => None,
        });
        if let Some(exec_details) = exec_details {
            exec_details
                .mut_time_detail()
                .set_total_rpc_wall_time_ns(elapsed.as_nanos() as u64);
            exec_details
                .mut_time_detail_v2()
                .set_total_rpc_wall_time_ns(elapsed.as_nanos() as u64);
        }
    }
}

async fn future_handle_empty(
    req: BatchCommandsEmptyRequest,
) -> ServerResult<BatchCommandsEmptyResponse> {
    let mut res = BatchCommandsEmptyResponse::default();
    res.set_test_id(req.get_test_id());
    // `BatchCommandsWaker` processes futures in notify. If delay_time is too small,
    // notify can be called immediately, so the future is polled recursively and
    // lead to deadlock.
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

fn future_get<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: GetRequest,
) -> impl Future<Output = ServerResult<GetResponse>> {
    let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
        req.get_context(),
        RequestType::KvGet,
        req.get_version(),
    )));
    set_tls_tracker_token(tracker);
    let start = Instant::now();
    let v = storage.get(
        req.take_context(),
        Key::from_raw(req.get_key()),
        req.get_version().into(),
    );

    async move {
        let v = v.await;
        let duration = start.saturating_elapsed();
        let mut resp = GetResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((val, stats)) => {
                    let exec_detail_v2 = resp.mut_exec_details_v2();
                    let scan_detail_v2 = exec_detail_v2.mut_scan_detail_v2();
                    stats.stats.write_scan_detail(scan_detail_v2);
                    GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                        tracker.write_scan_detail(scan_detail_v2);
                    });
                    set_time_detail(exec_detail_v2, duration, &stats.latency_stats);
                    match val {
                        Some(val) => resp.set_value(val),
                        None => resp.set_not_found(true),
                    }
                }
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }
        GLOBAL_TRACKERS.remove(tracker);
        Ok(resp)
    }
}

fn set_time_detail(
    exec_detail_v2: &mut ExecDetailsV2,
    total_dur: Duration,
    stats: &StageLatencyStats,
) {
    let duration_ns = total_dur.as_nanos() as u64;
    // deprecated. we will remove the `time_detail` field in future version.
    {
        let time_detail = exec_detail_v2.mut_time_detail();
        time_detail.set_kv_read_wall_time_ms(duration_ns / 1_000_000);
        time_detail.set_wait_wall_time_ms(stats.wait_wall_time_ns / 1_000_000);
        time_detail.set_process_wall_time_ms(stats.process_wall_time_ns / 1_000_000);
    }

    let time_detail_v2 = exec_detail_v2.mut_time_detail_v2();
    time_detail_v2.set_kv_read_wall_time_ns(duration_ns);
    time_detail_v2.set_wait_wall_time_ns(stats.wait_wall_time_ns);
    time_detail_v2.set_process_wall_time_ns(stats.process_wall_time_ns);
    // currently, the schedule suspend_wall_time is always 0 for get and
    // batch_get. TODO: once we support aync-io, we may also count the
    // schedule suspend duration here.
}

fn future_scan<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: ScanRequest,
) -> impl Future<Output = ServerResult<ScanResponse>> {
    let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
        req.get_context(),
        RequestType::KvScan,
        req.get_version(),
    )));
    set_tls_tracker_token(tracker);
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
        GLOBAL_TRACKERS.remove(tracker);
        Ok(resp)
    }
}

fn future_batch_get<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: BatchGetRequest,
) -> impl Future<Output = ServerResult<BatchGetResponse>> {
    let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
        req.get_context(),
        RequestType::KvBatchGet,
        req.get_version(),
    )));
    set_tls_tracker_token(tracker);
    let start = Instant::now();
    let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
    let v = storage.batch_get(req.take_context(), keys, req.get_version().into());

    async move {
        let v = v.await;
        let duration = start.saturating_elapsed();
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
                    GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                        tracker.write_scan_detail(scan_detail_v2);
                    });
                    set_time_detail(exec_detail_v2, duration, &stats.latency_stats);
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
        GLOBAL_TRACKERS.remove(tracker);
        Ok(resp)
    }
}

fn future_scan_lock<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: ScanLockRequest,
) -> impl Future<Output = ServerResult<ScanLockResponse>> {
    let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
        req.get_context(),
        RequestType::KvScanLock,
        req.get_max_version(),
    )));
    set_tls_tracker_token(tracker);
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
        GLOBAL_TRACKERS.remove(tracker);
        Ok(resp)
    }
}

async fn future_gc(_: GcRequest) -> ServerResult<GcResponse> {
    Err(Error::Grpc(GrpcError::RpcFailure(RpcStatus::new(
        RpcStatusCode::UNIMPLEMENTED,
    ))))
}

fn future_delete_range<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

// Preparing the flashback for a region will "lock" the region so that
// there is no any read, write or scheduling operation could be proposed before
// the actual flashback operation.
// NOTICE: the caller needs to make sure the version we want to flashback won't
// be between any transactions that have not been fully committed.
pub async fn future_prepare_flashback_to_version<E: Engine, L: LockManager, F: KvFormat>(
    // Keep this param to hint the type of E for the compiler.
    storage: Storage<E, L, F>,
    req: PrepareFlashbackToVersionRequest,
) -> ServerResult<PrepareFlashbackToVersionResponse> {
    let f = storage
        .get_engine()
        .start_flashback(req.get_context(), req.get_start_ts());
    let mut res = f.await.map_err(storage::Error::from);
    if matches!(res, Ok(())) {
        // After the region is put into the flashback state, we need to do a special
        // prewrite to prevent `resolved_ts` from advancing.
        let (cb, f) = paired_future_callback();
        res = storage.sched_txn_command(req.clone().into(), cb);
        if matches!(res, Ok(())) {
            res = f.await.unwrap_or_else(|e| Err(box_err!(e)));
        }
    }
    let mut resp = PrepareFlashbackToVersionResponse::default();
    if let Some(e) = extract_region_error(&res) {
        resp.set_region_error(e);
    } else if let Err(e) = res {
        resp.set_error(format!("{}", e));
    }
    Ok(resp)
}

// Flashback the region to a specific point with the given `version`, please
// make sure the region is "locked" by `PrepareFlashbackToVersion` first,
// otherwise this request will fail.
pub async fn future_flashback_to_version<E: Engine, L: LockManager, F: KvFormat>(
    storage: Storage<E, L, F>,
    req: FlashbackToVersionRequest,
) -> ServerResult<FlashbackToVersionResponse> {
    // Perform the data flashback transaction command. We will check if the region
    // is in the flashback state when proposing the flashback modification.
    let (cb, f) = paired_future_callback();
    let mut res = storage.sched_txn_command(req.clone().into(), cb);
    if matches!(res, Ok(())) {
        res = f.await.unwrap_or_else(|e| Err(box_err!(e)));
    }
    if matches!(res, Ok(())) {
        // Only finish when flashback executed successfully.
        let f = storage.get_engine().end_flashback(req.get_context());
        res = f.await.map_err(storage::Error::from);
    }
    let mut resp = FlashbackToVersionResponse::default();
    if let Some(err) = extract_region_error(&res) {
        resp.set_region_error(err);
    } else if let Err(e) = res {
        resp.set_error(format!("{}", e));
    }
    Ok(resp)
}

fn future_raw_get<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

fn future_raw_batch_get<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

fn future_raw_put<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawPutRequest,
) -> impl Future<Output = ServerResult<RawPutResponse>> {
    let (cb, f) = paired_future_callback();
    let for_atomic = req.get_for_cas();
    let res = if for_atomic {
        storage.raw_batch_put_atomic(
            req.take_context(),
            req.take_cf(),
            vec![(req.take_key(), req.take_value())],
            vec![req.get_ttl()],
            cb,
        )
    } else {
        storage.raw_put(
            req.take_context(),
            req.take_cf(),
            req.take_key(),
            req.take_value(),
            req.get_ttl(),
            cb,
        )
    };

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

fn future_raw_batch_put<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawBatchPutRequest,
) -> impl Future<Output = ServerResult<RawBatchPutResponse>> {
    let cf = req.take_cf();
    let pairs_len = req.get_pairs().len();
    // The TTL for each key in seconds.
    //
    // In some TiKV of old versions, only one TTL can be provided and the TTL will
    // be applied to all keys in the request. For compatibility reasons, if the
    // length of `ttls` is exactly one, then the TTL will be applied to all keys.
    // Otherwise, the length mismatch between `ttls` and `pairs` will return an
    // error.
    let ttls = if req.get_ttls().is_empty() {
        vec![0; pairs_len]
    } else if req.get_ttls().len() == 1 {
        vec![req.get_ttls()[0]; pairs_len]
    } else {
        req.take_ttls()
    };
    let pairs = req
        .take_pairs()
        .into_iter()
        .map(|mut x| (x.take_key(), x.take_value()))
        .collect();

    let (cb, f) = paired_future_callback();
    let for_atomic = req.get_for_cas();
    let res = if for_atomic {
        storage.raw_batch_put_atomic(req.take_context(), cf, pairs, ttls, cb)
    } else {
        storage.raw_batch_put(req.take_context(), cf, pairs, ttls, cb)
    };

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

fn future_raw_delete<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawDeleteRequest,
) -> impl Future<Output = ServerResult<RawDeleteResponse>> {
    let (cb, f) = paired_future_callback();
    let for_atomic = req.get_for_cas();
    let res = if for_atomic {
        storage.raw_batch_delete_atomic(req.take_context(), req.take_cf(), vec![req.take_key()], cb)
    } else {
        storage.raw_delete(req.take_context(), req.take_cf(), req.take_key(), cb)
    };

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

fn future_raw_batch_delete<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawBatchDeleteRequest,
) -> impl Future<Output = ServerResult<RawBatchDeleteResponse>> {
    let cf = req.take_cf();
    let keys = req.take_keys().into();
    let (cb, f) = paired_future_callback();
    let for_atomic = req.get_for_cas();
    let res = if for_atomic {
        storage.raw_batch_delete_atomic(req.take_context(), cf, keys, cb)
    } else {
        storage.raw_batch_delete(req.take_context(), cf, keys, cb)
    };

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

fn future_raw_scan<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

fn future_raw_batch_scan<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

fn future_raw_delete_range<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
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

fn future_raw_get_key_ttl<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawGetKeyTtlRequest,
) -> impl Future<Output = ServerResult<RawGetKeyTtlResponse>> {
    let v = storage.raw_get_key_ttl(req.take_context(), req.take_cf(), req.take_key());

    async move {
        let v = v.await;
        let mut resp = RawGetKeyTtlResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok(Some(ttl)) => resp.set_ttl(ttl),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        Ok(resp)
    }
}

fn future_raw_compare_and_swap<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawCasRequest,
) -> impl Future<Output = ServerResult<RawCasResponse>> {
    let (cb, f) = paired_future_callback();
    let previous_value = if req.get_previous_not_exist() {
        None
    } else {
        Some(req.take_previous_value())
    };
    let res = storage.raw_compare_and_swap_atomic(
        req.take_context(),
        req.take_cf(),
        req.take_key(),
        previous_value,
        req.take_value(),
        req.get_ttl(),
        cb,
    );
    async move {
        let v = match res {
            Ok(()) => f.await?,
            Err(e) => Err(e),
        };
        let mut resp = RawCasResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((val, succeed)) => {
                    if let Some(val) = val {
                        resp.set_previous_value(val);
                    } else {
                        resp.set_previous_not_exist(true);
                    }
                    resp.set_succeed(succeed);
                }
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        Ok(resp)
    }
}

fn future_raw_checksum<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    mut req: RawChecksumRequest,
) -> impl Future<Output = ServerResult<RawChecksumResponse>> {
    let f = storage.raw_checksum(
        req.take_context(),
        req.get_algorithm(),
        req.take_ranges().into(),
    );
    async move {
        let v = f.await;
        let mut resp = RawChecksumResponse::default();
        if let Some(err) = extract_region_error(&v) {
            resp.set_region_error(err);
        } else {
            match v {
                Ok((checksum, kvs, bytes)) => {
                    resp.set_checksum(checksum);
                    resp.set_total_kvs(kvs);
                    resp.set_total_bytes(bytes);
                }
                Err(e) => resp.set_error(format!("{}", e)),
            }
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

fn future_raw_coprocessor<E: Engine, L: LockManager, F: KvFormat>(
    copr_v2: &coprocessor_v2::Endpoint,
    storage: &Storage<E, L, F>,
    req: RawCoprocessorRequest,
) -> impl Future<Output = ServerResult<RawCoprocessorResponse>> {
    let ret = copr_v2.handle_request(storage, req);
    async move { Ok(ret.await) }
}

macro_rules! txn_command_future {
    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) {$($prelude: stmt)*}; ($v: ident, $resp: ident, $tracker: ident) $else_branch: block) => {
        txn_command_future!(inner $fn_name, $req_ty, $resp_ty, ($req) {$($prelude)*}; ($v, $resp, $tracker) {
            $else_branch
            GLOBAL_TRACKERS.with_tracker($tracker, |tracker| {
                tracker.write_scan_detail($resp.mut_exec_details_v2().mut_scan_detail_v2());
                tracker.write_write_detail($resp.mut_exec_details_v2().mut_write_detail());
            });
        });
    };

    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($v: ident, $resp: ident, $tracker: ident) $else_branch: block ) => {
        txn_command_future!(inner $fn_name, $req_ty, $resp_ty, (req) {}; ($v, $resp, $tracker) {
            $else_branch
            GLOBAL_TRACKERS.with_tracker($tracker, |tracker| {
                tracker.write_scan_detail($resp.mut_exec_details_v2().mut_scan_detail_v2());
                tracker.write_write_detail($resp.mut_exec_details_v2().mut_write_detail());
            });
        });
    };

    ($fn_name: ident, $req_ty: ident, $resp_ty: ident, ($v: ident, $resp: ident) $else_branch: block ) => {
        txn_command_future!(inner $fn_name, $req_ty, $resp_ty, (req) {}; ($v, $resp, tracker) { $else_branch });
    };

    (inner $fn_name: ident, $req_ty: ident, $resp_ty: ident, ($req: ident) {$($prelude: stmt)*}; ($v: ident, $resp: ident, $tracker: ident) $else_branch: block) => {
        fn $fn_name<E: Engine, L: LockManager, F: KvFormat>(
            storage: &Storage<E, L, F>,
            $req: $req_ty,
        ) -> impl Future<Output = ServerResult<$resp_ty>> {
            $($prelude)*
            let $tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
                $req.get_context(),
                RequestType::Unknown,
                0,
            )));
            set_tls_tracker_token($tracker);
            let (cb, f) = paired_future_callback();
            let res = storage.sched_txn_command($req.into(), cb);

            async move {
                defer!{{
                    GLOBAL_TRACKERS.remove($tracker);
                }};
                let $v = match res {
                    Err(e) => Err(e),
                    Ok(_) => f.await?,
                };
                let mut $resp = $resp_ty::default();
                if let Some(err) = extract_region_error(&$v) {
                    $resp.set_region_error(err);
                } else {
                    $else_branch
                }
                Ok($resp)
            }
        }
    };
}

txn_command_future!(future_prewrite, PrewriteRequest, PrewriteResponse, (v, resp, tracker) {
    if let Ok(v) = &v {
        resp.set_min_commit_ts(v.min_commit_ts.into_inner());
        resp.set_one_pc_commit_ts(v.one_pc_commit_ts.into_inner());
    }
    resp.set_errors(extract_key_errors(v.map(|v| v.locks)).into());
});
txn_command_future!(future_acquire_pessimistic_lock, PessimisticLockRequest, PessimisticLockResponse,
    (req) {
        let mode = req.get_wake_up_mode()
    };
    (v, resp, tracker) {{
        match v {
            Ok(Ok(res)) => {
                match mode {
                    PessimisticLockWakeUpMode::WakeUpModeForceLock => {
                        let (res, error) = res.into_pb();
                        resp.set_results(res.into());
                        if let Some(e) = error {
                            if let Some(region_error) = extract_region_error_from_error(&e.0) {
                                resp.set_region_error(region_error);
                            } else {
                                resp.set_errors(vec![extract_key_error(&e.0)].into());
                            }
                        }
                    }
                    PessimisticLockWakeUpMode::WakeUpModeNormal => {
                        let (values, not_founds) = res.into_legacy_values_and_not_founds();
                        resp.set_values(values.into());
                        resp.set_not_founds(not_founds);
                    }
                }
            },
            Err(e) | Ok(Err(e)) => {
                resp.set_errors(vec![extract_key_error(&e)].into())
            },
        }
    }}
);
txn_command_future!(future_pessimistic_rollback, PessimisticRollbackRequest, PessimisticRollbackResponse, (v, resp, tracker) {
    resp.set_errors(extract_key_errors(v).into())
});
txn_command_future!(future_batch_rollback, BatchRollbackRequest, BatchRollbackResponse, (v, resp, tracker) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    };
});
txn_command_future!(future_resolve_lock, ResolveLockRequest, ResolveLockResponse, (v, resp, tracker) {
    if let Err(e) = v {
        resp.set_error(extract_key_error(&e));
    }
});
txn_command_future!(future_commit, CommitRequest, CommitResponse, (v, resp, tracker) {
    match v {
        Ok(TxnStatus::Committed { commit_ts }) => {
            resp.set_commit_version(commit_ts.into_inner());
        },
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
txn_command_future!(future_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse, (v, resp, tracker) {
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
    (v, resp, tracker) {
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
txn_command_future!(future_check_secondary_locks, CheckSecondaryLocksRequest, CheckSecondaryLocksResponse, (status, resp, tracker) {
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

/// To measure execute time for a given request.
#[derive(Debug)]
pub struct GrpcRequestDuration {
    pub begin: Instant,
    pub label: GrpcTypeKind,
    pub source: String,
}
impl GrpcRequestDuration {
    pub fn new(begin: Instant, label: GrpcTypeKind, source: String) -> Self {
        GrpcRequestDuration {
            begin,
            label,
            source,
        }
    }
}

/// A single response, will be collected into `MeasuredBatchResponse`.
#[derive(Debug)]
pub struct MeasuredSingleResponse {
    pub id: u64,
    pub resp: MemoryTraceGuard<batch_commands_response::Response>,
    pub measure: GrpcRequestDuration,
}
impl MeasuredSingleResponse {
    pub fn new<T>(id: u64, resp: T, measure: GrpcRequestDuration) -> Self
    where
        MemoryTraceGuard<batch_commands_response::Response>: From<T>,
    {
        let resp = resp.into();
        MeasuredSingleResponse { id, resp, measure }
    }
}

/// A batch response.
pub struct MeasuredBatchResponse {
    pub batch_resp: BatchCommandsResponse,
    pub measures: Vec<GrpcRequestDuration>,
}
impl Default for MeasuredBatchResponse {
    fn default() -> Self {
        MeasuredBatchResponse {
            batch_resp: Default::default(),
            measures: Vec::with_capacity(GRPC_MSG_MAX_BATCH_SIZE),
        }
    }
}

fn collect_batch_resp(v: &mut MeasuredBatchResponse, mut e: MeasuredSingleResponse) {
    v.batch_resp.mut_request_ids().push(e.id);
    v.batch_resp.mut_responses().push(e.resp.consume());
    v.measures.push(e.measure);
}

fn needs_reject_raft_append(reject_messages_on_memory_ratio: f64) -> bool {
    fail_point!("needs_reject_raft_append", |_| true);
    if reject_messages_on_memory_ratio < f64::EPSILON {
        return false;
    }

    let mut usage = 0;
    if memory_usage_reaches_high_water(&mut usage) {
        let raft_msg_usage = (MEMTRACE_RAFT_ENTRIES.sum() + MEMTRACE_RAFT_MESSAGES.sum()) as u64;
        let cached_entries = RAFT_ENTRIES_CACHES_GAUGE.get() as u64;
        let applying_entries = MEMTRACE_APPLYS.sum() as u64;
        if (raft_msg_usage + cached_entries + applying_entries) as f64
            > usage as f64 * reject_messages_on_memory_ratio
        {
            // FIXME: this doesn't output to logfile.
            debug!("need reject log append on memory limit";
                "raft_messages" => raft_msg_usage,
                "cached_entries" => cached_entries,
                "applying_entries" => applying_entries,
                "current_usage" => usage,
                "reject_ratio" => reject_messages_on_memory_ratio);
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures::{channel::oneshot, executor::block_on};
    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;

    #[test]
    fn test_poll_future_notify_with_slow_source() {
        let (tx, rx) = oneshot::channel::<usize>();
        let (signal_tx, signal_rx) = oneshot::channel();

        thread::Builder::new()
            .name("source".to_owned())
            .spawn_wrapper(move || {
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
            .spawn_wrapper(move || {
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
