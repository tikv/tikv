// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod gc_worker;
mod kv_service;
mod lock_manager;
mod raft_client;
mod security;
mod status_server;

use std::sync::Arc;

use ::security::{SecurityConfig, SecurityManager};
use futures::future::FutureExt;
use grpcio::RpcStatusCode;
use grpcio::*;
use kvproto::coprocessor::*;
use kvproto::kvrpcpb::*;
use kvproto::mpp::*;
use kvproto::raft_serverpb::{Done, RaftMessage, SnapshotChunk};
use kvproto::tikvpb::{
    create_tikv, BatchCommandsRequest, BatchCommandsResponse, BatchRaftMessage, Tikv,
};

macro_rules! unary_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(&mut self, ctx: RpcContext<'_>, _: $req_name, sink: UnarySink<$resp_name>) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! sstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: $req_name,
            sink: ServerStreamingSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! cstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: RequestStream<$req_name>,
            sink: ClientStreamingSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! bstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: RequestStream<$req_name>,
            sink: DuplexSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! unary_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(&mut self, ctx: RpcContext<'_>, req: $req_name, sink: UnarySink<$resp_name>) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! sstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: $req_name,
            sink: ServerStreamingSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! cstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: RequestStream<$req_name>,
            sink: ClientStreamingSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! bstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: RequestStream<$req_name>,
            sink: DuplexSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

#[derive(Clone)]
struct MockKv<T>(pub T);

trait MockKvService {
    unary_call!(kv_get, GetRequest, GetResponse);
    unary_call!(kv_scan, ScanRequest, ScanResponse);
    unary_call!(kv_prewrite, PrewriteRequest, PrewriteResponse);
    unary_call!(
        kv_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    unary_call!(
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    unary_call!(kv_commit, CommitRequest, CommitResponse);
    unary_call!(kv_import, ImportRequest, ImportResponse);
    unary_call!(kv_cleanup, CleanupRequest, CleanupResponse);
    unary_call!(kv_batch_get, BatchGetRequest, BatchGetResponse);
    unary_call!(
        kv_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    unary_call!(kv_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse);
    unary_call!(
        kv_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    unary_call!(
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    unary_call!(kv_scan_lock, ScanLockRequest, ScanLockResponse);
    unary_call!(kv_resolve_lock, ResolveLockRequest, ResolveLockResponse);
    unary_call!(kv_gc, GcRequest, GcResponse);
    unary_call!(kv_delete_range, DeleteRangeRequest, DeleteRangeResponse);
    unary_call!(raw_get, RawGetRequest, RawGetResponse);
    unary_call!(raw_batch_get, RawBatchGetRequest, RawBatchGetResponse);
    unary_call!(raw_scan, RawScanRequest, RawScanResponse);
    unary_call!(raw_batch_scan, RawBatchScanRequest, RawBatchScanResponse);
    unary_call!(raw_put, RawPutRequest, RawPutResponse);
    unary_call!(raw_batch_put, RawBatchPutRequest, RawBatchPutResponse);
    unary_call!(raw_delete, RawDeleteRequest, RawDeleteResponse);
    unary_call!(
        raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    unary_call!(
        raw_delete_range,
        RawDeleteRangeRequest,
        RawDeleteRangeResponse
    );
    unary_call!(ver_get, VerGetRequest, VerGetResponse);
    unary_call!(ver_batch_get, VerBatchGetRequest, VerBatchGetResponse);
    unary_call!(ver_mut, VerMutRequest, VerMutResponse);
    unary_call!(ver_batch_mut, VerBatchMutRequest, VerBatchMutResponse);
    unary_call!(ver_scan, VerScanRequest, VerScanResponse);
    unary_call!(
        ver_delete_range,
        VerDeleteRangeRequest,
        VerDeleteRangeResponse
    );
    unary_call!(
        unsafe_destroy_range,
        UnsafeDestroyRangeRequest,
        UnsafeDestroyRangeResponse
    );
    unary_call!(
        register_lock_observer,
        RegisterLockObserverRequest,
        RegisterLockObserverResponse
    );
    unary_call!(
        check_lock_observer,
        CheckLockObserverRequest,
        CheckLockObserverResponse
    );
    unary_call!(
        remove_lock_observer,
        RemoveLockObserverRequest,
        RemoveLockObserverResponse
    );
    unary_call!(
        physical_scan_lock,
        PhysicalScanLockRequest,
        PhysicalScanLockResponse
    );
    unary_call!(dispatch_mpp_task, DispatchTaskRequest, DispatchTaskResponse);
    unary_call!(cancel_mpp_task, CancelTaskRequest, CancelTaskResponse);
    unary_call!(coprocessor, Request, Response);
    sstream_call!(batch_coprocessor, BatchRequest, BatchResponse);
    sstream_call!(coprocessor_stream, Request, Response);
    sstream_call!(
        establish_mpp_connection,
        EstablishMppConnectionRequest,
        MppDataPacket
    );
    cstream_call!(raft, RaftMessage, Done);
    cstream_call!(batch_raft, BatchRaftMessage, Done);
    cstream_call!(snapshot, SnapshotChunk, Done);
    unary_call!(
        mvcc_get_by_start_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    unary_call!(mvcc_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse);
    unary_call!(split_region, SplitRegionRequest, SplitRegionResponse);
    unary_call!(read_index, ReadIndexRequest, ReadIndexResponse);
    bstream_call!(batch_commands, BatchCommandsRequest, BatchCommandsResponse);
    unary_call!(check_leader, CheckLeaderRequest, CheckLeaderResponse);
    unary_call!(raw_get_key_ttl, RawGetKeyTtlRequest, RawGetKeyTtlResponse);
}

impl<T: MockKvService + Clone + Send + 'static> Tikv for MockKv<T> {
    unary_call_dispatch!(kv_get, GetRequest, GetResponse);
    unary_call_dispatch!(kv_scan, ScanRequest, ScanResponse);
    unary_call_dispatch!(kv_prewrite, PrewriteRequest, PrewriteResponse);
    unary_call_dispatch!(
        kv_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    unary_call_dispatch!(
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    unary_call_dispatch!(kv_commit, CommitRequest, CommitResponse);
    unary_call_dispatch!(kv_import, ImportRequest, ImportResponse);
    unary_call_dispatch!(kv_cleanup, CleanupRequest, CleanupResponse);
    unary_call_dispatch!(kv_batch_get, BatchGetRequest, BatchGetResponse);
    unary_call_dispatch!(
        kv_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    unary_call_dispatch!(kv_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse);
    unary_call_dispatch!(
        kv_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    unary_call_dispatch!(
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    unary_call_dispatch!(kv_scan_lock, ScanLockRequest, ScanLockResponse);
    unary_call_dispatch!(kv_resolve_lock, ResolveLockRequest, ResolveLockResponse);
    unary_call_dispatch!(kv_gc, GcRequest, GcResponse);
    unary_call_dispatch!(kv_delete_range, DeleteRangeRequest, DeleteRangeResponse);
    unary_call_dispatch!(raw_get, RawGetRequest, RawGetResponse);
    unary_call_dispatch!(raw_batch_get, RawBatchGetRequest, RawBatchGetResponse);
    unary_call_dispatch!(raw_scan, RawScanRequest, RawScanResponse);
    unary_call_dispatch!(raw_batch_scan, RawBatchScanRequest, RawBatchScanResponse);
    unary_call_dispatch!(raw_put, RawPutRequest, RawPutResponse);
    unary_call_dispatch!(raw_batch_put, RawBatchPutRequest, RawBatchPutResponse);
    unary_call_dispatch!(raw_delete, RawDeleteRequest, RawDeleteResponse);
    unary_call_dispatch!(
        raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    unary_call_dispatch!(
        raw_delete_range,
        RawDeleteRangeRequest,
        RawDeleteRangeResponse
    );
    unary_call_dispatch!(ver_get, VerGetRequest, VerGetResponse);
    unary_call_dispatch!(ver_batch_get, VerBatchGetRequest, VerBatchGetResponse);
    unary_call_dispatch!(ver_mut, VerMutRequest, VerMutResponse);
    unary_call_dispatch!(ver_batch_mut, VerBatchMutRequest, VerBatchMutResponse);
    unary_call_dispatch!(ver_scan, VerScanRequest, VerScanResponse);
    unary_call_dispatch!(
        ver_delete_range,
        VerDeleteRangeRequest,
        VerDeleteRangeResponse
    );
    unary_call_dispatch!(
        unsafe_destroy_range,
        UnsafeDestroyRangeRequest,
        UnsafeDestroyRangeResponse
    );
    unary_call_dispatch!(
        register_lock_observer,
        RegisterLockObserverRequest,
        RegisterLockObserverResponse
    );
    unary_call_dispatch!(
        check_lock_observer,
        CheckLockObserverRequest,
        CheckLockObserverResponse
    );
    unary_call_dispatch!(
        remove_lock_observer,
        RemoveLockObserverRequest,
        RemoveLockObserverResponse
    );
    unary_call_dispatch!(
        physical_scan_lock,
        PhysicalScanLockRequest,
        PhysicalScanLockResponse
    );
    unary_call_dispatch!(dispatch_mpp_task, DispatchTaskRequest, DispatchTaskResponse);
    unary_call_dispatch!(cancel_mpp_task, CancelTaskRequest, CancelTaskResponse);
    unary_call_dispatch!(coprocessor, Request, Response);
    sstream_call_dispatch!(batch_coprocessor, BatchRequest, BatchResponse);
    sstream_call_dispatch!(coprocessor_stream, Request, Response);
    sstream_call_dispatch!(
        establish_mpp_connection,
        EstablishMppConnectionRequest,
        MppDataPacket
    );
    cstream_call_dispatch!(raft, RaftMessage, Done);
    cstream_call_dispatch!(batch_raft, BatchRaftMessage, Done);
    cstream_call_dispatch!(snapshot, SnapshotChunk, Done);
    unary_call_dispatch!(
        mvcc_get_by_start_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    unary_call!(mvcc_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse);
    unary_call_dispatch!(split_region, SplitRegionRequest, SplitRegionResponse);
    unary_call_dispatch!(read_index, ReadIndexRequest, ReadIndexResponse);
    bstream_call_dispatch!(batch_commands, BatchCommandsRequest, BatchCommandsResponse);
    unary_call_dispatch!(check_leader, CheckLeaderRequest, CheckLeaderResponse);
    unary_call_dispatch!(raw_get_key_ttl, RawGetKeyTtlRequest, RawGetKeyTtlResponse);
}

fn mock_kv_service<T>(kv: MockKv<T>, ip: &str, port: u16) -> Result<Server>
where
    T: MockKvService + Clone + Send + 'static,
{
    let env = Arc::new(Environment::new(2));
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

    let channel_args = ChannelBuilder::new(Arc::clone(&env))
        .max_concurrent_stream(2)
        .max_receive_message_len(-1)
        .max_send_message_len(-1)
        .build_args();

    let mut sb = ServerBuilder::new(Arc::clone(&env))
        .channel_args(channel_args)
        .register_service(create_tikv(kv));
    sb = security_mgr.bind(sb, ip, port);
    sb.build()
}
