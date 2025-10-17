// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fail::fail_point;
use futures::{
    future::{self, FutureExt},
    stream::TryStreamExt,
};
use futures_util::TryFutureExt;
use grpcio::{
    ClientStreamingSink, DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode,
};
use kvproto::raft_serverpb::*;
use raft::eraftpb::MessageType;
use raftstore::{Error as RaftStoreError, Result as RaftStoreResult};
use tikv_kv::RaftExtension;
use tikv_util::{time::nanos_to_secs, worker::Scheduler};

use crate::{
    server::{
        MetadataSourceStoreId,
        metrics::{
            RAFT_APPEND_REJECTS, RAFT_MESSAGE_BATCH_SIZE, RAFT_MESSAGE_DURATION,
            RAFT_MESSAGE_RECV_COUNTER,
        },
        snap::{Task as SnapTask, Task},
    },
    storage::kv::Engine,
};

impl Task {
    fn get_sink(self) -> ClientStreamingSink<Done> {
        match self {
            Task::Recv { sink, .. } => sink,
            _ => unreachable!(),
        }
    }
}

/// Service handles the RPC messages for the `RaftServer` service.
#[derive(Clone)]
pub struct RaftService<E: Engine> {
    store_id: u64,
    ch: E::RaftExtension,
    snap_scheduler: Scheduler<SnapTask>,
    raft_message_filter: Arc<dyn super::kv::RaftGrpcMessageFilter>,
}

impl<E: Engine> RaftService<E> {
    pub fn new(
        store_id: u64,
        ch: E::RaftExtension,
        snap_scheduler: Scheduler<SnapTask>,
        raft_message_filter: Arc<dyn super::kv::RaftGrpcMessageFilter>,
    ) -> Self {
        RaftService {
            store_id,
            ch,
            snap_scheduler,
            raft_message_filter,
        }
    }

    fn handle_raft_message(
        store_id: u64,
        ch: &E::RaftExtension,
        msg: RaftMessage,
        raft_msg_filter: &Arc<dyn super::kv::RaftGrpcMessageFilter>,
    ) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if to_store_id != store_id {
            return Err(RaftStoreError::StoreNotMatch {
                to_store_id,
                my_store_id: store_id,
            });
        }

        if raft_msg_filter.should_reject_raft_message(&msg) {
            if msg.get_message().get_msg_type() == MessageType::MsgAppend {
                RAFT_APPEND_REJECTS.inc();
            }
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

impl<E: Engine> RaftServer for RaftService<E>
where
    E: Engine,
{
    fn raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        let source_store_id = Self::get_store_id_from_metadata(&ctx);
        info!(
            "raft RPC is called, new gRPC stream established";
            "source_store_id" => ?source_store_id,
        );

        let store_id = self.store_id;
        let ch = self.ch.clone();
        let raft_msg_filter = self.raft_message_filter.clone();
        let handle_stream = stream
            .map_err(crate::server::Error::from)
            .try_for_each(move |msg| {
                RAFT_MESSAGE_RECV_COUNTER.inc();

                future::ready(
                    Self::handle_raft_message(store_id, &ch, msg, &raft_msg_filter)
                        .map_err(crate::server::Error::from),
                )
            })
            .map_ok(|_| Done::default());

        ctx.spawn(
            handle_stream
                .then(|res| async move {
                    let status = match res {
                        Ok(_) => RpcStatus::new(RpcStatusCode::OK),
                        Err(e) => {
                            let msg = format!("{:?}", e);
                            RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                        }
                    };
                    let _ = sink.fail(status).await;
                })
                .map(|_| ()),
        );
    }

    fn batch_raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchRaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        let store_id = self.store_id;
        if let Some(source_store_id) = Self::get_store_id_from_metadata(&ctx) {
            info!(
                "batch_raft RPC is called, new gRPC stream established";
                "source_store_id" => source_store_id,
            );

            let ch = self.ch.clone();
            let raft_msg_filter = self.raft_message_filter.clone();
            let handle_stream = stream
                .map_err(crate::server::Error::from)
                .try_for_each(move |mut batch_msg| {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;
                    let elapsed = nanos_to_secs(now.saturating_sub(batch_msg.last_observed_time));
                    RAFT_MESSAGE_DURATION.receive_delay.observe(elapsed);

                    let len = batch_msg.get_msgs().len();
                    RAFT_MESSAGE_RECV_COUNTER.inc_by(len as u64);
                    RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);

                    for msg in batch_msg.take_msgs().into_iter() {
                        if let Err(e) =
                            Self::handle_raft_message(store_id, &ch, msg, &raft_msg_filter)
                        {
                            return future::err(crate::server::Error::from(e));
                        }
                    }
                    future::ok(())
                })
                .map_ok(|_| Done::default());
            ctx.spawn(
                handle_stream
                    .then(|res| async move {
                        let status = match res {
                            Ok(_) => RpcStatus::new(RpcStatusCode::OK),
                            Err(e) => {
                                let msg = format!("{:?}", e);
                                RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                            }
                        };
                        let _ = sink.fail(status).await;
                    })
                    .map(|_| ()),
            );
        } else {
            let status = RpcStatus::with_message(
                RpcStatusCode::INVALID_ARGUMENT,
                "missing source store id".to_owned(),
            );
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        let task = Task::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, e.to_string());
            ctx.spawn(
                e.into_inner()
                    .get_sink()
                    .fail(status)
                    .map_err(|e| error!("failed to send error to reply"; "err" => %e))
                    .map(|_| ()),
            );
        }
    }

    fn tablet_snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<TabletSnapshotRequest>,
        sink: DuplexSink<TabletSnapshotResponse>,
    ) {
        let task = Task::RecvTablet { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, e.to_string());
            let err_sink = match e.into_inner() {
                Task::RecvTablet { sink, .. } => sink,
                _ => unreachable!(),
            };
            ctx.spawn(
                err_sink
                    .fail(status)
                    .map_err(|e| error!("failed to send error to reply"; "err" => %e))
                    .map(|_| ()),
            )
        }
    }
}
