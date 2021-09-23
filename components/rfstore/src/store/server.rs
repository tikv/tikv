// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::channel::mpsc;
use grpcio;
use kvproto::raft_serverpb as rspb;
use kvproto::tikvpb;

use super::*;

pub struct InnerServer {
    pub(crate) engines: Engines,
    pub(crate) config: Config,
    pub(crate) router: Router,
    pub(crate) batch_system: RaftBatchSystem,
}

impl InnerServer {
    pub fn new(mut engines: Engines, config: Config) -> Self {
        let (router, batch_system) = create_raft_batch_system(&config);
        engines.listener.init_msg_ch(router.store_sender.clone());
        return Self {
            engines,
            config,
            router,
            batch_system,
        };
    }

    pub(crate) fn raft(
        &self,
        ctx: grpcio::RpcContext,
        stream: grpcio::RequestStream<rspb::RaftMessage>,
        sink: ::grpcio::ClientStreamingSink<rspb::Done>,
    ) {
        todo!()
    }

    pub(crate) fn batch_raft(
        &self,
        ctx: grpcio::RpcContext,
        stream: grpcio::RequestStream<tikvpb::BatchRaftMessage>,
        sink: ::grpcio::ClientStreamingSink<rspb::Done>,
    ) {
        todo!()
    }

    pub fn start(pd_client: Arc<pd_client::PdClient>) {
        todo!()
    }
}
