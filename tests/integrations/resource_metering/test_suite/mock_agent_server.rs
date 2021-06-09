// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crossbeam::channel::Sender;
use futures::prelude::*;
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcContext, Server,
    ServerBuilder,
};
use kvproto::resource_usage_agent::{
    create_resource_usage_agent, ReportCpuTimeRequest, ReportCpuTimeResponse, ResourceUsageAgent,
};

#[derive(Clone)]
pub struct MockAgentServer {
    tx: Sender<ReportCpuTimeRequest>,
}

impl MockAgentServer {
    pub fn new(tx: Sender<ReportCpuTimeRequest>) -> Self {
        Self { tx }
    }

    pub fn build_server(self, port: u16, env: Arc<Environment>) -> Server {
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .max_concurrent_stream(2)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();

        let server = ServerBuilder::new(env)
            .channel_args(channel_args)
            .bind("127.0.0.1", port)
            .register_service(create_resource_usage_agent(self));

        server.build().expect("failed to build mock agent server")
    }
}

impl ResourceUsageAgent for MockAgentServer {
    fn report_cpu_time(
        &mut self,
        ctx: RpcContext,
        mut stream: RequestStream<ReportCpuTimeRequest>,
        sink: ClientStreamingSink<ReportCpuTimeResponse>,
    ) {
        let tx = self.tx.clone();
        let f = async move {
            while let Some(req) = stream.try_next().await? {
                tx.send(req).unwrap();
            }
            sink.success(ReportCpuTimeResponse::default()).await?;

            Ok(())
        }
        .map_err(|_e: grpcio::Error| {})
        .map(|_| {});

        ctx.spawn(f);
    }
}
