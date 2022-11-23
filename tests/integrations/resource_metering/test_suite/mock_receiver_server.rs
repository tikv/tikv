// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

use crossbeam::channel::Sender;
use futures::prelude::*;
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcContext, Server,
    ServerBuilder,
};
use kvproto::resource_usage_agent::{
    create_resource_usage_agent, EmptyResponse, ResourceUsageAgent, ResourceUsageRecord,
};

pub struct MockReceiverServer {
    should_block: Arc<AtomicBool>,
    tx: Sender<Vec<ResourceUsageRecord>>,
    server: Option<Server>,
}

impl MockReceiverServer {
    pub fn new(tx: Sender<Vec<ResourceUsageRecord>>) -> Self {
        Self {
            should_block: Arc::default(),
            tx,
            server: None,
        }
    }

    pub fn start_server(&mut self, port: u16, env: Arc<Environment>) {
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .max_concurrent_stream(2)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();

        let server = ServerBuilder::new(env)
            .channel_args(channel_args)
            .bind("127.0.0.1", port)
            .register_service(create_resource_usage_agent(MockReceiverService {
                should_block: self.should_block.clone(),
                tx: self.tx.clone(),
            }));

        let mut server = server
            .build()
            .expect("failed to build mock receiver server");
        server.start();
        self.server = Some(server);
    }

    pub fn block(&self) {
        self.should_block.store(true, Ordering::SeqCst);
    }

    pub fn unblock(&self) {
        self.should_block.store(false, Ordering::SeqCst);
    }

    pub async fn shutdown_server(&mut self) {
        self.server.take().unwrap().shutdown().await.unwrap();
    }
}

#[derive(Clone)]
struct MockReceiverService {
    should_block: Arc<AtomicBool>,
    tx: Sender<Vec<ResourceUsageRecord>>,
}

impl ResourceUsageAgent for MockReceiverService {
    fn report(
        &mut self,
        ctx: RpcContext<'_>,
        mut stream: RequestStream<ResourceUsageRecord>,
        sink: ClientStreamingSink<EmptyResponse>,
    ) {
        while self.should_block.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(100));
        }

        let tx = self.tx.clone();
        let f = async move {
            let mut res = vec![];
            while let Some(req) = stream.try_next().await? {
                res.push(req);
            }
            tx.send(res).unwrap();
            sink.success(EmptyResponse::default()).await?;

            Ok(())
        }
        .map_err(|_e: grpcio::Error| {})
        .map(|_| {});

        ctx.spawn(f);
    }
}
