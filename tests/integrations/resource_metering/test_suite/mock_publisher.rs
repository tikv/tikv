// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio::{ChannelBuilder, Environment, Server, ServerBuilder};
use kvproto::resource_usage_agent::create_resource_metering_pub_sub;
use resource_metering::{Reporter, ResourceMeteringPublisher};
use std::sync::Arc;

#[derive(Clone)]
pub struct MockPublisherServer;

impl MockPublisherServer {
    pub fn build(port: u16, env: Arc<Environment>, rpt: &Reporter) -> Server {
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .max_concurrent_stream(2)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();

        let rmp = ResourceMeteringPublisher::new(rpt);

        let server_builder = ServerBuilder::new(env)
            .channel_args(channel_args)
            .bind("127.0.0.1", port)
            .register_service(create_resource_metering_pub_sub(rmp));

        server_builder
            .build()
            .expect("failed to build mock resource metering publisher server")
    }
}
