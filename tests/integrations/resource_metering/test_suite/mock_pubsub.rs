// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use grpcio::{ChannelBuilder, Environment, Server, ServerBuilder};
use kvproto::resource_usage_agent::create_resource_metering_pub_sub;
use resource_metering::{DataSinkRegHandle, PubSubService};

#[derive(Clone)]
pub struct MockPubSubServer;

impl MockPubSubServer {
    pub fn new(port: u16, env: Arc<Environment>, reg_handle: DataSinkRegHandle) -> Server {
        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .max_concurrent_stream(2)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();

        let ps = PubSubService::new(reg_handle);

        let server_builder = ServerBuilder::new(env)
            .channel_args(channel_args)
            .register_service(create_resource_metering_pub_sub(ps));
        let mut server = server_builder.build().unwrap();
        let _ = server
            .add_listening_port(format!("127.0.0.1:{}", 0), ServerCredentials::Insecure())
            .expect("failed to build mock resource metering publisher server");
        server
    }
}
