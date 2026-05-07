// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod debugger;
mod gc_worker;
mod kv_service;
mod lock_manager;
mod raft_client;
mod security;
mod server;
mod status_server;

use std::sync::Arc;

use ::security::{SecurityConfig, SecurityManager};
use grpcio::*;
use kvproto::tikvpb::{create_tikv, Tikv};

fn tikv_service<T>(kv: T, ip: &str, port: u16) -> Result<Server>
where
    T: Tikv + Clone + Send + 'static,
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
