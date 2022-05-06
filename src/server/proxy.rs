// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! A module for forward connections from client.
//!
//! Supposing server A is isolated from client, but it can still be accessed
//! by server B, then client can send the message to server B and let server
//! B forward the message to server A. TiKV will use request metadata key
//! `tikv_receiver_addr` to check where the message should delievered to. If
//! there is no such metadata, it will handle the message by itself.

use std::{
    ffi::CString,
    future::Future,
    str,
    sync::{Arc, Weak},
    time::Duration,
};

use collections::HashMap;
use grpcio::{CallOption, Channel, ChannelBuilder, Environment, MetadataBuilder, RpcContext};
use kvproto::tikvpb::TikvClient;
use security::SecurityManager;

use crate::server::Config;

const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

/// Checks if the message is supposed to send to another address.
pub fn get_target_address<'a>(ctx: &'a RpcContext<'_>) -> &'a str {
    let metadata = ctx.request_headers();
    // In most case, forwarding is unnecessary.
    if metadata.is_empty() {
        return "";
    }
    for (key, value) in metadata {
        if key == FORWARD_METADATA_KEY {
            if let Ok(addr) = str::from_utf8(value) {
                return addr;
            }
        }
    }
    ""
}

/// Build a call option that will make receiver to forward the rpc
/// to `target_address`.
///
/// `target_address` should be a ascii value.
pub fn build_forward_option(target_address: &str) -> CallOption {
    let mut builder = MetadataBuilder::with_capacity(1);
    builder
        .add_str(FORWARD_METADATA_KEY, target_address)
        .unwrap();
    let metadata = builder.build();
    CallOption::default().headers(metadata)
}

#[derive(Clone)]
struct Client {
    channel: Channel,
    client: TikvClient,
}

pub struct ClientPool {
    pool: Vec<Client>,
    last_pos: usize,
}

impl ClientPool {
    fn with_capacity(cap: usize) -> ClientPool {
        ClientPool {
            pool: Vec::with_capacity(cap),
            last_pos: 0,
        }
    }

    fn get_connection(
        &mut self,
        env: &Weak<Environment>,
        mgr: &SecurityManager,
        cfg: &Config,
        addr: &str,
    ) -> Option<Client> {
        if self.last_pos == self.pool.len() {
            if self.last_pos < cfg.forward_max_connections_per_address {
                let env = match env.upgrade() {
                    Some(e) => e,
                    None => return None,
                };
                let cb = ChannelBuilder::new(env)
                    .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
                    .max_receive_message_len(-1)
                    .max_send_message_len(-1)
                    // Memory should be limited by incomming connections already.
                    // And maintaining a shared resource quota doesn't seem easy.
                    .keepalive_time(cfg.grpc_keepalive_time.into())
                    .keepalive_timeout(cfg.grpc_keepalive_timeout.into())
                    .raw_cfg_int(CString::new("connection id").unwrap(), self.last_pos as i32);
                let ch = mgr.connect(cb, addr);
                let client = Client {
                    client: TikvClient::new(ch.clone()),
                    channel: ch,
                };
                self.pool.push(client.clone());
                self.last_pos += 1;
                Some(client)
            } else {
                self.last_pos = 0;
                Some(self.pool[self.last_pos].clone())
            }
        } else {
            let client = self.pool[self.last_pos].clone();
            self.last_pos += 1;
            Some(client)
        }
    }
}

/// A proxy struct that maintains connections to different addresses.
pub struct Proxy {
    mgr: Arc<SecurityManager>,
    env: Weak<Environment>,
    cfg: Arc<Config>,
    // todo: Release client if it's not used anymore. For example, become tombstone.
    pool: HashMap<String, ClientPool>,
}

impl Proxy {
    pub fn new(mgr: Arc<SecurityManager>, env: &Arc<Environment>, cfg: Arc<Config>) -> Proxy {
        Proxy {
            mgr,
            env: Arc::downgrade(env),
            cfg,
            pool: HashMap::default(),
        }
    }

    /// Get a client and do work on the client.
    pub fn call_on<C>(&mut self, addr: &str, callback: C) -> impl Future<Output = ()>
    where
        C: FnOnce(&TikvClient) + Send + 'static,
    {
        let client = match self.pool.get_mut(addr) {
            Some(p) => p.get_connection(&self.env, &self.mgr, &self.cfg, addr),
            None => {
                let p = ClientPool::with_capacity(self.cfg.forward_max_connections_per_address);
                self.pool.insert(addr.to_string(), p);
                self.pool
                    .get_mut(addr)
                    .unwrap()
                    .get_connection(&self.env, &self.mgr, &self.cfg, addr)
            }
        };

        async move {
            let c = match client {
                Some(c) => c,
                None => return,
            };

            // To make it simplied, we rely on gRPC to handle the lifecycle of a
            // Connection.
            if c.channel.wait_for_connected(Duration::from_secs(3)).await {
                callback(&c.client)
            }
        }
    }
}

impl Clone for Proxy {
    fn clone(&self) -> Proxy {
        Proxy {
            env: self.env.clone(),
            mgr: self.mgr.clone(),
            cfg: self.cfg.clone(),
            pool: HashMap::default(),
        }
    }
}

/// Redirect the unary RPC call if necessary.
#[macro_export]
macro_rules! forward_unary {
    ($proxy:expr, $func:ident, $ctx:ident, $req:ident, $resp:ident) => {{
        let addr = $crate::server::get_target_address(&$ctx);
        if !addr.is_empty() {
            $ctx.spawn($proxy.call_on(addr, move |client| {
                let f = paste::paste! {
                    client.[<$func _async>](&$req).unwrap()
                };
                client.spawn(async move {
                    let res = match f.await {
                        Ok(r) => $resp.success(r).await,
                        Err(grpcio::Error::RpcFailure(r)) => $resp.fail(r).await,
                        Err(e) => Err(e),
                    };
                    match res {
                        Ok(()) => GRPC_PROXY_MSG_COUNTER.$func.success.inc(),
                        Err(e) => {
                            debug!("forward kv rpc failed";
                                "request" => stringify!($func),
                                "err" => ?e
                            );
                            GRPC_PROXY_MSG_COUNTER.$func.fail.inc();
                        }
                    }
                })
            }));
            return;
        }
    }};
}

/// Redirect the duplex RPC if necessary.
#[macro_export]
macro_rules! forward_duplex {
    ($proxy:expr, $func:ident, $ctx:ident, $req:ident, $resp:ident) => {{
        let addr = $crate::server::get_target_address(&$ctx);
        if !addr.is_empty() {
            $ctx.spawn($proxy.call_on(addr, move |client| {
                let (mut forward_req, forward_resp) = client.$func().unwrap();
                client.spawn(async move {
                    let bridge_req = async move {
                        forward_req.enhance_batch(true);
                        forward_req.send_all(&mut $req.map(|i| i.map(|i| (i, WriteFlags::default())))).await?;
                        forward_req.close().await
                    };
                    let bridge_resp = async move {
                        $resp.enhance_batch(true);
                        $resp.send_all(&mut forward_resp.map(|i| i.map(|i| (i, WriteFlags::default())))).await?;
                        $resp.close().await
                    };
                    let res = futures::future::join(bridge_req, bridge_resp).await;
                    match res {
                        (Ok(()), Ok(())) => GRPC_PROXY_MSG_COUNTER.$func.success.inc(),
                        (req_res, resp_res) => {
                            debug!("forward kv rpc failed"; "request" => stringify!($func), "req_err" => ?req_res, "resp_err" => ?resp_res);
                            GRPC_PROXY_MSG_COUNTER.$func.fail.inc();
                        },
                    }
                })
            }));
            return;
        }
    }};
}
