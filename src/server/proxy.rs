// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! A module for proxy connections from client.
//!
//! Supposing server A is isolated from client, but it can still be accessed
//! by server B, then client can send the message to server B and let server
//! B redirect the message to server A. TiKV will use request metadata key
//! `tikv_receiver_addr` to check where the message should delievered to. If
//! there is no such metadata, it will handle the message by itself.

use collections::HashMap;
use grpcio::{CallOption, Channel, ChannelBuilder, Environment, MetadataBuilder, RpcContext};
use kvproto::tikvpb::TikvClient;
use security::SecurityManager;
use std::future::Future;
use std::str;
use std::sync::{Arc, Weak};
use std::time::Duration;

const PROXY_METADATA: &str = "tikv_receiver_addr";

/// Checks if the message is supposed to send to another address.
pub fn get_target_address<'a>(ctx: &'a RpcContext) -> &'a str {
    let metadata = ctx.request_headers();
    // In most case, proxy is unnecessary.
    if metadata.is_empty() {
        return "";
    }
    for (key, value) in metadata {
        if key == PROXY_METADATA {
            if let Ok(addr) = str::from_utf8(value) {
                return addr;
            }
        }
    }
    ""
}

/// Build a call option that will make receiver to redirect the rpc
/// to `target_address`.
///
/// `target_address` should be a ascii value.
pub fn build_proxy_option(target_address: &str) -> CallOption {
    let mut builder = MetadataBuilder::with_capacity(1);
    builder.add_str(PROXY_METADATA, target_address).unwrap();
    let metadata = builder.build();
    CallOption::default().headers(metadata)
}

#[derive(Clone)]
struct Client {
    channel: Channel,
    client: TikvClient,
}

/// A proxy struct that maintains connections to different addresses.
pub struct Proxy {
    mgr: Arc<SecurityManager>,
    env: Weak<Environment>,
    // todo: Release client if it's not used anymore. For example, become tombstone.
    clients: HashMap<String, Client>,
}

impl Proxy {
    pub fn new(mgr: Arc<SecurityManager>, env: &Arc<Environment>) -> Proxy {
        Proxy {
            mgr,
            env: Arc::downgrade(env),
            clients: HashMap::default(),
        }
    }

    fn connect(&self, addr: &str) -> Option<Client> {
        let env = match self.env.upgrade() {
            Some(e) => e,
            None => return None,
        };

        let cb = ChannelBuilder::new(env)
            .max_receive_message_len(1 << 30) // 1G.
            .max_send_message_len(1 << 30)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));

        let ch = self.mgr.connect(cb, addr);
        Some(Client {
            client: TikvClient::new(ch.clone()),
            channel: ch,
        })
    }

    /// Get a client and do work on the client.
    pub fn call_on<C>(&mut self, addr: &str, callback: C) -> impl Future<Output = ()>
    where
        C: FnOnce(&TikvClient) + Send + 'static,
    {
        let client = match self.clients.get(addr) {
            Some(client) => Some(client.clone()),
            None => match self.connect(addr) {
                Some(c) => {
                    self.clients.insert(addr.to_owned(), c.clone());
                    Some(c)
                }
                None => None,
            },
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
            clients: HashMap::default(),
        }
    }
}

/// Redirect the unary RPC call if necessary.
#[macro_export]
macro_rules! redirect_unary {
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
                            debug!("redirect kv rpc failed";
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
macro_rules! redirect_duplex {
    ($proxy:expr, $func:ident, $ctx:ident, $req:ident, $resp:ident) => {{
        let addr = $crate::server::get_target_address(&$ctx);
        if !addr.is_empty() {
            $ctx.spawn($proxy.call_on(addr, move |client| {
                let (mut proxy_req, proxy_resp) = client.$func().unwrap();
                client.spawn(async move {
                    let bridge_req = async move {
                        proxy_req.send_all(&mut $req.map(|i| i.map(|i| (i, WriteFlags::default())))).await?;
                        proxy_req.close().await
                    };
                    let bridge_resp = async move {
                        $resp.send_all(&mut proxy_resp.map(|i| i.map(|i| (i, WriteFlags::default())))).await?;
                        $resp.close().await
                    };
                    let res = futures::future::join(bridge_req, bridge_resp).await;
                    match res {
                        (Ok(()), Ok(())) => GRPC_PROXY_MSG_COUNTER.$func.success.inc(),
                        (req_res, resp_res) => {
                            debug!("redirect kv rpc failed"; "request" => stringify!($func), "req_err" => ?req_res, "resp_err" => ?resp_res);
                            GRPC_PROXY_MSG_COUNTER.$func.fail.inc();
                        },
                    }
                })
            }));
            return;
        }
    }};
}
