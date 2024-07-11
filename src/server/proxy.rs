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
    task::{Context, Poll},
    time::Duration,
};

use collections::HashMap;
use grpcio::{CallOption, MetadataBuilder};
use http_body::Body;
use kvproto::tikvpb_grpc::tikv_client::TikvClient;
use security::SecurityManager;
use tonic::{server::NamedService, transport::Channel};

use crate::server::Config;

const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

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
    client: TikvClient<Channel>,
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
        mgr: &SecurityManager,
        cfg: &Config,
        addr: &str,
    ) -> Option<Client> {
        if self.last_pos == self.pool.len() {
            if self.last_pos < cfg.forward_max_connections_per_address {
                let addr = tikv_util::format_url(addr, mgr.is_ssl_enabled());
                let ch = Channel::from_shared(addr)
                    .unwrap()
                    .initial_stream_window_size(cfg.grpc_stream_initial_window_size.0 as u32)
                    .http2_keep_alive_interval(cfg.grpc_keepalive_time.into())
                    .keep_alive_timeout(cfg.grpc_keepalive_timeout.into());
                let ch = mgr.set_tls_config(ch).unwrap().connect_lazy();

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
    // env: Weak<Environment>,
    cfg: Arc<Config>,
    // todo: Release client if it's not used anymore. For example, become tombstone.
    pool: HashMap<String, Channel>,
}

impl Proxy {
    pub fn new(mgr: Arc<SecurityManager>, cfg: Arc<Config>) -> Proxy {
        Proxy {
            mgr,
            cfg,
            pool: HashMap::default(),
        }
    }
}

impl Clone for Proxy {
    fn clone(&self) -> Proxy {
        Proxy {
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

pub struct ProxyService<S> {
    mgr: Arc<SecurityManager>,
    // env: Weak<Environment>,
    cfg: Arc<Config>,
    // todo: Release client if it's not used anymore. For example, become tombstone.
    pool: HashMap<String, Channel>,
    // the wrapped grpc service
    svc: S,
}

impl<S> ProxyService<S> {
    pub fn new(mgr: Arc<SecurityManager>, cfg: Arc<Config>, svc: S) -> Self {
        ProxyService {
            mgr,
            cfg,
            pool: HashMap::default(),
            svc,
        }
    }

    pub fn get_channel(&mut self, addr: &str) -> Option<Channel> {
        if let Some(c) = self.pool.get(addr) {
            return Some(c.clone());
        }

        let addr = tikv_util::format_url(addr, self.mgr.is_ssl_enabled());
        let mut endpoint = Channel::from_shared(addr.to_string()).ok()?;
        let ch = endpoint
            .initial_stream_window_size(self.cfg.grpc_stream_initial_window_size.0 as u32)
            .http2_keep_alive_interval(self.cfg.grpc_keepalive_time.into())
            .keep_alive_timeout(self.cfg.grpc_keepalive_timeout.into());
        // TODO: does lasy connect ok here?
        let ch = self.mgr.set_tls_config(ch).unwrap().connect_lazy();
        self.pool.insert(addr.to_string(), ch.clone());
        Some(ch)
    }
}

impl<S: Clone> Clone for ProxyService<S> {
    fn clone(&self) -> Self {
        Self {
            mgr: self.mgr.clone(),
            cfg: self.cfg.clone(),
            pool: HashMap::default(),
            svc: self.svc.clone(),
        }
    }
}

impl<S> tonic::codegen::Service<http::Request<hyper::body::Body>> for ProxyService<S>
where
    S: tonic::codegen::Service<
            http::Request<hyper::body::Body>,
            Response = http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
            Future = tonic::codegen::BoxFuture<
                http::Response<tonic::body::BoxBody>,
                std::convert::Infallible,
            >,
        >,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<hyper::body::Body>) -> Self::Future {
        if let Some(addr) = get_target_address(&request) {
            if let Some(mut ch) = self.get_channel(addr) {
                let (head, body) = request.into_parts();
                let boxed_body = to_boxed_body(body);
                let request = http::Request::from_parts(head, boxed_body);
                let fut = ch.call(request);
                return Box::pin(async move {
                    match fut.await {
                        Ok(r) => {
                            let (mut parts, body) = r.into_parts();
                            // Set the content type
                            parts.headers.insert(
                                http::header::CONTENT_TYPE,
                                http::header::HeaderValue::from_static("application/grpc"),
                            );
                            Ok(http::Response::from_parts(parts, to_boxed_body(body)))
                        }
                        Err(e) => Ok(tonic::Status::from_error(e.into()).to_http()),
                    }
                });
            }
        }

        self.svc.call(request)
    }
}

impl<S: NamedService> NamedService for ProxyService<S> {
    const NAME: &'static str = <S as NamedService>::NAME;
}

pub fn get_target_address<T>(req: &http::Request<T>) -> Option<&str> {
    req.headers()
        .get(FORWARD_METADATA_KEY)
        .and_then(|v| v.to_str().ok())
}

fn to_boxed_body(body: hyper::Body) -> tonic::body::BoxBody {
    body.map_err(|e| tonic::Status::from_error(e.into()))
        .boxed_unsync()
}
