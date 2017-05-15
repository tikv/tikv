// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::result;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;
use std::time::Duration;
use std::collections::HashSet;

use futures::{Future, BoxFuture};
use futures::future::ok;
use futures::future::{loop_fn, Loop};

use tokio_timer::Timer;

use grpc;
use grpc::futures_grpc::GrpcFutureSend;
use url::Url;
use rand::{self, Rng};

use kvproto::pdpb::{ResponseHeader, ErrorType, GetMembersRequest, GetMembersResponse, Member};
use kvproto::pdpb_grpc::{PDAsyncClient, PDAsync};

use util::HandyRwLock;

use super::super::{Result, Error, PdFuture};
use super::super::metrics::PD_SEND_MSG_HISTOGRAM;

pub struct Inner {
    client: PDAsyncClient,
    members: GetMembersResponse,

    last_update: Instant,
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Inner {
                client: client,
                members: members,

                last_update: Instant::now(),
            })),
        }
    }

    pub fn request<Req, Resp, F>(&self, req: Req, f: F, retry: usize) -> Request<Req, Resp, F>
        where Req: Clone + 'static,
              F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            timer: Timer::default(),
            client: LeaderClient { inner: self.inner.clone() },
            req: req,
            resp: None,
            func: f,
        }
    }

    pub fn get_leader(&self) -> Member {
        self.inner.rl().members.get_leader().clone()
    }

    // Re-establish connection with PD leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        let ((client, members), start) = {
            let inner = self.inner.rl();
            if inner.last_update.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC) {
                // Avoid unnecessary updating.
                return Ok(());
            }

            let start = Instant::now();
            (try!(try_connect_leader(&inner.members)), start)
        };

        {
            let mut inner = self.inner.wl();
            inner.client = client;
            inner.members = members;
            inner.last_update = Instant::now();
        }
        warn!("updating PD client done, spent {:?}", start.elapsed());
        Ok(())
    }
}

const RECONNECT_INTERVAL_SEC: u64 = 1; // 1s

/// The context of sending requets.
pub struct Request<Req, Resp, F> {
    reconnect_count: usize,
    request_sent: usize,
    timer: Timer,

    client: LeaderClient,

    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 5;

impl<Req, Resp, F> Request<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&PDAsyncClient, Req) -> PdFuture<Resp> + Send + 'static
{
    fn reconnect_if_needed(mut self) -> BoxFuture<Self, Self> {
        debug!("reconnect remains: {}", self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return ok(self).boxed();
        }

        // Updating client.
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        warn!("updating PD client, block the tokio core");
        match self.client.reconnect() {
            Ok(_) => {
                self.request_sent = 0;
                ok(self).boxed()
            }
            Err(_) => {
                self.timer
                    .sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC))
                    .then(|_| Err(self))
                    .boxed()
            }
        }
    }

    fn send_and_receive(mut self) -> BoxFuture<Self, Self> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();
        let req = (self.func)(&self.client.inner.rl().client, r);
        req.then(|resp| {
                match resp {
                    Ok(resp) => {
                        self.resp = Some(Ok(resp));
                        Ok(self)
                    }
                    Err(err) => {
                        error!("request failed: {:?}", err);
                        Err(self)
                    }
                }
            })
            .boxed()
    }

    fn break_or_continue(ctx: result::Result<Self, Self>) -> Result<Loop<Self, Self>> {
        let ctx = match ctx {
            Ok(ctx) | Err(ctx) => ctx,
        };
        let done = ctx.reconnect_count == 0 || ctx.resp.is_some();
        if done {
            Ok(Loop::Break(ctx))
        } else {
            Ok(Loop::Continue(ctx))
        }
    }

    fn post_loop(ctx: Result<Self>) -> Result<Resp> {
        let ctx = ctx.expect("end loop with Ok(_)");
        ctx.resp.unwrap_or_else(|| Err(box_err!("fail to request")))
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let ctx = self;
        loop_fn(ctx, |ctx| {
                ctx.reconnect_if_needed()
                    .and_then(Self::send_and_receive)
                    .then(Self::break_or_continue)
            })
            .then(Self::post_loop)
            .boxed()
    }
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &LeaderClient, retry: usize, func: F) -> Result<R>
    where F: Fn(&PDAsyncClient) -> GrpcFutureSend<R>
{
    for _ in 0..retry {
        let r = {
            let _timer = PD_SEND_MSG_HISTOGRAM.start_timer(); // observe on dropping.
            let f = func(&client.inner.rl().client);
            f.wait()
        };

        match r {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!("fail to request: {:?}", e);
                if let Err(e) = client.reconnect() {
                    error!("fail to reconnect: {:?}", e);
                }
            }
        }
    }

    Err(box_err!("fail to request"))
}

pub fn validate_endpoints(endpoints: &[String]) -> Result<(PDAsyncClient, GetMembersResponse)> {
    if endpoints.is_empty() {
        return Err(box_err!("empty PD endpoints"));
    }

    let len = endpoints.len();
    let mut endpoints_set = HashSet::with_capacity(len);

    let mut members = None;
    let mut cluster_id = None;
    for ep in endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("duplicate PD endpoint {}", ep));
        }

        let (_, resp) = match connect(ep) {
            Ok(resp) => resp,
            // Ignore failed PD node.
            Err(e) => {
                error!("PD endpoint {} failed to respond: {:?}", ep, e);
                continue;
            }
        };

        // Check cluster ID.
        let cid = resp.get_header().get_cluster_id();
        if let Some(sample) = cluster_id {
            if sample != cid {
                return Err(box_err!("PD response cluster_id mismatch, want {}, got {}",
                                    sample,
                                    cid));
            }
        } else {
            cluster_id = Some(cid);
        }
        // TODO: check all fields later?

        if members.is_none() {
            members = Some(resp);
        }
    }

    match members {
        Some(members) => {
            let (client, members) = try!(try_connect_leader(&members));
            info!("All PD endpoints are consistent: {:?}", endpoints);
            Ok((client, members))
        }
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

fn connect(addr: &str) -> Result<(PDAsyncClient, GetMembersResponse)> {
    debug!("connect to PD endpoint: {:?}", addr);
    let ep = box_try!(Url::parse(addr));
    let host = ep.host_str().unwrap();
    let port = ep.port().unwrap();

    let mut conf: grpc::client::GrpcClientConf = Default::default();
    conf.http.no_delay = Some(true);

    // TODO: It seems that `new` always return an Ok(_).
    PDAsyncClient::new(host, port, false, conf)
        .and_then(|client| {
            // try request.
            match client.GetMembers(GetMembersRequest::new()).wait() {
                Ok(resp) => Ok((client, resp)),
                Err(e) => Err(e),
            }
        })
        .map_err(Error::Grpc)
}

pub fn try_connect_leader(previous: &GetMembersResponse)
                          -> Result<(PDAsyncClient, GetMembersResponse)> {
    // Try to connect other members.
    // Randomize endpoints.
    let members = previous.get_members();
    let mut indexes: Vec<usize> = (0..members.len()).collect();
    rand::thread_rng().shuffle(&mut indexes);

    let cluster_id = previous.get_header().get_cluster_id();
    let mut resp = None;
    'outer: for i in indexes {
        for ep in members[i].get_client_urls() {
            match connect(ep.as_str()) {
                Ok((_, r)) => {
                    let new_cluster_id = r.get_header().get_cluster_id();
                    if new_cluster_id == cluster_id {
                        resp = Some(r);
                        break 'outer;
                    } else {
                        warn!("{} no longer belongs to cluster {}, it is in {}",
                              ep,
                              cluster_id,
                              new_cluster_id);
                        continue 'outer;
                    }
                }
                Err(e) => {
                    error!("failed to connect to {}, {:?}", ep, e);
                    continue;
                }
            }
        }
    }

    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(ep.as_str()) {
                info!("connect to PD leader {:?}", ep);
                return Ok((client, resp));
            }
        }
    }

    Err(box_err!("failed to connect to {:?}", members))
}

pub fn check_resp_header(header: &ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    // TODO: translate more error types
    let err = header.get_error();
    match err.get_field_type() {
        ErrorType::ALREADY_BOOTSTRAPPED => Err(Error::ClusterBootstrapped(header.get_cluster_id())),
        ErrorType::NOT_BOOTSTRAPPED => Err(Error::ClusterNotBootstrapped(header.get_cluster_id())),
        _ => Err(box_err!(err.get_message())),
    }
}
