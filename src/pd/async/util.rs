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

use futures::{Future, BoxFuture, Stream, Async, task, Poll};
use futures::task::Task;
use futures::future::{loop_fn, Loop, ok};
use futures::sync::mpsc::UnboundedSender;
use grpc::{Environment, ChannelBuilder, Result as GrpcResult, ClientDuplexSender,
           ClientDuplexReceiver};
use tokio_timer::Timer;
use rand::{self, Rng};
use kvproto::pdpb::{ResponseHeader, ErrorType, GetMembersRequest, GetMembersResponse, Member,
                    RegionHeartbeatRequest, RegionHeartbeatResponse};
use kvproto::pdpb_grpc::PdClient;
use prometheus::HistogramTimer;

use util::{HandyRwLock, Either};
use pd::{Result, Error, PdFuture};
use pd::metrics::PD_SEND_MSG_HISTOGRAM;

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<Option<ClientDuplexSender<RegionHeartbeatRequest>>,
                          UnboundedSender<RegionHeartbeatRequest>>,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Task>,
    pub client: PdClient,
    members: GetMembersResponse,

    last_update: Instant,
}

pub struct HeartbeatReceiver {
    receiver: Option<ClientDuplexReceiver<RegionHeartbeatResponse>>,
    inner: Arc<RwLock<Inner>>,
}

impl Stream for HeartbeatReceiver {
    type Item = RegionHeartbeatResponse;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        loop {
            if let Some(ref mut receiver) = self.receiver {
                match receiver.poll() {
                    Ok(Async::Ready(Some(item))) => return Ok(Async::Ready(Some(item))),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    // If it's None or there's error, we need to update receiver.
                    _ => {}
                }
            }

            self.receiver.take();

            let mut inner = self.inner.wl();
            let mut receiver = None;
            if let Either::Left(ref mut recv) = inner.hb_receiver {
                receiver = recv.take();
            }
            if receiver.is_some() {
                self.receiver = receiver;
            } else {
                inner.hb_receiver = Either::Right(task::current());
                return Ok(Async::NotReady);
            }
        }
    }
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    timer: Timer,
    inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(env: Arc<Environment>,
               client: PdClient,
               members: GetMembersResponse)
               -> LeaderClient {
        let (tx, rx) = client.region_heartbeat();
        LeaderClient {
            timer: Timer::default(),
            inner: Arc::new(RwLock::new(Inner {
                env: env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client: client,
                members: members,

                last_update: Instant::now(),
            })),
        }
    }

    pub fn handle_region_heartbeat_response<F>(&self, f: F) -> PdFuture<()>
        where F: Fn(RegionHeartbeatResponse) + Send + 'static
    {
        let recv = HeartbeatReceiver {
            receiver: None,
            inner: self.inner.clone(),
        };
        recv.for_each(move |resp| {
                f(resp);
                Ok(())
            })
            .map_err(|e| panic!("unexpected error: {:?}", e))
            .boxed()
    }

    pub fn request<Req, Resp, F>(&self, req: Req, f: F, retry: usize) -> Request<Req, Resp, F>
        where Req: Clone + 'static,
              F: FnMut(&RwLock<Inner>, Req) -> PdFuture<Resp> + Send + 'static
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            client: LeaderClient {
                timer: self.timer.clone(),
                inner: self.inner.clone(),
            },
            req: req,
            resp: None,
            func: f,
            timer: None,
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
            (try!(try_connect_leader(inner.env.clone(), &inner.members)), start)
        };

        {
            let mut inner = self.inner.wl();
            let (tx, rx) = client.region_heartbeat();
            inner.hb_sender = Either::Left(Some(tx));
            if let Either::Right(ref mut task) = inner.hb_receiver {
                task.notify();
            }
            inner.hb_receiver = Either::Left(Some(rx));
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

    client: LeaderClient,

    req: Req,
    resp: Option<Result<Resp>>,
    func: F,

    timer: Option<HistogramTimer>,
}

const MAX_REQUEST_COUNT: usize = 5;

impl<Req, Resp, F> Request<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&RwLock<Inner>, Req) -> PdFuture<Resp> + Send + 'static
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
                self.client
                    .timer
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

        ok(self)
            .and_then(|mut ctx| {
                ctx.timer = Some(PD_SEND_MSG_HISTOGRAM.start_timer());
                let req = (ctx.func)(&ctx.client.inner, r);
                req.then(|resp| {
                    // Observe on dropping, schedule time will be recorded too.
                    ctx.timer.take();
                    match resp {
                        Ok(resp) => {
                            ctx.resp = Some(Ok(resp));
                            Ok(ctx)
                        }
                        Err(err) => {
                            error!("request failed: {:?}", err);
                            Err(ctx)
                        }
                    }
                })
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
    where F: Fn(&PdClient) -> GrpcResult<R>
{
    for _ in 0..retry {
        let r = {
            let _timer = PD_SEND_MSG_HISTOGRAM.start_timer(); // observe on dropping.
            func(&client.inner.rl().client).map_err(Error::Grpc)
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

pub fn validate_endpoints(env: Arc<Environment>,
                          endpoints: &[String])
                          -> Result<(PdClient, GetMembersResponse)> {
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

        let (_, resp) = match connect(env.clone(), ep) {
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
            let (client, members) = try!(try_connect_leader(env.clone(), &members));
            info!("All PD endpoints are consistent: {:?}", endpoints);
            Ok((client, members))
        }
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

fn connect(env: Arc<Environment>, addr: &str) -> Result<(PdClient, GetMembersResponse)> {
    debug!("connect to PD endpoint: {:?}", addr);
    let addr = addr.trim_left_matches("http://");
    let channel = ChannelBuilder::new(env).connect(addr);
    let client = PdClient::new(channel);
    match client.get_members(GetMembersRequest::new()) {
        Ok(resp) => Ok((client, resp)),
        Err(e) => Err(Error::Grpc(e)),
    }
}

pub fn try_connect_leader(env: Arc<Environment>,
                          previous: &GetMembersResponse)
                          -> Result<(PdClient, GetMembersResponse)> {
    // Try to connect other members.
    // Randomize endpoints.
    let members = previous.get_members();
    let mut indexes: Vec<usize> = (0..members.len()).collect();
    rand::thread_rng().shuffle(&mut indexes);

    let cluster_id = previous.get_header().get_cluster_id();
    let mut resp = None;
    'outer: for i in indexes {
        for ep in members[i].get_client_urls() {
            match connect(env.clone(), ep.as_str()) {
                Ok((_, r)) => {
                    let new_cluster_id = r.get_header().get_cluster_id();
                    if new_cluster_id == cluster_id {
                        resp = Some(r);
                        break 'outer;
                    } else {
                        panic!("{} no longer belongs to cluster {}, it is in {}",
                               ep,
                               cluster_id,
                               new_cluster_id);
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
            if let Ok((client, _)) = connect(env.clone(), ep.as_str()) {
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
