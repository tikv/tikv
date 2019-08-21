// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use tikv_util::collections::HashSet;

use futures::future::{loop_fn, ok, Loop};
use futures::sync::mpsc::UnboundedSender;
use futures::task::Task;
use futures::{task, Async, Future, Poll, Stream};
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment,
    Result as GrpcResult,
};
use kvproto::pdpb::{
    ErrorType, GetMembersRequest, GetMembersResponse, Member, PdClient, RegionHeartbeatRequest,
    RegionHeartbeatResponse, ResponseHeader,
};
use tokio_timer::timer::Handle;

use super::{Config, Error, PdFuture, Result, REQUEST_TIMEOUT};
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::{Either, HandyRwLock};

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Task>,
    pub client: PdClient,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,

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
                debug!("heartbeat receiver is refreshed");
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
    timer: Handle,
    inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client: PdClient,
        members: GetMembersResponse,
    ) -> LeaderClient {
        let (tx, rx) = client.region_heartbeat().unwrap();
        LeaderClient {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: Arc::new(RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client,
                members,
                security_mgr,
                on_reconnect: None,

                last_update: Instant::now(),
            })),
        }
    }

    pub fn handle_region_heartbeat_response<F>(&self, f: F) -> PdFuture<()>
    where
        F: Fn(RegionHeartbeatResponse) + Send + 'static,
    {
        let recv = HeartbeatReceiver {
            receiver: None,
            inner: Arc::clone(&self.inner),
        };
        Box::new(
            recv.for_each(move |resp| {
                f(resp);
                Ok(())
            })
            .map_err(|e| panic!("unexpected error: {:?}", e)),
        )
    }

    pub fn on_reconnect(&self, f: Box<dyn Fn() + Sync + Send + 'static>) {
        let mut inner = self.inner.wl();
        inner.on_reconnect = Some(f);
    }

    pub fn request<Req, Resp, F>(&self, req: Req, func: F, retry: usize) -> Request<Req, Resp, F>
    where
        Req: Clone + 'static,
        F: FnMut(&RwLock<Inner>, Req) -> PdFuture<Resp> + Send + 'static,
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            client: LeaderClient {
                timer: self.timer.clone(),
                inner: Arc::clone(&self.inner),
            },
            req,
            resp: None,
            func,
        }
    }

    pub fn get_leader(&self) -> Member {
        self.inner.rl().members.get_leader().clone()
    }

    /// Re-establishes connection with PD leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        let ((client, members), start) = {
            let inner = self.inner.rl();
            if inner.last_update.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC) {
                // Avoid unnecessary updating.
                return Ok(());
            }

            let start = Instant::now();
            (
                try_connect_leader(Arc::clone(&inner.env), &inner.security_mgr, &inner.members)?,
                start,
            )
        };

        {
            let mut inner = self.inner.wl();
            let (tx, rx) = client.region_heartbeat().unwrap();
            info!("heartbeat sender and receiver are stale, refreshing ...");

            // Try to cancel an unused heartbeat sender.
            if let Either::Left(Some(ref mut r)) = inner.hb_sender {
                debug!("cancel region heartbeat sender");
                r.cancel();
            }
            inner.hb_sender = Either::Left(Some(tx));
            if let Either::Right(ref mut task) = inner.hb_receiver {
                task.notify();
            }
            inner.hb_receiver = Either::Left(Some(rx));
            inner.client = client;
            inner.members = members;
            inner.last_update = Instant::now();
            if let Some(ref on_reconnect) = inner.on_reconnect {
                on_reconnect();
            }
        }
        warn!("updating PD client done"; "spend" => ?start.elapsed());
        Ok(())
    }
}

pub const RECONNECT_INTERVAL_SEC: u64 = 1; // 1s

/// The context of sending requets.
pub struct Request<Req, Resp, F> {
    reconnect_count: usize,
    request_sent: usize,

    client: LeaderClient,

    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 3;

impl<Req, Resp, F> Request<Req, Resp, F>
where
    Req: Clone + Send + 'static,
    Resp: Send + 'static,
    F: FnMut(&RwLock<Inner>, Req) -> PdFuture<Resp> + Send + 'static,
{
    fn reconnect_if_needed(mut self) -> Box<dyn Future<Item = Self, Error = Self> + Send> {
        debug!("reconnecting ..."; "remain" => self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return Box::new(ok(self));
        }

        // Updating client.
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match self.client.reconnect() {
            Ok(_) => {
                self.request_sent = 0;
                Box::new(ok(self))
            }
            Err(_) => Box::new(
                self.client
                    .timer
                    .delay(Instant::now() + Duration::from_secs(RECONNECT_INTERVAL_SEC))
                    .then(|_| Err(self)),
            ),
        }
    }

    fn send_and_receive(mut self) -> Box<dyn Future<Item = Self, Error = Self> + Send> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();

        Box::new(ok(self).and_then(|mut ctx| {
            let req = (ctx.func)(&ctx.client.inner, r);
            req.then(|resp| match resp {
                Ok(resp) => {
                    ctx.resp = Some(Ok(resp));
                    Ok(ctx)
                }
                Err(err) => {
                    ctx.resp = Some(Err(err));
                    Err(ctx)
                }
            })
        }))
    }

    fn break_or_continue(ctx: result::Result<Self, Self>) -> Result<Loop<Self, Self>> {
        let ctx = match ctx {
            Ok(ctx) | Err(ctx) => ctx,
        };
        let done = ctx.reconnect_count == 0
            || (ctx.resp.is_some() && Self::should_not_retry(ctx.resp.as_ref().unwrap()));
        if done {
            Ok(Loop::Break(ctx))
        } else {
            Ok(Loop::Continue(ctx))
        }
    }

    fn should_not_retry(resp: &Result<Resp>) -> bool {
        match resp {
            Ok(_) => true,
            // Error::Incompatible is returned by response header from PD, no need to retry
            Err(Error::Incompatible) => true,
            Err(err) => {
                error!("request failed, retry"; "err" => ?err);
                false
            }
        }
    }

    fn post_loop(ctx: Result<Self>) -> Result<Resp> {
        let ctx = ctx.expect("end loop with Ok(_)");
        ctx.resp
            .unwrap_or_else(|| Err(box_err!("response is empty")))
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let ctx = self;
        Box::new(
            loop_fn(ctx, |ctx| {
                ctx.reconnect_if_needed()
                    .and_then(Self::send_and_receive)
                    .then(Self::break_or_continue)
            })
            .then(Self::post_loop),
        )
    }
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &LeaderClient, retry: usize, func: F) -> Result<R>
where
    F: Fn(&PdClient) -> GrpcResult<R>,
{
    for _ in 0..retry {
        // DO NOT put any lock operation in match statement, or it will cause dead lock!
        let ret = { func(&client.inner.rl().client).map_err(Error::Grpc) };
        match ret {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!("request failed"; "err" => ?e);
                if let Err(e) = client.reconnect() {
                    error!("reconnect failed"; "err" => ?e);
                }
            }
        }
    }

    Err(box_err!("fail to request"))
}

pub fn validate_endpoints(
    env: Arc<Environment>,
    cfg: &Config,
    security_mgr: &SecurityManager,
) -> Result<(PdClient, GetMembersResponse)> {
    let len = cfg.endpoints.len();
    let mut endpoints_set = HashSet::with_capacity_and_hasher(len, Default::default());

    let mut members = None;
    let mut cluster_id = None;
    for ep in &cfg.endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("duplicate PD endpoint {}", ep));
        }

        let (_, resp) = match connect(Arc::clone(&env), security_mgr, ep) {
            Ok(resp) => resp,
            // Ignore failed PD node.
            Err(e) => {
                info!("PD failed to respond"; "endpoints" => ep, "err" => ?e);
                continue;
            }
        };

        // Check cluster ID.
        let cid = resp.get_header().get_cluster_id();
        if let Some(sample) = cluster_id {
            if sample != cid {
                return Err(box_err!(
                    "PD response cluster_id mismatch, want {}, got {}",
                    sample,
                    cid
                ));
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
            let (client, members) = try_connect_leader(Arc::clone(&env), security_mgr, &members)?;
            info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);
            Ok((client, members))
        }
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

fn connect(
    env: Arc<Environment>,
    security_mgr: &SecurityManager,
    addr: &str,
) -> Result<(PdClient, GetMembersResponse)> {
    info!("connecting to PD endpoint"; "endpoints" => addr);
    let addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let cb = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = security_mgr.connect(cb, addr);
    let client = PdClient::new(channel);
    let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
    match client.get_members_opt(&GetMembersRequest::default(), option) {
        Ok(resp) => Ok((client, resp)),
        Err(e) => Err(Error::Grpc(e)),
    }
}

pub fn try_connect_leader(
    env: Arc<Environment>,
    security_mgr: &SecurityManager,
    previous: &GetMembersResponse,
) -> Result<(PdClient, GetMembersResponse)> {
    let previous_leader = previous.get_leader();
    let members = previous.get_members();
    let cluster_id = previous.get_header().get_cluster_id();
    let mut resp = None;
    // Try to connect to other members, then the previous leader.
    'outer: for m in members
        .iter()
        .filter(|m| *m != previous_leader)
        .chain(&[previous_leader.clone()])
    {
        for ep in m.get_client_urls() {
            match connect(Arc::clone(&env), security_mgr, ep.as_str()) {
                Ok((_, r)) => {
                    let new_cluster_id = r.get_header().get_cluster_id();
                    if new_cluster_id == cluster_id {
                        resp = Some(r);
                        break 'outer;
                    } else {
                        panic!(
                            "{} no longer belongs to cluster {}, it is in {}",
                            ep, cluster_id, new_cluster_id
                        );
                    }
                }
                Err(e) => {
                    error!("connect failed"; "endpoints" => ep, "err" => ?e);
                    continue;
                }
            }
        }
    }

    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(Arc::clone(&env), security_mgr, ep.as_str()) {
                info!("connected to PD leader"; "endpoints" => ep);
                return Ok((client, resp));
            }
        }
    }

    Err(box_err!("failed to connect to {:?}", members))
}

/// Convert a PD protobuf error to an `Error`.
pub fn check_resp_header(header: &ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    let err = header.get_error();
    match err.get_type() {
        ErrorType::AlreadyBootstrapped => Err(Error::ClusterBootstrapped(header.get_cluster_id())),
        ErrorType::NotBootstrapped => Err(Error::ClusterNotBootstrapped(header.get_cluster_id())),
        ErrorType::IncompatibleVersion => Err(Error::Incompatible),
        ErrorType::StoreTombstone => Err(Error::StoreTombstone(err.get_message().to_owned())),
        ErrorType::RegionNotFound => Err(Error::RegionNotFound(vec![])),
        ErrorType::Unknown => Err(box_err!(err.get_message())),
        ErrorType::Ok => Ok(()),
    }
}
