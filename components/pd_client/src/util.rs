// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use futures::future::{loop_fn, ok, Loop};
use futures::sync::mpsc::UnboundedSender;
use futures::task::Task;
use futures::{task, Async, Future, Poll, Stream};
use futures03::compat::Future01CompatExt;
use futures03::executor::block_on;
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment,
    Result as GrpcResult,
};
use kvproto::pdpb::{
    ErrorType, GetMembersRequest, GetMembersResponse, Member, PdClient as PdClientStub,
    RegionHeartbeatRequest, RegionHeartbeatResponse, ResponseHeader,
};
use security::SecurityManager;
use tikv_util::collections::HashSet;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::{Either, HandyRwLock};
use tokio_timer::timer::Handle;

<<<<<<< HEAD
use super::{Config, Error, PdFuture, Result, REQUEST_TIMEOUT};
=======
const RETRY_INTERVAL: Duration = Duration::from_secs(1); // 1s
const MAX_RETRY_TIMES: u64 = 5;
// The max duration when retrying to connect to leader. No matter if the MAX_RETRY_TIMES is reached.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(10);
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)

// FIXME: Use a request-independent way to handle reconnection.
const GLOBAL_RECONNECT_INTERVAL: Duration = Duration::from_millis(100); // 0.1s
pub const REQUEST_RECONNECT_INTERVAL: Duration = Duration::from_secs(1); // 1s

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Task>,
    pub client_stub: PdClientStub,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,

    last_try_reconnect: Instant,
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
    pub(crate) inner: Arc<RwLock<Inner>>,
}

impl LeaderClient {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client_stub: PdClientStub,
        members: GetMembersResponse,
    ) -> LeaderClient {
        let (tx, rx) = client_stub
            .region_heartbeat()
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
<<<<<<< HEAD

        LeaderClient {
=======
        Client {
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: Arc::new(RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client_stub,
                members,
                security_mgr,
                on_reconnect: None,
<<<<<<< HEAD

                last_update: Instant::now(),
            })),
=======
                last_try_reconnect: Instant::now(),
            }),
            feature_gate: FeatureGate::default(),
            enable_forwarding,
        }
    }

    pub fn update_client(
        &self,
        client_stub: PdClientStub,
        forwarded_host: String,
        members: GetMembersResponse,
    ) {
        let start_refresh = Instant::now();
        let mut inner = self.inner.wl();

        let (tx, rx) = client_stub
            .region_heartbeat_opt(call_option(&forwarded_host))
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        info!("heartbeat sender and receiver are stale, refreshing ...");

        // Try to cancel an unused heartbeat sender.
        if let Either::Left(Some(ref mut r)) = inner.hb_sender {
            r.cancel();
        }
        inner.hb_sender = Either::Left(Some(tx));
        let prev_receiver = std::mem::replace(&mut inner.hb_receiver, Either::Left(Some(rx)));
        let _ = prev_receiver.right().map(|t| t.wake());
        inner.client_stub = client_stub;
        let prev_forwarded_host =
            std::mem::replace(&mut inner.forwarded_host, forwarded_host.clone());
        inner.members = members;
        if let Some(ref on_reconnect) = inner.on_reconnect {
            on_reconnect();
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
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
            remain_reconnect_count: retry,
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

    /// Re-establishes connection with PD leader in asynchronized fashion.
<<<<<<< HEAD
    pub async fn reconnect(&self) -> Result<()> {
        let (future, start) = {
=======
    ///
    /// If `force` is false, it will reconnect only when members change.
    /// Note: Retrying too quickly will return an error due to cancellation. Please always try to reconnect after sending the request first.
    pub async fn reconnect(&self, force: bool) -> Result<()> {
        let start = Instant::now();

        let future = {
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
            let inner = self.inner.rl();
            if start
                .checked_duration_since(inner.last_try_reconnect)
                .map_or(true, |d| d < GLOBAL_RECONNECT_INTERVAL)
            {
                // Avoid unnecessary updating.
                // Prevent a large number of reconnections in a short time.
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
<<<<<<< HEAD

            let start = Instant::now();

            (
                try_connect_leader(
                    Arc::clone(&inner.env),
                    Arc::clone(&inner.security_mgr),
                    inner.members.clone(),
                ),
                start,
            )
        };
=======
            let connector = PdConnector::new(inner.env.clone(), inner.security_mgr.clone());
            let members = inner.members.clone();
            async move {
                connector
                    .reconnect_pd(
                        members,
                        self.get_forwarded_host(),
                        force,
                        self.enable_forwarding,
                    )
                    .await
            }
        };

        {
            let mut inner = self.inner.wl();
            if start
                .checked_duration_since(inner.last_try_reconnect)
                .map_or(true, |d| d < GLOBAL_RECONNECT_INTERVAL)
            {
                // There may be multiple reconnections that pass the read lock at the same time.
                // Check again in the write lock to avoid unnecessary updating.
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
            inner.last_try_reconnect = start;
        }

        slow_log!(start.elapsed(), "try reconnect pd");
        let (client, forwarded_host, members) = match future.await? {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        fail_point!("pd_client_reconnect", |_| Ok(()));
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)

        let (client, members) = future.await?;
        fail_point!("leader_client_reconnect");

        {
            let mut inner = self.inner.wl();
            let (tx, rx) = client.region_heartbeat().unwrap_or_else(|e| {
                panic!("fail to request PD {} err {:?}", "region_heartbeat", e)
            });
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
            inner.client_stub = client;
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

/// The context of sending requets.
<<<<<<< HEAD
pub struct Request<Req, Resp, F> {
    reconnect_count: usize,
=======
pub struct Request<Req, F> {
    remain_reconnect_count: usize,
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
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
<<<<<<< HEAD
    fn reconnect_if_needed(mut self) -> Box<dyn Future<Item = Self, Error = Self> + Send> {
        debug!("reconnecting ..."; "remain" => self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return Box::new(ok(self));
=======
    async fn reconnect_if_needed(&mut self) -> Result<()> {
        debug!("reconnecting ..."; "remain" => self.remain_reconnect_count);
        if self.request_sent < MAX_REQUEST_COUNT {
            return Ok(());
        }
        if self.remain_reconnect_count == 0 {
            return Err(box_err!("request retry exceeds limit"));
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
        }
        // Updating client.
        self.remain_reconnect_count -= 1;
        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match block_on(self.client.reconnect()) {
            Ok(_) => {
                self.request_sent = 0;
<<<<<<< HEAD
                Box::new(ok(self))
=======
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
            }
            Err(_) => Box::new(
                self.client
                    .timer
<<<<<<< HEAD
                    .delay(Instant::now() + Duration::from_secs(RECONNECT_INTERVAL_SEC))
                    .then(|_| Err(self)),
            ),
=======
                    .delay(Instant::now() + REQUEST_RECONNECT_INTERVAL)
                    .compat()
                    .await;
            }
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
        }
        Ok(())
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
                error!(?err; "request failed, retry");
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
<<<<<<< HEAD
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
=======
    pub fn execute(mut self) -> PdFuture<Resp> {
        Box::pin(async move {
            loop {
                {
                    let resp = self.send_and_receive().await;
                    if Self::should_not_retry(&resp) {
                        return resp;
                    }
                }
                self.reconnect_if_needed().await?;
            }
        })
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
    }
}

/// Do a request in synchronized fashion.
<<<<<<< HEAD
pub fn sync_request<F, R>(client: &LeaderClient, retry: usize, func: F) -> Result<R>
=======
pub fn sync_request<F, R>(client: &Client, mut retry: usize, func: F) -> Result<R>
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
where
    F: Fn(&PdClientStub) -> GrpcResult<R>,
{
    loop {
        let ret = {
            // Drop the read lock immediately to prevent the deadlock between the caller thread
            // which may hold the read lock and wait for PD client thread completing the request
            // and the PD client thread which may block on acquiring the write lock.
            let client_stub = client.inner.rl().client_stub.clone();
            func(&client_stub).map_err(Error::Grpc)
        };
        match ret {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!(?e; "request failed");
<<<<<<< HEAD
                if let Err(e) = block_on(client.reconnect()) {
                    error!(?e; "reconnect failed");
                }
                err.replace(e);
=======
                if retry == 0 {
                    return Err(e);
                }
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
            }
        }
        // try reconnect
        retry -= 1;
        if let Err(e) = block_on(client.reconnect(true)) {
            error!(?e; "reconnect failed");
            thread::sleep(REQUEST_RECONNECT_INTERVAL);
        }
    }
<<<<<<< HEAD

    Err(err.unwrap_or(box_err!("fail to request")))
=======
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
}

pub fn validate_endpoints(
    env: Arc<Environment>,
    cfg: &Config,
    security_mgr: Arc<SecurityManager>,
) -> Result<(PdClientStub, GetMembersResponse)> {
    let len = cfg.endpoints.len();
    let mut endpoints_set = HashSet::with_capacity_and_hasher(len, Default::default());

    let mut members = None;
    let mut cluster_id = None;
    for ep in &cfg.endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("duplicate PD endpoint {}", ep));
        }

        let (_, resp) = match block_on(connect(Arc::clone(&env), &security_mgr, ep)) {
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
            let (client, members) =
                block_on(try_connect_leader(Arc::clone(&env), security_mgr, members))?;
            info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);
            Ok((client, members))
        }
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

async fn connect(
    env: Arc<Environment>,
    security_mgr: &SecurityManager,
    addr: &str,
) -> Result<(PdClientStub, GetMembersResponse)> {
    info!("connecting to PD endpoint"; "endpoints" => addr);
    let addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let channel = {
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));
        security_mgr.connect(cb, addr)
    };
    let client = PdClientStub::new(channel);
    let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
    let response = client
        .get_members_async_opt(&GetMembersRequest::default(), option)
        .unwrap()
        .compat()
        .await;
    match response {
        Ok(resp) => Ok((client, resp)),
        Err(e) => Err(Error::Grpc(e)),
    }
}

pub async fn try_connect_leader(
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    previous: GetMembersResponse,
) -> Result<(PdClientStub, GetMembersResponse)> {
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
            match connect(Arc::clone(&env), &security_mgr, ep.as_str()).await {
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
                    error!(?e; "connect failed"; "endpoints" => ep,);
                    continue;
                }
            }
        }
    }

<<<<<<< HEAD
    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(Arc::clone(&env), &security_mgr, ep.as_str()).await {
                info!("connected to PD leader"; "endpoints" => ep);
                return Ok((client, resp));
=======
    pub async fn reconnect_leader(&self, leader: &Member) -> Result<(Option<PdClientStub>, bool)> {
        fail_point!("connect_leader", |_| Ok((None, true)));
        let mut retry_times = MAX_RETRY_TIMES;
        let timer = Instant::now();

        // Try to connect the PD cluster leader.
        loop {
            let (res, has_network_err) = self.connect_member(leader).await?;
            match res {
                Some((client, _)) => return Ok((Some(client), has_network_err)),
                None => {
                    if has_network_err && retry_times > 0 && timer.elapsed() <= MAX_RETRY_DURATION {
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                        retry_times -= 1;
                        continue;
                    }
                    return Ok((None, has_network_err));
                }
>>>>>>> 0e6f00aeb... pd_client: prevent a large number of reconnections in a short time (#9840)
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
