// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc::UnboundedSender;
use futures::compat::Future01CompatExt;
use futures::executor::block_on;
use futures::future::{self, TryFutureExt};
use futures::stream::Stream;
use futures::stream::TryStreamExt;
use futures::task::Context;
use futures::task::Poll;
use futures::task::Waker;

use super::{Config, Error, FeatureGate, PdFuture, Result, REQUEST_TIMEOUT};
use collections::HashSet;
use fail::fail_point;
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment,
    Result as GrpcResult,
};
use kvproto::pdpb::{
    ErrorType, GetMembersRequest, GetMembersResponse, Member, PdClient as PdClientStub,
    RegionHeartbeatRequest, RegionHeartbeatResponse, ResponseHeader,
};
use security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::{box_err, debug, error, info, slow_log, warn};
use tikv_util::{Either, HandyRwLock};
use tokio_timer::timer::Handle;

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Waker>,
    pub client_stub: PdClientStub,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,

    last_update: Instant,
}

pub struct HeartbeatReceiver {
    receiver: Option<ClientDuplexReceiver<RegionHeartbeatResponse>>,
    inner: Arc<LeaderClient>,
}

impl Stream for HeartbeatReceiver {
    type Item = Result<RegionHeartbeatResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut receiver) = self.receiver {
                match Pin::new(receiver).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
                    Poll::Pending => return Poll::Pending,
                    // If it's None or there's error, we need to update receiver.
                    _ => {}
                }
            }

            self.receiver.take();

            let mut inner = self.inner.inner.wl();
            let mut receiver = None;
            if let Either::Left(ref mut recv) = inner.hb_receiver {
                receiver = recv.take();
            }
            if receiver.is_some() {
                debug!("heartbeat receiver is refreshed");
                drop(inner);
                self.receiver = receiver;
            } else {
                inner.hb_receiver = Either::Right(cx.waker().clone());
                return Poll::Pending;
            }
        }
    }
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    timer: Handle,
    pub(crate) inner: RwLock<Inner>,
    pub feature_gate: FeatureGate,
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

        LeaderClient {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client_stub,
                members,
                security_mgr,
                on_reconnect: None,

                last_update: Instant::now(),
            }),
            feature_gate: FeatureGate::default(),
        }
    }

    pub fn handle_region_heartbeat_response<F>(self: &Arc<Self>, f: F) -> PdFuture<()>
    where
        F: Fn(RegionHeartbeatResponse) + Send + 'static,
    {
        let recv = HeartbeatReceiver {
            receiver: None,
            inner: self.clone(),
        };
        Box::pin(
            recv.try_for_each(move |resp| {
                f(resp);
                future::ready(Ok(()))
            })
            .map_err(|e| panic!("unexpected error: {:?}", e)),
        )
    }

    pub fn on_reconnect(&self, f: Box<dyn Fn() + Sync + Send + 'static>) {
        let mut inner = self.inner.wl();
        inner.on_reconnect = Some(f);
    }

    pub fn request<Req, Resp, F>(
        self: &Arc<Self>,
        req: Req,
        func: F,
        retry: usize,
    ) -> Request<Req, F>
    where
        Req: Clone + 'static,
        F: FnMut(&LeaderClient, Req) -> PdFuture<Resp> + Send + 'static,
    {
        Request {
            reconnect_count: retry,
            request_sent: 0,
            client: self.clone(),
            req,
            func,
        }
    }

    pub fn get_leader(&self) -> Member {
        self.inner.rl().members.get_leader().clone()
    }

    /// Re-establishes connection with PD leader in asynchronized fashion.
    pub async fn reconnect(&self) -> Result<()> {
        let (future, start) = {
            let inner = self.inner.rl();
            if inner.last_update.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC) {
                // Avoid unnecessary updating.
                return Ok(());
            }

            let start = Instant::now();
            let fut = try_connect_leader(
                Arc::clone(&inner.env),
                Arc::clone(&inner.security_mgr),
                inner.members.clone(),
            );
            slow_log!(start.elapsed(), "PD client try connect leader");
            (fut, start)
        };

        let (client, members) = future.await?;
        fail_point!("leader_client_reconnect", |_| Ok(()));

        {
            let start_refresh = Instant::now();
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
            let prev_receiver = std::mem::replace(&mut inner.hb_receiver, Either::Left(Some(rx)));
            let _ = prev_receiver.right().map(|t| t.wake());
            inner.client_stub = client;
            inner.members = members;
            inner.last_update = Instant::now();
            if let Some(ref on_reconnect) = inner.on_reconnect {
                on_reconnect();
            }
            slow_log!(
                start_refresh.elapsed(),
                "PD client refresh region heartbeat",
            );
        }
        info!("updating PD client done"; "spend" => ?start.elapsed());
        Ok(())
    }
}

pub const RECONNECT_INTERVAL_SEC: u64 = 1; // 1s

/// The context of sending requets.
pub struct Request<Req, F> {
    reconnect_count: usize,
    request_sent: usize,
    client: Arc<LeaderClient>,
    req: Req,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 3;

impl<Req, Resp, F> Request<Req, F>
where
    Req: Clone + Send + 'static,
    F: FnMut(&LeaderClient, Req) -> PdFuture<Resp> + Send + 'static,
{
    async fn reconnect_if_needed(&mut self) -> bool {
        debug!("reconnecting ..."; "remain" => self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return true;
        }

        // Updating client.
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match self.client.reconnect().await {
            Ok(_) => {
                self.request_sent = 0;
                true
            }
            Err(_) => {
                let _ = self
                    .client
                    .timer
                    .delay(Instant::now() + Duration::from_secs(RECONNECT_INTERVAL_SEC))
                    .compat()
                    .await;
                false
            }
        }
    }

    async fn send_and_receive(&mut self) -> Result<Resp> {
        self.request_sent += 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();

        (self.func)(&self.client, r).await
    }

    fn should_not_retry(resp: &Result<Resp>) -> bool {
        match resp {
            Ok(_) => true,
            // Error::Incompatible is returned by response header from PD, no need to retry
            Err(Error::Incompatible) => true,
            Err(err) => {
                error!(?*err; "request failed, retry");
                false
            }
        }
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(mut self) -> PdFuture<Resp> {
        Box::pin(async move {
            while self.reconnect_count != 0 {
                if self.reconnect_if_needed().await {
                    let resp = self.send_and_receive().await;
                    if Self::should_not_retry(&resp) {
                        return resp;
                    }
                }
            }
            Err(box_err!("request retry exceeds limit"))
        })
    }
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &LeaderClient, retry: usize, func: F) -> Result<R>
where
    F: Fn(&PdClientStub) -> GrpcResult<R>,
{
    let mut err = None;
    for _ in 0..retry {
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
                if let Err(e) = block_on(client.reconnect()) {
                    error!(?e; "reconnect failed");
                }
                err = Some(e);
            }
        }
    }

    Err(err.unwrap_or_else(|| box_err!("fail to request")))
}

pub async fn validate_endpoints(
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

        let (_, resp) = match connect(Arc::clone(&env), &security_mgr, ep).await {
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
                try_connect_leader(Arc::clone(&env), security_mgr, members).await?;
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
        .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "get_members", e))
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
                        // check whether the response have leader info, otherwise continue to loop the rest members
                        if r.has_leader() {
                            resp = Some(r);
                            break 'outer;
                        }
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

    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(Arc::clone(&env), &security_mgr, ep.as_str()).await {
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
