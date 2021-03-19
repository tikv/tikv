// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
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
    Error::RpcFailure, Metadata, MetadataBuilder, Result as GrpcResult, RpcStatus, RpcStatusCode,
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

const RETRY_INTERVAL_SEC: u64 = 1; // 1s
const MAX_RETRY_TIMES: u64 = 5;
// The max duration when retrying to connect to leader. No matter if the MAX_RETRY_TIMES is reached.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(10);

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Waker>,
    pub client_stub: PdClientStub,
    pub forwarded_host: String,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,

    last_update: Instant,
    last_try_reconnect: Instant,
}

pub struct HeartbeatReceiver {
    receiver: Option<ClientDuplexReceiver<RegionHeartbeatResponse>>,
    inner: Arc<Client>,
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
pub struct Client {
    timer: Handle,
    pub(crate) inner: RwLock<Inner>,
    pub feature_gate: FeatureGate,
    enable_forwarding: bool,
}

impl Client {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client_stub: PdClientStub,
        members: GetMembersResponse,
        forwarded_host: String,
        enable_forwarding: bool,
    ) -> Client {
        let (tx, rx) = client_stub
            .region_heartbeat_opt(call_option(&forwarded_host))
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        let now = Instant::now();
        Client {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client_stub,
                members,
                forwarded_host,
                security_mgr,
                on_reconnect: None,
                last_update: now,
                last_try_reconnect: now,
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
        inner.last_update = Instant::now();
        if let Some(ref on_reconnect) = inner.on_reconnect {
            on_reconnect();
        }
        info!("update pd client"; "prev_forwarded_host"=>prev_forwarded_host,"forworded_host"=>forwarded_host);
        slow_log!(
            start_refresh.elapsed(),
            "PD client refresh region heartbeat",
        );
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
        F: FnMut(&Client, Req) -> PdFuture<Resp> + Send + 'static,
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

    pub fn get_forwarded_host(&self) -> String {
        self.inner.rl().forwarded_host.clone()
    }

    /// Re-establishes connection with PD leader in asynchronized fashion.
    ///
    /// If `force` is false, it will reconnect only when members change.
    pub async fn reconnect(&self, force: bool) -> Result<()> {
        let start = Instant::now();

        let future = {
            let inner = self.inner.rl();
            if inner.last_update.elapsed() < RECONNECT_UPDATE_INTERVAL {
                // Avoid unnecessary updating.
                return Ok(());
            }
            if inner.last_try_reconnect.elapsed() < GLOBAL_RECONNECT_INTERVAL {
                // Prevent a large number of reconnections in a short time.
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
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
            slow_log!(start.elapsed(), "PD client try connect leader");
            let mut inner = self.inner.wl();
            if start > inner.last_try_reconnect {
                inner.last_try_reconnect = start;
            }
        }

        let (client, forwarded_host, members) = match future.await? {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        fail_point!("pd_client_reconnect", |_| Ok(()));

        self.update_client(client, forwarded_host, members);
        info!("tring to update PD client done"; "spend" => ?start.elapsed());
        Ok(())
    }
}

// The call option here doesn't need the timeout setting which is used by stream.
fn call_option(forwarded_host: &str) -> CallOption {
    if !forwarded_host.is_empty() {
        let metadata = build_forward_metadata(forwarded_host);
        return CallOption::default().headers(metadata);
    }
    CallOption::default()
}

pub fn build_forward_metadata(forwarded_host: &str) -> Metadata {
    let mut builder = MetadataBuilder::with_capacity(1);
    builder
        .add_str("pd-forwarded-host", forwarded_host)
        .unwrap();
    builder.build()
}

pub const REQUEST_RECONNECT_INTERVAL: Duration = Duration::from_millis(1000); // 1s
pub const GLOBAL_RECONNECT_INTERVAL: Duration = Duration::from_millis(100); // 0.1s
pub const RECONNECT_UPDATE_INTERVAL: Duration = Duration::from_millis(1500); // 1.5s, need to be greater than REQUEST_RECONNECT_INTERVAL

/// The context of sending requets.
pub struct Request<Req, F> {
    reconnect_count: usize,
    request_sent: usize,
    client: Arc<Client>,
    req: Req,
    func: F,
}

const MAX_REQUEST_COUNT: usize = 3;

impl<Req, Resp, F> Request<Req, F>
where
    Req: Clone + Send + 'static,
    F: FnMut(&Client, Req) -> PdFuture<Resp> + Send + 'static,
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
        match self.client.reconnect(true).await {
            Ok(_) => {
                self.request_sent = 0;
                true
            }
            Err(_) => {
                let _ = self
                    .client
                    .timer
                    .delay(Instant::now() + REQUEST_RECONNECT_INTERVAL)
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
pub fn sync_request<F, R>(client: &Client, mut retry: usize, func: F) -> Result<R>
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
                // try reconnect
                loop {
                    if retry == 0 {
                        return Err(e);
                    }
                    retry -= 1;
                    match block_on(client.reconnect(true)) {
                        Ok(()) => break,
                        Err(e) => {
                            error!(?e; "reconnect failed");
                            if retry > 0 {
                                // If the reconnection fails, and it needs to be retried.
                                thread::sleep(REQUEST_RECONNECT_INTERVAL);
                            }
                        }
                    }
                }
            }
        }
    }
}

pub type StubTuple = (PdClientStub, String, GetMembersResponse);

pub struct PdConnector {
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl PdConnector {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>) -> PdConnector {
        PdConnector { env, security_mgr }
    }

    pub async fn validate_endpoints(&self, cfg: &Config) -> Result<StubTuple> {
        let len = cfg.endpoints.len();
        let mut endpoints_set = HashSet::with_capacity_and_hasher(len, Default::default());
        let mut members = None;
        let mut cluster_id = None;
        for ep in &cfg.endpoints {
            if !endpoints_set.insert(ep) {
                return Err(box_err!("duplicate PD endpoint {}", ep));
            }

            let (_, resp) = match self.connect(ep).await {
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
                let (client, forwarded_host, resp) = self
                    .reconnect_pd(members, "".to_string(), true, cfg.enable_forwarding)
                    .await?
                    .unwrap();
                info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);
                Ok((client, forwarded_host, resp))
            }
            _ => Err(box_err!("PD cluster failed to respond")),
        }
    }

    pub async fn connect(&self, addr: &str) -> Result<(PdClientStub, GetMembersResponse)> {
        info!("connecting to PD endpoint"; "endpoints" => addr);
        let addr_trim = addr
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            self.security_mgr.connect(cb, addr_trim)
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

    pub async fn load_members(&self, previous: &GetMembersResponse) -> Result<GetMembersResponse> {
        let previous_leader = previous.get_leader();
        let members = previous.get_members();
        let cluster_id = previous.get_header().get_cluster_id();

        // Try to connect to other members, then the previous leader.
        for m in members
            .iter()
            .filter(|m| *m != previous_leader)
            .chain(&[previous_leader.clone()])
        {
            for ep in m.get_client_urls() {
                match self.connect(ep.as_str()).await {
                    Ok((_, r)) => {
                        let new_cluster_id = r.get_header().get_cluster_id();
                        if new_cluster_id == cluster_id {
                            // check whether the response have leader info, otherwise continue to loop the rest members
                            if r.has_leader() {
                                return Ok(r);
                            }
                        } else {
                            panic!(
                                "{} no longer belongs to cluster {}, it is in {}",
                                ep, cluster_id, new_cluster_id
                            );
                        }
                    }
                    Err(e) => {
                        error!("connect failed"; "endpoints" => ep, "error" => ?e);
                        continue;
                    }
                }
            }
        }
        Err(box_err!(
            "failed to connect to {:?}",
            previous.get_members()
        ))
    }

    // There are 3 kinds of situations we will return the new client:
    // 1. the force is true which represents the client is newly created or the original connection has some problem
    // 2. the previous forwared host is not empty and it can connect the leader now which represents the network partition problem to leader may be recovered
    // 3. the member information of PD has been changed
    async fn reconnect_pd(
        &self,
        members_resp: GetMembersResponse,
        pre_forwarded_host: String,
        force: bool,
        enable_forwarding: bool,
    ) -> Result<Option<StubTuple>> {
        let resp = self.load_members(&members_resp).await?;
        let leader = resp.get_leader();
        let members = resp.get_members();
        // Currently we connect to leader directly and there is no member change.
        // We don't need to connect to PD again.
        if !force && pre_forwarded_host.is_empty() && resp == members_resp {
            return Ok(None);
        }
        let (res, has_network_error) = self.reconnect_leader(leader).await?;
        match res {
            Some(client) => {
                return Ok(Some((client, "".to_string(), resp)));
            }
            None => {
                // If the force is false, we could have already forwarded the requests.
                // We don't need to try forwarding again.
                if !force && resp == members_resp {
                    return Err(box_err!("failed to connect to {:?}", leader));
                }
                if enable_forwarding && has_network_error {
                    if let Ok(Some((client, forwarded_host))) =
                        self.try_forward(members, leader).await
                    {
                        return Ok(Some((client, forwarded_host, resp)));
                    }
                }
            }
        }
        Err(box_err!(
            "failed to connect to {:?}",
            members_resp.get_members()
        ))
    }

    pub async fn connect_member(
        &self,
        peer: &Member,
    ) -> Result<(Option<(PdClientStub, GetMembersResponse)>, bool)> {
        let mut network_fail_num = 0;
        let mut has_network_error = false;
        let client_urls = peer.get_client_urls();
        for ep in client_urls {
            match self.connect(ep.as_str()).await {
                Ok((client, resp)) => {
                    info!("connected to PD member"; "endpoints" => ep);
                    return Ok((Some((client, resp)), false));
                }
                Err(Error::Grpc(e)) => {
                    if let RpcFailure(RpcStatus { status, details: _ }) = e {
                        if status == RpcStatusCode::UNAVAILABLE
                            || status == RpcStatusCode::DEADLINE_EXCEEDED
                        {
                            network_fail_num += 1;
                        }
                    }
                    error!("failed to connect to PD member"; "endpoints" => ep, "error" => ?e);
                }
                _ => unreachable!(),
            }
        }
        let url_num = client_urls.len();
        if url_num != 0 && url_num == network_fail_num {
            has_network_error = true;
        }
        Ok((None, has_network_error))
    }

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
                            .delay(Instant::now() + Duration::from_secs(RETRY_INTERVAL_SEC))
                            .compat()
                            .await;
                        retry_times -= 1;
                        continue;
                    }
                    return Ok((None, has_network_err));
                }
            }
        }
    }

    pub async fn try_forward(
        &self,
        members: &[Member],
        leader: &Member,
    ) -> Result<Option<(PdClientStub, String)>> {
        // Try to connect the PD cluster follower.
        for m in members.iter().filter(|m| *m != leader) {
            let (res, _) = self.connect_member(m).await?;
            match res {
                Some((client, resp)) => {
                    let leader = resp.get_leader();
                    let client_urls = leader.get_client_urls();
                    for leader_url in client_urls {
                        let response = client
                            .get_members_async_opt(
                                &GetMembersRequest::default(),
                                call_option(&leader_url)
                                    .timeout(Duration::from_secs(REQUEST_TIMEOUT)),
                            )
                            .unwrap_or_else(|e| {
                                panic!("fail to request PD {} err {:?}", "get_members", e)
                            })
                            .await;
                        match response {
                            Ok(_) => return Ok(Some((client, leader_url.to_string()))),
                            Err(_) => continue,
                        }
                    }
                }
                _ => continue,
            }
        }
        Err(box_err!("failed to connect to followers"))
    }
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
