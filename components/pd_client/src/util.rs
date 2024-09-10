// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc, RwLock},
    thread,
    time::Duration,
};

use collections::HashSet;
use fail::fail_point;
use futures::{
    channel::mpsc::UnboundedSender,
    compat::Future01CompatExt,
    executor::block_on,
    future::{self, TryFutureExt},
    stream::{Stream, TryStreamExt},
    task::{Context, Poll, Waker},
};
use grpcio::{
    CallOption, ChannelBuilder, ClientCStreamReceiver, ClientDuplexReceiver, ClientDuplexSender,
    Environment, Error::RpcFailure, MetadataBuilder, Result as GrpcResult, RpcStatusCode,
};
use kvproto::{
    meta_storagepb::MetaStorageClient as MetaStorageStub,
    metapb::BucketStats,
    pdpb::{
        ErrorType, GetMembersRequest, GetMembersResponse, Member, PdClient as PdClientStub,
        RegionHeartbeatRequest, RegionHeartbeatResponse, ReportBucketsRequest,
        ReportBucketsResponse, ResponseHeader,
    },
    resource_manager::{
        ResourceManagerClient as ResourceManagerStub, TokenBucketsRequest, TokenBucketsResponse,
    },
};
use security::SecurityManager;
use tikv_util::{
    box_err, debug, error, info, slow_log, time::Instant, timer::GLOBAL_TIMER_HANDLE, warn, Either,
    HandyRwLock,
};
use tokio_timer::timer::Handle;

use super::{
    metrics::*, tso::TimestampOracle, BucketMeta, Config, Error, FeatureGate, PdFuture, Result,
    REQUEST_TIMEOUT,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // 1s
const MAX_RETRY_TIMES: u64 = 5;
// The max duration when retrying to connect to leader. No matter if the
// MAX_RETRY_TIMES is reached.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(3);

// FIXME: Use a request-independent way to handle reconnection.
pub const REQUEST_RECONNECT_INTERVAL: Duration = Duration::from_secs(1); // 1s

#[derive(Clone)]
pub struct TargetInfo {
    target_url: String,
    via: String,
}

impl TargetInfo {
    pub(crate) fn new(target_url: String, via: &str) -> TargetInfo {
        TargetInfo {
            target_url,
            via: trim_http_prefix(via).to_string(),
        }
    }

    pub fn direct_connected(&self) -> bool {
        self.via.is_empty()
    }

    pub fn call_option(&self) -> CallOption {
        let opt = CallOption::default();
        if self.via.is_empty() {
            return opt;
        }

        let mut builder = MetadataBuilder::with_capacity(1);
        builder
            .add_str("pd-forwarded-host", &self.target_url)
            .unwrap();
        let metadata = builder.build();
        opt.headers(metadata)
    }
}

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Waker>,
    pub buckets_sender: Either<
        Option<ClientDuplexSender<ReportBucketsRequest>>,
        UnboundedSender<ReportBucketsRequest>,
    >,
    pub buckets_resp: Option<ClientCStreamReceiver<ReportBucketsResponse>>,
    pub client_stub: PdClientStub,
    target: TargetInfo,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,
    pub pending_heartbeat: Arc<AtomicU64>,
    pub pending_buckets: Arc<AtomicU64>,
    pub tso: TimestampOracle,
    pub meta_storage: MetaStorageStub,

    pub rg_sender: Either<
        Option<ClientDuplexSender<TokenBucketsRequest>>,
        UnboundedSender<TokenBucketsRequest>,
    >,
    pub rg_resp: Option<ClientDuplexReceiver<TokenBucketsResponse>>,

    last_try_reconnect: Instant,
    bo: ExponentialBackoff,
}

impl Inner {
    pub fn target_info(&self) -> &TargetInfo {
        &self.target
    }
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
    pub(crate) fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client_stub: PdClientStub,
        members: GetMembersResponse,
        target: TargetInfo,
        tso: TimestampOracle,
        enable_forwarding: bool,
        retry_interval: Duration,
    ) -> Client {
        if !target.direct_connected() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&target.via])
                .set(1);
        }
        let (hb_tx, hb_rx) = client_stub
            .region_heartbeat_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        let (buckets_tx, buckets_resp) = client_stub
            .report_buckets_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "report_buckets", e));
        let meta_storage =
            kvproto::meta_storagepb::MetaStorageClient::new(client_stub.client.channel().clone());
        let resource_manager = kvproto::resource_manager::ResourceManagerClient::new(
            client_stub.client.channel().clone(),
        );
        let (rg_sender, rg_rx) = resource_manager
            .acquire_token_buckets_opt(target.call_option())
            .unwrap_or_else(|e| {
                panic!("fail to request PD {} err {:?}", "acquire_token_buckets", e)
            });
        Client {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(hb_tx)),
                hb_receiver: Either::Left(Some(hb_rx)),
                buckets_sender: Either::Left(Some(buckets_tx)),
                buckets_resp: Some(buckets_resp),
                client_stub,
                members,
                target,
                security_mgr,
                on_reconnect: None,
                pending_heartbeat: Arc::default(),
                pending_buckets: Arc::default(),
                last_try_reconnect: Instant::now(),
                bo: ExponentialBackoff::new(retry_interval),
                tso,
                meta_storage,
                rg_sender: Either::Left(Some(rg_sender)),
                rg_resp: Some(rg_rx),
            }),
            feature_gate: FeatureGate::default(),
            enable_forwarding,
        }
    }

    fn update_client(
        &self,
        client_stub: PdClientStub,
        target: TargetInfo,
        members: GetMembersResponse,
        tso: TimestampOracle,
    ) {
        let start_refresh = Instant::now();
        let mut inner = self.inner.wl();

        let (hb_tx, hb_rx) = client_stub
            .region_heartbeat_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        info!("heartbeat sender and receiver are stale, refreshing ...");

        // Try to cancel an unused heartbeat sender.
        if let Either::Left(Some(ref mut r)) = inner.hb_sender {
            r.cancel();
        }
        inner.hb_sender = Either::Left(Some(hb_tx));
        let prev_receiver = std::mem::replace(&mut inner.hb_receiver, Either::Left(Some(hb_rx)));
        let _ = prev_receiver.right().map(|t| t.wake());

        let (buckets_tx, buckets_resp) = client_stub
            .report_buckets_opt(target.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_buckets", e));
        info!("buckets sender and receiver are stale, refreshing ...");
        // Try to cancel an unused buckets sender.
        if let Either::Left(Some(ref mut r)) = inner.buckets_sender {
            r.cancel();
        }
        inner.buckets_sender = Either::Left(Some(buckets_tx));
        inner.buckets_resp = Some(buckets_resp);

        inner.meta_storage = MetaStorageStub::new(client_stub.client.channel().clone());
        let resource_manager = ResourceManagerStub::new(client_stub.client.channel().clone());
        inner.client_stub = client_stub;
        inner.members = members;
        inner.tso = tso;

        let (rg_tx, rg_rx) = resource_manager
            .acquire_token_buckets_opt(target.call_option())
            .unwrap_or_else(|e| {
                panic!("fail to request PD {} err {:?}", "acquire_token_buckets", e)
            });
        info!("acquire_token_buckets sender and receiver are stale, refreshing ...");
        // Try to cancel an unused token buckets sender.
        if let Either::Left(Some(ref mut r)) = inner.rg_sender {
            r.cancel();
        }
        inner.rg_sender = Either::Left(Some(rg_tx));
        inner.rg_resp = Some(rg_rx);
        if let Some(ref on_reconnect) = inner.on_reconnect {
            on_reconnect();
        }

        if !inner.target.via.is_empty() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&inner.target.via])
                .set(0);
        }

        if !target.via.is_empty() {
            REQUEST_FORWARDED_GAUGE_VEC
                .with_label_values(&[&target.via])
                .set(1);
        }

        info!(
            "update pd client";
            "prev_leader" => &inner.target.target_url,
            "prev_via" => &inner.target.via,
            "leader" => &target.target_url,
            "via" => &target.via,
        );
        inner.target = target;
        slow_log!(
            start_refresh.saturating_elapsed(),
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
            remain_request_count: retry,
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
    ///
    /// If `force` is false, it will reconnect only when members change.
    /// Note: Retrying too quickly will return an error due to cancellation.
    /// Please always try to reconnect after sending the request first.
    pub async fn reconnect(&self, force: bool) -> Result<()> {
        PD_RECONNECT_COUNTER_VEC.try_connect.inc();
        let start = Instant::now();

        let future = {
            let inner = self.inner.rl();
            if start.saturating_duration_since(inner.last_try_reconnect) < inner.bo.get_interval() {
                // Avoid unnecessary updating.
                // Prevent a large number of reconnections in a short time.
                PD_RECONNECT_COUNTER_VEC.cancel.inc();
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
            let connector = PdConnector::new(inner.env.clone(), inner.security_mgr.clone());
            let members = inner.members.clone();
            async move {
                let direct_connected = self.inner.rl().target_info().direct_connected();
                connector
                    .reconnect_pd(
                        members,
                        direct_connected,
                        force,
                        self.enable_forwarding,
                        true,
                    )
                    .await
            }
        };

        {
            let mut inner = self.inner.wl();
            if start.saturating_duration_since(inner.last_try_reconnect) < inner.bo.get_interval() {
                // There may be multiple reconnections that pass the read lock at the same time.
                // Check again in the write lock to avoid unnecessary updating.
                PD_RECONNECT_COUNTER_VEC.cancel.inc();
                return Err(box_err!("cancel reconnection due to too small interval"));
            }
            inner.last_try_reconnect = start;
            inner.bo.next_backoff();
        }

        slow_log!(start.saturating_elapsed(), "try reconnect pd");
        let (client, target_info, members, tso) = match future.await {
            Err(e) => {
                PD_RECONNECT_COUNTER_VEC.failure.inc();
                return Err(e);
            }
            Ok(res) => {
                // Reset the retry count.
                {
                    let mut inner = self.inner.wl();
                    inner.bo.reset()
                }
                match res {
                    None => {
                        PD_RECONNECT_COUNTER_VEC.no_need.inc();
                        return Ok(());
                    }
                    Some(tuple) => {
                        PD_RECONNECT_COUNTER_VEC.success.inc();
                        tuple
                    }
                }
            }
        };

        fail_point!("pd_client_reconnect", |_| Ok(()));

        self.update_client(client, target_info, members, tso.unwrap());
        info!("trying to update PD client done"; "spend" => ?start.saturating_elapsed());
        Ok(())
    }
}

/// The context of sending request.
pub struct Request<Req, F> {
    remain_request_count: usize,
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
    async fn reconnect_if_needed(&mut self) -> Result<()> {
        debug!("reconnecting ..."; "remain" => self.remain_request_count);
        if self.request_sent < MAX_REQUEST_COUNT && self.request_sent < self.remain_request_count {
            return Ok(());
        }
        // Updating client.
        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match self.client.reconnect(true).await {
            Ok(_) => {
                self.request_sent = 0;
            }
            Err(_) => {
                let _ = self
                    .client
                    .timer
                    .delay(std::time::Instant::now() + REQUEST_RECONNECT_INTERVAL)
                    .compat()
                    .await;
            }
        }
        Ok(())
    }

    async fn send_and_receive(&mut self) -> Result<Resp> {
        if self.remain_request_count == 0 {
            return Err(box_err!("request retry exceeds limit"));
        }
        self.request_sent += 1;
        self.remain_request_count -= 1;
        debug!("request sent: {}", self.request_sent);
        let r = self.req.clone();
        (self.func)(&self.client, r).await
    }

    fn should_not_retry(&self, resp: &Result<Resp>) -> bool {
        match resp {
            Ok(_) => true,
            Err(err) => {
                // these errors are not caused by network, no need to retry
                if err.retryable() && self.remain_request_count > 0 {
                    error!(?*err; "request failed, retry");
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(mut self) -> PdFuture<Resp> {
        Box::pin(async move {
            loop {
                {
                    let resp = self.send_and_receive().await;
                    if self.should_not_retry(&resp) {
                        return resp;
                    }
                }
                self.reconnect_if_needed().await?;
            }
        })
    }
}

pub fn call_option_inner(inner: &Inner) -> CallOption {
    inner
        .target_info()
        .call_option()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT))
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &Client, mut retry: usize, func: F) -> Result<R>
where
    F: Fn(&PdClientStub, CallOption) -> GrpcResult<R>,
{
    loop {
        let ret = {
            // Drop the read lock immediately to prevent the deadlock between the caller
            // thread which may hold the read lock and wait for PD client thread
            // completing the request and the PD client thread which may block
            // on acquiring the write lock.
            let (client_stub, option) = {
                let inner = client.inner.rl();
                (inner.client_stub.clone(), call_option_inner(&inner))
            };

            func(&client_stub, option).map_err(Error::Grpc)
        };
        match ret {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => {
                error!(?e; "request failed");
                if retry == 0 {
                    return Err(e);
                }
            }
        }
        // try reconnect
        retry -= 1;
        if let Err(e) = block_on(client.reconnect(true)) {
            error!(?e; "reconnect failed");
            thread::sleep(REQUEST_RECONNECT_INTERVAL);
        }
    }
}

pub type StubTuple = (
    PdClientStub,
    TargetInfo,
    GetMembersResponse,
    // Only used by RpcClient, not by RpcClientV2.
    Option<TimestampOracle>,
);

#[derive(Clone)]
pub struct PdConnector {
    pub(crate) env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl PdConnector {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>) -> PdConnector {
        PdConnector { env, security_mgr }
    }

    pub async fn validate_endpoints(&self, cfg: &Config, build_tso: bool) -> Result<StubTuple> {
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
                let res = self
                    .reconnect_pd(members, true, true, cfg.enable_forwarding, build_tso)
                    .await?
                    .unwrap();
                info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);
                Ok(res)
            }
            _ => Err(box_err!("PD cluster failed to respond")),
        }
    }

    pub async fn connect(&self, addr: &str) -> Result<(PdClientStub, GetMembersResponse)> {
        info!("connecting to PD endpoint"; "endpoints" => addr);
        let addr_trim = trim_http_prefix(addr);
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .max_send_message_len(-1)
                .max_receive_message_len(-1)
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3))
                .max_reconnect_backoff(Duration::from_secs(5))
                .initial_reconnect_backoff(Duration::from_secs(1));
            self.security_mgr.connect(cb, addr_trim)
        };
        fail_point!("cluster_id_is_not_ready", |_| {
            Ok((
                PdClientStub::new(channel.clone()),
                GetMembersResponse::default(),
            ))
        });
        let client = PdClientStub::new(channel.clone());
        let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
        let timer = Instant::now();
        let response = client
            .get_members_async_opt(&GetMembersRequest::default(), option)
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "get_members", e))
            .await;
        PD_REQUEST_HISTOGRAM_VEC
            .get_members
            .observe(timer.saturating_elapsed_secs());
        match response {
            Ok(resp) => Ok((client, resp)),
            Err(e) => Err(Error::Grpc(e)),
        }
    }

    // load_members returns the PD members by calling getMember, there are two
    // abnormal scenes for the reponse:
    // 1. header has an error: the PD is not ready to serve.
    // 2. cluster id is zero: etcd start server but the follower did not get
    // cluster id yet.
    // In this case, load_members should return an error, so the client
    // will not update client address.
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
                        let header = r.get_header();
                        // Try next follower endpoint if the cluster has not ready since this pr:
                        // pd#5412.
                        if let Err(e) = check_resp_header(header) {
                            error!("connect pd failed";"endpoints" => ep, "error" => ?e);
                        } else {
                            let new_cluster_id = header.get_cluster_id();
                            // it is new cluster if the new cluster id is zero.
                            if cluster_id == 0 || new_cluster_id == cluster_id {
                                // check whether the response have leader info, otherwise continue
                                // to loop the rest members
                                if r.has_leader() {
                                    return Ok(r);
                                }
                            // Try next endpoint if PD server returns the
                            // cluster id is zero without any error.
                            } else if new_cluster_id == 0 {
                                error!("{} connect success, but cluster id is not ready", ep);
                            } else {
                                panic!(
                                    "{} no longer belongs to cluster {}, it is in {}",
                                    ep, cluster_id, new_cluster_id
                                );
                            }
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
    // 1. the force is true which represents the client is newly created or the
    // original connection has some problem 2. the previous forwarded host is
    // not empty and it can connect the leader now which represents the network
    // partition problem to leader may be recovered 3. the member information of
    // PD has been changed
    pub async fn reconnect_pd(
        &self,
        members_resp: GetMembersResponse,
        direct_connected: bool,
        force: bool,
        enable_forwarding: bool,
        build_tso: bool,
    ) -> Result<Option<StubTuple>> {
        let resp = self.load_members(&members_resp).await?;
        let leader = resp.get_leader();
        let members = resp.get_members();
        // Currently we connect to leader directly and there is no member change.
        // We don't need to connect to PD again.
        if !force && direct_connected && resp == members_resp {
            return Ok(None);
        }
        let (res, has_network_error) = self.reconnect_leader(leader).await?;
        match res {
            Some((client, target_url)) => {
                let info = TargetInfo::new(target_url, "");
                let tso = if build_tso {
                    Some(TimestampOracle::new(
                        resp.get_header().get_cluster_id(),
                        &client,
                        info.call_option(),
                    )?)
                } else {
                    None
                };
                return Ok(Some((client, info, resp, tso)));
            }
            None => {
                // If the force is false, we could have already forwarded the requests.
                // We don't need to try forwarding again.
                if !force && resp == members_resp {
                    return Err(box_err!("failed to connect to {:?}", leader));
                }
                if enable_forwarding && has_network_error {
                    if let Ok(Some((client, info))) = self.try_forward(members, leader).await {
                        let tso = if build_tso {
                            Some(TimestampOracle::new(
                                resp.get_header().get_cluster_id(),
                                &client,
                                info.call_option(),
                            )?)
                        } else {
                            None
                        };
                        return Ok(Some((client, info, resp, tso)));
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
    ) -> Result<(Option<(PdClientStub, String, GetMembersResponse)>, bool)> {
        let mut network_fail_num = 0;
        let mut has_network_error = false;
        let client_urls = peer.get_client_urls();
        for ep in client_urls {
            match self.connect(ep.as_str()).await {
                Ok((client, resp)) => {
                    info!("connected to PD member"; "endpoints" => ep);
                    return Ok((Some((client, ep.clone(), resp)), false));
                }
                Err(Error::Grpc(e)) => {
                    if let RpcFailure(ref status) = e {
                        if status.code() == RpcStatusCode::UNAVAILABLE
                            || status.code() == RpcStatusCode::DEADLINE_EXCEEDED
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

    async fn reconnect_leader(
        &self,
        leader: &Member,
    ) -> Result<(Option<(PdClientStub, String)>, bool)> {
        fail_point!("connect_leader", |_| Ok((None, true)));
        let mut retry_times = MAX_RETRY_TIMES;
        let timer = Instant::now();
        // Try to connect the PD cluster leader.
        loop {
            let (res, has_network_err) = self.connect_member(leader).await?;
            match res {
                Some((client, ep, _)) => {
                    return Ok((Some((client, ep)), has_network_err));
                }
                None => {
                    if has_network_err
                        && retry_times > 0
                        && timer.saturating_elapsed() <= MAX_RETRY_DURATION
                    {
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
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
    ) -> Result<Option<(PdClientStub, TargetInfo)>> {
        // Try to connect the PD cluster follower.
        for m in members.iter().filter(|m| *m != leader) {
            let (res, _) = self.connect_member(m).await?;
            match res {
                Some((client, ep, resp)) => {
                    let leader = resp.get_leader();
                    let client_urls = leader.get_client_urls();
                    for leader_url in client_urls {
                        let target = TargetInfo::new(leader_url.clone(), &ep);
                        let timer = Instant::now();
                        let response = client
                            .get_members_async_opt(
                                &GetMembersRequest::default(),
                                target
                                    .call_option()
                                    .timeout(Duration::from_secs(REQUEST_TIMEOUT)),
                            )
                            .unwrap_or_else(|e| {
                                panic!("fail to request PD {} err {:?}", "get_members", e)
                            })
                            .await;
                        PD_REQUEST_HISTOGRAM_VEC
                            .get_members
                            .observe(timer.saturating_elapsed_secs());
                        match response {
                            Ok(_) => return Ok(Some((client, target))),
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

/// Simple backoff strategy.
struct ExponentialBackoff {
    base: Duration,
    interval: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration) -> Self {
        Self {
            base,
            interval: base,
        }
    }
    pub fn next_backoff(&mut self) -> Duration {
        self.interval = std::cmp::min(self.interval * 2, MAX_BACKOFF);
        self.interval
    }

    pub fn get_interval(&self) -> Duration {
        self.interval
    }

    pub fn reset(&mut self) {
        self.interval = self.base;
    }
}

pub fn trim_http_prefix(s: &str) -> &str {
    s.trim_start_matches("http://")
        .trim_start_matches("https://")
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
        ErrorType::DataCompacted => Err(Error::DataCompacted(err.get_message().to_owned())),
        ErrorType::Ok => Ok(()),
        ErrorType::DuplicatedEntry | ErrorType::EntryNotFound => Err(box_err!(err.get_message())),
        ErrorType::Unknown => Err(box_err!(err.get_message())),
        ErrorType::InvalidValue => Err(box_err!(err.get_message())),
        ErrorType::GlobalConfigNotFound => panic!("unexpected error {:?}", err),
        _ => unreachable!(),
    }
}

pub fn new_bucket_stats(meta: &BucketMeta) -> BucketStats {
    let count = meta.keys.len() - 1;
    let mut stats = BucketStats::default();
    stats.set_write_bytes(vec![0; count]);
    stats.set_read_bytes(vec![0; count]);
    stats.set_write_qps(vec![0; count]);
    stats.set_read_qps(vec![0; count]);
    stats.set_write_keys(vec![0; count]);
    stats.set_read_keys(vec![0; count]);
    stats
}

pub fn find_bucket_index<S: AsRef<[u8]>>(key: &[u8], bucket_keys: &[S]) -> Option<usize> {
    let last_key = bucket_keys.last().unwrap().as_ref();
    let search_keys = &bucket_keys[..bucket_keys.len() - 1];
    search_keys
        .binary_search_by(|k| k.as_ref().cmp(key))
        .map_or_else(
            |idx| {
                if idx == 0 || (idx == search_keys.len() && !last_key.is_empty() && key >= last_key)
                {
                    None
                } else {
                    Some(idx - 1)
                }
            },
            Some,
        )
}

/// Merge incoming bucket stats. If a range in new buckets overlaps with
/// multiple ranges in current buckets, stats of the new range will be added to
/// all stats of current ranges.
pub fn merge_bucket_stats<C: AsRef<[u8]>, I: AsRef<[u8]>>(
    cur: &[C],
    cur_stats: &mut BucketStats,
    incoming: &[I],
    delta_stats: &BucketStats,
) {
    // Return [start, end] of indices of buckets
    fn find_overlay_ranges<S: AsRef<[u8]>>(
        range: (&[u8], &[u8]),
        keys: &[S],
    ) -> Option<(usize, usize)> {
        let bucket_cnt = keys.len() - 1;
        let last_bucket_idx = bucket_cnt - 1;
        let start = match find_bucket_index(range.0, keys) {
            Some(idx) => idx,
            None => {
                if range.0 < keys[0].as_ref() {
                    0
                } else {
                    // Not in the bucket range.
                    return None;
                }
            }
        };

        let end = if range.1.is_empty() {
            last_bucket_idx
        } else {
            match find_bucket_index(range.1, keys) {
                Some(idx) => {
                    // If end key is the start key of a bucket, this bucket should not be included.
                    if range.1 == keys[idx].as_ref() {
                        if idx == 0 {
                            return None;
                        }
                        idx - 1
                    } else {
                        idx
                    }
                }
                None => {
                    if range.1 >= keys[keys.len() - 1].as_ref() {
                        last_bucket_idx
                    } else {
                        // Not in the bucket range.
                        return None;
                    }
                }
            }
        };
        Some((start, end))
    }

    macro_rules! stats_add {
        ($right:ident, $ridx:expr, $left:ident, $lidx:expr, $member:ident) => {
            if let Some(s) = $right.$member.get_mut($ridx) {
                *s += $left.$member.get($lidx).copied().unwrap_or_default();
            }
        };
    }

    for new_idx in 0..(incoming.len() - 1) {
        let start = &incoming[new_idx];
        let end = &incoming[new_idx + 1];
        if let Some((start_idx, end_idx)) = find_overlay_ranges((start.as_ref(), end.as_ref()), cur)
        {
            for cur_idx in start_idx..=end_idx {
                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_bytes);
                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_bytes);

                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_qps);
                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_qps);

                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, read_keys);
                stats_add!(cur_stats, cur_idx, delta_stats, new_idx, write_keys);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use kvproto::metapb::BucketStats;

    use super::*;
    use crate::{merge_bucket_stats, util::find_bucket_index};

    const BASE_BACKOFF: Duration = Duration::from_millis(100);

    #[test]
    fn test_merge_bucket_stats() {
        #[allow(clippy::type_complexity)]
        let cases: &[((Vec<&[u8]>, _), (Vec<&[u8]>, _), _)] = &[
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                vec![2, 2, 2, 2],
            ),
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                (vec![b"k0", b"k6", b"k8"], vec![1, 1]),
                vec![2, 2, 3, 2],
            ),
            (
                (vec![b"k0", b"k6", b"k8"], vec![1, 1]),
                (
                    vec![b"k1", b"k3", b"k5", b"k7", b"k9", b"ka"],
                    vec![1, 1, 1, 1, 1],
                ),
                vec![4, 3],
            ),
            (
                (vec![b"k4", b"k6", b"kb"], vec![1, 1]),
                (
                    vec![b"k1", b"k3", b"k5", b"k7", b"k9", b"ka"],
                    vec![1, 1, 1, 1, 1],
                ),
                vec![3, 4],
            ),
            (
                (vec![b"k3", b"k5", b"k7"], vec![1, 1]),
                (vec![b"k4", b"k5"], vec![1]),
                vec![2, 1],
            ),
            (
                (vec![b"", b""], vec![1]),
                (vec![b"", b""], vec![1]),
                vec![2],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"", b"k2", b""], vec![1, 1]),
                vec![2, 3],
            ),
            (
                (vec![b"", b""], vec![1]),
                (vec![b"", b"k1", b""], vec![1, 1]),
                vec![3],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"", b""], vec![1]),
                vec![2, 2],
            ),
            (
                (vec![b"", b"k1", b""], vec![1, 1]),
                (vec![b"k2", b"k3"], vec![1]),
                vec![1, 2],
            ),
            (
                (vec![b"", b"k3", b""], vec![1, 1]),
                (vec![b"k1", b"k2"], vec![1]),
                vec![2, 1],
            ),
            (
                (vec![b"", b"k3"], vec![1]),
                (vec![b"k1", b""], vec![1]),
                vec![2],
            ),
            (
                (vec![b"", b"k3"], vec![1]),
                (vec![b"k4", b""], vec![1]),
                vec![1],
            ),
        ];
        for (current, incoming, expected) in cases {
            let cur_keys = &current.0;
            let incoming_keys = &incoming.0;
            let mut cur_stats = BucketStats::default();
            cur_stats.set_read_qps(current.1.to_vec());
            let mut incoming_stats = BucketStats::default();
            incoming_stats.set_read_qps(incoming.1.to_vec());
            merge_bucket_stats(cur_keys, &mut cur_stats, incoming_keys, &incoming_stats);
            assert_eq!(cur_stats.get_read_qps(), expected);
        }
    }

    #[test]
    fn test_find_bucket_index() {
        let keys = vec![
            b"k1".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec(),
            b"k7".to_vec(),
        ];
        assert_eq!(find_bucket_index(b"k1", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k5", &keys), Some(2));
        assert_eq!(find_bucket_index(b"k2", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k6", &keys), Some(2));
        assert_eq!(find_bucket_index(b"k7", &keys), None);
        assert_eq!(find_bucket_index(b"k0", &keys), None);
        assert_eq!(find_bucket_index(b"k8", &keys), None);
        let keys = vec![
            b"".to_vec(),
            b"k1".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec(),
            b"k7".to_vec(),
            b"".to_vec(),
        ];
        assert_eq!(find_bucket_index(b"k0", &keys), Some(0));
        assert_eq!(find_bucket_index(b"k7", &keys), Some(4));
        assert_eq!(find_bucket_index(b"k8", &keys), Some(4));
    }

    #[test]
    fn test_exponential_backoff() {
        let mut backoff = ExponentialBackoff::new(BASE_BACKOFF);
        assert_eq!(backoff.get_interval(), BASE_BACKOFF);

        assert_eq!(backoff.next_backoff(), 2 * BASE_BACKOFF);
        assert_eq!(backoff.next_backoff(), Duration::from_millis(400));
        assert_eq!(backoff.get_interval(), Duration::from_millis(400));

        // Should not exceed MAX_BACKOFF
        for _ in 0..20 {
            backoff.next_backoff();
        }
        assert_eq!(backoff.get_interval(), MAX_BACKOFF);

        backoff.reset();
        assert_eq!(backoff.get_interval(), BASE_BACKOFF);
    }
}
