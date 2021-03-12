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
use rand::Rng;

use super::{Config, Error, FeatureGate, PdFuture, Result, REQUEST_TIMEOUT};
use collections::HashSet;
use fail::fail_point;
use grpcio::{
    CallOption, ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, Environment,
    Error::RpcFailure, MetadataBuilder, Result as GrpcResult, RpcStatus, RpcStatusCode,
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

pub struct Inner {
    env: Arc<Environment>,
    pub hb_sender: Either<
        Option<ClientDuplexSender<RegionHeartbeatRequest>>,
        UnboundedSender<RegionHeartbeatRequest>,
    >,
    pub hb_receiver: Either<Option<ClientDuplexReceiver<RegionHeartbeatResponse>>, Waker>,
    pub client_stub: PdClientStub,
    address: String,
    members: GetMembersResponse,
    security_mgr: Arc<SecurityManager>,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,

    last_update: Instant,
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
}

impl Client {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        client_stub: PdClientStub,
        members: GetMembersResponse,
        address: String,
    ) -> Client {
        let (tx, rx) = client_stub
            .region_heartbeat()
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));

        Client {
            timer: GLOBAL_TIMER_HANDLE.clone(),
            inner: RwLock::new(Inner {
                env,
                hb_sender: Either::Left(Some(tx)),
                hb_receiver: Either::Left(Some(rx)),
                client_stub,
                members,
                address,
                security_mgr,
                on_reconnect: None,
                last_update: Instant::now(),
            }),
            feature_gate: FeatureGate::default(),
        }
    }

    pub fn update_client(
        &self,
        client_stub: PdClientStub,
        address: String,
        members_resp: Option<GetMembersResponse>,
    ) {
        let start_refresh = Instant::now();
        let mut inner = self.inner.wl();

        let members = if let Some(members) = members_resp {
            members
        } else {
            inner.members.clone()
        };

        let target_addr = get_random_member_addr(members.get_leader());

        let mut builder = MetadataBuilder::with_capacity(1);
        builder.add_str("pd-forwarded-host", &target_addr).unwrap();
        let metadata = builder.build();
        let call_option = CallOption::default().headers(metadata);

        let (tx, rx) = client_stub
            .region_heartbeat_opt(call_option)
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
        inner.address = address.clone();
        inner.members = members;
        inner.last_update = Instant::now();
        if let Some(ref on_reconnect) = inner.on_reconnect {
            on_reconnect();
        }
        info!("change pd client stub"; "from"=> &inner.address, "to" => address);
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

    /// Only used for test purpose.
    pub fn get_address(&self) -> String {
        self.inner.rl().address.clone()
    }

    /// Re-establishes connection with PD leader in asynchronized fashion.
    ///
    /// If `force` is false, it will reconnect only when members change.
    pub async fn reconnect(&self, force: bool, try_follower: bool) -> Result<()> {
        let (future, start) = {
            let inner = self.inner.rl();
            if inner.last_update.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC) {
                // Avoid unnecessary updating.
                return Ok(());
            }

            let start = Instant::now();
            let connector = PdConnector::new(inner.env.clone(), inner.security_mgr.clone());
            let members = inner.members.clone();
            let fut = self.reconnect_pd(connector, members, force, try_follower);
            slow_log!(start.elapsed(), "try reconnect pd");
            (fut, start)
        };

        let (client, address, members) = match future.await? {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        fail_point!("pd_client_reconnect", |_| Ok(()));

        self.update_client(client, address, Some(members));
        info!("tring to update PD client done"; "spend" => ?start.elapsed());
        Ok(())
    }

    async fn reconnect_pd(
        &self,
        connector: PdConnector,
        members_resp: GetMembersResponse,
        force: bool,
        try_follower: bool,
    ) -> Result<Option<StubTuple>> {
        let resp = connector.load_members(&members_resp).await?;
        if !force && resp == members_resp {
            return Ok(None);
        }
        let leader = resp.get_leader();
        let members = resp.get_members();
        if let Ok(Some((client, address))) = connector.reconnect_leader(leader).await {
            return Ok(Some((client, address, resp)));
        }
        if try_follower {
            if let Ok(Some((client, address))) =
                connector.reconnect_followers(members, leader).await
            {
                return Ok(Some((client, address, resp)));
            }
        }
        Err(box_err!(
            "failed to connect to {:?}",
            members_resp.get_members()
        ))
    }
}

pub const RECONNECT_INTERVAL_SEC: u64 = 1; // 1s

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
    async fn reconnect_if_needed(&mut self, try_follower: bool) -> bool {
        debug!("reconnecting ..."; "remain" => self.reconnect_count);

        if self.request_sent < MAX_REQUEST_COUNT {
            return true;
        }

        // Updating client.
        self.reconnect_count -= 1;

        // FIXME: should not block the core.
        debug!("(re)connecting PD client");
        match self.client.reconnect(true, try_follower).await {
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

    fn should_not_retry(resp: &Result<Resp>) -> (bool, bool) {
        match resp {
            Ok(_) => (true, false),
            // Error::Incompatible is returned by response header from PD, no need to retry
            Err(Error::Incompatible) => (true, false),
            Err(Error::Grpc(err)) => match err {
                RpcFailure(RpcStatus { status, details: _ }) => {
                    if *status == RpcStatusCode::UNAVAILABLE {
                        (false, true)
                    } else {
                        (false, false)
                    }
                }
                _ => (false, false),
            },
            Err(err) => {
                error!(?*err; "request failed, retry");
                (false, false)
            }
        }
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(mut self) -> PdFuture<Resp> {
        Box::pin(async move {
            let mut try_follower = false;
            while self.reconnect_count != 0 {
                if self.reconnect_if_needed(try_follower).await {
                    let resp = self.send_and_receive().await;
                    let (not_retry, network_err) = Self::should_not_retry(&resp);
                    if not_retry {
                        return resp;
                    }
                    if network_err {
                        try_follower = true
                    }
                }
            }
            Err(box_err!("request retry exceeds limit"))
        })
    }
}

/// Do a request in synchronized fashion.
pub fn sync_request<F, R>(client: &Client, retry: usize, func: F) -> Result<R>
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
                if let Err(e) = block_on(client.reconnect(true, false)) {
                    error!(?e; "reconnect failed");
                }
                err = Some(e);
            }
        }
    }

    Err(err.unwrap_or_else(|| box_err!("fail to request")))
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

            let (_, _, resp) = match self.connect(ep).await {
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
                let resp = self.load_members(&members).await?;
                let (client, address) = self.reconnect_leader(resp.get_leader()).await?.unwrap();
                info!("all PD endpoints are consistent"; "endpoints" => ?cfg.endpoints);
                Ok((client, address, resp))
            }
            _ => Err(box_err!("PD cluster failed to respond")),
        }
    }

    pub async fn connect(&self, addr: &str) -> Result<StubTuple> {
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
            Ok(resp) => Ok((client, addr.to_string(), resp)),
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
                    Ok((_, _, r)) => {
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

    pub async fn connect_member(
        &self,
        peer: &Member,
        retry: bool,
    ) -> Option<(PdClientStub, String)> {
        for ep in peer.get_client_urls() {
            if let Ok((client, address, _)) = self.connect(ep.as_str()).await {
                info!("connected to PD member"; "endpoints" => ep);
                return Some((client, address));
            }
            if retry {
                let _ = GLOBAL_TIMER_HANDLE
                    .delay(Instant::now() + Duration::from_secs(RETRY_INTERVAL_SEC))
                    .compat()
                    .await;
            }
        }
        None
    }

    pub async fn reconnect_leader(
        &self,
        leader: &Member,
    ) -> Result<Option<(PdClientStub, String)>> {
        fail_point!("connect_leader", |_| Err(box_err!(
            "failed to connect to leader"
        )));
        // Try to connect the PD cluster leader.
        match self.connect_member(leader, true).await {
            Some((client, address)) => Ok(Some((client, address))),
            None => Err(box_err!("failed to connect to {:?}", leader)),
        }
    }

    pub async fn reconnect_followers(
        &self,
        members: &[Member],
        leader: &Member,
    ) -> Result<Option<(PdClientStub, String)>> {
        // Try to connect the PD cluster follower.
        for m in members.iter().filter(|m| *m != leader) {
            match self.connect_member(m, false).await {
                Some((client, address)) => return Ok(Some((client, address))),
                None => continue,
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

pub fn get_random_member_addr(member: &Member) -> String {
    let urls = member.get_client_urls();
    if !urls.is_empty() {
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0, urls.len());
        return urls[idx].to_string();
    }
    "".to_string()
}

#[cfg(test)]
pub mod tests {
    use kvproto::pdpb::Member;

    use crate::util::get_random_member_addr;

    #[test]
    fn test_get_random_member_addr() {
        let mut m = Member::default();
        assert_eq!(get_random_member_addr(&m), "");
        m.set_client_urls(vec!["a".to_string(), "b".to_string(), "c".to_string()].into());

        for _ in 0..10 {
            assert_ne!(get_random_member_addr(&m), "");
        }
    }
}
