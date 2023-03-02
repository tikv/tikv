// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! PD Client V2
//!
//! In V1, the connection to PD and related states are all shared under a
//! `RwLock`. The maintenance of these states are implemented in a
//! decentralized way: each request will try to rebuild the connection on its
//! own if it encounters a network error.
//!
//! In V2, the responsibility to maintain the connection is moved into one
//! single long-running coroutine, namely [`reconnect_loop`]. Users of the
//! connection subscribe changes instead of altering it themselves.

use std::{
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant as StdInstant},
    u64,
};

use fail::fail_point;
use futures::{
    compat::{Compat, Future01CompatExt},
    executor::block_on,
    future::FutureExt,
    select,
    sink::SinkExt,
    stream::{Stream, StreamExt},
    task::{Context, Poll},
};
use grpcio::{
    CallOption, Channel, ClientDuplexReceiver, ConnectivityState, EnvBuilder, Environment,
    Error as GrpcError, Result as GrpcResult, WriteFlags,
};
use kvproto::{
    metapb,
    pdpb::{
        self, GetMembersResponse, PdClient as PdClientStub, RegionHeartbeatRequest,
        RegionHeartbeatResponse, ReportBucketsRequest, TsoRequest, TsoResponse,
    },
    replication_modepb::{ReplicationStatus, StoreDrAutoSyncStatus},
};
use security::SecurityManager;
use tikv_util::{
    box_err, error, info, mpsc::future as mpsc, slow_log, thd_name, time::Instant,
    timer::GLOBAL_TIMER_HANDLE, warn,
};
use tokio::sync::{broadcast, mpsc as tokio_mpsc};
use txn_types::TimeStamp;

use super::{
    client::{CLIENT_PREFIX, CQ_COUNT},
    metrics::*,
    util::{check_resp_header, PdConnector, TargetInfo},
    Config, Error, FeatureGate, RegionInfo, Result, UnixSecs,
    REQUEST_TIMEOUT as REQUEST_TIMEOUT_SEC,
};
use crate::PdFuture;

fn request_timeout() -> Duration {
    fail_point!("pd_client_v2_request_timeout", |s| {
        use std::str::FromStr;

        use tikv_util::config::ReadableDuration;
        ReadableDuration::from_str(&s.unwrap()).unwrap().0
    });
    Duration::from_secs(REQUEST_TIMEOUT_SEC)
}

/// Immutable context for making new connections.
struct ConnectContext {
    cfg: Config,
    connector: PdConnector,
}

#[derive(Clone)]
struct RawClient {
    stub: PdClientStub,
    target_info: TargetInfo,
    members: GetMembersResponse,
}

impl RawClient {
    async fn connect(ctx: &ConnectContext) -> Result<Self> {
        // -1 means the max.
        let retries = match ctx.cfg.retry_max_count {
            -1 => std::isize::MAX,
            v => v.saturating_add(1),
        };
        for i in 0..retries {
            match ctx.connector.validate_endpoints(&ctx.cfg, false).await {
                Ok((stub, target_info, members, _)) => {
                    return Ok(RawClient {
                        stub,
                        target_info,
                        members,
                    });
                }
                Err(e) => {
                    if i as usize % ctx.cfg.retry_log_every == 0 {
                        warn!("validate PD endpoints failed"; "err" => ?e);
                    }
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(StdInstant::now() + ctx.cfg.retry_interval.0)
                        .compat()
                        .await;
                }
            }
        }
        Err(box_err!("PD endpoints are invalid"))
    }

    /// Returns Ok(true) when a new connection is established.
    async fn maybe_reconnect(&mut self, ctx: &ConnectContext, force: bool) -> Result<bool> {
        PD_RECONNECT_COUNTER_VEC.with_label_values(&["try"]).inc();
        let start = Instant::now();

        let members = self.members.clone();
        let direct_connected = self.target_info.direct_connected();
        slow_log!(start.saturating_elapsed(), "try reconnect pd");
        let (stub, target_info, members, _) = match ctx
            .connector
            .reconnect_pd(
                members,
                direct_connected,
                force,
                ctx.cfg.enable_forwarding,
                false,
            )
            .await
        {
            Err(e) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["failure"])
                    .inc();
                return Err(e);
            }
            Ok(None) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["no-need"])
                    .inc();
                return Ok(false);
            }
            Ok(Some(tuple)) => {
                PD_RECONNECT_COUNTER_VEC
                    .with_label_values(&["success"])
                    .inc();
                tuple
            }
        };

        fail_point!("pd_client_v2_reconnect", |_| Ok(true));

        self.stub = stub;
        self.target_info = target_info;
        self.members = members;

        info!("trying to update PD client done"; "spend" => ?start.saturating_elapsed());
        Ok(true)
    }
}

struct CachedRawClientCore {
    context: ConnectContext,

    latest: Mutex<RawClient>,
    version: AtomicU64,
    on_reconnect_tx: broadcast::Sender<()>,
}

/// A shared [`RawClient`] with a local copy of cache.
pub struct CachedRawClient {
    core: Arc<CachedRawClientCore>,
    should_reconnect_tx: broadcast::Sender<u64>,
    on_reconnect_rx: broadcast::Receiver<()>,

    cache: RawClient,
    cache_version: u64,
}

impl Clone for CachedRawClient {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            should_reconnect_tx: self.should_reconnect_tx.clone(),
            on_reconnect_rx: self.core.on_reconnect_tx.subscribe(),
            cache: self.cache.clone(),
            cache_version: self.cache_version,
        }
    }
}

impl CachedRawClient {
    fn new(
        cfg: Config,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        should_reconnect_tx: broadcast::Sender<u64>,
    ) -> Self {
        let lame_stub = PdClientStub::new(Channel::lame(env.clone(), "0.0.0.0:0"));
        let client = RawClient {
            stub: lame_stub,
            target_info: TargetInfo::new("0.0.0.0:0".to_string(), ""),
            members: GetMembersResponse::new(),
        };
        let context = ConnectContext {
            cfg,
            connector: PdConnector::new(env, security_mgr),
        };
        let (tx, rx) = broadcast::channel(1);
        let core = CachedRawClientCore {
            context,
            latest: Mutex::new(client.clone()),
            version: AtomicU64::new(0),
            on_reconnect_tx: tx,
        };
        Self {
            core: Arc::new(core),
            should_reconnect_tx,
            on_reconnect_rx: rx,
            cache: client,
            cache_version: 0,
        }
    }

    #[inline]
    fn refresh_cache(&mut self) -> bool {
        if self.cache_version < self.core.version.load(Ordering::Acquire) {
            let latest = self.core.latest.lock().unwrap();
            self.cache = (*latest).clone();
            self.cache_version = self.core.version.load(Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    #[inline]
    fn publish_cache(&mut self) {
        let latest_version = {
            let mut latest = self.core.latest.lock().unwrap();
            *latest = self.cache.clone();
            let v = self.core.version.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = self.core.on_reconnect_tx.send(());
            v
        };
        debug_assert!(self.cache_version < latest_version);
        self.cache_version = latest_version;
    }

    #[inline]
    async fn wait_for_a_new_client(
        rx: &mut broadcast::Receiver<()>,
        current_version: u64,
        latest_version: &AtomicU64,
    ) -> bool {
        let deadline = StdInstant::now() + request_timeout();
        loop {
            if GLOBAL_TIMER_HANDLE
                .timeout(Compat::new(Box::pin(rx.recv())), deadline)
                .compat()
                .await
                .is_ok()
            {
                if current_version < latest_version.load(Ordering::Acquire) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    /// Refreshes the local cache with latest client, then waits for the
    /// connection to be ready.
    /// The connection must be available if this function returns `Ok(())`.
    async fn wait_for_ready(&mut self) -> Result<()> {
        self.refresh_cache();
        if self.channel().check_connectivity_state(false) == ConnectivityState::GRPC_CHANNEL_READY {
            return Ok(());
        }
        select! {
            r = self
                .cache
                .stub
                .client
                .channel()
                .wait_for_connected(request_timeout())
                .fuse() =>
            {
                if r {
                    return Ok(());
                }
            }
            r = Self::wait_for_a_new_client(
                &mut self.on_reconnect_rx,
                self.cache_version,
                &self.core.version,
            ).fuse() => {
                if r {
                    assert!(self.refresh_cache());
                    return Ok(());
                }
            }
        }
        let _ = self.should_reconnect_tx.send(self.cache_version);
        Err(box_err!(
            "Connection unavailable {:?}",
            self.channel().check_connectivity_state(false)
        ))
    }

    /// Makes the first connection.
    async fn connect(&mut self) -> Result<()> {
        self.cache = RawClient::connect(&self.core.context).await?;
        self.publish_cache();
        Ok(())
    }

    /// Increases global version only when a new connection is established.
    /// Might panic if `wait_for_ready` isn't called up-front.
    async fn reconnect(&mut self) -> Result<bool> {
        let force = (|| {
            fail_point!("pd_client_force_reconnect", |_| true);
            self.channel().check_connectivity_state(true)
                == ConnectivityState::GRPC_CHANNEL_SHUTDOWN
        })();
        if self
            .cache
            .maybe_reconnect(&self.core.context, force)
            .await?
        {
            self.publish_cache();
            return Ok(true);
        }
        Ok(false)
    }

    #[inline]
    fn check_resp<T>(&mut self, resp: GrpcResult<T>) -> GrpcResult<T> {
        if matches!(
            resp,
            Err(GrpcError::RpcFailure(_) | GrpcError::RemoteStopped | GrpcError::RpcFinished(_))
        ) {
            let _ = self.should_reconnect_tx.send(self.cache_version);
        }
        resp
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn stub(&self) -> &PdClientStub {
        &self.cache.stub
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn channel(&self) -> &Channel {
        self.cache.stub.client.channel()
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn call_option(&self) -> CallOption {
        self.cache.target_info.call_option()
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn cluster_id(&self) -> u64 {
        self.cache.members.get_header().get_cluster_id()
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::default();
        header.set_cluster_id(self.cluster_id());
        header
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[cfg(feature = "testexport")]
    #[inline]
    fn leader(&self) -> pdpb::Member {
        self.cache.members.get_leader().clone()
    }

    #[inline]
    fn initialized(&self) -> bool {
        self.cache_version != 0
    }
}

async fn reconnect_loop(
    mut client: CachedRawClient,
    cfg: Config,
    mut should_reconnect: broadcast::Receiver<u64>,
) {
    if let Err(e) = client.connect().await {
        error!("failed to connect pd"; "err" => ?e);
        return;
    }
    let backoff = (|| {
        fail_point!("pd_client_v2_backoff", |s| {
            use std::str::FromStr;

            use tikv_util::config::ReadableDuration;
            ReadableDuration::from_str(&s.unwrap()).unwrap().0
        });
        request_timeout()
    })();
    let mut last_connect = StdInstant::now();
    loop {
        if client.channel().wait_for_connected(request_timeout()).await {
            let state = ConnectivityState::GRPC_CHANNEL_READY;
            select! {
                // Checks for leader change periodically.
                _ = client
                    .channel()
                    .wait_for_state_change(state, cfg.update_interval.0)
                    .fuse() => {}
                v = should_reconnect.recv().fuse() => {
                    match v {
                        Ok(v) if v < client.cache_version => continue,
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        let target = last_connect + backoff;
        if target > StdInstant::now() {
            let _ = GLOBAL_TIMER_HANDLE.delay(target).compat().await;
        }
        last_connect = StdInstant::now();
        if let Err(e) = client.reconnect().await {
            warn!("failed to reconnect pd"; "err" => ?e);
        }
    }
}

#[derive(Clone)]
pub struct RpcClient {
    pub raw_client: CachedRawClient,
    feature_gate: FeatureGate,
}

impl RpcClient {
    pub fn new(
        cfg: &Config,
        shared_env: Option<Arc<Environment>>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<RpcClient> {
        let env = shared_env.unwrap_or_else(|| {
            Arc::new(
                EnvBuilder::new()
                    .cq_count(CQ_COUNT)
                    .name_prefix(thd_name!(CLIENT_PREFIX))
                    .build(),
            )
        });

        // Use broadcast channel for the lagging feature.
        let (tx, rx) = broadcast::channel(1);
        let raw_client = CachedRawClient::new(cfg.clone(), env, security_mgr, tx);
        raw_client
            .stub()
            .spawn(reconnect_loop(raw_client.clone(), cfg.clone(), rx));

        Ok(Self {
            raw_client,
            feature_gate: Default::default(),
        })
    }

    #[inline]
    pub fn subscribe_reconnect(&self) -> broadcast::Receiver<()> {
        self.raw_client.clone().on_reconnect_rx
    }

    #[cfg(feature = "testexport")]
    pub fn feature_gate(&self) -> &FeatureGate {
        &self.feature_gate
    }

    #[cfg(feature = "testexport")]
    pub fn get_leader(&mut self) -> pdpb::Member {
        block_on(self.raw_client.wait_for_ready()).unwrap();
        self.raw_client.leader()
    }

    #[cfg(feature = "testexport")]
    pub fn reconnect(&mut self) -> Result<bool> {
        block_on(self.raw_client.wait_for_ready())?;
        block_on(self.raw_client.reconnect())
    }

    #[cfg(feature = "testexport")]
    pub fn reset_to_lame_client(&mut self) {
        let env = self.raw_client.core.context.connector.env.clone();
        let lame = PdClientStub::new(Channel::lame(env, "0.0.0.0:0"));
        self.raw_client.core.latest.lock().unwrap().stub = lame.clone();
        self.raw_client.cache.stub = lame;
    }

    #[cfg(feature = "testexport")]
    pub fn initialized(&self) -> bool {
        self.raw_client.initialized()
    }
}

async fn get_region_resp_by_id(
    mut raw_client: CachedRawClient,
    region_id: u64,
) -> Result<pdpb::GetRegionResponse> {
    let timer = Instant::now_coarse();
    let mut req = pdpb::GetRegionByIdRequest::default();
    req.set_region_id(region_id);
    raw_client.wait_for_ready().await?;
    req.set_header(raw_client.header());
    let resp = raw_client
        .stub()
        .get_region_by_id_async_opt(&req, raw_client.call_option().timeout(request_timeout()))
        .unwrap_or_else(|e| {
            panic!("fail to request PD {} err {:?}", "get_region_by_id", e);
        })
        .await;
    PD_REQUEST_HISTOGRAM_VEC
        .get_region_by_id
        .observe(timer.saturating_elapsed_secs());
    let resp = raw_client.check_resp(resp)?;
    check_resp_header(resp.get_header())?;
    Ok(resp)
}
pub trait PdClient {
    type ResponseChannel<R: Debug>: Stream<Item = Result<R>>;

    fn create_region_heartbeat_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<(
        mpsc::Sender<RegionHeartbeatRequest>,
        Self::ResponseChannel<RegionHeartbeatResponse>,
    )>;

    fn create_report_region_buckets_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<mpsc::Sender<ReportBucketsRequest>>;

    fn create_tso_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<(mpsc::Sender<TsoRequest>, Self::ResponseChannel<TsoResponse>)>;

    fn fetch_cluster_id(&mut self) -> Result<u64>;

    fn load_global_config(&mut self, config_path: String) -> PdFuture<HashMap<String, String>>;

    fn watch_global_config(
        &mut self,
    ) -> Result<grpcio::ClientSStreamReceiver<pdpb::WatchGlobalConfigResponse>>;

    fn bootstrap_cluster(
        &mut self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>>;

    fn is_cluster_bootstrapped(&mut self) -> Result<bool>;

    fn alloc_id(&mut self) -> Result<u64>;

    fn is_recovering_marked(&mut self) -> Result<bool>;

    fn put_store(&mut self, store: metapb::Store) -> Result<Option<ReplicationStatus>>;

    fn get_store_and_stats(&mut self, store_id: u64)
    -> PdFuture<(metapb::Store, pdpb::StoreStats)>;

    fn get_store(&mut self, store_id: u64) -> Result<metapb::Store> {
        block_on(self.get_store_and_stats(store_id)).map(|r| r.0)
    }

    fn get_all_stores(&mut self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>>;

    fn get_cluster_config(&mut self) -> Result<metapb::Cluster>;

    fn get_region_and_leader(
        &mut self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)>;

    fn get_region(&mut self, key: &[u8]) -> Result<metapb::Region> {
        block_on(self.get_region_and_leader(key)).map(|r| r.0)
    }

    fn get_region_info(&mut self, key: &[u8]) -> Result<RegionInfo> {
        block_on(self.get_region_and_leader(key)).map(|r| RegionInfo::new(r.0, r.1))
    }

    fn get_region_by_id(&mut self, region_id: u64) -> PdFuture<Option<metapb::Region>>;

    fn get_buckets_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Buckets>>;

    fn get_region_leader_by_id(
        &mut self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>>;

    fn ask_split(&mut self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse>;

    fn ask_batch_split(
        &mut self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse>;

    fn store_heartbeat(
        &mut self,
        stats: pdpb::StoreStats,
        store_report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse>;

    fn report_batch_split(&mut self, regions: Vec<metapb::Region>) -> PdFuture<()>;

    fn scatter_region(&mut self, region: RegionInfo) -> Result<()>;

    fn get_gc_safe_point(&mut self) -> PdFuture<u64>;

    fn get_operator(&mut self, region_id: u64) -> Result<pdpb::GetOperatorResponse>;

    fn update_service_safe_point(
        &mut self,
        name: String,
        safe_point: TimeStamp,
        ttl: Duration,
    ) -> PdFuture<()>;

    fn report_min_resolved_ts(&mut self, store_id: u64, min_resolved_ts: u64) -> PdFuture<()>;
}

pub struct CachedDuplexResponse<T> {
    latest: tokio_mpsc::Receiver<ClientDuplexReceiver<T>>,
    cache: Option<ClientDuplexReceiver<T>>,
}

impl<T> CachedDuplexResponse<T> {
    fn new() -> (tokio_mpsc::Sender<ClientDuplexReceiver<T>>, Self) {
        let (tx, rx) = tokio_mpsc::channel(1);
        (
            tx,
            Self {
                latest: rx,
                cache: None,
            },
        )
    }
}

impl<T: Debug> Stream for CachedDuplexResponse<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut receiver) = self.cache {
                match Pin::new(receiver).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
                    Poll::Pending => return Poll::Pending,
                    // If it's None or there's error, we need to update receiver.
                    _ => {}
                }
            }

            match Pin::new(&mut self.latest).poll_recv(cx) {
                Poll::Ready(Some(receiver)) => self.cache = Some(receiver),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl PdClient for RpcClient {
    type ResponseChannel<R: Debug> = CachedDuplexResponse<R>;

    fn create_region_heartbeat_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<(
        mpsc::Sender<RegionHeartbeatRequest>,
        Self::ResponseChannel<RegionHeartbeatResponse>,
    )> {
        // TODO: use bounded channel.
        let (tx, rx) = mpsc::unbounded(wake_policy);
        let (resp_tx, resp_rx) = CachedDuplexResponse::<RegionHeartbeatResponse>::new();
        let mut raw_client = self.raw_client.clone();
        let mut requests = Box::pin(rx).map(|r| {
            fail::fail_point!("region_heartbeat_send_failed", |_| {
                Err(grpcio::Error::RemoteStopped)
            });
            Ok((r, WriteFlags::default()))
        });
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for RegionHeartbeat stream"; "err" => ?e);
                    continue;
                }
                let (mut hb_tx, hb_rx) = raw_client
                    .stub()
                    .region_heartbeat_opt(raw_client.call_option())
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "region_heartbeat", e)
                    });
                if resp_tx.send(hb_rx).await.is_err() {
                    break;
                }
                let res = hb_tx.send_all(&mut requests).await;
                if res.is_ok() {
                    // requests are drained.
                    break;
                } else {
                    let res = raw_client.check_resp(res);
                    warn!("region heartbeat stream exited"; "res" => ?res);
                }
                let _ = hb_tx.close().await;
            }
        });
        Ok((tx, resp_rx))
    }

    fn create_report_region_buckets_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<mpsc::Sender<ReportBucketsRequest>> {
        let (tx, rx) = mpsc::unbounded(wake_policy);
        let mut raw_client = self.raw_client.clone();
        let mut requests = Box::pin(rx).map(|r| Ok((r, WriteFlags::default())));
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for ReportRegionBuckets stream"; "err" => ?e);
                    continue;
                }
                let (mut bk_tx, bk_rx) = raw_client
                    .stub()
                    .report_buckets_opt(raw_client.call_option())
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "report_region_buckets", e)
                    });
                select! {
                    send_res = bk_tx.send_all(&mut requests).fuse() => {
                        if send_res.is_ok() {
                            // requests are drained.
                            break;
                        } else {
                            let res = raw_client.check_resp(send_res);
                            warn!("region buckets stream exited: {:?}", res);
                        }
                    }
                    recv_res = bk_rx.fuse() => {
                        let res = raw_client.check_resp(recv_res);
                        warn!("region buckets stream exited: {:?}", res);
                    }
                }
                let _ = bk_tx.close().await;
            }
        });
        Ok(tx)
    }

    fn create_tso_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<(mpsc::Sender<TsoRequest>, Self::ResponseChannel<TsoResponse>)> {
        let (tx, rx) = mpsc::unbounded(wake_policy);
        let (resp_tx, resp_rx) = CachedDuplexResponse::<TsoResponse>::new();
        let mut raw_client = self.raw_client.clone();
        let mut requests = Box::pin(rx).map(|r| Ok((r, WriteFlags::default())));
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for Tso stream"; "err" => ?e);
                    continue;
                }
                let (mut tso_tx, tso_rx) = raw_client
                    .stub()
                    .tso_opt(raw_client.call_option())
                    .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "tso", e));
                if resp_tx.send(tso_rx).await.is_err() {
                    break;
                }
                let res = tso_tx.send_all(&mut requests).await;
                if res.is_ok() {
                    // requests are drained.
                    break;
                } else {
                    let res = raw_client.check_resp(res);
                    warn!("tso exited"; "res" => ?res);
                }
                let _ = tso_tx.close().await;
            }
        });
        Ok((tx, resp_rx))
    }

    fn load_global_config(&mut self, config_path: String) -> PdFuture<HashMap<String, String>> {
        use kvproto::pdpb::LoadGlobalConfigRequest;
        let mut req = LoadGlobalConfigRequest::new();
        req.set_config_path(config_path);
        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            let fut = raw_client.stub().load_global_config_async(&req)?;
            match fut.await {
                Ok(grpc_response) => {
                    let mut res = HashMap::with_capacity(grpc_response.get_items().len());
                    for c in grpc_response.get_items() {
                        res.insert(c.get_name().to_owned(), c.get_value().to_owned());
                    }
                    Ok(res)
                }
                Err(err) => Err(box_err!("{:?}", err)),
            }
        })
    }

    fn watch_global_config(
        &mut self,
    ) -> Result<grpcio::ClientSStreamReceiver<pdpb::WatchGlobalConfigResponse>> {
        let req = pdpb::WatchGlobalConfigRequest::default();
        block_on(self.raw_client.wait_for_ready())?;
        Ok(self.raw_client.stub().watch_global_config(&req)?)
    }

    fn fetch_cluster_id(&mut self) -> Result<u64> {
        if !self.raw_client.initialized() {
            block_on(self.raw_client.wait_for_ready())?;
        }
        let id = self.raw_client.cluster_id();
        assert!(id > 0);
        Ok(id)
    }

    fn bootstrap_cluster(
        &mut self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .bootstrap_cluster
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.raw_client.header());
        req.set_store(stores);
        req.set_region(region);

        let resp = self.raw_client.stub().bootstrap_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let mut resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&mut self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .is_cluster_bootstrapped
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().is_bootstrapped_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&mut self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.alloc_id.start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().alloc_id_opt(
            &req,
            self.raw_client
                .call_option()
                .timeout(Duration::from_secs(10)),
        );
        let resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        let id = resp.get_id();
        if id == 0 {
            return Err(box_err!("pd alloc weird id 0"));
        }
        Ok(id)
    }

    fn is_recovering_marked(&mut self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .is_recovering_marked
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::IsSnapshotRecoveringRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().is_snapshot_recovering_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_marked())
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.put_store.start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.raw_client.header());
        req.set_store(store);

        let resp = self.raw_client.stub().put_store_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let mut resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store_and_stats(
        &mut self,
        store_id: u64,
    ) -> PdFuture<(metapb::Store, pdpb::StoreStats)> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_store_id(store_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .get_store_async_opt(&req, raw_client.call_option().timeout(request_timeout()))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_store_and_stats", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .get_store_and_stats
                .observe(timer.saturating_elapsed_secs());
            let mut resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            let store = resp.take_store();
            if store.get_state() != metapb::StoreState::Tombstone {
                Ok((store, resp.take_stats()))
            } else {
                Err(Error::StoreTombstone(format!("{:?}", store)))
            }
        })
    }

    fn get_all_stores(&mut self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_all_stores.start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.raw_client.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let resp = self.raw_client.stub().get_all_stores_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let mut resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into())
    }

    fn get_cluster_config(&mut self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .get_cluster_config
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().get_cluster_config_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let mut resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region_and_leader(
        &mut self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::GetRegionRequest::default();
        req.set_region_key(key.to_vec());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .get_region_async_opt(&req, raw_client.call_option().timeout(request_timeout()))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_async_opt", e)
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .get_region
                .observe(timer.saturating_elapsed_secs());
            let mut resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            let region = if resp.has_region() {
                resp.take_region()
            } else {
                return Err(Error::RegionNotFound(req.region_key));
            };
            let leader = if resp.has_leader() {
                Some(resp.take_leader())
            } else {
                None
            };
            Ok((region, leader))
        })
    }

    fn get_buckets_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Buckets>> {
        let pd_client = self.raw_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, region_id).await?;
            if resp.has_buckets() {
                Ok(Some(resp.take_buckets()))
            } else {
                Ok(None)
            }
        })
    }

    fn get_region_by_id(&mut self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let pd_client = self.raw_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, region_id).await?;
            if resp.has_region() {
                Ok(Some(resp.take_region()))
            } else {
                Ok(None)
            }
        })
    }

    fn get_region_leader_by_id(
        &mut self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        let pd_client = self.raw_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, region_id).await?;
            if resp.has_region() && resp.has_leader() {
                Ok(Some((resp.take_region(), resp.take_leader())))
            } else {
                Ok(None)
            }
        })
    }

    fn ask_split(&mut self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::AskSplitRequest::default();
        req.set_region(region);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .ask_split_async_opt(&req, raw_client.call_option().timeout(request_timeout()))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "ask_split", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .ask_split
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(resp)
        })
    }

    fn ask_batch_split(
        &mut self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::AskBatchSplitRequest::default();
        req.set_region(region);
        req.set_split_count(count as u32);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .ask_batch_split_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "ask_batch_split", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .ask_batch_split
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(resp)
        })
    }

    fn store_heartbeat(
        &mut self,
        mut stats: pdpb::StoreStats,
        store_report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::StoreHeartbeatRequest::default();
        stats
            .mut_interval()
            .set_end_timestamp(UnixSecs::now().into_inner());
        req.set_stats(stats);
        if let Some(report) = store_report {
            req.set_store_report(report);
        }
        if let Some(status) = dr_autosync_status {
            req.set_dr_autosync_status(status);
        }

        let mut raw_client = self.raw_client.clone();
        let feature_gate = self.feature_gate.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .store_heartbeat_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "store_heartbeat", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .store_heartbeat
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            match feature_gate.set_version(resp.get_cluster_version()) {
                Err(_) => warn!("invalid cluster version: {}", resp.get_cluster_version()),
                Ok(true) => info!("set cluster version to {}", resp.get_cluster_version()),
                _ => {}
            };
            Ok(resp)
        })
    }

    fn report_batch_split(&mut self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::ReportBatchSplitRequest::default();
        req.set_regions(regions.into());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .report_batch_split_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "report_batch_split", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .report_batch_split
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }

    fn scatter_region(&mut self, mut region: RegionInfo) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.scatter_region.start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::default();
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        block_on(self.raw_client.wait_for_ready())?;
        req.set_header(self.raw_client.header());
        let resp = self.raw_client.stub().scatter_region_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())
    }

    fn get_gc_safe_point(&mut self) -> PdFuture<u64> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::GetGcSafePointRequest::default();

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .get_gc_safe_point_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_gc_saft_point", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .get_gc_safe_point
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(resp.get_safe_point())
        })
    }

    fn get_operator(&mut self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_operator.start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.raw_client.header());
        req.set_region_id(region_id);

        let resp = self.raw_client.stub().get_operator_opt(
            &req,
            self.raw_client.call_option().timeout(request_timeout()),
        );
        let resp = self.raw_client.check_resp(resp)?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }

    fn update_service_safe_point(
        &mut self,
        name: String,
        safe_point: TimeStamp,
        ttl: Duration,
    ) -> PdFuture<()> {
        let timer = Instant::now_coarse();
        let mut req = pdpb::UpdateServiceGcSafePointRequest::default();
        req.set_service_id(name.into());
        req.set_ttl(ttl.as_secs() as _);
        req.set_safe_point(safe_point.into_inner());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .update_service_gc_safe_point_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to request PD {} err {:?}",
                        "update_service_safe_point", e
                    );
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .update_service_safe_point
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }

    fn report_min_resolved_ts(&mut self, store_id: u64, min_resolved_ts: u64) -> PdFuture<()> {
        let timer = Instant::now_coarse();

        let mut req = pdpb::ReportMinResolvedTsRequest::default();
        req.set_store_id(store_id);
        req.set_min_resolved_ts(min_resolved_ts);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .report_min_resolved_ts_async_opt(
                    &req,
                    raw_client.call_option().timeout(request_timeout()),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "min_resolved_ts", e);
                })
                .await;
            PD_REQUEST_HISTOGRAM_VEC
                .min_resolved_ts
                .observe(timer.saturating_elapsed_secs());
            let resp = raw_client.check_resp(resp)?;
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }
}
