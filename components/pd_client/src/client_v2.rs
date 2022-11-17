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
        Arc, Mutex as StdMutex,
    },
    time::Duration,
    u64,
};

use fail::fail_point;
use futures::{
    compat::Future01CompatExt,
    executor::block_on,
    future::{FutureExt, TryFutureExt},
    select,
    sink::SinkExt,
    stream::{Stream, StreamExt},
    task::{Context, Poll},
};
use grpcio::{
    CallOption, Channel, ClientDuplexReceiver, ConnectivityState, EnvBuilder, Environment,
    WriteFlags,
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
    box_err, error, info,
    mpsc::future as mpsc,
    slow_log, thd_name,
    time::{duration_to_sec, Instant},
    timer::GLOBAL_TIMER_HANDLE,
    warn,
};
use tokio::sync::{Mutex, Notify};
use txn_types::TimeStamp;

use super::{
    client::{CLIENT_PREFIX, CQ_COUNT},
    metrics::*,
    util::{check_resp_header, PdConnector, TargetInfo},
    Config, Error, FeatureGate, RegionInfo, Result, UnixSecs,
    REQUEST_TIMEOUT as REQUEST_TIMEOUT_SEC,
};
use crate::PdFuture;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(REQUEST_TIMEOUT_SEC);

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

/// A shared [`RawClient`] with a local copy of cache.
#[derive(Clone)]
pub struct CachedRawClient {
    context: Arc<ConnectContext>,

    latest: Arc<Mutex<Option<RawClient>>>,
    version: Arc<AtomicU64>,
    on_reconnect_notify: Arc<Notify>,

    cache: Option<RawClient>,
    cache_version: u64,
}

impl CachedRawClient {
    fn new(context: ConnectContext, on_reconnect_notify: Arc<Notify>) -> Self {
        Self {
            context: Arc::new(context),
            latest: Arc::new(Mutex::new(None)),
            version: Arc::new(AtomicU64::new(0)),
            on_reconnect_notify,
            cache: None,
            cache_version: 0,
        }
    }

    #[inline]
    async fn refresh_cache(&mut self) -> bool {
        if self.cache_version < self.version.load(Ordering::Acquire) {
            let latest = self.latest.lock().await;
            self.cache = (*latest).clone();
            self.cache_version = self.version.load(Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    #[inline]
    async fn publish_cache(&mut self) {
        let latest_version = {
            let mut latest = self.latest.lock().await;
            *latest = self.cache.clone();
            self.version.fetch_add(1, Ordering::AcqRel) + 1
        };
        debug_assert!(self.cache_version < latest_version);
        self.cache_version = latest_version;
    }

    /// Refreshes the local cache with latest client, then waits for the
    /// connection to be ready.
    /// The connection must be available if this function returns `Ok(())`.
    async fn wait_for_ready(&mut self) -> Result<()> {
        self.refresh_cache().await;
        if self.cache.is_none() {
            if tokio::time::timeout(REQUEST_TIMEOUT, self.on_reconnect_notify.notified())
                .await
                .is_ok()
            {
                self.refresh_cache().await;
                debug_assert!(self.cache.is_some());
            }
            if self.cache.is_none() {
                return Err(box_err!("Connection is not initialized in time"));
            }
        }
        select! {
            r = self.channel().wait_for_connected(REQUEST_TIMEOUT).fuse() => {
                if r {
                    return Ok(());
                }
            }
            _ = self.on_reconnect_notify.notified().fuse() => {
                let _prev_version = self.cache_version;
                self.refresh_cache().await;
                debug_assert!(_prev_version < self.cache_version);
                return Ok(())
            }
        }
        Err(box_err!(
            "Connection unavailable {:?}",
            self.channel().check_connectivity_state(false)
        ))
    }

    /// Makes the first connection.
    async fn connect(&mut self) -> Result<()> {
        debug_assert!(self.cache.is_none());
        // -1 means the max.
        let retries = match self.context.cfg.retry_max_count {
            -1 => std::isize::MAX,
            v => v.saturating_add(1),
        };
        for i in 0..retries {
            match self
                .context
                .connector
                .validate_endpoints(&self.context.cfg, false)
                .await
            {
                Ok((stub, target_info, members, _)) => {
                    self.cache = Some(RawClient {
                        stub,
                        target_info,
                        members,
                    });
                    self.publish_cache().await;
                }
                Err(e) => {
                    if i as usize % self.context.cfg.retry_log_every == 0 {
                        warn!("validate PD endpoints failed"; "err" => ?e);
                    }
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + self.context.cfg.retry_interval.0)
                        .compat()
                        .await;
                }
            }
        }
        Err(box_err!("endpoints are invalid"))
    }

    /// Increases global version only when a new connection is established.
    /// Might panic if `wait_for_ready` isn't called up-front.
    async fn reconnect(&mut self) -> Result<bool> {
        let force = self.channel().check_connectivity_state(true)
            == ConnectivityState::GRPC_CHANNEL_SHUTDOWN;
        if self
            .cache
            .as_mut()
            .unwrap()
            .maybe_reconnect(&self.context, force)
            .await?
        {
            self.publish_cache().await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn stub(&self) -> &PdClientStub {
        &self.cache.as_ref().unwrap().stub
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn channel(&self) -> &Channel {
        // self.cache.stub.client.channel()
        unimplemented!()
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn call_option(&self) -> CallOption {
        self.cache.as_ref().unwrap().target_info.call_option()
    }

    /// Might panic if `wait_for_ready` isn't called up-front.
    #[inline]
    fn cluster_id(&self) -> u64 {
        self.cache
            .as_ref()
            .unwrap()
            .members
            .get_header()
            .get_cluster_id()
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
        self.cache.as_ref().unwrap().members.get_leader().clone()
    }
}

pub struct CachedDuplexResponse<T> {
    latest: DuplexResponseHolder<T>,
    cache: Option<ClientDuplexReceiver<T>>,
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

            let latest = self.latest.lock().unwrap().take();
            self.cache = latest;
            if self.cache.is_none() {
                return Poll::Pending;
            }
        }
    }
}

type DuplexResponseHolder<T> = Arc<StdMutex<Option<ClientDuplexReceiver<T>>>>;
type CallbackHolder = Arc<StdMutex<Option<Box<dyn Fn() + Sync + Send + 'static>>>>;

#[derive(Clone)]
pub struct RpcClient {
    raw_client: CachedRawClient,
    feature_gate: FeatureGate,
    on_reconnect_cb: CallbackHolder,
}

async fn reconnect_loop(
    mut client: CachedRawClient,
    on_reconnect_notify: Arc<Notify>,
    on_reconnect_cb: CallbackHolder,
) {
    if let Err(e) = client.connect().await {
        error!("failed to connect pd"; "err" => ?e);
        return;
    } else {
        on_reconnect_notify.notify_waiters();
    }
    loop {
        if client.channel().wait_for_connected(REQUEST_TIMEOUT).await {
            let state = ConnectivityState::GRPC_CHANNEL_READY;
            // Checks for leader change periodically.
            client
                .channel()
                .wait_for_state_change(state, REQUEST_TIMEOUT)
                .await;
        }
        match client.reconnect().await {
            Ok(true) => {
                if let Some(ref f) = *on_reconnect_cb.lock().unwrap() {
                    f();
                }
                on_reconnect_notify.notify_waiters();
            }
            Err(e) => {
                warn!("failed to reconnect pd"; "err" => ?e);
            }
            _ => {}
        }
    }
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

        let on_reconnect_cb: CallbackHolder = Default::default();
        let on_reconnect_notify = Arc::new(Notify::new());
        let context = ConnectContext {
            cfg: cfg.clone(),
            connector: PdConnector::new(env.clone(), security_mgr),
        };
        let raw_client = CachedRawClient::new(context, on_reconnect_notify.clone());

        let lame_client = PdClientStub::new(Channel::lame(env, "0.0.0.0:0"));
        lame_client.spawn(reconnect_loop(
            raw_client.clone(),
            on_reconnect_notify,
            on_reconnect_cb.clone(),
        ));

        Ok(Self {
            raw_client,
            feature_gate: Default::default(),
            on_reconnect_cb,
        })
    }

    pub fn set_on_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        *self.on_reconnect_cb.lock().unwrap() = Some(Box::new(f));
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
        block_on(self.raw_client.reconnect())
    }

    #[cfg(feature = "testexport")]
    pub fn cluster_id(&self) -> u64 {
        self.raw_client.cluster_id()
    }
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

    fn load_global_config(&mut self, list: Vec<String>) -> PdFuture<HashMap<String, String>>;

    fn watch_global_config(
        &mut self,
    ) -> Result<grpcio::ClientSStreamReceiver<pdpb::WatchGlobalConfigResponse>>;

    fn get_cluster_id(&mut self) -> Result<u64>;

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

    fn get_store(&mut self, store_id: u64) -> Result<metapb::Store>;

    fn get_store_async(&mut self, store_id: u64) -> PdFuture<metapb::Store>;

    fn get_store_stats_async(&mut self, store_id: u64) -> PdFuture<pdpb::StoreStats>;

    fn get_all_stores(&mut self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>>;

    fn get_cluster_config(&mut self) -> Result<metapb::Cluster>;

    fn get_region_and_leader(
        &mut self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)>;

    fn get_region(&mut self, key: &[u8]) -> Result<metapb::Region>;

    fn get_region_info(&mut self, key: &[u8]) -> Result<RegionInfo>;

    fn get_region_async(&mut self, key: &[u8]) -> PdFuture<metapb::Region>;

    fn get_region_info_async(&mut self, key: &[u8]) -> PdFuture<RegionInfo>;

    fn get_region_by_id(&mut self, region_id: u64) -> PdFuture<Option<metapb::Region>>;

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

impl PdClient for RpcClient {
    type ResponseChannel<R: Debug> = CachedDuplexResponse<R>;

    fn create_region_heartbeat_stream(
        &mut self,
        wake_policy: mpsc::WakePolicy,
    ) -> Result<(
        mpsc::Sender<RegionHeartbeatRequest>,
        Self::ResponseChannel<RegionHeartbeatResponse>,
    )> {
        let (tx, rx) = mpsc::unbounded(wake_policy);
        let response_holder = Arc::new(std::sync::Mutex::new(None));
        let response_holder_clone = response_holder.clone();
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
                let (mut bk_tx, bk_rx) = raw_client
                    .stub()
                    .region_heartbeat_opt(raw_client.call_option())
                    .unwrap_or_else(|e| {
                        panic!("fail to request PD {} err {:?}", "region_heartbeat", e)
                    });
                *response_holder_clone.lock().unwrap() = Some(bk_rx);
                let res = bk_tx.send_all(&mut requests).await;
                if res.is_ok() {
                    // requests are drained.
                    break;
                } else {
                    warn!("region heartbeat stream exited"; "res" => ?res);
                }
                let _ = bk_tx.close().await;
            }
        });
        Ok((
            tx,
            CachedDuplexResponse {
                latest: response_holder,
                cache: None,
            },
        ))
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
                            warn!("region buckets stream exited: {:?}", send_res);
                        }
                    }
                    recv_res = bk_rx.fuse() => {
                        warn!("region buckets stream exited: {:?}", recv_res);
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
        let response_holder = Arc::new(std::sync::Mutex::new(None));
        let response_holder_clone = response_holder.clone();
        let mut raw_client = self.raw_client.clone();
        let mut requests = Box::pin(rx).map(|r| Ok((r, WriteFlags::default())));
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for Tso stream"; "err" => ?e);
                    continue;
                }
                let (mut bk_tx, bk_rx) = raw_client
                    .stub()
                    .tso_opt(raw_client.call_option())
                    .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "tso", e));
                *response_holder_clone.lock().unwrap() = Some(bk_rx);
                let res = bk_tx.send_all(&mut requests).await;
                if res.is_ok() {
                    // requests are drained.
                    break;
                } else {
                    warn!("tso exited"; "res" => ?res);
                }
                let _ = bk_tx.close().await;
            }
        });
        Ok((
            tx,
            CachedDuplexResponse {
                latest: response_holder,
                cache: None,
            },
        ))
    }

    fn load_global_config(&mut self, list: Vec<String>) -> PdFuture<HashMap<String, String>> {
        use kvproto::pdpb::LoadGlobalConfigRequest;
        let mut req = LoadGlobalConfigRequest::new();
        req.set_names(list.into());
        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            let fut = raw_client.stub().load_global_config_async(&req)?;
            match fut.await {
                Ok(grpc_response) => {
                    let mut res = HashMap::with_capacity(grpc_response.get_items().len());
                    for c in grpc_response.get_items() {
                        if c.has_error() {
                            error!("failed to load global config with key {:?}", c.get_error());
                        } else {
                            res.insert(c.get_name().to_owned(), c.get_value().to_owned());
                        }
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
        use kvproto::pdpb::WatchGlobalConfigRequest;
        let req = WatchGlobalConfigRequest::default();
        block_on(self.raw_client.wait_for_ready())?;
        Ok(self.raw_client.stub().watch_global_config(&req)?)
    }

    fn get_cluster_id(&mut self) -> Result<u64> {
        block_on(self.raw_client.wait_for_ready())?;
        Ok(self.raw_client.cluster_id())
    }

    fn bootstrap_cluster(
        &mut self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["bootstrap_cluster"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.raw_client.header());
        req.set_store(stores);
        req.set_region(region);

        let mut resp = self
            .raw_client
            .stub()
            .bootstrap_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&mut self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_cluster_bootstrapped"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self
            .raw_client
            .stub()
            .is_bootstrapped_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&mut self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["alloc_id"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().alloc_id_opt(
            &req,
            self.raw_client
                .call_option()
                .timeout(Duration::from_secs(10)),
        )?;
        check_resp_header(resp.get_header())?;

        let id = resp.get_id();
        if id == 0 {
            return Err(box_err!("pd alloc weird id 0"));
        }
        Ok(id)
    }

    fn is_recovering_marked(&mut self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_recovering_marked"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::IsSnapshotRecoveringRequest::default();
        req.set_header(self.raw_client.header());

        let resp = self.raw_client.stub().is_snapshot_recovering_opt(
            &req,
            self.raw_client.call_option().timeout(REQUEST_TIMEOUT),
        )?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_marked())
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["put_store"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.raw_client.header());
        req.set_store(store);

        let mut resp = self
            .raw_client
            .stub()
            .put_store_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store_and_stats(
        &mut self,
        store_id: u64,
    ) -> PdFuture<(metapb::Store, pdpb::StoreStats)> {
        let timer = Instant::now();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_store_id(store_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let mut resp = raw_client
                .stub()
                .get_store_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_store_and_stats", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["get_store_and_stats"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            let store = resp.take_store();
            if store.get_state() != metapb::StoreState::Tombstone {
                Ok((store, resp.take_stats()))
            } else {
                Err(Error::StoreTombstone(format!("{:?}", store)))
            }
        })
    }

    fn get_store(&mut self, store_id: u64) -> Result<metapb::Store> {
        let (store, _) = block_on(self.get_store_and_stats(store_id))?;
        Ok(store)
    }

    fn get_store_async(&mut self, store_id: u64) -> PdFuture<metapb::Store> {
        self.get_store_and_stats(store_id).map_ok(|x| x.0).boxed()
    }

    fn get_store_stats_async(&mut self, store_id: u64) -> PdFuture<pdpb::StoreStats> {
        self.get_store_and_stats(store_id).map_ok(|x| x.1).boxed()
    }

    fn get_all_stores(&mut self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_all_stores"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.raw_client.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let mut resp = self
            .raw_client
            .stub()
            .get_all_stores_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into())
    }

    fn get_cluster_config(&mut self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.raw_client.header());

        let mut resp = self
            .raw_client
            .stub()
            .get_cluster_config_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region_and_leader(
        &mut self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region"])
            .start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::default();
        req.set_region_key(key.to_vec());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let mut resp = raw_client
                .stub()
                .get_region_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_async_opt", e)
                })
                .await?;
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

    fn get_region(&mut self, key: &[u8]) -> Result<metapb::Region> {
        block_on(self.get_region_and_leader(key)).map(|x| x.0)
    }

    fn get_region_info(&mut self, key: &[u8]) -> Result<RegionInfo> {
        block_on(self.get_region_and_leader(key)).map(|x| RegionInfo::new(x.0, x.1))
    }

    fn get_region_async(&mut self, key: &[u8]) -> PdFuture<metapb::Region> {
        self.get_region_and_leader(key).map_ok(|x| x.0).boxed()
    }

    fn get_region_info_async(&mut self, key: &[u8]) -> PdFuture<RegionInfo> {
        self.get_region_and_leader(key)
            .map_ok(|x| RegionInfo::new(x.0, x.1))
            .boxed()
    }

    fn get_region_by_id(&mut self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_region_id(region_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let mut resp = raw_client
                .stub()
                .get_region_by_id_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_by_id", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["get_region_by_id"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
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
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_region_id(region_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let mut resp = raw_client
                .stub()
                .get_region_by_id_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to request PD {} err {:?}",
                        "get_region_leader_by_id", e
                    );
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["get_region_leader_by_id"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            if resp.has_region() && resp.has_leader() {
                Ok(Some((resp.take_region(), resp.take_leader())))
            } else {
                Ok(None)
            }
        })
    }

    fn ask_split(&mut self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskSplitRequest::default();
        req.set_region(region);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .ask_split_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "ask_split", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["ask_split"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            Ok(resp)
        })
    }

    fn ask_batch_split(
        &mut self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskBatchSplitRequest::default();
        req.set_region(region);
        req.set_split_count(count as u32);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .ask_batch_split_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "ask_batch_split", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["ask_batch_split"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
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
        let timer = Instant::now();

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
                .store_heartbeat_async_opt(&req, raw_client.call_option().timeout(REQUEST_TIMEOUT))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "store_heartbeat", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["store_heartbeat"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
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
        let timer = Instant::now();

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
                    raw_client.call_option().timeout(REQUEST_TIMEOUT),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "report_batch_split", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["report_batch_split"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }

    fn scatter_region(&mut self, mut region: RegionInfo) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["scatter_region"])
            .start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::default();
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        block_on(self.raw_client.wait_for_ready())?;
        req.set_header(self.raw_client.header());
        let resp = self
            .raw_client
            .stub()
            .scatter_region_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())
    }

    fn get_gc_safe_point(&mut self) -> PdFuture<u64> {
        let timer = Instant::now();

        let mut req = pdpb::GetGcSafePointRequest::default();

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
            req.set_header(raw_client.header());
            let resp = raw_client
                .stub()
                .get_gc_safe_point_async_opt(
                    &req,
                    raw_client.call_option().timeout(REQUEST_TIMEOUT),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_gc_saft_point", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["get_gc_saft_point"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            Ok(resp.get_safe_point())
        })
    }

    fn get_operator(&mut self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_operator"])
            .start_coarse_timer();

        block_on(self.raw_client.wait_for_ready())?;

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.raw_client.header());
        req.set_region_id(region_id);

        let resp = self
            .raw_client
            .stub()
            .get_operator_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }

    fn update_service_safe_point(
        &mut self,
        name: String,
        safe_point: TimeStamp,
        ttl: Duration,
    ) -> PdFuture<()> {
        let timer = Instant::now();
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
                    raw_client.call_option().timeout(REQUEST_TIMEOUT),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to request PD {} err {:?}",
                        "update_service_safe_point", e
                    );
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["update_service_safe_point"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }

    fn report_min_resolved_ts(&mut self, store_id: u64, min_resolved_ts: u64) -> PdFuture<()> {
        let timer = Instant::now();

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
                    raw_client.call_option().timeout(REQUEST_TIMEOUT),
                )
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "min_resolved_ts", e);
                })
                .await?;
            PD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["min_resolved_ts"])
                .observe(duration_to_sec(timer.saturating_elapsed()));
            check_resp_header(resp.get_header())?;
            Ok(())
        })
    }
}
