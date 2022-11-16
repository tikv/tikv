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
use tokio::sync::Mutex;
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
    enable_forwarding: bool,
    connector: PdConnector,
}

#[derive(Clone)]
struct RawClient {
    channel: Channel,
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
        let (channel, stub, target_info, members, _) = match ctx
            .connector
            .reconnect_pd(
                members,
                direct_connected,
                force,
                ctx.enable_forwarding,
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

        self.channel = channel;
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
    latest: Arc<Mutex<RawClient>>,
    version: Arc<AtomicU64>,
    cache: RawClient,
    cache_version: u64,
}

impl CachedRawClient {
    fn new(context: ConnectContext, client: RawClient) -> Self {
        Self {
            context: Arc::new(context),
            latest: Arc::new(Mutex::new(client.clone())),
            version: Arc::new(AtomicU64::new(0)),
            cache: client,
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

    /// Refreshes the local cache with latest client, then waits for the
    /// connection to be ready.
    async fn wait_for_ready(&mut self) -> Result<()> {
        self.refresh_cache().await;
        for _ in 0..3 {
            if self.channel().wait_for_connected(REQUEST_TIMEOUT).await {
                return Ok(());
            } else if !self.refresh_cache().await {
                // Only retry if client is updated.
                break;
            }
        }
        Err(box_err!(
            "Connection unavailable {:?}",
            self.channel().check_connectivity_state(false)
        ))
    }

    /// Increases global version only when a new connection is established.
    async fn reconnect(&mut self, callback: &CallbackHolder) -> Result<()> {
        let force = self.channel().check_connectivity_state(true)
            == ConnectivityState::GRPC_CHANNEL_SHUTDOWN;
        if self.cache.maybe_reconnect(&self.context, force).await? {
            let latest_version = {
                let mut latest = self.latest.lock().await;
                *latest = self.cache.clone();
                self.version.fetch_add(1, Ordering::AcqRel) + 1
            };
            debug_assert!(self.cache_version < latest_version);
            self.cache_version = latest_version;
            if let Some(ref f) = *callback.lock().unwrap() {
                f();
            }
        }
        Ok(())
    }

    #[inline]
    fn stub(&self) -> &PdClientStub {
        &self.cache.stub
    }

    #[inline]
    fn channel(&self) -> &Channel {
        &self.cache.channel
    }

    #[inline]
    fn cache_version(&self) -> u64 {
        self.cache_version
    }

    #[inline]
    fn call_option(&self) -> CallOption {
        self.cache.target_info.call_option()
    }

    #[cfg(feature = "testexport")]
    #[inline]
    fn leader(&self) -> pdpb::Member {
        self.cache.members.get_leader().clone()
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
    cluster_id: u64,
    raw_client: CachedRawClient,
    feature_gate: FeatureGate,
    reconnect_tx: mpsc::Sender<u64>,
    on_reconnect: CallbackHolder,
}

impl RpcClient {
    pub fn new(
        cfg: &Config,
        shared_env: Option<Arc<Environment>>,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<RpcClient> {
        block_on(Self::new_async(cfg, shared_env, security_mgr))
    }

    pub async fn new_async(
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

        // -1 means the max.
        let retries = match cfg.retry_max_count {
            -1 => std::isize::MAX,
            v => v.saturating_add(1),
        };
        let pd_connector = PdConnector::new(env.clone(), security_mgr.clone());
        for i in 0..retries {
            match pd_connector.validate_endpoints(cfg, false).await {
                Ok((channel, stub, target_info, members, _)) => {
                    let cluster_id = members.get_header().get_cluster_id();
                    let context = ConnectContext {
                        enable_forwarding: cfg.enable_forwarding,
                        connector: pd_connector,
                    };
                    let client = RawClient {
                        channel,
                        stub,
                        target_info,
                        members,
                    };
                    return Self::build_from_raw(cluster_id, CachedRawClient::new(context, client));
                }
                Err(e) => {
                    if i as usize % cfg.retry_log_every == 0 {
                        warn!("validate PD endpoints failed"; "err" => ?e);
                    }
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + cfg.retry_interval.0)
                        .compat()
                        .await;
                }
            }
        }
        Err(box_err!("endpoints are invalid"))
    }

    fn build_from_raw(cluster_id: u64, raw_client: CachedRawClient) -> Result<Self> {
        let (reconnect_tx, reconnect_rx) = mpsc::unbounded(mpsc::WakePolicy::Immediately);
        let on_reconnect: CallbackHolder = Default::default();
        raw_client.stub().spawn(reconnect_loop(
            raw_client.clone(),
            on_reconnect.clone(),
            reconnect_rx,
        ));

        Ok(Self {
            cluster_id,
            raw_client,
            feature_gate: Default::default(),
            on_reconnect,
            reconnect_tx,
        })
    }

    pub fn set_on_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        *self.on_reconnect.lock().unwrap() = Some(Box::new(f));
    }

    #[inline]
    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::default();
        header.set_cluster_id(self.cluster_id);
        header
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
    pub fn reconnect(&mut self) -> Result<()> {
        block_on(self.raw_client.reconnect(&self.on_reconnect))
    }

    #[cfg(feature = "testexport")]
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
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

    fn get_cluster_id(&self) -> Result<u64>;

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
        let reconnect = self.reconnect_tx.clone();
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
                    if reconnect.send(raw_client.cache_version()).is_err() {
                        break;
                    }
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
                if reconnect.send(raw_client.cache_version()).is_err() {
                    break;
                }
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
        let reconnect = self.reconnect_tx.clone();
        let mut requests = Box::pin(rx).map(|r| Ok((r, WriteFlags::default())));
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for ReportRegionBuckets stream"; "err" => ?e);
                    if reconnect.send(raw_client.cache_version()).is_err() {
                        break;
                    }
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
                if reconnect.send(raw_client.cache_version()).is_err() {
                    break;
                }
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
        let reconnect = self.reconnect_tx.clone();
        let mut requests = Box::pin(rx).map(|r| Ok((r, WriteFlags::default())));
        self.raw_client.stub().spawn(async move {
            loop {
                if let Err(e) = raw_client.wait_for_ready().await {
                    warn!("failed to acquire client for Tso stream"; "err" => ?e);
                    if reconnect.send(raw_client.cache_version()).is_err() {
                        break;
                    }
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
                if reconnect.send(raw_client.cache_version()).is_err() {
                    break;
                }
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

    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &mut self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["bootstrap_cluster"])
            .start_coarse_timer();

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        block_on(self.raw_client.wait_for_ready())?;
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

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.header());

        block_on(self.raw_client.wait_for_ready())?;
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

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.header());

        block_on(self.raw_client.wait_for_ready())?;
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

        let mut req = pdpb::IsSnapshotRecoveringRequest::default();
        req.set_header(self.header());

        block_on(self.raw_client.wait_for_ready())?;
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

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.header());
        req.set_store(store);

        block_on(self.raw_client.wait_for_ready())?;
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
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        block_on(self.raw_client.wait_for_ready())?;
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

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.header());

        block_on(self.raw_client.wait_for_ready())?;
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
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_region_id(region_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_region_id(region_id);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_region(region);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_region(region);
        req.set_split_count(count as u32);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
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
        req.set_header(self.header());
        req.set_regions(regions.into());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        block_on(self.raw_client.wait_for_ready())?;
        let resp = self
            .raw_client
            .stub()
            .scatter_region_opt(&req, self.raw_client.call_option().timeout(REQUEST_TIMEOUT))?;
        check_resp_header(resp.get_header())
    }

    fn get_gc_safe_point(&mut self) -> PdFuture<u64> {
        let timer = Instant::now();

        let mut req = pdpb::GetGcSafePointRequest::default();
        req.set_header(self.header());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        block_on(self.raw_client.wait_for_ready())?;
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
        req.set_header(self.header());
        req.set_service_id(name.into());
        req.set_ttl(ttl.as_secs() as _);
        req.set_safe_point(safe_point.into_inner());

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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
        req.set_header(self.header());
        req.set_store_id(store_id);
        req.set_min_resolved_ts(min_resolved_ts);

        let mut raw_client = self.raw_client.clone();
        Box::pin(async move {
            raw_client.wait_for_ready().await?;
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

async fn reconnect_loop(
    mut client: CachedRawClient,
    on_reconnect: CallbackHolder,
    mut reconnect: mpsc::Receiver<u64>,
) {
    loop {
        while !client.channel().wait_for_connected(REQUEST_TIMEOUT).await {
            // wait_for_connected will attempt to connect internally. When it fails we
            // should force reconnect.
            if let Err(e) = client.reconnect(&on_reconnect).await {
                warn!("failed to reconnect pd"; "err" => ?e);
            }
        }
        let state = ConnectivityState::GRPC_CHANNEL_READY;
        select! {
            _ = client.channel().wait_for_state_change(state, REQUEST_TIMEOUT).fuse() => {},
            v = reconnect.next().fuse() => {
                if v.is_none() {
                    break;
                } else if v.unwrap() < client.cache_version() {
                    continue;
                }
            }
        }
        if let Err(e) = client.reconnect(&on_reconnect).await {
            warn!("failed to reconnect pd"; "err" => ?e);
        }
    }
}
