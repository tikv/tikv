// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
    u64,
};

use fail::fail_point;
use futures::{
    channel::mpsc,
    compat::Future01CompatExt,
    executor::block_on,
    future::{FutureExt, TryFutureExt},
    join, select,
    sink::SinkExt,
    stream::StreamExt,
    task::AtomicWaker,
};
use grpcio::{CallOption, EnvBuilder, Environment, WriteFlags};
use kvproto::{
    metapb::{self, BucketStats},
    pdpb::{
        self, ErrorType, GetMembersRequest, GetMembersResponse, Member, PdClient as PdClientStub,
        RegionHeartbeatRequest, RegionHeartbeatResponse, ReportBucketsRequest,
        ReportBucketsResponse, ResponseHeader, TsoRequest,
    },
    replication_modepb::{RegionReplicationStatus, ReplicationStatus, StoreDrAutoSyncStatus},
};
use security::SecurityManager;
use tikv_util::{
    box_err, debug, error, info, slow_log,
    sys::thread::StdThreadBuildWrapper,
    thd_name,
    time::{duration_to_sec, Instant},
    timer::GLOBAL_TIMER_HANDLE,
    warn, Either, HandyRwLock,
};
use tokio::sync::Mutex;
use txn_types::TimeStamp;
use yatp::{task::future::TaskCell, ThreadPool};

use super::{
    metrics::*,
    tso::{TimestampRequest, TsoRequestStream},
    util::{call_option_inner, check_resp_header, sync_request, Client, PdConnector, TargetInfo},
    BucketStat, Config, Error, FeatureGate, PdClient, PdFuture, RegionInfo, RegionStat, Result,
    UnixSecs, REQUEST_TIMEOUT,
};

/// Immutable context for making requests.
struct Context {
    enable_forwarding: bool,
    on_reconnect: Option<Box<dyn Fn() + Sync + Send + 'static>>,
    connector: PdConnector,
}

#[derive(Clone)]
struct RawClient {
    pub stub: PdClientStub,
    pub target_info: TargetInfo,
    pub members: GetMembersResponse,
}

impl RawClient {
    fn call_option(&self) -> CallOption {
        self.target_info
            .call_option()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT))
    }

    /// Only updates client and version when the reconnection succeeds.
    async fn maybe_reconnect(&mut self, ctx: &Context, force: bool) -> Result<bool> {
        PD_RECONNECT_COUNTER_VEC.with_label_values(&["try"]).inc();
        let start = Instant::now();

        let members = self.members.clone();
        let direct_connected = self.target_info.direct_connected();
        slow_log!(start.saturating_elapsed(), "try reconnect pd");
        let (stub, target_info, members, _) = match ctx
            .connector
            .reconnect_pd(members, direct_connected, force, ctx.enable_forwarding)
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

        fail_point!("pd_client_reconnect", |_| Ok(false));

        self.stub = stub;
        self.target_info = target_info;
        self.members = members;
        info!("trying to update PD client done"; "spend" => ?start.saturating_elapsed());
        Ok(true)
    }
}

#[derive(Clone)]
pub struct CachedRawClient {
    context: Arc<Context>,
    latest: Arc<Mutex<RawClient>>,
    version: Arc<AtomicU64>,
    cache: RawClient,
    cache_version: u64,
}

impl CachedRawClient {
    fn new(context: Context, client: RawClient) -> Self {
        Self {
            context: Arc::new(context),
            latest: Arc::new(Mutex::new(client.clone())),
            version: Arc::new(AtomicU64::new(0)),
            cache: client,
            cache_version: 0,
        }
    }

    /// Refreshes the local cache with latest client.
    async fn refresh_cache(&mut self) -> &RawClient {
        if self.cache_version < self.version.load(Ordering::Acquire) {
            let latest = self.latest.lock().await;
            self.cache = (*latest).clone();
            self.cache_version = self.version.load(Ordering::Relaxed);
        }
        &self.cache
    }

    async fn reconnect_if_needed(&mut self) -> Result<()> {
        let mut latest = self.latest.lock().await;
        if latest.maybe_reconnect(&self.context, false).await? {
            let latest_version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
            debug_assert!(self.cache_version < latest_version);
            self.cache = latest.clone();
            self.cache_version = latest_version;
        }
        Ok(())
    }

    /// Reconnects the client if the provided version is already fresh.
    async fn force_reconnect(&mut self, version: u64) -> Result<()> {
        let mut latest = self.latest.lock().await;
        if self.version.load(Ordering::Relaxed) == version {
            let r = latest.maybe_reconnect(&self.context, true).await?;
            // Force reconnect should always return true.
            debug_assert!(r);
            let latest_version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
            debug_assert!(self.cache_version < latest_version);
            self.cache = latest.clone();
            self.cache_version = latest_version;
        }
        Ok(())
    }

    #[inline]
    fn stub(&self) -> &PdClientStub {
        &self.cache.stub
    }

    #[inline]
    fn cache_version(&self) -> u64 {
        self.cache_version
    }
}

#[derive(Clone)]
pub struct RpcClient {
    raw_client: CachedRawClient,
    heartbeat_sender: mpsc::UnboundedSender<RegionHeartbeatRequest>,
    tso_sender: tokio::sync::mpsc::Sender<TimestampRequest>,
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
            Arc::new(EnvBuilder::new()
                    // .cq_count(CQ_COUNT)
                    // .name_prefix(thd_name!(CLIENT_PREFIX))
                    .build())
        });

        // -1 means the max.
        let retries = match cfg.retry_max_count {
            -1 => std::isize::MAX,
            v => v.saturating_add(1),
        };
        let pd_connector = PdConnector::new(env.clone(), security_mgr.clone());
        for i in 0..retries {
            match pd_connector.validate_endpoints(cfg).await {
                Ok((stub, target_info, members, tso)) => {
                    let cluster_id = members.get_header().get_cluster_id();
                    let client = RawClient {
                        stub,
                        target_info,
                        members,
                    };
                    let context = Context {
                        enable_forwarding: cfg.enable_forwarding,
                        on_reconnect: None,
                        connector: pd_connector,
                    };
                    return Self::build_from_raw(CachedRawClient::new(context, client));
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

    fn build_from_raw(raw_client: CachedRawClient) -> Result<Self> {
        let (reconnect_tx, reconnect_rx) = mpsc::unbounded();
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded();
        raw_client.stub().spawn(heartbeat_loop(
            raw_client.clone(),
            heartbeat_rx,
            reconnect_tx.clone(),
        ));
        let (tso_tx, tso_rx) =
            tokio::sync::mpsc::channel::<TimestampRequest>(1 /* batch_size */);
        let raw_client_clone = raw_client.clone();
        thread::Builder::new()
            .name("tso-worker".into())
            .spawn_wrapper(move || {
                block_on(tso_loop(raw_client_clone, tso_rx, reconnect_tx.clone()))
            })
            .expect("unable to create tso worker thread");
        raw_client
            .stub()
            .spawn(reconnect_loop(raw_client.clone(), reconnect_rx));
        Ok(Self {
            raw_client,
            heartbeat_sender: heartbeat_tx,
            tso_sender: tso_tx,
        })
    }
}

impl RpcClient {
    async fn region_heartbeat(
        &mut self,
        term: u64,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) -> Result<()> {
        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut req = RegionHeartbeatRequest::default();
        req.set_term(term);
        // req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(region_stat.down_peers.into());
        req.set_pending_peers(region_stat.pending_peers.into());
        req.set_bytes_written(region_stat.written_bytes);
        req.set_keys_written(region_stat.written_keys);
        req.set_bytes_read(region_stat.read_bytes);
        req.set_keys_read(region_stat.read_keys);
        req.set_query_stats(region_stat.query_stats);
        req.set_approximate_size(region_stat.approximate_size);
        req.set_approximate_keys(region_stat.approximate_keys);
        req.set_cpu_usage(region_stat.cpu_usage);
        if let Some(s) = replication_status {
            req.set_replication_status(s);
        }
        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(region_stat.last_report_ts.into_inner());
        interval.set_end_timestamp(UnixSecs::now().into_inner());
        req.set_interval(interval);

        self.heartbeat_sender
            .send(req)
            .map_err(|e| Error::StreamDisconnect(e))
            .await
    }

    async fn get_region_by_id(&mut self, region_id: u64) -> Result<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        // req.set_header(self.header());
        req.set_region_id(region_id);

        let client = self.raw_client.refresh_cache().await;
        let mut resp = client
            .stub
            .get_region_by_id_async_opt(&req, client.call_option())
            .unwrap_or_else(|e| {
                panic!("fail to request PD {} err {:?}", "get_region_by_id", e);
            })
            .await?;
        PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region_by_id"])
            .observe(duration_to_sec(timer.saturating_elapsed()));
        // check_resp_header(resp.get_header())?;
        if resp.has_region() {
            Ok(Some(resp.take_region()))
        } else {
            Ok(None)
        }
    }

    async fn batch_get_tso(&self, count: u32) -> Result<TimeStamp> {
        let begin = Instant::now();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = TimestampRequest { sender: tx, count };
        self.tso_sender.send(req);
        let with_timeout = GLOBAL_TIMER_HANDLE
            .timeout(
                rx.compat(),
                std::time::Instant::now(), // + Duration::from_secs(REQUEST_TIMEOUT),
            )
            .compat();
        let ts = with_timeout.await.map_err(|e| -> Error {
            if e.is_timer() {
                box_err!("get timestamp timeout")
            } else {
                box_err!("Timestamp channel is dropped")
            }
        })?;
        PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["tso"])
            .observe(duration_to_sec(begin.saturating_elapsed()));
        Ok(ts)
    }
}

async fn heartbeat_loop(
    mut client: CachedRawClient,
    requests: mpsc::UnboundedReceiver<RegionHeartbeatRequest>,
    reconnect: mpsc::UnboundedSender<u64>,
) {
    let mut requests_with_flags =
        requests.map(|r| Ok((r, WriteFlags::default().buffer_hint(false))));
    loop {
        let c = client.refresh_cache().await;
        let (mut hb_tx, mut hb_rx) = c
            .stub
            .region_heartbeat_opt(c.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        let receive_and_handle_responses = async move {
            while let Some(Ok(resp)) = hb_rx.next().await {
                println!("got");
            }
            Ok(())
        };
        let (send_res, recv_res): (grpcio::Result<()>, Result<()>) = join!(
            hb_tx.send_all(&mut requests_with_flags),
            receive_and_handle_responses
        );
    }
}

async fn tso_loop(
    mut client: CachedRawClient,
    mut requests: tokio::sync::mpsc::Receiver<TimestampRequest>,
    mut reconnect: mpsc::UnboundedSender<u64>,
) {
    // The `TimestampRequest`s which are waiting for the responses from the PD
    // server
    let pending_requests = Rc::new(RefCell::new(VecDeque::with_capacity(
        1, // MAX_PENDING_COUNT
    )));

    // When there are too many pending requests, the `send_request` future will
    // refuse to fetch more requests from the bounded channel. This waker is
    // used to wake up the sending future if the queue containing pending
    // requests is no longer full.
    let sending_future_waker = Rc::new(AtomicWaker::new());

    let mut request_stream = TsoRequestStream {
        cluster_id: 0, // cluster_id
        request_rx: &mut requests,
        pending_requests: pending_requests.clone(),
        self_waker: sending_future_waker.clone(),
    }
    .map(Ok);

    loop {
        let c = client.refresh_cache().await;
        let (mut tso_tx, mut tso_rx) = c
            .stub
            .tso_opt(c.call_option())
            .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "region_heartbeat", e));
        let pending_requests = pending_requests.clone();
        let sending_future_waker = sending_future_waker.clone();
        let receive_and_handle_responses = async move {
            while let Some(Ok(resp)) = tso_rx.next().await {
                let mut pending_requests = pending_requests.borrow_mut();

                // Wake up the sending future blocked by too many pending requests as we are
                // consuming some of them here.
                if pending_requests.len() >= 1
                // MAX_PENDING_COUNT
                {
                    sending_future_waker.wake();
                }

                // allocate_timestamps(&resp, &mut pending_requests)?;
                // PD_PENDING_TSO_REQUEST_GAUGE.set(pending_requests.len() as
                // i64);
            }
            Ok(())
        };
        let (send_res, recv_res): (grpcio::Result<()>, Result<()>) = join!(
            tso_tx.send_all(&mut request_stream),
            receive_and_handle_responses
        );
    }
}

async fn reconnect_loop(mut client: CachedRawClient, mut reconnect: mpsc::UnboundedReceiver<u64>) {
    loop {
        select! {
            _ = GLOBAL_TIMER_HANDLE
            .delay(std::time::Instant::now()).compat().fuse() => {
                client.reconnect_if_needed().await;
            },
            v = reconnect.next() => {
                if let Some(v) = v {
                    client.force_reconnect(v).await;
                } else {
                    break;
                }
            }
        };
    }
}
