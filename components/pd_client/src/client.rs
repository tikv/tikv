// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
    u64,
};

use futures::{
    channel::mpsc,
    compat::{Compat, Future01CompatExt},
    executor::block_on,
    future::{self, BoxFuture, FutureExt, TryFlattenStream, TryFutureExt},
    sink::SinkExt,
    stream::{ErrInto, Stream, StreamExt},
    TryStreamExt,
};
use kvproto::{
    meta_storagepb::{
        self as mpb, DeleteRequest, GetRequest, PutRequest, WatchRequest, WatchResponse,
    },
    metapb,
    pdpb::{self, Member},
    replication_modepb::{RegionReplicationStatus, ReplicationStatus, StoreDrAutoSyncStatus},
    resource_manager::TokenBucketsRequest,
};
use security::SecurityManager;
use tikv_util::{
    box_err, debug, error, info, thd_name, time::Instant, timer::GLOBAL_TIMER_HANDLE, warn, Either,
    HandyRwLock,
};
use txn_types::TimeStamp;
use yatp::{task::future::TaskCell, ThreadPool};

use super::{
    meta_storage::{Delete, Get, MetaStorageClient, Put, Watch},
    metrics::*,
    util::{check_resp_header, Client, PdConnector},
    BucketStat, Config, Error, FeatureGate, PdClient, PdFuture, RegionInfo, RegionStat, Result,
    UnixSecs, REQUEST_TIMEOUT,
};

pub const CQ_COUNT: usize = 1;
pub const CLIENT_PREFIX: &str = "pd";
const DEFAULT_REGION_PER_BATCH: i32 = 128;

#[derive(Clone)]
pub struct RpcClient {
    cluster_id: u64,
    pd_client: Arc<Client>,
    monitor: Arc<ThreadPool<TaskCell>>,
}

impl RpcClient {
    pub fn new(
        cfg: &Config,
        security_mgr: Arc<SecurityManager>,
        spawn_handle: tokio::runtime::Handle,
    ) -> Result<RpcClient> {
        let handle = spawn_handle.clone();
        let cfg = cfg.clone();
        let join_handle =
            handle.spawn(async move { Self::new_async(&cfg, security_mgr, spawn_handle).await });
        block_on(join_handle).unwrap()
    }

    pub async fn new_async(
        cfg: &Config,
        security_mgr: Arc<SecurityManager>,
        spawn_handle: tokio::runtime::Handle,
    ) -> Result<RpcClient> {
        // -1 means the max.
        let retries = match cfg.retry_max_count {
            -1 => std::isize::MAX,
            v => v.saturating_add(1),
        };
        let monitor = Arc::new(
            yatp::Builder::new(thd_name!("pdmonitor"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        let pd_connector = PdConnector::new(security_mgr.clone(), spawn_handle.clone());
        for i in 0..retries {
            match pd_connector.validate_endpoints(cfg, true).await {
                Ok((client, target, members, tso)) => {
                    let cluster_id = members.get_header().get_cluster_id();
                    let rpc_client = RpcClient {
                        cluster_id,
                        pd_client: Arc::new(Client::new(
                            security_mgr.clone(),
                            client,
                            spawn_handle,
                            members,
                            target,
                            tso.unwrap(),
                            cfg.enable_forwarding,
                            cfg.retry_interval.0,
                        )),
                        monitor: monitor.clone(),
                    };

                    // spawn a background future to update PD information periodically
                    let duration = cfg.update_interval.0;
                    let client = Arc::downgrade(&rpc_client.pd_client);
                    let update_loop = async move {
                        loop {
                            let ok = GLOBAL_TIMER_HANDLE
                                .delay(std::time::Instant::now() + duration)
                                .compat()
                                .await
                                .is_ok();

                            if !ok {
                                warn!("failed to delay with global timer");
                                continue;
                            }

                            match client.upgrade() {
                                Some(cli) => {
                                    let req = cli.reconnect(false).await;
                                    if let Err(e) = req {
                                        warn!("failed to update PD client"; "error"=> ?e);
                                        // will update later anyway
                                    }
                                }
                                // if the client has been dropped, we can stop
                                None => break,
                            }
                        }
                    };

                    // `update_loop` contains RwLock that may block the monitor.
                    // Since the monitor does not have other critical task, it
                    // is not a major issue.
                    rpc_client.monitor.spawn(update_loop);

                    let client = Arc::downgrade(&rpc_client.pd_client);
                    let retry_interval = cfg.retry_interval.0;
                    let tso_check = async move {
                        while let Some(cli) = client.upgrade() {
                            let closed_fut = cli.inner.rl().tso.closed();
                            closed_fut.await;
                            info!("TSO stream is closed, reconnect to PD");
                            while let Err(e) = cli.reconnect(true).await {
                                warn!("failed to update PD client"; "error"=> ?e);
                                let _ = GLOBAL_TIMER_HANDLE
                                    .delay(std::time::Instant::now() + retry_interval)
                                    .compat()
                                    .await;
                            }
                        }
                    };
                    rpc_client.monitor.spawn(tso_check);

                    return Ok(rpc_client);
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

    /// Creates a new request header.
    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::default();
        header.set_cluster_id(self.cluster_id);
        header
    }

    /// Gets the leader of PD.
    pub fn get_leader(&self) -> Member {
        self.pd_client.get_leader()
    }

    /// Re-establishes connection with PD leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        block_on(self.pd_client.reconnect(true))
    }

    /// Gets given key's Region and Region's leader from PD.
    fn get_region_and_leader(
        &self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_region.start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::default();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let executor = move |client: &Client, req: pdpb::GetRegionRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let region_key = req.region_key.clone();
                let mut resp = client_stub.get_region(req).await?.into_inner();
                check_resp_header(resp.get_header())?;
                let region = if resp.has_region() {
                    resp.take_region()
                } else {
                    return Err(Error::RegionNotFound(region_key));
                };
                let leader = if resp.has_leader() {
                    Some(resp.take_leader())
                } else {
                    None
                };
                Ok((region, leader))
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_and_stats(&self, store_id: u64) -> PdFuture<(metapb::Store, pdpb::StoreStats)> {
        let timer = Instant::now();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let executor = move |client: &Client, req: pdpb::GetStoreRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let mut resp = client_stub.get_store(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .get_store_async
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                let store = resp.take_store();
                if store.get_state() != metapb::StoreState::Tombstone {
                    Ok((store, resp.take_stats()))
                } else {
                    Err(Error::StoreTombstone(format!("{:?}", store)))
                }
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }
}

fn get_region_resp_by_id(
    pd_client: Arc<Client>,
    header: pdpb::RequestHeader,
    region_id: u64,
) -> PdFuture<pdpb::GetRegionResponse> {
    let timer = Instant::now();
    let mut req = pdpb::GetRegionByIdRequest::default();
    req.set_header(header);
    req.set_region_id(region_id);

    let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
        let mut client_stub = client.inner.rl().client_stub.clone();
        Box::pin(async move {
            let resp = client_stub.get_region_by_id(req).await?.into_inner();
            PD_REQUEST_HISTOGRAM_VEC
                .get_region_by_id
                .observe(timer.saturating_elapsed_secs());
            check_resp_header(resp.get_header())?;
            Ok(resp)
        }) as PdFuture<_>
    };

    pd_client
        .request(req, executor, LEADER_CHANGE_RETRY)
        .execute()
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RpcClient")
            .field("cluster_id", &self.cluster_id)
            .field("leader", &self.get_leader())
            .finish()
    }
}

const LEADER_CHANGE_RETRY: usize = 10;
// periodic request like store_heartbeat, we don't need to retry.
const NO_RETRY: usize = 1;

impl PdClient for RpcClient {
    fn scan_regions(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i32,
    ) -> Result<Vec<pdpb::Region>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.scan_regions.start_coarse_timer();

        let mut req = pdpb::ScanRegionsRequest::default();
        req.set_header(self.header());
        req.set_start_key(start_key.to_vec());
        req.set_end_key(end_key.to_vec());
        req.set_limit(limit);

        let mut resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, scan_regions)?;
        check_resp_header(resp.get_header())?;
        Ok(resp.take_regions().into())
    }

    fn batch_load_regions(
        &self,
        mut start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Vec<Vec<pdpb::Region>> {
        let mut res = Vec::new();

        loop {
            let regions = self
                .scan_regions(&start_key, &end_key, DEFAULT_REGION_PER_BATCH)
                .unwrap();
            if regions.is_empty() {
                break;
            }
            res.push(regions.clone());

            let end_region = regions.last().unwrap().get_region();
            if end_region.get_end_key().is_empty() {
                break;
            }
            start_key = end_region.get_end_key().to_vec();
        }

        res
    }

    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
        stores: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .bootstrap_cluster
            .start_coarse_timer();

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let mut resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, bootstrap)?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .is_cluster_bootstrapped
            .start_coarse_timer();

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.header());

        let resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, is_bootstrapped)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.alloc_id.start_coarse_timer();

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.header());

        let resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, alloc_id)?;
        check_resp_header(resp.get_header())?;

        let id = resp.get_id();
        if id == 0 {
            return Err(box_err!("pd alloc weird id 0"));
        }
        Ok(id)
    }

    fn is_recovering_marked(&self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .is_recovering_marked
            .start_coarse_timer();

        let mut req = pdpb::IsSnapshotRecoveringRequest::default();
        req.set_header(self.header());

        let resp = sync_request!(
            &self.pd_client,
            &req,
            LEADER_CHANGE_RETRY,
            is_snapshot_recovering
        )?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_marked())
    }

    fn put_store(&self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.put_store.start_coarse_timer();

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.header());
        req.set_store(store);

        let mut resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, put_store)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_store.start_coarse_timer();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, get_store)?;
        check_resp_header(resp.get_header())?;

        let store = resp.take_store();
        if store.get_state() != metapb::StoreState::Tombstone {
            Ok(store)
        } else {
            Err(Error::StoreTombstone(format!("{:?}", store)))
        }
    }

    fn get_store_async(&self, store_id: u64) -> PdFuture<metapb::Store> {
        self.get_store_and_stats(store_id).map_ok(|x| x.0).boxed()
    }

    fn get_all_stores(&self, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_all_stores.start_coarse_timer();

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let mut resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, get_all_stores)?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .get_cluster_config
            .start_coarse_timer();

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.header());

        let mut resp = sync_request!(
            &self.pd_client,
            &req,
            LEADER_CHANGE_RETRY,
            get_cluster_config
        )?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        block_on(self.get_region_and_leader(key)).map(|x| x.0)
    }

    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        block_on(self.get_region_and_leader(key)).map(|x| RegionInfo::new(x.0, x.1))
    }

    fn get_region_async<'k>(&'k self, key: &'k [u8]) -> BoxFuture<'k, Result<metapb::Region>> {
        self.get_region_and_leader(key).map_ok(|x| x.0).boxed()
    }

    fn get_region_info_async<'k>(&'k self, key: &'k [u8]) -> BoxFuture<'k, Result<RegionInfo>> {
        self.get_region_and_leader(key)
            .map_ok(|x| RegionInfo::new(x.0, x.1))
            .boxed()
    }

    fn get_buckets_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Buckets>> {
        let header = self.header();
        let pd_client = self.pd_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, header, region_id).await?;
            if resp.has_buckets() {
                Ok(Some(resp.take_buckets()))
            } else {
                Ok(None)
            }
        }) as PdFuture<Option<_>>
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let header = self.header();
        let pd_client = self.pd_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, header, region_id).await?;
            if resp.has_region() {
                Ok(Some(resp.take_region()))
            } else {
                Ok(None)
            }
        })
    }

    fn get_region_leader_by_id(
        &self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        let header = self.header();
        let pd_client = self.pd_client.clone();
        Box::pin(async move {
            let mut resp = get_region_resp_by_id(pd_client, header, region_id).await?;
            if resp.has_region() && resp.has_leader() {
                Ok(Some((resp.take_region(), resp.take_leader())))
            } else {
                Ok(None)
            }
        })
    }

    fn region_heartbeat(
        &self,
        term: u64,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) -> PdFuture<()> {
        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut req = pdpb::RegionHeartbeatRequest::default();
        req.set_term(term);
        req.set_header(self.header());
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

        let executor = |client: &Client, req: pdpb::RegionHeartbeatRequest| {
            let mut inner = client.inner.wl();

            let last = inner.pending_heartbeat.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_HEARTBEAT_GAUGE.set(last as i64 + 1);
            let ret = inner
                .hb_sender
                .unbounded_send(req)
                .map_err(|e| Error::StreamDisconnect(e.into_send_error()));

            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn handle_region_heartbeat_response<F>(&self, _: u64, f: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        self.pd_client.handle_region_heartbeat_response(f)
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskSplitRequest::default();
        req.set_header(self.header());
        req.set_region(region);

        let executor = move |client: &Client, req: pdpb::AskSplitRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub.ask_split(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .ask_split
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn ask_batch_split(
        &self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_region(region);
        req.set_split_count(count as u32);

        let executor = move |client: &Client, req: pdpb::AskBatchSplitRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub.ask_batch_split(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .ask_batch_split
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(
        &self,
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
        let executor = move |client: &Client, req: pdpb::StoreHeartbeatRequest| {
            let feature_gate = client.feature_gate.clone();
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub
                    .store_heartbeat(req)
                    .await
                    .map(|res| {
                        PD_REQUEST_HISTOGRAM_VEC
                            .store_heartbeat
                            .observe(timer.saturating_elapsed_secs());
                        res
                    })?
                    .into_inner();
                check_resp_header(resp.get_header())?;
                match feature_gate.set_version(resp.get_cluster_version()) {
                    Err(_) => warn!("invalid cluster version: {}", resp.get_cluster_version()),
                    Ok(true) => info!("set cluster version to {}", resp.get_cluster_version()),
                    _ => {}
                };
                Ok(resp)
            }) as PdFuture<_>
        };

        self.pd_client.request(req, executor, NO_RETRY).execute()
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_regions(regions.into());

        let executor = move |client: &Client, req: pdpb::ReportBatchSplitRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub.report_batch_split(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .report_batch_split
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scatter_region(&self, mut region: RegionInfo) -> Result<()> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.scatter_region.start_coarse_timer();

        let mut req = pdpb::ScatterRegionRequest::default();
        req.set_header(self.header());
        req.set_region_id(region.get_id());
        if let Some(leader) = region.leader.take() {
            req.set_leader(leader);
        }
        req.set_region(region.region);

        let resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, scatter_region)?;
        check_resp_header(resp.get_header())
    }

    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        self.pd_client.on_reconnect(Box::new(f))
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        let timer = Instant::now();

        let mut req = pdpb::GetGcSafePointRequest::default();
        req.set_header(self.header());

        let executor = move |client: &Client, req: pdpb::GetGcSafePointRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub.get_gc_safe_point(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .get_gc_safe_point
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                Ok(resp.get_safe_point())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_stats_async(&self, store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        self.get_store_and_stats(store_id).map_ok(|x| x.1).boxed()
    }

    fn get_operator(&self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC.get_operator.start_coarse_timer();

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let resp = sync_request!(&self.pd_client, &req, LEADER_CHANGE_RETRY, get_operator)?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }

    fn batch_get_tso(&self, count: u32) -> PdFuture<TimeStamp> {
        let timer = Instant::now();
        let executor = move |client: &Client, _| {
            // Remove Box::pin and Compat when GLOBAL_TIMER_HANDLE supports futures 0.3
            let ts_fut = Compat::new(Box::pin(client.inner.rl().tso.get_timestamp(count)));
            let with_timeout = GLOBAL_TIMER_HANDLE
                .timeout(
                    ts_fut,
                    std::time::Instant::now() + Duration::from_secs(REQUEST_TIMEOUT),
                )
                .compat();
            Box::pin(async move {
                let ts = with_timeout.await.map_err(|e| {
                    if let Some(inner) = e.into_inner() {
                        inner
                    } else {
                        box_err!("get timestamp timeout")
                    }
                })?;
                PD_REQUEST_HISTOGRAM_VEC
                    .tso
                    .observe(timer.saturating_elapsed_secs());
                Ok(ts)
            }) as PdFuture<_>
        };
        self.pd_client
            .request((), executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn update_service_safe_point(
        &self,
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
        let req_for_check = req.clone();
        let executor = move |client: &Client, r: pdpb::UpdateServiceGcSafePointRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            let req = req_for_check.clone();
            Box::pin(async move {
                let resp = client_stub
                    .update_service_gc_safe_point(r)
                    .await?
                    .into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .update_service_safe_point
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.get_header())?;
                crate::check_update_service_safe_point_resp(&resp, &req)?;
                Ok(())
            }) as PdFuture<_>
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn feature_gate(&self) -> &FeatureGate {
        &self.pd_client.feature_gate
    }

    fn report_min_resolved_ts(&self, store_id: u64, min_resolved_ts: u64) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportMinResolvedTsRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);
        req.set_min_resolved_ts(min_resolved_ts);

        let executor = move |client: &Client, req: pdpb::ReportMinResolvedTsRequest| {
            let mut client_stub = client.inner.rl().client_stub.clone();
            Box::pin(async move {
                let resp = client_stub.report_min_resolved_ts(req).await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .min_resolved_ts
                    .observe(timer.saturating_elapsed_secs());
                check_resp_header(resp.into_inner().get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client.request(req, executor, NO_RETRY).execute()
    }

    fn report_region_buckets(&self, bucket_stat: &BucketStat, period: Duration) -> PdFuture<()> {
        PD_BUCKETS_COUNTER_VEC.with_label_values(&["send"]).inc();

        let mut buckets = metapb::Buckets::default();
        buckets.set_region_id(bucket_stat.meta.region_id);
        buckets.set_version(bucket_stat.meta.version);
        buckets.set_keys(bucket_stat.meta.keys.clone().into());
        buckets.set_period_in_ms(period.as_millis() as u64);
        buckets.set_stats(bucket_stat.stats.clone());
        let mut req = pdpb::ReportBucketsRequest::default();
        req.set_header(self.header());
        req.set_buckets(buckets);
        req.set_region_epoch(bucket_stat.meta.region_epoch.clone());

        let executor = |client: &Client, req: pdpb::ReportBucketsRequest| {
            let mut inner = client.inner.wl();

            let last = inner.pending_buckets.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_BUCKETS_GAUGE.set(last as i64 + 1);
            let ret = inner
                .buckets_sender
                .unbounded_send(req)
                .map_err(|e| Error::StreamDisconnect(e.into_send_error()));
            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_ru_metrics(&self, req: TokenBucketsRequest) -> PdFuture<()> {
        let executor = |client: &Client, req: TokenBucketsRequest| {
            let mut inner = client.inner.wl();
            let ret = inner
                .rg_sender
                .unbounded_send(req)
                .map_err(|e| Error::StreamDisconnect(e.into_send_error()));
            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client.request(req, executor, NO_RETRY).execute()
    }
}

impl RpcClient {
    fn fill_cluster_id_for(&self, header: &mut mpb::RequestHeader) {
        header.cluster_id = self.cluster_id;
    }
}

impl MetaStorageClient for RpcClient {
    fn get(&self, mut req: Get) -> PdFuture<kvproto::meta_storagepb::GetResponse> {
        let timer = Instant::now();
        self.fill_cluster_id_for(req.inner.mut_header());
        let executor = move |client: &Client, req: GetRequest| {
            let mut meta_storage = client.inner.rl().meta_storage.clone();
            Box::pin(async move {
                fail::fail_point!("meta_storage_get", req.key.ends_with(b"rejectme"), |_| {
                    Err(super::Error::Grpc(grpcio::Error::RemoteStopped))
                });
                let resp = meta_storage.get(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .meta_storage_get
                    .observe(timer.saturating_elapsed_secs());
                Ok(resp)
            }) as _
        };

        self.pd_client
            .request(req.into(), executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn put(&self, mut req: Put) -> PdFuture<kvproto::meta_storagepb::PutResponse> {
        let timer = Instant::now();
        self.fill_cluster_id_for(req.inner.mut_header());
        let executor = move |client: &Client, req: PutRequest| {
            let mut meta_storage = client.inner.rl().meta_storage.clone();
            Box::pin(async move {
                let resp = meta_storage.put(req).await?.into_inner();
                PD_REQUEST_HISTOGRAM_VEC
                    .meta_storage_put
                    .observe(timer.saturating_elapsed_secs());
                Ok(resp)
            }) as _
        };

        self.pd_client
            .request(req.into(), executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn delete(&self, mut req: Delete) -> PdFuture<kvproto::meta_storagepb::DeleteResponse> {
        let timer = Instant::now();
        self.fill_cluster_id_for(req.inner.mut_header());
        let executor = move |client: &Client, req: DeleteRequest| {
            let mut meta_storage = client.inner.rl().meta_storage.clone();
            Box::pin(async move {
                let resp = meta_storage.delete(req).await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .meta_storage_delete
                    .observe(timer.saturating_elapsed_secs());
                Ok(resp.into_inner())
            }) as _
        };

        self.pd_client
            .request(req.into(), executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn watch(&self, mut req: Watch) -> Self::WatchStream {
        let timer = Instant::now();
        self.fill_cluster_id_for(req.inner.mut_header());
        let executor = move |client: &Client, req: WatchRequest| {
            let mut meta_storage = client.inner.rl().meta_storage.clone();
            Box::pin(async move {
                let resp = meta_storage.watch(req).await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .meta_storage_watch
                    .observe(timer.saturating_elapsed_secs());
                Ok(resp.into_inner().err_into())
            }) as _
        };

        self.pd_client
            .request(req.into(), executor, LEADER_CHANGE_RETRY)
            .execute()
            .try_flatten_stream()
    }

    type WatchStream =
        TryFlattenStream<PdFuture<ErrInto<tonic::Streaming<WatchResponse>, crate::Error>>>;
}

pub struct StreamWatchResult(Either<Option<Error>, tonic::Streaming<WatchResponse>>);

impl Stream for StreamWatchResult {
    type Item = Result<WatchResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.0 {
            Either::Left(e) => {
                return Poll::Ready(e.take().map(Err));
            }
            Either::Right(r) => Pin::new(r).poll_next(cx).map_err(|e| Error::Tonic(e)),
        }
    }
}
